"""相邻批次热区沿革：串接原始聚类、分裂/合并去向、漏测待确认、迟到批次插回与断点续算。"""
import os, secrets
from datetime import datetime, timezone
from sqlalchemy import select
from ..models import (
    AnalysisJob, LineageJob, ManholeAsset, ManholeZoneMembership, RepairZone,
    SurveyBatch, SurveyPoint, ZoneLineageLink,
)

LINEAGE_QUEUE = "manhole_settlement_lineage_jobs"


def segment_key(prev_batch_id, next_batch_id):
    return f"{prev_batch_id}->{next_batch_id}"


def _now():
    return datetime.now(timezone.utc)


def enqueue_lineage(db, anchor_batch_id, rpush=True):
    """为出片批次建立沿革任务；同批次已有排队/运行中的任务时直接复用，不新增。"""
    active = db.execute(
        select(LineageJob).where(
            LineageJob.anchor_batch_id == anchor_batch_id,
            LineageJob.status.in_(["queued", "running"]),
        )
    ).scalars().first()
    if active:
        return active, False
    for stale_job in db.execute(select(LineageJob).where(
        LineageJob.anchor_batch_id == anchor_batch_id, LineageJob.status == "failed"
    )).scalars().all():
        for link in db.execute(select(ZoneLineageLink).where(
            ZoneLineageLink.job_id == stale_job.id, ZoneLineageLink.status == "staged"
        )).scalars().all():
            db.delete(link)
    db.flush()
    anchor = db.get(SurveyBatch, anchor_batch_id)
    job = LineageJob(
        job_code=f"LJ-{anchor.batch_code}-{int(_now().timestamp())}-{secrets.token_hex(2)}",
        anchor_batch_id=anchor_batch_id, status="queued", checkpoint={},
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    if rpush:
        try:
            from redis import Redis
            Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True).rpush(LINEAGE_QUEUE, str(job.id))
        except Exception:
            # 派发失败不拖垮出片：任务保持 queued，worker 重启恢复或手动重建时会再入队
            pass
    return job, True


def requeue_unfinished(db, queue):
    """worker 启动时把中断的沿革任务重新入队，恢复后接着算。"""
    stale = db.execute(select(LineageJob).where(LineageJob.status.in_(["queued", "running"]))).scalars().all()
    for job in stale:
        queue.rpush(LINEAGE_QUEUE, str(job.id))
    return len(stale)


def purge_batch_lineage(db, batch_id):
    """重出片前清理本批次旧沿革：链接与归属随旧片区一起失效，由后续沿革任务重建。"""
    zone_ids = [z.id for z in db.execute(select(RepairZone).where(RepairZone.batch_id == batch_id)).scalars().all()]
    if zone_ids:
        for link in db.execute(select(ZoneLineageLink).where(
            (ZoneLineageLink.from_zone_id.in_(zone_ids)) | (ZoneLineageLink.to_zone_id.in_(zone_ids))
        )).scalars().all():
            db.delete(link)
    for row in db.execute(select(ManholeZoneMembership).where(ManholeZoneMembership.batch_id == batch_id)).scalars().all():
        db.delete(row)
    db.flush()


def _clustered_batches(db):
    return db.execute(
        select(SurveyBatch).where(SurveyBatch.status == "clustered").order_by(SurveyBatch.measured_at, SurveyBatch.id)
    ).scalars().all()


def build_plan(db, anchor_batch_id):
    """按巡测日期定位锚点批次，给出需重算的前后两段与受波及的旧关联段。"""
    batches = _clustered_batches(db)
    ids = [b.id for b in batches]
    if anchor_batch_id not in ids:
        raise ValueError("锚点批次尚未出片，不能计算沿革")
    idx = ids.index(anchor_batch_id)
    prev_batch = batches[idx - 1] if idx > 0 else None
    next_batch = batches[idx + 1] if idx + 1 < len(batches) else None
    segments = []
    if prev_batch:
        segments.append({"key": segment_key(prev_batch.id, anchor_batch_id), "prev": prev_batch.id, "next": anchor_batch_id})
    if next_batch:
        segments.append({"key": segment_key(anchor_batch_id, next_batch.id), "prev": anchor_batch_id, "next": next_batch.id})
    touch = {b.id for b in (prev_batch, batches[idx], next_batch) if b}
    stale_keys = set()
    for link in db.execute(select(ZoneLineageLink).where(ZoneLineageLink.status.in_(["current", "pending_update"]))).scalars().all():
        try:
            a, b = (int(x) for x in link.segment_key.split("->"))
        except ValueError:
            continue
        if a in touch or b in touch:
            stale_keys.add(link.segment_key)
    return {
        "segments": segments,
        "stale_keys": sorted(stale_keys),
        "bootstrap_batch_id": None if prev_batch else anchor_batch_id,
    }


def _zone_manholes(db, batch_id):
    """原始聚类（非道路拆分片）→ 井盖集合。"""
    zones = db.execute(
        select(RepairZone).where(RepairZone.batch_id == batch_id, RepairZone.split_from_id.is_(None))
    ).scalars().all()
    point_ids = [pid for z in zones for pid in (z.point_ids or [])]
    points = db.execute(select(SurveyPoint).where(SurveyPoint.id.in_(point_ids))).scalars().all() if point_ids else []
    owner = {p.id: p.manhole_id for p in points}
    return {z.id: (z, {owner[pid] for pid in (z.point_ids or []) if pid in owner}) for z in zones}


def compute_segment(db, prev_batch, next_batch):
    """计算一段相邻批次沿革：共享井盖连成关联边，分裂/合并保留各支去向，漏测标待确认。"""
    prev_map = _zone_manholes(db, prev_batch.id) if prev_batch else {}
    next_map = _zone_manholes(db, next_batch.id)
    edges = []
    for prev_id, (_, prev_members) in prev_map.items():
        for next_id, (_, next_members) in next_map.items():
            shared = sorted(prev_members & next_members)
            if shared:
                edges.append({"from_zone_id": prev_id, "to_zone_id": next_id, "shared_manhole_ids": shared})
    out_degree = {}
    in_degree = {}
    for edge in edges:
        out_degree[edge["from_zone_id"]] = out_degree.get(edge["from_zone_id"], 0) + 1
        in_degree[edge["to_zone_id"]] = in_degree.get(edge["to_zone_id"], 0) + 1
    links = []
    for edge in edges:
        if out_degree[edge["from_zone_id"]] > 1:
            link_type = "split"
        elif in_degree[edge["to_zone_id"]] > 1:
            link_type = "merge"
        else:
            link_type = "continue"
        links.append({**edge, "link_type": link_type})
    for next_id in next_map:
        if in_degree.get(next_id, 0) == 0:
            links.append({"from_zone_id": None, "to_zone_id": next_id, "shared_manhole_ids": [], "link_type": "emerge"})
    for prev_id in prev_map:
        if out_degree.get(prev_id, 0) == 0:
            links.append({"from_zone_id": prev_id, "to_zone_id": None, "shared_manhole_ids": [], "link_type": "dissolve"})
    membership = _membership_rows(db, prev_batch, next_batch, next_map)
    return {"links": links, "membership": membership}


def _membership_rows(db, prev_batch, next_batch, next_map):
    """下一批次各井盖归属：实测即落区，上一批热区成员本轮漏测标待确认。"""
    points = db.execute(select(SurveyPoint).where(SurveyPoint.batch_id == next_batch.id)).scalars().all()
    manhole_zone = {}
    for zone_id, (_, members) in next_map.items():
        for manhole_id in members:
            manhole_zone[manhole_id] = zone_id
    rows = []
    measured = {p.manhole_id for p in points}
    for manhole_id in sorted(measured):
        rows.append({
            "manhole_id": manhole_id, "batch_id": next_batch.id,
            "zone_id": manhole_zone.get(manhole_id),
            "presence": "measured", "confirm_status": "confirmed",
        })
    if prev_batch:
        prev_members = set()
        for _, members in _zone_manholes(db, prev_batch.id).values():
            prev_members |= members
        for manhole_id in sorted(prev_members - measured):
            rows.append({
                "manhole_id": manhole_id, "batch_id": next_batch.id,
                "zone_id": None, "presence": "missing", "confirm_status": "pending_confirmation",
            })
    return rows


def run_lineage_job(db, job_id):
    """断点续算的沿革任务：旧关联标待更新 → 分段计算落暂存 → 前后两段齐备后同事务替换。"""
    job = db.get(LineageJob, job_id)
    if not job or job.status == "succeeded":
        return
    job.status = "running"
    job.updated_at = _now()
    db.commit()
    try:
        checkpoint = job.checkpoint or {}
        if "plan" not in checkpoint:
            checkpoint = {"plan": build_plan(db, job.anchor_batch_id), "done_segments": [], "membership": {}, "swapped": False}
            job.checkpoint = checkpoint
            job.updated_at = _now()
            db.commit()
        plan = checkpoint["plan"]
        _mark_pending_update(db, plan["stale_keys"])
        db.commit()
        batches = {b.id: b for b in _clustered_batches(db)}
        for segment in plan["segments"]:
            if segment["key"] in checkpoint["done_segments"]:
                continue
            _replace_staged_segment(db, job, segment, batches, checkpoint)
            db.commit()
        bootstrap_id = plan.get("bootstrap_batch_id")
        if bootstrap_id and str(bootstrap_id) not in checkpoint["membership"]:
            anchor = db.get(SurveyBatch, bootstrap_id)
            checkpoint["membership"][str(bootstrap_id)] = _membership_rows(db, None, anchor, _zone_manholes(db, anchor.id))
            job.checkpoint = dict(checkpoint)
            db.commit()
        if not checkpoint.get("swapped"):
            _swap(db, job, plan, checkpoint)
            job.checkpoint = {**checkpoint, "swapped": True}
            db.commit()
        job.status = "succeeded"
        job.finished_at = _now()
        job.updated_at = _now()
        db.commit()
    except Exception as exc:
        db.rollback()
        job = db.get(LineageJob, job_id)
        job.status = "failed"
        job.error = str(exc)
        job.finished_at = _now()
        job.updated_at = _now()
        db.commit()


def _mark_pending_update(db, stale_keys):
    if not stale_keys:
        return
    for link in db.execute(select(ZoneLineageLink).where(
        ZoneLineageLink.segment_key.in_(stale_keys), ZoneLineageLink.status == "current"
    )).scalars().all():
        link.status = "pending_update"
    db.flush()


def _replace_staged_segment(db, job, segment, batches, checkpoint):
    """重算一段：先清本段暂存再写入，重试不会新增同一段沿革。"""
    for old in db.execute(select(ZoneLineageLink).where(
        ZoneLineageLink.segment_key == segment["key"], ZoneLineageLink.status == "staged"
    )).scalars().all():
        db.delete(old)
    db.flush()
    result = compute_segment(db, batches[segment["prev"]], batches[segment["next"]])
    for link in result["links"]:
        db.add(ZoneLineageLink(
            segment_key=segment["key"], from_zone_id=link["from_zone_id"], to_zone_id=link["to_zone_id"],
            link_type=link["link_type"], shared_manhole_ids=link["shared_manhole_ids"],
            status="staged", job_id=job.id,
        ))
    checkpoint["membership"][str(segment["next"])] = result["membership"]
    checkpoint["done_segments"] = sorted({*checkpoint["done_segments"], segment["key"]})
    job.checkpoint = dict(checkpoint)
    job.updated_at = _now()
    db.flush()


def _swap(db, job, plan, checkpoint):
    """前后两段都算好后一起替换：删待更新旧关联、暂存转正、归属重写，单事务提交。"""
    new_keys = [s["key"] for s in plan["segments"]]
    stale_keys = plan["stale_keys"]
    if stale_keys:
        for link in db.execute(select(ZoneLineageLink).where(
            ZoneLineageLink.segment_key.in_(stale_keys), ZoneLineageLink.status == "pending_update"
        )).scalars().all():
            db.delete(link)
    if new_keys:
        for link in db.execute(select(ZoneLineageLink).where(
            ZoneLineageLink.segment_key.in_(new_keys), ZoneLineageLink.status == "staged"
        )).scalars().all():
            link.status = "current"
    batch_ids = [int(b) for b in checkpoint["membership"].keys()]
    if batch_ids:
        for row in db.execute(select(ManholeZoneMembership).where(ManholeZoneMembership.batch_id.in_(batch_ids))).scalars().all():
            db.delete(row)
        db.flush()
        for batch_id, rows in checkpoint["membership"].items():
            for row in rows:
                db.add(ManholeZoneMembership(**row))
    db.flush()


def link_view(link, zones, assets):
    from_zone = zones.get(link.from_zone_id)
    to_zone = zones.get(link.to_zone_id)
    return {
        "id": link.id, "segment_key": link.segment_key,
        "from_zone_id": link.from_zone_id, "from_zone_code": from_zone.zone_code if from_zone else None,
        "to_zone_id": link.to_zone_id, "to_zone_code": to_zone.zone_code if to_zone else None,
        "link_type": link.link_type, "status": link.status,
        "shared_manhole_ids": link.shared_manhole_ids or [],
        "shared_asset_codes": [assets[m].asset_code for m in (link.shared_manhole_ids or []) if m in assets],
    }


def lineage_links_view(db):
    links = db.execute(
        select(ZoneLineageLink).where(ZoneLineageLink.status.in_(["current", "pending_update"])).order_by(ZoneLineageLink.id)
    ).scalars().all()
    zone_ids = {z for link in links for z in (link.from_zone_id, link.to_zone_id) if z}
    zones = {z.id: z for z in db.execute(select(RepairZone).where(RepairZone.id.in_(zone_ids))).scalars().all()} if zone_ids else {}
    manhole_ids = {m for link in links for m in (link.shared_manhole_ids or [])}
    assets = {a.id: a for a in db.execute(select(ManholeAsset).where(ManholeAsset.id.in_(manhole_ids))).scalars().all()} if manhole_ids else {}
    return [link_view(link, zones, assets) for link in links]


def manhole_history(db, asset):
    """按井盖查历次归属：逐批次的落区与漏测待确认。"""
    rows = db.execute(
        select(ManholeZoneMembership, SurveyBatch)
        .join(SurveyBatch, SurveyBatch.id == ManholeZoneMembership.batch_id)
        .where(ManholeZoneMembership.manhole_id == asset.id)
        .order_by(SurveyBatch.measured_at, SurveyBatch.id)
    ).all()
    zone_ids = {m.zone_id for m, _ in rows if m.zone_id}
    zones = {z.id: z for z in db.execute(select(RepairZone).where(RepairZone.id.in_(zone_ids))).scalars().all()} if zone_ids else {}
    points = db.execute(select(SurveyPoint).where(SurveyPoint.manhole_id == asset.id)).scalars().all()
    settlement = {p.batch_id: p.settlement_mm for p in points}
    return [{
        "batch_id": batch.id, "batch_code": batch.batch_code, "measured_at": batch.measured_at.isoformat() if batch.measured_at else None,
        "zone_id": m.zone_id, "zone_code": zones[m.zone_id].zone_code if m.zone_id in zones else None,
        "zone_name": zones[m.zone_id].name if m.zone_id in zones else None,
        "presence": m.presence, "confirm_status": m.confirm_status,
        "settlement_mm": settlement.get(batch.id),
    } for m, batch in rows]
