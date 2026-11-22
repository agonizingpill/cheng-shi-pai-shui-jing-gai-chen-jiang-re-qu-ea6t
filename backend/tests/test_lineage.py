import os
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ["SEED_DEMO"] = "false"

import pytest
from datetime import datetime, timezone
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.database import Base
from src.models import (
    LineageJob, ManholeAsset, ManholeZoneMembership, RepairZone, SurveyBatch,
    SurveyPoint, ZoneLineageLink,
)
from src.services import lineage


@pytest.fixture()
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    for i in range(1, 9):
        session.add(ManholeAsset(
            asset_code=f"MH-{i:03d}", street_name="南河路", x=1000 + i * 5, y=2000,
            coordinate_reference="WGS84-METER", cover_type="球墨铸铁", depth_mm=2100,
            status="active", last_inspected_at=datetime(2026, 11, 1, tzinfo=timezone.utc),
        ))
    session.commit()
    yield session
    session.close()


def make_batch(db, code, day, zone_members):
    """zone_members: {cluster_label: [manhole_id, ...]}；并集即本批实测井盖。"""
    batch = SurveyBatch(
        batch_code=code, measured_at=datetime(2026, 11, day, 7, 30, tzinfo=timezone.utc),
        instrument="Leica MS60 / RUN", coordinate_reference="WGS84-METER", status="clustered",
    )
    db.add(batch)
    db.flush()
    zones = {}
    for label, members in sorted(zone_members.items()):
        point_ids = []
        for manhole_id in members:
            point = SurveyPoint(
                point_code=f"SP-{code[-9:]}-{manhole_id:02d}", batch_id=batch.id, manhole_id=manhole_id,
                x=1000 + manhole_id * 5, y=2000, elevation_mm=101200.0, settlement_mm=11.5,
                coordinate_reference="WGS84-METER",
                measured_at=datetime(2026, 11, day, 7, 30, tzinfo=timezone.utc), quality_status="clustered",
            )
            db.add(point)
            db.flush()
            point_ids.append(point.id)
        zone = RepairZone(
            zone_code=f"RZ-{code[-9:]}-{label:02d}", batch_id=batch.id, cluster_label=label,
            name=f"沉降热区 {label}", status="proposed", point_ids=point_ids,
            center_x=1000.0, center_y=2000.0, average_settlement_mm=11.5, max_settlement_mm=13.0,
            local_density=5.0, boundary={"min_x": 990, "max_x": 1050, "min_y": 1990, "max_y": 2010},
            original_cluster_evidence={"algorithm": "DBSCAN"},
        )
        db.add(zone)
        db.flush()
        zones[label] = zone
    db.commit()
    return batch, zones


def run_lineage(db, batch):
    job, _ = lineage.enqueue_lineage(db, batch.id, rpush=False)
    lineage.run_lineage_job(db, job.id)
    db.refresh(job)
    return job


def links(db, status="current"):
    return db.execute(select(ZoneLineageLink).where(ZoneLineageLink.status == status)).scalars().all()


def memberships(db, batch_id):
    return db.execute(select(ManholeZoneMembership).where(ManholeZoneMembership.batch_id == batch_id)).scalars().all()


def test_continue_links_and_membership(db):
    batch_a, za = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2, 3], 2: [4, 5]})
    batch_b, zb = make_batch(db, "SURV-2026-1121", 21, {1: [1, 2, 3], 2: [4, 5]})
    run_lineage(db, batch_a)
    run_lineage(db, batch_b)
    current = links(db)
    assert len(current) == 2
    assert {(l.from_zone_id, l.to_zone_id, l.link_type) for l in current} == {
        (za[1].id, zb[1].id, "continue"), (za[2].id, zb[2].id, "continue"),
    }
    rows = memberships(db, batch_b.id)
    assert len(rows) == 5
    assert all(r.presence == "measured" and r.confirm_status == "confirmed" for r in rows)
    assert all(r.zone_id is not None for r in rows)


def test_split_keeps_each_branch(db):
    batch_a, za = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2, 3, 4]})
    batch_b, zb = make_batch(db, "SURV-2026-1121", 21, {1: [1, 2], 2: [3, 4]})
    run_lineage(db, batch_b)
    current = links(db)
    assert len(current) == 2
    assert all(l.link_type == "split" and l.from_zone_id == za[1].id for l in current)
    destinations = {l.to_zone_id for l in current}
    assert destinations == {zb[1].id, zb[2].id}
    shared = sorted(tuple(sorted(l.shared_manhole_ids)) for l in current)
    assert shared == [(1, 2), (3, 4)]


def test_merge_links_into_single_zone(db):
    batch_a, za = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2], 2: [3, 4]})
    batch_b, zb = make_batch(db, "SURV-2026-1121", 21, {1: [1, 2, 3, 4]})
    run_lineage(db, batch_b)
    current = links(db)
    assert len(current) == 2
    assert all(l.link_type == "merge" and l.to_zone_id == zb[1].id for l in current)
    assert {l.from_zone_id for l in current} == {za[1].id, za[2].id}


def test_emerge_and_dissolve(db):
    batch_a, za = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2], 2: [7, 8]})
    batch_b, zb = make_batch(db, "SURV-2026-1121", 21, {1: [1, 2], 2: [3, 4]})
    run_lineage(db, batch_b)
    current = links(db)
    kinds = {(l.from_zone_id, l.to_zone_id, l.link_type) for l in current}
    assert (za[1].id, zb[1].id, "continue") in kinds
    assert (None, zb[2].id, "emerge") in kinds
    assert (za[2].id, None, "dissolve") in kinds


def test_missing_manhole_marked_pending_confirmation(db):
    batch_a, za = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2, 3]})
    batch_b, zb = make_batch(db, "SURV-2026-1121", 21, {1: [1, 2]})
    run_lineage(db, batch_a)
    run_lineage(db, batch_b)
    rows = {r.manhole_id: r for r in memberships(db, batch_b.id)}
    assert rows[3].presence == "missing"
    assert rows[3].confirm_status == "pending_confirmation"
    assert rows[3].zone_id is None
    assert rows[1].presence == "measured"
    asset = db.get(ManholeAsset, 3)
    history = lineage.manhole_history(db, asset)
    assert [(h["presence"], h["confirm_status"]) for h in history] == [
        ("measured", "confirmed"), ("missing", "pending_confirmation"),
    ]
    assert history[0]["zone_code"] == za[1].zone_code


def test_late_batch_inserted_and_old_links_swapped_together(db):
    batch_a, za = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2, 3]})
    batch_c, zc = make_batch(db, "SURV-2026-1121", 21, {1: [1, 2, 3]})
    run_lineage(db, batch_c)
    assert {l.segment_key for l in links(db)} == {f"{batch_a.id}->{batch_c.id}"}
    batch_b, zb = make_batch(db, "SURV-2026-1114", 14, {1: [1, 2, 3]})
    job = run_lineage(db, batch_b)
    assert job.status == "succeeded"
    current = links(db)
    assert {l.segment_key for l in current} == {
        f"{batch_a.id}->{batch_b.id}", f"{batch_b.id}->{batch_c.id}",
    }
    assert not links(db, status="pending_update")
    assert all(l.link_type == "continue" for l in current)
    rows = memberships(db, batch_b.id)
    assert len(rows) == 3 and all(r.presence == "measured" for r in rows)


def test_resume_after_interruption_without_duplicates(db, monkeypatch):
    batch_a, _ = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2, 3]})
    batch_c, _ = make_batch(db, "SURV-2026-1121", 21, {1: [1, 2, 3]})
    run_lineage(db, batch_c)
    batch_b, _ = make_batch(db, "SURV-2026-1114", 14, {1: [1, 2, 3]})
    job, _ = lineage.enqueue_lineage(db, batch_b.id, rpush=False)
    original = lineage.compute_segment
    calls = []
    def flaky(db_, prev, nxt):
        calls.append(nxt.id)
        if len(calls) == 2:
            raise RuntimeError("worker 中断")
        return original(db_, prev, nxt)
    monkeypatch.setattr(lineage, "compute_segment", flaky)
    lineage.run_lineage_job(db, job.id)
    db.refresh(job)
    assert job.status == "failed"
    assert len(job.checkpoint["done_segments"]) == 1
    monkeypatch.setattr(lineage, "compute_segment", original)
    lineage.run_lineage_job(db, job.id)
    db.refresh(job)
    assert job.status == "succeeded"
    current = links(db)
    assert len(current) == 2
    assert len({(l.segment_key, l.from_zone_id, l.to_zone_id) for l in current}) == 2
    assert len(memberships(db, batch_b.id)) == 3
    lineage.run_lineage_job(db, job.id)
    assert len(links(db)) == 2


def test_enqueue_reuses_active_job(db):
    batch_a, _ = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2, 3]})
    job1, created1 = lineage.enqueue_lineage(db, batch_a.id, rpush=False)
    job2, created2 = lineage.enqueue_lineage(db, batch_a.id, rpush=False)
    assert created1 and not created2
    assert job1.id == job2.id
    assert db.execute(select(LineageJob)).scalars().all().__len__() == 1


def test_failed_job_retry_cleans_staged_links(db, monkeypatch):
    batch_a, _ = make_batch(db, "SURV-2026-1107", 7, {1: [1, 2, 3]})
    batch_b, _ = make_batch(db, "SURV-2026-1121", 21, {1: [1, 2]})
    job, _ = lineage.enqueue_lineage(db, batch_b.id, rpush=False)
    original = lineage.compute_segment
    def broken(db_, prev, nxt):
        raise RuntimeError("中断")
    monkeypatch.setattr(lineage, "compute_segment", broken)
    lineage.run_lineage_job(db, job.id)
    db.refresh(job)
    assert job.status == "failed"
    monkeypatch.setattr(lineage, "compute_segment", original)
    job2, created = lineage.enqueue_lineage(db, batch_b.id, rpush=False)
    assert created and job2.id != job.id
    lineage.run_lineage_job(db, job2.id)
    db.refresh(job2)
    assert job2.status == "succeeded"
    assert not links(db, status="staged")
    assert len(links(db)) == 1
