import os
os.environ["SEED_DEMO"] = "false"
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ["REDIS_URL"] = "redis://127.0.0.1:1/0"

from datetime import datetime, timezone
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.database import Base, get_db
from src.main import app
from src.models import ManholeAsset, RepairZone, SurveyBatch, SurveyPoint, User
from src.security import create_token, hash_password
from src.services import lineage

engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
TestingSession = sessionmaker(bind=engine)
Base.metadata.create_all(engine)


def override_get_db():
    db = TestingSession()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


def seed_domain():
    db = TestingSession()
    user = User(username="supervisor", display_name="顾航", password_hash=hash_password("supervisor123"), role="supervisor")
    db.add(user)
    for i in range(1, 4):
        db.add(ManholeAsset(
            asset_code=f"MH-{i:03d}", street_name="南河路", x=1000 + i * 5, y=2000,
            coordinate_reference="WGS84-METER", cover_type="球墨铸铁", depth_mm=2100,
            status="active", last_inspected_at=datetime(2026, 11, 1, tzinfo=timezone.utc),
        ))
    db.commit()
    batches = {}
    for code, day in [("SURV-2026-1107", 7), ("SURV-2026-1121", 21)]:
        batch = SurveyBatch(batch_code=code, measured_at=datetime(2026, 11, day, 7, 30, tzinfo=timezone.utc),
                            instrument="Leica MS60 / RUN", coordinate_reference="WGS84-METER", status="clustered")
        db.add(batch)
        db.flush()
        point_ids = []
        for manhole_id in (1, 2, 3):
            point = SurveyPoint(point_code=f"SP-{code[-9:]}-{manhole_id:02d}", batch_id=batch.id, manhole_id=manhole_id,
                                x=1000 + manhole_id * 5, y=2000, elevation_mm=101200.0, settlement_mm=11.5,
                                coordinate_reference="WGS84-METER",
                                measured_at=datetime(2026, 11, day, 7, 30, tzinfo=timezone.utc), quality_status="clustered")
            db.add(point)
            db.flush()
            point_ids.append(point.id)
        db.add(RepairZone(zone_code=f"RZ-{code[-9:]}-01", batch_id=batch.id, cluster_label=1, name="沉降热区 1",
                          status="proposed", point_ids=point_ids, center_x=1005.0, center_y=2000.0,
                          average_settlement_mm=11.5, max_settlement_mm=13.0, local_density=5.0,
                          boundary={"min_x": 990, "max_x": 1050, "min_y": 1990, "max_y": 2010},
                          original_cluster_evidence={"algorithm": "DBSCAN"}))
        db.commit()
        batches[code] = batch.id
    for code in batches:
        job, _ = lineage.enqueue_lineage(db, batches[code], rpush=False)
        lineage.run_lineage_job(db, job.id)
    token = create_token(db.query(User).filter_by(username="supervisor").one())
    db.close()
    return token, batches


def test_lineage_endpoints():
    token, batches = seed_domain()
    headers = {"Authorization": f"Bearer {token}"}
    with TestClient(app) as client:
        dashboard = client.get("/api/dashboard", headers=headers)
        assert dashboard.status_code == 200
        payload = dashboard.json()
        assert len(payload["lineage_links"]) == 1
        link = payload["lineage_links"][0]
        assert link["link_type"] == "continue" and link["status"] == "current"
        assert link["shared_asset_codes"] == ["MH-001", "MH-002", "MH-003"]
        assert payload["lineage_jobs"][0]["status"] == "succeeded"

        history = client.get("/api/lineage/manholes/MH-002/history", headers=headers)
        assert history.status_code == 200
        entries = history.json()["entries"]
        assert [e["batch_code"] for e in entries] == ["SURV-2026-1107", "SURV-2026-1121"]
        assert all(e["zone_code"] and e["confirm_status"] == "confirmed" for e in entries)

        missing = client.get("/api/lineage/manholes/MH-999/history", headers=headers)
        assert missing.status_code == 404

        rebuild = client.post(f"/api/lineage/rebuild/{batches['SURV-2026-1121']}", headers=headers)
        assert rebuild.status_code == 200
        job_id = rebuild.json()["job_id"]
        detail = client.get(f"/api/lineage/jobs/{job_id}", headers=headers)
        assert detail.status_code == 200
        again = client.post(f"/api/lineage/rebuild/{batches['SURV-2026-1121']}", headers=headers)
        assert again.json()["reused"] is True
        assert again.json()["job_id"] == job_id

        anon = client.get("/api/lineage/manholes/MH-002/history")
        assert anon.status_code == 401
