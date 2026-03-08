import sys
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "venv" / "app"
sys.path.append(str(APP_DIR))

import main  # noqa: E402


client = TestClient(main.app)


def test_analyze_success():
    csv_bytes = (
        "product,revenue,quantity,date\n"
        "A,100,2,2026-03-01\n"
        "B,250,5,2026-03-02\n"
        "A,80,1,2026-03-03\n"
        "C,40,1,2026-03-03\n"
    ).encode("utf-8")

    resp = client.post(
        "/analyze",
        files={"file": ("sample.csv", csv_bytes, "text/csv")},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert "analysis" in payload
    assert "insights" in payload
    assert "recommendations" in payload


def test_analyze_missing_columns():
    csv_bytes = (
        "product,revenue\n"
        "A,100\n"
        "B,250\n"
    ).encode("utf-8")

    resp = client.post(
        "/analyze",
        files={"file": ("bad.csv", csv_bytes, "text/csv")},
    )
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["detail"]["missing_required_columns"] == ["date"]
