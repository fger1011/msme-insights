from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Body
from fastapi.responses import JSONResponse, Response
import pandas as pd
import io
import json
import sqlite3
from pathlib import Path
from datetime import datetime
import os
import logging
import requests

app = FastAPI()

REQUIRED_COLUMNS = {"product", "revenue", "date"}
OPTIONAL_COLUMNS = {"quantity"}
DB_PATH = Path(__file__).resolve().parents[2] / "data" / "insights.db"
UPLOADS_DIR = Path(__file__).resolve().parents[2] / "data" / "uploads"
LOG_DIR = Path(__file__).resolve().parents[2] / "data" / "logs"
API_TOKEN = os.getenv("MSME_API_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "api.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("msme_api")


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df = df.dropna()
    df.columns = df.columns.str.lower().str.strip()
    return df


def missing_required_columns(df: pd.DataFrame):
    missing = REQUIRED_COLUMNS.difference(set(df.columns))
    return sorted(missing)


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                rows INTEGER NOT NULL,
                columns TEXT NOT NULL,
                date_min TEXT,
                date_max TEXT,
                analysis_json TEXT NOT NULL,
                insights_json TEXT NOT NULL,
                recommendations_json TEXT NOT NULL,
                raw_path TEXT,
                cleaned_path TEXT
            )
            """
        )
        conn.commit()

        cols = [row[1] for row in conn.execute("PRAGMA table_info(analysis_history)").fetchall()]
        if "raw_path" not in cols:
            conn.execute("ALTER TABLE analysis_history ADD COLUMN raw_path TEXT")
        if "cleaned_path" not in cols:
            conn.execute("ALTER TABLE analysis_history ADD COLUMN cleaned_path TEXT")
        conn.commit()


def save_analysis(df: pd.DataFrame, analysis, insights, recommendations, raw_bytes: bytes):
    date_min = None
    date_max = None
    if "date" in df.columns:
        dates = pd.to_datetime(df["date"], errors="coerce").dropna()
        if not dates.empty:
            date_min = dates.min().date().isoformat()
            date_max = dates.max().date().isoformat()

    payload = (
        datetime.utcnow().isoformat(),
        int(len(df)),
        json.dumps(list(df.columns)),
        date_min,
        date_max,
        json.dumps(analysis),
        json.dumps(insights),
        json.dumps(recommendations),
    )

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            """
            INSERT INTO analysis_history
            (created_at, rows, columns, date_min, date_max, analysis_json, insights_json, recommendations_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        analysis_id = cur.lastrowid
        conn.commit()

    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = UPLOADS_DIR / f"raw_{analysis_id}.csv"
    cleaned_path = UPLOADS_DIR / f"cleaned_{analysis_id}.csv"
    raw_path.write_bytes(raw_bytes)
    cleaned_path.write_bytes(df.to_csv(index=False).encode("utf-8"))

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            UPDATE analysis_history
            SET raw_path = ?, cleaned_path = ?
            WHERE id = ?
            """,
            (str(raw_path), str(cleaned_path), analysis_id),
        )
        conn.commit()

    return analysis_id


def analyze_df(df: pd.DataFrame):
    analysis = {}

    if "revenue" in df.columns:
        analysis["total_revenue"] = float(df["revenue"].sum())
        analysis["average_revenue"] = float(df["revenue"].mean())

    if "quantity" in df.columns:
        analysis["total_quantity"] = int(df["quantity"].sum())

    if "product" in df.columns and "revenue" in df.columns:
        top_products = (
            df.groupby("product")["revenue"].sum()
            .sort_values(ascending=False)
            .head(5)
        )
        analysis["top_products"] = top_products.to_dict()

    return analysis


def format_insights(analysis):
    insights = []

    if "total_revenue" in analysis:
        insights.append(
            f"Total revenue is PHP {analysis['total_revenue']:.2f}."
        )
    if "average_revenue" in analysis:
        insights.append(
            f"Average revenue per transaction is PHP {analysis['average_revenue']:.2f}."
        )
    if "total_quantity" in analysis:
        insights.append(
            f"Total items sold: {analysis['total_quantity']}."
        )
    if "top_products" in analysis and analysis["top_products"]:
        top = list(analysis["top_products"].items())[0]
        insights.append(
            f"Top product is {top[0]} with PHP {top[1]:.2f} revenue."
        )
    return insights


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if API_TOKEN:
        token = request.headers.get("X-API-Token")
        if token != API_TOKEN:
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    return await call_next(request)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        detail = exc.detail if isinstance(exc.detail, dict) else {"error": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=detail)
    logger.exception("Unhandled error", extra={"path": str(request.url)})
    return JSONResponse(status_code=500, content={"error": "Internal server error"})


@app.get("/")
def read_root():
    return {"message": "MSME Insights API is running"}


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    contents = await file.read()
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    df = normalize_df(df)
    missing = missing_required_columns(df)

    return {
        "message": "File uploaded successfully",
        "rows": len(df),
        "columns": df.columns.tolist(),
        "required_columns": sorted(REQUIRED_COLUMNS),
        "optional_columns": sorted(OPTIONAL_COLUMNS),
        "missing_required_columns": missing,
    }


@app.post("/analyze")
async def analyze_csv(file: UploadFile = File(...)):
    contents = await file.read()
    logger.info("Analyze request: filename=%s size=%s", file.filename, len(contents))
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    df = normalize_df(df)
    missing = missing_required_columns(df)
    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Missing required columns",
                "missing_required_columns": missing,
            },
        )

    analysis = analyze_df(df)
    insights = format_insights(analysis)
    recommendations = generate_recommendations(df, analysis)
    init_db()
    save_analysis(df, analysis, insights, recommendations, contents)

    return {
        "message": "Analysis complete",
        "analysis": analysis,
        "insights": insights,
        "recommendations": recommendations,
    }


@app.post("/export/cleaned")
async def export_cleaned_csv(file: UploadFile = File(...)):
    contents = await file.read()
    logger.info("Export cleaned: filename=%s size=%s", file.filename, len(contents))
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    df = normalize_df(df)
    missing = missing_required_columns(df)
    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Missing required columns",
                "missing_required_columns": missing,
            },
        )

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return Response(content=csv_bytes, media_type="text/csv")


@app.post("/export/summary")
async def export_summary_json(file: UploadFile = File(...)):
    contents = await file.read()
    logger.info("Export summary: filename=%s size=%s", file.filename, len(contents))
    df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    df = normalize_df(df)
    missing = missing_required_columns(df)
    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Missing required columns",
                "missing_required_columns": missing,
            },
        )

    analysis = analyze_df(df)
    payload = {
        "analysis": analysis,
        "insights": format_insights(analysis),
        "recommendations": generate_recommendations(df, analysis),
    }
    return JSONResponse(content=payload)


def generate_recommendations(df, analysis):
    recommendations = []

    top_products = analysis.get("top_products") or {}
    if top_products:
        top_product = list(top_products.items())[0]
        recommendations.append(
            f"{top_product[0]} generates the most revenue. Consider increasing stock or promotions."
        )

    # Low sales volume insight
    if "total_quantity" in analysis and analysis["total_quantity"] < 50:
        recommendations.append(
            "Sales volume appears low. Considering marketing campaigns or discounts."
        )

    # Revenue diversification
    if top_products and len(top_products) <= 2:
        recommendations.append(
            "Revenue is concentrated in few products. Expanding product variety could reduce risk."
        )

    return recommendations


def extract_response_text(payload: dict) -> str:
    text = payload.get("output_text")
    if text:
        return text.strip()

    parts = []
    for item in payload.get("output", []):
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") in {"output_text", "text"}:
                    parts.append(content.get("text", ""))
    return "\n".join([p for p in parts if p]).strip()


@app.get("/history")
def get_history(limit: int = 20):
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, created_at, rows, columns, date_min, date_max,
                   analysis_json, insights_json, recommendations_json,
                   raw_path, cleaned_path
            FROM analysis_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    history = []
    for row in rows:
        history.append(
            {
                "id": row["id"],
                "created_at": row["created_at"],
                "rows": row["rows"],
                "columns": json.loads(row["columns"]),
                "date_min": row["date_min"],
                "date_max": row["date_max"],
                "analysis": json.loads(row["analysis_json"]),
                "insights": json.loads(row["insights_json"]),
                "recommendations": json.loads(row["recommendations_json"]),
                "raw_path": row["raw_path"],
                "cleaned_path": row["cleaned_path"],
            }
        )
    return {"history": history}


@app.post("/ai/summary")
def ai_summary(payload: dict = Body(...)):
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail={"error": "OPENAI_API_KEY not set"})

    instructions = (
        "You are a business analyst. Create a concise AI summary for MSME owners. "
        "Provide a 2-sentence narrative followed by 3-5 bullet points. "
        "Use plain language, avoid jargon, and keep the total under 120 words."
    )

    user_content = (
        "Use the following data to summarize performance and actions:\n"
        f"Analysis: {json.dumps(payload.get('analysis', {}))}\n"
        f"Insights: {payload.get('insights', [])}\n"
        f"Recommendations: {payload.get('recommendations', [])}\n"
    )

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": OPENAI_MODEL,
        "instructions": instructions,
        "input": [
            {
                "role": "user",
                "content": user_content,
            }
        ],
        "max_output_tokens": 300,
    }

    logger.info("AI summary request: model=%s", OPENAI_MODEL)
    resp = requests.post(OPENAI_RESPONSES_URL, headers=headers, json=body, timeout=30)
    if resp.status_code != 200:
        logger.error("OpenAI error: status=%s body=%s", resp.status_code, resp.text)
        try:
            err = resp.json().get("error", {})
        except Exception:
            err = {"message": resp.text}
        raise HTTPException(
            status_code=resp.status_code,
            detail={
                "error": "OpenAI API error",
                "openai_status": resp.status_code,
                "openai_type": err.get("type"),
                "openai_code": err.get("code"),
                "openai_message": err.get("message", "Unknown error"),
            },
        )

    data = resp.json()
    summary = extract_response_text(data)
    if not summary:
        raise HTTPException(status_code=502, detail={"error": "AI summary empty"})

    return {"summary": summary, "model": OPENAI_MODEL}
