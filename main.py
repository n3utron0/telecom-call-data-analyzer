# main.py
# ------------------------------------------------------------
# TCDA_Better: FastAPI entry point
# ------------------------------------------------------------

import os
import asyncio
import time
import uuid
from fastapi import FastAPI, UploadFile, File, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from dotenv import load_dotenv
from models import ExtractedCallData
from core.config import log, MAX_CONCURRENT_TASKS
from core.utils_core import process_audio_file, process_batch, METRICS, generate_customer_id
from core.utils_bq import insert_into_bigquery
from pydantic import BaseModel
from core.query_utils import run_chatbot_query  # adjust import if your structure differs
# ------------------------------------------------------------
# Load environment
# ------------------------------------------------------------
load_dotenv()
DEFAULT_AUDIO_PATH = os.getenv("LOCAL_AUDIO_PATH", "sample_audio")

# ------------------------------------------------------------
# App setup
# ------------------------------------------------------------
app = FastAPI(
    title="TCDA_Better - Telecom Call Data Analyzer",
    description="Optimized async backend for telecom call analysis using Gemini + BigQuery.",
    version="2.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For testing; restrict later for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------
# Serve frontend
# ------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve frontend HTML."""
    frontend_path = os.path.join(os.path.dirname(__file__), "frontend", "index.html")
    if os.path.exists(frontend_path):
        return FileResponse(frontend_path, media_type="text/html")
    raise HTTPException(status_code=404, detail="Frontend not found. Check /frontend directory.")


@app.get("/chat", response_class=HTMLResponse)
async def serve_chat():
    """Serve the chatbot frontend page."""
    frontend_path = os.path.join(os.path.dirname(__file__), "frontend", "chat.html")
    if os.path.exists(frontend_path):
        return FileResponse(frontend_path, media_type="text/html")
    raise HTTPException(status_code=404, detail="Chatbot page not found.")


# ------------------------------------------------------------
# Upload a single audio file (no DB insert until confirm)
# ------------------------------------------------------------
@app.post("/upload_audio")
async def upload_audio(file: UploadFile = File(...)):
    """
    Upload one audio file and extract data (Gemini).
    Returns full extracted details for user confirmation.
    """
    temp_filename = f"temp_{uuid.uuid4()}_{file.filename}"
    try:
        with open(temp_filename, "wb") as buffer:
            buffer.write(await file.read())

        log.info(f"📁 Received file: {file.filename}")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        result = await process_audio_file(temp_filename, semaphore, is_batch=False)

        if result.get("status") != "success":
            raise HTTPException(status_code=500, detail=result.get("error", "Gemini processing failed."))

        # Build frontend data
        data = {
            "customer_id": "TEMP",
            "phone_number": result.get("phone_number", "Not Found"),
            "complaint_type": result.get("complaint_type", "Others"),
            "customer_sentiment": result.get("customer_sentiment", "Neutral"),
            "resolved": result.get("resolved", False),
            "transcript": result.get("transcript", ""),
            "processing_time_sec": result.get("processing_time_sec", 0),
        }

        return {"status": "success", "data": data}

    except Exception as e:
        log.error(f"❌ Single upload failed for {file.filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

# ------------------------------------------------------------
# Batch upload (frontend-selectable folder)
# ------------------------------------------------------------
@app.post("/upload_batch")
async def upload_batch(payload: dict = Body(...)):
    """
    Process all audio files from a selected folder.
    Frontend sends: { "folder_path": "C:/User/Audio" }
    """
    folder_path = payload.get("folder_path") or DEFAULT_AUDIO_PATH

    if not folder_path or not os.path.exists(folder_path):
        raise HTTPException(status_code=400, detail=f"Invalid folder path: {folder_path}")

    files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith((".wav", ".mp3"))
    ]
    if not files:
        raise HTTPException(status_code=400, detail="No audio files found in selected folder.")

    start = time.perf_counter()
    log.info(f"🚀 Starting batch from folder: {folder_path} ({len(files)} files)")
    results = await process_batch(files)
    duration = round(time.perf_counter() - start, 2)

    log.info(f"🏁 Batch from {folder_path} complete in {duration}s")
    return {
        "message": f"Processed {len(files)} files from {folder_path} in {duration}s",
        "details": results,
    }

# ------------------------------------------------------------
# Confirm upload (insert single record after user approval)
# ------------------------------------------------------------
@app.post("/confirm_upload")
async def confirm_upload(record: dict):
    """
    Insert confirmed record into BigQuery.
    Ensures valid customer_id and keeps only required fields.
    """
    try:
        allowed_fields = [
            "customer_id", "phone_number", "transcript",
            "complaint_type", "customer_sentiment", "resolved"
        ]

        # Ensure proper customer_id
        if not record.get("customer_id") or record["customer_id"] == "TEMP":
            record["customer_id"] = generate_customer_id()

        # Ensure resolved is boolean
        record["resolved"] = bool(record.get("resolved", False))

        # Sanitize to allowed columns
        clean_record = {k: record[k] for k in allowed_fields if k in record}

        await insert_into_bigquery(clean_record)

        log.info(f"✅ Confirmed and inserted record for {clean_record.get('phone_number', 'unknown')}")
        return {"status": "success", "message": "Record inserted into BigQuery."}

    except Exception as e:
        log.error(f"❌ Confirm upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------------------------------------
# Metrics dashboard
# ------------------------------------------------------------
@app.get("/metrics")
async def get_metrics():
    """Return current runtime metrics."""
    uptime_hours = round(METRICS["total_runtime_sec"] / 3600, 2)
    return {
        "summary": {
            "total_files_processed": METRICS["total_files"],
            "success_count": METRICS["success_count"],
            "failed_count": METRICS["failed_count"],
            "avg_time_per_file_sec": METRICS["avg_time_per_file"],
            "last_batch_time_sec": METRICS["last_batch_time_sec"],
            "total_runtime_sec": METRICS["total_runtime_sec"],
            "uptime_hours": uptime_hours,
        }
    }

@app.post("/reset_metrics")
async def reset_metrics():
    """Reset all runtime metrics to zero."""
    for key in METRICS:
        METRICS[key] = 0 if isinstance(METRICS[key], (int, float)) else []
    log.info("🧮 Metrics have been reset to zero.")
    return {"status": "success", "message": "Metrics reset successfully."}

# ------------------------------------------------------------
# Chatbot Query Endpoint (Gemini + BigQuery)
# ------------------------------------------------------------

class QueryRequest(BaseModel):
    query: str

@app.post("/chatbot_query")
async def chatbot_query(request: QueryRequest):
    """
    Handles chatbot-style queries.
    Simple greetings are answered locally without hitting Gemini.
    """
    user_input = request.query.strip().lower()

    # --- Local responses for greetings ---
    friendly_replies = {
        "hi": "Hey there! 👋 How can I help you today?",
        "hello": "Hello! Hope you're having a good day ☀️",
        "thanks": "You're most welcome! 😊",
        "thank you": "You're most welcome! 😊",
        "bye": "Goodbye! 👋 Come back anytime.",
    }

    for key, reply in friendly_replies.items():
        if key in user_input:
            return {"answer": reply}

    # --- Otherwise, forward to Gemini for analysis ---
    try:
        log.info(f"🗣 Received chatbot query: {request.query}")
        response = await run_chatbot_query(request.query)
        return {"answer": response}
    except Exception as e:
        log.error(f"❌ Chatbot query failed: {e}")
        return {"answer": "⚠️ Something went wrong while processing your query."}

