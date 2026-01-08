# core/utils_core.py
# ------------------------------------------------------------
# Core pipeline orchestrator for TCDA_Better
# ------------------------------------------------------------

import asyncio
import os
import random
import string
import time
from core.config import log, MAX_CONCURRENT_TASKS
from core.utils_gcs import upload_to_gcs, delete_from_gcs
from core.utils_gemini import analyze_call

# Global runtime metrics
METRICS = {
    "total_files": 0,
    "success_count": 0,
    "failed_count": 0,
    "avg_time_per_file": 0.0,
    "total_runtime_sec": 0.0,
    "last_batch_time_sec": 0.0,
}

# ------------------------------------------------------------
# Utility: Generate prefixed Customer ID
# ------------------------------------------------------------
def generate_customer_id() -> str:
    """Creates a customer ID like CUST-XXXXX (5 random alphanumeric chars)."""
    return "CUST-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=5))

# ------------------------------------------------------------
# Pipeline: Process a single audio file
# ------------------------------------------------------------
async def process_audio_file(file_path: str, semaphore: asyncio.Semaphore, is_batch: bool = False) -> dict:
    """
    Executes the complete async pipeline:
    Upload → Analyze (Gemini) → (optional insert) → Delete.
    Returns full extracted data (always includes transcript).
    """
    async with semaphore:
        start = time.perf_counter()
        gcs_uri = None
        file_name = os.path.basename(file_path)
        log.info(f"🎧 Starting processing: {file_name}")

        try:
            # Step 1: Upload
            gcs_uri = await upload_to_gcs(file_path)

            # Step 2: Analyze via Gemini
            data = await analyze_call(gcs_uri)
            if "error" in data:
                raise ValueError(f"Gemini returned invalid data for {file_name}")

            # Step 3: Add metadata
            data["customer_id"] = generate_customer_id()
            data["resolved"] = bool(data.get("resolved", False))
            if not data.get("phone_number"):
                data["phone_number"] = None

            duration = round(time.perf_counter() - start, 2)
            data["processing_time_sec"] = duration
            log.info(f"✅ Extracted data from {file_name} in {duration}s")

            # Update metrics (for single mode)
            if not is_batch:
                METRICS["total_files"] += 1
                METRICS["success_count"] += 1
                if METRICS["total_files"] > 0:
                    METRICS["avg_time_per_file"] = round(
                        (METRICS["avg_time_per_file"] * (METRICS["total_files"] - 1) + duration)
                        / METRICS["total_files"],
                        2,
                    )
                METRICS["total_runtime_sec"] += duration

            # Step 4: Skip insert (single mode handled by confirm)
            if is_batch:
                # We'll collect in batch manager
                pass
            else:
                log.info(f"🧾 Skipping BigQuery insert for {file_name} (single mode).")

            return {"status": "success", "file": file_name, **data}

        except Exception as e:
            duration = round(time.perf_counter() - start, 2)
            METRICS["failed_count"] += 1
            log.error(f"❌ Failed processing {file_name} after {duration}s: {e}")
            return {"status": "failed", "file": file_name, "error": str(e), "time_sec": duration}

        finally:
            if gcs_uri:
                await delete_from_gcs(gcs_uri)

# ------------------------------------------------------------
# Batch processor
# ------------------------------------------------------------
from core.utils_bq import insert_batch_into_bigquery

async def process_batch(files: list[str]):
    """
    Process multiple audio files concurrently.
    Only one bulk insert into BigQuery at the end.
    """
    if not files:
        log.warning("⚠️ No files to process.")
        return {"message": "No files provided."}

    log.info(f"🚀 Starting batch processing: {len(files)} files (max {MAX_CONCURRENT_TASKS} concurrent).")
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    batch_start = time.perf_counter()

    tasks = [process_audio_file(f, semaphore, is_batch=True) for f in files]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter valid data
    valid_records = [
        {
            "customer_id": r["customer_id"],
            "phone_number": r.get("phone_number") if r.get("phone_number") not in ("", "Not Found", None) else None,
            "transcript": r.get("transcript", ""),
            "complaint_type": r.get("complaint_type", "Others"),
            "customer_sentiment": r.get("customer_sentiment", "Neutral"),
            "resolved": bool(r.get("resolved", False)),
        }
        for r in results
        if isinstance(r, dict) and r.get("status") == "success"
    ]
    failed = [r for r in results if isinstance(r, dict) and r.get("status") == "failed"]

    # Bulk insert once
    inserted_count = 0
    if valid_records:
        inserted_count = await insert_batch_into_bigquery(valid_records)

    total_time = round(time.perf_counter() - batch_start, 2)
    log.info(
        f"🏁 Batch complete — Processed: {len(files)}, Inserted: {inserted_count}, "
        f"Failed: {len(failed)}, Time: {total_time}s"
    )

    # Update global metrics
    METRICS["total_files"] += len(files)
    METRICS["success_count"] += len(valid_records)
    METRICS["failed_count"] += len(failed)
    METRICS["last_batch_time_sec"] = total_time
    if METRICS["total_files"] > 0:
        METRICS["avg_time_per_file"] = round(
            (METRICS["avg_time_per_file"] * (METRICS["total_files"] - len(files)) + total_time)
            / METRICS["total_files"],
            2,
        )
    METRICS["total_runtime_sec"] += total_time

    return {
        "total_files": len(files),
        "inserted": inserted_count,
        "failed": len(failed),
        "total_time_sec": total_time,
        "results": results,
    }
