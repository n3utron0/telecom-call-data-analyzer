# core/utils_gcs.py
# ------------------------------------------------------------
# Handles all Google Cloud Storage operations for TCDA_Better.
# - Async uploads & deletions
# - Retry with exponential backoff
# - Non-blocking (uses asyncio.to_thread for I/O)
# ------------------------------------------------------------

import os
import time
import asyncio
from google.api_core.exceptions import GoogleAPIError
from core.config import log, storage_client, GCS_BUCKET_NAME, MAX_RETRIES, BACKOFF_BASE_DELAY

# ------------------------------------------------------------
# Helper: Exponential backoff decorator
# ------------------------------------------------------------
async def retry_with_backoff(func, *args, **kwargs):
    """
    Retries a given function call with exponential backoff.
    Used for transient GCS or network issues.
    """
    delay = BACKOFF_BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 2):  # +1 final attempt
        try:
            return await func(*args, **kwargs)
        except GoogleAPIError as e:
            if attempt <= MAX_RETRIES:
                log.warning(f"⚠️ Attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                log.error(f"❌ Max retries reached for {func.__name__}: {e}")
                raise
        except Exception as e:
            log.error(f"❌ Unexpected error in {func.__name__}: {e}")
            raise

# ------------------------------------------------------------
# Upload file to GCS (non-blocking)
# ------------------------------------------------------------
async def upload_to_gcs(file_path: str) -> str:
    """
    Uploads a local file to GCS asynchronously.
    Returns the GCS URI (gs://bucket/path).
    """
    async def _upload():
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob_name = f"calls/{os.path.basename(file_path)}"
        blob = bucket.blob(blob_name)

        # Use a background thread for blocking upload
        await asyncio.to_thread(blob.upload_from_filename, file_path, content_type="audio/wav")
        gcs_uri = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
        log.info(f"✅ Uploaded {file_path} → {gcs_uri}")
        return gcs_uri

    return await retry_with_backoff(_upload)

# ------------------------------------------------------------
# Delete file from GCS (non-blocking)
# ------------------------------------------------------------
async def delete_from_gcs(gcs_uri: str):
    """
    Deletes a GCS object given its URI.
    Runs in a background thread to avoid blocking.
    """
    async def _delete():
        try:
            bucket_name, blob_path = gcs_uri.replace("gs://", "").split("/", 1)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            await asyncio.to_thread(blob.delete)
            log.info(f"🗑️ Deleted {gcs_uri} from GCS.")
        except Exception as e:
            log.warning(f"⚠️ Could not delete {gcs_uri}: {e}")

    await retry_with_backoff(_delete)
