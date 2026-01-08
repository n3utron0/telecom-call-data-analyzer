# core/utils_gemini.py
# ------------------------------------------------------------
# Handles Gemini interactions for TCDA_Better
# - Sends audio to Gemini for transcription + structured data extraction
# - Reuses GEMINI_PROMPT from models
# - Includes retry with exponential backoff
# - Non-blocking and concurrency-safe
# ------------------------------------------------------------

import re
import json
import asyncio
from core.config import log, genai_client, GEMINI_MODEL, MAX_RETRIES, BACKOFF_BASE_DELAY
from models import GEMINI_PROMPT

# ------------------------------------------------------------
# Helper: Exponential backoff
# ------------------------------------------------------------
async def retry_with_backoff(func, *args, **kwargs):
    """Reusable retry helper for Gemini-related network errors."""
    delay = BACKOFF_BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            # Retry only on transient errors or rate limits
            if "RESOURCE_EXHAUSTED" in str(e) or "429" in str(e):
                if attempt <= MAX_RETRIES:
                    log.warning(f"⏳ Gemini rate-limited. Retry {attempt}/{MAX_RETRIES} in {delay}s...")
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
            log.error(f"❌ Gemini request failed permanently: {e}")
            raise

# ------------------------------------------------------------
# Main: Analyze call with Gemini
# ------------------------------------------------------------
async def analyze_call(gcs_uri: str) -> dict:
    """
    Sends the audio file (GCS URI) to Gemini for:
    1. Full transcription
    2. Extraction of phone_number, complaint_type, sentiment, and resolution status
    Returns a parsed dictionary.
    """
    mime_type = "audio/mpeg" if gcs_uri.lower().endswith(".mp3") else "audio/wav"

    async def _analyze():
        response = await asyncio.to_thread(
            genai_client.models.generate_content,
            model=GEMINI_MODEL,
            contents=[
                {"file_data": {"file_uri": gcs_uri, "mime_type": mime_type}},
                {"text": GEMINI_PROMPT},
            ],
        )

        text = response.text.strip()

        # Try to extract JSON from Gemini’s output
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            log.warning(f"⚠️ Gemini output not in JSON format: {text}")
            return {"error": "Invalid Gemini response"}

        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError as e:
            log.error(f"❌ Failed to parse Gemini JSON: {e}")
            return {"error": "JSON parse error"}

        log.info(f"✅ Gemini extracted data successfully for {gcs_uri}")
        return data

    return await retry_with_backoff(_analyze)
