#core/config.py
"""
TCDA_Better - core/config.py
----------------------------

This module handles:
- Loading environment variables (.env)
- Setting up logging (console + optional file)
- Initializing Google Cloud clients (Storage, BigQuery, Gemini)
- Exposing runtime configuration values (e.g. concurrency, retries)
"""

import os
import logging
from dotenv import load_dotenv
from google.cloud import storage, bigquery
from google import genai

# ------------------------------------------------------------
# 1️⃣ Load environment variables
# ------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

# --- GCP Config ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION", "asia-south1")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# --- BigQuery ---
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

# --- Gemini / AI ---
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# --- Performance Controls ---
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", 9))
BATCH_DELAY_SECONDS = int(os.getenv("BATCH_DELAY_SECONDS", 10))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 2))
BACKOFF_BASE_DELAY = int(os.getenv("BACKOFF_BASE_DELAY", 2))

# --- Logging Controls ---
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "True").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ------------------------------------------------------------
# 2️⃣ Configure logging
# ------------------------------------------------------------
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOG_DIR, "app.log")

# 🧹 Clear previous log file on every run
if os.path.exists(LOG_FILE_PATH):
    # Option 1: truncate (delete contents)
    with open(LOG_FILE_PATH, "w", encoding="utf-8") as f:
        f.truncate(0)

    # Option 2 (optional): keep old log with timestamp
    # import time
    # timestamp = time.strftime("%Y%m%d_%H%M%S")
    # os.rename(LOG_FILE_PATH, os.path.join(LOG_DIR, f"app_{timestamp}.log"))

handlers = [logging.StreamHandler()]
if LOG_TO_FILE:
    handlers.append(logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=handlers,
)

log = logging.getLogger("TCDA_Better")
log.info("🚀 Logging initialized for TCDA_Better")

# ------------------------------------------------------------
# 3️⃣ Validate environment setup
# ------------------------------------------------------------
if not GCP_PROJECT_ID:
    log.error("❌ Missing GCP_PROJECT_ID in environment.")
if not GCS_BUCKET_NAME:
    log.error("❌ Missing GCS_BUCKET_NAME in environment.")
if not BQ_DATASET or not BQ_TABLE:
    log.error("❌ BigQuery dataset or table not set in environment.")

# Check credentials (no need if already in system env)
if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
    creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    if not os.path.exists(creds_path):
        log.warning(f"⚠️ GOOGLE_APPLICATION_CREDENTIALS path not found: {creds_path}")
    else:
        log.info(f"✅ Credentials detected at: {creds_path}")
else:
    log.info("ℹ️ Using system-level credentials for GCP access.")

# ------------------------------------------------------------
# 4️⃣ Initialize GCP clients
# ------------------------------------------------------------
try:
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    genai_client = genai.Client(project=GCP_PROJECT_ID, location=GCP_REGION, vertexai=True)
    log.info(f"✅ GCP clients initialized for project '{GCP_PROJECT_ID}' in region '{GCP_REGION}'.")
except Exception as e:
    log.error(f"❌ Failed to initialize GCP clients: {e}")
    raise

# ------------------------------------------------------------
# 5️⃣ Export runtime configuration
# ------------------------------------------------------------
__all__ = [
    "log",
    "storage_client",
    "bigquery_client",
    "genai_client",
    "GCP_PROJECT_ID",
    "GCP_REGION",
    "GCS_BUCKET_NAME",
    "BQ_DATASET",
    "BQ_TABLE",
    "GEMINI_MODEL",
    "MAX_CONCURRENT_TASKS",
    "BATCH_DELAY_SECONDS",
    "MAX_RETRIES",
    "BACKOFF_BASE_DELAY",
]
