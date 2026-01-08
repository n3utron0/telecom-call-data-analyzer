# core/utils_bq.py
# ------------------------------------------------------------
# Optimized BigQuery handling for TCDA_Better
# - Single insert (for confirm_upload)
# - Resilient bulk insert (batch mode)
# ------------------------------------------------------------

import asyncio
import json
import re
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError, BadRequest
from core.config import log, bigquery_client, BQ_DATASET, BQ_TABLE, MAX_RETRIES, BACKOFF_BASE_DELAY

# ------------------------------------------------------------
# Helper: Exponential backoff
# ------------------------------------------------------------
async def retry_with_backoff(func, *args, **kwargs):
    delay = BACKOFF_BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            return await func(*args, **kwargs)
        except GoogleAPIError as e:
            if attempt <= MAX_RETRIES:
                log.warning(f"⚠️ BigQuery error (attempt {attempt}/{MAX_RETRIES}): {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                delay *= 2
            else:
                log.error(f"❌ BigQuery operation failed permanently: {e}")
                raise
        except Exception as e:
            log.error(f"❌ Unexpected BigQuery error: {e}")
            raise

# ------------------------------------------------------------
# Single record insert
# ------------------------------------------------------------
async def insert_into_bigquery(record: dict):
    """Insert one record (used for single uploads)."""
    table_id = f"{bigquery_client.project}.{BQ_DATASET}.{BQ_TABLE}"

    async def _insert():
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        await asyncio.to_thread(
            bigquery_client.load_table_from_json,
            [record],
            table_id,
            job_config=job_config,
        )
        log.info(f"📦 Inserted record for {record.get('phone_number', 'unknown')} into BigQuery.")
        return True

    return await retry_with_backoff(_insert)

# ------------------------------------------------------------
# Bulk insert (hybrid: SQL first, fallback to load job)
# ------------------------------------------------------------
async def insert_batch_into_bigquery(records: list[dict]):
    """
    Attempts to insert all records at once via a single SQL INSERT statement.
    Falls back to BigQuery load job if SQL insert fails.
    """
    if not records:
        log.warning("⚠️ No records to insert into BigQuery (empty list).")
        return 0

    table_id = f"{bigquery_client.project}.{BQ_DATASET}.{BQ_TABLE}"
    columns = ["customer_id", "phone_number", "transcript", "complaint_type", "customer_sentiment", "resolved"]

    # Escape single quotes and newlines safely for SQL
    def escape_sql(value):
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        val = str(value)
        val = val.replace("'", "\\'").replace("\n", " ").replace("\r", " ")
        return f"'{val}'"

    async def _insert_sql():
        values_clause = ", ".join(
            "(" + ", ".join(escape_sql(r.get(col, "")) for col in columns) + ")" for r in records
        )
        sql = f"INSERT INTO `{table_id}` ({', '.join(columns)}) VALUES {values_clause}"
        log.info(f"🧠 Executing bulk SQL insert for {len(records)} records...")
        await asyncio.to_thread(bigquery_client.query, sql)
        log.info(f"✅ SQL bulk insert succeeded for {len(records)} records.")
        return len(records)

    async def _insert_fallback():
        """If SQL insert fails, fallback to BigQuery load job."""
        log.warning("⚠️ SQL insert failed. Falling back to BigQuery load job method...")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        await asyncio.to_thread(
            bigquery_client.load_table_from_json,
            records,
            table_id,
            job_config=job_config,
        )
        log.info(f"✅ Fallback load job inserted {len(records)} records into BigQuery.")
        return len(records)

    try:
        return await retry_with_backoff(_insert_sql)
    except (BadRequest, GoogleAPIError, Exception) as e:
        log.error(f"❌ Bulk SQL insert failed: {e}")
        return await retry_with_backoff(_insert_fallback)
