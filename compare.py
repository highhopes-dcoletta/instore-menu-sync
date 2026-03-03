#!/usr/bin/env python3
"""compare.py — High Hopes Menu Sync: Parallel Validation Comparison

Compares the copy table against the main table field-by-field.
Run daily during the parallel validation phase.

Success criterion: zero field-level discrepancies across 3 consecutive days
before cutover to --target main is approved.

Usage:
    python compare.py
"""

import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Fields synced by sync.py — exclude manual/formula fields from comparison
# ---------------------------------------------------------------------------

SYNCED_FIELDS = [
    "Brand",
    "Category",
    "Subcategory",
    "Name",
    "Strain",
    "Potency",
    "Potency Unit",
    "Potency Range",
    "Price",
    "Quantity",
    "Unit Weight",
    "Image URL",
    "Description",
    "Effects",
    "Tags",
    "Active",
]

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
LOG_FILE = Path("compare.log")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging():
    fmt = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger("compare")
    logger.setLevel(logging.INFO)

    fh = RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


log = setup_logging()

# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------


def with_retry(fn, label, retries=3, backoff=(2, 4, 8)):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                wait = backoff[attempt]
                log.warning(f"Retry {attempt + 1}/{retries} for {label}: {exc} — waiting {wait}s")
                time.sleep(wait)
    raise last_exc


# ---------------------------------------------------------------------------
# Airtable fetch
# ---------------------------------------------------------------------------


def fetch_all_records(base_id, table_id, pat, label):
    """Fetch all records from a table. Returns dict keyed by the ID field value."""
    headers = {"Authorization": f"Bearer {pat}"}
    url = f"{AIRTABLE_API_BASE}/{base_id}/{table_id}"
    records = {}
    offset = None

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        def do_fetch(u=url, p=params):
            resp = requests.get(u, headers=headers, params=p, timeout=30)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Airtable GET {label} returned {resp.status_code}: {resp.text[:200]}"
                )
            return resp.json()

        data = with_retry(do_fetch, f"fetch {label}")

        for record in data.get("records") or []:
            fields = record.get("fields") or {}
            variant_id = fields.get("ID")
            if variant_id:
                records[variant_id] = fields

        offset = data.get("offset")
        if not offset:
            break

    return records


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def normalize(value):
    """Normalize a field value for consistent comparison."""
    if isinstance(value, list):
        # Sort so order differences don't cause false positives
        return sorted(str(v).strip() for v in value)
    if value is None:
        return None
    if isinstance(value, float):
        return round(value, 4)
    return value


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    log.info("Comparison started")

    pat = os.environ["AIRTABLE_PAT"]
    base_id = os.environ["AIRTABLE_BASE_ID"]
    main_table_id = os.environ["AIRTABLE_MAIN_TABLE_ID"]
    copy_table_id = os.environ["AIRTABLE_COPY_TABLE_ID"]

    log.info("Fetching main table records...")
    main_records = fetch_all_records(base_id, main_table_id, pat, "main")
    log.info(f"Fetched {len(main_records)} records from main table")

    log.info("Fetching copy table records...")
    copy_records = fetch_all_records(base_id, copy_table_id, pat, "copy")
    log.info(f"Fetched {len(copy_records)} records from copy table")

    main_ids = set(main_records.keys())
    copy_ids = set(copy_records.keys())

    only_in_main = main_ids - copy_ids
    only_in_copy = copy_ids - main_ids
    common_ids = main_ids & copy_ids

    if only_in_main:
        log.warning(
            f"IDs only in main table ({len(only_in_main)}): "
            f"{sorted(only_in_main)[:20]}"
        )
    if only_in_copy:
        log.warning(
            f"IDs only in copy table ({len(only_in_copy)}): "
            f"{sorted(only_in_copy)[:20]}"
        )

    discrepancies = 0
    for variant_id in sorted(common_ids):
        main_fields = main_records[variant_id]
        copy_fields = copy_records[variant_id]

        for field in SYNCED_FIELDS:
            main_val = normalize(main_fields.get(field))
            copy_val = normalize(copy_fields.get(field))

            if main_val != copy_val:
                log.warning(
                    f"DISCREPANCY [{variant_id}] {field!r}: "
                    f"main={json.dumps(main_val, default=str)} "
                    f"copy={json.dumps(copy_val, default=str)}"
                )
                discrepancies += 1

    total_compared = len(common_ids)
    log.info(
        f"Comparison complete: {total_compared} records compared, "
        f"{discrepancies} field-level discrepancy(ies), "
        f"{len(only_in_main)} only in main, {len(only_in_copy)} only in copy"
    )

    if discrepancies == 0 and not only_in_main and not only_in_copy:
        log.info("PASS: Tables are in sync")
    else:
        log.info("FAIL: Discrepancies found — review warnings above")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
