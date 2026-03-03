#!/usr/bin/env python3
"""sync.py — High Hopes Menu Sync

Fetches product data from the Dutchie GraphQL API and upserts it into Airtable.

Usage:
    python sync.py               # Default: dry-run (safe)
    python sync.py --dry-run     # Explicit dry-run: log what would be written, don't touch Airtable
    python sync.py --target copy # Write to copy table (parallel validation phase)
    python sync.py --target main # Write to main table (production)
"""

import argparse
import json
import logging
import os
import smtplib
import time
from datetime import datetime
from email.mime.text import MIMEText
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DUTCHIE_ENDPOINT = "https://plus.dutchie.com/plus/2021-07/graphql"
AIRTABLE_API_BASE = "https://api.airtable.com/v0"

LAST_COUNT_FILE = Path("last_count.json")
LOG_FILE = Path("sync.log")

BATCH_SIZE = 10
MIN_COUNT_FLOOR = 100  # minimum variant count on first ever run

DUTCHIE_QUERY = """
fragment productFragment on Product {
  brand { name }
  id
  category
  subcategory
  image
  name
  tags
  effects
  staffPick
  description
  strainType
  potencyThc { formatted unit }
  variants { id priceRec quantity option }
}
query MenuQuery($retailerId: ID!, $offset: Int!, $limit: Int!) {
  menu(
    retailerId: $retailerId
    sort: { key: POPULAR, direction: DESC }
    pagination: { offset: $offset, limit: $limit }
  ) {
    products { ...productFragment }
  }
}
"""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    fmt = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger("sync")
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
# State management
# ---------------------------------------------------------------------------

def load_state():
    if LAST_COUNT_FILE.exists():
        with open(LAST_COUNT_FILE) as f:
            return json.load(f)
    return {
        "last_count": None,
        "first_run_complete": False,
        "consecutive_failures": 0,
        "last_errors": [],
    }


def save_state(state):
    with open(LAST_COUNT_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

def with_retry(fn, label, retries=3, backoff=(2, 4, 8)):
    """Call fn up to retries+1 times with exponential backoff on failure."""
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
# Dutchie API
# ---------------------------------------------------------------------------

def fetch_dutchie(retailer_id, bearer_token):
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
        "apollo-require-preflight": "true",
    }
    payload = {
        "query": DUTCHIE_QUERY,
        "variables": {"retailerId": retailer_id, "limit": 900, "offset": 0},
    }

    def do_request():
        resp = requests.post(DUTCHIE_ENDPOINT, json=payload, headers=headers, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Dutchie returned HTTP {resp.status_code}: {resp.text[:200]}")
        return resp

    return with_retry(do_request, "Dutchie API")


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def parse_potency_range(potency_thc):
    """Return THC as a float, or None if unit is 'mg' or value is unparseable."""
    if not potency_thc:
        return None
    unit = (potency_thc.get("unit") or "").lower()
    if unit == "mg":
        return None
    try:
        return float(potency_thc.get("formatted", "").replace("%", "").strip())
    except (ValueError, AttributeError):
        return None


def flatten_products(products):
    """Expand products into a flat list of (product, variant) tuples."""
    rows = []
    for product in products:
        for variant in product.get("variants") or []:
            rows.append((product, variant))
    return rows


def build_fields(product, variant):
    """Build Airtable field dict for one variant row."""
    effects = [e.strip() for e in (product.get("effects") or [])]
    tags = [t.strip() for t in (product.get("tags") or [])]

    potency_thc = product.get("potencyThc")
    brand = product.get("brand") or {}

    return {
        "ID": product.get("id"),
        "Brand": brand.get("name"),
        "Category": product.get("category"),
        "Subcategory": product.get("subcategory"),
        "Name": product.get("name"),
        "Strain": product.get("strainType"),
        "Potency": potency_thc.get("formatted") if potency_thc else None,
        "Potency Unit": potency_thc.get("unit") if potency_thc else None,
        "Potency Range": parse_potency_range(potency_thc),
        "Price": variant.get("priceRec"),
        "Quantity": variant.get("quantity"),
        "Unit Weight": variant.get("option"),
        "Image URL": product.get("image"),
        "Description": product.get("description"),
        "Effects": effects,
        "Tags": tags,
        "Active": True,
    }


# ---------------------------------------------------------------------------
# Airtable
# ---------------------------------------------------------------------------

def fetch_airtable_records(base_id, table_id, pat):
    """Fetch all records from a table. Returns dict keyed by ID field value."""
    headers = {"Authorization": f"Bearer {pat}"}
    url = f"{AIRTABLE_API_BASE}/{base_id}/{table_id}"
    records = {}
    offset = None

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        # Capture loop variables by default arg to avoid closure-over-loop-var issues
        def do_fetch(u=url, p=params):
            resp = requests.get(u, headers=headers, params=p, timeout=30)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Airtable GET returned {resp.status_code}: {resp.text[:200]}"
                )
            return resp.json()

        data = with_retry(do_fetch, "Airtable fetch")

        for record in data.get("records") or []:
            fields = record.get("fields") or {}
            variant_id = fields.get("ID")
            if variant_id:
                records[variant_id] = {
                    "airtable_record_id": record["id"],
                }

        offset = data.get("offset")
        if not offset:
            break

    return records


def write_batch(method, url, payload, pat, label):
    headers = {
        "Authorization": f"Bearer {pat}",
        "Content-Type": "application/json",
    }

    def do_write():
        if method == "POST":
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
        else:
            resp = requests.patch(url, json=payload, headers=headers, timeout=30)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Airtable {method} returned {resp.status_code}: {resp.text[:200]}"
            )
        return resp.json()

    return with_retry(do_write, label)


def write_to_airtable(creates, updates, base_id, table_id, pat):
    """Write creates and updates to Airtable in batches of 10. Returns error count."""
    url = f"{AIRTABLE_API_BASE}/{base_id}/{table_id}"
    errors = 0

    for i in range(0, len(creates), BATCH_SIZE):
        batch = creates[i : i + BATCH_SIZE]
        payload = {"records": [{"fields": r} for r in batch]}
        batch_num = i // BATCH_SIZE + 1
        try:
            write_batch("POST", url, payload, pat, f"create batch {batch_num}")
        except Exception as e:
            log.error(f"Create batch {batch_num} failed: {e}")
            errors += 1

    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i : i + BATCH_SIZE]
        payload = {"records": batch}
        batch_num = i // BATCH_SIZE + 1
        try:
            write_batch("PATCH", url, payload, pat, f"update batch {batch_num}")
        except Exception as e:
            log.error(f"Update batch {batch_num} failed: {e}")
            errors += 1

    return errors


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------

def send_alert(last_errors, consecutive_failures):
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    alert_email = os.environ.get("ALERT_EMAIL")

    if not all([smtp_host, smtp_user, smtp_password, alert_email]):
        log.warning("Alert email not fully configured — skipping")
        return

    subject = f"[High Hopes Sync] ALERT: {consecutive_failures} consecutive failures"
    body = "\n".join(last_errors) if last_errors else "No error details available."

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = alert_email

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        log.info(f"Alert email sent to {alert_email}")
    except Exception as e:
        log.error(f"Failed to send alert email: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="High Hopes Menu Sync")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and transform, log what would be written, don't touch Airtable",
    )
    group.add_argument(
        "--target",
        choices=["copy", "main"],
        help="Airtable table to write to",
    )
    args = parser.parse_args()

    dry_run = args.dry_run or (args.target is None)
    target = args.target if args.target else "dry-run"

    log.info(f"Sync started (target: {target})")
    start_time = time.time()

    state = load_state()
    alert_threshold = int(os.environ.get("ALERT_CONSECUTIVE_FAILURES", 3))

    try:
        # ------------------------------------------------------------------
        # Step 1: Fetch from Dutchie
        # ------------------------------------------------------------------
        retailer_id = os.environ["DUTCHIE_RETAILER_ID"]
        bearer_token = os.environ["DUTCHIE_BEARER_TOKEN"]

        try:
            resp = fetch_dutchie(retailer_id, bearer_token)
        except Exception as e:
            raise RuntimeError(f"Dutchie API failed after retries: {e}") from e

        # Sanity Check 1 — HTTP 200 (already enforced inside fetch_dutchie)
        raw_text = resp.text
        data = resp.json()

        if "errors" in data:
            raise RuntimeError(f"Dutchie GraphQL errors: {data['errors']}")

        products = (data.get("data") or {}).get("menu", {}).get("products") or []
        rows = flatten_products(products)
        variant_count = len(rows)

        log.info(f"Fetched {variant_count} variants from Dutchie")

        # Sanity Check 2 — Minimum count
        last_count = state.get("last_count")
        if last_count is None:
            floor = MIN_COUNT_FLOOR
            log.info(
                f"Sanity check passed (first-run floor: {floor}, new count: {variant_count})"
            )
        else:
            floor = last_count * 0.8
            if variant_count < floor:
                raise RuntimeError(
                    f"Sanity check failed: {variant_count} variants < {floor:.0f} minimum "
                    f"(last count: {last_count})"
                )
            log.info(f"Sanity check passed (last count: {last_count}, new count: {variant_count})")

        if variant_count < MIN_COUNT_FLOOR:
            raise RuntimeError(
                f"Sanity check failed: {variant_count} variants below hard floor of {MIN_COUNT_FLOOR}"
            )

        # Sanity Check 3 — First run gate
        if not state.get("first_run_complete"):
            log.info("=" * 70)
            log.info("FIRST RUN: Logging raw Dutchie response for operator verification")
            log.info("=" * 70)
            log.info(f"Raw response (first 3000 chars):\n{raw_text[:3000]}")
            log.info("=" * 70)
            log.info("ACTION REQUIRED:")
            log.info("  1. Verify that each variant has a unique 'id' field")
            log.info("     Expected format: '685c4fb2686cf71c389469e1'")
            log.info("  2. Confirm variant IDs match the 'ID' field in Airtable")
            log.info("  3. Once confirmed, edit last_count.json and set:")
            log.info('       "first_run_complete": true')
            log.info("Halting without writing to Airtable.")
            log.info("=" * 70)
            # Save count so next run has a reference, but don't mark complete
            state["last_count"] = variant_count
            save_state(state)
            return

        # ------------------------------------------------------------------
        # Dry-run path: log samples and exit
        # ------------------------------------------------------------------
        if dry_run:
            log.info(f"DRY RUN — no Airtable writes will be made")
            log.info(f"Total variants to sync: {variant_count}")
            log.info("Sample records (first 3):")
            for i, (product, variant) in enumerate(rows[:3]):
                fields = build_fields(product, variant)
                log.info(f"  [{i + 1}] {json.dumps(fields, default=str)}")
            elapsed = time.time() - start_time
            log.info(f"DRY RUN completed in {elapsed:.1f}s")
            state["consecutive_failures"] = 0
            state["last_count"] = variant_count
            save_state(state)
            return

        # ------------------------------------------------------------------
        # Steps 5–10: Fetch Airtable, diff, write
        # ------------------------------------------------------------------
        pat = os.environ["AIRTABLE_PAT"]
        base_id = os.environ["AIRTABLE_BASE_ID"]
        table_id = (
            os.environ["AIRTABLE_COPY_TABLE_ID"]
            if target == "copy"
            else os.environ["AIRTABLE_MAIN_TABLE_ID"]
        )

        existing = fetch_airtable_records(base_id, table_id, pat)
        log.info(f"Fetched {len(existing)} existing records from Airtable")

        creates = []
        updates = []
        dutchie_ids = set()

        for product, variant in rows:
            variant_id = product.get("id")
            if not variant_id:
                log.warning(f"Skipping product with no id: {product.get('name')}")
                continue

            dutchie_ids.add(variant_id)

            if variant_id in existing:
                airtable_record_id = existing[variant_id]["airtable_record_id"]
                fields = build_fields(product, variant)
                updates.append({"id": airtable_record_id, "fields": fields})
                log.info(f"  UPDATE  {product.get('name')}")
            else:
                fields = build_fields(product, variant)
                creates.append(fields)
                log.info(f"  CREATE  {product.get('name')}")

        # Soft-delete Airtable records not present in Dutchie response
        soft_deletes = 0
        for variant_id, record_info in existing.items():
            if variant_id not in dutchie_ids:
                updates.append({
                    "id": record_info["airtable_record_id"],
                    "fields": {"Active": False},
                })
                log.info(f"  INACTIVE  {variant_id}")
                soft_deletes += 1

        # Write to Airtable
        errors = write_to_airtable(creates, updates, base_id, table_id, pat)

        regular_updates = len(updates) - soft_deletes
        elapsed = time.time() - start_time
        log.info(
            f"Created: {len(creates)}, Updated: {regular_updates}, "
            f"Soft-deleted: {soft_deletes}, Errors: {errors}"
        )
        log.info(f"Sync completed in {elapsed:.1f}s")

        if errors > 0:
            raise RuntimeError(f"{errors} batch write error(s) during sync")

        # Success — reset failure counter and persist new count
        state["consecutive_failures"] = 0
        state["last_errors"] = []
        state["last_count"] = variant_count
        save_state(state)

    except Exception as e:
        error_msg = str(e)
        log.error(f"ERROR: {error_msg}")

        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
        state.setdefault("last_errors", []).append(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {error_msg}"
        )
        # Keep only the last N errors for the alert body
        state["last_errors"] = state["last_errors"][-alert_threshold:]

        log.error(f"Consecutive failures: {state['consecutive_failures']} of {alert_threshold}")

        if state["consecutive_failures"] >= alert_threshold:
            log.info(
                f"ALERT: {state['consecutive_failures']} consecutive failures — sending alert email"
            )
            send_alert(state["last_errors"], state["consecutive_failures"])

        save_state(state)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
