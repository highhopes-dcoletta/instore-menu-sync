---

# High Hopes Menu Sync — Technical Spec v2

## Background
High Hopes LLC is a cannabis dispensary in Massachusetts. They run an in-store menu web app at `menu.highhopesma.com` built on Softr, backed by an Airtable base. Product data flows from their Dutchie POS system into Airtable via a tool called DataFetcher. DataFetcher costs ~$1,200/year and has been erroring since Feb 20, 2026 due to Dutchie returning 500 errors. This project replaces DataFetcher with a custom Python sync script running on a VPS.

---

## Goal
Replace DataFetcher's "Get Products" task with a Python script that:
- Fetches product data from the Dutchie GraphQL API
- Transforms it appropriately
- Upserts it into Airtable
- Runs on a cron schedule on a Linux VPS (~$5/month, e.g. Hetzner or DigitalOcean)

The Softr front-end and Airtable base are unchanged. From Softr's perspective nothing changes.

---

## Security Note
Before using this spec, rotate the Airtable PAT — it was exposed in plaintext during the planning conversation. Generate a new one from the Airtable Developer Hub. The Dutchie bearer token is a long-lived public token (JWT expiry ~2075) and is lower risk, but treat it as sensitive regardless.

---

## Credentials & Config

All credentials stored in a `.env` file, never hardcoded. The `.env` file must be in `.gitignore`.

```
DUTCHIE_BEARER_TOKEN=<rotate before use>
DUTCHIE_RETAILER_ID=fdbaec7b-3a75-4f47-84d7-79de799c1b32
AIRTABLE_PAT=<rotate before use>
AIRTABLE_BASE_ID=apptyyJaa7BU1hN8R
AIRTABLE_MAIN_TABLE_ID=tblOdltdiIqtteHp9
AIRTABLE_COPY_TABLE_ID=tbl2PAAFMd8kBBYWy
SYNC_INTERVAL_MINUTES=5
ALERT_EMAIL=david.coletta@high-hopes.biz
ALERT_CONSECUTIVE_FAILURES=3
SMTP_HOST=<smtp host>
SMTP_PORT=587
SMTP_USER=<smtp username>
SMTP_PASSWORD=<smtp password>
```

---

## Dutchie API

**Endpoint:** `POST https://plus.dutchie.com/plus/2021-07/graphql`

**Auth:** `Authorization: Bearer {DUTCHIE_BEARER_TOKEN}`

**GraphQL query:**
```graphql
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
```

**Variables:** `{ "retailerId": DUTCHIE_RETAILER_ID, "limit": 900, "offset": 0 }`

**Note on variant ID:** `id` has been added inside `variants { }` — it was not present in the original DataFetcher query. On the very first run in dry-run mode, log the raw Dutchie response and confirm that each variant has its own unique `id` field. The expectation is that variant IDs look like `685c4fb2686cf71c389469e1` and match the `ID` field in Airtable. Do not write to Airtable until this is confirmed.

**Retry logic:** All Dutchie API calls should retry up to 3 times with exponential backoff (2s, 4s, 8s) before failing. Log each retry attempt.

---

## Airtable Schema Changes Required

Before deploying the script, add the following field to the **High Hopes Products** table (and the copy table):

| Field Name | Type | Purpose |
|---|---|---|
| `Active` | Checkbox | True for products currently in Dutchie catalog, False for products that have been removed |

Softr should be updated to filter on `Active = true` so removed products don't appear on the menu.

**Retry logic:** All Airtable API calls should retry up to 3 times with exponential backoff (2s, 4s, 8s) before failing. Log each retry attempt.

---

## Data Model

Each product in Dutchie has multiple variants (different sizes/prices). Each variant becomes one row in Airtable. So a product like "High Hopes | OGKB" with sizes 1/8oz, 1/4oz, 1/2oz, 1oz becomes 4 Airtable records.

---

## Field Mappings

| Airtable Field | Source | Transformation |
|---|---|---|
| ID | `variant.id` | Direct — **confirm on first run** |
| Brand | `product.brand.name` | Direct |
| Category | `product.category` | Direct |
| Subcategory | `product.subcategory` | Direct |
| Name | `product.name` | Direct |
| Strain | `product.strainType` | Direct |
| Potency | `product.potencyThc.formatted` | Direct (e.g. "26.22%") |
| Potency Unit | `product.potencyThc.unit` | Direct (e.g. "%") |
| Potency Range | `product.potencyThc.formatted` | Strip "%" and parse as float (e.g. 26.22). If unit is "mg" or value can't be parsed, set to null |
| Price | `variant.priceRec` | Direct (numeric) |
| Quantity | `variant.quantity` | Direct (integer) |
| Unit Weight | `variant.option` | Direct (e.g. "1/8oz", "1g") |
| Image URL | `product.image` | Direct |
| Description | `product.description` | Direct |
| Effects | `product.effects` | Normalize whitespace (strip leading/trailing spaces from each value) before writing |
| Tags | `product.tags` + existing Airtable tags | See Tags Merge Logic below |
| Active | Derived | True for all products present in Dutchie response; False for soft-deleted records |

**Fields never written by the script:**
- `Staff Pick` (checkbox, manually maintained)
- `Popularity` (auto-number, managed by Airtable)
- `Category Counts`, `Category Totals` (unused scratch fields)
- All formula fields (Airtable computes these automatically)

---

## Tags Merge Logic

The Tags field in Airtable is partially auto-populated (category-level tags like "Flower", "Preroll", "Vape") and partially manually maintained (e.g. "Sleep", "Weekly Special").

**Important:** Both Dutchie tags and existing Airtable tags may contain leading/trailing whitespace. Normalize all tags by stripping whitespace before any comparison or deduplication.

**Manual tags to preserve — never overwrite these:**
```python
MANUAL_TAGS = {
    "Sleep", "Pain", "Weekly Special", "January Special",
    "Staff Pick", "Best Seller", "High Potency", "Organic",
    "High CBD", "2:1", "Top Shelf", "Rare"
}
```

**Merge logic:**
```python
# Normalize all tags first
existing_normalized = [t.strip() for t in existing_tags]
incoming_normalized = [t.strip() for t in incoming_tags]

# Preserve manual tags from existing record
manual = [t for t in existing_normalized if t in MANUAL_TAGS]

# Combine with incoming auto-tags from Dutchie
final_tags = list(dict.fromkeys(manual + incoming_normalized))  # deduped, order preserved
```

For new records (not yet in Airtable), just use normalized incoming tags from Dutchie directly.

---

## Sync Logic

```
1. Fetch all products from Dutchie API (with retry)
2. Run sanity checks (see below) — abort if they fail
3. On first-ever run: log raw response and halt for manual verification
4. Flatten products into one record per variant
5. Fetch all existing records from Airtable (paginate until complete, with retry)
   - Build a dict keyed by ID field value -> { airtable_record_id, current_tags }
6. For each Dutchie variant:
   - If ID exists in Airtable -> add to update list (Active = True)
   - If ID not in Airtable -> add to create list (Active = True)
7. For each Airtable record whose ID is NOT in Dutchie response:
   - Add to update list with Active = False (soft delete)
   - Do NOT modify any other fields, especially Tags and Staff Pick
8. Write to Airtable in batches of 10 (API limit), with retry per batch
   - Creates: POST /records
   - Updates: PATCH /records
9. Persist the successful variant count to disk (for next run's sanity check)
10. Log results
11. If consecutive failure threshold reached, send alert email
```

**Idempotency:** The upsert-by-ID approach is inherently idempotent. Updates are safe to run multiple times. Creates check for ID existence before inserting. Cron overlap is safe.

---

## Sanity Checks

Before writing anything to Airtable:

**Check 1 — HTTP status**
Dutchie must return HTTP 200. Any other status: log error, abort, do not touch Airtable.

**Check 2 — Minimum count**
Load the last successful variant count from disk (`last_count.json`). If the new count is less than 80% of the last successful count, abort — this suggests a partial or corrupted response. On the very first run (no `last_count.json` exists), use a hardcoded floor of 100.

**Check 3 — First run gate**
If `first_run_complete` flag is not set in `last_count.json`, log the raw Dutchie response, print a message asking the operator to verify the variant ID structure, and halt without writing to Airtable. Once verified, the operator sets the flag manually to allow subsequent runs to proceed.

---

## Modes

```bash
# Fetch and transform, log what would be written, don't touch Airtable
python sync.py --dry-run

# Write to copy table (parallel validation phase)
python sync.py --target copy

# Write to main table (production)
python sync.py --target main
```

Default if no `--target` specified: `--dry-run` (safe by default).

---

## Logging

Logs to both stdout and `sync.log` (append mode):

```
[2026-03-01 09:00:01] Sync started (target: copy)
[2026-03-01 09:00:02] Fetched 731 variants from Dutchie
[2026-03-01 09:00:02] Sanity check passed (last count: 731, new count: 731)
[2026-03-01 09:00:03] Fetched 731 existing records from Airtable
[2026-03-01 09:00:05] Created: 0, Updated: 729, Soft-deleted: 2, Errors: 0
[2026-03-01 09:00:05] Sync completed in 4.2s
```

On error:
```
[2026-03-01 09:00:02] ERROR: Dutchie returned 500 - aborting, Airtable not touched
[2026-03-01 09:00:02] Consecutive failures: 2 of 3
```

On alert threshold:
```
[2026-03-01 09:15:02] ALERT: 3 consecutive failures - sending alert email
```

Rotate `sync.log` at 10MB to prevent unbounded growth.

---

## Alerting

When `ALERT_CONSECUTIVE_FAILURES` consecutive runs fail, send an email to `ALERT_EMAIL` via SMTP:

**Subject:** `[High Hopes Sync] ALERT: {N} consecutive failures`

**Body:** Include the last N error messages from the log.

Reset the consecutive failure counter on any successful run.

---

## Parallel Validation Phase

During the parallel run, a separate comparison script (`compare.py`) should be run daily to diff the main table against the copy table.

**Success criterion:** Zero field-level discrepancies across 3 consecutive days of comparison before cutover to main table is approved.

**What `compare.py` does:**
```
1. Fetch all records from main table, keyed by ID
2. Fetch all records from copy table, keyed by ID
3. For each ID present in both:
   - Compare every synced field (not manual fields)
   - Log any discrepancies
4. Report IDs present in one table but not the other
5. Print summary: N records compared, N discrepancies found
```

---

## Dependencies

Pin all versions in `requirements.txt`:

```
requests==2.31.0
python-dotenv==1.0.0
```

If using an external email service instead of raw SMTP, add its SDK here.

---

## Deployment

- **Platform:** Linux VPS (Hetzner CAX11 or DigitalOcean Basic Droplet, ~$4-6/month)
- **OS:** Ubuntu 24.04
- **Language:** Python 3.11+
- **Scheduling:** cron
- **Cron expression:** `*/5 9-21 * * *` (every 5 minutes, 9am–9:55pm ET, every day)
- **Working directory:** `/home/highhopes/sync/`
- **Log location:** `/home/highhopes/sync/sync.log`
- **State file:** `/home/highhopes/sync/last_count.json`

---

## Deployment Checklist

- [ ] Rotate Airtable PAT, update `.env`
- [ ] Add `Active` field to both Airtable tables
- [ ] Update Softr to filter on `Active = true`
- [ ] Provision VPS
- [ ] Clone repo, install dependencies
- [ ] Copy `.env` to server (never commit it)
- [ ] Run `python sync.py --dry-run` and verify raw Dutchie response
- [ ] Confirm variant ID structure, set `first_run_complete` flag
- [ ] Run `python sync.py --target copy` manually once and verify output
- [ ] Set up cron job pointing at copy table
- [ ] Run `compare.py` daily for 3 days, verify zero discrepancies
- [ ] Switch cron job to `--target main`
- [ ] Monitor for one full day
- [ ] Cancel DataFetcher subscription

---

## Future Considerations
- Once sync is validated and stable, evaluate replacing Softr with a custom front-end (~$700/year saving)
- At that point, consider querying Dutchie directly from the front-end and eliminating Airtable entirely (~$250/year additional saving)
- Total potential savings if all three tools replaced: ~$2,150/year

---

That's v2. Ready to hand to Claude Code.