# Excel → Supabase V2.0 (Node.js + TypeScript)

Production CLI for normalizing daily Excel reports and inserting into Supabase/Postgres with zero import errors.

## Requirements
- Node.js 20+
- Supabase URL + Service Role key in env vars

## Install
```
npm install
```

## Run (CLI)
```
npm run ingest -- --file="/path/file.xlsx" --template="booking_export|planning|booking_lowest|events" --hotel_id="H2258" --hotel_name="Folkestone Opéra"
```

## Environment variables
- `SUPABASE_URL`
- `SUPABASE_KEY`
- `BATCH_SIZE` (default: 500)
- `ERROR_RATE_THRESHOLD` (default: 0.05)

## Docker / Coolify
The Docker image uses `npm run ingest --` as its entrypoint so you can pass CLI args directly.
Example:
```
docker run --rm \
  -e SUPABASE_URL=... \
  -e SUPABASE_KEY=... \
  excel-supabase-v2 \
  --file=/data/report.xlsx --template=booking_lowest --hotel_id=H2258 --hotel_name="Folkestone Opéra"
```

## Migrations
Apply `supabase/migrations/001_init.sql` to create:
- hotels, hotel_competitor_set, import_runs
- planning_tarifs, disponibilites
- booking_apercu, booking_tarifs_market, booking_tarifs_competitors
- booking_vs_market, booking_vs_competitors
- booking_export, events_calendar
- views `vw_booking_tarifs_wide_json`, `vw_booking_vs_wide_json`

If you change schema in Supabase, restart PostgREST to refresh the schema cache.

## Parsing rules
- Dates normalized to `YYYY-MM-DD` (Excel serials supported).
- BookingExport splits datetimes into DATE + TIME columns.
- Numeric parsing supports FR/EN separators (`1 234,56`, `1,234.56`).
- Non-numeric text in numeric fields is coerced to 0 for Booking.com numeric tables, and raw values are preserved in `raw_price`, `raw_delta`, or `raw_row`.
- Annotation rows (e.g. LOS markers) are skipped.

## Import run audit
Each run writes into `import_runs` with status, error message, and a `meta` JSON containing inserted/skipped/coerced counts.

## Batch inserts
Data is inserted in batches using Supabase. Errors are logged into `import_runs.meta` and the run is marked failed only if error rate exceeds `ERROR_RATE_THRESHOLD`.
