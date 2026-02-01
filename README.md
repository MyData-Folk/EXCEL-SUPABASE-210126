# Excel → Supabase Normalization

This service normalizes Excel files and writes clean, typed rows into Supabase tables. The API expects a `hotel_code` (preferred) or `hotel_id` for every import and resolves `hotel_id` via `public.hotels(code)`.

## Report types → parsers → tables

| Report type (category) | Filename detection fallback | Parser | Output tables |
| --- | --- | --- | --- |
| `FOLKESTONE OPERA PLANNING` | `*FOLKESTONE*PLANNING*.xlsx` | `FolkestonePlanningProcessor` | `public.disponibilites`, `public.planning_tarifs` |
| `BOOKING.COM LOWEST LOS` | `*bookingdotcom_lowest*.xlsx` | `BookingComProcessor` | `public.booking_apercu`, `public.booking_tarifs`, `public.booking_infos_tarifs`, `public.booking_vs_hier`, `public.booking_infos_vs_hier`, `public.booking_vs_3j`, `public.booking_infos_vs_3j`, `public.booking_vs_7j`, `public.booking_infos_vs_7j` |
| `BOOKING EXPORT` | `BookingExport*.xlsx` | `BookingExportProcessor` | `public.booking_export` |
| `DATE SALONS ET EVENEMENTS` | `DATE SALONS*.xlsx` | `EventsCalendarProcessor` | `public.events_calendar` |

> Existing D-EDGE and OTA Insight processors remain available and are selected by their existing category strings.

## Normalization highlights

### File 1 — Planning (Folkestone Opera)
* Sheet `Planning` is unpivoted from dates into rows.
* `metric == "Left for sale"` (case/spacing-insensitive) is routed to `public.disponibilites`.
* Price metrics (`Price (EUR)`, `Tarif`, etc.) are routed to `public.planning_tarifs`.

### File 2 — Booking.com lowest (multi-sheets)
* Data starts at `B5` in every sheet (header on row 5).
* `date_mise_a_jour` is extracted from `Tarifs!G3` and added to every output table.
* Competitor columns are dynamic and preserved exactly as found in Excel.
* Numeric tables convert any text to `0`.
* “Infos” tables preserve raw text markers as `TEXT`.

### File 3 — BookingExport
* All columns are preserved.
* Datetimes are split into DATE + TIME columns.
* Money values accept comma or dot decimals and thousands separators.

### File 4 — Events calendar
* Dates are normalized to `YYYY-MM-DD`.
* Numeric columns accept comma/dot decimals; invalid values become NULL.

## API usage

```
POST /api/auto-process
{
  "filename": "H2258 - FOLKESTONE OPERA - Planning - 2026-01-16....xlsx",
  "category": "FOLKESTONE OPERA PLANNING",
  "hotel_code": "H2258"
}
```

```
POST /api/auto-process
{
  "filename": "folkestone-opéra_bookingdotcom_lowest_los1_2guests.xlsx",
  "category": "BOOKING.COM LOWEST LOS",
  "hotel_code": "H2258"
}
```

## Validation checklist (manual)
1. Upload a file to `/api/upload`.
2. Call `/api/auto-process` with `hotel_code` and `category`.
3. Verify inserted rows in Supabase tables (counts and sample rows).

## Supabase DDL

See [`supabase_tables.sql`](supabase_tables.sql) for the required table schemas and column types, including placeholders for dynamic competitor columns.
