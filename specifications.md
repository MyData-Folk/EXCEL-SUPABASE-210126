# Technical Specifications - RMS Sync v2.0 (Supabase Auto-Importer)

## 1. Project Overview
### 1.1 Context
**RMS Sync v2.0** is an ETL (Extract, Transform, Load) automation tool designed for the hospitality industry. Its primary purpose is to streamline the importation of complex data from Excel files (D-EDGE reports, OTA Insight, etc.) into a **Supabase** (PostgreSQL) database.

### 1.2 Objective
To automate the cleaning, transformation, and synchronization of hotel data to power Revenue Management System (RMS) dashboards.

---

## 2. Technical Architecture
### 2.1 Technology Stack
- **Frontend**: HTML5, Vanilla JavaScript, CSS3 (Modern and responsive design).
- **Backend**: Python 3.10+ with Flask.
- **Data Manipulation**: Pandas (for complex Excel/CSV processing).
- **Database**: Supabase (PostgreSQL) with Row Level Security (RLS).
- **Containerization**: Docker & Docker Compose.
- **Web Server**: Nginx (Reverse Proxy).

### 2.2 Infrastructure & URLs
- **Admin**: `https://admin.hotelmanager.fr`
- **API**: `api.hotelmanager.fr`
- **Frontend**: `https://hotel.hotelmanager.fr`

---

## 3. Functional Specifications
### 3.1 File Management
- **Multi-format Upload**: Supports `.csv`, `.xlsx`, `.xls` files.
- **Robust Preview**: Preview the first 10 rows before importing.
- **Sheet Management**: Ability to select specific sheets within Excel files.

### 3.2 Transformation Engine (Processor)
The core logic resides in `processor.py`, which handles specialized report types:
- **D-EDGE Planning**: Transforms cross-tabulated data (unpivot) into flat list format.
- **D-EDGE Reservations**: Automatic date normalization and hotel ID injection.
- **OTA Insight**: Intelligent column mapping and header cleaning.
- **Salons & Events**: Normalization of event calendars.

### 3.3 Supabase Integration
- **"Create" Mode**: Automatically generates SQL tables based on the Excel file structure.
- **"Append" Mode**: Inserts data into existing tables with column mapping.
- **Automatic Injection**: Systematically adds a `hotel_id` column for data segmentation.

---

## 4. Technical Specifications
### 4.1 Security
- Uses `execute_sql` via a PostgreSQL RPC function with `SECURITY DEFINER` for schema modifications.
- RLS (Row Level Security) enabled on critical tables (`hotels`, `import_templates`).

### 4.2 Data Cleaning
- **Dates**: Intelligent conversion of Excel serial dates, ISO, and regional formats (DD/MM/YYYY) into SQL `YYYY-MM-DD`.
- **Numbers**: Cleans currency symbols, spaces, and thousand separators (FR/EN formats).
- **Text**: Normalizes column names to `snake_case` for SQL compatibility.

---

## 5. Maintenance and Scalability
### 5.1 Import Templates
The application allows users to save mapping "Templates" to reuse configurations for future imports (Stored in `import_templates` table).

### 5.2 Deployment
The project is production-ready with Docker:
- `Dockerfile` for the Flask backend.
- `docker-compose.yml` to orchestrate Flask and Nginx.
