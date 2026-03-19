# LBSNAA Form Builder

A Flask-based course registration platform with:

- Admin dashboard to create and manage course-specific forms
- Public registration forms (`/form/<slug>`) for participants
- AI-assisted document validation for `PHOTO`, `ID`, and `LETTER`
- Submission storage in SQLite with CSV export
- Optional celebrity face detection for uploaded photos

---

## Table of Contents

- Overview
- Features
- Architecture
- Project Structure
- How It Works
- Prerequisites
- Installation and Run (Local)
- Run with Docker
- Environment Variables
- API Endpoints
- Admin Workflows
- Database Schema
- AI Validation Pipeline
- Deployment Notes
- Troubleshooting
- Security Notes
- Roadmap Ideas

---

## Overview

This project helps teams run course/event registrations where each course can define:

- Which participant fields are shown and required
- Which documents are required
- Whether registrations are open/closed

Participants submit forms through a public URL, and each uploaded document is validated before final submission.

---

## Features

### Admin

- Password-protected admin login
- Create/edit/delete courses
- Toggle course active state
- Configure default and custom fields
- Configure required document types (`PHOTO`, `ID`, `LETTER`)
- View submissions and per-document validation status
- Export submissions as CSV

### Public Form

- Dynamic fields rendered from course configuration
- Drag-and-drop document upload
- Client-side validation (required fields, email, mobile)
- Real-time server validation for each uploaded document
- Final JSON submission to backend

### AI Validation

- Type classification using pre-trained ML model
- Outlier detection (invalid/suspicious files)
- PDF-to-image conversion + best-page selection
- OCR extraction for ID/LETTER documents
- OCR-assisted correction for misclassification edge cases
- Name matching against OCR text (for ID/LETTER)
- Optional LLM fallback for OCR extraction (OpenAI)
- Optional celebrity face detection for PHOTO uploads

---

## Architecture

1. Flask app (`app.py`) serves admin UI, public UI, and APIs.
2. SQLite (`database.py`) stores courses and submissions.
3. `ModelManager` loads and caches ML artifacts on startup.
4. `validation_engine.py` executes the document validation pipeline.
5. `celebrity_detection.py` (optional) checks uploaded faces against a reference dataset.
6. Frontend JS (`static/form.js`, `static/admin.js`) drives client behavior.

---

## Project Structure

```text
LBSNAAFormBuilder/
  app.py
  database.py
  validation_engine.py
  model_manager.py
  celebrity_detection.py
  requirements.txt
  Dockerfile
  docker-compose.yml
  data/
    lbsnaa.db
  models/
    document_classifier.pkl
    outlier_detector.pkl
    feature_names.pkl
    celebrity_embeddings.pkl
  celebrity_reference/
    <celebrity folders with images>
  static/
    admin.js
    form.js
  templates/
    admin/
    public/
    base.html
  temp_uploads/
```

---

## How It Works

### 1. Startup

- Initializes SQLite DB tables
- Loads ML models from `models/`
- Initializes optional celebrity detector and cached embeddings

### 2. Admin Configures a Course

- Sets course name, slug, description
- Enables/disables default fields
- Adds custom fields
- Enables/disables required document types

### 3. Participant Submits Form

- Opens `/form/<slug>`
- Fills fields
- Uploads documents
- Each document is validated via `POST /api/validate/<doc_type>`
- Final submission is posted to `POST /form/<slug>/submit`

### 4. Admin Reviews Data

- Views submissions table and per-document status
- Exports CSV
- Deletes invalid submissions if needed

---

## Prerequisites

### Python

- Python 3.10 recommended (matches Docker image)

### System Dependencies

Required for OCR/PDF/image processing:

- Tesseract OCR
- Poppler utilities (for `pdf2image`)
- OpenCV runtime libraries

Linux packages used in Docker:

- `tesseract-ocr`
- `poppler-utils`
- `libgl1`
- `libglib2.0-0`

Windows note:

- The code attempts common Tesseract install paths automatically.
- If not found, OCR for ID/LETTER will degrade.

---

## Installation and Run (Local)

1. Create and activate virtual environment:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Ensure model and reference directories exist:

- `models/` with required `.pkl` files
- `celebrity_reference/` (optional but recommended for celebrity detection)

4. Set environment variables (minimum):

```bash
# Windows PowerShell
$env:ADMIN_PASSWORD="change-me"
$env:SECRET_KEY="change-this-secret"
```

5. Start app:

```bash
python app.py
```

6. Open:

- Admin login: `http://localhost:5000/admin/login`
- Health check: `http://localhost:5000/health`

---

## Run with Docker

### Docker Compose (recommended)

```bash
docker compose up --build
```

Default mapping:

- App: `http://localhost:5000`
- Data persisted via `./data:/app/data`
- Models/reference mounted from host

### Standalone Docker

```bash
docker build -t lbsnaa-form-builder .
docker run -p 5000:5000 \
  -e ADMIN_PASSWORD=change-me \
  -e SECRET_KEY=change-this-secret \
  -e DATA_DIR=/app/data \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/models:/app/models:ro \
  -v $(pwd)/celebrity_reference:/app/celebrity_reference:ro \
  lbsnaa-form-builder
```

---

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `ADMIN_PASSWORD` | `admin` | Admin login password |
| `SECRET_KEY` | `dev-secret-change-me` | Flask session signing key |
| `DATA_DIR` | `data` | Folder where `lbsnaa.db` is stored |
| `PORT` | `5000` | Flask server port |
| `FLASK_ENV` | `development` | Enables debug mode if set to `development` |
| `OPENAI_API_KEY` | unset | Enables LLM OCR fallback in validation engine |

---

## API Endpoints

### Health

- `GET /health`
- Returns app status and model readiness.

### Document Validation

- `POST /api/validate/<doc_type>`
- `doc_type` in: `ID`, `PHOTO`, `LETTER`
- Multipart form fields:
  - `file` (required)
  - `name` (optional; improves OCR name match)

Accepts file extensions:

- `jpg`, `jpeg`, `png`, `pdf`, `webp`, `bmp`, `tiff`, `tif`, `gif`

Max size:

- 5 MB (`MAX_CONTENT_LENGTH`)

Response shape:

- `status`: `success` or `error`
- `validation`: includes `is_valid`, `result`, `confidence`, `message`, OCR details, name match, and celebrity warning

### Form Submission

- `POST /form/<slug>/submit`
- JSON body:
  - `form_data`: key-value participant data
  - `doc_results`: validation outcomes collected by frontend

Returns:

- `201` with `submission_id` on success
- `409` if same email already submitted for that course

---

## Admin Workflows

### Course Configuration

Default fields are pre-seeded, including:

- Name, Email, iNomination Number, Gender, Job Title, Service, Batch, Cadre, Zone, State, Department, Mobile

Special behavior:

- `name` and `email` are locked to enabled+required
- Custom fields support `text`, `email`, `tel`, `textarea`, `select`

### Submission Review

- Table view with configurable field columns
- Document status per submission (`Accepted`, `Rejected`, `N/A`)
- Expandable detail rows with confidence and name-match info
- CSV export with enabled fields and document statuses

---

## Database Schema

SQLite file: `DATA_DIR/lbsnaa.db`

### `courses`

- `id` (PK)
- `name`
- `slug` (UNIQUE)
- `description`
- `fields_config` (JSON)
- `doc_config` (JSON)
- `is_active` (0/1)
- `created_at` (ISO timestamp)

### `submissions`

- `id` (PK)
- `course_id` (FK -> `courses.id`, cascade delete)
- `submitted_at`
- `email`
- `form_data` (JSON)
- `photo_valid`, `photo_result` (JSON)
- `id_valid`, `id_result` (JSON)
- `letter_valid`, `letter_result` (JSON)

Indexes:

- Unique `(course_id, email)`
- Index on `course_id`

---

## AI Validation Pipeline

Implemented in `validation_engine.py`.

1. Accept image/PDF.
2. If PDF:
   - Convert all pages to PNG
   - Score pages by content + edge density
   - Select best page
3. Reject blank/mostly white documents.
4. Preprocess image (resize, denoise, contrast normalization).
5. Extract handcrafted features:
   - Aspect ratio
   - Content density
   - Edge density
6. Run outlier detector.
7. Run classifier and confidence check.
8. For ID/LETTER:
   - OCR text extraction (`pytesseract`)
   - Optional LLM fallback (OpenAI) if OCR confidence is low
   - Name matching against provided user name
   - ID pattern checks / letter keyword boosts in specific mismatch cases
9. For PHOTO:
   - Optional celebrity face comparison via InsightFace

---

## Deployment Notes

- The app expects model files at startup. Missing model files will fail initialization.
- For production:
  - Set strong `SECRET_KEY` and `ADMIN_PASSWORD`
  - Run behind a reverse proxy (Nginx/Caddy)
  - Restrict CORS policy (currently permissive)
  - Use HTTPS
- SQLite is fine for low/medium traffic. Consider PostgreSQL if scaling.

---

## Troubleshooting

### `503 Models not loaded`

- Verify required model files exist in `models/`:
  - `document_classifier.pkl`
  - `outlier_detector.pkl`
  - `feature_names.pkl` (optional but recommended)

### OCR returns empty/low confidence

- Confirm Tesseract is installed and accessible
- Improve image quality (sharpness, contrast)
- Set `OPENAI_API_KEY` to enable LLM fallback

### PDF validation fails

- Install Poppler utilities
- Verify `pdf2image` is installed

### Celebrity detection unavailable

- Install `insightface` + `onnxruntime`
- Ensure `celebrity_reference/` has enough images per person (minimum 3)

### Duplicate submission error

- Same email cannot submit twice for the same course due to unique DB index

---

## Security Notes

- Admin auth is single-password session-based auth (no users/roles)
- CSRF protection is not implemented
- CORS is enabled globally
- No rate limiting on validation API

For internet-facing deployments, add:

- CSRF protection
- Rate limiting
- Strong auth (multi-user)
- Audit logging and monitoring

---

## Roadmap Ideas

- Replace single-password admin login with user accounts + RBAC
- Add test suite (unit + integration + API)
- Add migration framework (Alembic or similar)
- Move from SQLite to PostgreSQL for multi-instance deployments
- Add async queue for heavy document validation workloads
- Add observability dashboards and metrics

