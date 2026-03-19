"""
database.py - SQLite database for courses and submissions
Stores course configurations and participant form submissions.
Uses /app/data/ directory for Railway persistent volume support.
"""

import sqlite3
import json
import logging
import os
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

# Use /app/data for Railway volume mount, fallback to ./data for local dev
DATA_DIR = os.environ.get("DATA_DIR", "data")
DB_PATH = os.path.join(DATA_DIR, "lbsnaa.db")


def get_conn():
    """Get a database connection with row_factory set."""
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS courses (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT NOT NULL,
                slug         TEXT NOT NULL UNIQUE,
                description  TEXT DEFAULT '',
                fields_config TEXT NOT NULL,
                doc_config   TEXT NOT NULL,
                is_active    INTEGER DEFAULT 1,
                created_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS submissions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id     INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
                submitted_at  TEXT NOT NULL,
                email         TEXT NOT NULL,
                form_data     TEXT NOT NULL,
                photo_valid   INTEGER,
                photo_result  TEXT,
                id_valid      INTEGER,
                id_result     TEXT,
                letter_valid  INTEGER,
                letter_result TEXT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_course_email
                ON submissions(course_id, email);

            CREATE INDEX IF NOT EXISTS idx_submissions_course
                ON submissions(course_id);
        """)
        conn.commit()
        logger.info("Database initialized: courses + submissions tables ready")
    finally:
        conn.close()


# ============================================================================
# DEFAULT CONFIGS
# ============================================================================

DEFAULT_FIELDS_CONFIG = {
    "default_fields": [
        {"key": "name", "label": "Full Name", "type": "text", "enabled": True, "required": True, "locked": True},
        {"key": "email", "label": "Email", "type": "email", "enabled": True, "required": True, "locked": True},
        {"key": "i_nomination", "label": "iNomination Number", "type": "text", "enabled": True, "required": True},
        {"key": "gender", "label": "Gender", "type": "select", "enabled": True, "required": True, "options": ["Male", "Female", "Other"]},
        {"key": "job_title", "label": "Job Title", "type": "text", "enabled": True, "required": False},
        {"key": "service", "label": "Service", "type": "text", "enabled": True, "required": True},
        {"key": "batch", "label": "Batch", "type": "text", "enabled": True, "required": True},
        {"key": "cadre", "label": "Cadre", "type": "text", "enabled": True, "required": True},
        {"key": "zone", "label": "Zone", "type": "text", "enabled": True, "required": False},
        {"key": "state", "label": "State", "type": "text", "enabled": True, "required": False},
        {"key": "department", "label": "Department", "type": "text", "enabled": True, "required": False},
        {"key": "mobile", "label": "Mobile", "type": "tel", "enabled": True, "required": True},
    ],
    "custom_fields": []
}

DEFAULT_DOC_CONFIG = {
    "PHOTO": {"enabled": True, "required": True, "label": "Passport Photo"},
    "ID": {"enabled": True, "required": True, "label": "Government ID"},
    "LETTER": {"enabled": True, "required": True, "label": "Nomination Letter"},
}


def get_default_fields_config():
    """Return a deep copy of the default fields config."""
    return json.loads(json.dumps(DEFAULT_FIELDS_CONFIG))


def get_default_doc_config():
    """Return a deep copy of the default doc config."""
    return json.loads(json.dumps(DEFAULT_DOC_CONFIG))


# ============================================================================
# COURSE CRUD
# ============================================================================

def create_course(name, slug, description, fields_config, doc_config):
    """Create a new course. Returns the course id."""
    conn = get_conn()
    try:
        cursor = conn.execute(
            """INSERT INTO courses (name, slug, description, fields_config, doc_config, is_active, created_at)
               VALUES (?, ?, ?, ?, ?, 1, ?)""",
            (name, slug, description, json.dumps(fields_config), json.dumps(doc_config), datetime.utcnow().isoformat())
        )
        conn.commit()
        course_id = cursor.lastrowid
        logger.info(f"Created course '{name}' (id={course_id}, slug={slug})")
        return course_id
    finally:
        conn.close()


def get_all_courses():
    """Return all courses with submission counts."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT c.*, COUNT(s.id) as submission_count
            FROM courses c
            LEFT JOIN submissions s ON s.course_id = c.id
            GROUP BY c.id
            ORDER BY c.created_at DESC
        """).fetchall()
        return [_parse_course_row(row) for row in rows]
    finally:
        conn.close()


def get_course_by_id(course_id):
    """Return a single course by id."""
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM courses WHERE id = ?", (course_id,)).fetchone()
        return _parse_course_row(row) if row else None
    finally:
        conn.close()


def get_course_by_slug(slug):
    """Return a single course by slug."""
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM courses WHERE slug = ?", (slug,)).fetchone()
        return _parse_course_row(row) if row else None
    finally:
        conn.close()


def update_course(course_id, name, slug, description, fields_config, doc_config):
    """Update an existing course."""
    conn = get_conn()
    try:
        conn.execute(
            """UPDATE courses SET name=?, slug=?, description=?, fields_config=?, doc_config=?
               WHERE id=?""",
            (name, slug, description, json.dumps(fields_config), json.dumps(doc_config), course_id)
        )
        conn.commit()
        logger.info(f"Updated course id={course_id}")
    finally:
        conn.close()


def toggle_course(course_id):
    """Toggle is_active between 0 and 1. Returns new state."""
    conn = get_conn()
    try:
        conn.execute("UPDATE courses SET is_active = 1 - is_active WHERE id = ?", (course_id,))
        conn.commit()
        row = conn.execute("SELECT is_active FROM courses WHERE id = ?", (course_id,)).fetchone()
        new_state = row["is_active"] if row else None
        logger.info(f"Toggled course id={course_id} -> is_active={new_state}")
        return new_state
    finally:
        conn.close()


def delete_course(course_id):
    """Delete a course and all its submissions (CASCADE)."""
    conn = get_conn()
    try:
        conn.execute("DELETE FROM courses WHERE id = ?", (course_id,))
        conn.commit()
        logger.info(f"Deleted course id={course_id} and its submissions")
    finally:
        conn.close()


def _parse_course_row(row):
    """Convert a course db row to a dict with parsed JSON."""
    if row is None:
        return None
    d = dict(row)
    d["fields_config"] = json.loads(d["fields_config"]) if d.get("fields_config") else get_default_fields_config()
    d["doc_config"] = json.loads(d["doc_config"]) if d.get("doc_config") else get_default_doc_config()
    return d


# ============================================================================
# SUBMISSION CRUD
# ============================================================================

def save_submission(course_id, email, form_data, doc_results):
    """
    Save a submission. Returns submission id.

    doc_results: dict with keys PHOTO, ID, LETTER each containing
                 {"valid": bool, "result": dict} or None
    """
    conn = get_conn()
    try:
        photo = doc_results.get("PHOTO") or {}
        id_doc = doc_results.get("ID") or {}
        letter = doc_results.get("LETTER") or {}

        cursor = conn.execute(
            """INSERT INTO submissions
               (course_id, submitted_at, email, form_data,
                photo_valid, photo_result, id_valid, id_result, letter_valid, letter_result)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                course_id,
                datetime.utcnow().isoformat(),
                email,
                json.dumps(form_data),
                1 if photo.get("valid") else (0 if photo else None),
                json.dumps(photo.get("result")) if photo.get("result") else None,
                1 if id_doc.get("valid") else (0 if id_doc else None),
                json.dumps(id_doc.get("result")) if id_doc.get("result") else None,
                1 if letter.get("valid") else (0 if letter else None),
                json.dumps(letter.get("result")) if letter.get("result") else None,
            )
        )
        conn.commit()
        sid = cursor.lastrowid
        logger.info(f"Saved submission id={sid} for course_id={course_id}, email={email}")
        return sid
    finally:
        conn.close()


def get_submissions_by_course(course_id):
    """Return all submissions for a course, newest first."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM submissions WHERE course_id = ? ORDER BY id DESC",
            (course_id,)
        ).fetchall()
        return [_parse_submission_row(row) for row in rows]
    finally:
        conn.close()


def get_submission_count(course_id):
    """Return submission count for a course."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM submissions WHERE course_id = ?",
            (course_id,)
        ).fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()


def delete_submission(submission_id):
    """Delete a single submission."""
    conn = get_conn()
    try:
        conn.execute("DELETE FROM submissions WHERE id = ?", (submission_id,))
        conn.commit()
        logger.info(f"Deleted submission id={submission_id}")
    finally:
        conn.close()


def _parse_submission_row(row):
    """Convert a submission db row to a dict with parsed JSON."""
    if row is None:
        return None
    d = dict(row)
    d["form_data"] = json.loads(d["form_data"]) if d.get("form_data") else {}
    for field in ("photo_result", "id_result", "letter_result"):
        try:
            d[field] = json.loads(d[field]) if d[field] else None
        except (json.JSONDecodeError, TypeError):
            d[field] = None
    return d
