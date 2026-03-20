"""
app.py - LBSNAA Course Form Builder
Flask application with admin panel, public forms, and document validation API.
"""

import os
import io
import csv
import re
import json
import logging
from datetime import datetime
from pathlib import Path
from functools import wraps

from flask import (
    Flask, render_template, request, jsonify, redirect,
    url_for, session, flash, Response, abort, send_file
)
from flask_cors import CORS
from werkzeug.utils import secure_filename

from model_manager import ModelManager
from validation_engine import validate_document
from storage import get_storage
from database import (
    init_db, get_default_fields_config, get_default_doc_config,
    create_course, get_all_courses, get_course_by_id, get_course_by_slug,
    update_course, toggle_course, delete_course,
    save_submission, get_submissions_by_course, get_submission_count,
    delete_submission, update_submission_files, get_submission_by_id
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)
CORS(app)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024  # 5MB
app.config['UPLOAD_FOLDER'] = Path('temp_uploads')
app.config['UPLOAD_FOLDER'].mkdir(exist_ok=True)

ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin')

ALLOWED_EXTENSIONS = {'jpg', 'jpeg', 'png', 'pdf', 'webp', 'bmp', 'tiff', 'tif', 'gif'}
VALID_DOC_TYPES = {'ID', 'PHOTO', 'LETTER'}


# ============================================================================
# HELPERS
# ============================================================================

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def save_temp_file(file):
    try:
        filename = secure_filename(file.filename)
        if not filename:
            return None
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
        filename = timestamp + filename
        filepath = app.config['UPLOAD_FOLDER'] / filename
        file.save(str(filepath))
        return str(filepath)
    except Exception as e:
        logger.error(f"Error saving temp file: {e}")
        return None


def cleanup_file(filepath):
    try:
        if filepath and Path(filepath).exists():
            Path(filepath).unlink()
    except Exception as e:
        logger.error(f"Error cleaning up temp file: {e}")


def slugify(text):
    """Convert text to URL-friendly slug."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')


def require_models(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not ModelManager.is_ready():
            return jsonify({'status': 'error', 'message': 'Models not loaded.'}), 503
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    """Decorator to protect admin routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated


# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.route('/health')
def health():
    return jsonify({
        'status': 'healthy',
        'models_loaded': ModelManager.is_ready(),
        'timestamp': datetime.now().isoformat()
    })


# ============================================================================
# DOCUMENT VALIDATION API (unchanged from original)
# ============================================================================

@app.route('/api/validate/<doc_type>', methods=['POST'])
@require_models
def validate(doc_type):
    if doc_type.upper() not in VALID_DOC_TYPES:
        return jsonify({'status': 'error', 'message': f'Invalid document type.'}), 400
    doc_type = doc_type.upper()

    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'No file selected'}), 400
    if not allowed_file(file.filename):
        return jsonify({'status': 'error', 'message': 'File type not allowed'}), 400

    user_name = request.form.get('name', '').strip()
    temp_filepath = save_temp_file(file)
    if not temp_filepath:
        return jsonify({'status': 'error', 'message': 'Failed to save file'}), 500

    try:
        ml_model = ModelManager.get_ml_model()
        outlier_model = ModelManager.get_outlier_model()
        result = validate_document(
            image_path=temp_filepath,
            expected_type=doc_type,
            ml_model=ml_model,
            outlier_model=outlier_model,
            user_name=user_name
        )

        # Save file to pending storage if upload_session_id is provided
        upload_session_id = request.form.get('upload_session_id', '').strip()
        if upload_session_id:
            try:
                storage = get_storage()
                storage.save_pending(upload_session_id, doc_type, temp_filepath)
            except Exception as e:
                logger.error(f"Failed to save pending file: {e}")

        return jsonify({
            'status': 'success',
            'validation': {
                'is_valid': result['is_valid'],
                'expected_type': result['expected_type'],
                'actual_type': result['actual_type'],
                'confidence': result['confidence'],
                'result': result['result'],
                'message': result['message'],
                'outlier_score': result['outlier_score'],
                'ocr_text': result.get('ocr_text', ''),
                'name_match': result.get('name_match', {}),
                'ocr_status': result.get('ocr_status', 'UNAVAILABLE'),
                'ocr_confidence': result.get('ocr_confidence', 0.0),
                'extraction_method': result.get('extraction_method', 'NONE'),
                'keywords_found': result.get('keywords_found', []),
                'celebrity_warning': result.get('celebrity_warning', None)
            }
        }), 200
    except Exception as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'Validation failed'}), 500
    finally:
        cleanup_file(temp_filepath)


# ============================================================================
# ADMIN AUTH
# ============================================================================

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        password = request.form.get('password', '')
        if password == ADMIN_PASSWORD:
            session['admin_logged_in'] = True
            return redirect(url_for('admin_dashboard'))
        flash('Incorrect password.', 'error')
    return render_template('admin/login.html')


@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('admin_login'))


# ============================================================================
# ADMIN DASHBOARD
# ============================================================================

@app.route('/admin')
@require_admin
def admin_dashboard():
    courses = get_all_courses()
    return render_template('admin/dashboard.html', courses=courses)


# ============================================================================
# COURSE MANAGEMENT
# ============================================================================

@app.route('/admin/course/new', methods=['GET', 'POST'])
@require_admin
def admin_course_new():
    if request.method == 'POST':
        return _save_course(is_new=True)
    return render_template('admin/course_form.html',
                           course=None,
                           fields_config=get_default_fields_config(),
                           doc_config=get_default_doc_config())


@app.route('/admin/course/<int:course_id>/edit', methods=['GET', 'POST'])
@require_admin
def admin_course_edit(course_id):
    course = get_course_by_id(course_id)
    if not course:
        abort(404)
    if request.method == 'POST':
        return _save_course(is_new=False, course_id=course_id)
    return render_template('admin/course_form.html',
                           course=course,
                           fields_config=course['fields_config'],
                           doc_config=course['doc_config'])


def _save_course(is_new, course_id=None):
    """Process the course create/edit form submission."""
    name = request.form.get('name', '').strip()
    slug = request.form.get('slug', '').strip() or slugify(name)
    description = request.form.get('description', '').strip()

    if not name or not slug:
        flash('Course name is required.', 'error')
        return redirect(request.url)

    # Build fields_config from form
    fields_config = {"default_fields": [], "custom_fields": []}
    default_keys = request.form.getlist('default_field_keys')

    for key in default_keys:
        label = request.form.get(f'field_label_{key}', key)
        ftype = request.form.get(f'field_type_{key}', 'text')
        enabled = request.form.get(f'field_enabled_{key}') == '1'
        required = request.form.get(f'field_required_{key}') == '1'
        locked = key in ('name', 'email')
        options_str = request.form.get(f'field_options_{key}', '')
        options = [o.strip() for o in options_str.split(',') if o.strip()] if ftype == 'select' else []

        if locked:
            enabled = True
            required = True

        field = {"key": key, "label": label, "type": ftype, "enabled": enabled, "required": required}
        if locked:
            field["locked"] = True
        if options:
            field["options"] = options
        fields_config["default_fields"].append(field)

    # Custom fields — iterate by index
    custom_labels = request.form.getlist('custom_field_label')
    custom_types = request.form.getlist('custom_field_type')
    custom_options = request.form.getlist('custom_field_options')

    for i, label in enumerate(custom_labels):
        label = label.strip()
        if not label:
            continue
        ftype = custom_types[i] if i < len(custom_types) else 'text'
        # Checkbox: check for indexed required flag
        req = request.form.get(f'custom_field_required_{i}') == '1'
        opts_str = custom_options[i] if i < len(custom_options) else ''
        options = [o.strip() for o in opts_str.split(',') if o.strip()] if ftype == 'select' else []

        key = slugify(label).replace('-', '_')
        field = {"key": key, "label": label, "type": ftype, "required": req}
        if options:
            field["options"] = options
        fields_config["custom_fields"].append(field)

    # Build doc_config
    doc_config = {}
    for doc_type in ['PHOTO', 'ID', 'LETTER']:
        doc_enabled = request.form.get(f'doc_{doc_type}_enabled') == '1'
        doc_required = request.form.get(f'doc_{doc_type}_required') == '1'
        doc_label = request.form.get(f'doc_{doc_type}_label', doc_type)
        doc_config[doc_type] = {"enabled": doc_enabled, "required": doc_required, "label": doc_label}

    try:
        if is_new:
            create_course(name, slug, description, fields_config, doc_config)
            flash(f'Course "{name}" created.', 'success')
        else:
            update_course(course_id, name, slug, description, fields_config, doc_config)
            flash(f'Course "{name}" updated.', 'success')
        return redirect(url_for('admin_dashboard'))
    except Exception as e:
        logger.error(f"Error saving course: {e}", exc_info=True)
        flash(f'Error: {e}', 'error')
        return redirect(request.url)


@app.route('/admin/course/<int:course_id>/toggle', methods=['POST'])
@require_admin
def admin_course_toggle(course_id):
    new_state = toggle_course(course_id)
    status = "activated" if new_state else "deactivated"
    flash(f'Course {status}.', 'success')
    return redirect(url_for('admin_dashboard'))


@app.route('/admin/course/<int:course_id>/delete', methods=['POST'])
@require_admin
def admin_course_delete(course_id):
    course = get_course_by_id(course_id)
    if course:
        delete_course(course_id)
        flash(f'Course "{course["name"]}" deleted.', 'success')
    return redirect(url_for('admin_dashboard'))


# ============================================================================
# ADMIN SUBMISSIONS
# ============================================================================

@app.route('/admin/course/<int:course_id>/submissions')
@require_admin
def admin_submissions(course_id):
    course = get_course_by_id(course_id)
    if not course:
        abort(404)
    submissions = get_submissions_by_course(course_id)

    # Build list of enabled field keys for table headers
    enabled_fields = []
    for f in course['fields_config'].get('default_fields', []):
        if f.get('enabled'):
            enabled_fields.append(f)
    for f in course['fields_config'].get('custom_fields', []):
        enabled_fields.append(f)

    # Build list of enabled docs
    enabled_docs = []
    for doc_type in ['PHOTO', 'ID', 'LETTER']:
        dc = course['doc_config'].get(doc_type, {})
        if dc.get('enabled'):
            enabled_docs.append({"type": doc_type, "label": dc.get("label", doc_type)})

    return render_template('admin/submissions.html',
                           course=course,
                           submissions=submissions,
                           enabled_fields=enabled_fields,
                           enabled_docs=enabled_docs)


@app.route('/admin/submission/<int:submission_id>/delete', methods=['POST'])
@require_admin
def admin_submission_delete(submission_id):
    # Get course_id before deleting so we can redirect back
    from database import get_conn
    conn = get_conn()
    try:
        row = conn.execute("SELECT course_id FROM submissions WHERE id = ?", (submission_id,)).fetchone()
        course_id = row['course_id'] if row else None
    finally:
        conn.close()

    delete_submission(submission_id)
    flash('Submission deleted.', 'success')
    if course_id:
        return redirect(url_for('admin_submissions', course_id=course_id))
    return redirect(url_for('admin_dashboard'))


# ============================================================================
# FILE DOWNLOAD
# ============================================================================

@app.route('/admin/submission/<int:submission_id>/file/<doc_type>')
@require_admin
def admin_download_file(submission_id, doc_type):
    """Serve an uploaded document file to the admin."""
    doc_type = doc_type.upper()
    if doc_type not in VALID_DOC_TYPES:
        abort(400)

    sub = get_submission_by_id(submission_id)
    if not sub:
        abort(404)

    file_key = sub.get(f'{doc_type.lower()}_file')
    if not file_key:
        abort(404)

    storage = get_storage()
    file_path = storage.get_path(file_key)
    if not file_path:
        abort(404)

    download = request.args.get('download', '0') == '1'
    return send_file(
        file_path,
        as_attachment=download,
        download_name=Path(file_key).name if download else None
    )


# ============================================================================
# CSV EXPORT
# ============================================================================

@app.route('/admin/course/<int:course_id>/export')
@require_admin
def admin_export_csv(course_id):
    course = get_course_by_id(course_id)
    if not course:
        abort(404)
    submissions = get_submissions_by_course(course_id)

    # Build column list
    field_keys = []
    field_labels = []
    for f in course['fields_config'].get('default_fields', []):
        if f.get('enabled'):
            field_keys.append(f['key'])
            field_labels.append(f['label'])
    for f in course['fields_config'].get('custom_fields', []):
        field_keys.append(f['key'])
        field_labels.append(f['label'])

    # Add doc status columns
    doc_columns = []
    for doc_type in ['PHOTO', 'ID', 'LETTER']:
        dc = course['doc_config'].get(doc_type, {})
        if dc.get('enabled'):
            doc_columns.append((doc_type, dc.get('label', doc_type)))

    headers = ['#', 'Submitted At'] + field_labels
    for _, label in doc_columns:
        headers.append(f'{label} Status')

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)

    for i, sub in enumerate(submissions, 1):
        fd = sub.get('form_data', {})
        row = [i, sub.get('submitted_at', '')]
        for key in field_keys:
            row.append(fd.get(key, ''))
        for doc_type, _ in doc_columns:
            valid_key = f'{doc_type.lower()}_valid'
            result_key = f'{doc_type.lower()}_result'
            val = sub.get(valid_key)
            result = sub.get(result_key) or {}
            confidence = result.get('confidence', 0)
            conf_pct = f"{confidence * 100:.1f}%" if confidence else ''
            if val is None:
                row.append('N/A')
            elif val:
                row.append(f'ACCEPTED ({conf_pct})' if conf_pct else 'ACCEPTED')
            else:
                row.append(f'REJECTED ({conf_pct})' if conf_pct else 'REJECTED')
        writer.writerow(row)

    output.seek(0)
    filename = f"{course['slug']}_{datetime.now().strftime('%Y%m%d')}.csv"
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


# ============================================================================
# PUBLIC FORM
# ============================================================================

@app.route('/form/<slug>')
def public_form(slug):
    course = get_course_by_slug(slug)
    if not course:
        abort(404)
    if not course['is_active']:
        return render_template('public/closed.html', course=course)

    # Build enabled fields and docs for the template
    enabled_fields = []
    for f in course['fields_config'].get('default_fields', []):
        if f.get('enabled'):
            enabled_fields.append(f)
    for f in course['fields_config'].get('custom_fields', []):
        enabled_fields.append(f)

    enabled_docs = []
    for doc_type in ['PHOTO', 'ID', 'LETTER']:
        dc = course['doc_config'].get(doc_type, {})
        if dc.get('enabled'):
            enabled_docs.append({
                "type": doc_type,
                "label": dc.get("label", doc_type),
                "required": dc.get("required", True)
            })

    return render_template('public/form.html',
                           course=course,
                           enabled_fields=enabled_fields,
                           enabled_docs=enabled_docs)


@app.route('/form/<slug>/submit', methods=['POST'])
def public_form_submit(slug):
    course = get_course_by_slug(slug)
    if not course:
        return jsonify({'status': 'error', 'message': 'Course not found'}), 404
    if not course['is_active']:
        return jsonify({'status': 'error', 'message': 'Registration is closed'}), 400

    data = request.get_json(silent=True)
    if not data:
        return jsonify({'status': 'error', 'message': 'No data received'}), 400

    form_data = data.get('form_data', {})
    doc_results = data.get('doc_results', {})

    # Validate required fields
    all_fields = course['fields_config'].get('default_fields', []) + course['fields_config'].get('custom_fields', [])
    for f in all_fields:
        if f.get('enabled') and f.get('required'):
            val = form_data.get(f['key'], '').strip()
            if not val:
                return jsonify({'status': 'error', 'message': f'{f["label"]} is required'}), 400

    email = form_data.get('email', '').strip()
    if not email or not re.match(r'^[^\s@]+@[^\s@]+\.[^\s@]+$', email):
        return jsonify({'status': 'error', 'message': 'Valid email is required'}), 400

    # Validate mobile if present and enabled
    mobile = form_data.get('mobile', '').strip()
    mobile_field = next((f for f in all_fields if f['key'] == 'mobile' and f.get('enabled')), None)
    if mobile_field and mobile_field.get('required') and (not mobile or not re.match(r'^\d{10}$', mobile)):
        return jsonify({'status': 'error', 'message': 'Valid 10-digit mobile number is required'}), 400

    try:
        submission_id = save_submission(
            course_id=course['id'],
            email=email,
            form_data=form_data,
            doc_results=doc_results
        )

        # Finalize uploaded files from pending to permanent storage
        upload_session_id = data.get('upload_session_id', '').strip()
        if upload_session_id:
            try:
                storage = get_storage()
                file_keys = storage.finalize(upload_session_id, course['slug'], submission_id)
                if file_keys:
                    update_submission_files(submission_id, file_keys)
            except Exception as e:
                logger.error(f"Failed to finalize files for submission {submission_id}: {e}")

        return jsonify({
            'status': 'success',
            'submission_id': submission_id,
            'message': 'Registration submitted successfully!'
        }), 201
    except Exception as e:
        if 'UNIQUE constraint' in str(e):
            return jsonify({'status': 'error', 'message': 'A submission with this email already exists for this course.'}), 409
        logger.error(f"Submission error: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': 'Failed to save submission'}), 500


# ============================================================================
# ROOT REDIRECT
# ============================================================================

@app.route('/')
def index():
    return redirect(url_for('admin_login'))


# ============================================================================
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(413)
def handle_file_too_large(e):
    return jsonify({'status': 'error', 'message': 'File size exceeds 5MB limit'}), 413

@app.errorhandler(404)
def handle_not_found(e):
    return render_template('public/closed.html', course=None, message="Page not found"), 404

@app.errorhandler(500)
def handle_internal_error(e):
    return jsonify({'status': 'error', 'message': 'Internal server error'}), 500


# ============================================================================
# INITIALIZATION
# ============================================================================

def initialize_app():
    logger.info("=" * 70)
    logger.info("Starting LBSNAA Form Builder")
    logger.info("=" * 70)

    app.config['UPLOAD_FOLDER'].mkdir(exist_ok=True)

    try:
        init_db()
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

    try:
        ModelManager.initialize(model_dir='models')
        logger.info("Models loaded")
    except Exception as e:
        logger.error(f"Failed to load models: {e}")
        raise

    # Clean up stale pending uploads
    try:
        cleanup_hours = int(os.environ.get('UPLOAD_CLEANUP_HOURS', '24'))
        storage = get_storage()
        storage.cleanup_stale_pending(max_age_hours=cleanup_hours)
    except Exception as e:
        logger.warning(f"Pending upload cleanup failed: {e}")

    logger.info("=" * 70)
    logger.info("Server ready")
    logger.info("=" * 70)


if __name__ == '__main__':
    initialize_app()
    debug_mode = os.environ.get('FLASK_ENV', 'development') == 'development'
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=debug_mode, use_reloader=False)
