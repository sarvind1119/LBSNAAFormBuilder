/**
 * form.js — Public registration form logic
 * Handles: field validation, file upload, document validation API calls, form submission
 */

(function () {
    // State
    const docResults = {};   // { PHOTO: { valid, result }, ID: ..., LETTER: ... }
    const docFiles = {};     // { PHOTO: File, ... }
    const uploadSessionId = crypto.randomUUID();

    // ========================================================================
    // FIELD VALIDATION
    // ========================================================================

    function getFormData() {
        const data = {};
        document.querySelectorAll('.form-field').forEach(el => {
            const key = el.dataset.field;
            if (key) data[key] = el.value.trim();
        });
        return data;
    }

    function validateFields() {
        const data = getFormData();
        let allValid = true;

        ENABLED_FIELDS.forEach(field => {
            const errEl = document.querySelector(`[data-error="${field.key}"]`);
            const val = data[field.key] || '';

            let error = '';
            if (field.required && !val) {
                error = `${field.label} is required`;
            } else if (field.key === 'email' && val && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(val)) {
                error = 'Invalid email format';
            } else if (field.key === 'mobile' && val && !/^\d{10}$/.test(val)) {
                error = 'Must be 10 digits';
            }

            if (errEl) {
                if (error) {
                    errEl.textContent = error;
                    errEl.classList.remove('hidden');
                    allValid = false;
                } else {
                    errEl.classList.add('hidden');
                }
            } else if (error) {
                allValid = false;
            }
        });

        return allValid;
    }

    // ========================================================================
    // DOCUMENT UPLOAD & VALIDATION
    // ========================================================================

    function setupDropZones() {
        document.querySelectorAll('.drop-zone').forEach(zone => {
            const docType = zone.dataset.docType;
            const fileInput = zone.querySelector('.doc-file-input');

            zone.addEventListener('click', () => fileInput.click());

            zone.addEventListener('dragover', e => {
                e.preventDefault();
                zone.classList.add('dragover');
            });
            zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
            zone.addEventListener('drop', e => {
                e.preventDefault();
                zone.classList.remove('dragover');
                if (e.dataTransfer.files.length) {
                    handleFile(docType, e.dataTransfer.files[0]);
                }
            });

            fileInput.addEventListener('change', () => {
                if (fileInput.files.length) {
                    handleFile(docType, fileInput.files[0]);
                }
            });
        });

        // Remove buttons
        document.querySelectorAll('.doc-remove').forEach(btn => {
            btn.addEventListener('click', () => {
                const docType = btn.dataset.docType;
                resetDoc(docType);
            });
        });
    }

    function handleFile(docType, file) {
        // Validate file
        const validTypes = ['image/jpeg', 'image/png', 'application/pdf'];
        if (!validTypes.includes(file.type)) {
            showDocError(docType, 'Only JPG, PNG, and PDF files are accepted.');
            return;
        }
        if (file.size > 5 * 1024 * 1024) {
            showDocError(docType, 'File size must be under 5MB.');
            return;
        }

        docFiles[docType] = file;
        validateDocument(docType, file);
    }

    async function validateDocument(docType, file) {
        const section = document.querySelector(`.doc-upload-section[data-doc-type="${docType}"]`);
        const loading = section.querySelector('.doc-loading');
        const resultEl = section.querySelector('.doc-result');
        const removeBtn = section.querySelector('.doc-remove');
        const dropZone = section.querySelector('.drop-zone');

        // Show loading
        loading.classList.remove('hidden');
        resultEl.classList.add('hidden');
        removeBtn.classList.add('hidden');
        dropZone.classList.add('hidden');

        // Get name field value for name matching
        const nameField = document.querySelector('[data-field="name"]');
        const userName = nameField ? nameField.value.trim() : '';

        const formData = new FormData();
        formData.append('file', file);
        formData.append('name', userName);
        formData.append('upload_session_id', uploadSessionId);

        try {
            const response = await fetch(`/api/validate/${docType}`, {
                method: 'POST',
                body: formData
            });
            const data = await response.json();

            loading.classList.add('hidden');

            if (data.status === 'success' && data.validation) {
                const v = data.validation;
                docResults[docType] = {
                    valid: v.is_valid && v.result === 'ACCEPT',
                    result: v
                };
                showDocResult(docType, v);
            } else {
                showDocError(docType, data.message || 'Validation failed');
            }
        } catch (err) {
            loading.classList.add('hidden');
            showDocError(docType, 'Failed to connect to validation server.');
        }

        removeBtn.classList.remove('hidden');
        updateSubmitState();
    }

    function showDocResult(docType, v) {
        const resultEl = document.querySelector(`.doc-result[data-doc-type="${docType}"]`);
        resultEl.classList.remove('hidden');

        let html = '';
        const confidence = Math.round((v.confidence || 0) * 100);

        if (v.result === 'ACCEPT') {
            html = `
                <div class="bg-green-50 border border-green-200 rounded-lg p-3">
                    <div class="flex items-center gap-2">
                        <span class="text-green-600 font-bold">&#10003;</span>
                        <span class="text-sm font-medium text-green-800">Accepted</span>
                        <span class="text-xs text-green-600 ml-auto">${confidence}% confidence</span>
                    </div>
                    <p class="text-xs text-green-600 mt-1">${v.message || ''}</p>
                </div>`;
        } else if (v.result === 'SUSPICIOUS') {
            html = `
                <div class="bg-amber-50 border border-amber-200 rounded-lg p-3">
                    <div class="flex items-center gap-2">
                        <span class="text-amber-600 font-bold">&#9888;</span>
                        <span class="text-sm font-medium text-amber-800">Suspicious</span>
                        <span class="text-xs text-amber-600 ml-auto">${confidence}% confidence</span>
                    </div>
                    <p class="text-xs text-amber-600 mt-1">${v.message || 'Low confidence in document type.'}</p>
                </div>`;
        } else {
            // MISMATCH, WRONG, BLANK
            const label = v.result === 'MISMATCH' ? 'Wrong Document Type' : v.result === 'BLANK' ? 'Blank Document' : 'Invalid Document';
            html = `
                <div class="bg-red-50 border border-red-200 rounded-lg p-3">
                    <div class="flex items-center gap-2">
                        <span class="text-red-600 font-bold">&#10007;</span>
                        <span class="text-sm font-medium text-red-800">${label}</span>
                    </div>
                    <p class="text-xs text-red-600 mt-1">${v.message || ''}</p>
                </div>`;
        }

        // Name match warning
        if (v.name_match && v.name_match.match_status === 'NO_MATCH') {
            html += `
                <div class="bg-amber-50 border border-amber-200 rounded-lg p-2 mt-2">
                    <p class="text-xs text-amber-700"><strong>Name Mismatch:</strong> ${v.name_match.message || 'Name not found in document.'}</p>
                </div>`;
        } else if (v.name_match && v.name_match.match_status === 'PARTIAL') {
            html += `
                <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-2 mt-2">
                    <p class="text-xs text-yellow-700"><strong>Partial Match:</strong> ${v.name_match.message || ''}</p>
                </div>`;
        }

        // Celebrity warning
        if (v.celebrity_warning && v.celebrity_warning.detected) {
            html += `
                <div class="bg-orange-50 border border-orange-200 rounded-lg p-2 mt-2">
                    <p class="text-xs text-orange-700"><strong>Celebrity Detected:</strong> This photo resembles ${v.celebrity_warning.celebrity_name || 'a known celebrity'}.</p>
                </div>`;
        }

        resultEl.innerHTML = html;
    }

    function showDocError(docType, message) {
        const resultEl = document.querySelector(`.doc-result[data-doc-type="${docType}"]`);
        resultEl.classList.remove('hidden');
        resultEl.innerHTML = `
            <div class="bg-red-50 border border-red-200 rounded-lg p-3">
                <p class="text-sm text-red-700">${message}</p>
            </div>`;
        docResults[docType] = { valid: false, result: null };

        const removeBtn = document.querySelector(`.doc-remove[data-doc-type="${docType}"]`);
        removeBtn.classList.remove('hidden');
        updateSubmitState();
    }

    function resetDoc(docType) {
        const section = document.querySelector(`.doc-upload-section[data-doc-type="${docType}"]`);
        const dropZone = section.querySelector('.drop-zone');
        const loading = section.querySelector('.doc-loading');
        const resultEl = section.querySelector('.doc-result');
        const removeBtn = section.querySelector('.doc-remove');
        const fileInput = section.querySelector('.doc-file-input');

        dropZone.classList.remove('hidden');
        loading.classList.add('hidden');
        resultEl.classList.add('hidden');
        resultEl.innerHTML = '';
        removeBtn.classList.add('hidden');
        fileInput.value = '';

        delete docResults[docType];
        delete docFiles[docType];
        updateSubmitState();
    }

    // ========================================================================
    // SUBMIT
    // ========================================================================

    function updateSubmitState() {
        const btn = document.getElementById('submitBtn');
        const hint = document.getElementById('submitHint');

        const fieldsValid = validateFields();

        // Check all required docs are accepted
        let docsValid = true;
        let docHint = '';
        ENABLED_DOCS.forEach(doc => {
            if (doc.required) {
                const r = docResults[doc.type];
                if (!r || !r.valid) {
                    docsValid = false;
                    docHint = `${doc.label} must be accepted`;
                }
            }
        });

        const canSubmit = fieldsValid && docsValid;
        btn.disabled = !canSubmit;

        if (!fieldsValid) {
            hint.textContent = 'Fill all required fields';
        } else if (!docsValid) {
            hint.textContent = docHint;
        } else {
            hint.textContent = '';
        }
    }

    async function handleSubmit() {
        const btn = document.getElementById('submitBtn');
        btn.disabled = true;
        btn.textContent = 'Submitting...';

        const formData = getFormData();

        // Build doc_results for submission
        const submissionDocResults = {};
        ENABLED_DOCS.forEach(doc => {
            const r = docResults[doc.type];
            if (r) {
                submissionDocResults[doc.type] = r;
            }
        });

        try {
            const response = await fetch(`/form/${COURSE_SLUG}/submit`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    form_data: formData,
                    doc_results: submissionDocResults,
                    upload_session_id: uploadSessionId
                })
            });
            const data = await response.json();

            if (data.status === 'success') {
                document.getElementById('formContainer').classList.add('hidden');
                document.getElementById('successScreen').classList.remove('hidden');
                document.getElementById('submissionId').textContent = data.submission_id;
            } else {
                showError(data.message || 'Submission failed');
                btn.disabled = false;
                btn.textContent = 'Submit Registration';
            }
        } catch (err) {
            showError('Failed to connect to server.');
            btn.disabled = false;
            btn.textContent = 'Submit Registration';
        }
    }

    function showError(message) {
        const banner = document.getElementById('errorBanner');
        banner.textContent = message;
        banner.classList.remove('hidden');
        banner.scrollIntoView({ behavior: 'smooth', block: 'center' });
        setTimeout(() => banner.classList.add('hidden'), 8000);
    }

    // ========================================================================
    // INIT
    // ========================================================================

    document.addEventListener('DOMContentLoaded', () => {
        setupDropZones();

        // Live validation on field change
        document.querySelectorAll('.form-field').forEach(el => {
            el.addEventListener('input', updateSubmitState);
            el.addEventListener('change', updateSubmitState);
        });

        document.getElementById('submitBtn').addEventListener('click', handleSubmit);
        updateSubmitState();
    });
})();
