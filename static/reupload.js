/**
 * reupload.js - Client-side logic for the document re-upload page.
 * Handles file selection, drag-and-drop, validation via API, and result display.
 * Globals from template: REUPLOAD_TOKEN, DOC_TYPE, DOC_LABEL
 */

(function () {
    const MAX_FILE_SIZE = 5 * 1024 * 1024; // 5MB
    const ALLOWED_TYPES = ['image/jpeg', 'image/png', 'application/pdf'];
    const ALLOWED_EXTS = ['jpg', 'jpeg', 'png', 'pdf'];

    const dropZone = document.getElementById('dropZone');
    const fileInput = document.getElementById('fileInput');
    const uploadPrompt = document.getElementById('uploadPrompt');
    const uploadLoading = document.getElementById('uploadLoading');
    const validationResult = document.getElementById('validationResult');
    const uploadError = document.getElementById('uploadError');
    const mainContent = document.getElementById('mainContent');
    const successScreen = document.getElementById('successScreen');

    // Drag and drop
    ['dragenter', 'dragover'].forEach(evt => {
        dropZone.addEventListener(evt, (e) => {
            e.preventDefault();
            e.stopPropagation();
            dropZone.classList.add('border-primary-400', 'bg-primary-50');
        });
    });

    ['dragleave', 'drop'].forEach(evt => {
        dropZone.addEventListener(evt, (e) => {
            e.preventDefault();
            e.stopPropagation();
            dropZone.classList.remove('border-primary-400', 'bg-primary-50');
        });
    });

    dropZone.addEventListener('drop', (e) => {
        const files = e.dataTransfer.files;
        if (files.length > 0) handleFile(files[0]);
    });

    fileInput.addEventListener('change', () => {
        if (fileInput.files.length > 0) handleFile(fileInput.files[0]);
    });

    function handleFile(file) {
        // Reset state
        uploadError.classList.add('hidden');
        validationResult.classList.add('hidden');

        // Validate file type
        const ext = file.name.split('.').pop().toLowerCase();
        if (!ALLOWED_EXTS.includes(ext)) {
            showError('Invalid file type. Please upload JPG, PNG, or PDF.');
            return;
        }

        // Validate file size
        if (file.size > MAX_FILE_SIZE) {
            showError('File size exceeds 5MB limit.');
            return;
        }

        uploadDocument(file);
    }

    async function uploadDocument(file) {
        // Show loading
        uploadPrompt.classList.add('hidden');
        uploadLoading.classList.remove('hidden');
        dropZone.style.pointerEvents = 'none';

        const formData = new FormData();
        formData.append('file', file);

        try {
            const resp = await fetch(`/api/reupload/${REUPLOAD_TOKEN}`, {
                method: 'POST',
                body: formData,
            });

            const data = await resp.json();

            if (resp.ok && data.status === 'success') {
                showResult(data.validation, file.name);
            } else {
                showError(data.message || 'Validation failed. Please try again.');
                resetUploadUI();
            }
        } catch (err) {
            showError('Network error. Please check your connection and try again.');
            resetUploadUI();
        }
    }

    function showResult(validation, filename) {
        uploadLoading.classList.add('hidden');

        const isValid = validation.is_valid;
        const confidence = validation.confidence ? (validation.confidence * 100).toFixed(0) + '%' : '';
        const resultType = validation.result || '';
        const message = validation.message || '';

        let bgClass, textClass, icon, title;

        if (isValid) {
            bgClass = 'bg-green-50 border border-green-200';
            textClass = 'text-green-700';
            icon = '<svg class="w-6 h-6 text-green-600 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>';
            title = 'Document Accepted';
        } else {
            bgClass = 'bg-red-50 border border-red-200';
            textClass = 'text-red-700';
            icon = '<svg class="w-6 h-6 text-red-600 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>';
            title = 'Document Rejected';
        }

        let html = `
            <div class="flex items-start gap-3">
                ${icon}
                <div>
                    <p class="font-semibold ${textClass}">${title}</p>
                    <p class="text-sm text-gray-600 mt-1">${message}</p>
                    ${confidence ? `<p class="text-xs text-gray-500 mt-1">Confidence: ${confidence}</p>` : ''}
                    <p class="text-xs text-gray-400 mt-1">File: ${filename}</p>
                </div>
            </div>`;

        // Name match warning
        if (validation.name_match && validation.name_match.match_status && validation.name_match.match_status !== 'MATCH') {
            html += `<p class="text-xs text-amber-600 mt-2">Name match: ${validation.name_match.message || validation.name_match.match_status}</p>`;
        }

        // Celebrity warning
        if (validation.celebrity_warning && validation.celebrity_warning.detected) {
            html += `<p class="text-xs text-orange-600 mt-2 font-medium">Celebrity detected: ${validation.celebrity_warning.celebrity_name}</p>`;
        }

        validationResult.className = `mt-4 p-4 rounded-lg ${bgClass}`;
        validationResult.innerHTML = html;
        validationResult.classList.remove('hidden');

        // Show success screen if accepted
        if (isValid) {
            setTimeout(() => {
                mainContent.classList.add('hidden');
                successScreen.classList.remove('hidden');
                document.getElementById('successMessage').textContent =
                    `Your ${DOC_LABEL} has been re-uploaded and validated successfully (${confidence} confidence).`;
            }, 2000);
        } else {
            // For rejected, the token is already used. Show message.
            validationResult.innerHTML += `
                <p class="text-sm text-gray-600 mt-3 border-t pt-3">
                    The document was re-uploaded but did not pass validation. Please contact the administrator for further assistance.
                </p>`;
        }
    }

    function showError(msg) {
        uploadError.textContent = msg;
        uploadError.classList.remove('hidden');
    }

    function resetUploadUI() {
        uploadLoading.classList.add('hidden');
        uploadPrompt.classList.remove('hidden');
        dropZone.style.pointerEvents = '';
        fileInput.value = '';
    }
})();
