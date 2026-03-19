/**
 * admin.js — Admin panel interactivity
 * Handles: slug auto-generation, custom field add/remove, re-indexing on submit
 */

(function () {
    document.addEventListener('DOMContentLoaded', () => {
        const nameInput = document.getElementById('courseName');
        const slugInput = document.getElementById('courseSlug');
        const slugPreview = document.getElementById('slugPreview');
        const addCustomBtn = document.getElementById('addCustomField');
        const customList = document.getElementById('customFieldsList');
        const form = document.getElementById('courseForm');

        // Auto-generate slug from name
        let autoSlug = true;
        if (slugInput && slugInput.value) {
            autoSlug = false;
        }

        if (nameInput) {
            nameInput.addEventListener('input', () => {
                if (autoSlug && slugInput) {
                    const slug = slugify(nameInput.value);
                    slugInput.value = slug;
                    if (slugPreview) slugPreview.textContent = slug || '...';
                }
            });
        }

        if (slugInput) {
            slugInput.addEventListener('input', () => {
                autoSlug = false;
                if (slugPreview) slugPreview.textContent = slugInput.value || '...';
            });
        }

        // Add custom field
        if (addCustomBtn && customList) {
            addCustomBtn.addEventListener('click', () => {
                const row = document.createElement('div');
                row.className = 'flex items-center gap-3 p-3 rounded-lg border bg-white custom-field-row';
                row.innerHTML = `
                    <input type="text" name="custom_field_label" placeholder="Field label"
                           class="flex-1 px-3 py-1.5 border border-gray-300 rounded text-sm focus:ring-2 focus:ring-accent-500 outline-none">
                    <select name="custom_field_type" class="px-3 py-1.5 border border-gray-300 rounded text-sm">
                        <option value="text">Text</option>
                        <option value="email">Email</option>
                        <option value="tel">Phone</option>
                        <option value="textarea">Textarea</option>
                        <option value="select">Dropdown</option>
                    </select>
                    <input type="text" name="custom_field_options" placeholder="Options (comma sep.)"
                           class="w-48 px-3 py-1.5 border border-gray-300 rounded text-sm focus:ring-2 focus:ring-accent-500 outline-none">
                    <label class="flex items-center gap-1 text-sm">
                        <input type="checkbox" class="custom-req-checkbox h-3.5 w-3.5 rounded" value="1">
                        Req
                    </label>
                    <button type="button" onclick="this.closest('.custom-field-row').remove()" class="text-red-400 hover:text-red-600 text-lg">&times;</button>
                `;
                customList.appendChild(row);
            });
        }

        // Before form submit: re-index all custom field required checkboxes
        if (form) {
            form.addEventListener('submit', () => {
                const rows = customList ? customList.querySelectorAll('.custom-field-row') : [];
                rows.forEach((row, i) => {
                    const cb = row.querySelector('.custom-req-checkbox, [name^="custom_field_required"]');
                    if (cb) {
                        cb.name = `custom_field_required_${i}`;
                    }
                });
            });
        }
    });

    function slugify(text) {
        return text
            .toLowerCase()
            .trim()
            .replace(/[^\w\s-]/g, '')
            .replace(/[\s_]+/g, '-')
            .replace(/-+/g, '-')
            .replace(/^-|-$/g, '');
    }
})();
