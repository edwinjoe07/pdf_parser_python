/* ═══════════════════════════════════════════════════════════════════════════
   PDF Parser Engine — Advanced Dashboard Application
   ═══════════════════════════════════════════════════════════════════════════ */

const API = '';

// ─── State ────────────────────────────────────────────────────────────────
const S = {
    jobs: {},              // job_id -> job object
    activeJobId: null,     // currently reviewing
    result: null,          // current parse result
    questions: [],         // current questions list
    selectedQ: null,       // selected question number
    pollTimers: {},        // polling intervals
    uploadFile: null,      // file to upload
};

// ─── Boot ─────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    initNav();
    initUploadModal();
    initReviewPage();
    initMissingPanel();
    initLightbox();
    checkHealth();
    loadPreviousJobs();
    setInterval(checkHealth, 12000);
});

// ═══════════════════════════════════════════════════════════════════════════
// LOAD PREVIOUS JOBS (persisted in SQLite)
// ═══════════════════════════════════════════════════════════════════════════

async function loadPreviousJobs() {
    try {
        const r = await fetch(`${API}/api/jobs`);
        if (!r.ok) return;
        const list = await r.json();
        for (const j of list) {
            if (S.jobs[j.id]) continue; // already in memory
            S.jobs[j.id] = {
                id: j.id,
                exam_db_id: j.exam_db_id || null,
                filename: j.filename || 'unknown.pdf',
                pdf_path: j.pdf_path || '',
                status: j.status || 'completed',
                progress: j.progress ?? 100,
                created_at: j.created_at || new Date().toISOString(),
                error: j.error || null,
                questions_count: j.questions_count,
                confidence: null,
                _result: null,
            };
        }
    } catch (e) {
        console.warn('Failed to load previous jobs:', e);
    }

    // Also load from /exams (new-style background-parsed exams)
    try {
        const r2 = await fetch(`${API}/exams`);
        if (r2.ok) {
            const exams = await r2.json();
            for (const ex of exams) {
                const key = `exam_${ex.id}`;
                if (S.jobs[key]) continue;
                // Check if this exam is already loaded by job_id
                const alreadyLoaded = Object.values(S.jobs).some(
                    j => j.exam_db_id === ex.id
                );
                if (alreadyLoaded) continue;

                S.jobs[key] = {
                    id: key,
                    exam_db_id: ex.id,
                    filename: ex.original_filename || ex.name || 'unknown.pdf',
                    pdf_path: ex.file_path || '',
                    size: ex.file_size_bytes || 0,
                    status: mapExamStatus(ex.status),
                    progress: ex.status === 'completed' ? 100 : Math.round(((ex.current_page || 0) / Math.max(ex.total_pages || 1, 1)) * 100),
                    created_at: ex.created_at || new Date().toISOString(),
                    error: ex.last_error || null,
                    questions_count: ex.total_questions || 0,
                    pages: ex.total_pages || 0,
                    confidence: null,
                    _result: null,
                };

                // Start polling if still processing
                if (ex.status === 'processing' || ex.status === 'pending') {
                    startExamPoll(ex.id, key);
                }
            }
        }
    } catch (e) {
        console.warn('Failed to load /exams:', e);
    }

    renderImportsTable();
    updateStatusCards();
}

function mapExamStatus(dbStatus) {
    const map = {
        'completed': 'parsed',
        'processing': 'processing',
        'pending': 'queued',
        'paused': 'paused',
        'failed': 'failed',
    };
    return map[dbStatus] || dbStatus;
}

function startExamPoll(examDbId, jobKey) {
    if (S.pollTimers[jobKey]) return;
    S.pollTimers[jobKey] = setInterval(async () => {
        try {
            const r = await fetch(`${API}/exam/${examDbId}/progress`);
            if (!r.ok) return;
            const p = await r.json();

            const j = S.jobs[jobKey];
            if (!j) return;
            j.progress = p.percentage || 0;
            j.questions_count = p.total_questions || 0;
            j.pages = p.total_pages || 0;
            j.status = mapExamStatus(p.status);

            if (p.status === 'completed' || p.status === 'failed') {
                clearInterval(S.pollTimers[jobKey]);
                delete S.pollTimers[jobKey];
                if (p.status === 'completed') {
                    toast(`\u2713 Parsed: ${j.filename || jobKey}`, 'success');
                } else {
                    toast(`\u2717 Failed: ${p.last_error || 'Unknown error'}`, 'error');
                }
            }

            renderImportsTable();
            updateStatusCards();
        } catch { }
    }, 3000);
}

// ═══════════════════════════════════════════════════════════════════════════
// NAVIGATION
// ═══════════════════════════════════════════════════════════════════════════

function initNav() {
    document.querySelectorAll('.leftnav__btn').forEach(btn => {
        btn.addEventListener('click', () => showPage(btn.dataset.page));
    });
}

function showPage(id) {
    document.querySelectorAll('.leftnav__btn').forEach(b =>
        b.classList.toggle('leftnav__btn--active', b.dataset.page === id)
    );
    document.querySelectorAll('.page').forEach(p =>
        p.classList.toggle('page--active', p.id === `page-${id}`)
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// HEALTH
// ═══════════════════════════════════════════════════════════════════════════

async function checkHealth() {
    const dot = document.getElementById('server-dot');
    const txt = document.getElementById('server-text');
    try {
        const r = await fetch(`${API}/api/health`);
        const d = await r.json();
        dot.className = 'server-dot server-dot--on';
        txt.textContent = `Online — ${d.active_jobs || 0} active`;
    } catch {
        dot.className = 'server-dot server-dot--off';
        txt.textContent = 'Offline';
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// UPLOAD MODAL
// ═══════════════════════════════════════════════════════════════════════════

function initUploadModal() {
    const modal = document.getElementById('upload-modal');
    const zone = document.getElementById('drop-zone');
    const picker = document.getElementById('file-picker');

    document.getElementById('btn-upload-pdf').addEventListener('click', () => {
        modal.style.display = 'flex';
        S.uploadFile = null;
        resetUploadModal();
    });

    document.getElementById('modal-close').addEventListener('click', () => modal.style.display = 'none');
    document.getElementById('btn-upload-cancel').addEventListener('click', () => modal.style.display = 'none');

    modal.addEventListener('click', (e) => {
        if (e.target === modal) modal.style.display = 'none';
    });

    zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('drop-zone--active'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drop-zone--active'));
    zone.addEventListener('drop', (e) => {
        e.preventDefault();
        zone.classList.remove('drop-zone--active');
        const f = Array.from(e.dataTransfer.files).find(f => f.name.endsWith('.pdf'));
        if (f) selectFile(f);
        else toast('Please select a PDF file', 'warning');
    });

    zone.addEventListener('click', () => picker.click());
    picker.addEventListener('change', () => {
        if (picker.files[0]) selectFile(picker.files[0]);
        picker.value = '';
    });

    document.getElementById('btn-upload-start').addEventListener('click', doUpload);
}

function resetUploadModal() {
    document.getElementById('upload-fileinfo').style.display = 'none';
    document.getElementById('modal-fields').style.display = 'none';
    document.getElementById('btn-upload-start').disabled = true;
    document.getElementById('up-exam-name').value = '';
    document.getElementById('up-provider').value = '';
    document.getElementById('up-version').value = '';
}

function selectFile(file) {
    S.uploadFile = file;
    const info = document.getElementById('upload-fileinfo');
    info.style.display = 'block';
    info.textContent = `📄 ${file.name}  (${fmtSize(file.size)})`;

    document.getElementById('modal-fields').style.display = 'flex';
    document.getElementById('btn-upload-start').disabled = false;

    // Auto fill exam name
    document.getElementById('up-exam-name').value = file.name.replace('.pdf', '').replace(/[_-]/g, ' ');
}

async function doUpload() {
    if (!S.uploadFile) return;

    const btn = document.getElementById('btn-upload-start');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Uploading...';

    const fd = new FormData();
    fd.append('file', S.uploadFile);
    const en = document.getElementById('up-exam-name').value;
    const pr = document.getElementById('up-provider').value;
    const vr = document.getElementById('up-version').value;
    if (en) fd.append('exam_name', en);
    if (pr) fd.append('exam_provider', pr);
    if (vr) fd.append('exam_version', vr);
    fd.append('log_level', document.getElementById('up-loglevel').value);

    try {
        const res = await fetch(`${API}/upload`, { method: 'POST', body: fd });
        if (!res.ok) throw new Error((await res.json()).error || 'Upload failed');

        const data = await res.json();
        toast(`Parse started: ${S.uploadFile.name}`, 'success');

        const jobKey = `exam_${data.exam_id}`;
        S.jobs[jobKey] = {
            id: jobKey,
            exam_db_id: data.exam_id,
            filename: S.uploadFile.name,
            size: S.uploadFile.size,
            status: 'processing',
            created_at: new Date().toISOString(),
            questions_count: 0,
            confidence: null,
            pages: data.total_pages || null,
            _result: null,
        };

        startExamPoll(data.exam_id, jobKey);
        document.getElementById('upload-modal').style.display = 'none';
        renderImportsTable();
        updateStatusCards();

    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg> Start Parsing';
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// JOB POLLING
// ═══════════════════════════════════════════════════════════════════════════

function startPoll(jobId) {
    if (S.pollTimers[jobId]) return;
    S.pollTimers[jobId] = setInterval(async () => {
        try {
            const r = await fetch(`${API}/api/status/${jobId}`);
            if (!r.ok) return;
            const d = await r.json();

            // Merge into state
            S.jobs[jobId] = { ...S.jobs[jobId], ...d };

            if (d.status === 'completed' || d.status === 'failed') {
                clearInterval(S.pollTimers[jobId]);
                delete S.pollTimers[jobId];

                if (d.status === 'completed') {
                    S.jobs[jobId].status = 'parsed';  // rename for display
                    // fetch result metadata
                    try {
                        const rr = await fetch(`${API}/api/result/${jobId}`);
                        if (rr.ok) {
                            const result = await rr.json();
                            S.jobs[jobId].pages = result.exam?.total_pages || null;
                            S.jobs[jobId].questions_count = result.questions?.length || 0;
                            const v = result.validation || {};
                            const det = v.total_questions_detected || 0;
                            const suc = v.structured_successfully || 0;
                            S.jobs[jobId].confidence = det > 0 ? Math.round((suc / det) * 100) : 0;
                            S.jobs[jobId]._result = result;
                        }
                    } catch { }
                    toast(`✓ Parsed: ${S.jobs[jobId].filename || jobId}`, 'success');
                } else {
                    toast(`✗ Failed: ${d.error || 'Unknown error'}`, 'error');
                }
            }

            renderImportsTable();
            updateStatusCards();
        } catch { }
    }, 1500);
}

// ═══════════════════════════════════════════════════════════════════════════
// IMPORTS TABLE
// ═══════════════════════════════════════════════════════════════════════════

function renderImportsTable() {
    const tbody = document.getElementById('imports-tbody');
    const ids = Object.keys(S.jobs);

    if (ids.length === 0) {
        tbody.innerHTML = `
            <tr class="dtable__empty"><td colspan="9">
                <div class="empty-msg">
                    <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" opacity=".35"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/></svg>
                    <p>No PDFs uploaded yet</p>
                    <span>Upload a PDF to start parsing</span>
                </div>
            </td></tr>`;
        return;
    }

    let rows = '';
    for (const id of ids.reverse()) {
        const j = S.jobs[id];
        const st = j.status || 'queued';
        const statusMap = {
            'queued': 'queued',
            'processing': 'processing',
            'parsed': 'parsed',
            'completed': 'parsed',
            'paused': 'paused',
            'failed': 'failed'
        };
        const statusKey = statusMap[st] || st;
        const statusLabel = statusKey.charAt(0).toUpperCase() + statusKey.slice(1);

        // Confidence
        let confHtml = '<span style="color:var(--t4)">—</span>';
        if (j.confidence != null) {
            const cls = j.confidence >= 90 ? 'high' : j.confidence >= 50 ? 'mid' : 'low';
            confHtml = `
                <div class="conf-bar">
                    <div class="conf-bar__track">
                        <div class="conf-bar__fill conf-bar__fill--${cls}" style="width:${j.confidence}%"></div>
                    </div>
                    <span class="conf-bar__text">${j.confidence}%</span>
                </div>`;
        }

        // Actions
        let actions = '';
        if (statusKey === 'parsed') {
            actions = `
                <button class="alink alink--review" onclick="openReview('${id}')">Review</button>
                <button class="alink alink--reparse" onclick="reParse('${id}')">Re-parse</button>
                <button class="alink alink--logs" onclick="showLogs('${id}')">Logs</button>
                <button class="alink alink--delete" onclick="deleteJob('${id}')">Delete</button>`;
        } else if (statusKey === 'failed') {
            actions = `
                <button class="alink alink--resume" onclick="resumeJob('${id}')">Resume</button>
                <button class="alink alink--reparse" onclick="reParse('${id}')">Re-parse</button>
                <button class="alink alink--logs" onclick="showLogs('${id}')">Logs</button>
                <button class="alink alink--delete" onclick="deleteJob('${id}')">Delete</button>`;
        } else if (statusKey === 'processing') {
            actions = `
                <button class="alink alink--review" onclick="openReview('${id}')">Review</button>
                <button class="alink alink--pause" onclick="pauseJob('${id}')">Pause</button>
                <button class="alink alink--cancel" onclick="cancelJob('${id}')">Cancel</button>
                <button class="alink alink--logs" onclick="showLogs('${id}')">Logs</button>`;
        } else if (statusKey === 'paused') {
            actions = `
               <button class="alink alink--review" onclick="openReview('${id}')">Review</button>
               <button class="alink alink--resume" onclick="resumeJob('${id}')">Resume</button>
               <button class="alink alink--cancel" onclick="cancelJob('${id}')">Cancel</button>
               <button class="alink alink--logs" onclick="showLogs('${id}')">Logs</button>`;
        } else if (statusKey === 'queued') {
            actions = `
                <button class="alink alink--cancel" onclick="cancelJob('${id}')">Cancel</button>
                <button class="alink alink--logs" onclick="showLogs('${id}')">Logs</button>`;
        } else {
            actions = `<span class="spinner"></span>`;
        }

        // Warning
        let warningRow = '';
        if (statusKey === 'failed' && j.error) {
            warningRow = `</tr><tr><td colspan="9" style="padding:0"><div class="table-warning">⚠ ${esc(j.error)}</div></td>`;
        } else if (statusKey === 'parsed' && j.questions_count === 0) {
            warningRow = `</tr><tr><td colspan="9" style="padding:0"><div class="table-warning">⚠ No questions detected after parsing</div></td>`;
        }

        // Status Badge + Progress
        let statusBadge = `<span class="sbadge sbadge--${statusKey}"><span class="sbadge__dot"></span> ${statusLabel}</span>`;
        if (statusKey === 'processing' && j.progress > 0 && j.progress < 100) {
            statusBadge = `
                <div class="progress-col">
                    <span class="sbadge sbadge--processing"><span class="sbadge__dot"></span> ${j.progress}%</span>
                    <div class="mini-progress"><div class="mini-progress__fill" style="width:${j.progress}%"></div></div>
                </div>`;
        }

        rows += `
            <tr>
                <td class="fid">${id.slice(0, 6)}</td>
                <td class="fname">${esc(j.filename || 'unknown.pdf')}</td>
                <td>${j.size ? fmtSize(j.size) : '—'}</td>
                <td>${j.pages || '—'}</td>
                <td>${statusBadge}</td>
                <td>${confHtml}</td>
                <td>${j.questions_count != null ? j.questions_count : '—'}</td>
                <td>${fmtAgo(j.created_at)}</td>
                <td><div class="action-links">${actions}</div></td>
                ${warningRow}
            </tr>`;
    }

    tbody.innerHTML = rows;
}

function updateStatusCards() {
    const all = Object.values(S.jobs);
    const pending = all.filter(j => j.status === 'queued' || j.status === 'paused').length;
    const active = all.filter(j => j.status === 'processing').length;
    const done = all.filter(j => j.status === 'parsed' || j.status === 'completed').length;

    document.getElementById('stat-pending').textContent = `${pending} File${pending !== 1 ? 's' : ''}`;
    document.getElementById('stat-active').textContent = `${active} Active`;
    document.getElementById('stat-done').textContent = `${done} Total`;
}

// ═══════════════════════════════════════════════════════════════════════════
// TABLE ACTIONS
// ═══════════════════════════════════════════════════════════════════════════

async function deleteJob(jobId) {
    if (!confirm('Delete this parse job and all its data?')) return;

    const j = S.jobs[jobId];
    // Call server DELETE endpoint
    if (j && j.exam_db_id) {
        try {
            await fetch(`${API}/exam/${j.exam_db_id}`, { method: 'DELETE' });
        } catch (e) {
            console.warn('Server delete failed:', e);
        }
    }

    delete S.jobs[jobId];
    if (S.pollTimers[jobId]) {
        clearInterval(S.pollTimers[jobId]);
        delete S.pollTimers[jobId];
    }
    renderImportsTable();
    updateStatusCards();
    toast('Job deleted', 'info');
}

async function pauseJob(jobId) {
    const j = S.jobs[jobId];
    if (!j || !j.exam_db_id) return;
    try {
        const res = await fetch(`${API}/exam/${j.exam_db_id}/pause`, { method: 'POST' });
        if (!res.ok) throw new Error((await res.json()).error || 'Pause failed');
        toast('Pause signal sent', 'info');
        // Let polling update the UI, or force it now
        j.status = 'paused';
        renderImportsTable();
    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    }
}

async function resumeJob(jobId) {
    const j = S.jobs[jobId];
    if (!j || !j.exam_db_id) return;
    try {
        const res = await fetch(`${API}/exam/${j.exam_db_id}/resume`, { method: 'POST' });
        if (!res.ok) throw new Error((await res.json()).error || 'Resume failed');
        toast('Resume signal sent', 'success');
        j.status = 'processing';
        renderImportsTable();
        startExamPoll(j.exam_db_id, jobId);
    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    }
}

async function cancelJob(jobId) {
    if (!confirm('Cancel this parsing job?')) return;
    const j = S.jobs[jobId];
    if (!j || !j.exam_db_id) return;
    try {
        const res = await fetch(`${API}/exam/${j.exam_db_id}/cancel`, { method: 'POST' });
        if (!res.ok) throw new Error((await res.json()).error || 'Cancel failed');
        toast('Parsing cancelled', 'warning');
        j.status = 'failed';
        renderImportsTable();
    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    }
}

async function reParse(jobId) {
    const j = S.jobs[jobId];
    if (!j) return;

    if (j.pdf_path) {
        if (!confirm(`Re-parse existing file: ${j.filename}?\nThis will clear current results.`)) return;

        try {
            toast('Starting re-parse...', 'info');
            const res = await fetch(`${API}/api/parse`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    file_path: j.pdf_path,
                    exam_name: j.filename.replace('.pdf', ''),
                    exam_id: j.id.startsWith('exam_') ? j.id.replace('exam_', '') : j.id
                })
            });
            if (!res.ok) throw new Error((await res.json()).error || 'Re-parse failed');
            const data = await res.json();

            // Re-use current job entry or let polling update it
            j.status = 'queued';
            j.progress = 0;
            j.error = null;

            startPoll(data.job_id); // Start polling the new job
            toast('Re-parse started', 'success');
            renderImportsTable();
        } catch (err) {
            toast(`Error: ${err.message}`, 'error');
        }
    } else {
        toast('Original file path not found. Please upload again.', 'warning');
        document.getElementById('upload-modal').style.display = 'flex';
        resetUploadModal();
    }
}

function showLogs(jobId) {
    const modal = document.getElementById('logs-modal');
    const content = document.getElementById('logs-content');
    const j = S.jobs[jobId];

    let logText = `Job ID: ${jobId}\n`;
    logText += `Filename: ${j?.filename || 'N/A'}\n`;
    logText += `Status: ${j?.status || 'N/A'}\n`;
    logText += `Created: ${j?.created_at || 'N/A'}\n`;
    if (j?.started_at) logText += `Started: ${new Date(j.started_at * 1000).toISOString()}\n`;
    if (j?.completed_at) logText += `Completed: ${new Date(j.completed_at * 1000).toISOString()}\n`;
    if (j?.duration) logText += `Duration: ${j.duration}s\n`;
    if (j?.error) logText += `\n❌ ERROR:\n${j.error}\n`;
    if (j?.questions_count != null) logText += `\nQuestions parsed: ${j.questions_count}\n`;

    content.textContent = logText;
    modal.style.display = 'flex';

    document.getElementById('logs-close').onclick = () => modal.style.display = 'none';
    modal.addEventListener('click', (e) => {
        if (e.target === modal) modal.style.display = 'none';
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// REVIEW PAGE
// ═══════════════════════════════════════════════════════════════════════════

function initReviewPage() {
    document.getElementById('btn-back-imports').addEventListener('click', () => {
        document.getElementById('nav-review').style.display = 'none';
        showPage('imports');
    });

    document.getElementById('btn-parse-logs').addEventListener('click', () => {
        if (S.activeJobId) showLogs(S.activeJobId);
    });

    document.getElementById('filter-search').addEventListener('input', renderQList);
    document.getElementById('filter-status').addEventListener('change', renderQList);
    document.getElementById('filter-type').addEventListener('change', renderQList);

    document.getElementById('btn-jump-incomplete').addEventListener('click', jumpToIncomplete);

    // Detail actions
    document.getElementById('btn-save-q').addEventListener('click', () => toast('Changes saved locally', 'success'));
    document.getElementById('btn-approve-q').addEventListener('click', () => {
        toast('Question approved ✓', 'success');
    });
    document.getElementById('btn-reject-q').addEventListener('click', () => {
        toast('Question rejected ✗', 'warning');
    });
    document.getElementById('btn-delete-q').addEventListener('click', () => {
        toast('Question deleted', 'info');
    });

    // Edit/Preview toggle
    document.getElementById('btn-edit-mode').addEventListener('click', () => {
        document.getElementById('btn-edit-mode').classList.add('toggle-btn--active');
        document.getElementById('btn-preview-mode').classList.remove('toggle-btn--active');
        setFieldsEditable(true);
    });
    document.getElementById('btn-preview-mode').addEventListener('click', () => {
        document.getElementById('btn-preview-mode').classList.add('toggle-btn--active');
        document.getElementById('btn-edit-mode').classList.remove('toggle-btn--active');
        setFieldsEditable(false);
    });

    document.getElementById('btn-add-option').addEventListener('click', addOption);

    // Filter events
    document.getElementById('filter-status').addEventListener('change', renderQList);
    document.getElementById('filter-type').addEventListener('change', renderQList);
    document.getElementById('filter-search').addEventListener('input', renderQList);

    // Char counter
    document.getElementById('qd-question').addEventListener('input', (e) => {
        document.getElementById('qd-charcount').textContent = `${e.target.value.length} chars`;
    });
}

function initLightbox() {
    const modal = document.getElementById('lightbox-modal');
    const close = document.getElementById('lightbox-close');

    close.addEventListener('click', () => modal.style.display = 'none');
    modal.addEventListener('click', (e) => {
        // Close if clicking overlay OR the black container area (not the image itself)
        if (e.target === modal || e.target.classList.contains('lightbox__container')) {
            modal.style.display = 'none';
        }
    });
}

function openLightbox(src, caption = '') {
    const modal = document.getElementById('lightbox-modal');
    const img = document.getElementById('lightbox-img');
    const cap = document.getElementById('lightbox-caption');

    img.src = src;
    cap.textContent = caption;
    modal.style.display = 'flex';
}


function setFieldsEditable(editable) {
    const fields = document.querySelectorAll('.field-textarea, .field-input, .field-select');
    fields.forEach(f => {
        f.disabled = !editable;
        f.style.opacity = editable ? '1' : '0.6';
    });
}

async function openReview(jobId) {
    S.activeJobId = jobId;
    const j = S.jobs[jobId];

    // Get result
    let result = j._result;
    if (!result) {
        try {
            let r;
            if (j.exam_db_id) {
                // New-style exam: fetch by integer exam_id
                r = await fetch(`${API}/exam/${j.exam_db_id}`);
            } else {
                // Old-style job: fetch by UUID job_id
                r = await fetch(`${API}/api/result/${jobId}`);
            }
            if (!r.ok) throw new Error('Failed to fetch');
            result = await r.json();
            S.jobs[jobId]._result = result;

            // Update confidence from validation data
            const v = result.validation || {};
            const det = v.total_questions_detected || 0;
            const suc = v.structured_successfully || v.fully_structured_count || 0;
            j.confidence = det > 0 ? Math.round((suc / det) * 100) : 0;
            j.pages = result.exam?.total_pages || j.pages;
            j.questions_count = result.questions?.length || j.questions_count;
        } catch (err) {
            toast(`Error loading result: ${err.message}`, 'error');
            return;
        }
    }

    S.result = result;
    S.questions = result.questions || [];

    // Show review nav
    document.getElementById('nav-review').style.display = 'flex';

    // Fill header
    document.getElementById('review-filename').textContent = j.filename || 'Unknown PDF';
    const idDisplay = j.exam_db_id ? `Exam #${j.exam_db_id}` : `#${jobId.slice(0, 6)}`;
    document.getElementById('review-meta').textContent =
        `${idDisplay} \u00b7 ${j.size ? fmtSize(j.size) : (result.exam?.file_size_bytes ? fmtSize(result.exam.file_size_bytes) : '0.0 B')} \u00b7 ${result.exam?.total_pages || 0} pages`;

    // Fill stats
    const v = result.validation || {};
    const detected = v.total_questions_detected || 0;
    const parsed = v.structured_successfully || 0;
    const trulyMissing = (v.missing_questions || []).length;
    const partialCount = (v.partially_structured || []).length;
    const noAnswerCount = (v.questions_missing_answer || []).length;
    const incomplete = trulyMissing + partialCount;
    const integrity = detected > 0 ? Math.round((parsed / detected) * 100) : 0;

    document.getElementById('rs-detected').textContent = detected;
    document.getElementById('rs-parsed').textContent = parsed;
    document.getElementById('rs-missing').textContent = incomplete;
    document.getElementById('rs-integrity').textContent = `${integrity}%`;

    // Update the card label based on what the issues actually are
    const missingLabel = document.getElementById('rs-missing-label');
    const missingDesc = document.getElementById('rs-missing-desc');
    if (missingLabel && missingDesc) {
        if (trulyMissing > 0 && partialCount > 0) {
            missingLabel.textContent = 'MISSING / INCOMPLETE';
            missingDesc.textContent = `${trulyMissing} lost, ${partialCount} partially structured`;
        } else if (trulyMissing > 0) {
            missingLabel.textContent = 'MISSING / LOST';
            missingDesc.textContent = 'Detected but failed to structure';
        } else if (partialCount > 0) {
            missingLabel.textContent = 'INCOMPLETE';
            missingDesc.textContent = 'Parsed but missing required fields';
        } else {
            missingLabel.textContent = 'MISSING / LOST';
            missingDesc.textContent = 'Detection but failed to structure';
        }
    }

    // Alert banner
    const alert = document.getElementById('review-alert');
    const alertText = document.getElementById('review-alert-text');
    const btnShowMissing = document.getElementById('btn-show-missing');
    const hasIssues = trulyMissing > 0 || partialCount > 0 || noAnswerCount > 0;

    if (trulyMissing > 0 && partialCount > 0) {
        alert.className = 'alert-banner alert-banner--warning';
        alertText.textContent = `\u26a0 INTEGRITY ALERT: ${trulyMissing} MISSING, ${partialCount} INCOMPLETE`;
        btnShowMissing.style.display = 'inline-flex';
        btnShowMissing.textContent = 'View Details';
    } else if (trulyMissing > 0) {
        alert.className = 'alert-banner alert-banner--warning';
        alertText.textContent = `\u26a0 INTEGRITY ALERT: ${trulyMissing} QUESTION${trulyMissing !== 1 ? 'S' : ''} MISSING`;
        btnShowMissing.style.display = 'inline-flex';
        btnShowMissing.textContent = 'View Details';
    } else if (partialCount > 0 || noAnswerCount > 0) {
        alert.className = 'alert-banner alert-banner--warning';
        alertText.textContent = `\u26a0 ${partialCount} INCOMPLETE QUESTION${partialCount !== 1 ? 'S' : ''}${noAnswerCount > 0 ? `, ${noAnswerCount} MISSING ANSWER${noAnswerCount !== 1 ? 'S' : ''}` : ''}`;
        btnShowMissing.style.display = 'inline-flex';
        btnShowMissing.textContent = 'View Details';
    } else if (integrity === 100) {
        alert.className = 'alert-banner alert-banner--success';
        alertText.textContent = `\u2713 ALL ${detected} QUESTIONS PARSED SUCCESSFULLY`;
        btnShowMissing.style.display = 'none';
    } else {
        alert.className = 'alert-banner alert-banner--hidden';
        btnShowMissing.style.display = 'none';
    }

    // Build missing questions panel data
    buildMissingPanel(v);

    // Reset detail
    document.getElementById('qdetail-content').classList.remove('qdetail-content--visible');
    document.getElementById('qdetail-empty').style.display = 'flex';
    S.selectedQ = null;

    // Show page
    showPage('review');

    // Render question list
    renderQList();
}

// ═══════════════════════════════════════════════════════════════════════════
// MISSING QUESTIONS PANEL
// ═══════════════════════════════════════════════════════════════════════════

function initMissingPanel() {
    // "View Details" button on alert banner
    document.getElementById('btn-show-missing').addEventListener('click', () => {
        const panel = document.getElementById('missing-panel');
        panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
    });

    // Close button
    document.getElementById('btn-close-missing').addEventListener('click', () => {
        document.getElementById('missing-panel').style.display = 'none';
    });

    // Tab switching
    document.querySelectorAll('.missing-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.missing-tab').forEach(t => t.classList.remove('missing-tab--active'));
            document.querySelectorAll('.missing-tab-content').forEach(c => c.classList.remove('missing-tab-content--active'));
            tab.classList.add('missing-tab--active');
            const target = tab.dataset.tab;
            document.getElementById(`content-${target}`).classList.add('missing-tab-content--active');
        });
    });
}

function buildMissingPanel(v) {
    const missingQuestions = v.missing_questions || [];
    const partiallyStructured = v.partially_structured || [];
    const noAnswer = v.questions_missing_answer || [];
    const sequenceGaps = v.sequence_gaps || [];
    const duplicates = v.duplicate_question_numbers || [];
    const rawDetected = v.total_questions_detected || 0;
    const fullyStructured = v.fully_structured_count || v.structured_successfully || 0;

    const totalIssues = missingQuestions.length + partiallyStructured.length;

    // Panel header count
    document.getElementById('missing-panel-count').textContent = totalIssues;

    // Summary stats
    document.getElementById('ms-raw-detected').textContent = rawDetected;
    document.getElementById('ms-fully-parsed').textContent = fullyStructured;
    document.getElementById('ms-partial').textContent = partiallyStructured.length;
    document.getElementById('ms-missing').textContent = missingQuestions.length;
    document.getElementById('ms-gaps').textContent = sequenceGaps.length;
    document.getElementById('ms-duplicates').textContent = duplicates.length;

    // Tab badges
    document.getElementById('tab-missing-count').textContent = missingQuestions.length;
    document.getElementById('tab-partial-count').textContent = partiallyStructured.length;
    document.getElementById('tab-no-answer-count').textContent = noAnswer.length;
    document.getElementById('tab-gaps-count').textContent = sequenceGaps.length;

    // ── Tab: Missing Questions ──
    const missingTbody = document.getElementById('missing-tbody');
    const missingEmpty = document.getElementById('missing-empty');
    if (missingQuestions.length === 0) {
        missingTbody.innerHTML = '';
        missingEmpty.style.display = 'flex';
    } else {
        missingEmpty.style.display = 'none';
        missingTbody.innerHTML = missingQuestions.map(mq => {
            const reasons = (mq.reason || 'Unknown').split(';').map(r => r.trim()).filter(Boolean);
            const reasonHtml = reasons.map(r => {
                let cls = 'red';
                if (r.toLowerCase().includes('header') || r.toLowerCase().includes('footer') || r.toLowerCase().includes('noise')) cls = 'amber';
                if (r.toLowerCase().includes('split') || r.toLowerCase().includes('page boundary')) cls = 'cyan';
                return `<span class="reason-tag reason-tag--${cls}">${esc(r)}</span>`;
            }).join(' ');
            return `
                <tr>
                    <td class="qnum-cell">Q${mq.question_number}</td>
                    <td class="page-cell">${mq.page_detected || '—'}</td>
                    <td class="reason-cell">${reasonHtml}</td>
                </tr>`;
        }).join('');
    }

    // ── Tab: Partially Structured ──
    const partialTbody = document.getElementById('partial-tbody');
    const partialEmpty = document.getElementById('partial-empty');
    if (partiallyStructured.length === 0) {
        partialTbody.innerHTML = '';
        partialEmpty.style.display = 'flex';
    } else {
        partialEmpty.style.display = 'none';
        partialTbody.innerHTML = partiallyStructured.map(pq => {
            const issues = (pq.reasons || []).map(r => {
                const label = r.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
                return `<span class="reason-tag reason-tag--amber">${esc(label)}</span>`;
            }).join(' ');
            return `
                <tr>
                    <td class="qnum-cell">Q${pq.question_number}</td>
                    <td class="page-cell">${pq.page_start || '—'}${pq.page_end && pq.page_end !== pq.page_start ? '-' + pq.page_end : ''}</td>
                    <td>${pq.has_question_text ? '<span class="cell-check">\u2713</span>' : '<span class="cell-cross">\u2717</span>'}</td>
                    <td>${pq.has_answer ? '<span class="cell-check">\u2713</span>' : '<span class="cell-cross">\u2717</span>'}</td>
                    <td>${pq.has_explanation ? '<span class="cell-check">\u2713</span>' : '<span class="cell-cross">\u2717</span>'}</td>
                    <td class="reason-cell">${issues || '<span style="color:var(--t4)">—</span>'}</td>
                </tr>`;
        }).join('');
    }

    // ── Tab: No Answer ──
    const noAnswerGrid = document.getElementById('no-answer-grid');
    const noAnswerEmpty = document.getElementById('no-answer-empty');
    if (noAnswer.length === 0) {
        noAnswerGrid.innerHTML = '';
        noAnswerEmpty.style.display = 'flex';
    } else {
        noAnswerEmpty.style.display = 'none';
        noAnswerGrid.innerHTML = noAnswer.map(n =>
            `<span class="numgrid-chip numgrid-chip--amber" title="Question ${n} is missing an answer">Q${n}</span>`
        ).join('');
    }

    // ── Tab: Sequence Gaps ──
    const gapsGrid = document.getElementById('gaps-grid');
    const gapsEmpty = document.getElementById('gaps-empty');
    if (sequenceGaps.length === 0) {
        gapsGrid.innerHTML = '';
        gapsEmpty.style.display = 'flex';
    } else {
        gapsEmpty.style.display = 'none';
        gapsGrid.innerHTML = sequenceGaps.map(n =>
            `<span class="numgrid-chip numgrid-chip--red" title="Question ${n} — not found in raw text or parsed output">Q${n}</span>`
        ).join('');
    }

    // Auto-show panel if there are missing questions
    if (missingQuestions.length > 0 || partiallyStructured.length > 0) {
        document.getElementById('missing-panel').style.display = 'block';
    } else {
        document.getElementById('missing-panel').style.display = 'none';
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// QUESTION LIST
// ═══════════════════════════════════════════════════════════════════════════

function renderQList() {
    const qs = filterQuestions();
    document.getElementById('qlist-count').textContent = `QUESTIONS (${qs.length})`;

    const body = document.getElementById('qlist-body');
    if (qs.length === 0) {
        body.innerHTML = '<div style="padding:24px;text-align:center;color:var(--t4);font-size:.85rem;">No matching questions</div>';
        return;
    }

    body.innerHTML = qs.map(q => {
        const score = q.anomaly_score || 0;
        const anomalies = q.anomalies || [];
        const hasAnswer = getSectionText(q, 'answer').length > 0;
        const hasExplanation = getSectionText(q, 'explanation').length > 0;

        // Badge
        let badgeCls = 'clean', badgeText = 'CLEAN';
        if (score > 30 || !hasAnswer) {
            badgeCls = 'error';
            badgeText = !hasAnswer ? 'MISSING ANSWER' : 'HIGH ANOMALY';
        } else if (score > 0 || anomalies.length > 0) {
            badgeCls = 'anomaly';
            badgeText = anomalies[0]?.type?.toUpperCase() || 'ANOMALY';
        }

        // Progress bar (completeness)
        let completeness = 33; // has question
        if (hasAnswer) completeness += 34;
        if (hasExplanation) completeness += 33;
        const progCls = completeness === 100 ? 'ok' : completeness >= 67 ? 'warn' : 'err';

        const active = S.selectedQ === q.question_number;
        const preview = getQuestionPreview(q);

        return `
            <div class="qitem ${active ? 'qitem--active' : ''}" onclick="selectQ(${q.question_number})" data-qn="${q.question_number}">
                <input type="checkbox" class="qitem__check" onclick="event.stopPropagation()">
                <div class="qitem__body">
                    <span class="qitem__badge qitem__badge--${badgeCls}">${badgeText}</span>
                    <div class="qitem__num">#${q.question_number}</div>
                    <div class="qitem__preview">Question: ${q.question_number} ${esc(preview)}</div>
                    <div class="qitem__progress">
                        <div class="qitem__progress-fill qitem__progress-fill--${progCls}" style="width:${completeness}%"></div>
                    </div>
                </div>
                <span class="qitem__score">s${score}</span>
            </div>`;
    }).join('');
}

function filterQuestions() {
    const search = document.getElementById('filter-search').value.toLowerCase();
    const status = document.getElementById('filter-status').value;
    const type = document.getElementById('filter-type').value;

    return S.questions.filter(q => {
        // Search
        if (search) {
            const text = getQuestionPreview(q).toLowerCase();
            if (!text.includes(search) && !String(q.question_number).includes(search)) return false;
        }

        // Status filter
        const hasAnswer = getSectionText(q, 'answer').length > 0;
        const hasExpl = getSectionText(q, 'explanation').length > 0;
        const score = q.anomaly_score || 0;

        if (status === 'clean' && (score > 0 || !hasAnswer)) return false;
        if (status === 'anomaly' && score === 0) return false;
        if (status === 'missing-answer' && hasAnswer) return false;
        if (status === 'missing-explanation' && hasExpl) return false;

        // Type filter
        const images = countImages(q);
        const multiPage = q.page_start !== q.page_end;

        if (type === 'with-images' && images === 0) return false;
        if (type === 'text-only' && images > 0) return false;
        if (type === 'multi-page' && !multiPage) return false;

        return true;
    });
}

function jumpToIncomplete() {
    const q = S.questions.find(q => {
        const hasAnswer = getSectionText(q, 'answer').length > 0;
        return !hasAnswer;
    });
    if (q) {
        selectQ(q.question_number);
        // Scroll to it
        const el = document.querySelector(`.qitem[data-qn="${q.question_number}"]`);
        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    } else {
        toast('All questions appear complete', 'info');
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// QUESTION DETAIL
// ═══════════════════════════════════════════════════════════════════════════

function selectQ(qn) {
    S.selectedQ = qn;
    const q = S.questions.find(q => q.question_number === qn);
    if (!q) return;

    // Highlight in list
    document.querySelectorAll('.qitem').forEach(el =>
        el.classList.toggle('qitem--active', parseInt(el.dataset.qn) === qn)
    );

    // Show detail
    document.getElementById('qdetail-empty').style.display = 'none';
    document.getElementById('qdetail-content').classList.add('qdetail-content--visible');

    // Header
    document.getElementById('qd-qnum').textContent = qn;
    document.getElementById('qd-line').innerHTML =
        `PAGE ${q.page_start}${q.page_end !== q.page_start ? '-' + q.page_end : ''} IN PDF • <span class="qdetail-mode">REVEAL_ONLY MODE</span>`;

    // Options & Cleaning
    const questionText = getSectionText(q, 'question');
    const optionsText = getSectionText(q, 'options');
    let options = [];
    let displayQuestion = questionText;

    if (optionsText) {
        // Use pre-parsed options from its own section
        options = extractOptions(optionsText);
    } else {
        // Fallback: extract and strip from question statement (legacy)
        options = extractOptions(questionText);
        displayQuestion = stripOptions(questionText, options);
    }

    document.getElementById('qd-question').value = displayQuestion;
    document.getElementById('qd-charcount').textContent = `${displayQuestion.length} chars`;

    // Answer
    const answerText = getSectionText(q, 'answer');
    // Strip label "Answer: B" -> "B"
    const cleanAnswer = answerText.replace(/^\s*(?:Correct\s+)?(?:Answer|Ans|Key)[\s.:]*/i, '').replace(/\.$/, '').trim();
    document.getElementById('qd-answer').value = cleanAnswer.slice(0, 100);

    // Explanation
    const explText = getSectionText(q, 'explanation');
    document.getElementById('qd-explanation').value = explText;

    // Render Media (section-scoped — never combined)
    renderMedia(q, 'question', 'qd-media-question');
    // Option images are rendered inline per-option inside renderOptionsUI
    clearMediaBox('qd-media-options');
    renderMedia(q, 'answer', 'qd-media-answer');
    renderMedia(q, 'explanation', 'qd-media-explanation');

    // Type
    document.getElementById('qd-type').value = (q.question_type || 'MCQ').toUpperCase();

    // Render options (with per-option images from blocks)
    renderOptionsUI(options, getSectionText(q, 'answer'), q);

    // Raw output
    renderRawOutput(q);

    // Anomalies
    const anomalies = q.anomalies || [];
    const anomBlock = document.getElementById('qd-anomalies-block');
    const anomList = document.getElementById('qd-anomalies');

    if (anomalies.length > 0) {
        anomBlock.style.display = 'block';
        anomList.innerHTML = anomalies.map(a => `
            <div class="anomaly-row">
                <span class="anomaly-sev">${a.severity || 'WARN'}</span>
                <span>${esc(a.type || 'unknown')}: ${esc(a.message || '')}</span>
            </div>`
        ).join('');
    } else {
        anomBlock.style.display = 'none';
    }
}

function extractOptions(text) {
    const optionRegex = /^[  ]*([A-Z])[.):\s]+(.+)/gm;
    const options = [];
    let m;
    while ((m = optionRegex.exec(text)) !== null) {
        let rawText = m[2].trim();
        // Remove trailing "Answer: X" if it leaked onto the same line
        rawText = rawText.replace(/\s*(?:Correct\s+)?(?:Answer|Ans|Key)[\s.:]*[A-Z].*$/i, '');
        options.push({ letter: m[1], text: rawText.trim(), raw: m[0] });
    }
    return options;
}

function stripOptions(text, options) {
    let cleaned = text;
    options.forEach(opt => {
        cleaned = cleaned.replace(opt.raw, '');
    });
    return cleaned.trim();
}

function renderOptionsUI(options, answerText = '', q = null) {
    const container = document.getElementById('qd-options');

    // Clean answer text for checking
    const cleanAnswer = answerText.replace(/^\s*(?:Correct\s+)?(?:Answer|Ans|Key)[\s.:]*/i, '').replace(/\.$/, '').trim();

    // If no options found, show placeholders
    if (options.length === 0) {
        const letters = ['A', 'B', 'C', 'D'];
        options = letters.map(l => ({
            letter: l,
            text: '',
            correct: cleanAnswer.includes(l)
        }));
    }

    // Associate images from blocks.options with individual option letters
    const optionImages = buildOptionImageMap(q);

    container.innerHTML = options.map(opt => {
        const imgs = optionImages[opt.letter] || [];
        const imagesHtml = imgs.length > 0
            ? `<div class="option-media">${imgs.map(src => {
                const imgSrc = imgSrcFromContent(src);
                return `<img src="${imgSrc}" class="q-thumbnail option-thumbnail" 
                             onclick="openLightbox('${imgSrc}', 'Option ${opt.letter}')" 
                             title="Option ${opt.letter} image">`;
            }).join('')}</div>`
            : '';

        return `
            <div class="option-item">
                <span class="option-letter">${opt.letter}</span>
                <div style="flex:1; min-width:0;">
                    <span class="option-text">${esc(opt.text) || '<em style="color:var(--t4)">Empty option</em>'}</span>
                    ${imagesHtml}
                </div>
                <input type="checkbox" class="option-check" ${opt.correct || cleanAnswer.includes(opt.letter) ? 'checked' : ''} title="Correct answer">
                <button class="option-remove" onclick="this.closest('.option-item').remove()" title="Remove">×</button>
            </div>
        `;
    }).join('');
}

/**
 * Build a map of option letter -> image paths from blocks.options.
 * Images are associated with the most recently seen option letter.
 * Applies proximity filtering on the options section first, then
 * deduplicates per-option.
 */
function buildOptionImageMap(q) {
    const map = {};
    if (!q || !q.blocks || !q.blocks.options) return map;

    // Pre-filter: only keep images that pass proximity check
    const filteredBlocks = getFilteredOptionBlocks(q.blocks.options);

    let currentLetter = null;
    const optLetterRegex = /^\s*([A-Z])[.):\s]/;
    const seenPerOption = {};

    for (const block of filteredBlocks) {
        if (block.type === 'text') {
            const m = optLetterRegex.exec(block.content);
            if (m) {
                currentLetter = m[1];
                if (!map[currentLetter]) {
                    map[currentLetter] = [];
                    seenPerOption[currentLetter] = new Set();
                }
            }
        } else if (block.type === 'image' && currentLetter) {
            const key = block.content.replace(/\\/g, '/');
            if (!seenPerOption[currentLetter]) {
                seenPerOption[currentLetter] = new Set();
                map[currentLetter] = [];
            }
            if (!seenPerOption[currentLetter].has(key)) {
                seenPerOption[currentLetter].add(key);
                map[currentLetter].push(block.content);
            }
        }
    }
    return map;
}

/**
 * Filter option blocks by per-text-block proximity.
 * Keeps all text blocks, but only images whose order_index is
 * within MARGIN of ANY text block’s order_index.
 */
function getFilteredOptionBlocks(blocks) {
    const MARGIN = 10;
    const textIndices = blocks
        .filter(b => b.type === 'text' && typeof b.order_index === 'number')
        .map(b => b.order_index);

    if (textIndices.length === 0) return blocks; // keep all if no text

    return blocks.filter(b => {
        if (b.type !== 'image') return true; // keep all non-image blocks
        const idx = b.order_index;
        if (typeof idx !== 'number') return true;
        return textIndices.some(ti => Math.abs(idx - ti) <= MARGIN);
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// IMAGE FILTERING — order_index proximity + deduplication
// ═══════════════════════════════════════════════════════════════════════════

/**
 * Filter images to only those that contextually belong to a section.
 *
 * Strategy:
 *   1. For each TEXT block, define a proximity window:
 *      [text.order_index - MARGIN, text.order_index + MARGIN].
 *   2. Keep only images whose order_index falls within ANY text
 *      block’s proximity window (per-block, NOT min/max range).
 *   3. Deduplicate by image path (same file shown once only).
 *
 * Per-block proximity avoids the problem where widely-spaced text
 * blocks create a huge range that captures unrelated images.
 */
function getFilteredSectionImages(blocks) {
    const MARGIN = 10; // order_index tolerance around each text block

    const images = blocks.filter(b => b.type === 'image');
    if (images.length === 0) return [];

    // Gather text block order_indices
    const textIndices = blocks
        .filter(b => b.type === 'text' && typeof b.order_index === 'number')
        .map(b => b.order_index);

    let filtered;
    if (textIndices.length > 0) {
        filtered = images.filter(img => {
            const idx = img.order_index;
            if (typeof idx !== 'number') return true; // keep if no index data
            // Image must be within MARGIN of ANY text block
            return textIndices.some(ti => Math.abs(idx - ti) <= MARGIN);
        });
    } else {
        // No text in section — keep all images (dedup will still apply)
        filtered = images;
    }

    // Deduplicate by content path, then cap to prevent UI flooding
    const MAX_SECTION_IMAGES = 15;
    const deduped = dedupeImageBlocks(filtered);
    return deduped.slice(0, MAX_SECTION_IMAGES);
}

/** Remove duplicate image blocks (same content path). */
function dedupeImageBlocks(images) {
    const seen = new Set();
    return images.filter(img => {
        const key = img.content.replace(/\\/g, '/');
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

/** Convert an image content path to a root-relative URL. */
function imgSrcFromContent(content) {
    let c = content.replace(/\\/g, '/');
    let src;
    if (c.startsWith('questions/') || c.startsWith('storage/') || c.startsWith('output/')) {
        src = '/' + c;
    } else {
        src = '/questions/' + c;
    }
    return src.replace(/\/+/g, '/');
}

/**
 * Render images for a specific section only.
 * Images are proximity-filtered + deduplicated so that only
 * contextually relevant images appear under their section.
 */
function renderMedia(q, section, containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const blocks = (q.blocks && q.blocks[section]) || [];
    const images = getFilteredSectionImages(blocks);

    if (images.length === 0) {
        container.style.display = 'none';
        container.innerHTML = '';
        return;
    }

    container.style.display = 'flex';
    container.innerHTML = images.map(img => {
        const src = imgSrcFromContent(img.content);
        return `<img src="${src}" class="q-thumbnail" 
                     onclick="openLightbox('${src}', 'Question ${q.question_number} — ${section.toUpperCase()}')" 
                     title="Click to preview">`;
    }).join('');
}

/** Hide a media box container (used to clear the combined options grid). */
function clearMediaBox(containerId) {
    const container = document.getElementById(containerId);
    if (container) {
        container.style.display = 'none';
        container.innerHTML = '';
    }
}

function addOption() {
    const container = document.getElementById('qd-options');
    const count = container.children.length;
    const letter = String.fromCharCode(65 + count);

    const div = document.createElement('div');
    div.className = 'option-item';
    div.innerHTML = `
        <span class="option-letter">${letter}</span>
        <div style="flex:1; min-width:0;">
            <input type="text" class="field-input" placeholder="Option text..." style="padding:4px 8px;font-size:.82rem">
        </div>
        <input type="checkbox" class="option-check" title="Correct answer">
        <button class="option-remove" onclick="this.closest('.option-item').remove()" title="Remove">×</button>
    `;
    container.appendChild(div);
}

function renderRawOutput(q) {
    const raw = document.getElementById('qd-raw');
    let text = '';

    if (q.blocks) {
        for (const [section, blocks] of Object.entries(q.blocks)) {
            if (blocks && blocks.length > 0) {
                text += `[${section.toUpperCase()}]\n`;
                for (const b of blocks) {
                    if (b.type === 'text') {
                        text += b.content + '\n';
                    } else if (b.type === 'image') {
                        text += `[IMAGE: ${b.content}]\n`;
                    }
                }
                text += '\n';
            }
        }
    }

    raw.textContent = text || 'No raw data available';
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

function getQuestionPreview(q) {
    if (!q.blocks || !q.blocks.question) return '';
    return q.blocks.question
        .filter(b => b.type === 'text')
        .map(b => b.content)
        .join('\n')
        .trim();
}

function getSectionText(q, section) {
    if (!q.blocks || !q.blocks[section]) return '';
    return q.blocks[section]
        .filter(b => b.type === 'text')
        .map(b => b.content)
        .join('\n')
        .trim();
}

function countImages(q) {
    if (!q.blocks) return 0;
    let count = 0;
    for (const blocks of Object.values(q.blocks)) {
        if (Array.isArray(blocks)) {
            count += getFilteredSectionImages(blocks).length;
        }
    }
    return count;
}

function esc(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.appendChild(document.createTextNode(str));
    return d.innerHTML;
}

function fmtSize(bytes) {
    if (!bytes) return '0.0 B';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1048576).toFixed(1) + ' MB';
}

function fmtAgo(iso) {
    if (!iso) return '—';
    try {
        const d = new Date(iso);
        const now = new Date();
        const diff = Math.floor((now - d) / 1000);
        if (diff < 60) return 'just now';
        if (diff < 3600) return `${Math.floor(diff / 60)} minutes ago`;
        if (diff < 86400) return `${Math.floor(diff / 3600)} hours ago`;
        return `${Math.floor(diff / 86400)} days ago`;
    } catch {
        return '—';
    }
}

function toast(msg, type = 'info') {
    const rack = document.getElementById('toast-rack');
    const el = document.createElement('div');
    el.className = `toast toast--${type}`;
    el.textContent = msg;
    rack.appendChild(el);

    setTimeout(() => {
        el.classList.add('toast--out');
        setTimeout(() => el.remove(), 250);
    }, 4000);
}
