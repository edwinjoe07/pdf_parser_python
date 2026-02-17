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
    initLightbox();
    checkHealth();
    setInterval(checkHealth, 12000);
});

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
        const res = await fetch(`${API}/api/parse`, { method: 'POST', body: fd });
        if (!res.ok) throw new Error((await res.json()).error || 'Upload failed');

        const data = await res.json();
        toast(`Parse started: ${S.uploadFile.name}`, 'success');

        S.jobs[data.job_id] = {
            id: data.job_id,
            filename: S.uploadFile.name,
            size: S.uploadFile.size,
            status: 'queued',
            created_at: new Date().toISOString(),
            questions_count: null,
            confidence: null,
            pages: null,
        };

        startPoll(data.job_id);
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
                <button class="alink alink--reparse" onclick="reParse('${id}')">Re-parse</button>
                <button class="alink alink--logs" onclick="showLogs('${id}')">Logs</button>
                <button class="alink alink--delete" onclick="deleteJob('${id}')">Delete</button>`;
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

        rows += `
            <tr>
                <td class="fid">${id.slice(0, 6)}</td>
                <td class="fname">${esc(j.filename || 'unknown.pdf')}</td>
                <td>${j.size ? fmtSize(j.size) : '—'}</td>
                <td>${j.pages || '—'}</td>
                <td><span class="sbadge sbadge--${statusKey}"><span class="sbadge__dot"></span> ${statusLabel}</span></td>
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
    const pending = all.filter(j => j.status === 'queued').length;
    const active = all.filter(j => j.status === 'processing').length;
    const done = all.filter(j => j.status === 'parsed' || j.status === 'completed').length;

    document.getElementById('stat-pending').textContent = `${pending} File${pending !== 1 ? 's' : ''}`;
    document.getElementById('stat-active').textContent = `${active} Active`;
    document.getElementById('stat-done').textContent = `${done} Total`;
}

// ═══════════════════════════════════════════════════════════════════════════
// TABLE ACTIONS
// ═══════════════════════════════════════════════════════════════════════════

function deleteJob(jobId) {
    if (!confirm('Delete this parse job?')) return;
    delete S.jobs[jobId];
    if (S.pollTimers[jobId]) {
        clearInterval(S.pollTimers[jobId]);
        delete S.pollTimers[jobId];
    }
    renderImportsTable();
    updateStatusCards();
    toast('Job deleted', 'info');
}

async function reParse(jobId) {
    const j = S.jobs[jobId];
    if (!j) return;
    toast('Re-parse: Please upload the file again', 'warning');
    document.getElementById('upload-modal').style.display = 'flex';
    resetUploadModal();
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
            const r = await fetch(`${API}/api/result/${jobId}`);
            if (!r.ok) throw new Error('Failed to fetch');
            result = await r.json();
            S.jobs[jobId]._result = result;
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
    document.getElementById('review-meta').textContent =
        `ID #${jobId.slice(0, 6)} · ${j.size ? fmtSize(j.size) : '0.0 B'} · ${result.exam?.total_pages || 0} pages`;

    // Fill stats
    const v = result.validation || {};
    const detected = v.total_questions_detected || 0;
    const parsed = v.structured_successfully || 0;
    const missing = detected - parsed;
    const integrity = detected > 0 ? Math.round((parsed / detected) * 100) : 0;

    document.getElementById('rs-detected').textContent = detected;
    document.getElementById('rs-parsed').textContent = parsed;
    document.getElementById('rs-missing').textContent = missing;
    document.getElementById('rs-integrity').textContent = `${integrity}%`;

    // Alert banner
    const alert = document.getElementById('review-alert');
    if (missing > 0) {
        alert.className = 'alert-banner alert-banner--warning';
        alert.innerHTML = `⚠ INTEGRITY ALERT: ${missing} QUESTIONS MISSING`;
    } else if (integrity === 100) {
        alert.className = 'alert-banner alert-banner--success';
        alert.innerHTML = `✓ ALL ${detected} QUESTIONS PARSED SUCCESSFULLY`;
    } else {
        alert.className = 'alert-banner alert-banner--hidden';
    }

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
    const questionText = getQuestionPreview(q);
    const options = extractOptions(questionText);
    const cleanedQuestion = stripOptions(questionText, options);

    document.getElementById('qd-question').value = cleanedQuestion;
    document.getElementById('qd-charcount').textContent = `${cleanedQuestion.length} chars`;

    // Answer
    const answerText = getSectionText(q, 'answer');
    document.getElementById('qd-answer').value = answerText.slice(0, 100);

    // Explanation
    const explText = getSectionText(q, 'explanation');
    document.getElementById('qd-explanation').value = explText;

    // Render Media
    renderMedia(q, 'question', 'qd-media-question');
    renderMedia(q, 'answer', 'qd-media-answer');
    renderMedia(q, 'explanation', 'qd-media-explanation');

    // Type
    document.getElementById('qd-type').value = (q.question_type || 'MCQ').toUpperCase();

    // Render options
    renderOptionsUI(options, getSectionText(q, 'answer'));

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
    const optionRegex = /^[  ]*([A-F])[.):\s]+(.+)/gm;
    const options = [];
    let m;
    while ((m = optionRegex.exec(text)) !== null) {
        options.push({ letter: m[1], text: m[2].trim(), raw: m[0] });
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

function renderOptionsUI(options, answerText = '') {
    const container = document.getElementById('qd-options');

    // If no options found, show placeholders
    if (options.length === 0) {
        const letters = ['A', 'B', 'C', 'D'];
        options = letters.map(l => ({
            letter: l,
            text: '',
            correct: answerText.includes(l)
        }));
    }

    container.innerHTML = options.map(opt => `
        <div class="option-item">
            <span class="option-letter">${opt.letter}</span>
            <div style="flex:1; min-width:0;">
                <span class="option-text">${esc(opt.text) || '<em style="color:var(--t4)">Empty option</em>'}</span>
            </div>
            <input type="checkbox" class="option-check" ${opt.correct || answerText.includes(opt.letter) ? 'checked' : ''} title="Correct answer">
            <button class="option-remove" onclick="this.closest('.option-item').remove()" title="Remove">×</button>
        </div>
    `).join('');
}

function renderMedia(q, section, containerId) {
    const container = document.getElementById(containerId);
    if (!container) return;

    const blocks = (q.blocks && q.blocks[section]) || [];
    const images = blocks.filter(b => b.type === 'image');

    if (images.length === 0) {
        container.style.display = 'none';
        container.innerHTML = '';
        return;
    }

    container.style.display = 'flex';
    container.innerHTML = images.map(img => {
        // Path normalization: replace backslashes (Windows legacy) and ensure single leading slash
        const src = `/${img.content}`.replace(/\\/g, '/').replace(/\/+/g, '/');
        return `<img src="${src}" class="q-thumbnail" 
                     onclick="openLightbox('${src}', 'Question ${q.question_number} — ${section.toUpperCase()}')"
                     title="Click to preview">`;
    }).join('');
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
            count += blocks.filter(b => b.type === 'image').length;
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
