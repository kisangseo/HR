const state = {
  applicants: []
};

const els = {
  csvFile: document.getElementById('csvFile'),
  loadCsvBtn: document.getElementById('loadCsvBtn'),
  clearApplicantsBtn: document.getElementById('clearApplicantsBtn'),
  ingestStatus: document.getElementById('ingestStatus'),
  ingestDetails: document.getElementById('ingestDetails'),
  nameFilter: document.getElementById('nameFilter'),
  dateFromFilter: document.getElementById('dateFromFilter'),
  dateToFilter: document.getElementById('dateToFilter'),
  jobTitleFilter: document.getElementById('jobTitleFilter'),
  clearFiltersBtn: document.getElementById('clearFiltersBtn'),
  applicantRows: document.getElementById('applicantRows')
};

els.loadCsvBtn.addEventListener('click', async () => {
  const file = els.csvFile.files?.[0];
  if (!file) {
    setStatus('Choose a CSV file first.', true);
    return;
  }

  try {
    const text = await file.text();
    const response = await fetch('/api/ingest-csv', {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
      body: text
    });

    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || 'Ingest failed.');
    }

    await loadApplicants();
    const parsedRows = Number.isFinite(result.parsed_rows)
      ? result.parsed_rows
      : (result.inserted || 0) + (result.skipped || 0);

    setStatus(
      `Ingest complete. Inserted ${result.inserted || 0}, skipped ${result.skipped || 0}, parsed ${parsedRows}.`,
      false
    );
    renderIngestDetails(result);
  } catch (error) {
    setStatus(error.message, true);
    els.ingestDetails.textContent = '';
  }
});

els.clearApplicantsBtn.addEventListener('click', async () => {
  const confirmed = window.confirm('Clear all applicants from this local app database?');
  if (!confirmed) {
    return;
  }

  try {
    const response = await fetch('/api/clear-applicants', { method: 'POST' });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || 'Could not clear applicants.');
    }

    await loadApplicants();
    setStatus(`Cleared ${result.deleted || 0} applicants from local SQLite.`, false);
    els.ingestDetails.textContent = 'Tip: This clears the local hr.db used by python3 app.py.';
  } catch (error) {
    setStatus(error.message, true);
  }
});

els.nameFilter.addEventListener('input', loadApplicants);
els.dateFromFilter.addEventListener('change', loadApplicants);
els.dateToFilter.addEventListener('change', loadApplicants);
els.jobTitleFilter.addEventListener('input', loadApplicants);

els.clearFiltersBtn.addEventListener('click', () => {
  els.nameFilter.value = '';
  els.dateFromFilter.value = '';
  els.dateToFilter.value = '';
  els.jobTitleFilter.value = '';
  loadApplicants();
});

async function loadApplicants() {
  const params = new URLSearchParams({
    name: els.nameFilter.value.trim(),
    date_from: els.dateFromFilter.value,
    date_to: els.dateToFilter.value,
    job_title: els.jobTitleFilter.value.trim()
  });

  const response = await fetch(`/api/applicants?${params.toString()}`);
  const payload = await response.json();
  state.applicants = payload.applicants || [];
  renderTable(state.applicants);
}

function renderTable(applicants) {
  if (!applicants.length) {
    els.applicantRows.innerHTML = '<tr><td colspan="6">No applicants found.</td></tr>';
    return;
  }

  els.applicantRows.innerHTML = applicants
    .map((applicant) => {
      const other = applicant.otherPositions?.length ? applicant.otherPositions.join(', ') : '—';
      return `<tr>
        <td>${escapeHtml(applicant.name)}</td>
        <td>${formatDate(applicant.submittedAt)}</td>
        <td>${escapeHtml(applicant.primaryPosition || '—')}</td>
        <td>${escapeHtml(other)}</td>
        <td>${escapeHtml(applicant.email || '—')}</td>
        <td>${escapeHtml(applicant.phone || '—')}</td>
      </tr>`;
    })
    .join('');
}

function formatDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value || '—';
  }
  return date.toLocaleString();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function setStatus(message, isError) {
  els.ingestStatus.textContent = message;
  els.ingestStatus.classList.toggle('error', isError);
  els.ingestStatus.classList.toggle('ok', !isError);
}

function renderIngestDetails(result) {
  const lines = [];
  if (!('parsed_rows' in result) || !('issues' in result) || !('detected_headers' in result)) {
    lines.push(
      'Warning: API response is missing diagnostics fields. You may be running an older server process.'
    );
    lines.push('Please stop and restart the app with: python3 app.py');
    lines.push('');
  }

  lines.push(`App version: ${result.app_version || 'unknown'}`);
  lines.push(`Detected delimiter: ${result.detected_delimiter || 'unknown'}`);
  lines.push(`Detected headers (${(result.detected_headers || []).length} shown):`);
  lines.push((result.detected_headers || []).join(', ') || '(none)');

  const issues = result.issues || [];
  const summary = result.issue_summary || {};
  const summaryEntries = Object.entries(summary);
  if (summaryEntries.length) {
    lines.push('\nTop issue summary:');
    for (const [message, count] of summaryEntries) {
      lines.push(`- ${count}x ${message}`);
    }
  }

  if (!issues.length) {
    lines.push('\\nNo ingest issues reported.');
  } else {
    const totalIssues = result.issue_count ?? issues.length;
    const maxToShow = 40;
    lines.push(`\\nIssues / warnings (showing ${Math.min(issues.length, maxToShow)} of ${totalIssues}):`);
    for (const issue of issues.slice(0, maxToShow)) {
      const details = (issue.details || []).join(' | ');
      lines.push(`- row ${issue.row}: ${issue.reason}${details ? ` -> ${details}` : ''}`);
    }
    if (totalIssues > maxToShow) {
      lines.push(`... ${totalIssues - maxToShow} more issue rows not shown.`);
    }
  }

  els.ingestDetails.textContent = lines.join('\\n');
}

loadApplicants().catch((error) => setStatus(error.message, true));
