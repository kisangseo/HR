const state = {
  applicants: []
};

const els = {
  nameFilter: document.getElementById('nameFilter'),
  dateRangeFilter: document.getElementById('dateRangeFilter'),
  datePicker: document.getElementById('datePicker'),
  jobTitleFilter: document.getElementById('jobTitleFilter'),
  statusFilter: document.getElementById('statusFilter'),
  clearFiltersBtn: document.getElementById('clearFiltersBtn'),
  applicantRows: document.getElementById('applicantRows')
};

const dateRangeState = {
  from: '',
  to: ''
};

els.nameFilter.addEventListener('input', loadApplicants);
els.dateRangeFilter.addEventListener('click', openDatePicker);
els.datePicker.addEventListener('change', handleDateSelection);
els.jobTitleFilter.addEventListener('change', loadApplicants);
els.statusFilter.addEventListener('change', loadApplicants);

els.clearFiltersBtn.addEventListener('click', () => {
  els.nameFilter.value = '';
  dateRangeState.from = '';
  dateRangeState.to = '';
  renderDateRangeFilter();
  els.jobTitleFilter.value = '';
  els.statusFilter.value = '';
  loadApplicants();
});

function openDatePicker() {
  // reset so selecting the same date twice still triggers change event
  els.datePicker.value = '';
  if (typeof els.datePicker.showPicker === 'function') {
    els.datePicker.showPicker();
  } else {
    els.datePicker.focus();
    els.datePicker.click();
  }
}

function handleDateSelection() {
  const selected = (els.datePicker.value || '').trim();
  if (!selected) return;

  if (!dateRangeState.from || dateRangeState.to) {
    dateRangeState.from = selected;
    dateRangeState.to = '';
    renderDateRangeFilter(true);
    openDatePicker();
    return;
  }

  if (selected < dateRangeState.from) {
    dateRangeState.to = dateRangeState.from;
    dateRangeState.from = selected;
  } else {
    dateRangeState.to = selected;
  }
  renderDateRangeFilter();
  loadApplicants();
}

function renderDateRangeFilter(awaitingSecond = false) {
  if (!dateRangeState.from && !dateRangeState.to) {
    els.dateRangeFilter.value = '';
    return;
  }
  if (awaitingSecond && dateRangeState.from && !dateRangeState.to) {
    els.dateRangeFilter.value = `${formatDate(dateRangeState.from)} → pick end date`;
    return;
  }
  const toValue = dateRangeState.to || dateRangeState.from;
  els.dateRangeFilter.value = `${formatDate(dateRangeState.from)} - ${formatDate(toValue)}`;
}

async function loadApplicants() {
  const dateFrom = dateRangeState.from;
  const dateTo = dateRangeState.to || dateRangeState.from;
  const params = new URLSearchParams({
    name: els.nameFilter.value.trim(),
    date_from: dateFrom,
    date_to: dateTo,
    job_title: els.jobTitleFilter.value.trim(),
    status: els.statusFilter.value.trim()
  });

  const response = await fetch(`/api/applicants?${params.toString()}`);
  const payload = await response.json();
  state.applicants = payload.applicants || [];
  renderTable(state.applicants);
}

function renderTable(applicants) {
  if (!applicants.length) {
    els.applicantRows.innerHTML = '<tr><td colspan="8">No applicants found.</td></tr>';
    return;
  }

  els.applicantRows.innerHTML = applicants
    .map((applicant) => {
      const primary = shortenPosition(cleanDisplayPosition(applicant.primaryPosition || '—'));
      const other = applicant.otherPositions?.length
        ? applicant.otherPositions
            .map((value) => shortenPosition(cleanDisplayPosition(value)))
            .filter((value) => value && value !== '—')
            .join(', ')
        : '—';
      return `<tr>
        <td>${escapeHtml(applicant.name)}</td>
        <td>${formatDate(applicant.submittedAt)}</td>
        <td>${escapeHtml(primary)}</td>
        <td>${escapeHtml(other)}</td>
        <td>${escapeHtml(applicant.status || '—')}</td>
        <td>${escapeHtml(applicant.email || '—')}</td>
        <td>${escapeHtml(applicant.phone || '—')}</td>
        <td>${renderPdfLink(applicant.cognitoPdfUrl || applicant.cognitoDocumentLink)}</td>
      </tr>`;
    })
    .join('');
}


function renderPdfLink(url) {
  const text = String(url || '').trim();
  if (!text) return '—';
  return `<a href="${escapeHtml(text)}" target="_blank" rel="noopener noreferrer">Download PDF</a>`;
}
function cleanDisplayPosition(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  return text
    .replace(/\s*sent from the baltimore city sheriff[’']?s office.*$/i, '')
    .trim()
    .replace(/[,\-;]+$/, '')
    .trim();
}

function shortenPosition(value) {
  const text = String(value || '').trim();
  const key = text.toLowerCase();
  if (key === 'court security officer') return 'CSO';
  if (key === 'deputy sheriff') return 'Deputy';
  if (key === 'radio dispatcher') return 'Radio';
  if (key === 'information technology') return 'IT';
  return text || '—';
}

function formatDate(value) {
  const text = String(value || '').trim();
  const isoDateOnlyMatch = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoDateOnlyMatch) {
    const [, year, month, day] = isoDateOnlyMatch;
    return `${Number(month)}/${Number(day)}/${year}`;
  }

  const date = new Date(text);
  if (Number.isNaN(date.getTime())) {
    return value || '—';
  }
  return date.toLocaleDateString('en-US');
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

async function loadJobTitles() {
  const response = await fetch('/api/job-titles');
  const payload = await response.json();
  const titles = payload.job_titles || [];
  const existing = new Set(
    Array.from(els.jobTitleFilter.options).map((option) => option.value.toLowerCase())
  );
  for (const title of titles) {
    const text = String(title || '').trim();
    if (!text || existing.has(text.toLowerCase())) continue;
    const option = document.createElement('option');
    option.value = text;
    option.textContent = text;
    els.jobTitleFilter.appendChild(option);
  }
}


async function loadStatuses() {
  const response = await fetch('/api/statuses');
  const payload = await response.json();
  const statuses = payload.statuses || [];
  const existing = new Set(Array.from(els.statusFilter.options).map((option) => option.value.toLowerCase()));
  for (const status of statuses) {
    const text = String(status || '').trim();
    if (!text || existing.has(text.toLowerCase())) continue;
    const option = document.createElement('option');
    option.value = text;
    option.textContent = text;
    els.statusFilter.appendChild(option);
  }
}

Promise.all([loadJobTitles(), loadStatuses(), loadApplicants()]).catch((error) => console.error(error));
