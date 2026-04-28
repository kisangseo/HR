const state = {
  applicants: []
};

const els = {
  nameFilter: document.getElementById('nameFilter'),
  dateFromFilter: document.getElementById('dateFromFilter'),
  dateToFilter: document.getElementById('dateToFilter'),
  jobTitleFilter: document.getElementById('jobTitleFilter'),
  clearFiltersBtn: document.getElementById('clearFiltersBtn'),
  applicantRows: document.getElementById('applicantRows')
};

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
      const primary = shortenPosition(applicant.primaryPosition || '—');
      const other = applicant.otherPositions?.length
        ? applicant.otherPositions.map(shortenPosition).join(', ')
        : '—';
      return `<tr>
        <td>${escapeHtml(applicant.name)}</td>
        <td>${formatDate(applicant.submittedAt)}</td>
        <td>${escapeHtml(primary)}</td>
        <td>${escapeHtml(other)}</td>
        <td>${escapeHtml(applicant.email || '—')}</td>
        <td>${escapeHtml(applicant.phone || '—')}</td>
      </tr>`;
    })
    .join('');
}

function shortenPosition(value) {
  const text = String(value || '').trim();
  const key = text.toLowerCase();
  if (key === 'court security officer') return 'CSO';
  if (key === 'deputy sheriff') return 'Deputy';
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

loadApplicants().catch((error) => console.error(error));
