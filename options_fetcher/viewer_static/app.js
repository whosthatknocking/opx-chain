const state = {
  files: [],
  rows: [],
  columns: [],
  summary: null,
  selectedFile: null,
  sortColumn: null,
  sortDirection: 'asc',
  columnFilters: {},
  activeFilterColumn: null,
  filterSearchTerm: '',
  currentPage: 1,
  pageSize: 100,
  columnWidths: {},
  selectedRow: null,
};

const elements = {
  fileSelect: document.getElementById('fileSelect'),
  rowCount: document.getElementById('rowCount'),
  pageSizeSelect: document.getElementById('pageSizeSelect'),
  freshnessSummary: document.getElementById('freshnessSummary'),
  datasetCards: document.getElementById('datasetCards'),
  dataTable: document.getElementById('dataTable'),
  tableHead: document.querySelector('#dataTable thead'),
  tableBody: document.querySelector('#dataTable tbody'),
  tableStatus: document.getElementById('tableStatus'),
  prevPageButton: document.getElementById('prevPageButton'),
  nextPageButton: document.getElementById('nextPageButton'),
  pageInfo: document.getElementById('pageInfo'),
  filterPopover: document.getElementById('filterPopover'),
  filterPopoverTitle: document.getElementById('filterPopoverTitle'),
  filterSearchWrap: document.getElementById('filterSearchWrap'),
  filterValueSearch: document.getElementById('filterValueSearch'),
  filterRangeWrap: document.getElementById('filterRangeWrap'),
  filterMinValue: document.getElementById('filterMinValue'),
  filterMaxValue: document.getElementById('filterMaxValue'),
  filterOptionList: document.getElementById('filterOptionList'),
  clearFilterButton: document.getElementById('clearFilterButton'),
  rowModal: document.getElementById('rowModal'),
  rowModalTitle: document.getElementById('rowModalTitle'),
  rowModalMeta: document.getElementById('rowModalMeta'),
  rowDetailGrid: document.getElementById('rowDetailGrid'),
  closeRowModalButton: document.getElementById('closeRowModalButton'),
  tabButtons: [...document.querySelectorAll('.tab-button')],
  summaryTab: document.getElementById('summaryTab'),
  summaryContent: document.getElementById('summaryContent'),
  summaryStatus: document.getElementById('summaryStatus'),
  summaryHighlights: document.getElementById('summaryHighlights'),
  summaryTickerGrid: document.getElementById('summaryTickerGrid'),
  tableTab: document.getElementById('tableTab'),
  readmeTab: document.getElementById('readmeTab'),
  readmeContent: document.getElementById('readmeContent'),
  themeToggle: document.getElementById('themeToggle'),
};

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function formatCell(value) {
  if (value === null || value === undefined || value === '') return '—';
  return String(value);
}

function compareValues(left, right) {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  const leftNumeric = Number.isFinite(leftNumber);
  const rightNumeric = Number.isFinite(rightNumber);

  if (leftNumeric && rightNumeric) return leftNumber - rightNumber;
  return String(left ?? '').localeCompare(String(right ?? ''), undefined, { numeric: true, sensitivity: 'base' });
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined || !Number.isFinite(Number(seconds))) return '—';
  const value = Math.max(0, Math.round(Number(seconds)));
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.round(value / 60)}m`;
  if (value < 86400) return `${Math.round(value / 3600)}h`;
  return `${Math.round(value / 86400)}d`;
}

function formatDatasetValue(card) {
  if (!card) return '—';
  if (card.name === 'vix_level' || card.name === 'risk_free_rate_used') {
    const number = Number(card.value);
    if (Number.isFinite(number)) {
      return card.name === 'risk_free_rate_used' ? `${(number * 100).toFixed(2)}%` : number.toFixed(2);
    }
  }
  return formatCell(card.value);
}

function renderFreshnessSummary(summary) {
  if (!summary) {
    elements.freshnessSummary.innerHTML = '';
    return;
  }

  elements.freshnessSummary.innerHTML = `
    <article class="freshness-card">
      ${renderFieldLabel('File Age', 'freshness-label')}
      <strong>${escapeHtml(formatDuration(summary.file_age_seconds))}</strong>
      <span class="freshness-detail">${escapeHtml(summary.file_modified_at || '—')}</span>
    </article>
    <article class="freshness-card">
      ${renderFieldLabel('Option Quotes', 'freshness-label')}
      <strong>${escapeHtml(formatDuration(summary.option_quote_age_median_seconds))}</strong>
      <span class="freshness-detail">median · max ${escapeHtml(formatDuration(summary.option_quote_age_max_seconds))}</span>
    </article>
    <article class="freshness-card">
      ${renderFieldLabel('Underlying', 'freshness-label')}
      <strong>${escapeHtml(formatDuration(summary.underlying_quote_age_median_seconds))}</strong>
      <span class="freshness-detail">median · max ${escapeHtml(formatDuration(summary.underlying_quote_age_max_seconds))}</span>
    </article>
  `;
}

function renderDatasetCards(cards) {
  if (!cards || cards.length === 0) {
    elements.datasetCards.innerHTML = '';
    return;
  }

  elements.datasetCards.innerHTML = cards.map((card) => `
    <article class="freshness-card">
      ${renderFieldLabel(card.name, 'freshness-label')}
      <strong title="${escapeHtml(card.description || '')}">${escapeHtml(formatDatasetValue(card))}</strong>
    </article>
  `).join('');
}

function formatNumber(value, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return Number(value).toFixed(digits);
}

function formatSignedPercent(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  const number = Number(value);
  const sign = number > 0 ? '+' : '';
  return `${sign}${number.toFixed(1)}%`;
}

function getFieldDescription(label) {
  const summaryDescriptions = {
    'File Age': 'How long ago the selected CSV file was written.',
    'Option Quotes': 'Age of option quotes in the file, shown as median and max quote_age_seconds.',
    Underlying: 'Age of the underlying price snapshots in the file, shown as median and max underlying_price_age_seconds.',
    Rows: 'Number of option records currently visible after active filters are applied.',
    'Latest Status': 'Compact status derived from the latest underlying day move and the relationship between implied volatility and historical volatility.',
    'IV / HV': 'Ratio of implied volatility to historical volatility. Values above 1 mean options are priced richer than recent realized volatility.',
    'Best ROM': 'Highest return_on_margin_annualized among candidate contracts for this ticker.',
    'Moderate ROM': 'return_on_margin_annualized for the selected moderate-risk candidate for this ticker.',
    'Calls / Puts': 'Count of call and put option rows available for this underlying symbol.',
    'Most Profitable': 'Heuristic pick for the highest annualized return on margin among candidate contracts.',
    'Moderate Risk': 'Heuristic pick balancing return on margin with lower ITM probability, wider distance from spot, and tighter spread.',
  };
  const columnDescription = getColumnDefinition(label)?.description;
  return columnDescription || summaryDescriptions[label] || '';
}

function renderFieldLabel(label, className = '') {
  const description = getFieldDescription(label);
  const titleAttribute = description ? ` title="${escapeHtml(description)}"` : '';
  const cssClass = className ? `field-label-tooltip ${className}` : 'field-label-tooltip';
  return `<span class="${cssClass}"${titleAttribute}>${escapeHtml(label)}</span>`;
}

function metricToneClass(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '';
  return Number(value) < 0 ? 'negative' : 'positive';
}

function renderOpportunityCard(title, opportunity, tone = 'default') {
  if (!opportunity) {
    return `
      <article class="opportunity-card opportunity-card-${tone}">
        ${renderFieldLabel(title, 'opportunity-label')}
        <strong>No candidate</strong>
        <span class="opportunity-detail">No row matched the current heuristic.</span>
      </article>
    `;
  }
  return `
    <article class="opportunity-card opportunity-card-${tone}">
      ${renderFieldLabel(title, 'opportunity-label')}
      <strong>${escapeHtml(`${opportunity.option_type} ${formatNumber(opportunity.strike, 2)} · ${opportunity.expiration_date}`)}</strong>
      <span class="opportunity-detail">${escapeHtml(opportunity.summary || 'No summary available.')}</span>
    </article>
  `;
}

function renderSummaryHighlights(highlights) {
  const cards = [];
  const profitable = highlights?.most_profitable;
  const moderate = highlights?.moderate_risk;
  if (profitable?.profitable_opportunity) {
    cards.push(`
      <article class="summary-highlight-card">
        <span class="summary-highlight-label">Most Profitable</span>
        <strong>${escapeHtml(profitable.ticker)}</strong>
        <span class="summary-highlight-detail">${escapeHtml(profitable.profitable_opportunity.summary || 'No summary available.')}</span>
      </article>
    `);
  }
  if (moderate?.moderate_risk_opportunity) {
    cards.push(`
      <article class="summary-highlight-card">
        <span class="summary-highlight-label">Moderate Risk</span>
        <strong>${escapeHtml(moderate.ticker)}</strong>
        <span class="summary-highlight-detail">${escapeHtml(moderate.moderate_risk_opportunity.summary || 'No summary available.')}</span>
      </article>
    `);
  }
  elements.summaryHighlights.innerHTML = cards.join('');
}

function renderSummaryTickerGrid(tickers) {
  elements.summaryTickerGrid.innerHTML = tickers.map((item) => `
    <article class="ticker-summary-card">
      <header class="ticker-summary-header">
        <div class="ticker-summary-title-block">
          <div>
            <p class="ticker-summary-kicker">${renderFieldLabel('Underlying Symbol', 'ticker-summary-kicker')}</p>
            <h2>${escapeHtml(item.ticker)}</h2>
          </div>
          <div class="ticker-summary-primary-metrics">
            <div class="ticker-summary-spot">
              ${renderFieldLabel('Underlying Price')}
              <strong>${escapeHtml(formatNumber(item.underlying_price, 2))}</strong>
            </div>
            <span class="ticker-summary-status-pill" title="${escapeHtml(getFieldDescription('Latest Status'))}">${escapeHtml(item.latest_status || 'Snapshot available')}</span>
          </div>
        </div>
      </header>
      <div class="ticker-summary-stats">
        <div class="ticker-summary-stat">
          ${renderFieldLabel('Underlying Day Change %')}
          <strong class="${metricToneClass(item.underlying_day_change_pct)}">${escapeHtml(formatSignedPercent(item.underlying_day_change_pct))}</strong>
        </div>
        <div class="ticker-summary-stat">
          ${renderFieldLabel('Implied Volatility')}
          <strong>${escapeHtml(formatNumber(item.median_implied_volatility_pct, 1))}%</strong>
        </div>
        <div class="ticker-summary-stat">
          ${renderFieldLabel('Historical Volatility')}
          <strong>${escapeHtml(formatNumber(item.historical_volatility_pct, 1))}%</strong>
        </div>
        <div class="ticker-summary-stat">
          ${renderFieldLabel('IV / HV')}
          <strong>${escapeHtml(formatNumber(item.iv_hv_ratio, 2))}</strong>
        </div>
        <div class="ticker-summary-stat">
          ${renderFieldLabel('Best ROM')}
          <strong>${escapeHtml(formatSignedPercent(item.profitable_opportunity?.return_on_margin_annualized_pct))}</strong>
        </div>
        <div class="ticker-summary-stat">
          ${renderFieldLabel('Moderate ROM')}
          <strong>${escapeHtml(formatSignedPercent(item.moderate_risk_opportunity?.return_on_margin_annualized_pct))}</strong>
        </div>
      </div>
      <div class="opportunity-grid">
        ${renderOpportunityCard('Most Profitable', item.profitable_opportunity, 'profit')}
        ${renderOpportunityCard('Moderate Risk', item.moderate_risk_opportunity, 'moderate')}
      </div>
    </article>
  `).join('');
}

function renderSummary(summary) {
  if (!summary) {
    elements.summaryStatus.textContent = 'No summary loaded.';
    elements.summaryHighlights.innerHTML = '';
    elements.summaryTickerGrid.innerHTML = '';
    return;
  }
  elements.summaryStatus.textContent = `${summary.selected_file} · ${summary.tickers.length} tickers summarized`;
  elements.summaryHighlights.innerHTML = '';
  renderSummaryTickerGrid(summary.tickers);
}

function normalizeFilterValue(value) {
  return value === null || value === undefined || value === '' ? '—' : String(value);
}

function parseFilterNumber(value) {
  if (value === '' || value === null || value === undefined) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function getColumnDefinition(columnName) {
  return state.columns.find((column) => column.name === columnName) || null;
}

function isRangeFilter(columnName) {
  return getColumnDefinition(columnName)?.is_numeric === true;
}

function hasActiveColumnFilter(columnName) {
  const filter = state.columnFilters[columnName];
  if (!filter) return false;
  if (filter.type === 'range') {
    return filter.min !== null || filter.max !== null;
  }
  return filter.values.size > 0;
}

function getColumnFilterValues(columnName) {
  const values = new Set();
  state.rows.forEach((row) => values.add(normalizeFilterValue(row[columnName])));
  return [...values].sort((left, right) => compareValues(left, right));
}

function getFilteredRows() {
  let rows = state.rows.slice();

  Object.entries(state.columnFilters).forEach(([columnName, filter]) => {
    if (filter.type === 'range') {
      if (filter.min !== null || filter.max !== null) {
        rows = rows.filter((row) => {
          const value = Number(row[columnName]);
          if (!Number.isFinite(value)) return false;
          if (filter.min !== null && value < filter.min) return false;
          if (filter.max !== null && value > filter.max) return false;
          return true;
        });
      }
      return;
    }

    if (filter.values.size > 0) {
      rows = rows.filter((row) => filter.values.has(normalizeFilterValue(row[columnName])));
    }
  });

  if (state.sortColumn) {
    rows.sort((a, b) => {
      const delta = compareValues(a[state.sortColumn], b[state.sortColumn]);
      return state.sortDirection === 'asc' ? delta : -delta;
    });
  }

  return rows;
}

function closeFilterPopover() {
  state.activeFilterColumn = null;
  state.filterSearchTerm = '';
  elements.filterPopover.classList.remove('open');
  elements.filterPopover.setAttribute('aria-hidden', 'true');
}

function renderFilterOptions() {
  if (!state.activeFilterColumn) return;
  if (isRangeFilter(state.activeFilterColumn)) {
    elements.filterOptionList.innerHTML = '';
    return;
  }

  const activeFilter = state.columnFilters[state.activeFilterColumn];
  const selectedValues = activeFilter?.type === 'set' ? activeFilter.values : new Set();
  const values = getColumnFilterValues(state.activeFilterColumn).filter((value) =>
    value.toLowerCase().includes(state.filterSearchTerm.toLowerCase())
  );
  elements.filterOptionList.innerHTML = '';

  if (values.length === 0) {
    const emptyState = document.createElement('div');
    emptyState.className = 'filter-option-empty';
    emptyState.textContent = 'No matching values';
    elements.filterOptionList.appendChild(emptyState);
    return;
  }

  values.forEach((value) => {
    const label = document.createElement('label');
    label.className = 'filter-option';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.checked = selectedValues.has(value);
    checkbox.addEventListener('change', () => {
      if (!state.columnFilters[state.activeFilterColumn] || state.columnFilters[state.activeFilterColumn].type !== 'set') {
        state.columnFilters[state.activeFilterColumn] = { type: 'set', values: new Set() };
      }
      if (checkbox.checked) {
        state.columnFilters[state.activeFilterColumn].values.add(value);
      } else {
        state.columnFilters[state.activeFilterColumn].values.delete(value);
        if (state.columnFilters[state.activeFilterColumn].values.size === 0) {
          delete state.columnFilters[state.activeFilterColumn];
        }
      }
      state.currentPage = 1;
      renderTable();
      renderFilterOptions();
    });
    const text = document.createElement('span');
    text.textContent = value;
    label.appendChild(checkbox);
    label.appendChild(text);
    elements.filterOptionList.appendChild(label);
  });
}

function openFilterPopover(columnName, anchor) {
  state.activeFilterColumn = columnName;
  state.filterSearchTerm = '';
  elements.filterPopoverTitle.textContent = `${columnName} filter`;
  elements.filterValueSearch.value = '';
  elements.filterSearchWrap.hidden = isRangeFilter(columnName);
  elements.filterRangeWrap.hidden = !isRangeFilter(columnName);

  if (isRangeFilter(columnName)) {
    const filter = state.columnFilters[columnName];
    elements.filterMinValue.placeholder = `Min ${columnName}`;
    elements.filterMaxValue.placeholder = `Max ${columnName}`;
    elements.filterMinValue.value = filter?.type === 'range' && filter.min !== null ? String(filter.min) : '';
    elements.filterMaxValue.value = filter?.type === 'range' && filter.max !== null ? String(filter.max) : '';
  }

  renderFilterOptions();

  const rect = anchor.getBoundingClientRect();
  elements.filterPopover.style.top = `${window.scrollY + rect.bottom + 6}px`;
  elements.filterPopover.style.left = `${Math.max(8, window.scrollX + rect.left - 180 + rect.width)}px`;
  elements.filterPopover.classList.add('open');
  elements.filterPopover.setAttribute('aria-hidden', 'false');
  if (isRangeFilter(columnName)) {
    elements.filterMinValue.focus();
  } else {
    elements.filterValueSearch.focus();
  }
}

function getPagedRows(rows) {
  if (state.pageSize === 'all') {
    state.currentPage = 1;
    return {
      rows,
      totalPages: 1,
    };
  }

  const totalPages = Math.max(1, Math.ceil(rows.length / state.pageSize));
  state.currentPage = Math.min(state.currentPage, totalPages);
  const start = (state.currentPage - 1) * state.pageSize;
  return {
    rows: rows.slice(start, start + state.pageSize),
    totalPages,
  };
}

function ensureColumnGroup() {
  let colgroup = elements.dataTable.querySelector('colgroup');
  if (!colgroup) {
    colgroup = document.createElement('colgroup');
    elements.dataTable.insertBefore(colgroup, elements.tableHead);
  }
  colgroup.innerHTML = '';
  state.columns.forEach((column) => {
    const col = document.createElement('col');
    const width = state.columnWidths[column.name];
    if (width) {
      col.style.width = `${width}px`;
    }
    colgroup.appendChild(col);
  });
}

function startColumnResize(event, columnName) {
  event.preventDefault();
  event.stopPropagation();
  const th = event.target.closest('th');
  const startX = event.clientX;
  const startWidth = th.getBoundingClientRect().width;

  const onMove = (moveEvent) => {
    const nextWidth = Math.max(96, startWidth + (moveEvent.clientX - startX));
    state.columnWidths[columnName] = nextWidth;
    ensureColumnGroup();
  };

  const onUp = () => {
    window.removeEventListener('pointermove', onMove);
    window.removeEventListener('pointerup', onUp);
    document.body.classList.remove('is-resizing');
  };

  document.body.classList.add('is-resizing');
  window.addEventListener('pointermove', onMove);
  window.addEventListener('pointerup', onUp);
}

function renderTable() {
  const filteredRows = getFilteredRows();
  const { rows, totalPages } = getPagedRows(filteredRows);
  elements.rowCount.textContent = filteredRows.length.toLocaleString();

  ensureColumnGroup();
  elements.tableHead.innerHTML = '';
  const headerRow = document.createElement('tr');
  state.columns.forEach((column) => {
    const th = document.createElement('th');
    const headerInner = document.createElement('div');
    headerInner.className = 'header-cell';
    const button = document.createElement('button');
    const sortMark = state.sortColumn === column.name ? (state.sortDirection === 'asc' ? ' ↑' : ' ↓') : '';
    button.innerHTML = `<span class="tooltip-label" title="${escapeHtml(column.description)}">${escapeHtml(column.name)}</span>${sortMark}`;
    button.addEventListener('click', () => {
      if (state.sortColumn === column.name) {
        state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
      } else {
        state.sortColumn = column.name;
        state.sortDirection = 'asc';
      }
      renderTable();
    });
    const filterButton = document.createElement('button');
    filterButton.type = 'button';
    filterButton.className = `header-filter-button ${hasActiveColumnFilter(column.name) ? 'active' : ''}`;
    filterButton.title = `Filter ${column.name}`;
    filterButton.setAttribute('aria-label', hasActiveColumnFilter(column.name)
      ? `Filter ${column.name}, active`
      : `Filter ${column.name}`);
    const filterIcon = document.createElement('span');
    filterIcon.className = 'header-filter-icon';
    filterIcon.setAttribute('aria-hidden', 'true');
    filterIcon.innerHTML = `
      <svg viewBox="0 0 16 16" focusable="false">
        <path d="M2.5 3.5h11l-4.25 4.75v3.1l-2.5 1.45V8.25z"></path>
      </svg>
    `;
    filterButton.appendChild(filterIcon);
    if (hasActiveColumnFilter(column.name)) {
      const filterCount = document.createElement('span');
      filterCount.className = 'header-filter-count';
      if (isRangeFilter(column.name)) {
        filterCount.textContent = 'R';
      } else {
        filterCount.textContent = String(state.columnFilters[column.name].values.size);
      }
      filterButton.appendChild(filterCount);
    }
    filterButton.addEventListener('click', (event) => {
      event.stopPropagation();
      event.preventDefault();
      if (state.activeFilterColumn === column.name && elements.filterPopover.classList.contains('open')) {
        closeFilterPopover();
      } else {
        openFilterPopover(column.name, filterButton);
      }
    });
    const resizer = document.createElement('span');
    resizer.className = 'column-resizer';
    resizer.title = `Resize ${column.name}`;
    resizer.addEventListener('pointerdown', (event) => startColumnResize(event, column.name));
    headerInner.appendChild(button);
    headerInner.appendChild(filterButton);
    headerInner.appendChild(resizer);
    th.appendChild(headerInner);
    headerRow.appendChild(th);
  });
  elements.tableHead.appendChild(headerRow);

  elements.tableBody.innerHTML = '';
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.className = 'data-row';
    tr.tabIndex = 0;
    tr.addEventListener('click', () => openRowModal(row));
    tr.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        openRowModal(row);
      }
    });
    state.columns.forEach((column) => {
      const td = document.createElement('td');
      td.textContent = formatCell(row[column.name]);
      tr.appendChild(td);
    });
    elements.tableBody.appendChild(tr);
  });

  const pageStart = filteredRows.length === 0 ? 0 : (
    state.pageSize === 'all' ? 1 : ((state.currentPage - 1) * state.pageSize) + 1
  );
  const pageEnd = state.pageSize === 'all'
    ? filteredRows.length
    : Math.min(state.currentPage * state.pageSize, filteredRows.length);
  elements.tableStatus.textContent =
    `${state.selectedFile} · showing ${pageStart.toLocaleString()}-${pageEnd.toLocaleString()} of ${filteredRows.length.toLocaleString()} filtered rows`;
  elements.pageInfo.textContent = state.pageSize === 'all'
    ? 'All rows'
    : `Page ${state.currentPage} of ${totalPages}`;
  elements.prevPageButton.disabled = state.pageSize === 'all' || state.currentPage <= 1;
  elements.nextPageButton.disabled = state.pageSize === 'all' || state.currentPage >= totalPages;
}

function openRowModal(row) {
  state.selectedRow = row;
  const identityParts = [
    row.underlying_symbol,
    row.option_type,
    row.expiration_date,
    row.strike !== undefined && row.strike !== null ? `strike ${row.strike}` : null,
  ].filter(Boolean);
  const identityText = identityParts.join(' · ');
  elements.rowModalTitle.textContent = identityText || 'Record';
  elements.rowModalMeta.textContent = state.selectedFile ? `Source ${state.selectedFile}` : '';
  elements.rowDetailGrid.innerHTML = '';

  state.columns.forEach((column) => {
    const item = document.createElement('article');
    item.className = 'row-detail-item';

    const label = document.createElement('div');
    label.className = 'row-detail-label';
    label.textContent = column.name;
    label.title = column.description;

    const value = document.createElement('div');
    value.className = 'row-detail-value';
    value.textContent = formatCell(row[column.name]);

    const description = document.createElement('div');
    description.className = 'row-detail-description';
    description.textContent = column.description;

    item.appendChild(label);
    item.appendChild(value);
    item.appendChild(description);
    elements.rowDetailGrid.appendChild(item);
  });

  elements.rowModal.classList.add('open');
  elements.rowModal.setAttribute('aria-hidden', 'false');
  document.body.classList.add('modal-open');
}

function closeRowModal() {
  state.selectedRow = null;
  elements.rowModal.classList.remove('open');
  elements.rowModal.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('modal-open');
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function renderMarkdown(markdown) {
  const lines = markdown.split('\n');
  let html = '';
  let inList = false;
  let inCode = false;

  const closeList = () => {
    if (inList) {
      html += '</ul>';
      inList = false;
    }
  };

  for (const line of lines) {
    if (line.startsWith('```')) {
      closeList();
      html += inCode ? '</code></pre>' : '<pre><code>';
      inCode = !inCode;
      continue;
    }

    if (inCode) {
      html += `${escapeHtml(line)}\n`;
      continue;
    }

    if (!line.trim()) {
      closeList();
      continue;
    }

    if (line.startsWith('### ')) {
      closeList();
      html += `<h3>${inlineMarkdown(line.slice(4))}</h3>`;
      continue;
    }
    if (line.startsWith('## ')) {
      closeList();
      html += `<h2>${inlineMarkdown(line.slice(3))}</h2>`;
      continue;
    }
    if (line.startsWith('# ')) {
      closeList();
      html += `<h1>${inlineMarkdown(line.slice(2))}</h1>`;
      continue;
    }
    if (line.startsWith('- ')) {
      if (!inList) {
        html += '<ul>';
        inList = true;
      }
      html += `<li>${inlineMarkdown(line.slice(2))}</li>`;
      continue;
    }

    closeList();
    html += `<p>${inlineMarkdown(line)}</p>`;
  }

  closeList();
  if (inCode) html += '</code></pre>';
  return html;
}

function inlineMarkdown(text) {
  return escapeHtml(text)
    .replace(/`([^`]+)`/g, '<code>$1</code>');
}

async function loadFiles() {
  const payload = await fetchJson('/api/files');
  state.files = payload.files;
  elements.fileSelect.innerHTML = '';

  state.files.forEach((file) => {
    const option = document.createElement('option');
    option.value = file.name;
    option.textContent = file.name;
    elements.fileSelect.appendChild(option);
  });
}

async function loadData(fileName) {
  const [dataResult, summaryResult] = await Promise.allSettled([
    fetchJson(`/api/data?file=${encodeURIComponent(fileName)}`),
    fetchJson(`/api/summary?file=${encodeURIComponent(fileName)}`),
  ]);
  if (dataResult.status !== 'fulfilled') {
    throw dataResult.reason;
  }
  const payload = dataResult.value;
  state.selectedFile = payload.selected_file;
  state.rows = payload.rows;
  state.columns = payload.columns;
  state.summary = summaryResult.status === 'fulfilled' ? summaryResult.value : null;
  state.columnFilters = {};
  state.currentPage = 1;
  state.columnWidths = {};
  elements.fileSelect.value = state.selectedFile;
  renderFreshnessSummary(payload.freshness_summary);
  renderDatasetCards(payload.dataset_cards);
  if (summaryResult.status === 'fulfilled') {
    renderSummary(summaryResult.value);
  } else {
    elements.summaryStatus.textContent = `Summary unavailable: ${summaryResult.reason.message}`;
    elements.summaryHighlights.innerHTML = '';
    elements.summaryTickerGrid.innerHTML = '';
  }
  renderTable();
}

async function loadReadme() {
  const payload = await fetchJson('/api/readme');
  elements.readmeContent.innerHTML = renderMarkdown(payload.markdown);
}

function activateTab(tabName) {
  elements.tabButtons.forEach((button) => {
    button.classList.toggle('active', button.dataset.tab === tabName);
  });
  elements.summaryTab.classList.toggle('active', tabName === 'summary');
  elements.tableTab.classList.toggle('active', tabName === 'table');
  elements.readmeTab.classList.toggle('active', tabName === 'readme');
}

function updateThemeToggleLabel(theme) {
  elements.themeToggle.textContent = theme === 'dark' ? 'Light' : 'Dark';
}

function setTheme(theme) {
  document.body.dataset.theme = theme;
  localStorage.setItem('options-fetcher-theme', theme);
  updateThemeToggleLabel(theme);
}

function initializeTheme() {
  const savedTheme = localStorage.getItem('options-fetcher-theme');
  setTheme(savedTheme || 'light');
}

async function initialize() {
  initializeTheme();
  await Promise.all([loadFiles(), loadReadme()]);
  if (state.files.length > 0) {
    await loadData(state.files[0].name);
  } else {
    elements.tableStatus.textContent = 'No CSV files found in the project root.';
  }

  elements.fileSelect.addEventListener('change', async (event) => {
    await loadData(event.target.value);
  });

  elements.pageSizeSelect.addEventListener('change', (event) => {
    state.pageSize = event.target.value === 'all' ? 'all' : Number(event.target.value);
    state.currentPage = 1;
    renderTable();
  });

  elements.prevPageButton.addEventListener('click', () => {
    if (state.currentPage > 1) {
      state.currentPage -= 1;
      renderTable();
    }
  });

  elements.nextPageButton.addEventListener('click', () => {
    state.currentPage += 1;
    renderTable();
  });

  elements.filterValueSearch.addEventListener('input', (event) => {
    state.filterSearchTerm = event.target.value;
    renderFilterOptions();
  });

  const applyRangeFilter = () => {
    if (!state.activeFilterColumn || !isRangeFilter(state.activeFilterColumn)) return;
    const min = parseFilterNumber(elements.filterMinValue.value);
    const max = parseFilterNumber(elements.filterMaxValue.value);
    if (min === null && max === null) {
      delete state.columnFilters[state.activeFilterColumn];
    } else {
      state.columnFilters[state.activeFilterColumn] = { type: 'range', min, max };
    }
    state.currentPage = 1;
    renderTable();
  };

  elements.filterMinValue.addEventListener('input', applyRangeFilter);
  elements.filterMaxValue.addEventListener('input', applyRangeFilter);

  elements.clearFilterButton.addEventListener('click', () => {
    if (state.activeFilterColumn) {
      delete state.columnFilters[state.activeFilterColumn];
      elements.filterMinValue.value = '';
      elements.filterMaxValue.value = '';
      state.currentPage = 1;
      renderTable();
      renderFilterOptions();
    }
  });

  document.addEventListener('click', (event) => {
    if (elements.filterPopover.classList.contains('open') && !elements.filterPopover.contains(event.target)) {
      closeFilterPopover();
    }
  });

  elements.closeRowModalButton.addEventListener('click', closeRowModal);
  elements.rowModal.addEventListener('click', (event) => {
    if (event.target.dataset.closeModal === 'true') {
      closeRowModal();
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && state.selectedRow) {
      closeRowModal();
    }
  });

  elements.tabButtons.forEach((button) => {
    button.addEventListener('click', () => activateTab(button.dataset.tab));
  });

  elements.themeToggle.addEventListener('click', () => {
    setTheme(document.body.dataset.theme === 'dark' ? 'light' : 'dark');
  });
}

initialize().catch((error) => {
  elements.tableStatus.textContent = error.message;
});
