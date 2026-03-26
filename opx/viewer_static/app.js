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
  activeTab: 'table',
  chainRenderToken: 0,
  chainView: {
    ticker: null,
    expiration: null,
    optionType: 'all',
    deltaXAxis: 'strike',
  },
};

const VALID_TABS = new Set(['table', 'summary', 'chain', 'readme']);

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
  summaryTickerGrid: document.getElementById('summaryTickerGrid'),
  tableTab: document.getElementById('tableTab'),
  chainTab: document.getElementById('chainTab'),
  chainTickerSelect: document.getElementById('chainTickerSelect'),
  chainExpirationSelect: document.getElementById('chainExpirationSelect'),
  chainOptionTypeSelect: document.getElementById('chainOptionTypeSelect'),
  chainDeltaXAxisSelect: document.getElementById('chainDeltaXAxisSelect'),
  chainStatus: document.getElementById('chainStatus'),
  chainDeltaChart: document.getElementById('chainDeltaChart'),
  chainPremiumChart: document.getElementById('chainPremiumChart'),
  chainThetaChart: document.getElementById('chainThetaChart'),
  chainSummaryPanel: document.getElementById('chainSummaryPanel'),
  readmeTab: document.getElementById('readmeTab'),
  readmeContent: document.getElementById('readmeContent'),
  themeToggle: document.getElementById('themeToggle'),
};

let chainTooltipElement = null;

function getTabFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const requestedTab = params.get('tab');
  return VALID_TABS.has(requestedTab) ? requestedTab : 'table';
}

function syncTabUrl(tabName) {
  const url = new URL(window.location.href);
  if (tabName === 'table') {
    url.searchParams.delete('tab');
  } else {
    url.searchParams.set('tab', tabName);
  }
  window.history.replaceState({}, '', url);
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function formatCell(value) {
  if (value === null || value === undefined || value === '') return '—';
  const number = Number(value);
  if (Number.isFinite(number) && typeof value !== 'boolean') {
    return number.toFixed(4);
  }
  return String(value);
}

function getLedgerPillTone(columnName, formattedValue) {
  const normalizedColumn = String(columnName || '').toLowerCase();
  const normalizedValue = String(formattedValue || '').trim().toLowerCase();

  if (!normalizedValue || normalizedValue === '—') return null;
  if (!/(risk|status|signal|sentiment|bias|state)/.test(normalizedColumn)) return null;

  if (['high', 'volatile', 'warning', 'bearish', 'downtrend'].includes(normalizedValue)) {
    return 'negative';
  }
  if (['medium', 'moderate', 'watch'].includes(normalizedValue)) {
    return 'neutral';
  }
  if (['low', 'bullish', 'positive', 'stable', 'open', 'regular'].includes(normalizedValue)) {
    return 'positive';
  }
  return 'neutral';
}

function appendCellValue(container, columnName, value) {
  const formattedValue = formatCell(value);
  const pillTone = getLedgerPillTone(columnName, formattedValue);

  if (pillTone) {
    const pill = document.createElement('span');
    pill.className = `ledger-pill ledger-pill-${pillTone}`;
    pill.textContent = formattedValue;
    container.appendChild(pill);
    return;
  }

  container.textContent = formattedValue;
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
  if (card.name === 'risk_free_rate_used') {
    const number = Number(card.value);
    if (Number.isFinite(number)) {
      return `${(number * 100).toFixed(4)}%`;
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

function formatNumber(value, digits = 4) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return Number(value).toFixed(digits);
}

function formatSignedPercent(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  const number = Number(value);
  const sign = number > 0 ? '+' : '';
  return `${sign}${number.toFixed(4)}%`;
}

function getFieldDescription(label) {
  const summaryDescriptions = {
    'File Age': 'How long ago the selected CSV file was written. Lower is fresher.',
    'Option Quotes': 'Age of option quotes in the file, shown as median and max quote_age_seconds. Lower is better.',
    Underlying: 'Age of the underlying price snapshots in the file, shown as median and max underlying_price_age_seconds. Lower is better.',
    Rows: 'Number of option records currently visible after active filters are applied. More rows mean broader coverage, not necessarily better candidates.',
    'Latest Status': 'Compact status derived from the latest underlying day move and the relationship between implied volatility and historical volatility.',
    'IV / HV': 'Ratio of implied volatility to historical volatility. Above 1 usually means options are priced richer than recent realized movement.',
    'Best ROM': 'Highest return_on_margin_annualized among candidate contracts for this ticker. Higher can be attractive, but extreme values usually mean more risk.',
    'Moderate ROM': 'return_on_margin_annualized for the selected lower-delta candidate for this ticker. A middle-ground value is often more realistic than the top outlier.',
    'Option Score': 'Shared 0-100 row score built from IV-adjusted premium/day, spread execution quality, DTE execution quality, delta-only risk, and theta efficiency. Higher is better within one run.',
    'Final Score': 'Option Score after the row-level score validation adjustment is applied.',
    'Risk Level': 'Prompt-aligned row risk label using delta as the score-driving risk input and probability_itm only as a consistency check.',
    'Spread Score': 'Execution-quality score derived from spread percent tiers. Higher is better.',
    'DTE Score': 'Execution-quality score derived from DTE tiers. Higher is better.',
    'Theta Efficiency': 'Row-level daily theta capture per $1,000 of capital required. Higher is better.',
    'Calls / Puts': 'Count of call and put option rows available for this underlying symbol.',
    'Most Profitable': 'Heuristic pick for the highest annualized return on margin among candidate contracts. Highest return is not always the safest setup.',
    'Moderate Risk': 'Heuristic pick balancing return on margin with lower delta and acceptable spread after primary-screen filtering.',
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
  const optionScore = Number.isFinite(Number(opportunity.option_score))
    ? `${Number(opportunity.option_score).toFixed(4)}`
    : '—';
  const finalScore = Number.isFinite(Number(opportunity.final_score))
    ? `${Number(opportunity.final_score).toFixed(4)}`
    : '—';
  const spreadScore = Number.isFinite(Number(opportunity.spread_score))
    ? `${Number(opportunity.spread_score).toFixed(4)}`
    : '—';
  const dteScore = Number.isFinite(Number(opportunity.dte_score))
    ? `${Number(opportunity.dte_score).toFixed(4)}`
    : '—';
  const thetaEfficiency = Number.isFinite(Number(opportunity.theta_efficiency))
    ? `${Number(opportunity.theta_efficiency).toFixed(4)}`
    : '—';
  const riskLevel = opportunity.risk_level || '—';
  return `
    <article class="opportunity-card opportunity-card-${tone}">
      ${renderFieldLabel(title, 'opportunity-label')}
      <strong>${escapeHtml(`${opportunity.option_type} ${formatNumber(opportunity.strike, 4)} · ${opportunity.expiration_date}`)}</strong>
      <span class="opportunity-detail">${escapeHtml(opportunity.summary || 'No summary available.')}</span>
      <span class="opportunity-detail">${renderFieldLabel('Final Score')} ${escapeHtml(finalScore)}</span>
      <span class="opportunity-detail">${renderFieldLabel('Option Score')} ${escapeHtml(optionScore)}</span>
      <span class="opportunity-detail">${renderFieldLabel('Risk Level')} ${escapeHtml(riskLevel)}</span>
      <span class="opportunity-detail">${renderFieldLabel('Spread Score')} ${escapeHtml(spreadScore)} · ${renderFieldLabel('DTE Score')} ${escapeHtml(dteScore)}</span>
      <span class="opportunity-detail">${renderFieldLabel('Theta Efficiency')} ${escapeHtml(thetaEfficiency)}</span>
    </article>
  `;
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
              <strong>${escapeHtml(formatNumber(item.underlying_price, 4))}</strong>
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
          <strong>${escapeHtml(formatNumber(item.median_implied_volatility_pct, 4))}%</strong>
        </div>
        <div class="ticker-summary-stat">
          ${renderFieldLabel('Historical Volatility')}
          <strong>${escapeHtml(formatNumber(item.historical_volatility_pct, 4))}%</strong>
        </div>
        <div class="ticker-summary-stat">
          ${renderFieldLabel('IV / HV')}
          <strong>${escapeHtml(formatNumber(item.iv_hv_ratio, 4))}</strong>
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
    elements.summaryTickerGrid.innerHTML = '';
    return;
  }
  elements.summaryStatus.textContent = `${summary.selected_file} · ${summary.tickers.length} tickers summarized`;
  renderSummaryTickerGrid(summary.tickers);
}

function formatCompactNumber(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return new Intl.NumberFormat(undefined, {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(Number(value));
}

function formatPercentValue(value, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

function isTruthyValue(value) {
  return ['true', '1', 'yes'].includes(String(value).trim().toLowerCase());
}

function getRiskColor(value) {
  const normalized = String(value || '').trim().toUpperCase();
  if (normalized === 'LOW') return '#10b981';
  if (normalized === 'MEDIUM' || normalized === 'MODERATE') return '#3b82f6';
  if (normalized === 'HIGH') return '#ef4444';
  return '#94a3b8';
}

function getChainTickerOptions() {
  const values = new Set();
  state.rows.forEach((row) => {
    if (row.underlying_symbol !== null && row.underlying_symbol !== undefined && row.underlying_symbol !== '') {
      values.add(String(row.underlying_symbol));
    }
  });
  return [...values].sort((left, right) => compareValues(left, right));
}

function getChainExpirationOptions(ticker) {
  const values = new Set();
  state.rows.forEach((row) => {
    if (String(row.underlying_symbol) !== String(ticker)) return;
    if (row.expiration_date !== null && row.expiration_date !== undefined && row.expiration_date !== '') {
      values.add(String(row.expiration_date));
    }
  });
  return [...values].sort((left, right) => compareValues(left, right));
}

function syncChainViewState() {
  const tickers = getChainTickerOptions();
  if (!tickers.includes(state.chainView.ticker)) {
    state.chainView.ticker = tickers[0] || null;
  }

  const expirations = state.chainView.ticker ? getChainExpirationOptions(state.chainView.ticker) : [];
  if (!expirations.includes(state.chainView.expiration)) {
    state.chainView.expiration = expirations[0] || null;
  }

  elements.chainTickerSelect.innerHTML = tickers.map((ticker) => (
    `<option value="${escapeHtml(ticker)}">${escapeHtml(ticker)}</option>`
  )).join('');
  elements.chainTickerSelect.value = state.chainView.ticker || '';

  elements.chainExpirationSelect.innerHTML = expirations.map((expiration) => (
    `<option value="${escapeHtml(expiration)}">${escapeHtml(expiration)}</option>`
  )).join('');
  elements.chainExpirationSelect.value = state.chainView.expiration || '';
  elements.chainOptionTypeSelect.value = state.chainView.optionType;
  elements.chainDeltaXAxisSelect.value = state.chainView.deltaXAxis;
}

function getSelectedChainRows() {
  return state.rows
    .filter((row) => String(row.underlying_symbol) === String(state.chainView.ticker))
    .filter((row) => String(row.expiration_date) === String(state.chainView.expiration))
    .filter((row) => state.chainView.optionType === 'all' || row.option_type === state.chainView.optionType)
    .slice()
    .sort((left, right) => compareValues(left.strike, right.strike));
}

function getChainRowByContractSymbol(contractSymbol) {
  if (!contractSymbol) return null;
  return getSelectedChainRows().find((row) => String(row.contract_symbol) === String(contractSymbol)) || null;
}

function sampleRowsForChart(rows, maxPoints) {
  if (rows.length <= maxPoints) return rows;
  const result = [];
  const lastIndex = rows.length - 1;
  for (let index = 0; index < maxPoints; index += 1) {
    const rowIndex = Math.round((index / Math.max(maxPoints - 1, 1)) * lastIndex);
    result.push(rows[rowIndex]);
  }
  return result;
}

function buildNumericTicks(domain, count = 4) {
  if (!domain) return [];
  const [min, max] = domain;
  if (!Number.isFinite(min) || !Number.isFinite(max)) return [];
  if (count <= 1 || min === max) return [min];
  return Array.from({ length: count }, (_, index) => min + (((max - min) * index) / (count - 1)));
}

function buildIndexedTicks(rows, count = 5) {
  if (rows.length === 0) return [];
  if (rows.length <= count) {
    return rows.map((row, index) => ({ row, index }));
  }
  const lastIndex = rows.length - 1;
  const seen = new Set();
  const ticks = [];
  for (let index = 0; index < count; index += 1) {
    const rowIndex = Math.round((index / Math.max(count - 1, 1)) * lastIndex);
    if (seen.has(rowIndex)) continue;
    seen.add(rowIndex);
    ticks.push({ row: rows[rowIndex], index: rowIndex });
  }
  return ticks;
}

function getNumericExtent(values) {
  const numeric = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value));
  if (numeric.length === 0) return null;
  return [Math.min(...numeric), Math.max(...numeric)];
}

function expandExtent(extent, paddingFraction = 0.08) {
  if (!extent) return null;
  const [min, max] = extent;
  if (min === max) {
    const delta = min === 0 ? 1 : Math.abs(min) * paddingFraction;
    return [min - delta, max + delta];
  }
  const padding = (max - min) * paddingFraction;
  return [min - padding, max + padding];
}

function scaleLinear(value, domain, range) {
  if (!domain || !range) return range?.[0] ?? 0;
  const [domainMin, domainMax] = domain;
  const [rangeMin, rangeMax] = range;
  if (domainMax === domainMin) return (rangeMin + rangeMax) / 2;
  const ratio = (value - domainMin) / (domainMax - domainMin);
  return rangeMin + ((rangeMax - rangeMin) * ratio);
}

function renderChartEmpty(element, title) {
  element.innerHTML = `
    <div class="chart-empty">
      <strong>${escapeHtml(title)}</strong>
      <span>No numeric rows are available for this view.</span>
    </div>
  `;
}

function ensureChainTooltip() {
  if (chainTooltipElement) return chainTooltipElement;
  chainTooltipElement = document.createElement('div');
  chainTooltipElement.className = 'chain-tooltip';
  chainTooltipElement.setAttribute('aria-hidden', 'true');
  document.body.appendChild(chainTooltipElement);
  return chainTooltipElement;
}

function showChainTooltip(event, html) {
  const tooltip = ensureChainTooltip();
  tooltip.innerHTML = html;
  tooltip.classList.add('open');
  tooltip.setAttribute('aria-hidden', 'false');
  const pointerX = event.clientX + 14;
  const pointerY = event.clientY + 14;
  const maxLeft = window.innerWidth - tooltip.offsetWidth - 12;
  const maxTop = window.innerHeight - tooltip.offsetHeight - 12;
  tooltip.style.left = `${Math.max(12, Math.min(pointerX, maxLeft))}px`;
  tooltip.style.top = `${Math.max(12, Math.min(pointerY, maxTop))}px`;
}

function hideChainTooltip() {
  if (!chainTooltipElement) return;
  chainTooltipElement.classList.remove('open');
  chainTooltipElement.setAttribute('aria-hidden', 'true');
}

function renderChainTooltip(row, metrics = []) {
  if (!row) return '';
  const primary = [
    row.underlying_symbol,
    row.option_type,
    row.expiration_date,
    row.strike !== undefined && row.strike !== null ? `strike ${formatNumber(row.strike, 2)}` : null,
  ].filter(Boolean).join(' · ');
  return `
    <div class="chain-tooltip-title">${escapeHtml(primary || row.contract_symbol || 'Contract')}</div>
    <div class="chain-tooltip-subtitle">${escapeHtml(row.contract_symbol || '—')}</div>
    <div class="chain-tooltip-grid">
      ${metrics.map((item) => `
        <div class="chain-tooltip-item">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </div>
      `).join('')}
    </div>
  `;
}

function bindChainInteractions(container, metricFactory) {
  container.querySelectorAll('[data-chain-contract]').forEach((node) => {
    node.addEventListener('mouseenter', (event) => {
      const row = getChainRowByContractSymbol(node.dataset.chainContract);
      if (!row) return;
      showChainTooltip(event, renderChainTooltip(row, metricFactory(row)));
    });
    node.addEventListener('mousemove', (event) => {
      const row = getChainRowByContractSymbol(node.dataset.chainContract);
      if (!row) return;
      showChainTooltip(event, renderChainTooltip(row, metricFactory(row)));
    });
    node.addEventListener('mouseleave', hideChainTooltip);
    node.addEventListener('click', () => {
      const row = getChainRowByContractSymbol(node.dataset.chainContract);
      if (row) openRowModal(row);
    });
    node.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter' && event.key !== ' ') return;
      event.preventDefault();
      const row = getChainRowByContractSymbol(node.dataset.chainContract);
      if (row) openRowModal(row);
    });
  });
}

function renderDeltaChart(rows) {
  const xKey = state.chainView.deltaXAxis;
  const xLabel = xKey === 'strike_vs_spot_pct' ? 'Moneyness vs spot' : 'Strike';
  const chartRows = sampleRowsForChart(rows, 320)
    .map((row) => ({
      contractSymbol: row.contract_symbol,
      optionType: row.option_type,
      x: Number(row[xKey]),
      delta: Number(row.delta_abs),
      premium: Number(row.iv_adjusted_premium_per_day),
      riskLevel: row.risk_level,
      passesPrimaryScreen: isTruthyValue(row.passes_primary_screen),
      strike: row.strike,
    }))
    .filter((row) => Number.isFinite(row.x) && Number.isFinite(row.delta));

  if (chartRows.length === 0) {
    renderChartEmpty(elements.chainDeltaChart, 'Delta chart unavailable');
    return;
  }

  const width = 960;
  const height = 280;
  const margin = { top: 24, right: 68, bottom: 42, left: 56 };
  const xDomain = expandExtent(getNumericExtent(chartRows.map((row) => row.x)), 0.05);
  const yDomain = [0, 1];
  const premiumDomain = expandExtent(getNumericExtent(chartRows.map((row) => row.premium)), 0.1)
    || [0, 1];
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const linePoints = chartRows
    .filter((row) => Number.isFinite(row.premium))
    .sort((left, right) => left.x - right.x)
    .map((row) => `${scaleLinear(row.x, xDomain, [margin.left, margin.left + plotWidth]).toFixed(2)},${scaleLinear(row.premium, premiumDomain, [margin.top + plotHeight, margin.top]).toFixed(2)}`);

  const circles = chartRows.map((row) => {
    const cx = scaleLinear(row.x, xDomain, [margin.left, margin.left + plotWidth]);
    const cy = scaleLinear(row.delta, yDomain, [margin.top + plotHeight, margin.top]);
    const radius = row.passesPrimaryScreen ? 6.5 : 4.5;
    const labelX = xKey === 'strike_vs_spot_pct' ? formatPercentValue(row.x, 1) : formatNumber(row.x, 2);
    return `
      <circle class="chart-hit-target" tabindex="0" role="button" aria-label="${escapeHtml(`${row.contractSymbol || 'Contract'} details`)}" data-chain-contract="${escapeHtml(row.contractSymbol || '')}" cx="${cx.toFixed(2)}" cy="${cy.toFixed(2)}" r="${radius}" fill="${getRiskColor(row.riskLevel)}" fill-opacity="0.85" stroke="rgba(15,23,42,0.28)" stroke-width="1">
        <title>${escapeHtml(`${row.contractSymbol || 'Contract'} · ${row.optionType || 'option'} · ${xLabel} ${labelX} · delta ${formatNumber(row.delta, 3)} · risk ${row.riskLevel || '—'}`)}</title>
      </circle>
    `;
  }).join('');

  const yTicks = [0, 0.25, 0.5, 0.75, 1];
  const xTicks = buildNumericTicks(xDomain, 4);
  const premiumTop = premiumDomain[1];

  elements.chainDeltaChart.innerHTML = `
    <div class="chart-legend">
      <span><i class="legend-dot" style="background:#10b981"></i>LOW</span>
      <span><i class="legend-dot" style="background:#3b82f6"></i>MEDIUM</span>
      <span><i class="legend-dot" style="background:#ef4444"></i>HIGH</span>
      <span><i class="legend-line"></i>IV-adjusted premium/day</span>
      <span><i class="legend-badge"></i>Primary-screen pass = larger point</span>
    </div>
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Delta versus strike scatter chart">
      <rect x="${margin.left}" y="${margin.top}" width="${plotWidth}" height="${plotHeight}" rx="10" fill="rgba(148,163,184,0.06)"></rect>
      ${yTicks.map((tick) => {
        const y = scaleLinear(tick, yDomain, [margin.top + plotHeight, margin.top]);
        return `
          <line x1="${margin.left}" y1="${y.toFixed(2)}" x2="${margin.left + plotWidth}" y2="${y.toFixed(2)}" stroke="rgba(148,163,184,0.22)" stroke-dasharray="4 6"></line>
          <text x="${margin.left - 12}" y="${(y + 4).toFixed(2)}" text-anchor="end" class="chart-axis-label">${tick.toFixed(2)}</text>
        `;
      }).join('')}
      ${xTicks.map((tick) => {
        const x = scaleLinear(tick, xDomain, [margin.left, margin.left + plotWidth]);
        const label = xKey === 'strike_vs_spot_pct' ? formatPercentValue(tick, 1) : formatNumber(tick, 2);
        return `
          <line x1="${x.toFixed(2)}" y1="${margin.top}" x2="${x.toFixed(2)}" y2="${margin.top + plotHeight}" stroke="rgba(148,163,184,0.18)"></line>
          <text x="${x.toFixed(2)}" y="${height - 10}" text-anchor="middle" class="chart-axis-label">${escapeHtml(label)}</text>
        `;
      }).join('')}
      ${linePoints.length > 1 ? `<polyline fill="none" stroke="#f59e0b" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" points="${linePoints.join(' ')}"></polyline>` : ''}
      ${circles}
      <text x="${margin.left}" y="14" class="chart-axis-title">Delta Abs</text>
      <text x="${width - margin.right + 12}" y="14" class="chart-axis-title">IV-Adj Premium/Day</text>
      <text x="${width / 2}" y="${height - 4}" text-anchor="middle" class="chart-axis-title">${escapeHtml(xLabel)}</text>
      <text x="${width - margin.right + 14}" y="${margin.top + 12}" class="chart-axis-label">${escapeHtml(formatNumber(premiumTop, 2))}</text>
      <text x="${width - margin.right + 14}" y="${margin.top + plotHeight}" class="chart-axis-label">${escapeHtml(formatNumber(premiumDomain[0], 2))}</text>
    </svg>
  `;
  bindChainInteractions(elements.chainDeltaChart, (row) => [
    { label: 'Delta Abs', value: formatNumber(row.delta_abs, 3) },
    { label: xLabel, value: xKey === 'strike_vs_spot_pct' ? formatPercentValue(row[xKey], 1) : formatNumber(row[xKey], 2) },
    { label: 'IV-Adj Premium/Day', value: formatNumber(row.iv_adjusted_premium_per_day, 2) },
    { label: 'Risk', value: String(row.risk_level || '—') },
  ]);
}

function renderPremiumChart(rows) {
  const chartRows = sampleRowsForChart(rows, 240)
    .map((row) => ({
      contractSymbol: row.contract_symbol,
      strike: Number(row.strike),
      premium: Number(row.mark_price_mid),
      spreadPct: Number(row.bid_ask_spread_pct_of_mid),
      passesPrimaryScreen: isTruthyValue(row.passes_primary_screen),
    }))
    .filter((row) => Number.isFinite(row.strike) && Number.isFinite(row.premium));

  if (chartRows.length === 0) {
    renderChartEmpty(elements.chainPremiumChart, 'Premium chart unavailable');
    return;
  }

  const width = 960;
  const height = 280;
  const margin = { top: 24, right: 68, bottom: 42, left: 56 };
  const premiumDomain = expandExtent(getNumericExtent(chartRows.map((row) => row.premium)), 0.08) || [0, 1];
  const spreadDomain = expandExtent(getNumericExtent(chartRows.map((row) => row.spreadPct)), 0.1) || [0, 1];
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const barWidth = Math.max(10, (plotWidth / Math.max(chartRows.length, 1)) * 0.72);
  const spreadLinePoints = chartRows
    .filter((row) => Number.isFinite(row.spreadPct))
    .map((row, index) => {
      const cx = margin.left + ((index + 0.5) * plotWidth / chartRows.length);
      const cy = scaleLinear(row.spreadPct, spreadDomain, [margin.top + plotHeight, margin.top]);
      return `${cx.toFixed(2)},${cy.toFixed(2)}`;
    });

  elements.chainPremiumChart.innerHTML = `
    <div class="chart-legend">
      <span><i class="legend-bar"></i>Mark price mid</span>
      <span><i class="legend-line legend-line-danger"></i>Spread % of mid</span>
      <span><i class="legend-badge"></i>Darker bar = primary-screen pass</span>
    </div>
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Premium and spread by strike chart">
      <rect x="${margin.left}" y="${margin.top}" width="${plotWidth}" height="${plotHeight}" rx="10" fill="rgba(148,163,184,0.06)"></rect>
      ${chartRows.map((row, index) => {
        const x = margin.left + ((index + 0.5) * plotWidth / chartRows.length) - (barWidth / 2);
        const y = scaleLinear(row.premium, premiumDomain, [margin.top + plotHeight, margin.top]);
        const labelY = height - 10;
        const fill = row.passesPrimaryScreen ? '#0f766e' : '#94a3b8';
        const spread = Number.isFinite(row.spreadPct) ? formatPercentValue(row.spreadPct, 1) : '—';
        return `
          <rect class="chart-hit-target" tabindex="0" role="button" aria-label="${escapeHtml(`${row.contractSymbol || 'Contract'} details`)}" data-chain-contract="${escapeHtml(row.contractSymbol || '')}" x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${barWidth.toFixed(2)}" height="${(margin.top + plotHeight - y).toFixed(2)}" rx="6" fill="${fill}" fill-opacity="${row.passesPrimaryScreen ? '0.88' : '0.56'}">
            <title>${escapeHtml(`${row.contractSymbol || 'Contract'} · strike ${formatNumber(row.strike, 2)} · mid ${formatNumber(row.premium, 2)} · spread ${spread}`)}</title>
          </rect>
        `;
      }).join('')}
      ${buildIndexedTicks(chartRows, 5).map(({ row, index }) => {
        const x = margin.left + (((index + 0.5) * plotWidth) / chartRows.length);
        return `<text x="${x.toFixed(2)}" y="${height - 10}" text-anchor="middle" class="chart-axis-label">${escapeHtml(formatNumber(row.strike, 0))}</text>`;
      }).join('')}
      ${spreadLinePoints.length > 1 ? `<polyline fill="none" stroke="#ef4444" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" points="${spreadLinePoints.join(' ')}"></polyline>` : ''}
      <text x="${margin.left}" y="14" class="chart-axis-title">Mark Price Mid</text>
      <text x="${width - margin.right + 12}" y="14" class="chart-axis-title">Spread % of Mid</text>
      <text x="${width / 2}" y="${height - 4}" text-anchor="middle" class="chart-axis-title">Strike</text>
      <text x="${margin.left - 12}" y="${margin.top + 12}" text-anchor="end" class="chart-axis-label">${escapeHtml(formatNumber(premiumDomain[1], 2))}</text>
      <text x="${margin.left - 12}" y="${margin.top + plotHeight}" text-anchor="end" class="chart-axis-label">${escapeHtml(formatNumber(premiumDomain[0], 2))}</text>
      <text x="${width - margin.right + 14}" y="${margin.top + 12}" class="chart-axis-label">${escapeHtml(formatPercentValue(spreadDomain[1], 1))}</text>
      <text x="${width - margin.right + 14}" y="${margin.top + plotHeight}" class="chart-axis-label">${escapeHtml(formatPercentValue(spreadDomain[0], 1))}</text>
    </svg>
  `;
  bindChainInteractions(elements.chainPremiumChart, (row) => [
    { label: 'Mark Price Mid', value: formatNumber(row.mark_price_mid, 2) },
    { label: 'Spread % Mid', value: formatPercentValue(row.bid_ask_spread_pct_of_mid, 1) },
    { label: 'Volume', value: formatCompactNumber(row.volume) },
    { label: 'Open Interest', value: formatCompactNumber(row.open_interest) },
  ]);
}

function getIvPremiumColor(value, domain) {
  if (value === null || value === undefined || !Number.isFinite(Number(value)) || !domain) {
    return '#94a3b8';
  }
  const [min, max] = domain;
  const ratio = max === min ? 0.5 : Math.max(0, Math.min(1, (Number(value) - min) / (max - min)));
  const lightness = 72 - (ratio * 34);
  return `hsl(42 92% ${lightness.toFixed(1)}%)`;
}

function renderThetaChart(rows) {
  const chartRows = sampleRowsForChart(rows, 320)
    .map((row) => ({
      contractSymbol: row.contract_symbol,
      deltaAbs: Number(row.delta_abs),
      thetaEfficiency: Number(row.theta_efficiency),
      ivAdjustedPremiumPerDay: Number(row.iv_adjusted_premium_per_day),
      riskLevel: row.risk_level,
    }))
    .filter((row) => Number.isFinite(row.deltaAbs) && Number.isFinite(row.thetaEfficiency));

  if (chartRows.length === 0) {
    renderChartEmpty(elements.chainThetaChart, 'Theta chart unavailable');
    return;
  }

  const width = 960;
  const height = 280;
  const margin = { top: 24, right: 24, bottom: 42, left: 56 };
  const xDomain = expandExtent(getNumericExtent(chartRows.map((row) => row.deltaAbs)), 0.08) || [0, 1];
  const yDomain = expandExtent(getNumericExtent(chartRows.map((row) => row.thetaEfficiency)), 0.12) || [0, 1];
  const colorDomain = getNumericExtent(chartRows.map((row) => row.ivAdjustedPremiumPerDay));
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;

  elements.chainThetaChart.innerHTML = `
    <div class="chart-legend">
      <span><i class="legend-dot legend-dot-gold"></i>Higher IV-adjusted premium/day</span>
      <span><i class="legend-dot" style="background:#94a3b8"></i>Lower IV-adjusted premium/day</span>
    </div>
    <svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Theta efficiency versus delta chart">
      <rect x="${margin.left}" y="${margin.top}" width="${plotWidth}" height="${plotHeight}" rx="10" fill="rgba(148,163,184,0.06)"></rect>
      ${buildNumericTicks(xDomain, 5).map((tick) => {
        const x = scaleLinear(tick, xDomain, [margin.left, margin.left + plotWidth]);
        return `
          <line x1="${x.toFixed(2)}" y1="${margin.top}" x2="${x.toFixed(2)}" y2="${margin.top + plotHeight}" stroke="rgba(148,163,184,0.18)"></line>
          <text x="${x.toFixed(2)}" y="${height - 10}" text-anchor="middle" class="chart-axis-label">${tick.toFixed(2)}</text>
        `;
      }).join('')}
      ${chartRows.map((row) => {
        const cx = scaleLinear(row.deltaAbs, xDomain, [margin.left, margin.left + plotWidth]);
        const cy = scaleLinear(row.thetaEfficiency, yDomain, [margin.top + plotHeight, margin.top]);
        const fill = getIvPremiumColor(row.ivAdjustedPremiumPerDay, colorDomain);
        return `
          <circle class="chart-hit-target" tabindex="0" role="button" aria-label="${escapeHtml(`${row.contractSymbol || 'Contract'} details`)}" data-chain-contract="${escapeHtml(row.contractSymbol || '')}" cx="${cx.toFixed(2)}" cy="${cy.toFixed(2)}" r="6" fill="${fill}" stroke="${getRiskColor(row.riskLevel)}" stroke-width="1.4">
            <title>${escapeHtml(`${row.contractSymbol || 'Contract'} · delta ${formatNumber(row.deltaAbs, 3)} · theta efficiency ${formatNumber(row.thetaEfficiency, 2)} · IV-adjusted premium/day ${formatNumber(row.ivAdjustedPremiumPerDay, 2)}`)}</title>
          </circle>
        `;
      }).join('')}
      <text x="${margin.left}" y="14" class="chart-axis-title">Theta Efficiency</text>
      <text x="${width / 2}" y="${height - 4}" text-anchor="middle" class="chart-axis-title">Delta Abs</text>
      <text x="${margin.left - 12}" y="${margin.top + 12}" text-anchor="end" class="chart-axis-label">${escapeHtml(formatNumber(yDomain[1], 2))}</text>
      <text x="${margin.left - 12}" y="${margin.top + plotHeight}" text-anchor="end" class="chart-axis-label">${escapeHtml(formatNumber(yDomain[0], 2))}</text>
    </svg>
  `;
  bindChainInteractions(elements.chainThetaChart, (row) => [
    { label: 'Theta Efficiency', value: formatNumber(row.theta_efficiency, 2) },
    { label: 'Delta Abs', value: formatNumber(row.delta_abs, 3) },
    { label: 'IV-Adj Premium/Day', value: formatNumber(row.iv_adjusted_premium_per_day, 2) },
    { label: 'Risk', value: String(row.risk_level || '—') },
  ]);
}

function renderChainSummary(rows) {
  if (rows.length === 0) {
    elements.chainSummaryPanel.innerHTML = `
      <div class="chart-empty">
        <strong>Chain summary unavailable</strong>
        <span>No rows match the current chain selection.</span>
      </div>
    `;
    return;
  }

  const passCount = rows.filter((row) => isTruthyValue(row.passes_primary_screen)).length;
  const riskCounts = ['LOW', 'MEDIUM', 'HIGH'].map((riskLevel) => ({
    riskLevel,
    count: rows.filter((row) => String(row.risk_level || '').toUpperCase() === riskLevel).length,
  }));
  const totalVolume = rows.reduce((sum, row) => sum + (Number.isFinite(Number(row.volume)) ? Number(row.volume) : 0), 0);
  const totalOpenInterest = rows.reduce((sum, row) => sum + (Number.isFinite(Number(row.open_interest)) ? Number(row.open_interest) : 0), 0);
  const strikeLiquidity = rows
    .map((row) => ({
      strike: Number(row.strike),
      volume: Number.isFinite(Number(row.volume)) ? Number(row.volume) : 0,
      openInterest: Number.isFinite(Number(row.open_interest)) ? Number(row.open_interest) : 0,
    }))
    .filter((row) => Number.isFinite(row.strike))
    .sort((left, right) => (right.openInterest + right.volume) - (left.openInterest + left.volume))
    .slice(0, 6);
  const maxLiquidity = Math.max(...strikeLiquidity.map((row) => Math.max(row.volume, row.openInterest)), 1);

  elements.chainSummaryPanel.innerHTML = `
    <div class="chain-summary-stats">
      <article class="chain-stat-card">
        <span>Contracts</span>
        <strong>${rows.length.toLocaleString()}</strong>
      </article>
      <article class="chain-stat-card">
        <span>Primary Screen Pass</span>
        <strong>${formatPercentValue(passCount / rows.length, 1)}</strong>
      </article>
      <article class="chain-stat-card">
        <span>Total Volume</span>
        <strong>${formatCompactNumber(totalVolume)}</strong>
      </article>
      <article class="chain-stat-card">
        <span>Total OI</span>
        <strong>${formatCompactNumber(totalOpenInterest)}</strong>
      </article>
    </div>
    <div class="chain-summary-section">
      <h3>Risk Mix</h3>
      <div class="chain-bar-list">
        ${riskCounts.map((item) => {
          const share = rows.length === 0 ? 0 : item.count / rows.length;
          return `
            <div class="chain-bar-row">
              <span class="chain-bar-label">${escapeHtml(item.riskLevel)}</span>
              <div class="chain-bar-track"><div class="chain-bar-fill" style="width:${(share * 100).toFixed(1)}%; background:${getRiskColor(item.riskLevel)};"></div></div>
              <strong>${item.count.toLocaleString()} · ${formatPercentValue(share, 1)}</strong>
            </div>
          `;
        }).join('')}
      </div>
    </div>
    <div class="chain-summary-section">
      <h3>Liquidity by Strike</h3>
      <div class="chain-liquidity-list">
        ${strikeLiquidity.map((item) => `
          <div class="chain-liquidity-row">
            <div class="chain-liquidity-header">
              <span>Strike ${escapeHtml(formatNumber(item.strike, 2))}</span>
              <strong>OI ${escapeHtml(formatCompactNumber(item.openInterest))} · Vol ${escapeHtml(formatCompactNumber(item.volume))}</strong>
            </div>
            <div class="chain-liquidity-bars">
              <div class="chain-liquidity-track"><div class="chain-liquidity-fill chain-liquidity-fill-oi" style="width:${((item.openInterest / maxLiquidity) * 100).toFixed(1)}%;"></div></div>
              <div class="chain-liquidity-track"><div class="chain-liquidity-fill chain-liquidity-fill-volume" style="width:${((item.volume / maxLiquidity) * 100).toFixed(1)}%;"></div></div>
            </div>
          </div>
        `).join('')}
      </div>
    </div>
  `;
}

function renderChainView() {
  syncChainViewState();
  const tickers = getChainTickerOptions();
  if (tickers.length === 0 || !state.chainView.ticker || !state.chainView.expiration) {
    elements.chainStatus.textContent = 'No option rows available for chain visualization.';
    renderChartEmpty(elements.chainDeltaChart, 'Delta chart unavailable');
    renderChartEmpty(elements.chainPremiumChart, 'Premium chart unavailable');
    renderChartEmpty(elements.chainThetaChart, 'Theta chart unavailable');
    renderChainSummary([]);
    return;
  }

  const chainRows = getSelectedChainRows();
  const optionTypeLabel = state.chainView.optionType === 'all'
    ? 'calls + puts'
    : `${state.chainView.optionType}s`;
  elements.chainStatus.textContent = `${state.chainView.ticker} · ${state.chainView.expiration} · ${chainRows.length.toLocaleString()} ${optionTypeLabel}`;
  renderDeltaChart(chainRows);
  renderPremiumChart(chainRows);
  renderThetaChart(chainRows);
  renderChainSummary(chainRows);
}

function scheduleChainRender(force = false) {
  syncChainViewState();

  if (!force && state.activeTab !== 'chain') {
    const tickers = getChainTickerOptions();
    if (tickers.length === 0 || !state.chainView.ticker || !state.chainView.expiration) {
      elements.chainStatus.textContent = 'No option rows available for chain visualization.';
    } else {
      elements.chainStatus.textContent = `${state.chainView.ticker} · ${state.chainView.expiration} · chain view ready`;
    }
    return;
  }

  const token = state.chainRenderToken + 1;
  state.chainRenderToken = token;
  elements.chainStatus.textContent = 'Rendering chain view...';

  window.setTimeout(() => {
    if (state.chainRenderToken !== token) return;
    try {
      renderChainView();
    } catch (error) {
      elements.chainStatus.textContent = `Chain view error: ${error.message}`;
      renderChartEmpty(elements.chainDeltaChart, 'Delta chart unavailable');
      renderChartEmpty(elements.chainPremiumChart, 'Premium chart unavailable');
      renderChartEmpty(elements.chainThetaChart, 'Theta chart unavailable');
      renderChainSummary([]);
    }
  }, 0);
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
      appendCellValue(td, column.name, row[column.name]);
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
    label.innerHTML = renderFieldLabel(column.name);

    const value = document.createElement('div');
    value.className = 'row-detail-value';
    value.textContent = formatCell(row[column.name]);

    const description = document.createElement('div');
    description.className = 'row-detail-description';
    description.textContent = getFieldDescription(column.name);

    item.appendChild(label);
    item.appendChild(value);
    item.appendChild(description);
    elements.rowDetailGrid.appendChild(item);
  });

  elements.rowModal.classList.add('open');
  elements.rowModal.setAttribute('aria-hidden', 'false');
  document.body.classList.add('modal-open');
  requestAnimationFrame(() => {
    syncRowDetailCardHeights();
  });
}

function closeRowModal() {
  state.selectedRow = null;
  elements.rowModal.classList.remove('open');
  elements.rowModal.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('modal-open');
}

function syncRowDetailCardHeights() {
  const items = [...elements.rowDetailGrid.querySelectorAll('.row-detail-item')];
  if (items.length === 0) return;

  items.forEach((item) => {
    item.style.height = '';
    item.style.minHeight = '';
  });

  const tallest = items.reduce((maxHeight, item) => Math.max(maxHeight, item.scrollHeight), 0);
  const targetHeight = Math.max(112, tallest);
  items.forEach((item) => {
    item.style.height = `${targetHeight}px`;
    item.style.minHeight = `${targetHeight}px`;
  });
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function parseTableCells(line) {
  const trimmed = line.trim();
  if (!trimmed.includes('|')) return [];
  return trimmed
    .replace(/^\|/, '')
    .replace(/\|$/, '')
    .split('|')
    .map((cell) => cell.trim());
}

function isMarkdownTableSeparator(line) {
  const cells = parseTableCells(line);
  if (cells.length === 0) return false;
  return cells.every((cell) => /^:?-{3,}:?$/.test(cell));
}

function renderMarkdownTable(lines) {
  const [headerLine, , ...bodyLines] = lines;
  const headers = parseTableCells(headerLine);
  const rows = bodyLines
    .map((line) => parseTableCells(line))
    .filter((cells) => cells.length > 0);

  const headHtml = headers.map((cell) => `<th>${inlineMarkdown(cell)}</th>`).join('');
  const bodyHtml = rows.map((cells) => {
    const normalizedCells = headers.map((_, index) => cells[index] || '');
    return `<tr>${normalizedCells.map((cell) => `<td>${inlineMarkdown(cell)}</td>`).join('')}</tr>`;
  }).join('');

  return `
    <div class="reference-table-wrap">
      <table class="reference-table">
        <thead><tr>${headHtml}</tr></thead>
        <tbody>${bodyHtml}</tbody>
      </table>
    </div>
  `;
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

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
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

    const nextLine = lines[index + 1];
    if (nextLine && line.includes('|') && isMarkdownTableSeparator(nextLine)) {
      closeList();
      const tableLines = [line, nextLine];
      index += 2;
      while (index < lines.length) {
        const candidate = lines[index];
        if (!candidate.trim() || !candidate.includes('|')) {
          index -= 1;
          break;
        }
        tableLines.push(candidate);
        index += 1;
      }
      if (index === lines.length) {
        index -= 1;
      }
      html += renderMarkdownTable(tableLines);
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

function formatFileOptionLabel(fileName) {
  const match = /^options_engine_output_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.csv$/.exec(fileName);
  if (!match) return fileName;

  const [, year, month, day, hour, minute, second] = match;
  const parsedDate = new Date(
    Number(year),
    Number(month) - 1,
    Number(day),
    Number(hour),
    Number(minute),
    Number(second),
  );
  if (Number.isNaN(parsedDate.getTime())) return fileName;

  return new Intl.DateTimeFormat(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  }).format(parsedDate);
}

async function loadFiles() {
  const payload = await fetchJson('/api/files');
  state.files = payload.files;
  elements.fileSelect.innerHTML = '';

  state.files.forEach((file) => {
    const option = document.createElement('option');
    option.value = file.name;
    option.textContent = formatFileOptionLabel(file.name);
    option.title = file.name;
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
    elements.summaryTickerGrid.innerHTML = '';
  }
  renderTable();
  scheduleChainRender(false);
}

async function loadReference() {
  const payload = await fetchJson('/api/reference');
  elements.readmeContent.innerHTML = renderMarkdown(payload.markdown);
}

function activateTab(tabName) {
  const nextTab = VALID_TABS.has(tabName) ? tabName : 'table';
  if (state.activeTab === nextTab) {
    syncTabUrl(nextTab);
  }
  state.activeTab = nextTab;
  elements.tabButtons.forEach((button) => {
    button.classList.toggle('active', button.dataset.tab === nextTab);
  });
  elements.summaryTab.classList.toggle('active', nextTab === 'summary');
  elements.tableTab.classList.toggle('active', nextTab === 'table');
  elements.chainTab.classList.toggle('active', nextTab === 'chain');
  elements.readmeTab.classList.toggle('active', nextTab === 'readme');
  syncTabUrl(nextTab);
  if (nextTab === 'chain') {
    scheduleChainRender(true);
  }
}

function updateThemeToggleLabel(theme) {
  elements.themeToggle.textContent = theme === 'dark' ? 'Light' : 'Dark';
}

function setTheme(theme) {
  document.documentElement.dataset.theme = theme;
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
  activateTab(getTabFromUrl());
  await Promise.all([loadFiles(), loadReference()]);
  if (state.files.length > 0) {
    await loadData(state.files[0].name);
  } else {
    elements.tableStatus.textContent = 'No CSV files found in the output directory.';
    elements.chainStatus.textContent = 'No CSV files found in the output directory.';
    renderChartEmpty(elements.chainDeltaChart, 'Delta chart unavailable');
    renderChartEmpty(elements.chainPremiumChart, 'Premium chart unavailable');
    renderChartEmpty(elements.chainThetaChart, 'Theta chart unavailable');
    renderChainSummary([]);
  }

  elements.fileSelect.addEventListener('change', async (event) => {
    await loadData(event.target.value);
  });

  elements.pageSizeSelect.addEventListener('change', (event) => {
    state.pageSize = event.target.value === 'all' ? 'all' : Number(event.target.value);
    state.currentPage = 1;
    renderTable();
  });

  elements.chainTickerSelect.addEventListener('change', (event) => {
    state.chainView.ticker = event.target.value || null;
    state.chainView.expiration = null;
    scheduleChainRender(true);
  });

  elements.chainExpirationSelect.addEventListener('change', (event) => {
    state.chainView.expiration = event.target.value || null;
    scheduleChainRender(true);
  });

  elements.chainOptionTypeSelect.addEventListener('change', (event) => {
    state.chainView.optionType = event.target.value;
    scheduleChainRender(true);
  });

  elements.chainDeltaXAxisSelect.addEventListener('change', (event) => {
    state.chainView.deltaXAxis = event.target.value;
    scheduleChainRender(true);
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
  window.addEventListener('resize', () => {
    if (state.selectedRow) {
      syncRowDetailCardHeights();
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

  window.addEventListener('popstate', () => {
    activateTab(getTabFromUrl());
  });

  elements.themeToggle.addEventListener('click', () => {
    setTheme(document.body.dataset.theme === 'dark' ? 'light' : 'dark');
  });
}

initialize().catch((error) => {
  elements.tableStatus.textContent = error.message;
});
