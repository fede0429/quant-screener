/**
 * app.js — Main Application Logic
 * A股量化选股系统
 */

'use strict';

// ─── App State ────────────────────────────────────────────────────────────────
const AppState = {
  data: null,
  screenerResults: [],
  selectedStock: null,
  sortCol: 'compositeScore',
  sortDir: 'desc',
  weights: { value: 25, growth: 25, quality: 25, momentum: 25 },
  filters: {},
  backtestConfig: {
    frequency: 'monthly',
    topN: 10,
    startDate: null,
    endDate: null
  },
  backtestResults: null
};

// ─── Number Formatting ────────────────────────────────────────────────────────
const Fmt = {
  price: v => v == null ? '--' : v >= 1000 ? v.toFixed(1) : v.toFixed(2),
  pct: v => v == null ? '--' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`,
  pctNoSign: v => v == null ? '--' : `${v.toFixed(2)}%`,
  num1: v => v == null ? '--' : v.toFixed(1),
  num2: v => v == null ? '--' : v.toFixed(2),
  score: v => v == null ? '--' : v.toFixed(1),

  /** Market cap in 亿yuan */
  mcap: v => {
    if (v == null) return '--';
    if (v >= 10000) return `${(v / 10000).toFixed(2)}万亿`;
    return `${v.toFixed(2)}亿`;
  },

  /** Raw number to 万/亿 */
  yuan: v => {
    if (v == null) return '--';
    const abs = Math.abs(v);
    if (abs >= 1e12) return `${(v / 1e12).toFixed(2)}万亿`;
    if (abs >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
    if (abs >= 1e4) return `${(v / 1e4).toFixed(2)}万`;
    return v.toFixed(0);
  },

  vol: v => {
    if (v == null) return '--';
    if (v >= 1e8) return `${(v / 1e8).toFixed(2)}亿`;
    if (v >= 1e4) return `${(v / 1e4).toFixed(0)}万`;
    return v.toFixed(0);
  }
};

// ─── Color helpers (Chinese convention: RED=UP, GREEN=DOWN) ───────────────────
function colorClass(val) {
  if (val > 0) return 'val-up';
  if (val < 0) return 'val-down';
  return 'val-neutral';
}

function scoreColor(score) {
  if (score >= 70) return '#3B82F6';
  if (score >= 55) return '#8B5CF6';
  if (score >= 40) return '#F59E0B';
  return '#4E5A6E';
}

// ─── Tab Navigation ───────────────────────────────────────────────────────────
function initTabs() {
  const tabBtns = document.querySelectorAll('.tab-btn');
  const tabContents = document.querySelectorAll('.tab-content');

  tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      const target = btn.dataset.tab;
      tabBtns.forEach(b => b.classList.toggle('active', b.dataset.tab === target));
      tabContents.forEach(c => {
        const wasActive = c.classList.contains('active');
        c.classList.toggle('active', c.id === `tab-${target}`);
        if (!wasActive && c.id === `tab-${target}`) {
          onTabActivated(target);
        }
      });
    });
  });
}

function onTabActivated(tab) {
  if (tab === 'analysis' && AppState.selectedStock) {
    renderStockAnalysis(AppState.selectedStock);
  }
}

// ─── Clock ────────────────────────────────────────────────────────────────────
function startClock() {
  function update() {
    const now = new Date();
    const timeEl = document.getElementById('header-time');
    const dateEl = document.getElementById('header-date');
    if (timeEl) {
      timeEl.textContent = now.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    }
    if (dateEl) {
      dateEl.textContent = now.toLocaleDateString('zh-CN', { year: 'numeric', month: 'long', day: 'numeric', weekday: 'short' });
    }
  }
  update();
  setInterval(update, 1000);
}

// ─── Data Loading ─────────────────────────────────────────────────────────────
async function loadData() {
  const loadingEl = document.getElementById('app-loading');
  const statusEl = document.getElementById('loading-status');
  if (loadingEl) loadingEl.style.display = 'flex';
  if (statusEl) statusEl.textContent = '正在加载数据...';

  try {
    const response = await fetch('./data/demo_data.json');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    if (statusEl) statusEl.textContent = '解析数据中...';
    const data = await response.json();
    AppState.data = data;

    if (statusEl) statusEl.textContent = '初始化完成';

    // Init after data load
    initSectorFilters(data.stocks);
    initDatePickers(data.stocks);
    initWeightSliders();
    initFilterControls();
    initBacktestControls();

    // Run initial screening
    runScreener();

    if (loadingEl) {
      loadingEl.style.opacity = '0';
      loadingEl.style.transition = 'opacity 0.4s ease';
      setTimeout(() => { loadingEl.remove(); }, 400);
    }
  } catch (err) {
    console.error('数据加载失败:', err);
    if (statusEl) statusEl.textContent = `加载失败: ${err.message}`;
    if (loadingEl) {
      const spinner = loadingEl.querySelector('.loading-spinner');
      if (spinner) spinner.style.display = 'none';
    }
  }
}

// ─── Sector Filter Chips ──────────────────────────────────────────────────────
function initSectorFilters(stocks) {
  const sectors = [...new Set(stocks.map(s => s.sector))].sort();
  const container = document.getElementById('sector-chips');
  if (!container) return;

  container.innerHTML = sectors.map(sector => `
    <label class="sector-chip selected" data-sector="${sector}">
      <input type="checkbox" value="${sector}" checked>
      ${sector}
    </label>
  `).join('');

  container.querySelectorAll('.sector-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      chip.classList.toggle('selected');
      chip.querySelector('input').checked = chip.classList.contains('selected');
    });
  });
}

function getSelectedSectors() {
  const chips = document.querySelectorAll('#sector-chips .sector-chip');
  const selected = [];
  chips.forEach(chip => {
    if (chip.classList.contains('selected')) {
      selected.push(chip.dataset.sector);
    }
  });
  return selected;
}

// ─── Date Pickers Init ────────────────────────────────────────────────────────
function initDatePickers(stocks) {
  if (!stocks || stocks.length === 0) return;
  const dates = stocks[0].prices.map(p => p.date);
  const minDate = dates[0];
  const maxDate = dates[dates.length - 1];

  // Backtest dates
  const startInput = document.getElementById('bt-start-date');
  const endInput = document.getElementById('bt-end-date');
  if (startInput) {
    startInput.min = minDate;
    startInput.max = maxDate;
    // Default: 2 years back from end
    const defaultStart = dates[Math.max(0, dates.length - 500)];
    startInput.value = defaultStart;
    AppState.backtestConfig.startDate = defaultStart;
  }
  if (endInput) {
    endInput.min = minDate;
    endInput.max = maxDate;
    endInput.value = maxDate;
    AppState.backtestConfig.endDate = maxDate;
  }
}

// ─── Weight Sliders ───────────────────────────────────────────────────────────
function initWeightSliders() {
  const factors = ['value', 'growth', 'quality', 'momentum'];

  factors.forEach(f => {
    const slider = document.getElementById(`weight-${f}`);
    if (!slider) return;

    slider.addEventListener('input', () => {
      AppState.weights[f] = parseInt(slider.value);
      updateWeightDisplay();
    });
  });
  updateWeightDisplay();
}

function updateWeightDisplay() {
  const factors = ['value', 'growth', 'quality', 'momentum'];
  const total = factors.reduce((s, f) => s + AppState.weights[f], 0);

  factors.forEach(f => {
    const valEl = document.getElementById(`weight-val-${f}`);
    const fillEl = document.getElementById(`weight-fill-${f}`);
    if (valEl) {
      const normalized = total > 0 ? Math.round(AppState.weights[f] / total * 100) : 25;
      valEl.textContent = `${normalized}%`;
    }
    if (fillEl) {
      const pct = total > 0 ? (AppState.weights[f] / total * 100) : 25;
      fillEl.style.width = `${pct}%`;
    }
  });

  const totalEl = document.getElementById('weight-total');
  if (totalEl) {
    totalEl.textContent = `${total}`;
    totalEl.className = `weight-total-val ${total === 100 ? 'ok' : total > 100 ? 'over' : ''}`;
  }
}

// ─── Filter Controls ──────────────────────────────────────────────────────────
function initFilterControls() {
  // Collapsible sections
  document.querySelectorAll('.filter-section-header').forEach(header => {
    header.addEventListener('click', () => {
      const section = header.closest('.filter-section');
      section.classList.toggle('collapsed');
    });
  });

  // Reset button
  const resetBtn = document.getElementById('reset-btn');
  if (resetBtn) {
    resetBtn.addEventListener('click', resetFilters);
  }

  // Run button
  const runBtn = document.getElementById('run-btn');
  if (runBtn) {
    runBtn.addEventListener('click', runScreener);
  }
}

function resetFilters() {
  // Reset all number inputs
  document.querySelectorAll('.filter-input').forEach(el => {
    el.value = el.defaultValue || '';
  });

  // Reset sector chips
  document.querySelectorAll('#sector-chips .sector-chip').forEach(chip => {
    chip.classList.add('selected');
    chip.querySelector('input').checked = true;
  });

  // Reset weights
  AppState.weights = { value: 25, growth: 25, quality: 25, momentum: 25 };
  ['value', 'growth', 'quality', 'momentum'].forEach(f => {
    const slider = document.getElementById(`weight-${f}`);
    if (slider) slider.value = 25;
  });
  updateWeightDisplay();

  runScreener();
}

function getFilters() {
  function getNum(id) {
    const el = document.getElementById(id);
    if (!el || el.value === '') return null;
    const v = parseFloat(el.value);
    return isFinite(v) ? v : null;
  }

  return {
    peMin: getNum('f-pe-min'),
    peMax: getNum('f-pe-max'),
    pbMin: getNum('f-pb-min'),
    pbMax: getNum('f-pb-max'),
    divYieldMin: getNum('f-div-min'),
    psMax: getNum('f-ps-max'),
    revGrowthMin: getNum('f-rev-growth-min'),
    netIncomeGrowthMin: getNum('f-ni-growth-min'),
    roeMin: getNum('f-roe-min'),
    grossMarginMin: getNum('f-gm-min'),
    debtRatioMax: getNum('f-debt-max'),
    currentRatioMin: getNum('f-cr-min'),
    mom20Min: getNum('f-mom20-min'),
    mom20Max: getNum('f-mom20-max'),
    rsiMin: getNum('f-rsi-min'),
    rsiMax: getNum('f-rsi-max'),
    sectors: getSelectedSectors()
  };
}

// ─── Run Screener ─────────────────────────────────────────────────────────────
function runScreener() {
  if (!AppState.data) return;

  const runBtn = document.getElementById('run-btn');
  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = '计算中...';
  }

  // Use setTimeout to let UI update before heavy computation
  setTimeout(() => {
    const filters = getFilters();
    AppState.filters = filters;

    const results = Screener.runScreener(AppState.data.stocks, filters, AppState.weights);
    AppState.screenerResults = results;

    renderScreenerResults(results);

    if (runBtn) {
      runBtn.disabled = false;
      runBtn.textContent = '开始选股';
    }
  }, 30);
}

// ─── Render Screener Results ──────────────────────────────────────────────────
function renderScreenerResults(results) {
  renderKPICards(results);
  renderResultsTable(results);
  renderBottomCharts(results);
}

function renderKPICards(results) {
  const stats = Screener.computeStats(results);

  const setKPI = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
  };

  setKPI('kpi-count', stats.count);
  setKPI('kpi-avg-score', Fmt.score(stats.avgScore));
  setKPI('kpi-avg-pe', Fmt.num1(stats.avgPE));
  setKPI('kpi-avg-roe', `${Fmt.num1(stats.avgROE)}%`);
}

function renderResultsTable(results) {
  const countEl = document.getElementById('table-count');
  if (countEl) countEl.textContent = `共 ${results.length} 只`;

  const tbody = document.getElementById('results-tbody');
  if (!tbody) return;

  if (results.length === 0) {
    tbody.innerHTML = `<tr><td colspan="14" style="text-align:center;padding:40px;color:var(--text-muted)">
      没有符合条件的股票，请调整筛选条件
    </td></tr>`;
    return;
  }

  // Sort
  const sorted = sortResults([...results]);

  tbody.innerHTML = sorted.map((s, i) => {
    const f = s.financials;
    const t = s.technicals;
    const rankClass = i === 0 ? 'rank-1' : i === 1 ? 'rank-2' : i === 2 ? 'rank-3' : 'rank-other';
    const sc = s.compositeScore;

    // Price change (compare last two prices)
    const prices = s.prices;
    const lastPrice = prices[prices.length - 1];
    const prevPrice = prices[prices.length - 2];
    const priceChange = prevPrice ? (lastPrice.close - prevPrice.close) / prevPrice.close * 100 : 0;
    const priceClass = priceChange > 0 ? 'val-up' : priceChange < 0 ? 'val-down' : 'val-neutral';

    return `<tr data-code="${s.code}" onclick="selectStock('${s.code}')">
      <td><span class="rank-badge ${rankClass}">${i + 1}</span></td>
      <td><span class="stock-code">${s.code}</span></td>
      <td><span class="stock-name">${s.name}</span></td>
      <td><span class="industry-tag">${s.industry}</span></td>
      <td>
        <div class="score-bar-cell">
          <div class="score-bar-track">
            <div class="score-bar-fill" style="width:${sc}%;background:${scoreColor(sc)}"></div>
          </div>
          <span class="score-val" style="color:${scoreColor(sc)}">${Fmt.score(sc)}</span>
        </div>
      </td>
      <td style="color:${scoreColor(s.valueScore)}">${Fmt.score(s.valueScore)}</td>
      <td style="color:${scoreColor(s.growthScore)}">${Fmt.score(s.growthScore)}</td>
      <td style="color:${scoreColor(s.qualityScore)}">${Fmt.score(s.qualityScore)}</td>
      <td style="color:${scoreColor(s.momentumScore)}">${Fmt.score(s.momentumScore)}</td>
      <td class="${priceClass}">¥${Fmt.price(t.price || lastPrice?.close)}</td>
      <td>${Fmt.num1(f.pe)}</td>
      <td>${Fmt.num2(f.pb)}</td>
      <td>${Fmt.num1(f.roe)}%</td>
      <td class="${colorClass(f.revenue_growth_yoy)}">${Fmt.pct(f.revenue_growth_yoy)}</td>
      <td class="${colorClass(t.momentum_20d)}">${Fmt.pct(t.momentum_20d)}</td>
    </tr>`;
  }).join('');

  // Update sort header indicators
  updateSortIndicators();
}

function sortResults(results) {
  const col = AppState.sortCol;
  const dir = AppState.sortDir === 'asc' ? 1 : -1;

  return results.sort((a, b) => {
    let va, vb;
    switch (col) {
      case 'rank': va = a.rank; vb = b.rank; break;
      case 'compositeScore': va = a.compositeScore; vb = b.compositeScore; break;
      case 'valueScore': va = a.valueScore; vb = b.valueScore; break;
      case 'growthScore': va = a.growthScore; vb = b.growthScore; break;
      case 'qualityScore': va = a.qualityScore; vb = b.qualityScore; break;
      case 'momentumScore': va = a.momentumScore; vb = b.momentumScore; break;
      case 'pe': va = a.financials.pe; vb = b.financials.pe; break;
      case 'pb': va = a.financials.pb; vb = b.financials.pb; break;
      case 'roe': va = a.financials.roe; vb = b.financials.roe; break;
      case 'revGrowth': va = a.financials.revenue_growth_yoy; vb = b.financials.revenue_growth_yoy; break;
      case 'mom20': va = a.technicals.momentum_20d; vb = b.technicals.momentum_20d; break;
      case 'price': {
        va = a.technicals.price || a.prices[a.prices.length - 1]?.close || 0;
        vb = b.technicals.price || b.prices[b.prices.length - 1]?.close || 0;
        break;
      }
      default: va = a.compositeScore; vb = b.compositeScore;
    }
    if (va == null) va = -Infinity;
    if (vb == null) vb = -Infinity;
    return (va < vb ? -1 : va > vb ? 1 : 0) * dir;
  });
}

function initTableSort() {
  document.querySelectorAll('thead th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.sort;
      if (AppState.sortCol === col) {
        AppState.sortDir = AppState.sortDir === 'asc' ? 'desc' : 'asc';
      } else {
        AppState.sortCol = col;
        AppState.sortDir = 'desc';
      }
      renderResultsTable(AppState.screenerResults);
    });
  });
}

function updateSortIndicators() {
  document.querySelectorAll('thead th[data-sort]').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    if (th.dataset.sort === AppState.sortCol) {
      th.classList.add(AppState.sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
    }
  });
}

// ─── Bottom Charts ────────────────────────────────────────────────────────────
function renderBottomCharts(results) {
  const sectorDist = Screener.getSectorDistribution(results);
  Charts.renderSectorChart('sector-chart', sectorDist);

  const histogram = Screener.getScoreHistogram(results, 10);
  Charts.renderScoreHistogram('score-histogram', histogram);
}

// ─── Stock Selection ──────────────────────────────────────────────────────────
function selectStock(code) {
  const stock = AppState.screenerResults.find(s => s.code === code) ||
    AppState.data?.stocks.find(s => s.code === code);
  if (!stock) return;

  AppState.selectedStock = stock;

  // Highlight row
  document.querySelectorAll('#results-tbody tr').forEach(tr => {
    tr.classList.toggle('selected', tr.dataset.code === code);
  });

  // Switch to analysis tab
  document.querySelector('[data-tab="analysis"]').click();
}

// ─── Stock Analysis Tab ───────────────────────────────────────────────────────
function renderStockAnalysis(stock) {
  const container = document.getElementById('analysis-content');
  if (!container) return;

  const f = stock.financials;
  const t = stock.technicals;
  const prices = stock.prices;
  const lastPrice = prices[prices.length - 1];
  const prevPrice = prices[prices.length - 2];
  const priceChange = prevPrice ? (lastPrice.close - prevPrice.close) / prevPrice.close * 100 : 0;
  const priceAbs = lastPrice.close - (prevPrice?.close || lastPrice.close);
  const changeClass = priceChange > 0 ? 'up' : priceChange < 0 ? 'down' : '';
  const changeSign = priceChange > 0 ? '+' : '';

  container.style.display = 'block';
  document.getElementById('no-stock-msg').style.display = 'none';

  // Render HTML
  document.getElementById('analysis-stock-name').textContent = stock.name;
  document.getElementById('analysis-stock-code').textContent = stock.code;
  document.getElementById('analysis-stock-sector').textContent = `${stock.sector} · ${stock.industry}`;
  document.getElementById('analysis-price').textContent = `¥${Fmt.price(lastPrice.close)}`;
  document.getElementById('analysis-change').textContent = `${changeSign}${Fmt.price(priceAbs)} (${changeSign}${Fmt.pctNoSign(priceChange)})`;
  document.getElementById('analysis-change').className = `stock-change ${changeClass}`;

  // Financials
  const finData = [
    ['市盈率(PE)', Fmt.num1(f.pe) + 'x'],
    ['市净率(PB)', Fmt.num2(f.pb) + 'x'],
    ['市销率(PS)', Fmt.num2(f.ps) + 'x'],
    ['净资产收益率', Fmt.num1(f.roe) + '%'],
    ['总资产收益率', Fmt.num1(f.roa) + '%'],
    ['毛利率', Fmt.num1(f.gross_margin) + '%'],
    ['净利率', Fmt.num1(f.net_margin) + '%'],
    ['股息收益率', Fmt.num2(f.dividend_yield) + '%'],
    ['营收增长(YoY)', Fmt.pct(f.revenue_growth_yoy)],
    ['净利增长(YoY)', Fmt.pct(f.net_income_growth_yoy)],
    ['资产负债率', Fmt.num1(f.debt_ratio) + '%'],
    ['流动比率', Fmt.num2(f.current_ratio) + 'x'],
    ['市值', Fmt.mcap(f.market_cap)],
    ['自由现金流收益率', Fmt.num2(f.free_cash_flow_yield) + '%']
  ];

  const finGrid = document.getElementById('financials-grid');
  if (finGrid) {
    finGrid.innerHTML = finData.map(([label, val]) => `
      <div class="fin-item">
        <div class="fin-label">${label}</div>
        <div class="fin-value">${val}</div>
      </div>
    `).join('');
  }

  // Technical indicators
  const techItems = [
    ['MA5', `¥${Fmt.price(t.ma5)}`],
    ['MA20', `¥${Fmt.price(t.ma20)}`],
    ['MA60', `¥${Fmt.price(t.ma60)}`],
    ['MA120', `¥${Fmt.price(t.ma120)}`],
    ['20日动量', Fmt.pct(t.momentum_20d)],
    ['60日动量', Fmt.pct(t.momentum_60d)],
    ['成交量比', Fmt.num2(t.volume_ratio)],
    ['布林带位置', Fmt.pctNoSign(t.bb_position * 100)]
  ];

  const techGrid = document.getElementById('tech-grid');
  if (techGrid) {
    techGrid.innerHTML = techItems.map(([label, val]) => `
      <div class="fin-item">
        <div class="fin-label">${label}</div>
        <div class="fin-value">${val}</div>
      </div>
    `).join('');
  }

  // RSI gauge
  const rsiVal = t.rsi_14 || 50;
  const rsiEl = document.getElementById('rsi-value');
  const rsiThumb = document.getElementById('rsi-thumb');
  if (rsiEl) {
    rsiEl.textContent = rsiVal.toFixed(1);
    rsiEl.style.color = rsiVal > 70 ? 'var(--market-up)' : rsiVal < 30 ? 'var(--market-down)' : 'var(--accent-blue)';
  }
  if (rsiThumb) {
    rsiThumb.style.left = `${rsiVal}%`;
  }

  const rsiLabel = document.getElementById('rsi-label');
  if (rsiLabel) {
    rsiLabel.textContent = rsiVal > 70 ? '超买区域' : rsiVal < 30 ? '超卖区域' : rsiVal > 50 ? '强势区间' : '弱势区间';
  }

  // MA signals
  const maSignals = [];
  if (t.above_ma20) maSignals.push({ txt: '站上MA20', good: true });
  else maSignals.push({ txt: '跌破MA20', good: false });
  if (t.above_ma60) maSignals.push({ txt: '站上MA60', good: true });
  else maSignals.push({ txt: '跌破MA60', good: false });
  if (t.golden_cross_ma) maSignals.push({ txt: '金叉信号', good: true });

  const signalsEl = document.getElementById('ma-signals');
  if (signalsEl) {
    signalsEl.innerHTML = maSignals.map(s => `
      <span class="signal-badge ${s.good ? 'signal-good' : 'signal-bad'}">${s.txt}</span>
    `).join('');
  }

  // Scores (if available)
  if (stock.compositeScore != null) {
    const scoreEl = document.getElementById('stock-composite-score');
    if (scoreEl) scoreEl.textContent = Fmt.score(stock.compositeScore);

    ['value', 'growth', 'quality', 'momentum'].forEach(f => {
      const el = document.getElementById(`stock-score-${f}`);
      const bar = document.getElementById(`stock-score-bar-${f}`);
      const sc = stock[`${f}Score`];
      if (el) {
        el.textContent = Fmt.score(sc);
        el.style.color = scoreColor(sc);
      }
      if (bar) {
        bar.style.width = `${sc || 0}%`;
        bar.style.background = scoreColor(sc);
      }
    });
  }

  // Render charts
  setTimeout(() => {
    Charts.renderPriceChart('price-chart', prices, t);
    Charts.renderVolumeChart('volume-chart', prices);
    if (f.quarters && f.quarters.length > 0) {
      Charts.renderQuarterlyChart('quarterly-chart', f.quarters);
    }
    Charts.renderRSIChart('rsi-chart', prices);
    Charts.renderMACDChart('macd-chart', prices);
  }, 50);
}

// ─── Backtest Controls ────────────────────────────────────────────────────────
function initBacktestControls() {
  // Frequency toggle
  document.querySelectorAll('#bt-frequency .toggle-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#bt-frequency .toggle-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      AppState.backtestConfig.frequency = btn.dataset.value;
    });
  });

  // TopN toggle
  document.querySelectorAll('#bt-topn .toggle-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#bt-topn .toggle-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      AppState.backtestConfig.topN = parseInt(btn.dataset.value);
    });
  });

  // Date pickers
  const startInput = document.getElementById('bt-start-date');
  const endInput = document.getElementById('bt-end-date');
  if (startInput) startInput.addEventListener('change', () => { AppState.backtestConfig.startDate = startInput.value; });
  if (endInput) endInput.addEventListener('change', () => { AppState.backtestConfig.endDate = endInput.value; });

  // Run button
  const runBtBtn = document.getElementById('run-backtest-btn');
  if (runBtBtn) {
    runBtBtn.addEventListener('click', runBacktest);
  }
}

function runBacktest() {
  if (!AppState.data) return;

  const btn = document.getElementById('run-backtest-btn');
  if (btn) { btn.disabled = true; btn.textContent = '计算中...'; }

  const resultArea = document.getElementById('backtest-results');
  if (resultArea) resultArea.style.display = 'none';

  setTimeout(() => {
    const config = {
      ...AppState.backtestConfig,
      weights: AppState.weights
    };

    if (!config.startDate || !config.endDate) {
      alert('请选择回测起止日期');
      if (btn) { btn.disabled = false; btn.textContent = '运行回测'; }
      return;
    }

    const results = Backtest.run(
      AppState.data.stocks,
      AppState.data.benchmark,
      config
    );

    if (results.error) {
      alert(results.error);
      if (btn) { btn.disabled = false; btn.textContent = '运行回测'; }
      return;
    }

    AppState.backtestResults = results;
    renderBacktestResults(results);

    if (resultArea) resultArea.style.display = 'block';
    if (btn) { btn.disabled = false; btn.textContent = '运行回测'; }
  }, 50);
}

function renderBacktestResults(results) {
  const { metrics, equityCurve, holdingsHistory, monthlyReturns } = results;

  // Metrics
  const m = metrics;
  const setMetric = (id, val, colorize = true) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = val;
    if (colorize) {
      const num = parseFloat(val);
      el.style.color = num > 0 ? 'var(--market-up)' : num < 0 ? 'var(--market-down)' : 'var(--text-primary)';
    }
  };

  setMetric('bt-annual-return', `${m.annualizedReturn > 0 ? '+' : ''}${m.annualizedReturn.toFixed(2)}%`);
  setMetric('bt-max-drawdown', `-${m.maxDrawdown.toFixed(2)}%`, false);
  document.getElementById('bt-max-drawdown').style.color = 'var(--market-down)';
  setMetric('bt-sharpe', m.sharpeRatio.toFixed(2), false);
  document.getElementById('bt-sharpe').style.color = m.sharpeRatio > 1 ? 'var(--market-up)' : m.sharpeRatio > 0.5 ? 'var(--text-secondary)' : 'var(--market-down)';
  setMetric('bt-win-rate', `${m.winRate.toFixed(1)}%`, false);
  document.getElementById('bt-win-rate').style.color = m.winRate > 50 ? 'var(--market-up)' : 'var(--market-down)';
  setMetric('bt-alpha', `${m.alpha > 0 ? '+' : ''}${m.alpha.toFixed(2)}%`);

  // Equity chart
  setTimeout(() => {
    Charts.renderEquityChart('equity-chart', equityCurve);
  }, 50);

  // Monthly heatmap
  renderMonthlyHeatmap(monthlyReturns);

  // Holdings table
  renderHoldingsTable(holdingsHistory);
}

function renderMonthlyHeatmap(monthlyReturns) {
  const container = document.getElementById('heatmap-grid');
  if (!container) return;

  const years = Object.keys(monthlyReturns).sort();
  const months = ['1月','2月','3月','4月','5月','6月','7月','8月','9月','10月','11月','12月'];

  let html = `<div class="heatmap-header"></div>`;
  months.forEach(m => { html += `<div class="heatmap-header">${m}</div>`; });

  years.forEach(year => {
    html += `<div class="heatmap-year">${year}</div>`;
    for (let mo = 1; mo <= 12; mo++) {
      const ret = monthlyReturns[year]?.[mo];
      if (ret == null) {
        html += `<div class="heatmap-cell zero">—</div>`;
      } else {
        const pct = (ret * 100).toFixed(1);
        const isPos = ret > 0;
        const isStrong = Math.abs(ret) > 0.03;
        const cls = isPos ? `positive${isStrong ? ' strong' : ''}` : `negative${isStrong ? ' strong' : ''}`;
        html += `<div class="heatmap-cell ${cls}">${isPos ? '+' : ''}${pct}%</div>`;
      }
    }
  });

  container.innerHTML = html;
}

function renderHoldingsTable(holdingsHistory) {
  const tbody = document.getElementById('holdings-tbody');
  if (!tbody || !holdingsHistory.length) return;

  // Show last 6 periods
  const recent = holdingsHistory.slice(-6).reverse();

  tbody.innerHTML = recent.map(period => `
    <tr>
      <td>${period.date}</td>
      <td>${period.stocks.map(s => `<span class="stock-code" style="margin-right:4px">${s.code}</span>`).join('')}</td>
      <td>${period.stocks.map(s => s.name).join('、')}</td>
      <td>${period.stocks[0] ? Fmt.score(period.stocks[0].score) : '--'}</td>
    </tr>
  `).join('');
}

// ─── Data Mode Detection ─────────────────────────────────────────────────────
AppState.dataMode = 'demo';  // 'demo' or 'live'
AppState.backendAvailable = false;

async function checkBackend() {
  try {
    if (typeof ApiClient !== 'undefined') {
      const available = await ApiClient.isAvailable();
      AppState.backendAvailable = available;
      return available;
    }
  } catch { }
  return false;
}

// ─── Settings Tab ─────────────────────────────────────────────────────────────
function initSettings() {
  // Test Connection Button
  const testBtn = document.getElementById('test-connection-btn');
  if (testBtn) {
    testBtn.addEventListener('click', async () => {
      const token = document.getElementById('tushare-token').value.trim();
      const statusEl = document.getElementById('connection-status');

      if (!token) {
        if (statusEl) {
          statusEl.className = 'connection-status error';
          statusEl.innerHTML = '<span>● 请输入 Token</span>';
        }
        return;
      }

      if (statusEl) {
        statusEl.className = 'connection-status demo';
        statusEl.innerHTML = '<span class="refresh-progress">● 验证中...</span>';
      }

      if (!AppState.backendAvailable) {
        if (statusEl) {
          statusEl.className = 'connection-status error';
          statusEl.innerHTML = '<span>● 后端服务未启动。请先启动 API 服务器。</span>';
        }
        return;
      }

      try {
        const result = await ApiClient.setToken(token);
        if (statusEl) {
          statusEl.className = 'connection-status connected';
          statusEl.innerHTML = `<span>● ${result.message || 'Token 验证成功'}</span>`;
        }
        AppState.dataMode = 'live';
        updateDataStatusUI();
      } catch (err) {
        if (statusEl) {
          statusEl.className = 'connection-status error';
          statusEl.innerHTML = `<span>● ${err.message}</span>`;
        }
      }
    });
  }

  // Data Refresh Buttons
  const incrBtn = document.getElementById('refresh-incremental-btn');
  const fullBtn = document.getElementById('refresh-full-btn');

  if (incrBtn) {
    incrBtn.addEventListener('click', () => doRefreshData(false));
  }
  if (fullBtn) {
    fullBtn.addEventListener('click', () => doRefreshData(true));
  }

  // Check backend on init
  checkBackend().then(available => {
    if (available) {
      updateDataStatusUI();
      // Auto-check token status
      ApiClient.getTokenStatus().then(status => {
        const statusEl = document.getElementById('connection-status');
        if (status.has_token && status.connected) {
          AppState.dataMode = 'live';
          if (statusEl) {
            statusEl.className = 'connection-status connected';
            statusEl.innerHTML = `<span>● 已连接 (${status.token_preview || 'Tushare Pro'})</span>`;
          }
          if (status.token_preview) {
            const tokenInput = document.getElementById('tushare-token');
            if (tokenInput) tokenInput.placeholder = `已保存: ${status.token_preview}`;
          }
        }
      }).catch(() => {});
    }
  });
}

async function doRefreshData(full) {
  if (!AppState.backendAvailable) {
    alert('后端服务未启动。请先运行 API 服务器。');
    return;
  }

  const statusEl = document.getElementById('refresh-status');
  const incrBtn = document.getElementById('refresh-incremental-btn');
  const fullBtn = document.getElementById('refresh-full-btn');

  // Disable buttons
  if (incrBtn) incrBtn.disabled = true;
  if (fullBtn) fullBtn.disabled = true;

  const label = full ? '全量刷新' : '增量更新';
  if (statusEl) {
    statusEl.className = 'refresh-progress';
    statusEl.textContent = `⏳ 正在${label}数据，请稍候...`;
  }

  try {
    const result = await ApiClient.refreshData(full);
    if (statusEl) {
      statusEl.className = '';
      statusEl.style.color = '#10B981';
      statusEl.textContent = `✅ ${label}完成（耗时 ${result.elapsed_seconds}s）`;
    }
    updateDataStatusUI();

    // Reload data if live mode
    if (AppState.dataMode === 'live') {
      await loadLiveData();
    }
  } catch (err) {
    if (statusEl) {
      statusEl.className = '';
      statusEl.style.color = '#EF4444';
      statusEl.textContent = `❌ ${label}失败: ${err.message}`;
    }
  } finally {
    if (incrBtn) incrBtn.disabled = false;
    if (fullBtn) fullBtn.disabled = false;
  }
}

async function updateDataStatusUI() {
  if (!AppState.backendAvailable) return;

  try {
    const status = await ApiClient.getDataStatus();
    const el = (id) => document.getElementById(id);

    if (el('ds-stock-count')) el('ds-stock-count').textContent = status.stock_count || '--';
    if (el('ds-last-price')) {
      const d = status.last_price_date;
      el('ds-last-price').textContent = d ? `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6)}` : '--';
    }
    if (el('ds-cache-age')) {
      el('ds-cache-age').textContent = status.cache_age_hours != null ? `${status.cache_age_hours}h` : '--';
    }
    if (el('ds-data-state')) {
      const hasAll = status.has_prices && status.has_financials && status.has_indicators;
      el('ds-data-state').textContent = hasAll ? '完整' : '部分';
      el('ds-data-state').style.color = hasAll ? '#10B981' : '#F59E0B';
    }
  } catch { }
}

async function loadLiveData() {
  const loadingEl = document.getElementById('app-loading');
  const statusEl = document.getElementById('loading-status');

  try {
    if (statusEl) statusEl.textContent = '从后端加载实时数据...';

    // Fetch screener results from API
    const screenerResult = await ApiClient.runScreener(
      AppState.weights, {}, []
    );

    // Transform API results to match frontend format
    AppState.screenerResults = screenerResult.results || [];
    renderResults(AppState.screenerResults);
    renderKPI(AppState.screenerResults);
    renderCharts(AppState.screenerResults);

    if (loadingEl) {
      loadingEl.style.opacity = '0';
      loadingEl.style.transition = 'opacity 0.4s ease';
      setTimeout(() => { loadingEl.style.display = 'none'; }, 400);
    }

    // Update header data source indicator
    const dsIndicator = document.querySelector('.data-source');
    if (dsIndicator) {
      dsIndicator.innerHTML = '● Tushare Pro (实时)';
      dsIndicator.style.color = '#10B981';
    }
  } catch (err) {
    console.error('实时数据加载失败:', err);
    if (statusEl) statusEl.textContent = `实时数据加载失败: ${err.message}`;
  }
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  startClock();
  initTableSort();
  initSettings();
  loadData();  // Always start with demo data, then switch if backend is available
});
