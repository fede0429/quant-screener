/**
 * app.js — Main Application Logic
 * A股量化选股系统
 * 
 * 支持两种模式:
 *   1. 演示模式: 读取 demo_data.json 本地数据，前端计算选股/回测
 *   2. 后端模式: 通过 API 与 Tushare 实时数据交互，后端计算选股/回测
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
  backtestResults: null,
  // Backend state
  backendConnected: false,
  tushareConnected: false,
  cachedStocks: 0,
  mode: 'none' // 'demo', 'backend', 'none'
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
  const dl = window._debugLog || function(){};
  dl('initTabs: found ' + tabBtns.length + ' btns, ' + tabContents.length + ' contents');

  tabBtns.forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const target = btn.dataset.tab;
      dl('Tab clicked: ' + target, '#0ff');
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
  if (tab === 'settings') {
    refreshDataStatus();
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
  if (statusEl) statusEl.textContent = '正在连接后端...';

  // Step 1: Check backend health
  let backendOk = false;
  let health = null;
  try {
    health = await ApiClient.health();
    backendOk = true;
    AppState.backendConnected = true;
    AppState.tushareConnected = health.tushare_connected;
    AppState.cachedStocks = health.cached_stocks;
    if (statusEl) statusEl.textContent = '后端已连接，检查数据...';
  } catch (e) {
    AppState.backendConnected = false;
    if (statusEl) statusEl.textContent = '后端未连接，尝试加载演示数据...';
  }

  let dataLoaded = false;

  // Step 2: If backend has cached stocks, use backend mode
  if (backendOk && health && health.cached_stocks > 0) {
    AppState.mode = 'backend';
    dataLoaded = true;
    if (statusEl) statusEl.textContent = `已缓存 ${health.cached_stocks} 只股票，加载中...`;
  }

  // Step 3: Try loading demo data (works offline or as fallback)
  if (!dataLoaded) {
    try {
      const response = await fetch('./data/demo_data.json');
      if (response.ok) {
        if (statusEl) statusEl.textContent = '解析演示数据中...';
        const data = await response.json();
        AppState.data = data;
        AppState.mode = 'demo';
        dataLoaded = true;
      }
    } catch (err) {
      console.log('Demo data not available');
    }
  }

  if (statusEl) statusEl.textContent = '初始化界面...';

  // Init UI controls regardless of data
  initWeightSliders();
  initFilterControls();
  initBacktestControls();
  initSettingsControls();

  if (AppState.mode === 'demo' && AppState.data) {
    // Demo mode: local data
    initSectorFilters(AppState.data.stocks);
    initDatePickers(AppState.data.stocks);
    runScreener();
  } else if (AppState.mode === 'backend') {
    // Backend mode: run screener via API
    initSectorFilters([]);
    initDatePickers([]);
    await runScreenerFromBackend();
  } else if (backendOk) {
    // Backend is up but no data
    initSectorFilters([]);
    initDatePickers([]);
    showBackendModeMessage();
  } else {
    showNoDataMessage();
  }

  // Update header badge
  updateHeaderBadge();
  removeOverlay();
}

function updateHeaderBadge() {
  const badge = document.querySelector('.header-badge span');
  const dot = document.querySelector('.badge-dot');
  if (!badge) return;

  if (AppState.mode === 'backend' && AppState.tushareConnected) {
    badge.textContent = `Tushare Pro · ${AppState.cachedStocks} 只股票`;
    if (dot) dot.style.background = '#10B981';
  } else if (AppState.backendConnected) {
    badge.textContent = 'Tushare Pro (未配置数据)';
    if (dot) dot.style.background = '#F59E0B';
  } else if (AppState.mode === 'demo') {
    badge.textContent = 'Tushare Pro (Demo)';
    if (dot) dot.style.background = '#8B5CF6';
  } else {
    badge.textContent = '未连接';
    if (dot) dot.style.background = '#EF4444';
  }
}

// ─── Backend Screener ─────────────────────────────────────────────────────────
async function runScreenerFromBackend() {
  const runBtn = document.getElementById('run-btn');
  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = '正在选股...';
  }

  try {
    const filters = getFilters();
    AppState.filters = filters;

    const resp = await ApiClient.runScreener(AppState.weights, filters, filters.sectors || []);
    const results = resp.results || [];

    // Enrich with rank
    results.forEach((s, i) => { s.rank = i + 1; });

    AppState.screenerResults = results;
    renderScreenerResults(results);

    // Update KPI from results
    const stats = computeStatsFromResults(results);
    renderKPICards(stats);

  } catch (err) {
    console.error('Backend screener error:', err);
    const tbody = document.getElementById('results-tbody');
    if (tbody) {
      tbody.innerHTML = `<tr><td colspan="15" style="text-align:center;padding:3rem;color:var(--text-secondary,#888);">
        <div style="font-size:1.25rem;margin-bottom:0.5rem;">选股失败</div>
        <div>${err.message || '请先在「Tushare 配置」中刷新数据'}</div>
      </td></tr>`;
    }
  }

  if (runBtn) {
    runBtn.disabled = false;
    runBtn.textContent = '开始选股';
  }
}

function computeStatsFromResults(results) {
  if (!results.length) return { count: 0, avgScore: 0, avgPE: 0, avgROE: 0 };
  const n = results.length;
  const avgScore = results.reduce((s, x) => s + (x.compositeScore || 0), 0) / n;
  const avgPE = results.reduce((s, x) => s + (x.financials?.pe || x.pe || 0), 0) / n;
  const avgROE = results.reduce((s, x) => s + (x.financials?.roe || x.roe || 0), 0) / n;
  return {
    count: n,
    avgScore: Math.round(avgScore * 10) / 10,
    avgPE: Math.round(avgPE * 10) / 10,
    avgROE: Math.round(avgROE * 10) / 10
  };
}

// ─── Backend Backtest ─────────────────────────────────────────────────────────
async function runBacktestFromBackend() {
  const runBtn = document.getElementById('run-backtest-btn');
  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = '回测运行中...';
  }

  try {
    const filters = getFilters();
    const params = {
      weights: AppState.weights,
      filters: filters,
      sectors: filters.sectors || [],
      frequency: AppState.backtestConfig.frequency,
      top_n: AppState.backtestConfig.topN,
      start_date: AppState.backtestConfig.startDate,
      end_date: AppState.backtestConfig.endDate
    };

    const results = await ApiClient.runBacktest(params);
    AppState.backtestResults = results;

    // Show results section
    document.getElementById('backtest-results').style.display = 'block';

    // Render metrics
    renderBacktestMetrics(results);

    // Render equity curve
    if (results.equity_curve) {
      renderEquityCurve(results);
    }

    // Render monthly returns heatmap
    if (results.monthly_returns) {
      renderMonthlyHeatmap(results.monthly_returns);
    }

    // Render holdings
    if (results.holdings_history) {
      renderHoldingsHistory(results.holdings_history);
    }

  } catch (err) {
    console.error('Backend backtest error:', err);
    alert(`回测失败: ${err.message || '请先刷新数据'}`);
  }

  if (runBtn) {
    runBtn.disabled = false;
    runBtn.textContent = '运行回测';
  }
}

function renderBacktestMetrics(results) {
  const set = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
  };

  set('bt-annual-return', results.annual_return != null ? Fmt.pct(results.annual_return) : '--');
  set('bt-max-drawdown', results.max_drawdown != null ? Fmt.pct(results.max_drawdown) : '--');
  set('bt-sharpe', results.sharpe_ratio != null ? Fmt.num2(results.sharpe_ratio) : '--');
  set('bt-win-rate', results.win_rate != null ? Fmt.pctNoSign(results.win_rate) : '--');
  set('bt-alpha', results.alpha != null ? Fmt.pct(results.alpha) : '--');

  // Color coding
  const arEl = document.getElementById('bt-annual-return');
  if (arEl && results.annual_return != null) {
    arEl.className = `metric-value ${results.annual_return >= 0 ? 'val-up' : 'val-down'}`;
  }
  const alphaEl = document.getElementById('bt-alpha');
  if (alphaEl && results.alpha != null) {
    alphaEl.className = `metric-value ${results.alpha >= 0 ? 'val-up' : 'val-down'}`;
  }
}

function renderEquityCurve(results) {
  const canvas = document.getElementById('equity-chart');
  if (!canvas) return;

  // Destroy existing chart
  if (canvas._chartInstance) canvas._chartInstance.destroy();

  const dates = results.equity_curve.map(p => p.date);
  const strategy = results.equity_curve.map(p => p.strategy);
  const benchmark = results.equity_curve.map(p => p.benchmark);

  const chart = new Chart(canvas, {
    type: 'line',
    data: {
      labels: dates,
      datasets: [
        {
          label: '策略净值',
          data: strategy,
          borderColor: '#3B82F6',
          backgroundColor: 'rgba(59,130,246,0.08)',
          fill: true,
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.1
        },
        {
          label: '沪深300',
          data: benchmark,
          borderColor: '#94A3B8',
          borderWidth: 1,
          pointRadius: 0,
          borderDash: [4, 2],
          tension: 0.1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'top', labels: { font: { size: 11 } } }
      },
      scales: {
        x: {
          ticks: { maxTicksLimit: 8, font: { size: 10 } },
          grid: { display: false }
        },
        y: {
          ticks: { font: { size: 10 } },
          grid: { color: 'rgba(0,0,0,0.06)' }
        }
      }
    }
  });
  canvas._chartInstance = chart;
}

function renderMonthlyHeatmap(monthlyReturns) {
  const grid = document.getElementById('heatmap-grid');
  if (!grid || !monthlyReturns.length) return;

  grid.innerHTML = monthlyReturns.map(m => {
    const val = m.return_pct;
    const cls = val > 0 ? 'val-up' : val < 0 ? 'val-down' : '';
    return `<div class="heatmap-cell ${cls}" title="${m.month}: ${Fmt.pct(val)}">
      <span class="heatmap-month">${m.month}</span>
      <span class="heatmap-val">${Fmt.pct(val)}</span>
    </div>`;
  }).join('');
}

function renderHoldingsHistory(holdings) {
  const tbody = document.getElementById('holdings-tbody');
  if (!tbody) return;

  const recent = holdings.slice(-10);
  tbody.innerHTML = recent.map(h => `<tr>
    <td>${h.date}</td>
    <td>${(h.codes || []).join(', ')}</td>
    <td>${(h.names || []).join(', ')}</td>
    <td>${h.top_score != null ? Fmt.score(h.top_score) : '--'}</td>
  </tr>`).join('');
}

// ─── Settings Controls ────────────────────────────────────────────────────────
function initSettingsControls() {
  // Test connection button
  const testBtn = document.getElementById('test-connection-btn');
  if (testBtn) {
    testBtn.addEventListener('click', async () => {
      const tokenInput = document.getElementById('tushare-token');
      const token = tokenInput?.value?.trim();
      if (!token) {
        updateConnectionStatus('error', '请输入 Token');
        return;
      }

      testBtn.disabled = true;
      testBtn.textContent = '验证中...';
      updateConnectionStatus('pending', '正在验证 Token...');

      try {
        const result = await ApiClient.setToken(token);
        updateConnectionStatus('success', '✅ Token 验证成功，Tushare 已连接');
        AppState.tushareConnected = true;
        updateHeaderBadge();
      } catch (err) {
        updateConnectionStatus('error', `❌ ${err.message}`);
      }

      testBtn.disabled = false;
      testBtn.textContent = '测试连接';
    });
  }

  // Incremental refresh button
  const incrBtn = document.getElementById('refresh-incremental-btn');
  if (incrBtn) {
    incrBtn.addEventListener('click', () => doRefreshData(false));
  }

  // Full refresh button
  const fullBtn = document.getElementById('refresh-full-btn');
  if (fullBtn) {
    fullBtn.addEventListener('click', () => doRefreshData(true));
  }

  // Initial status check
  refreshDataStatus();
}

function updateConnectionStatus(state, message) {
  const el = document.getElementById('connection-status');
  if (!el) return;
  el.className = `connection-status ${state}`;
  el.innerHTML = `<span>${message}</span>`;
}

async function doRefreshData(full) {
  const statusEl = document.getElementById('refresh-status');
  const incrBtn = document.getElementById('refresh-incremental-btn');
  const fullBtn = document.getElementById('refresh-full-btn');

  if (incrBtn) incrBtn.disabled = true;
  if (fullBtn) fullBtn.disabled = true;

  const label = full ? '全量刷新' : '增量更新';
  if (statusEl) statusEl.textContent = `⏳ ${label}启动中...`;
  if (statusEl) statusEl.style.color = 'var(--accent-blue, #3B82F6)';

  try {
    // Start the background refresh
    const startResp = await ApiClient.refreshData(full);
    if (statusEl) statusEl.textContent = `⏳ ${startResp.message || label + '已启动'}...`;

    // Poll for progress
    await pollRefreshStatus(statusEl, label);

  } catch (err) {
    if (statusEl) {
      statusEl.textContent = `❌ ${label}失败: ${err.message}`;
      statusEl.style.color = 'var(--market-down, #EF4444)';
    }
  }

  if (incrBtn) incrBtn.disabled = false;
  if (fullBtn) fullBtn.disabled = false;
}

async function pollRefreshStatus(statusEl, label) {
  const POLL_INTERVAL = 3000; // 3 seconds
  const MAX_POLLS = 2400;     // max ~2 hours

  for (let i = 0; i < MAX_POLLS; i++) {
    await new Promise(r => setTimeout(r, POLL_INTERVAL));

    try {
      const s = await ApiClient.refreshStatus();

      // Update progress bar text
      if (statusEl) {
        const elapsed = s.elapsed_seconds ? ` (${Math.round(s.elapsed_seconds)}秒)` : '';
        const pctText = s.pct > 0 ? ` ${s.pct}%` : '';
        statusEl.textContent = `⏳${pctText} ${s.progress || label + '中...'}${elapsed}`;
        statusEl.style.color = 'var(--accent-blue, #3B82F6)';
      }

      // Update the mini progress bar if we add one
      updateRefreshProgressBar(s.pct || 0);

      // Check if done
      if (!s.running) {
        if (s.error) {
          if (statusEl) {
            statusEl.textContent = `❌ ${label}失败: ${s.error}`;
            statusEl.style.color = 'var(--market-down, #EF4444)';
          }
          return;
        }

        // Success!
        const elapsed = s.elapsed_seconds ? Math.round(s.elapsed_seconds) : '?';
        if (statusEl) {
          statusEl.textContent = `✅ ${label}完成！耗时 ${elapsed} 秒`;
          statusEl.style.color = 'var(--market-up, #10B981)';
        }
        updateRefreshProgressBar(100);

        // Refresh data status display
        await refreshDataStatus();

        // Update app state
        const health = await ApiClient.health();
        AppState.tushareConnected = health.tushare_connected;
        AppState.cachedStocks = health.cached_stocks;
        AppState.mode = health.cached_stocks > 0 ? 'backend' : AppState.mode;
        updateHeaderBadge();
        return;
      }

    } catch (pollErr) {
      // Network hiccup during poll — just retry
      console.log('Poll error (retrying):', pollErr.message);
    }
  }

  // Exceeded max polls
  if (statusEl) {
    statusEl.textContent = `⚠️ ${label}仍在后台运行，请稍后查看数据状态`;
    statusEl.style.color = 'var(--text-muted)';
  }
}

function updateRefreshProgressBar(pct) {
  let bar = document.getElementById('refresh-progress-bar');
  if (!bar) {
    // Create progress bar on first call
    const container = document.getElementById('refresh-status');
    if (!container || !container.parentNode) return;
    bar = document.createElement('div');
    bar.id = 'refresh-progress-bar';
    bar.style.cssText = 'width:100%;height:4px;background:var(--bg-input,#1e1e2e);border-radius:2px;margin-top:8px;overflow:hidden;';
    bar.innerHTML = '<div id="refresh-progress-fill" style="height:100%;width:0%;background:var(--accent-blue,#3B82F6);border-radius:2px;transition:width 0.5s ease;"></div>';
    container.parentNode.insertBefore(bar, container.nextSibling);
  }
  const fill = document.getElementById('refresh-progress-fill');
  if (fill) fill.style.width = `${pct}%`;
  // Hide bar when complete
  if (pct >= 100) {
    setTimeout(() => { if (bar) bar.style.opacity = '0.3'; }, 2000);
  } else {
    bar.style.opacity = '1';
  }
}

async function refreshDataStatus() {
  try {
    const status = await ApiClient.getDataStatus();

    const set = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.textContent = val;
    };

    set('ds-stock-count', status.stock_count || 0);
    set('ds-last-price', status.last_price_date || '--');
    set('ds-cache-age', status.cache_age_hours != null ? `${status.cache_age_hours.toFixed(1)}h` : '--');

    let stateText = '--';
    if (status.has_prices && status.has_financials) stateText = '完整';
    else if (status.has_prices) stateText = '仅行情';
    else if (status.stock_count > 0) stateText = '仅列表';
    set('ds-data-state', stateText);

    // Update connection status display
    const health = await ApiClient.health();
    if (health.tushare_connected) {
      updateConnectionStatus('success', `✅ Tushare 已连接 · ${health.cached_stocks} 只股票`);
    } else {
      updateConnectionStatus('demo', '● 未连接 Tushare');
    }

    // If we have token already set via env var, show in preview
    const tokenStatus = await ApiClient.getTokenStatus();
    if (tokenStatus.token_preview) {
      const tokenInput = document.getElementById('tushare-token');
      if (tokenInput && !tokenInput.value) {
        tokenInput.placeholder = `已配置: ${tokenStatus.token_preview}`;
      }
    }

  } catch (err) {
    console.log('Could not fetch data status:', err.message);
  }
}

// ─── Show messages ────────────────────────────────────────────────────────────
function showBackendModeMessage() {
  const container = document.getElementById('results-tbody');
  if (container) {
    container.innerHTML = `<tr><td colspan="15" style="text-align:center;padding:3rem;color:var(--text-secondary,#888);">
      <div style="font-size:1.25rem;margin-bottom:0.5rem;">后端已连接 ✓</div>
      <div>请在「Tushare 配置」面板中点击「全量刷新数据」拉取 A 股数据后开始选股</div>
    </td></tr>`;
  }
}

function showNoDataMessage() {
  const container = document.getElementById('results-tbody');
  if (container) {
    container.innerHTML = `<tr><td colspan="15" style="text-align:center;padding:3rem;color:var(--text-secondary,#888);">
      <div style="font-size:1.25rem;margin-bottom:0.5rem;">暂无数据</div>
      <div>请确保后端服务正常运行，并设置 Tushare Token</div>
    </td></tr>`;
  }
}

// ─── Sector Filter Chips ──────────────────────────────────────────────────────
function initSectorFilters(stocks) {
  const sectors = [...new Set(stocks.map(s => s.sector).filter(Boolean))].sort();
  const container = document.getElementById('sector-chips');
  if (!container) return;

  if (sectors.length === 0) {
    container.innerHTML = '<div style="color:var(--text-muted);font-size:12px">刷新数据后显示行业列表</div>';
    return;
  }

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
  if (!stocks[0].prices || stocks[0].prices.length === 0) return;

  const dates = stocks[0].prices.map(p => p.date);
  const minDate = dates[0];
  const maxDate = dates[dates.length - 1];

  const startInput = document.getElementById('bt-start-date');
  const endInput = document.getElementById('bt-end-date');
  if (startInput) {
    startInput.min = minDate;
    startInput.max = maxDate;
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
    runBtn.addEventListener('click', () => {
      if (AppState.mode === 'backend') {
        runScreenerFromBackend();
      } else {
        runScreener();
      }
    });
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
    const input = chip.querySelector('input');
    if (input) input.checked = true;
  });

  // Reset weights
  AppState.weights = { value: 25, growth: 25, quality: 25, momentum: 25 };
  ['value', 'growth', 'quality', 'momentum'].forEach(f => {
    const slider = document.getElementById(`weight-${f}`);
    if (slider) slider.value = 25;
  });
  updateWeightDisplay();

  if (AppState.mode === 'backend') {
    runScreenerFromBackend();
  } else {
    runScreener();
  }
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

// ─── Run Screener (Demo Mode) ─────────────────────────────────────────────────
function runScreener() {
  if (!AppState.data) return;

  const runBtn = document.getElementById('run-btn');
  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = '计算中...';
  }

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
  renderKPICardsFromResults(results);
  renderResultsTable(results);
  renderBottomCharts(results);
}

function renderKPICards(stats) {
  const setKPI = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
  };

  setKPI('kpi-count', stats.count);
  setKPI('kpi-avg-score', Fmt.score(stats.avgScore));
  setKPI('kpi-avg-pe', Fmt.num1(stats.avgPE));
  setKPI('kpi-avg-roe', `${Fmt.num1(stats.avgROE)}%`);
}

function renderKPICardsFromResults(results) {
  let stats;
  if (typeof Screener !== 'undefined' && Screener.computeStats) {
    stats = Screener.computeStats(results);
  } else {
    stats = computeStatsFromResults(results);
  }
  renderKPICards(stats);
}

function renderResultsTable(results) {
  const countEl = document.getElementById('table-count');
  if (countEl) countEl.textContent = `共 ${results.length} 只`;

  const tbody = document.getElementById('results-tbody');
  if (!tbody) return;

  if (results.length === 0) {
    tbody.innerHTML = `<tr><td colspan="15" style="text-align:center;padding:40px;color:var(--text-muted)">
      没有符合条件的股票，请调整筛选条件
    </td></tr>`;
    return;
  }

  // Sort
  const sorted = sortResults([...results]);

  tbody.innerHTML = sorted.map((s, i) => {
    const f = s.financials || {};
    const t = s.technicals || {};
    const rankClass = i === 0 ? 'rank-1' : i === 1 ? 'rank-2' : i === 2 ? 'rank-3' : 'rank-other';
    const sc = s.compositeScore || 0;

    // Price change
    let priceChange = 0;
    let lastPrice = t.price || 0;
    if (s.prices && s.prices.length >= 2) {
      const lp = s.prices[s.prices.length - 1];
      const pp = s.prices[s.prices.length - 2];
      lastPrice = lp.close;
      priceChange = pp ? (lp.close - pp.close) / pp.close * 100 : 0;
    } else if (s.pct_chg != null) {
      priceChange = s.pct_chg;
    }
    const priceClass = priceChange > 0 ? 'val-up' : priceChange < 0 ? 'val-down' : 'val-neutral';

    return `<tr data-code="${s.code}" onclick="selectStock('${s.code}')">
      <td><span class="rank-badge ${rankClass}">${i + 1}</span></td>
      <td><span class="stock-code">${s.code || '--'}</span></td>
      <td><span class="stock-name">${s.name || '--'}</span></td>
      <td><span class="industry-tag">${s.industry || s.sector || '--'}</span></td>
      <td>
        <div class="score-bar-cell">
          <div class="score-bar-track">
            <div class="score-bar-fill" style="width:${sc}%;background:${scoreColor(sc)}"></div>
          </div>
          <span class="score-val" style="color:${scoreColor(sc)}">${Fmt.score(sc)}</span>
        </div>
      </td>
      <td style="color:${scoreColor(s.valueScore || 0)}">${Fmt.score(s.valueScore)}</td>
      <td style="color:${scoreColor(s.growthScore || 0)}">${Fmt.score(s.growthScore)}</td>
      <td style="color:${scoreColor(s.qualityScore || 0)}">${Fmt.score(s.qualityScore)}</td>
      <td style="color:${scoreColor(s.momentumScore || 0)}">${Fmt.score(s.momentumScore)}</td>
      <td class="${priceClass}">¥${Fmt.price(lastPrice || f.price || t.price)}</td>
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
      case 'pe': va = a.financials?.pe; vb = b.financials?.pe; break;
      case 'pb': va = a.financials?.pb; vb = b.financials?.pb; break;
      case 'roe': va = a.financials?.roe; vb = b.financials?.roe; break;
      case 'revGrowth': va = a.financials?.revenue_growth_yoy; vb = b.financials?.revenue_growth_yoy; break;
      case 'mom20': va = a.technicals?.momentum_20d; vb = b.technicals?.momentum_20d; break;
      case 'price': {
        va = a.technicals?.price || a.prices?.[a.prices?.length - 1]?.close || 0;
        vb = b.technicals?.price || b.prices?.[b.prices?.length - 1]?.close || 0;
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
  if (typeof Screener !== 'undefined') {
    const sectorDist = Screener.getSectorDistribution(results);
    Charts.renderSectorChart('sector-chart', sectorDist);

    const histogram = Screener.getScoreHistogram(results, 10);
    Charts.renderScoreHistogram('score-histogram', histogram);
  } else {
    // Compute stats locally for backend results
    const sectorDist = {};
    for (const s of results) {
      const sec = s.sector || s.industry || '未知';
      sectorDist[sec] = (sectorDist[sec] || 0) + 1;
    }
    const sectorArr = Object.entries(sectorDist).map(([sector, count]) => ({ sector, count })).sort((a, b) => b.count - a.count);
    Charts.renderSectorChart('sector-chart', sectorArr);

    const bins = 10;
    const binSize = 100 / bins;
    const hist = Array(bins).fill(0);
    for (const s of results) {
      const bin = Math.min(Math.floor((s.compositeScore || 0) / binSize), bins - 1);
      hist[bin]++;
    }
    const histArr = hist.map((count, i) => ({
      label: `${Math.round(i * binSize)}-${Math.round((i + 1) * binSize)}`,
      count
    }));
    Charts.renderScoreHistogram('score-histogram', histArr);
  }
}

// ─── Stock Selection ──────────────────────────────────────────────────────────
function selectStock(code) {
  const stock = AppState.screenerResults.find(s => s.code === code) ||
    AppState.data?.stocks?.find(s => s.code === code);
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

  const f = stock.financials || {};
  const t = stock.technicals || {};
  const prices = stock.prices || [];

  // If no prices, show basic info
  const hasPrice = prices.length > 0;
  const lastPrice = hasPrice ? prices[prices.length - 1] : null;
  const prevPrice = hasPrice && prices.length >= 2 ? prices[prices.length - 2] : null;
  const priceChange = prevPrice ? (lastPrice.close - prevPrice.close) / prevPrice.close * 100 : (stock.pct_chg || 0);
  const priceAbs = prevPrice ? lastPrice.close - prevPrice.close : 0;
  const changeClass = priceChange > 0 ? 'up' : priceChange < 0 ? 'down' : '';
  const changeSign = priceChange > 0 ? '+' : '';

  container.style.display = 'block';
  document.getElementById('no-stock-msg').style.display = 'none';

  // Render HTML
  document.getElementById('analysis-stock-name').textContent = stock.name || '--';
  document.getElementById('analysis-stock-code').textContent = stock.code || '--';
  document.getElementById('analysis-stock-sector').textContent = `${stock.sector || '--'} · ${stock.industry || '--'}`;
  document.getElementById('analysis-price').textContent = `¥${Fmt.price(lastPrice?.close || t.price || 0)}`;
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
        <span class="fin-label">${label}</span>
        <span class="fin-value">${val}</span>
      </div>
    `).join('');
  }

  // Score card
  document.getElementById('stock-composite-score').textContent = Fmt.score(stock.compositeScore);
  document.getElementById('stock-composite-score').style.color = scoreColor(stock.compositeScore);

  const scoreItems = ['value', 'growth', 'quality', 'momentum'];
  scoreItems.forEach(key => {
    const score = stock[`${key}Score`] || 0;
    const bar = document.getElementById(`stock-score-bar-${key}`);
    const val = document.getElementById(`stock-score-${key}`);
    if (bar) bar.style.width = `${score}%`;
    if (val) val.textContent = Fmt.score(score);
  });

  // RSI
  const rsi = t.rsi_14 || 50;
  document.getElementById('rsi-value').textContent = Fmt.num1(rsi);
  const rsiLabel = rsi > 70 ? '超买' : rsi < 30 ? '超卖' : '中性';
  document.getElementById('rsi-label').textContent = rsiLabel;
  document.getElementById('rsi-thumb').style.left = `${rsi}%`;

  // Technical grid
  const techData = [
    ['20日动量', Fmt.pct(t.momentum_20d)],
    ['60日动量', Fmt.pct(t.momentum_60d)],
    ['成交量比', Fmt.num2(t.volume_ratio || 1) + 'x'],
    ['MA5', t.ma5 ? `¥${Fmt.price(t.ma5)}` : '--'],
    ['MA20', t.ma20 ? `¥${Fmt.price(t.ma20)}` : '--'],
    ['MA60', t.ma60 ? `¥${Fmt.price(t.ma60)}` : '--']
  ];

  const techGrid = document.getElementById('tech-grid');
  if (techGrid) {
    techGrid.innerHTML = techData.map(([label, val]) => `
      <div class="fin-item">
        <span class="fin-label">${label}</span>
        <span class="fin-value">${val}</span>
      </div>
    `).join('');
  }

  // Price chart
  if (hasPrice && typeof Charts !== 'undefined') {
    Charts.renderPriceChart('price-chart', prices);
    Charts.renderVolumeChart('volume-chart', prices);
    Charts.renderMACDChart('macd-chart', prices);
    Charts.renderRSIChart('rsi-chart', prices);
  }

  // MA signals
  const signalsEl = document.getElementById('ma-signals');
  if (signalsEl && hasPrice) {
    const signals = [];
    const close = lastPrice.close;
    if (t.ma5 && close > t.ma5) signals.push({ text: '站上MA5', good: true });
    if (t.ma20 && close > t.ma20) signals.push({ text: '站上MA20', good: true });
    if (t.ma60 && close > t.ma60) signals.push({ text: '站上MA60', good: true });
    if (t.ma5 && close < t.ma5) signals.push({ text: '跌破MA5', good: false });
    if (t.ma20 && close < t.ma20) signals.push({ text: '跌破MA20', good: false });
    if (t.ma60 && close < t.ma60) signals.push({ text: '跌破MA60', good: false });
    if (rsi > 70) signals.push({ text: 'RSI超买', good: false });
    if (rsi < 30) signals.push({ text: 'RSI超卖', good: true });

    signalsEl.innerHTML = signals.map(s =>
      `<span class="signal-badge ${s.good ? 'signal-good' : 'signal-bad'}">${s.text}</span>`
    ).join('');
  }

  // Quarterly chart (if data available)
  if (stock.quarterly && typeof Charts !== 'undefined') {
    Charts.renderQuarterlyChart('quarterly-chart', stock.quarterly);
  }
}

// ─── Backtest Controls ────────────────────────────────────────────────────────
function initBacktestControls() {
  // Frequency toggle
  const freqGroup = document.getElementById('bt-frequency');
  if (freqGroup) {
    freqGroup.querySelectorAll('.toggle-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        freqGroup.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        AppState.backtestConfig.frequency = btn.dataset.value;
      });
    });
  }

  // Top N toggle
  const topnGroup = document.getElementById('bt-topn');
  if (topnGroup) {
    topnGroup.querySelectorAll('.toggle-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        topnGroup.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        AppState.backtestConfig.topN = parseInt(btn.dataset.value);
      });
    });
  }

  // Date inputs
  const startInput = document.getElementById('bt-start-date');
  const endInput = document.getElementById('bt-end-date');
  if (startInput) {
    startInput.addEventListener('change', () => {
      AppState.backtestConfig.startDate = startInput.value;
    });
  }
  if (endInput) {
    endInput.addEventListener('change', () => {
      AppState.backtestConfig.endDate = endInput.value;
    });
  }

  // Run backtest button
  const runBtBtn = document.getElementById('run-backtest-btn');
  if (runBtBtn) {
    runBtBtn.addEventListener('click', () => {
      if (AppState.mode === 'backend') {
        runBacktestFromBackend();
      } else if (AppState.mode === 'demo' && AppState.data) {
        runBacktestLocal();
      } else {
        alert('请先加载数据');
      }
    });
  }
}

function runBacktestLocal() {
  if (!AppState.data || typeof Backtest === 'undefined') return;

  const runBtn = document.getElementById('run-backtest-btn');
  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = '回测运行中...';
  }

  setTimeout(() => {
    const config = AppState.backtestConfig;
    const results = Backtest.run(
      AppState.data.stocks,
      AppState.data.benchmark,
      AppState.weights,
      config.frequency,
      config.topN,
      config.startDate,
      config.endDate
    );

    AppState.backtestResults = results;
    document.getElementById('backtest-results').style.display = 'block';

    // Render local results
    Backtest.renderResults(results);

    if (runBtn) {
      runBtn.disabled = false;
      runBtn.textContent = '运行回测';
    }
  }, 50);
}

// ─── Remove Loading Overlay ───────────────────────────────────────────────────
function removeOverlay() {
  const overlay = document.getElementById('app-loading');
  if (overlay) {
    overlay.classList.add('fade-out');
    setTimeout(() => {
      if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
    }, 500);
  }
}

// ─── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  const dl = window._debugLog || function(){};
  dl('DOMContentLoaded fired');
  try {
    initTabs();
    dl('initTabs OK');
    initTableSort();
    dl('initTableSort OK');
    startClock();
    dl('startClock OK');
  } catch (e) {
    dl('Init error: ' + e.message, '#f44');
    console.error('[Quant] Init error:', e);
  }
  // Always remove overlay after 3 seconds max, even if loadData fails
  setTimeout(removeOverlay, 3000);
  loadData().then(() => {
    dl('loadData complete');
  }).catch(e => {
    dl('loadData error: ' + e.message, '#f44');
    console.error('[Quant] loadData error:', e);
    removeOverlay();
  });
});
