/**
 * charts.js — Chart configurations and rendering
 * A股量化选股系统
 */

'use strict';

const Charts = (() => {

  // ─── Color palette ────────────────────────────────────────────────────
  const C = {
    blue: '#3B82F6',
    blueDim: 'rgba(59,130,246,0.15)',
    green: '#10B981',
    greenDim: 'rgba(16,185,129,0.15)',
    red: '#EF4444',
    redDim: 'rgba(239,68,68,0.15)',
    amber: '#F59E0B',
    purple: '#8B5CF6',
    cyan: '#06B6D4',
    gray: '#6B7280',
    grayDim: 'rgba(107,114,128,0.15)',
    text: '#8B95A8',
    textLight: '#4E5A6E',
    grid: 'rgba(255,255,255,0.05)',
    border: 'rgba(255,255,255,0.06)'
  };

  const SECTOR_COLORS = [
    '#3B82F6','#10B981','#F59E0B','#8B5CF6','#EF4444',
    '#06B6D4','#EC4899','#84CC16','#F97316','#14B8A6',
    '#A78BFA','#FB7185'
  ];

  // ─── Shared Chart.js defaults ────────────────────────────────────────────
  function getBaseOptions(opts = {}) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: opts.animDuration || 600, easing: 'easeOutQuart' },
      plugins: {
        legend: {
          display: opts.legend || false,
          labels: {
            color: C.text,
            font: { family: "'Inter', sans-serif", size: 11 },
            boxWidth: 12,
            padding: 16
          }
        },
        tooltip: {
          backgroundColor: 'rgba(15,21,38,0.95)',
          borderColor: 'rgba(255,255,255,0.1)',
          borderWidth: 1,
          titleColor: '#E8ECF4',
          bodyColor: '#8B95A8',
          titleFont: { family: "'Inter', sans-serif", size: 12, weight: '600' },
          bodyFont: { family: "'JetBrains Mono', monospace", size: 11 },
          padding: 10,
          cornerRadius: 6,
          displayColors: opts.tooltipColors !== false,
          callbacks: opts.tooltipCallbacks || {}
        }
      },
      scales: {}
    };
  }

  function getBaseScales(opts = {}) {
    return {
      x: {
        grid: { color: C.grid, drawTicks: false },
        ticks: {
          color: C.text,
          font: { family: "'JetBrains Mono', monospace", size: 10 },
          maxRotation: 0,
          maxTicksLimit: opts.xMaxTicks || 8
        },
        border: { color: 'transparent' }
      },
      y: {
        position: opts.yPosition || 'right',
        grid: { color: C.grid, drawTicks: false },
        ticks: {
          color: C.text,
          font: { family: "'JetBrains Mono', monospace", size: 10 },
          padding: 8,
          maxTicksLimit: opts.yMaxTicks || 6,
          callback: opts.yTickCallback || (v => v)
        },
        border: { color: 'transparent' }
      }
    };
  }

  // ─── Registry to track instances ──────────────────────────────────────
  const _charts = {};

  function destroyChart(id) {
    if (_charts[id]) {
      _charts[id].destroy();
      delete _charts[id];
    }
  }

  function createChart(id, config) {
    destroyChart(id);
    const canvas = document.getElementById(id);
    if (!canvas) return null;
    const chart = new Chart(canvas, config);
    _charts[id] = chart;
    return chart;
  }

  // ─── Price Line Chart (Stock Analysis) ─────────────────────────────────

  function renderPriceChart(canvasId, prices, technicals) {
    // Use last 250 days
    const recent = prices.slice(-250);
    const labels = recent.map(p => p.date.slice(5));

    const closes = recent.map(p => p.close);

    // Build MA arrays aligned to the same 250-day window
    const allPrices = prices;
    const offset = prices.length - 250;

    function buildMA(period) {
      const result = [];
      for (let i = 0; i < recent.length; i++) {
        const absIdx = offset + i;
        if (absIdx < period - 1) { result.push(null); continue; }
        let sum = 0;
        for (let j = 0; j < period; j++) sum += allPrices[absIdx - j].close;
        result.push(Math.round(sum / period * 100) / 100);
      }
      return result;
    }

    const ma5 = buildMA(5);
    const ma20 = buildMA(20);
    const ma60 = buildMA(60);

    const options = getBaseOptions({ legend: true, animDuration: 800 });
    options.scales = getBaseScales({ yMaxTicks: 7 });
    options.scales.x.ticks.maxTicksLimit = 12;
    options.plugins.tooltip.callbacks = {
      title: (items) => items[0].label,
      label: (item) => {
        const val = item.parsed.y;
        if (val == null) return null;
        return ` ${item.dataset.label}: ¥${val.toFixed(2)}`;
      }
    };

    return createChart(canvasId, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: '收盘价',
            data: closes,
            borderColor: C.blue,
            borderWidth: 1.5,
            pointRadius: 0,
            pointHoverRadius: 4,
            fill: {
              target: 'origin',
              above: 'rgba(59,130,246,0.05)'
            },
            tension: 0.2,
            order: 1
          },
          {
            label: 'MA5',
            data: ma5,
            borderColor: C.amber,
            borderWidth: 1,
            pointRadius: 0,
            borderDash: [],
            tension: 0.3,
            order: 0
          },
          {
            label: 'MA20',
            data: ma20,
            borderColor: C.purple,
            borderWidth: 1,
            pointRadius: 0,
            tension: 0.3,
            order: 0
          },
          {
            label: 'MA60',
            data: ma60,
            borderColor: C.green,
            borderWidth: 1.5,
            pointRadius: 0,
            tension: 0.3,
            order: 0
          }
        ]
      },
      options
    });
  }

  // ─── Volume Bar Chart ─────────────────────────────────────────────────

  function renderVolumeChart(canvasId, prices) {
    const recent = prices.slice(-250);
    const labels = recent.map(p => p.date.slice(5));
    const volumes = recent.map(p => p.volume);
    const colors = recent.map((p, i) => {
      if (i === 0) return C.gray;
      return p.close >= recent[i - 1].close ? C.red : C.green;
    });

    const options = getBaseOptions({ animDuration: 500 });
    options.scales = getBaseScales({ yMaxTicks: 4 });
    options.scales.x.ticks.maxTicksLimit = 12;
    options.scales.y.ticks.callback = v => {
      if (v >= 1e8) return `${(v / 1e8).toFixed(1)}亿`;
      if (v >= 1e4) return `${(v / 1e4).toFixed(0)}万`;
      return v;
    };

    return createChart(canvasId, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: '成交量',
          data: volumes,
          backgroundColor: colors,
          borderWidth: 0,
          borderRadius: 1
        }]
      },
      options
    });
  }

  // ─── Quarterly Revenue/Income Bar Chart ──────────────────────────────────

  function renderQuarterlyChart(canvasId, quarters) {
    if (!quarters || quarters.length === 0) return;
    // Quarter data: { year, quarter, revenue (亿元), net_income (亿元) }
    const labels = quarters.map(q => `${q.year}Q${q.quarter}`);
    const revenue = quarters.map(q => q.revenue); // already 亿元
    const netIncome = quarters.map(q => q.net_income); // already 亿元

    const options = getBaseOptions({ legend: true, animDuration: 700 });
    options.scales = getBaseScales({ yMaxTicks: 6 });
    options.scales.y.ticks.callback = v => v >= 1000 ? `${(v/1000).toFixed(1)}千亿` : `${v.toFixed(0)}亿`;
    options.scales.x.ticks.maxTicksLimit = 8;

    return createChart(canvasId, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label: '营业收入',
            data: revenue,
            backgroundColor: 'rgba(59,130,246,0.7)',
            borderColor: C.blue,
            borderWidth: 1,
            borderRadius: 3
          },
          {
            label: '净利润',
            data: netIncome,
            backgroundColor: 'rgba(16,185,129,0.7)',
            borderColor: C.green,
            borderWidth: 1,
            borderRadius: 3
          }
        ]
      },
      options
    });
  }

  // ─── MACD Chart ─────────────────────────────────────────────────────────

  function renderMACDChart(canvasId, prices) {
    const recent = prices.slice(-120);
    const labels = recent.map(p => p.date.slice(5));

    // Calculate MACD from prices
    function ema(data, period) {
      const k = 2 / (period + 1);
      const result = [data[0]];
      for (let i = 1; i < data.length; i++) {
        result.push(data[i] * k + result[i - 1] * (1 - k));
      }
      return result;
    }

    const closes = recent.map(p => p.close);
    const ema12 = ema(closes, 12);
    const ema26 = ema(closes, 26);
    const dif = ema12.map((v, i) => Math.round((v - ema26[i]) * 100) / 100);

    // DEA (9-day EMA of DIF)
    const k = 2 / (9 + 1);
    const dea = [dif[0]];
    for (let i = 1; i < dif.length; i++) {
      dea.push(dif[i] * k + dea[i - 1] * (1 - k));
    }

    const macd = dif.map((v, i) => Math.round((v - dea[i]) * 2 * 100) / 100);

    const options = getBaseOptions({ legend: true, animDuration: 500 });
    options.scales = getBaseScales({ yMaxTicks: 5 });
    options.scales.x.ticks.maxTicksLimit = 8;

    return createChart(canvasId, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label: 'MACD',
            data: macd,
            backgroundColor: macd.map(v => v >= 0 ? 'rgba(239,68,68,0.6)' : 'rgba(16,185,129,0.6)'),
            borderWidth: 0,
            borderRadius: 1,
            type: 'bar',
            order: 1
          },
          {
            label: 'DIF',
            data: dif,
            borderColor: C.amber,
            borderWidth: 1.5,
            pointRadius: 0,
            type: 'line',
            tension: 0.3,
            order: 0
          },
          {
            label: 'DEA',
            data: dea,
            borderColor: C.purple,
            borderWidth: 1.5,
            pointRadius: 0,
            type: 'line',
            tension: 0.3,
            order: 0
          }
        ]
      },
      options
    });
  }

  // ─── Sector Donut Chart ────────────────────────────────────────────────

  function renderSectorChart(canvasId, sectorDist) {
    if (!sectorDist || sectorDist.length === 0) return;

    const options = {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 700, easing: 'easeOutQuart' },
      cutout: '62%',
      layout: { padding: { right: 10 } },
      plugins: {
        legend: {
          display: true,
          position: 'right',
          labels: {
            color: C.text,
            font: { family: "'Inter', sans-serif", size: 10 },
            boxWidth: 10,
            padding: 6,
            generateLabels: (chart) => {
              const data = chart.data;
              return data.labels.map((label, i) => ({
                text: `${label}(${data.datasets[0].data[i]})`,
                fillStyle: data.datasets[0].backgroundColor[i],
                strokeStyle: 'transparent',
                hidden: false,
                index: i
              }));
            }
          }
        },
        tooltip: {
          backgroundColor: 'rgba(15,21,38,0.95)',
          borderColor: 'rgba(255,255,255,0.1)',
          borderWidth: 1,
          titleColor: '#E8ECF4',
          bodyColor: '#8B95A8',
          bodyFont: { family: "'JetBrains Mono', monospace", size: 11 },
          padding: 10,
          cornerRadius: 6,
          callbacks: {
            label: (item) => {
              const total = item.chart.data.datasets[0].data.reduce((a, b) => a + b, 0);
              const pct = Math.round(item.parsed / total * 1000) / 10;
              return ` ${item.label}: ${item.parsed} 只 (${pct}%)`;
            }
          }
        }
      }
    };

    return createChart(canvasId, {
      type: 'doughnut',
      data: {
        labels: sectorDist.map(s => s.sector),
        datasets: [{
          data: sectorDist.map(s => s.count),
          backgroundColor: SECTOR_COLORS.slice(0, sectorDist.length),
          borderColor: '#0A0E1A',
          borderWidth: 2
        }]
      },
      options
    });
  }

  // ─── Score Histogram ───────────────────────────────────────────────────

  function renderScoreHistogram(canvasId, histogram) {
    const options = getBaseOptions({ animDuration: 600 });
    options.scales = getBaseScales({ yMaxTicks: 5, yPosition: 'left' });
    options.scales.y.ticks.callback = v => v;
    options.scales.y.grace = '15%'; // padding at top so tallest bar doesn't clip
    options.scales.x.ticks.maxTicksLimit = 10;

    const maxVal = Math.max(...histogram.map(h => h.count), 1);
    const colors = histogram.map(h => {
      const pct = h.count / maxVal;
      const score = parseInt(h.label.split('-')[0]);
      if (score >= 70) return `rgba(59,130,246,${0.4 + pct * 0.6})`;
      if (score >= 50) return `rgba(139,92,246,${0.4 + pct * 0.6})`;
      return `rgba(107,114,128,${0.3 + pct * 0.5})`;
    });

    return createChart(canvasId, {
      type: 'bar',
      data: {
        labels: histogram.map(h => h.label),
        datasets: [{
          label: '股票数量',
          data: histogram.map(h => h.count),
          backgroundColor: colors,
          borderWidth: 0,
          borderRadius: 3
        }]
      },
      options
    });
  }

  // ─── Equity Curve Chart (Backtest) ─────────────────────────────────────────

  function renderEquityChart(canvasId, equityCurve) {
    const labels = equityCurve.map(p => p.date.slice(0, 7));
    const portfolio = equityCurve.map(p => p.portfolio);
    const benchmark = equityCurve.map(p => p.benchmark);

    const options = getBaseOptions({ legend: true, animDuration: 1000 });
    options.scales = getBaseScales({ yMaxTicks: 7 });
    options.scales.x.ticks.maxTicksLimit = 10;
    options.scales.y.ticks.callback = v => `${v.toFixed(0)}`;
    options.plugins.tooltip.callbacks = {
      label: (item) => {
        const val = item.parsed.y;
        const ret = ((val - 100) / 100 * 100).toFixed(2);
        return ` ${item.dataset.label}: ${val.toFixed(2)} (${ret > 0 ? '+' : ''}${ret}%)`;
      }
    };

    return createChart(canvasId, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: '策略组合',
            data: portfolio,
            borderColor: C.blue,
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 4,
            fill: {
              target: 'origin',
              above: 'rgba(59,130,246,0.08)'
            },
            tension: 0.2
          },
          {
            label: '沪深300',
            data: benchmark,
            borderColor: C.amber,
            borderWidth: 1.5,
            borderDash: [4, 3],
            pointRadius: 0,
            pointHoverRadius: 4,
            fill: false,
            tension: 0.2
          }
        ]
      },
      options
    });
  }

  // ─── RSI Chart ──────────────────────────────────────────────────────────

  function renderRSIChart(canvasId, prices) {
    const recent = prices.slice(-60);
    const labels = recent.map(p => p.date.slice(5));

    // Calculate RSI-14
    const closes = prices.map(p => p.close);
    const startIdx = prices.length - 60;

    function calcRSI(allCloses, from, len) {
      const rsiVals = [];
      for (let i = from; i < from + len; i++) {
        if (i < 14) { rsiVals.push(50); continue; }
        let gains = 0, losses = 0;
        for (let j = i - 13; j <= i; j++) {
          const d = allCloses[j] - allCloses[j - 1];
          if (d >= 0) gains += d; else losses -= d;
        }
        const avgGain = gains / 14;
        const avgLoss = losses / 14;
        const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
        rsiVals.push(Math.round((100 - 100 / (1 + rs)) * 10) / 10);
      }
      return rsiVals;
    }

    const rsiData = calcRSI(closes, startIdx, 60);

    const options = getBaseOptions({ legend: false, animDuration: 400 });
    options.scales = getBaseScales({ yMaxTicks: 5 });
    options.scales.y.min = 0;
    options.scales.y.max = 100;
    options.scales.x.ticks.maxTicksLimit = 8;
    options.scales.y.ticks.callback = v => v;

    // Add reference lines via plugin
    options.plugins.annotation = {
      annotations: {
        overbought: {
          type: 'line',
          yMin: 70, yMax: 70,
          borderColor: 'rgba(239,68,68,0.4)',
          borderWidth: 1,
          borderDash: [4, 3]
        },
        oversold: {
          type: 'line',
          yMin: 30, yMax: 30,
          borderColor: 'rgba(16,185,129,0.4)',
          borderWidth: 1,
          borderDash: [4, 3]
        }
      }
    };

    return createChart(canvasId, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'RSI(14)',
          data: rsiData,
          borderColor: C.purple,
          borderWidth: 1.5,
          pointRadius: 0,
          pointHoverRadius: 3,
          fill: {
            target: { value: 50 },
            above: 'rgba(239,68,68,0.05)',
            below: 'rgba(16,185,129,0.05)'
          },
          tension: 0.3
        }]
      },
      options
    });
  }

  // ─── Public API ───────────────────────────────────────────────────

  return {
    renderPriceChart,
    renderVolumeChart,
    renderQuarterlyChart,
    renderMACDChart,
    renderSectorChart,
    renderScoreHistogram,
    renderEquityChart,
    renderRSIChart,
    destroyChart,
    SECTOR_COLORS
  };

})();
