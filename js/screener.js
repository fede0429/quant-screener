/**
 * screener.js — Factor Calculation & Scoring Engine
 * A股量化选股系统
 */

'use strict';

const Screener = (() => {

  // ─── Percentile helpers ───────────────────────────────────────────────────

  /**
   * Compute the percentile rank of a value within an array.
   * higher value = better (ascending=false for "higher is better")
   * lower value = better (ascending=true for "lower is better")
   */
  function percentileRank(values, val, ascending = false) {
    if (!values || values.length === 0) return 50;
    const valid = values.filter(v => v != null && isFinite(v));
    if (valid.length === 0) return 50;
    const sorted = [...valid].sort((a, b) => a - b);
    const rank = sorted.filter(v => (ascending ? v <= val : v >= val)).length;
    return (rank / sorted.length) * 100;
  }

  /**
   * Z-score then convert to 0-100 scale via percentile.
   * Returns score between 0 and 100.
   * higherIsBetter: true => higher raw = higher score
   */
  function computeFactorScore(values, val, higherIsBetter = true) {
    if (val == null || !isFinite(val)) return 50;
    const valid = values.filter(v => v != null && isFinite(v));
    if (valid.length < 2) return 50;

    const mean = valid.reduce((s, v) => s + v, 0) / valid.length;
    const variance = valid.reduce((s, v) => s + (v - mean) ** 2, 0) / valid.length;
    const std = Math.sqrt(variance) || 1;

    const zscore = (val - mean) / std;
    // Convert z-score to percentile using normal CDF approximation
    const percentile = normalCDF(zscore) * 100;
    return higherIsBetter ? percentile : 100 - percentile;
  }

  /** Normal CDF approximation (Abramowitz & Stegun) */
  function normalCDF(z) {
    const t = 1 / (1 + 0.2316419 * Math.abs(z));
    const d = 0.3989423 * Math.exp(-z * z / 2);
    const poly = t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
    const cdf = 1 - d * poly;
    return z >= 0 ? cdf : 1 - cdf;
  }

  // ─── Extract universe values ───────────────────────────────────────────────

  function extractValues(stocks, field) {
    return stocks.map(s => {
      const parts = field.split('.');
      let v = s;
      for (const p of parts) v = v?.[p];
      return (v != null && isFinite(v)) ? v : null;
    });
  }

  // ─── Main Scoring ──────────────────────────────────────────────────────────

  /**
   * Score a single stock against the full universe.
   * Returns { valueScore, growthScore, qualityScore, momentumScore, compositeScore }
   */
  function scoreStock(stock, universe, weights) {
    const f = stock.financials;
    const t = stock.technicals;

    // ── VALUE SCORE ──────────────────────────────────────────────────────────
    // PE: lower is better
    const peVals = extractValues(universe, 'financials.pe');
    const peScore = computeFactorScore(peVals, f.pe, false);

    // PB: lower is better
    const pbVals = extractValues(universe, 'financials.pb');
    const pbScore = computeFactorScore(pbVals, f.pb, false);

    // Dividend yield: higher is better
    const divVals = extractValues(universe, 'financials.dividend_yield');
    const divScore = computeFactorScore(divVals, f.dividend_yield, true);

    // PS: lower is better
    const psVals = extractValues(universe, 'financials.ps');
    const psScore = computeFactorScore(psVals, f.ps, false);

    const valueScore = clamp((peScore * 0.35 + pbScore * 0.30 + divScore * 0.20 + psScore * 0.15), 0, 100);

    // ── GROWTH SCORE ─────────────────────────────────────────────────────────
    const revGrowthVals = extractValues(universe, 'financials.revenue_growth_yoy');
    const revGrowthScore = computeFactorScore(revGrowthVals, f.revenue_growth_yoy, true);

    const netIncomeVals = extractValues(universe, 'financials.net_income_growth_yoy');
    const netIncomeScore = computeFactorScore(netIncomeVals, f.net_income_growth_yoy, true);

    const roeVals = extractValues(universe, 'financials.roe');
    const roeScore = computeFactorScore(roeVals, f.roe, true);

    const growthScore = clamp((revGrowthScore * 0.40 + netIncomeScore * 0.35 + roeScore * 0.25), 0, 100);

    // ── QUALITY SCORE ────────────────────────────────────────────────────────
    const grossMarginVals = extractValues(universe, 'financials.gross_margin');
    const grossMarginScore = computeFactorScore(grossMarginVals, f.gross_margin, true);

    const debtVals = extractValues(universe, 'financials.debt_ratio');
    const debtScore = computeFactorScore(debtVals, f.debt_ratio, false); // lower debt = better

    const currentRatioVals = extractValues(universe, 'financials.current_ratio');
    const currentRatioScore = computeFactorScore(currentRatioVals, f.current_ratio, true);

    const fcfVals = extractValues(universe, 'financials.free_cash_flow_yield');
    const fcfScore = computeFactorScore(fcfVals, f.free_cash_flow_yield, true);

    const qualityScore = clamp((grossMarginScore * 0.30 + debtScore * 0.25 + currentRatioScore * 0.25 + fcfScore * 0.20), 0, 100);

    // ── MOMENTUM SCORE ───────────────────────────────────────────────────────
    const mom20Vals = extractValues(universe, 'technicals.momentum_20d');
    const mom20Score = computeFactorScore(mom20Vals, t.momentum_20d, true);

    const mom60Vals = extractValues(universe, 'technicals.momentum_60d');
    const mom60Score = computeFactorScore(mom60Vals, t.momentum_60d, true);

    // RSI: optimal 50-70 range → distance from 60 is penalty
    const rsi = t.rsi_14 || 50;
    const rsiOptimal = 60;
    const rsiDistance = Math.abs(rsi - rsiOptimal) / rsiOptimal;
    const rsiScore = clamp(100 * (1 - rsiDistance * 1.5), 0, 100);

    const volRatioVals = extractValues(universe, 'technicals.volume_ratio');
    const volRatioScore = computeFactorScore(volRatioVals, t.volume_ratio || 1, true);

    const momentumScore = clamp((mom20Score * 0.35 + mom60Score * 0.30 + rsiScore * 0.20 + volRatioScore * 0.15), 0, 100);

    // ── COMPOSITE ────────────────────────────────────────────────────────────
    const w = normalizeWeights(weights);
    const compositeScore = clamp(
      valueScore * w.value +
      growthScore * w.growth +
      qualityScore * w.quality +
      momentumScore * w.momentum,
      0, 100
    );

    return {
      valueScore: Math.round(valueScore * 10) / 10,
      growthScore: Math.round(growthScore * 10) / 10,
      qualityScore: Math.round(qualityScore * 10) / 10,
      momentumScore: Math.round(momentumScore * 10) / 10,
      compositeScore: Math.round(compositeScore * 10) / 10
    };
  }

  /** Normalize weights to sum to 1.0 */
  function normalizeWeights(weights) {
    const total = weights.value + weights.growth + weights.quality + weights.momentum;
    if (total === 0) return { value: 0.25, growth: 0.25, quality: 0.25, momentum: 0.25 };
    return {
      value: weights.value / total,
      growth: weights.growth / total,
      quality: weights.quality / total,
      momentum: weights.momentum / total
    };
  }

  function clamp(val, min, max) {
    return Math.max(min, Math.min(max, val));
  }

  // ─── Filter logic ──────────────────────────────────────────────────────────

  function applyFilters(stocks, filters) {
    return stocks.filter(s => {
      const f = s.financials;
      const t = s.technicals;

      // Value filters
      if (filters.peMin != null && f.pe < filters.peMin) return false;
      if (filters.peMax != null && f.pe > filters.peMax) return false;
      if (filters.pbMin != null && f.pb < filters.pbMin) return false;
      if (filters.pbMax != null && f.pb > filters.pbMax) return false;
      if (filters.divYieldMin != null && f.dividend_yield < filters.divYieldMin) return false;
      if (filters.psMax != null && f.ps > filters.psMax) return false;

      // Growth filters
      if (filters.revGrowthMin != null && f.revenue_growth_yoy < filters.revGrowthMin) return false;
      if (filters.netIncomeGrowthMin != null && f.net_income_growth_yoy < filters.netIncomeGrowthMin) return false;
      if (filters.roeMin != null && f.roe < filters.roeMin) return false;

      // Quality filters
      if (filters.grossMarginMin != null && f.gross_margin < filters.grossMarginMin) return false;
      if (filters.debtRatioMax != null && f.debt_ratio > filters.debtRatioMax) return false;
      if (filters.currentRatioMin != null && f.current_ratio < filters.currentRatioMin) return false;

      // Momentum filters
      if (filters.mom20Min != null && t.momentum_20d < filters.mom20Min) return false;
      if (filters.mom20Max != null && t.momentum_20d > filters.mom20Max) return false;
      if (filters.rsiMin != null && t.rsi_14 < filters.rsiMin) return false;
      if (filters.rsiMax != null && t.rsi_14 > filters.rsiMax) return false;

      // Sector filter
      if (filters.sectors && filters.sectors.length > 0) {
        if (!filters.sectors.includes(s.sector)) return false;
      }

      return true;
    });
  }

  // ─── Main screening function ──────────────────────────────────────────────

  /**
   * Run the full screening pipeline.
   * @param {Array} stocks - full stock universe
   * @param {Object} filters - filter criteria
   * @param {Object} weights - { value, growth, quality, momentum } (0-100 each)
   * @returns {Array} scored and sorted stocks
   */
  function runScreener(stocks, filters, weights) {
    // Step 1: Filter
    const filtered = applyFilters(stocks, filters);
    if (filtered.length === 0) return [];

    // Step 2: Score against the FILTERED universe (relative ranking)
    // But we use the full universe for percentile context
    const universe = stocks; // use full universe for relative ranking

    const scored = filtered.map(stock => {
      const scores = scoreStock(stock, universe, weights);
      return { ...stock, ...scores };
    });

    // Step 3: Sort by composite score desc
    scored.sort((a, b) => b.compositeScore - a.compositeScore);

    // Step 4: Add rank
    scored.forEach((s, i) => { s.rank = i + 1; });

    return scored;
  }

  // ─── Stats helpers ────────────────────────────────────────────────────────

  function computeStats(scoredStocks) {
    if (!scoredStocks.length) return { count: 0, avgScore: 0, avgPE: 0, avgROE: 0 };
    const n = scoredStocks.length;
    const avgScore = scoredStocks.reduce((s, x) => s + x.compositeScore, 0) / n;
    const avgPE = scoredStocks.reduce((s, x) => s + (x.financials?.pe || 0), 0) / n;
    const avgROE = scoredStocks.reduce((s, x) => s + (x.financials?.roe || 0), 0) / n;
    return {
      count: n,
      avgScore: Math.round(avgScore * 10) / 10,
      avgPE: Math.round(avgPE * 10) / 10,
      avgROE: Math.round(avgROE * 10) / 10
    };
  }

  function getSectorDistribution(scoredStocks) {
    const dist = {};
    for (const s of scoredStocks) {
      dist[s.sector] = (dist[s.sector] || 0) + 1;
    }
    return Object.entries(dist)
      .map(([sector, count]) => ({ sector, count }))
      .sort((a, b) => b.count - a.count);
  }

  function getScoreHistogram(scoredStocks, bins = 10) {
    const binSize = 100 / bins;
    const hist = Array(bins).fill(0);
    for (const s of scoredStocks) {
      const bin = Math.min(Math.floor(s.compositeScore / binSize), bins - 1);
      hist[bin]++;
    }
    return hist.map((count, i) => ({
      label: `${Math.round(i * binSize)}-${Math.round((i + 1) * binSize)}`,
      count
    }));
  }

  // ─── Score stocks at a historical point in time ───────────────────────────
  // Used by backtest — score using historical prices at a given index

  function scoreAtDate(stocks, priceIdx, weights) {
    // Build snapshot financials (same fundamentals, different price-based metrics)
    const snapshot = stocks.map(s => {
      const px = s.prices[priceIdx];
      if (!px) return null;
      // Recompute simple momentum from history
      const prev20 = priceIdx >= 20 ? s.prices[priceIdx - 20]?.close : null;
      const prev60 = priceIdx >= 60 ? s.prices[priceIdx - 60]?.close : null;
      const mom20 = prev20 ? (px.close - prev20) / prev20 * 100 : 0;
      const mom60 = prev60 ? (px.close - prev60) / prev60 * 100 : 0;

      return {
        ...s,
        _priceIdx: priceIdx,
        _snapshotPrice: px.close,
        technicals: {
          ...s.technicals,
          price: px.close,
          momentum_20d: mom20,
          momentum_60d: mom60
        }
      };
    }).filter(Boolean);

    const universe = snapshot;
    return snapshot.map(stock => {
      const scores = scoreStock(stock, universe, weights);
      return { ...stock, ...scores };
    }).sort((a, b) => b.compositeScore - a.compositeScore);
  }

  // ─── Public API ───────────────────────────────────────────────────────────

  return {
    runScreener,
    scoreStock,
    scoreAtDate,
    normalizeWeights,
    computeStats,
    getSectorDistribution,
    getScoreHistogram,
    computeFactorScore,
    clamp
  };

})();
