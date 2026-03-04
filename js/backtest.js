/**
 * backtest.js — Backtesting Engine
 * A股量化选股系统
 */

'use strict';

const Backtest = (() => {

  // ─── Date / index helpers ─────────────────────────────────────────────────

  function dateToStr(d) {
    return d.toISOString().split('T')[0];
  }

  function getDateRange(stocks) {
    const allDates = stocks[0]?.prices.map(p => p.date) || [];
    return {
      start: allDates[0],
      end: allDates[allDates.length - 1],
      dates: allDates
    };
  }

  function findPriceIndex(dates, targetDate) {
    // Binary search for nearest date
    let lo = 0, hi = dates.length - 1, best = 0;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (dates[mid] <= targetDate) { best = mid; lo = mid + 1; }
      else { hi = mid - 1; }
    }
    return best;
  }

  /**
   * Get rebalance dates within the range.
   * frequency: 'monthly' or 'quarterly'
   */
  function getRebalanceDates(allDates, startDate, endDate, frequency) {
    const result = [];
    let lastYM = null;
    let lastYQ = null;

    for (let i = 0; i < allDates.length; i++) {
      const d = allDates[i];
      if (d < startDate || d > endDate) continue;

      const year = d.slice(0, 4);
      const month = parseInt(d.slice(5, 7));
      const quarter = Math.floor((month - 1) / 3);

      if (frequency === 'monthly') {
        const ym = `${year}-${String(month).padStart(2, '0')}`;
        if (ym !== lastYM) {
          result.push({ date: d, idx: i });
          lastYM = ym;
        }
      } else {
        const yq = `${year}-Q${quarter}`;
        if (yq !== lastYQ) {
          result.push({ date: d, idx: i });
          lastYQ = yq;
        }
      }
    }
    return result;
  }

  // ─── Main Backtest ────────────────────────────────────────────────────────

  /**
   * Run the backtest.
   * @param {Array} stocks - full stock universe
   * @param {Object} benchmark - { prices: [{date, close}...] }
   * @param {Object} config - { startDate, endDate, frequency, topN, weights }
   * @returns {Object} backtest results
   */
  function run(stocks, benchmark, config) {
    const { startDate, endDate, frequency = 'monthly', topN = 10, weights } = config;

    const allDates = stocks[0]?.prices.map(p => p.date) || [];
    const rebalanceDates = getRebalanceDates(allDates, startDate, endDate, frequency);

    if (rebalanceDates.length < 2) {
      return { error: '时间段太短，请选择更长的回测区间' };
    }

    // Build price index lookup for each stock
    const stockPriceByDate = {};
    for (const s of stocks) {
      stockPriceByDate[s.code] = {};
      for (const p of s.prices) {
        stockPriceByDate[s.code][p.date] = p;
      }
    }

    // Build benchmark price lookup
    const benchPriceByDate = {};
    for (const p of benchmark.prices) {
      benchPriceByDate[p.date] = p;
    }

    // ── Equity Curve ─────────────────────────────────────────────────────────
    let portfolioValue = 100;
    let benchmarkValue = 100;

    const equityCurve = []; // { date, portfolio, benchmark }
    const holdingsHistory = []; // { period, stocks }
    const periodReturns = []; // { date, return }

    let currentHoldings = []; // array of stock codes
    let holdingEntryPrices = {}; // { code: price }

    // Benchmark start price
    const benchStartIdx = findPriceIndex(allDates, startDate);
    const benchStartPrice = benchPriceByDate[allDates[benchStartIdx]]?.close || 1;
    let lastBenchPrice = benchStartPrice;

    for (let r = 0; r < rebalanceDates.length; r++) {
      const rb = rebalanceDates[r];
      const nextRb = rebalanceDates[r + 1] || { date: endDate, idx: allDates.length - 1 };

      // Score stocks at this rebalance point
      const scored = Screener.scoreAtDate(stocks, rb.idx, weights);
      const topStocks = scored.slice(0, topN);
      const newCodes = topStocks.map(s => s.code);

      // Record holdings
      holdingsHistory.push({
        date: rb.date,
        stocks: topStocks.map(s => ({
          code: s.code,
          name: s.name,
          score: s.compositeScore,
          price: s._snapshotPrice
        }))
      });

      // Calculate portfolio return from rb to nextRb
      let periodReturn = 0;
      let validCount = 0;

      for (const code of newCodes) {
        const entryDate = rb.date;
        const exitDate = nextRb.date;
        const entryPriceData = stockPriceByDate[code]?.[entryDate];
        const exitPriceData = stockPriceByDate[code]?.[exitDate];

        if (entryPriceData && exitPriceData && entryPriceData.close > 0) {
          const ret = (exitPriceData.close - entryPriceData.close) / entryPriceData.close;
          periodReturn += ret;
          validCount++;
        }
      }

      if (validCount > 0) periodReturn /= validCount;

      // Benchmark return for this period
      const benchEntryData = benchPriceByDate[rb.date];
      const benchExitData = benchPriceByDate[nextRb.date];
      let benchPeriodReturn = 0;
      if (benchEntryData && benchExitData && benchEntryData.close > 0) {
        benchPeriodReturn = (benchExitData.close - benchEntryData.close) / benchEntryData.close;
      }

      portfolioValue *= (1 + periodReturn);
      benchmarkValue *= (1 + benchPeriodReturn);

      equityCurve.push({
        date: nextRb.date,
        portfolio: Math.round(portfolioValue * 100) / 100,
        benchmark: Math.round(benchmarkValue * 100) / 100
      });

      periodReturns.push({
        date: rb.date,
        portfolioReturn: periodReturn,
        benchmarkReturn: benchPeriodReturn,
        alpha: periodReturn - benchPeriodReturn
      });
    }

    // Prepend start point
    equityCurve.unshift({ date: startDate, portfolio: 100, benchmark: 100 });

    // ── Performance Metrics ───────────────────────────────────────────────────

    const totalReturn = (portfolioValue - 100) / 100;
    const benchTotalReturn = (benchmarkValue - 100) / 100;

    // Annualized return
    const startD = new Date(startDate);
    const endD = new Date(endDate);
    const years = Math.max((endD - startD) / (365.25 * 24 * 3600 * 1000), 0.1);
    const annualizedReturn = Math.pow(1 + totalReturn, 1 / years) - 1;
    const benchAnnualizedReturn = Math.pow(1 + benchTotalReturn, 1 / years) - 1;

    // Max drawdown
    let peak = 100;
    let maxDrawdown = 0;
    for (const pt of equityCurve) {
      if (pt.portfolio > peak) peak = pt.portfolio;
      const dd = (peak - pt.portfolio) / peak;
      if (dd > maxDrawdown) maxDrawdown = dd;
    }

    // Sharpe ratio (assume risk-free = 3% annual)
    const rf = 0.03;
    const rets = periodReturns.map(p => p.portfolioReturn);
    const avgRet = rets.reduce((s, v) => s + v, 0) / rets.length;
    const stdRet = Math.sqrt(rets.reduce((s, v) => s + (v - avgRet) ** 2, 0) / rets.length);
    const periodsPerYear = frequency === 'monthly' ? 12 : 4;
    const sharpeRatio = stdRet > 0 ? ((avgRet * periodsPerYear - rf) / (stdRet * Math.sqrt(periodsPerYear))) : 0;

    // Win rate
    const wins = periodReturns.filter(p => p.portfolioReturn > p.benchmarkReturn).length;
    const winRate = periodReturns.length > 0 ? wins / periodReturns.length : 0;

    // Alpha
    const alpha = annualizedReturn - benchAnnualizedReturn;

    // Monthly returns heatmap
    const monthlyReturns = buildMonthlyReturns(periodReturns, frequency);

    return {
      equityCurve,
      holdingsHistory,
      periodReturns,
      monthlyReturns,
      metrics: {
        totalReturn: Math.round(totalReturn * 10000) / 100,
        annualizedReturn: Math.round(annualizedReturn * 10000) / 100,
        maxDrawdown: Math.round(maxDrawdown * 10000) / 100,
        sharpeRatio: Math.round(sharpeRatio * 100) / 100,
        winRate: Math.round(winRate * 10000) / 100,
        alpha: Math.round(alpha * 10000) / 100,
        benchAnnualizedReturn: Math.round(benchAnnualizedReturn * 10000) / 100
      }
    };
  }

  // ─── Monthly Returns Heatmap ──────────────────────────────────────────────

  function buildMonthlyReturns(periodReturns, frequency) {
    const monthly = {}; // { '2023': { 1: ret, 2: ret, ... } }

    if (frequency === 'monthly') {
      for (const p of periodReturns) {
        const year = p.date.slice(0, 4);
        const month = parseInt(p.date.slice(5, 7));
        if (!monthly[year]) monthly[year] = {};
        monthly[year][month] = (monthly[year][month] || 0) + p.portfolioReturn;
      }
    } else {
      // Quarterly: distribute to months in quarter
      for (const p of periodReturns) {
        const year = p.date.slice(0, 4);
        const month = parseInt(p.date.slice(5, 7));
        const quarter = Math.floor((month - 1) / 3);
        const monthsInQ = [quarter * 3 + 1, quarter * 3 + 2, quarter * 3 + 3];
        const perMonth = p.portfolioReturn / 3;
        if (!monthly[year]) monthly[year] = {};
        for (const m of monthsInQ) {
          monthly[year][m] = (monthly[year][m] || 0) + perMonth;
        }
      }
    }

    return monthly;
  }

  // ─── Public API ───────────────────────────────────────────────────────────

  return {
    run,
    getDateRange
  };

})();
