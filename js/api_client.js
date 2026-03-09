/**
 * api_client.js — 后端 API 客户端
 * 处理与 FastAPI 后端的所有通信
 */

'use strict';

const ApiClient = (() => {
  // 自部署模式：Nginx 在同一端口代理 /api/ 到 FastAPI 后端
  // Perplexity 平台部署时使用 '__PORT_8000__'
  const BASE = '';

  async function request(path, options = {}) {
    const url = `${BASE}${path}`;
    const defaults = {
      headers: { 'Content-Type': 'application/json' },
    };
    const config = { ...defaults, ...options };
    if (options.body && typeof options.body === 'object') {
      config.body = JSON.stringify(options.body);
    }

    try {
      const res = await fetch(url, config);
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.detail || `HTTP ${res.status}`);
      }
      return data;
    } catch (err) {
      if (err.message.includes('Failed to fetch') || err.message.includes('NetworkError')) {
        throw new Error('后端服务未启动或不可达。当前使用演示数据模式。');
      }
      throw err;
    }
  }

  return {
    // ─── Health ──────────────────────────────────────────────
    health: () => request('/api/health'),

    // ─── Token ──────────────────────────────────────────────
    setToken: (token) => request('/api/token', {
      method: 'POST',
      body: { token }
    }),

    getTokenStatus: () => request('/api/token/status'),

    // ─── Data ───────────────────────────────────────────────
    refreshData: (full = false) => request(`/api/data/refresh?full=${full}`, {
      method: 'POST'
    }),

    refreshStatus: () => request('/api/data/refresh/status'),

    getDataStatus: () => request('/api/data/status'),

    // ─── Stocks ─────────────────────────────────────────────
    listStocks: () => request('/api/stocks'),

    getStockDetail: (code) => request(`/api/stocks/${encodeURIComponent(code)}`),

    // ─── Screener ───────────────────────────────────────────
    runScreener: (weights, filters = {}, sectors = []) => request('/api/screener', {
      method: 'POST',
      body: { weights, filters, sectors }
    }),

    // ─── Backtest ───────────────────────────────────────────
    runBacktest: (params) => request('/api/backtest', {
      method: 'POST',
      body: params
    }),

    // ─── Benchmark ──────────────────────────────────────────
    getBenchmark: () => request('/api/benchmark'),

    // ─── Sectors ────────────────────────────────────────────
    getSectors: () => request('/api/sectors'),

    // ─── Utility ────────────────────────────────────────────
    isAvailable: async () => {
      try {
        await request('/api/health');
        return true;
      } catch {
        return false;
      }
    }
  };
})();
