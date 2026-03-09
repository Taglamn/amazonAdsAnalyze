import React, { useEffect, useMemo, useState } from 'https://esm.sh/react@18.2.0';
import { createRoot } from 'https://esm.sh/react-dom@18.2.0/client';
import htm from 'https://esm.sh/htm@3.1.1';

const html = htm.bind(React.createElement);

const NAV_KEYS = ['dashboard', 'playbook', 'adGroups', 'history'];

const I18N = {
  en: {
    pageTitle: 'Amazon Ads Multi-Tenant Dashboard',
    appTitle: 'Amazon Ads Panel',
    appSubtitle: 'Multi-tenant workspace',
    nav: {
      dashboard: 'Dashboard',
      playbook: 'Playbook Logic',
      adGroups: 'Ad Groups',
      history: 'Optimization History',
    },
    headerNote: 'All APIs enforce strict store-level data isolation.',
    storeSwitcher: 'Store Switcher',
    language: 'Language',
    loading: 'Loading...',
    requestFailed: 'Request failed',
    genericEmpty: 'No content returned.',
    status: {
      healthy: 'Healthy',
      warning: 'Warning',
    },
    dashboardTable: {
      date: 'Date',
      clicks: 'Clicks',
      spend: 'Spend',
      acos: 'ACoS',
      sales: 'Sales',
      status: 'Status',
    },
    recTable: {
      adGroup: 'Ad Group',
      currentBid: 'Current Bid',
      suggestedBid: 'Suggested Bid',
      latestAcos: 'Latest ACoS',
      reason: 'Reason',
    },
    unknownGroup: 'Unknown Group',
    impact: {
      roiChange: 'ROI Change',
      acosChange: 'ACoS Change',
      na: 'N/A',
    },
    playbook: {
      title: 'AI Decision Engine',
      subtitle: 'Generate strategy whitepaper and bid advice with Gemini API.',
      whitepaperBtn: 'Generate Whitepaper',
      adviceBtn: 'Generate Ad Group Advice',
      whitepaperOutput: 'Whitepaper Output',
      adviceOutput: 'Ad Group Advice Output',
      whitepaperEmpty: 'No whitepaper generated yet.',
      adviceEmpty: 'No advice generated yet.',
    },
  },
  zh: {
    pageTitle: '亚马逊广告多店铺管理看板',
    appTitle: '亚马逊广告管理台',
    appSubtitle: '多店铺数据隔离工作台',
    nav: {
      dashboard: '总览看板',
      playbook: '策略白皮书逻辑',
      adGroups: '广告组调优',
      history: '优化历史复盘',
    },
    headerNote: '所有接口均按店铺维度隔离，避免跨店数据混用。',
    storeSwitcher: '店铺切换',
    language: '语言',
    loading: '正在加载...',
    requestFailed: '请求失败',
    genericEmpty: '未返回内容。',
    status: {
      healthy: '健康',
      warning: '预警',
    },
    dashboardTable: {
      date: '日期',
      clicks: '点击量',
      spend: '花费',
      acos: 'ACoS',
      sales: '销售额',
      status: '状态',
    },
    recTable: {
      adGroup: '广告组',
      currentBid: '当前出价',
      suggestedBid: '建议出价',
      latestAcos: '最新 ACoS',
      reason: '调整依据',
    },
    unknownGroup: '未知广告组',
    impact: {
      roiChange: 'ROI 变化',
      acosChange: 'ACoS 变化',
      na: '暂无',
    },
    playbook: {
      title: 'AI 决策引擎',
      subtitle: '基于 Gemini 自动生成策略白皮书与调价建议。',
      whitepaperBtn: '生成策略白皮书',
      adviceBtn: '生成广告组建议',
      whitepaperOutput: '白皮书输出',
      adviceOutput: '广告组建议输出',
      whitepaperEmpty: '暂未生成白皮书。',
      adviceEmpty: '暂未生成建议。',
    },
  },
};

function getDefaultLanguage() {
  try {
    const saved = window.localStorage.getItem('ui_lang');
    if (saved === 'zh' || saved === 'en') {
      return saved;
    }
  } catch (_) {
    // Ignore localStorage failures.
  }
  return navigator.language.toLowerCase().startsWith('zh') ? 'zh' : 'en';
}

function localizeRecommendationReason(reason, language) {
  if (language !== 'zh') return reason;

  const mapping = {
    'ACoS above 45%, reduce bid aggressively.': 'ACoS 高于 45%，建议明显下调出价，优先控损。',
    'ACoS above 30%, reduce bid moderately.': 'ACoS 高于 30%，建议温和下调出价，先稳住效率。',
    'ACoS below 20%, room to scale traffic.': 'ACoS 低于 20%，可适度提价放量，扩大优质流量。',
    'ACoS in target range, hold bid.': 'ACoS 在目标区间内，建议维持当前出价。',
  };

  return mapping[reason] || reason;
}

function statusBadge(acos, t) {
  const warning = Number(acos) > 30;
  const label = warning ? t.status.warning : t.status.healthy;
  const badgeStyle = warning
    ? 'bg-blue-100 text-blue-900 border-blue-300'
    : 'bg-blue-600 text-white border-blue-600';
  return html`<span className=${`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold border ${badgeStyle}`}>${label}</span>`;
}

function fmtMoney(value, language) {
  const amount = Number(value).toFixed(2);
  return language === 'zh' ? `US$${amount}` : `$${amount}`;
}

function fmtPct(value) {
  return `${Number(value).toFixed(2)}%`;
}

function fmtImpact(value, fallbackText) {
  return value === null || value === undefined ? fallbackText : `${value}%`;
}

async function fetchJson(url, options, requestFailedText) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    const err = detail?.detail || `${requestFailedText}: ${res.status}`;
    throw new Error(err);
  }
  return res.json();
}

function DashboardTable({ rows, t, language }) {
  return html`
    <div className="overflow-hidden rounded-xl border border-brand-100 bg-white shadow-sm">
      <table className="min-w-full divide-y divide-brand-100 text-sm">
        <thead className="bg-brand-50">
          <tr>
            <th className="px-4 py-3 text-left font-semibold">${t.dashboardTable.date}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.dashboardTable.clicks}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.dashboardTable.spend}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.dashboardTable.acos}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.dashboardTable.sales}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.dashboardTable.status}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-brand-50">
          ${rows.map(
            (row) => html`
              <tr key=${row.date} className="hover:bg-brand-50/70">
                <td className="px-4 py-3">${row.date}</td>
                <td className="px-4 py-3">${row.clicks}</td>
                <td className="px-4 py-3">${fmtMoney(row.spend, language)}</td>
                <td className="px-4 py-3">${fmtPct(row.acos)}</td>
                <td className="px-4 py-3">${fmtMoney(row.sales, language)}</td>
                <td className="px-4 py-3">${statusBadge(row.acos, t)}</td>
              </tr>
            `,
          )}
        </tbody>
      </table>
    </div>
  `;
}

function RecommendationsTable({ rows, t, language }) {
  return html`
    <div className="overflow-hidden rounded-xl border border-brand-100 bg-white shadow-sm">
      <table className="min-w-full divide-y divide-brand-100 text-sm">
        <thead className="bg-brand-50">
          <tr>
            <th className="px-4 py-3 text-left font-semibold">${t.recTable.adGroup}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.recTable.currentBid}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.recTable.suggestedBid}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.recTable.latestAcos}</th>
            <th className="px-4 py-3 text-left font-semibold">${t.recTable.reason}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-brand-50">
          ${rows.map(
            (row) => html`
              <tr key=${row.ad_group} className="hover:bg-brand-50/70">
                <td className="px-4 py-3">${row.ad_group}</td>
                <td className="px-4 py-3">${fmtMoney(row.current_bid, language)}</td>
                <td className="px-4 py-3 font-semibold text-brand-700">${fmtMoney(row.suggested_bid, language)}</td>
                <td className="px-4 py-3">${fmtPct(row.latest_acos)}</td>
                <td className="px-4 py-3">${localizeRecommendationReason(row.reason, language)}</td>
              </tr>
            `,
          )}
        </tbody>
      </table>
    </div>
  `;
}

function OptimizationCases({ cases, t, language }) {
  return html`
    <div className="grid gap-3 md:grid-cols-2">
      ${cases.map(
        (item) => html`
          <article key=${item.case_id} className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
            <p className="text-xs uppercase tracking-wide text-brand-500">${item.case_id}</p>
            <h3 className="mt-1 text-base font-semibold">${item.change_event?.ad_group || t.unknownGroup}</h3>
            <p className="mt-1 text-sm text-brand-700">
              ${item.target_date}: ${fmtMoney(item.change_event?.old_bid || 0, language)} -> ${fmtMoney(item.change_event?.new_bid || 0, language)}
            </p>
            <div className="mt-3 rounded-lg bg-brand-50 p-3 text-sm">
              <p>${t.impact.roiChange}: <strong>${fmtImpact(item.impact?.roi_change_pct, t.impact.na)}</strong></p>
              <p>${t.impact.acosChange}: <strong>${fmtImpact(item.impact?.acos_change_pct, t.impact.na)}</strong></p>
            </div>
          </article>
        `,
      )}
    </div>
  `;
}

function App() {
  const [language, setLanguage] = useState(getDefaultLanguage());
  const t = useMemo(() => I18N[language], [language]);

  const [stores, setStores] = useState([]);
  const [selectedStore, setSelectedStore] = useState('');
  const [view, setView] = useState('dashboard');
  const [performanceRows, setPerformanceRows] = useState([]);
  const [recommendations, setRecommendations] = useState([]);
  const [cases, setCases] = useState([]);
  const [whitepaper, setWhitepaper] = useState('');
  const [advice, setAdvice] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    document.title = t.pageTitle;
    document.documentElement.lang = language === 'zh' ? 'zh-CN' : 'en';
    try {
      window.localStorage.setItem('ui_lang', language);
    } catch (_) {
      // Ignore localStorage failures.
    }
  }, [t, language]);

  useEffect(() => {
    fetchJson('/api/stores', undefined, I18N.en.requestFailed)
      .then((data) => {
        setStores(data.stores || []);
        if (data.stores?.length) {
          setSelectedStore(data.stores[0]);
        }
      })
      .catch((err) => setError(err.message));
  }, []);

  useEffect(() => {
    if (!selectedStore) return;
    setLoading(true);
    setError('');

    Promise.all([
      fetchJson(`/api/stores/${selectedStore}/performance`, undefined, t.requestFailed),
      fetchJson(`/api/stores/${selectedStore}/ad-group-recommendations`, undefined, t.requestFailed),
      fetchJson(`/api/stores/${selectedStore}/optimization-cases`, undefined, t.requestFailed),
    ])
      .then(([perfRes, recRes, caseRes]) => {
        setPerformanceRows(perfRes.daily_performance || []);
        setRecommendations(recRes.recommendations || []);
        setCases(caseRes.cases || []);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [selectedStore, t.requestFailed]);

  const onGenerateWhitepaper = async () => {
    if (!selectedStore) return;
    setLoading(true);
    setError('');
    try {
      const data = await fetchJson(
        `/api/stores/${selectedStore}/ai/whitepaper`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ lang: language }),
        },
        t.requestFailed,
      );
      setWhitepaper(data.whitepaper || t.genericEmpty);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onGenerateAdvice = async () => {
    if (!selectedStore) return;
    setLoading(true);
    setError('');
    try {
      const data = await fetchJson(
        `/api/stores/${selectedStore}/ai/advice`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ lang: language }),
        },
        t.requestFailed,
      );
      setAdvice(data.advice || t.genericEmpty);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return html`
    <div className="min-h-screen md:flex">
      <aside className="w-full border-b border-brand-100 bg-brand-900 text-brand-50 md:w-64 md:border-b-0 md:border-r">
        <div className="px-5 py-6">
          <h1 className="text-lg font-semibold">${t.appTitle}</h1>
          <p className="mt-1 text-xs text-brand-200">${t.appSubtitle}</p>
        </div>
        <nav className="px-3 pb-6">
          ${NAV_KEYS.map(
            (key) => html`
              <button
                key=${key}
                onClick=${() => setView(key)}
                className=${`mb-2 w-full rounded-lg px-3 py-2 text-left text-sm transition ${
                  key === view
                    ? 'bg-brand-600 text-white'
                    : 'text-brand-100 hover:bg-brand-800'
                }`}
              >
                ${t.nav[key]}
              </button>
            `,
          )}
        </nav>
      </aside>

      <main className="flex-1 p-4 md:p-8">
        <header className="mb-6 flex flex-col gap-3 rounded-xl border border-brand-100 bg-white p-4 shadow-sm md:flex-row md:items-center md:justify-between">
          <div>
            <h2 className="text-xl font-semibold">${t.nav[view]}</h2>
            <p className="text-sm text-brand-600">${t.headerNote}</p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <label htmlFor="language" className="text-sm font-medium text-brand-700">${t.language}</label>
            <select
              id="language"
              className="rounded-md border border-brand-200 px-3 py-2 text-sm"
              value=${language}
              onChange=${(e) => setLanguage(e.target.value)}
            >
              <option value="zh">中文</option>
              <option value="en">English</option>
            </select>

            <label htmlFor="store" className="ml-2 text-sm font-medium text-brand-700">${t.storeSwitcher}</label>
            <select
              id="store"
              className="rounded-md border border-brand-200 px-3 py-2 text-sm"
              value=${selectedStore}
              onChange=${(e) => setSelectedStore(e.target.value)}
            >
              ${stores.map((store) => html`<option key=${store} value=${store}>${store}</option>`) }
            </select>
          </div>
        </header>

        ${error
          ? html`<div className="mb-4 rounded-lg border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-800">${error}</div>`
          : null}

        ${loading ? html`<div className="mb-4 text-sm text-brand-600">${t.loading}</div>` : null}

        ${view === 'dashboard'
          ? html`<${DashboardTable} rows=${performanceRows} t=${t} language=${language} />`
          : null}

        ${view === 'adGroups'
          ? html`<${RecommendationsTable} rows=${recommendations} t=${t} language=${language} />`
          : null}

        ${view === 'history'
          ? html`<${OptimizationCases} cases=${cases} t=${t} language=${language} />`
          : null}

        ${view === 'playbook'
          ? html`
              <section className="space-y-4">
                <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                  <h3 className="text-base font-semibold">${t.playbook.title}</h3>
                  <p className="mt-1 text-sm text-brand-600">${t.playbook.subtitle}</p>
                  <div className="mt-4 flex flex-wrap gap-2">
                    <button
                      onClick=${onGenerateWhitepaper}
                      className="rounded-md bg-brand-700 px-4 py-2 text-sm font-semibold text-white hover:bg-brand-800"
                    >
                      ${t.playbook.whitepaperBtn}
                    </button>
                    <button
                      onClick=${onGenerateAdvice}
                      className="rounded-md border border-brand-300 bg-white px-4 py-2 text-sm font-semibold text-brand-700 hover:bg-brand-50"
                    >
                      ${t.playbook.adviceBtn}
                    </button>
                  </div>
                </div>

                <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                  <h4 className="text-sm font-semibold">${t.playbook.whitepaperOutput}</h4>
                  <pre className="mt-2 max-h-72 overflow-auto whitespace-pre-wrap rounded-lg bg-brand-50 p-3 text-sm text-brand-800">${whitepaper || t.playbook.whitepaperEmpty}</pre>
                </div>

                <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                  <h4 className="text-sm font-semibold">${t.playbook.adviceOutput}</h4>
                  <pre className="mt-2 max-h-72 overflow-auto whitespace-pre-wrap rounded-lg bg-brand-50 p-3 text-sm text-brand-800">${advice || t.playbook.adviceEmpty}</pre>
                </div>
              </section>
            `
          : null}
      </main>
    </div>
  `;
}

createRoot(document.getElementById('root')).render(html`<${App} />`);
