import React, { useEffect, useMemo, useRef, useState } from 'https://esm.sh/react@18.2.0';
import { createRoot } from 'https://esm.sh/react-dom@18.2.0/client';
import htm from 'https://esm.sh/htm@3.1.1';

const html = htm.bind(React.createElement);

const ADS_NAV_KEYS = ['dashboard', 'playbook', 'adGroups', 'history'];
const AUTO_REPLY_NAV_KEYS = ['autoReplyMail'];
const SETTINGS_NAV_KEYS = ['userManagement'];
const AUTH_TOKEN_KEY = 'auth_token';
const AUTH_REFRESH_TOKEN_KEY = 'auth_refresh_token';
const AUTH_UNAUTHORIZED_EVENT = 'auth:unauthorized';
let refreshRequestPromise = null;

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
      autoReplyMail: 'Auto Reply Mail',
      userManagement: 'User Management',
    },
    navGroup: {
      ads: 'Ads',
      autoReply: 'Auto Reply',
      settings: 'Admin',
    },
    autoReply: {
      title: 'Auto Reply Mail',
      subtitle: 'Chat-style customer-service workspace for buyer emails.',
      tip: 'Flow: fetch mail -> AI fills draft -> confirm -> send.',
      fetchBtn: 'Fetch Messages',
      reloadBtn: 'Reload',
      processBtn: 'Generate Reply',
      saveBtn: 'Save Draft',
      sendBtn: 'Send',
      detailLoading: 'Loading detail...',
      leftListTitle: 'Inbox',
      subjectLabel: 'Subject',
      fromLabel: 'From',
      buyerLabel: 'Buyer',
      sellerLabel: 'Our Reply',
      uploadAttachment: 'Upload Attachment',
      attachmentEmpty: 'No attachment selected',
      removeAttachment: 'Remove',
      sentTag: 'Sent',
      draftPlaceholder: 'AI draft reply appears here. You can edit before sending.',
      sendHint: 'Send will auto-approve this draft.',
      empty: 'No messages for this store yet.',
      loadedCount: 'Loaded',
      fetchResult: 'Fetch result',
      selectedCount: 'Selected',
    },
    userMgmt: {
      title: 'User Management',
      onlyAdmin: 'Only admin can manage users.',
      createTitle: 'Create User',
      username: 'Username',
      email: 'Email',
      password: 'Password',
      role: 'Role',
      createBtn: 'Create',
      listTitle: 'Users',
      status: 'Status',
      actions: 'Actions',
      resetPwd: 'Reset Password',
      saveRole: 'Save Role',
      deactivate: 'Deactivate',
      activate: 'Activate',
      stores: 'Store Permissions',
      reload: 'Reload',
      updated: 'Updated',
    },
    headerNote: 'All APIs enforce strict store-level data isolation.',
    storeSwitcher: 'Store Switcher',
    language: 'Language',
    logout: 'Logout',
    loading: 'Loading...',
    requestFailed: 'Request failed',
    genericEmpty: 'No content returned.',
    auth: {
      loginTitle: 'Sign In',
      loginSubtitle: 'Use your account to access store-scoped data.',
      accountLabel: 'Username or Email',
      accountPlaceholder: '',
      passwordLabel: 'Password',
      loginBtn: 'Login',
      loggingIn: 'Signing in...',
      invalid: 'Invalid account or password',
    },
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
      subtitle: 'Generate Lingxing auto-rule blueprint and optimization advice with Gemini API.',
      syncBtn: 'Sync Lingxing Data',
      syncDateRangeLabel: 'Lingxing Sync Date Range',
      syncStartDate: 'Start Date',
      syncEndDate: 'End Date',
      syncQuick30: 'Last 30 Days',
      syncQuick60: 'Last 60 Days',
      syncQuickClear: 'Clear',
      forceRefetchBeforeDate: 'Force Re-fetch Before Date',
      forceRefetchBeforeDateHint: 'Re-fetch all dates <= this date even if local cache exists.',
      syncDateHint: 'Leave both empty for default recent 365 days.',
      syncDateInvalid: 'Please set both start and end dates, and ensure start <= end.',
      syncCurrentStoreOnly: 'Only the currently selected store will be synced.',
      contextExportBtn: 'Download Context Package',
      uploadBtn: 'Upload & Analyze File',
      uploadLabel: 'Ad Group Excel File',
      uploadHint: 'Upload an Excel file with two sheets: daily ad data + operation history.',
      uploadRequired: 'Please choose an Excel file first.',
      whitepaperBtn: 'Generate Auto-Rule Blueprint',
      adviceBtn: 'Optimize Auto Rules',
      whitepaperImportLabel: 'Rule Blueprint Import',
      whitepaperImportHint: 'Upload .txt or .md to overwrite this store auto-rule blueprint.',
      whitepaperImportBtn: 'Import Rule Blueprint',
      whitepaperExportStoredBtn: 'Export Stored Blueprint',
      whitepaperImportRequired: 'Please choose a blueprint file first.',
      uploadSummaryLabel: 'Uploaded File Summary',
      uploadSummaryEmpty: 'No upload analysis has been run yet.',
      whitepaperStorageLabel: 'Stored Auto-Rule Blueprint',
      whitepaperStorageEmpty: 'No auto-rule blueprint has been saved for this store.',
      whitepaperOutput: 'Auto-Rule Blueprint Output',
      adviceOutput: 'Auto-Rule Optimization Output',
      expand: 'Expand',
      collapse: 'Collapse',
      exportTxt: 'Export .txt',
      metaLabel: 'Response Meta',
      finishReason: 'Finish',
      chars: 'Chars',
      lines: 'Lines',
      whitepaperEmpty: 'No auto-rule blueprint generated yet.',
      adviceEmpty: 'No advice generated yet.',
      syncSummaryEmpty: 'Lingxing sync has not run yet.',
      syncSummaryLabel: 'Lingxing Sync Summary',
      syncRangeTotals: 'Selected Date Range Total',
      syncTotalClicks: 'Clicks',
      syncTotalSpend: 'Total Cost',
      syncTotalSales: 'Sales',
      syncTotalAcos: 'ACoS',
      syncTotalAdGroups: 'Ad Groups with Spend',
      syncTotalDays: 'Days',
      storeNotSyncedHint: 'Selected store has no local data yet. Run Lingxing sync first.',
      syncTable: {
        adCombo: 'Ad Type',
        campaign: 'Campaign name',
        adGroup: 'Ad Group',
        currentBid: 'Current Bid',
        suggestedBid: 'Suggested Bid',
        clicks: 'Clicks',
        spend: 'Total Cost',
        sales: 'Sales',
        acos: 'ACoS',
      },
      syncTableEmpty: 'No ad groups with spend in the selected period.',
      sortAsc: 'Ascending',
      sortDesc: 'Descending',
      syncJobLabel: 'Lingxing Sync Task',
      syncJobIdle: 'No Lingxing sync task is running.',
      syncJobQueued: 'Sync task queued, waiting for execution.',
      syncJobRunning: 'Lingxing sync task is running...',
      syncJobDone: 'Lingxing sync completed.',
      syncJobFailed: 'Lingxing sync failed.',
      syncJobProgress: 'Progress',
      syncJobStage: 'Stage',
      syncJobUpdatedAt: 'Last Update',
      syncJobNetworkRetry: 'Network unstable while polling. Task is still running on server; retrying...',
      emptyGuardTitle: 'Historical Data Warning',
      emptyGuardSummary: 'Some historical dates returned empty payloads and were guarded to avoid silent zero overwrite.',
      emptyGuardPending: 'Pending',
      emptyGuardSuspect: 'Suspect',
      emptyGuardConfirmed: 'Confirmed Empty',
      emptyGuardCategory: {
        ad_group_reports: 'Ad Group Reports',
        targeting: 'Targeting',
        negative_targeting: 'Negative Targeting',
        ads: 'Ads',
      },
      contextStoreHint: 'Context Package supports Lingxing stores only (store_id should start with lingxing_).',
      contextJobLabel: 'Context Package Export Task',
      contextJobIdle: 'No context export task is running.',
      contextJobQueued: 'Task queued, waiting for execution.',
      contextJobRunning: 'Task is running...',
      contextJobDone: 'Task finished. Download started.',
      contextJobFailed: 'Context export failed.',
      contextJobProgress: 'Progress',
      contextJobStage: 'Stage',
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
      autoReplyMail: '自动回复邮件',
      userManagement: '用户管理',
    },
    navGroup: {
      ads: '广告',
      autoReply: '自动回复邮件',
      settings: '管理员',
    },
    autoReply: {
      title: '自动回复邮件',
      subtitle: '聊天框模式的买家邮件客服工作台。',
      tip: '流程：拉取邮件 -> AI填充草稿 -> 人工确认 -> 发送。',
      fetchBtn: '拉取邮件',
      reloadBtn: '刷新列表',
      processBtn: '生成回复',
      saveBtn: '保存草稿',
      sendBtn: '发送',
      detailLoading: '正在加载详情...',
      leftListTitle: '邮件列表',
      subjectLabel: '主题',
      fromLabel: '发件人',
      buyerLabel: '买家消息',
      sellerLabel: '历史回复',
      uploadAttachment: '上传附件',
      attachmentEmpty: '暂未选择附件',
      removeAttachment: '移除',
      sentTag: '已发送',
      draftPlaceholder: 'AI 自动回复草稿会填充在这里，可编辑后发送。',
      sendHint: '点击发送会自动审核并发送。',
      empty: '当前店铺暂无邮件消息。',
      loadedCount: '已加载',
      fetchResult: '拉取结果',
      selectedCount: '当前会话',
    },
    userMgmt: {
      title: '用户管理',
      onlyAdmin: '仅管理员可管理用户。',
      createTitle: '新增用户',
      username: '用户名',
      email: '邮箱',
      password: '密码',
      role: '角色',
      createBtn: '创建',
      listTitle: '用户列表',
      status: '状态',
      actions: '操作',
      resetPwd: '重置密码',
      saveRole: '保存角色',
      deactivate: '停用',
      activate: '启用',
      stores: '店铺权限',
      reload: '刷新',
      updated: '已更新',
    },
    headerNote: '所有接口均按店铺维度隔离，避免跨店数据混用。',
    storeSwitcher: '店铺切换',
    language: '语言',
    logout: '退出登录',
    loading: '正在加载...',
    requestFailed: '请求失败',
    genericEmpty: '未返回内容。',
    auth: {
      loginTitle: '登录系统',
      loginSubtitle: '请使用账号访问已授权店铺数据。',
      accountLabel: '用户名或邮箱',
      accountPlaceholder: '',
      passwordLabel: '密码',
      loginBtn: '登录',
      loggingIn: '登录中...',
      invalid: '账号或密码错误',
    },
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
      subtitle: '基于 Gemini 生成可落地的领星自动规则方案，并给出规则优化建议。',
      syncBtn: '同步领星数据并分析',
      syncDateRangeLabel: '领星同步日期筛选',
      syncStartDate: '开始日期',
      syncEndDate: '结束日期',
      syncQuick30: '近30天',
      syncQuick60: '近60天',
      syncQuickClear: '清空',
      forceRefetchBeforeDate: '强制重拉截止日期',
      forceRefetchBeforeDateHint: '即使本地有缓存，也会对该日期及更早日期重新拉取。',
      syncDateHint: '开始和结束都留空时，默认同步最近 365 天。',
      syncDateInvalid: '请同时填写开始/结束日期，并确保开始日期不晚于结束日期。',
      syncCurrentStoreOnly: '仅同步当前已选店铺的数据。',
      contextExportBtn: '下载 Context Package',
      uploadBtn: '上传文档并分析',
      uploadLabel: '广告组 Excel 文件',
      uploadHint: '请上传包含两个 sheet 的 Excel：广告日数据 + 操作历史。',
      uploadRequired: '请先选择 Excel 文件。',
      whitepaperBtn: '生成自动规则方案',
      adviceBtn: '优化自动规则',
      whitepaperImportLabel: '规则方案导入',
      whitepaperImportHint: '上传 .txt 或 .md，覆盖当前店铺自动规则方案。',
      whitepaperImportBtn: '导入规则方案',
      whitepaperExportStoredBtn: '导出已保存规则方案',
      whitepaperImportRequired: '请先选择规则方案文件。',
      uploadSummaryLabel: '上传文档解析结果',
      uploadSummaryEmpty: '尚未执行文档分析。',
      whitepaperStorageLabel: '店铺自动规则方案',
      whitepaperStorageEmpty: '当前店铺还没有保存自动规则方案。',
      whitepaperOutput: '自动规则方案输出',
      adviceOutput: '自动规则优化建议输出',
      expand: '展开全文',
      collapse: '收起',
      exportTxt: '导出 .txt',
      metaLabel: '响应元信息',
      finishReason: '结束原因',
      chars: '字符数',
      lines: '行数',
      whitepaperEmpty: '暂未生成自动规则方案。',
      adviceEmpty: '暂未生成建议。',
      syncSummaryEmpty: '尚未执行领星同步。',
      syncSummaryLabel: '领星同步结果',
      syncRangeTotals: '所选日期区间总计',
      syncTotalClicks: '点击次数',
      syncTotalSpend: '总花费',
      syncTotalSales: '销售额',
      syncTotalAcos: 'ACoS',
      syncTotalAdGroups: '有消耗广告组',
      syncTotalDays: '天数',
      storeNotSyncedHint: '当前店铺还没有本地数据，请先执行领星同步。',
      syncTable: {
        adCombo: '广告组合',
        campaign: 'Campaign name',
        adGroup: '广告组',
        currentBid: '当前bid',
        suggestedBid: '建议bid',
        clicks: '点击次数',
        spend: '总花费',
        sales: '销售额',
        acos: 'ACoS',
      },
      syncTableEmpty: '所选周期内没有消耗金额大于 0 的广告组。',
      sortAsc: '升序',
      sortDesc: '降序',
      syncJobLabel: '领星同步任务',
      syncJobIdle: '当前没有进行中的领星同步任务。',
      syncJobQueued: '同步任务已入队，等待执行。',
      syncJobRunning: '领星同步任务执行中...',
      syncJobDone: '领星同步完成。',
      syncJobFailed: '领星同步失败。',
      syncJobProgress: '进度',
      syncJobStage: '阶段',
      syncJobUpdatedAt: '最后更新时间',
      syncJobNetworkRetry: '轮询网络异常，任务仍在服务器后台运行，正在重试...',
      emptyGuardTitle: '历史数据告警',
      emptyGuardSummary: '部分历史日期返回空数据，系统已启用保护，避免静默覆盖成 0。',
      emptyGuardPending: '待确认',
      emptyGuardSuspect: '疑似空返回',
      emptyGuardConfirmed: '确认空返回',
      emptyGuardCategory: {
        ad_group_reports: '广告组日报',
        targeting: '定向报表',
        negative_targeting: '否定定向报表',
        ads: '广告报表',
      },
      contextStoreHint: 'Context Package 仅支持领星店铺（store_id 需以 lingxing_ 开头）。',
      contextJobLabel: 'Context Package 导出任务',
      contextJobIdle: '当前没有进行中的导出任务。',
      contextJobQueued: '任务已入队，等待执行。',
      contextJobRunning: '任务执行中...',
      contextJobDone: '任务完成，已开始下载。',
      contextJobFailed: 'Context 导出失败。',
      contextJobProgress: '进度',
      contextJobStage: '阶段',
    },
  },
};

function getStoredAuthToken() {
  try {
    return window.localStorage.getItem(AUTH_TOKEN_KEY) || '';
  } catch (_) {
    return '';
  }
}

function getStoredRefreshToken() {
  try {
    return window.localStorage.getItem(AUTH_REFRESH_TOKEN_KEY) || '';
  } catch (_) {
    return '';
  }
}

function setStoredAuthToken(token) {
  try {
    if (token) {
      window.localStorage.setItem(AUTH_TOKEN_KEY, token);
    } else {
      window.localStorage.removeItem(AUTH_TOKEN_KEY);
    }
  } catch (_) {
    // Ignore localStorage failures.
  }
}

function setStoredRefreshToken(token) {
  try {
    if (token) {
      window.localStorage.setItem(AUTH_REFRESH_TOKEN_KEY, token);
    } else {
      window.localStorage.removeItem(AUTH_REFRESH_TOKEN_KEY);
    }
  } catch (_) {
    // Ignore localStorage failures.
  }
}

function clearStoredAuthTokens() {
  setStoredAuthToken('');
  setStoredRefreshToken('');
}

function notifyUnauthorized() {
  window.dispatchEvent(new CustomEvent(AUTH_UNAUTHORIZED_EVENT));
}

function withAuthHeaders(headers = {}, includeJson = false) {
  const merged = { ...headers };
  const token = getStoredAuthToken();
  if (token && !merged.Authorization) {
    merged.Authorization = `Bearer ${token}`;
  }
  if (includeJson && !merged['Content-Type']) {
    merged['Content-Type'] = 'application/json';
  }
  return merged;
}

function authBypass(url) {
  const text = String(url || '');
  return text.includes('/api/auth/login') || text.includes('/api/auth/register') || text.includes('/api/auth/refresh');
}

function stripAuthorization(headers = {}) {
  const normalized = { ...headers };
  delete normalized.Authorization;
  delete normalized.authorization;
  return normalized;
}

async function refreshAccessToken(requestFailedText) {
  if (refreshRequestPromise) {
    return refreshRequestPromise;
  }

  const refreshToken = getStoredRefreshToken();
  if (!refreshToken) {
    throw new Error('Missing refresh token');
  }

  refreshRequestPromise = (async () => {
    const res = await fetch('/api/auth/refresh', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });

    if (!res.ok) {
      const detail = await res.json().catch(() => ({}));
      const err = detail?.detail || `${requestFailedText}: ${res.status}`;
      clearStoredAuthTokens();
      notifyUnauthorized();
      throw new Error(err);
    }

    const data = await res.json();
    const nextAccessToken = String(data.access_token || '').trim();
    const nextRefreshToken = String(data.refresh_token || '').trim();
    if (!nextAccessToken || !nextRefreshToken) {
      clearStoredAuthTokens();
      notifyUnauthorized();
      throw new Error('Invalid refresh response');
    }

    setStoredAuthToken(nextAccessToken);
    setStoredRefreshToken(nextRefreshToken);
    return nextAccessToken;
  })().finally(() => {
    refreshRequestPromise = null;
  });

  return refreshRequestPromise;
}

async function fetchWithAuthRetry(url, options, requestFailedText) {
  const finalOptions = options ? { ...options } : {};
  finalOptions.headers = withAuthHeaders(finalOptions.headers || {});

  let res = await fetch(url, finalOptions);
  if (res.status === 401) {
    try {
      await refreshAccessToken(requestFailedText);
      const retryOptions = {
        ...finalOptions,
        headers: withAuthHeaders(stripAuthorization(finalOptions.headers || {})),
      };
      res = await fetch(url, retryOptions);
    } catch (_) {
      // refreshAccessToken already handled auth reset + event.
    }
  }

  if (res.status === 401) {
    clearStoredAuthTokens();
    notifyUnauthorized();
  }
  return res;
}

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

function normalizeStores(rawStores) {
  if (!Array.isArray(rawStores)) return [];

  const stores = rawStores
    .map((item) => {
      if (typeof item === 'string') {
        const storeId = item.trim();
        if (!storeId) return null;
        if (storeId === 'store_a' || storeId === 'store_b') return null;
        return { store_id: storeId, store_name: storeId, has_local_data: true };
      }

      if (item && typeof item === 'object') {
        const storeId = String(item.store_id || '').trim();
        if (!storeId) return null;
        if (storeId === 'store_a' || storeId === 'store_b') return null;
        const storeName = String(item.store_name || storeId).trim() || storeId;
        return { store_id: storeId, store_name: storeName, has_local_data: Boolean(item.has_local_data) };
      }

      return null;
    })
    .filter(Boolean);

  stores.sort((a, b) => a.store_name.localeCompare(b.store_name));
  return stores;
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

function downloadTextFile(filename, text) {
  const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
  downloadBlobFile(filename, blob);
}

function downloadBlobFile(filename, blob) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}

function TextOutputBlock({ title, text, emptyText, meta, expanded, onToggle, onExport, t }) {
  const hasText = Boolean(text);
  const finishReason = meta?.finish_reason || '-';
  const lineCount = meta?.line_count ?? '-';
  const charCount = meta?.char_count ?? '-';
  const bodyClass = expanded
    ? 'mt-2 whitespace-pre-wrap break-words rounded-lg bg-brand-50 p-3 text-sm leading-6 text-brand-800'
    : 'mt-2 max-h-56 overflow-hidden whitespace-pre-wrap break-words rounded-lg bg-brand-50 p-3 text-sm leading-6 text-brand-800';

  return html`
    <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
      <div className="flex items-center justify-between gap-2">
        <h4 className="text-sm font-semibold">${title}</h4>
        <div className="flex items-center gap-2">
          <button
            onClick=${onExport}
            disabled=${!hasText}
            className=${`rounded-md border px-2 py-1 text-xs font-semibold ${
              hasText
                ? 'border-brand-200 bg-white text-brand-700 hover:bg-brand-50'
                : 'border-brand-100 bg-brand-50 text-brand-300 cursor-not-allowed'
            }`}
          >
            ${t.playbook.exportTxt}
          </button>
          <button
            onClick=${onToggle}
            className="rounded-md border border-brand-200 bg-white px-2 py-1 text-xs font-semibold text-brand-700 hover:bg-brand-50"
          >
            ${expanded ? t.playbook.collapse : t.playbook.expand}
          </button>
        </div>
      </div>
      <p className="mt-2 text-xs text-brand-600">
        ${t.playbook.metaLabel}: ${t.playbook.finishReason}=${finishReason}, ${t.playbook.lines}=${lineCount}, ${t.playbook.chars}=${charCount}
      </p>
      <pre className=${bodyClass}>${hasText ? text : emptyText}</pre>
    </div>
  `;
}

async function fetchJson(url, options, requestFailedText) {
  const finalOptions = options ? { ...options } : {};
  const bypass = authBypass(url);
  const res = bypass
    ? await fetch(url, finalOptions)
    : await fetchWithAuthRetry(url, finalOptions, requestFailedText);
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    const err = detail?.detail || `${requestFailedText}: ${res.status}`;
    throw new Error(err);
  }
  return res.json();
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
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

function formatDateTime(isoValue) {
  if (!isoValue) return '-';
  const dt = new Date(isoValue);
  if (Number.isNaN(dt.getTime())) return String(isoValue);
  return dt.toLocaleString();
}

function stripHtmlText(raw) {
  if (!raw) return '';
  return String(raw)
    .replace(/<br\s*\/?\s*>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function readFileAsBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const raw = String(reader.result || '');
      const content = raw.includes(',') ? raw.split(',', 2)[1] : raw;
      resolve({
        name: file.name || 'attachment',
        content_base64: content,
        content_type: file.type || 'application/octet-stream',
        size: Number(file.size || 0),
      });
    };
    reader.onerror = () => reject(new Error('Failed to read attachment'));
    reader.readAsDataURL(file);
  });
}

function LingxingSyncTable({ rows, t, language, sortKey, sortDir, onSort }) {
  if (!rows.length) {
    return html`<p className="mt-2 rounded-lg bg-brand-50 p-3 text-sm text-brand-800">${t.playbook.syncTableEmpty}</p>`;
  }

  const keyMap = {
    ad_combo: 'ad_combo',
    campaign: 'campaign_name',
    ad_group: 'ad_group',
    current_bid: 'current_bid',
    suggested_bid: 'suggested_bid',
    clicks: 'clicks',
    spend: 'spend',
    sales: 'sales',
    acos: 'acos',
  };

  const dataKey = keyMap[sortKey] || 'campaign_name';
  const sortedRows = [...rows].sort((a, b) => {
    const av = a[dataKey];
    const bv = b[dataKey];
    if (av === bv) {
      if (a.ad_combo !== b.ad_combo) return String(a.ad_combo).localeCompare(String(b.ad_combo));
      if (a.campaign_name !== b.campaign_name) return String(a.campaign_name).localeCompare(String(b.campaign_name));
      return String(a.ad_group).localeCompare(String(b.ad_group));
    }

    const aNum = typeof av === 'number' ? av : Number(av);
    const bNum = typeof bv === 'number' ? bv : Number(bv);
    let result = 0;
    if (!Number.isNaN(aNum) && !Number.isNaN(bNum)) {
      result = aNum - bNum;
    } else {
      result = String(av ?? '').localeCompare(String(bv ?? ''));
    }
    return sortDir === 'asc' ? result : -result;
  });

  const rowSpans = new Array(sortedRows.length).fill(0);
  for (let i = 0; i < sortedRows.length; i += 1) {
    if (
      i > 0
      && sortedRows[i].ad_combo === sortedRows[i - 1].ad_combo
      && sortedRows[i].campaign_name === sortedRows[i - 1].campaign_name
    ) continue;
    let count = 1;
    while (
      i + count < sortedRows.length
      && sortedRows[i + count].ad_combo === sortedRows[i].ad_combo
      && sortedRows[i + count].campaign_name === sortedRows[i].campaign_name
    ) {
      count += 1;
    }
    rowSpans[i] = count;
  }

  const fmtBid = (value) => (value === null || value === undefined ? '-' : Number(value).toFixed(2));
  const renderSortTh = (label, key) => {
    const active = sortKey === key;
    const arrow = active ? (sortDir === 'asc' ? '↑' : '↓') : '';
    const nextLabel = active && sortDir === 'asc' ? t.playbook.sortDesc : t.playbook.sortAsc;
    return html`
      <button
        type="button"
        onClick=${() => onSort(key)}
        title=${nextLabel}
        className="inline-flex items-center gap-1 font-semibold text-brand-800 hover:text-brand-600"
      >
        <span>${label}</span>
        <span>${arrow}</span>
      </button>
    `;
  };

  return html`
    <div className="mt-2 overflow-hidden rounded-lg border border-brand-100 bg-white">
      <table className="min-w-full divide-y divide-brand-100 text-sm">
        <thead className="bg-brand-50">
          <tr>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.adCombo, 'ad_combo')}</th>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.campaign, 'campaign')}</th>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.adGroup, 'ad_group')}</th>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.currentBid, 'current_bid')}</th>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.suggestedBid, 'suggested_bid')}</th>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.clicks, 'clicks')}</th>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.spend, 'spend')}</th>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.sales, 'sales')}</th>
            <th className="px-3 py-2 text-left">${renderSortTh(t.playbook.syncTable.acos, 'acos')}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-brand-50">
          ${sortedRows.map(
            (row, index) => html`
              <tr key=${`${row.ad_combo}__${row.campaign_name}__${row.ad_group}__${index}`} className="align-top hover:bg-brand-50/70">
                <td className="px-3 py-2">${row.ad_combo || '-'}</td>
                ${rowSpans[index]
                  ? html`<td rowSpan=${rowSpans[index]} className="px-3 py-2 font-semibold text-brand-800">${row.campaign_name || row.campaign || '-'}</td>`
                  : null}
                <td className="px-3 py-2">${row.ad_group}</td>
                <td className="px-3 py-2">${fmtBid(row.current_bid)}</td>
                <td className="px-3 py-2 font-semibold text-brand-700">${fmtBid(row.suggested_bid)}</td>
                <td className="px-3 py-2">${row.clicks ?? 0}</td>
                <td className="px-3 py-2">${fmtMoney(row.spend ?? 0, language)}</td>
                <td className="px-3 py-2">${fmtMoney(row.sales ?? 0, language)}</td>
                <td className="px-3 py-2">${fmtPct(row.acos ?? 0)}</td>
              </tr>
            `,
          )}
        </tbody>
      </table>
    </div>
  `;
}

function App() {
  const [language, setLanguage] = useState(getDefaultLanguage());
  const t = useMemo(() => I18N[language], [language]);
  const [authToken, setAuthToken] = useState(getStoredAuthToken());
  const [authUser, setAuthUser] = useState(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [loginAccount, setLoginAccount] = useState('');
  const [loginPassword, setLoginPassword] = useState('');
  const [loginLoading, setLoginLoading] = useState(false);
  const [loginError, setLoginError] = useState('');

  const [stores, setStores] = useState([]);
  const [selectedStore, setSelectedStore] = useState('');
  const [view, setView] = useState('dashboard');
  const [performanceRows, setPerformanceRows] = useState([]);
  const [recommendations, setRecommendations] = useState([]);
  const [cases, setCases] = useState([]);
  const [whitepaper, setWhitepaper] = useState('');
  const [advice, setAdvice] = useState('');
  const [whitepaperMeta, setWhitepaperMeta] = useState({});
  const [adviceMeta, setAdviceMeta] = useState({});
  const [whitepaperExpanded, setWhitepaperExpanded] = useState(true);
  const [adviceExpanded, setAdviceExpanded] = useState(true);
  const [uploadFile, setUploadFile] = useState(null);
  const [whitepaperFile, setWhitepaperFile] = useState(null);
  const [uploadSummary, setUploadSummary] = useState('');
  const [whitepaperStorageSummary, setWhitepaperStorageSummary] = useState('');
  const [syncSummary, setSyncSummary] = useState('');
  const [syncRows, setSyncRows] = useState([]);
  const [syncTotals, setSyncTotals] = useState(null);
  const [syncWindow, setSyncWindow] = useState(null);
  const [syncSortKey, setSyncSortKey] = useState('campaign');
  const [syncSortDir, setSyncSortDir] = useState('asc');
  const [syncStartDate, setSyncStartDate] = useState('');
  const [syncEndDate, setSyncEndDate] = useState('');
  const [forceRefetchBeforeDate, setForceRefetchBeforeDate] = useState('');
  const [syncJobStatus, setSyncJobStatus] = useState(null);
  const [syncEmptyGuard, setSyncEmptyGuard] = useState(null);
  const [contextJobStatus, setContextJobStatus] = useState(null);
  const [adminUsers, setAdminUsers] = useState([]);
  const [adminStores, setAdminStores] = useState([]);
  const [userStoreAccessMap, setUserStoreAccessMap] = useState({});
  const [userRoleDrafts, setUserRoleDrafts] = useState({});
  const [userPasswordDrafts, setUserPasswordDrafts] = useState({});
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminNotice, setAdminNotice] = useState('');
  const [newUserUsername, setNewUserUsername] = useState('');
  const [newUserPassword, setNewUserPassword] = useState('');
  const [newUserRole, setNewUserRole] = useState('viewer');
  const [mailRows, setMailRows] = useState([]);
  const [mailLoading, setMailLoading] = useState(false);
  const [mailNotice, setMailNotice] = useState('');
  const [replyDrafts, setReplyDrafts] = useState({});
  const [selectedMessageId, setSelectedMessageId] = useState(null);
  const [mailAttachmentMap, setMailAttachmentMap] = useState({});
  const [mailDetailMap, setMailDetailMap] = useState({});
  const [mailDetailLoadingMap, setMailDetailLoadingMap] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const syncPollNonceRef = useRef(0);
  const syncPollingJobIdRef = useRef('');

  const contextJobRunning = contextJobStatus && ['queued', 'running'].includes(contextJobStatus.status);
  const syncJobRunning = syncJobStatus && ['queued', 'running'].includes(syncJobStatus.status);
  const selectedMailRow = useMemo(
    () => mailRows.find((item) => item.id === selectedMessageId) || mailRows[0] || null,
    [mailRows, selectedMessageId],
  );
  const selectedMailDetail = selectedMailRow ? (mailDetailMap[selectedMailRow.id] || null) : null;
  const selectedMailDetailLoading = selectedMailRow ? Boolean(mailDetailLoadingMap[selectedMailRow.id]) : false;
  const selectedBuyerText = stripHtmlText(
    selectedMailDetail?.text_plain
      || selectedMailDetail?.text_html
      || selectedMailRow?.buyer_message
      || '',
  );
  const selectedHistoryReply = String(selectedMailRow?.final_reply || selectedMailRow?.ai_reply || '').trim();
  const selectedDraft = selectedMailRow ? (replyDrafts[selectedMailRow.id] || '') : '';
  const selectedAttachments = selectedMailRow ? (mailAttachmentMap[selectedMailRow.id] || []) : [];

  const fileTimeTag = () => new Date().toISOString().replace(/[:.]/g, '-');

  const toDateInput = (dt) => {
    const year = dt.getFullYear();
    const month = String(dt.getMonth() + 1).padStart(2, '0');
    const day = String(dt.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  };

  const applyQuickSyncRange = (days) => {
    const end = new Date();
    end.setDate(end.getDate() - 1);
    const start = new Date(end);
    start.setDate(start.getDate() - Math.max(1, days) + 1);
    setSyncStartDate(toDateInput(start));
    setSyncEndDate(toDateInput(end));
  };

  const stopSyncPolling = () => {
    syncPollNonceRef.current += 1;
    syncPollingJobIdRef.current = '';
  };

  const applyLingxingSyncResult = async (syncRes, activeStoreId) => {
    const summary = JSON.stringify(syncRes || {}, null, 2);
    setSyncSummary(summary);
    const stores = (syncRes?.stores) || [];
    const selectedStoreResult = stores.find((item) => item.store_id === activeStoreId) || stores[0] || null;
    const tableRows = selectedStoreResult?.lingxing_output_rows || [];
    setSyncRows(tableRows);
    setSyncWindow(syncRes?.window || null);
    setSyncEmptyGuard(selectedStoreResult?.empty_data_guard || null);
    if (selectedStoreResult?.selected_range_totals) {
      setSyncTotals(selectedStoreResult.selected_range_totals);
    } else {
      const clicks = tableRows.reduce((acc, item) => acc + Number(item.clicks || 0), 0);
      const spend = tableRows.reduce((acc, item) => acc + Number(item.spend || 0), 0);
      const sales = tableRows.reduce((acc, item) => acc + Number(item.sales || 0), 0);
      setSyncTotals({
        start_date: syncRes?.window?.start_date || '',
        end_date: syncRes?.window?.end_date || '',
        days: 0,
        clicks,
        spend: Number(spend.toFixed(2)),
        sales: Number(sales.toFixed(2)),
        acos: sales > 0 ? Number(((spend / sales) * 100).toFixed(2)) : 0,
        ad_groups_with_spend: tableRows.length,
      });
    }
    setSyncSortKey('campaign');
    setSyncSortDir('asc');

    const storeRes = await fetchJson('/api/stores?include_bound=true', undefined, t.requestFailed);
    const newStores = normalizeStores(storeRes.stores || storeRes.store_ids || []);
    setStores(newStores);
    const storeIds = newStores.map((item) => item.store_id);

    if (!newStores.length) return;
    if (!storeIds.includes(activeStoreId)) {
      setSelectedStore(newStores[0].store_id);
      return;
    }

    const [perfRes, recRes, caseRes] = await Promise.all([
      fetchJson(`/api/stores/${activeStoreId}/performance`, undefined, t.requestFailed),
      fetchJson(`/api/stores/${activeStoreId}/ad-group-recommendations`, undefined, t.requestFailed),
      fetchJson(`/api/stores/${activeStoreId}/optimization-cases`, undefined, t.requestFailed),
    ]);
    setPerformanceRows(perfRes.daily_performance || []);
    setRecommendations(recRes.recommendations || []);
    setCases(caseRes.cases || []);
  };

  const pollLingxingSyncJob = async (jobId, activeStoreId) => {
    if (!jobId) return;
    if (syncPollingJobIdRef.current === jobId) return;

    const pollNonce = syncPollNonceRef.current + 1;
    syncPollNonceRef.current = pollNonce;
    syncPollingJobIdRef.current = jobId;
    let pollErrors = 0;

    try {
      while (true) {
        await sleep(2000);
        if (syncPollNonceRef.current !== pollNonce) return;

        let statusResp = null;
        try {
          statusResp = await fetchJson(`/api/lingxing/sync/jobs/${jobId}`, undefined, t.requestFailed);
          pollErrors = 0;
        } catch (err) {
          pollErrors += 1;
          if (syncPollNonceRef.current !== pollNonce) return;

          setSyncJobStatus((prev) => ({
            ...(prev || {}),
            status: 'running',
            stage: 'polling',
            message: `${t.playbook.syncJobNetworkRetry} (${pollErrors})`,
          }));

          if (pollErrors >= 60) {
            throw err;
          }
          continue;
        }

        if (syncPollNonceRef.current !== pollNonce) return;
        setSyncJobStatus(statusResp);

        if (statusResp.is_stale) {
          throw new Error(statusResp.message || t.playbook.syncJobFailed);
        }

        if (statusResp.status === 'failed') {
          throw new Error(statusResp.message || t.playbook.syncJobFailed);
        }
        if (statusResp.status !== 'succeeded') {
          continue;
        }

        await applyLingxingSyncResult(statusResp.result || {}, activeStoreId);
        setSyncJobStatus({
          ...statusResp,
          message: t.playbook.syncJobDone,
        });
        return;
      }
    } finally {
      if (syncPollNonceRef.current === pollNonce) {
        syncPollingJobIdRef.current = '';
      }
    }
  };

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
    const onUnauthorized = () => {
      stopSyncPolling();
      clearStoredAuthTokens();
      setAuthToken('');
      setAuthUser(null);
      setStores([]);
      setSelectedStore('');
      setError('');
      setLoginError('');
      setAuthLoading(false);
    };
    window.addEventListener(AUTH_UNAUTHORIZED_EVENT, onUnauthorized);
    return () => window.removeEventListener(AUTH_UNAUTHORIZED_EVENT, onUnauthorized);
  }, []);

  useEffect(() => {
    if (!authToken) {
      setAuthUser(null);
      setAuthLoading(false);
      return;
    }

    let cancelled = false;
    setAuthLoading(true);
    fetchJson('/api/auth/me', undefined, t.requestFailed)
      .then((me) => {
        if (cancelled) return;
        setAuthUser(me);
        setLoginError('');
      })
      .catch((err) => {
        if (cancelled) return;
        setAuthToken('');
        setAuthUser(null);
        setLoginError(err.message || t.auth.invalid);
      })
      .finally(() => {
        if (cancelled) return;
        setAuthLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [authToken, t.requestFailed, t.auth.invalid]);

  const onLogin = async (event) => {
    event.preventDefault();
    const account = (loginAccount || '').trim();
    if (!account || !loginPassword) {
      setLoginError(t.auth.invalid);
      return;
    }

    setLoginLoading(true);
    setLoginError('');
    try {
      const data = await fetchJson(
        '/api/auth/login',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            account,
            password: loginPassword,
          }),
        },
        t.requestFailed,
      );
      const token = data.access_token || '';
      const refreshToken = data.refresh_token || '';
      setStoredAuthToken(token);
      setStoredRefreshToken(refreshToken);
      setAuthToken(token);
      setLoginPassword('');
    } catch (err) {
      clearStoredAuthTokens();
      setAuthToken('');
      setLoginError(err.message || t.auth.invalid);
    } finally {
      setLoginLoading(false);
    }
  };

  const onLogout = () => {
    stopSyncPolling();
    clearStoredAuthTokens();
    setAuthToken('');
    setAuthUser(null);
    setStores([]);
    setSelectedStore('');
  };

  const loadUserManagementData = async () => {
    if (!authUser || authUser.role !== 'admin') return;
    setAdminLoading(true);
    setAdminNotice('');
    setError('');
    try {
      const [usersResp, storesResp] = await Promise.all([
        fetchJson('/api/auth/users', undefined, t.requestFailed),
        fetchJson('/api/stores?include_bound=true', undefined, t.requestFailed),
      ]);

      const users = usersResp.items || [];
      const storesList = normalizeStores(storesResp.stores || storesResp.store_ids || []);
      const storeAccessEntries = await Promise.all(
        users.map(async (user) => {
          const accessResp = await fetchJson(
            `/api/auth/users/${user.user_id}/stores`,
            undefined,
            t.requestFailed,
          );
          const accessMap = {};
          (accessResp.stores || []).forEach((s) => {
            accessMap[s.external_store_id] = true;
          });
          return [user.user_id, accessMap];
        }),
      );

      setAdminUsers(users);
      setAdminStores(storesList);
      setUserStoreAccessMap(Object.fromEntries(storeAccessEntries));
      setUserRoleDrafts(
        Object.fromEntries(users.map((u) => [u.user_id, u.role || 'viewer'])),
      );
      setUserPasswordDrafts({});
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const onCreateManagedUser = async () => {
    if (!newUserUsername.trim() || !newUserPassword) {
      setError(t.requestFailed);
      return;
    }
    setAdminLoading(true);
    setAdminNotice('');
    setError('');
    try {
      await fetchJson(
        '/api/auth/users',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            username: newUserUsername.trim(),
            password: newUserPassword,
            role: newUserRole,
          }),
        },
        t.requestFailed,
      );
      setNewUserUsername('');
      setNewUserPassword('');
      setNewUserRole('viewer');
      setAdminNotice(t.userMgmt.updated);
      await loadUserManagementData();
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const onSaveManagedUserRole = async (userId) => {
    const role = userRoleDrafts[userId] || 'viewer';
    setAdminLoading(true);
    setAdminNotice('');
    setError('');
    try {
      await fetchJson(
        `/api/auth/users/${userId}/role`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ role }),
        },
        t.requestFailed,
      );
      setAdminNotice(t.userMgmt.updated);
      await loadUserManagementData();
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const onResetManagedUserPassword = async (userId) => {
    const newPassword = userPasswordDrafts[userId] || '';
    if (!newPassword) return;
    setAdminLoading(true);
    setAdminNotice('');
    setError('');
    try {
      await fetchJson(
        `/api/auth/users/${userId}/password`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ new_password: newPassword }),
        },
        t.requestFailed,
      );
      setUserPasswordDrafts((prev) => ({ ...prev, [userId]: '' }));
      setAdminNotice(t.userMgmt.updated);
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const onToggleManagedUserStatus = async (user) => {
    const nextStatus = user.status === 'active' ? 'inactive' : 'active';
    setAdminLoading(true);
    setAdminNotice('');
    setError('');
    try {
      await fetchJson(
        `/api/auth/users/${user.user_id}/status`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ status: nextStatus }),
        },
        t.requestFailed,
      );
      setAdminNotice(t.userMgmt.updated);
      await loadUserManagementData();
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const onToggleManagedStoreAccess = async (userId, store) => {
    const hasAccess = Boolean(userStoreAccessMap?.[userId]?.[store.store_id]);
    setAdminLoading(true);
    setAdminNotice('');
    setError('');
    try {
      if (hasAccess) {
        await fetchJson(
          `/api/auth/users/${userId}/stores/${encodeURIComponent(store.store_id)}`,
          { method: 'DELETE' },
          t.requestFailed,
        );
      } else {
        await fetchJson(
          `/api/auth/users/${userId}/stores`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              external_store_id: store.store_id,
              store_name: store.store_name,
            }),
          },
          t.requestFailed,
        );
      }
      setAdminNotice(t.userMgmt.updated);
      await loadUserManagementData();
    } catch (err) {
      setError(err.message);
    } finally {
      setAdminLoading(false);
    }
  };

  const loadCustomerMessages = async () => {
    if (!selectedStore) return;
    setMailLoading(true);
    setMailNotice('');
    setError('');
    try {
      const data = await fetchJson(
        `/api/customer-service/stores/${encodeURIComponent(selectedStore)}/messages?limit=200`,
        undefined,
        t.requestFailed,
      );
      const items = data.items || [];
      setMailRows(items);
      setReplyDrafts(
        Object.fromEntries(
          items.map((item) => [item.id, item.final_reply || item.ai_reply || '']),
        ),
      );
      setSelectedMessageId((prev) => {
        if (items.some((item) => item.id === prev)) return prev;
        return items.length ? items[0].id : null;
      });
      setMailDetailMap({});
      setMailDetailLoadingMap({});
      setMailAttachmentMap({});
      setMailNotice(`${t.autoReply.loadedCount}: ${items.length}`);
    } catch (err) {
      setError(err.message);
    } finally {
      setMailLoading(false);
    }
  };

  const onFetchCustomerMessages = async () => {
    if (!selectedStore) return;
    setMailLoading(true);
    setMailNotice('');
    setError('');
    try {
      const result = await fetchJson(
        `/api/customer-service/stores/${encodeURIComponent(selectedStore)}/messages/fetch`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            auto_process: true,
            async_mode: false,
          }),
        },
        t.requestFailed,
      );
      setMailNotice(
        `${t.autoReply.fetchResult}: fetched=${result.fetched_count || 0}, created=${result.created_count || 0}, processed=${result.processed_count || 0}`,
      );
      await loadCustomerMessages();
    } catch (err) {
      setError(err.message);
      setMailLoading(false);
    }
  };

  const onProcessMessage = async (messageId) => {
    if (!selectedStore) return;
    setMailLoading(true);
    setMailNotice('');
    setError('');
    try {
      await fetchJson(
        `/api/customer-service/stores/${encodeURIComponent(selectedStore)}/messages/${messageId}/process`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            async_mode: false,
            force_regenerate: true,
            allow_auto_send: false,
          }),
        },
        t.requestFailed,
      );
      await loadCustomerMessages();
    } catch (err) {
      setError(err.message);
      setMailLoading(false);
    }
  };

  const onSaveReply = async (messageId) => {
    if (!selectedStore) return;
    const finalReply = String(replyDrafts[messageId] || '').trim();
    if (!finalReply) return;
    setMailLoading(true);
    setMailNotice('');
    setError('');
    try {
      await fetchJson(
        `/api/customer-service/stores/${encodeURIComponent(selectedStore)}/messages/${messageId}/reply`,
        {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ final_reply: finalReply }),
        },
        t.requestFailed,
      );
      await loadCustomerMessages();
    } catch (err) {
      setError(err.message);
      setMailLoading(false);
    }
  };

  const onSendMessage = async (messageId) => {
    if (!selectedStore) return;
    const finalReply = String(replyDrafts[messageId] || '').trim();
    if (!finalReply) {
      setError(t.requestFailed);
      return;
    }
    setMailLoading(true);
    setMailNotice('');
    setError('');
    try {
      await fetchJson(
        `/api/customer-service/stores/${encodeURIComponent(selectedStore)}/messages/${messageId}/reply`,
        {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ final_reply: finalReply }),
        },
        t.requestFailed,
      );

      await fetchJson(
        `/api/customer-service/stores/${encodeURIComponent(selectedStore)}/messages/${messageId}/approve-send`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            async_mode: false,
            attachments: mailAttachmentMap[messageId] || [],
          }),
        },
        t.requestFailed,
      );
      setMailAttachmentMap((prev) => ({ ...prev, [messageId]: [] }));
      await loadCustomerMessages();
    } catch (err) {
      setError(err.message);
      setMailLoading(false);
    }
  };

  const ensureMessageDetail = async (row) => {
    if (!selectedStore || !row) return null;
    if (mailDetailMap[row.id]) return mailDetailMap[row.id];
    setMailDetailLoadingMap((prev) => ({ ...prev, [row.id]: true }));
    setError('');
    try {
      const detail = await fetchJson(
        `/api/customer-service/stores/${encodeURIComponent(selectedStore)}/messages/${row.id}/detail`,
        undefined,
        t.requestFailed,
      );
      setMailDetailMap((prev) => ({ ...prev, [row.id]: detail }));
      return detail;
    } catch (err) {
      setError(err.message);
      return null;
    } finally {
      setMailDetailLoadingMap((prev) => ({ ...prev, [row.id]: false }));
    }
  };

  const onSelectMessage = (row) => {
    if (!row) return;
    setSelectedMessageId(row.id);
    ensureMessageDetail(row);
  };

  const onUploadAttachments = async (messageId, files) => {
    if (!messageId || !files?.length) return;
    try {
      const prepared = [];
      for (const file of Array.from(files)) {
        prepared.push(await readFileAsBase64(file));
      }
      setMailAttachmentMap((prev) => ({
        ...prev,
        [messageId]: [...(prev[messageId] || []), ...prepared],
      }));
    } catch (err) {
      setError(err.message || t.requestFailed);
    }
  };

  const onRemoveAttachment = (messageId, index) => {
    setMailAttachmentMap((prev) => ({
      ...prev,
      [messageId]: (prev[messageId] || []).filter((_, i) => i !== index),
    }));
  };

  useEffect(() => {
    if (!authUser) return;
    fetchJson('/api/stores?include_bound=true', undefined, I18N.en.requestFailed)
      .then((data) => {
        const options = normalizeStores(data.stores || data.store_ids || []);
        setStores(options);
        if (options.length) {
          const firstLocal = options.find((item) => item.has_local_data);
          setSelectedStore((firstLocal || options[0]).store_id);
        }
      })
      .catch((err) => setError(err.message));
  }, [authUser]);

  useEffect(() => {
    if (!selectedStore) return;
    stopSyncPolling();
    setContextJobStatus(null);
    setSyncJobStatus(null);
    setSyncSummary('');
    setSyncRows([]);
    setSyncTotals(null);
    setSyncWindow(null);
    setSyncEmptyGuard(null);
    setForceRefetchBeforeDate('');
    setSelectedMessageId(null);
    setMailAttachmentMap({});
  }, [selectedStore]);

  useEffect(() => {
    if (!authUser || !selectedStore) return;
    let disposed = false;

    fetchJson(
      `/api/lingxing/sync/jobs/latest/by-store?store_id=${encodeURIComponent(selectedStore)}`,
      undefined,
      t.requestFailed,
    )
      .then(async (payload) => {
        if (disposed) return;
        const latestJob = payload?.job;
        if (!latestJob) return;

        setSyncJobStatus(latestJob);
        if (latestJob.status === 'succeeded' && latestJob.result) {
          await applyLingxingSyncResult(latestJob.result, selectedStore);
          if (!disposed) {
            setSyncJobStatus({
              ...latestJob,
              message: t.playbook.syncJobDone,
            });
          }
          return;
        }

        if (latestJob.status === 'queued' || latestJob.status === 'running') {
          await pollLingxingSyncJob(latestJob.job_id, selectedStore);
        }
      })
      .catch(() => {
        // Ignore missing latest job or transient errors.
      });

    return () => {
      disposed = true;
    };
  }, [authUser, selectedStore, t.requestFailed, t.playbook.syncJobDone]);

  useEffect(() => {
    if (!authUser || !selectedStore) return;
    if (!ADS_NAV_KEYS.includes(view)) return;
    const selectedOption = stores.find((item) => item.store_id === selectedStore);
    if (selectedOption && !selectedOption.has_local_data) {
      setLoading(true);
      setError('');
      setPerformanceRows([]);
      setRecommendations([]);
      setCases([]);

      fetchJson(`/api/stores/${selectedStore}/whitepaper`, undefined, t.requestFailed)
        .then((whitepaperRes) => {
          setWhitepaper(whitepaperRes.content || '');
          setWhitepaperMeta(
            whitepaperRes.exists
              ? {
                  finish_reason: 'STORED',
                  finish_reasons: ['STORED'],
                  char_count: whitepaperRes.char_count || 0,
                  line_count: whitepaperRes.line_count || 0,
                }
              : {},
          );
          setWhitepaperStorageSummary(
            whitepaperRes.exists
              ? `${t.playbook.lines}: ${whitepaperRes.line_count || 0}, ${t.playbook.chars}: ${whitepaperRes.char_count || 0}`
              : t.playbook.storeNotSyncedHint,
          );
        })
        .catch(() => {
          setWhitepaper('');
          setWhitepaperMeta({});
          setWhitepaperStorageSummary(t.playbook.storeNotSyncedHint);
        })
        .finally(() => setLoading(false));
      return;
    }

    setLoading(true);
    setError('');

    Promise.all([
      fetchJson(`/api/stores/${selectedStore}/performance`, undefined, t.requestFailed),
      fetchJson(`/api/stores/${selectedStore}/ad-group-recommendations`, undefined, t.requestFailed),
      fetchJson(`/api/stores/${selectedStore}/optimization-cases`, undefined, t.requestFailed),
      fetchJson(`/api/stores/${selectedStore}/whitepaper`, undefined, t.requestFailed),
    ])
      .then(([perfRes, recRes, caseRes, whitepaperRes]) => {
        setPerformanceRows(perfRes.daily_performance || []);
        setRecommendations(recRes.recommendations || []);
        setCases(caseRes.cases || []);
        setWhitepaper(whitepaperRes.content || '');
        setWhitepaperMeta(
          whitepaperRes.exists
            ? {
                finish_reason: 'STORED',
                finish_reasons: ['STORED'],
                char_count: whitepaperRes.char_count || 0,
                line_count: whitepaperRes.line_count || 0,
              }
            : {},
        );
        setWhitepaperStorageSummary(
          whitepaperRes.exists
            ? `${t.playbook.lines}: ${whitepaperRes.line_count || 0}, ${t.playbook.chars}: ${whitepaperRes.char_count || 0}`
            : t.playbook.whitepaperStorageEmpty,
        );
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [
    authUser,
    selectedStore,
    view,
    stores,
    t.requestFailed,
    t.playbook.lines,
    t.playbook.chars,
    t.playbook.whitepaperStorageEmpty,
    t.playbook.storeNotSyncedHint,
  ]);

  useEffect(() => {
    if (ADS_NAV_KEYS.includes(view)) return;
    setLoading(false);
  }, [view]);

  useEffect(() => {
    if (!authUser || !selectedStore || view !== 'autoReplyMail') return;
    loadCustomerMessages();
  }, [authUser, selectedStore, view]);

  useEffect(() => {
    if (!selectedMailRow || view !== 'autoReplyMail') return;
    ensureMessageDetail(selectedMailRow);
  }, [selectedMailRow?.id, view]);

  useEffect(() => {
    if (!authUser || authUser.role !== 'admin' || view !== 'userManagement') return;
    loadUserManagementData();
  }, [authUser, view]);

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
      setWhitepaperMeta(data.whitepaper_meta || {});
      const wc = data.whitepaper_meta?.char_count ?? 0;
      const wl = data.whitepaper_meta?.line_count ?? 0;
      setWhitepaperStorageSummary(`${t.playbook.lines}: ${wl}, ${t.playbook.chars}: ${wc}`);
      setWhitepaperExpanded(true);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onSyncLingxing = async () => {
    if (!selectedStore) return;
    if (syncJobRunning) return;
    if ((syncStartDate && !syncEndDate) || (!syncStartDate && syncEndDate)) {
      setError(t.playbook.syncDateInvalid);
      return;
    }
    if (syncStartDate && syncEndDate && syncStartDate > syncEndDate) {
      setError(t.playbook.syncDateInvalid);
      return;
    }

    setLoading(true);
    setError('');
    setSyncRows([]);
    setSyncSummary('');
    setSyncEmptyGuard(null);
    setSyncJobStatus({
      status: 'queued',
      progress_pct: 0,
      stage: 'queued',
      message: t.playbook.syncJobQueued,
    });
    try {
      const requestBody = { store_id: selectedStore };
      if (syncStartDate && syncEndDate) {
        requestBody.start_date = syncStartDate;
        requestBody.end_date = syncEndDate;
      }
      if (forceRefetchBeforeDate) {
        requestBody.force_refetch_before_date = forceRefetchBeforeDate;
      }

      const createResp = await fetchJson(
        '/api/lingxing/sync/jobs',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody),
        },
        t.requestFailed,
      );
      const jobId = createResp.job_id;
      setSyncJobStatus({
        job_id: jobId,
        status: 'queued',
        progress_pct: 0,
        stage: 'queued',
        message: t.playbook.syncJobQueued,
      });
      await pollLingxingSyncJob(jobId, selectedStore);
    } catch (err) {
      const message = err?.message || t.playbook.syncJobFailed;
      const looksLikeNetworkIssue = String(message).toLowerCase().includes('failed to fetch');
      setError(message);
      if (looksLikeNetworkIssue) {
        setSyncJobStatus((prev) => ({
          ...(prev || {}),
          status: 'running',
          stage: 'polling',
          message: t.playbook.syncJobNetworkRetry,
        }));
      } else {
        setSyncJobStatus((prev) => ({
          ...(prev || {}),
          status: 'failed',
          message,
        }));
      }
    } finally {
      setLoading(false);
    }
  };

  const onAnalyzeUploadFile = async () => {
    if (!uploadFile) {
      setError(t.playbook.uploadRequired);
      return;
    }

    setLoading(true);
    setError('');
    try {
      const formData = new FormData();
      formData.append('file', uploadFile);
      formData.append('store_id', selectedStore || 'uploaded_store');
      formData.append('lang', language);
      formData.append('run_gemini', 'true');

      const response = await fetchWithAuthRetry('/api/ai/upload-analysis', {
        method: 'POST',
        headers: withAuthHeaders(),
        body: formData,
      }, t.requestFailed);

      let payload = {};
      try {
        payload = await response.json();
      } catch (_) {
        payload = {};
      }

      if (!response.ok) {
        const detail = payload?.detail || `${t.requestFailed}: ${response.status}`;
        throw new Error(detail);
      }

      const summaryText = `${payload.summary?.performance_sheet || '-'} | ${payload.summary?.history_sheet || '-'} | ${payload.summary?.date_range?.start || '-'} ~ ${payload.summary?.date_range?.end || '-'}`;
      setUploadSummary(summaryText);
      setWhitepaper(payload.whitepaper || t.playbook.whitepaperEmpty);
      setAdvice(payload.advice || t.playbook.adviceEmpty);
      setWhitepaperMeta(payload.whitepaper_meta || {});
      setAdviceMeta(payload.advice_meta || {});
      const wc = payload.whitepaper_meta?.char_count ?? 0;
      const wl = payload.whitepaper_meta?.line_count ?? 0;
      setWhitepaperStorageSummary(`${t.playbook.lines}: ${wl}, ${t.playbook.chars}: ${wc}`);
      setWhitepaperExpanded(true);
      setAdviceExpanded(true);
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
      setAdviceMeta(data.advice_meta || {});
      const whitepaperRes = await fetchJson(`/api/stores/${selectedStore}/whitepaper`, undefined, t.requestFailed);
      setWhitepaper(whitepaperRes.content || '');
      setWhitepaperMeta(
        whitepaperRes.exists
          ? {
              finish_reason: 'STORED',
              finish_reasons: ['STORED'],
              char_count: whitepaperRes.char_count || 0,
              line_count: whitepaperRes.line_count || 0,
            }
          : {},
      );
      setWhitepaperStorageSummary(
        whitepaperRes.exists
          ? `${t.playbook.lines}: ${whitepaperRes.line_count || 0}, ${t.playbook.chars}: ${whitepaperRes.char_count || 0}`
          : t.playbook.whitepaperStorageEmpty,
      );
      setAdviceExpanded(true);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onExportWhitepaper = () => {
    const content = whitepaper || '';
    if (!content) return;
    const filename = `${selectedStore || 'store'}_whitepaper_${language}_${fileTimeTag()}.txt`;
    downloadTextFile(filename, content);
  };

  const onExportAdvice = () => {
    const content = advice || '';
    if (!content) return;
    const filename = `${selectedStore || 'store'}_advice_${language}_${fileTimeTag()}.txt`;
    downloadTextFile(filename, content);
  };

  const onDownloadContextPackage = async () => {
    if (!selectedStore) return;
    if (contextJobRunning) return;
    setError('');
    setContextJobStatus({
      status: 'queued',
      progress_pct: 0,
      stage: 'queued',
      message: t.playbook.contextJobQueued,
    });
    try {
      const createResp = await fetchJson(
        '/api/lingxing/context-package/jobs',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            store_id: selectedStore,
            days: 365,
          }),
        },
        t.requestFailed,
      );

      const jobId = createResp.job_id;
      setContextJobStatus({
        job_id: jobId,
        status: 'queued',
        progress_pct: 0,
        stage: 'queued',
        message: t.playbook.contextJobQueued,
      });

      while (true) {
        await sleep(2000);
        const statusResp = await fetchJson(
          `/api/lingxing/context-package/jobs/${jobId}`,
          undefined,
          t.requestFailed,
        );
        setContextJobStatus(statusResp);

        if (statusResp.status === 'failed') {
          throw new Error(statusResp.message || t.playbook.contextJobFailed);
        }

        if (statusResp.status !== 'succeeded') {
          continue;
        }

        const downloadResp = await fetchWithAuthRetry(
          `/api/lingxing/context-package/jobs/${jobId}/download`,
          undefined,
          t.requestFailed,
        );
        if (!downloadResp.ok) {
          const detail = await downloadResp.json().catch(() => ({}));
          throw new Error(detail?.detail || `${t.requestFailed}: ${downloadResp.status}`);
        }

        const blob = await downloadResp.blob();
        const disposition = downloadResp.headers.get('content-disposition') || '';
        const matched = disposition.match(/filename="?([^"]+)"?/i);
        const filename = matched?.[1] || `${selectedStore}_context_package.json`;
        downloadBlobFile(filename, blob);
        setContextJobStatus({
          ...statusResp,
          message: t.playbook.contextJobDone,
        });
        break;
      }
    } catch (err) {
      const message = err?.message || t.playbook.contextJobFailed;
      setError(message);
      setContextJobStatus((prev) => ({
        ...(prev || {}),
        status: 'failed',
        message,
      }));
    }
  };

  const onImportWhitepaper = async () => {
    if (!selectedStore) return;
    if (!whitepaperFile) {
      setError(t.playbook.whitepaperImportRequired);
      return;
    }

    setLoading(true);
    setError('');
    try {
      const formData = new FormData();
      formData.append('file', whitepaperFile);
      const response = await fetchWithAuthRetry(`/api/stores/${selectedStore}/whitepaper/import`, {
        method: 'POST',
        headers: withAuthHeaders(),
        body: formData,
      }, t.requestFailed);

      let payload = {};
      try {
        payload = await response.json();
      } catch (_) {
        payload = {};
      }

      if (!response.ok) {
        const detail = payload?.detail || `${t.requestFailed}: ${response.status}`;
        throw new Error(detail);
      }

      setWhitepaper(payload.content || '');
      setWhitepaperMeta(
        payload.exists
          ? {
              finish_reason: 'STORED',
              finish_reasons: ['STORED'],
              char_count: payload.char_count || 0,
              line_count: payload.line_count || 0,
            }
          : {},
      );
      setWhitepaperStorageSummary(
        payload.exists
          ? `${t.playbook.lines}: ${payload.line_count || 0}, ${t.playbook.chars}: ${payload.char_count || 0}`
          : t.playbook.whitepaperStorageEmpty,
      );
      setWhitepaperExpanded(true);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const onExportStoredWhitepaper = async () => {
    if (!selectedStore) return;
    setLoading(true);
    setError('');
    try {
      const response = await fetchWithAuthRetry(`/api/stores/${selectedStore}/whitepaper/export`, {
        headers: withAuthHeaders(),
      }, t.requestFailed);
      if (!response.ok) {
        const detail = await response.json().catch(() => ({}));
        throw new Error(detail?.detail || `${t.requestFailed}: ${response.status}`);
      }

      const blob = await response.blob();
      const disposition = response.headers.get('content-disposition') || '';
      const matched = disposition.match(/filename="?([^"]+)"?/i);
      const filename = matched?.[1] || `${selectedStore}_whitepaper.txt`;
      downloadBlobFile(filename, blob);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  if (authLoading) {
    return html`
      <div className="flex min-h-screen items-center justify-center bg-brand-50 p-6">
        <div className="rounded-xl border border-brand-100 bg-white px-6 py-5 text-sm text-brand-700 shadow-sm">${t.loading}</div>
      </div>
    `;
  }

  if (!authUser) {
    return html`
      <div className="flex min-h-screen items-center justify-center bg-brand-50 p-4">
        <form className="w-full max-w-md rounded-2xl border border-brand-100 bg-white p-6 shadow-lg" onSubmit=${onLogin} autoComplete="on">
          <div className="mb-4 flex items-center justify-between">
            <h1 className="text-xl font-semibold text-brand-900">${t.auth.loginTitle}</h1>
            <select
              className="rounded-md border border-brand-200 px-2 py-1 text-sm"
              value=${language}
              onChange=${(e) => setLanguage(e.target.value)}
            >
              <option value="zh">中文</option>
              <option value="en">English</option>
            </select>
          </div>
          <p className="mb-4 text-sm text-brand-600">${t.auth.loginSubtitle}</p>
          <label className="mb-3 block">
            <span className="mb-1 block text-xs font-medium text-brand-700">${t.auth.accountLabel}</span>
            <input
              type="text"
              name="login_account"
              autoComplete="username"
              value=${loginAccount}
              onChange=${(e) => setLoginAccount(e.target.value)}
              className="block w-full rounded-md border border-brand-200 bg-white px-3 py-2 text-sm"
              placeholder=${t.auth.accountPlaceholder}
            />
          </label>
          <label className="mb-2 block">
            <span className="mb-1 block text-xs font-medium text-brand-700">${t.auth.passwordLabel}</span>
            <input
              type="password"
              name="login_password"
              autoComplete="current-password"
              value=${loginPassword}
              onChange=${(e) => setLoginPassword(e.target.value)}
              className="block w-full rounded-md border border-brand-200 bg-white px-3 py-2 text-sm"
              placeholder="********"
            />
          </label>
          ${loginError
            ? html`<p className="mt-2 rounded-md border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-700">${loginError}</p>`
            : null}
          <button
            type="submit"
            disabled=${Boolean(loginLoading)}
            className=${`mt-4 w-full rounded-md px-4 py-2 text-sm font-semibold ${
              loginLoading ? 'cursor-not-allowed bg-brand-200 text-white' : 'bg-brand-700 text-white hover:bg-brand-800'
            }`}
          >
            ${loginLoading ? t.auth.loggingIn : t.auth.loginBtn}
          </button>
        </form>
      </div>
    `;
  }

  return html`
    <div className="min-h-screen overflow-x-hidden md:flex">
      <aside className="w-full shrink-0 border-b border-brand-100 bg-brand-900 text-brand-50 md:w-64 md:flex-none md:border-b-0 md:border-r">
        <div className="px-5 py-6">
          <h1 className="text-lg font-semibold">${t.appTitle}</h1>
          <p className="mt-1 text-xs text-brand-200">${t.appSubtitle}</p>
        </div>
        <nav className="px-3 pb-6">
          <p className="mb-2 px-2 text-xs font-semibold uppercase tracking-wide text-brand-300">${t.navGroup.ads}</p>
          ${ADS_NAV_KEYS.map(
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

          <p className="mb-2 mt-4 px-2 text-xs font-semibold uppercase tracking-wide text-brand-300">${t.navGroup.autoReply}</p>
          ${AUTO_REPLY_NAV_KEYS.map(
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

          <p className="mb-2 mt-4 px-2 text-xs font-semibold uppercase tracking-wide text-brand-300">${t.navGroup.settings}</p>
          ${SETTINGS_NAV_KEYS.map(
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

      <main className="min-w-0 flex-1 p-4 md:p-8">
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
              ${stores.map(
                (store) =>
                  html`<option key=${store.store_id} value=${store.store_id}>${store.store_name}</option>`,
              )}
            </select>
            <span className="ml-2 rounded-md bg-brand-50 px-2 py-1 text-xs font-semibold text-brand-700">${authUser.username}</span>
            <button
              onClick=${onLogout}
              className="rounded-md border border-brand-300 bg-white px-3 py-2 text-sm font-semibold text-brand-700 hover:bg-brand-50"
            >
              ${t.logout}
            </button>
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
                  <div className="mt-4 rounded-lg border border-brand-100 bg-brand-50 p-3">
                    <label className="block text-sm font-semibold text-brand-800">${t.playbook.syncDateRangeLabel}</label>
                    <p className="mt-1 text-xs text-brand-600">
                      ${t.playbook.syncDateHint}
                      <br />
                      ${t.playbook.syncCurrentStoreOnly}
                    </p>
                    <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-2">
                      <label className="block">
                        <span className="mb-1 block text-xs font-medium text-brand-700">${t.playbook.syncStartDate}</span>
                        <input
                          type="date"
                          value=${syncStartDate}
                          onChange=${(e) => setSyncStartDate(e.target.value)}
                          className="block w-full rounded-md border border-brand-200 bg-white px-3 py-2 text-sm"
                        />
                      </label>
                      <label className="block">
                        <span className="mb-1 block text-xs font-medium text-brand-700">${t.playbook.syncEndDate}</span>
                        <input
                          type="date"
                          value=${syncEndDate}
                          onChange=${(e) => setSyncEndDate(e.target.value)}
                          className="block w-full rounded-md border border-brand-200 bg-white px-3 py-2 text-sm"
                        />
                      </label>
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2">
                      <button
                        type="button"
                        onClick=${() => applyQuickSyncRange(30)}
                        className="rounded-md border border-brand-300 bg-white px-3 py-1.5 text-xs font-semibold text-brand-700 hover:bg-brand-50"
                      >
                        ${t.playbook.syncQuick30}
                      </button>
                      <button
                        type="button"
                        onClick=${() => applyQuickSyncRange(60)}
                        className="rounded-md border border-brand-300 bg-white px-3 py-1.5 text-xs font-semibold text-brand-700 hover:bg-brand-50"
                      >
                        ${t.playbook.syncQuick60}
                      </button>
                      <button
                        type="button"
                        onClick=${() => {
                          setSyncStartDate('');
                          setSyncEndDate('');
                        }}
                        className="rounded-md border border-brand-300 bg-white px-3 py-1.5 text-xs font-semibold text-brand-700 hover:bg-brand-50"
                      >
                        ${t.playbook.syncQuickClear}
                      </button>
                    </div>
                    <label className="mt-3 block">
                      <span className="mb-1 block text-xs font-medium text-brand-700">${t.playbook.forceRefetchBeforeDate}</span>
                      <input
                        type="date"
                        value=${forceRefetchBeforeDate}
                        onChange=${(e) => setForceRefetchBeforeDate(e.target.value)}
                        className="block w-full rounded-md border border-brand-200 bg-white px-3 py-2 text-sm"
                      />
                      <span className="mt-1 block text-xs text-brand-600">${t.playbook.forceRefetchBeforeDateHint}</span>
                    </label>
                  </div>
                  <div className="mt-4 rounded-lg border border-brand-100 bg-brand-50 p-3">
                    <label className="block text-sm font-semibold text-brand-800">${t.playbook.uploadLabel}</label>
                    <p className="mt-1 text-xs text-brand-600">${t.playbook.uploadHint}</p>
                    <input
                      type="file"
                      accept=".xlsx,.xls"
                      className="mt-2 block w-full rounded-md border border-brand-200 bg-white px-3 py-2 text-sm"
                      onChange=${(e) => setUploadFile(e.target.files?.[0] || null)}
                    />
                  </div>
                  <div className="mt-4 rounded-lg border border-brand-100 bg-brand-50 p-3">
                    <label className="block text-sm font-semibold text-brand-800">${t.playbook.whitepaperImportLabel}</label>
                    <p className="mt-1 text-xs text-brand-600">${t.playbook.whitepaperImportHint}</p>
                    <input
                      type="file"
                      accept=".txt,.md,text/plain,text/markdown"
                      className="mt-2 block w-full rounded-md border border-brand-200 bg-white px-3 py-2 text-sm"
                      onChange=${(e) => setWhitepaperFile(e.target.files?.[0] || null)}
                    />
                  </div>
                  <div className="mt-4 flex flex-wrap gap-2">
                    <button
                      onClick=${onSyncLingxing}
                      disabled=${Boolean(syncJobRunning)}
                      className=${`rounded-md border px-4 py-2 text-sm font-semibold ${
                        syncJobRunning
                          ? 'cursor-not-allowed border-brand-100 bg-brand-50 text-brand-300'
                          : 'border-brand-300 bg-white text-brand-700 hover:bg-brand-50'
                      }`}
                    >
                      ${t.playbook.syncBtn}
                    </button>
                    <button
                      onClick=${onDownloadContextPackage}
                      disabled=${Boolean(contextJobRunning)}
                      className=${`rounded-md border px-4 py-2 text-sm font-semibold ${
                        contextJobRunning
                          ? 'cursor-not-allowed border-brand-100 bg-brand-50 text-brand-300'
                          : 'border-brand-300 bg-white text-brand-700 hover:bg-brand-50'
                      }`}
                    >
                      ${t.playbook.contextExportBtn}
                    </button>
                    <button
                      onClick=${onAnalyzeUploadFile}
                      className="rounded-md border border-brand-300 bg-white px-4 py-2 text-sm font-semibold text-brand-700 hover:bg-brand-50"
                    >
                      ${t.playbook.uploadBtn}
                    </button>
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
                    <button
                      onClick=${onImportWhitepaper}
                      className="rounded-md border border-brand-300 bg-white px-4 py-2 text-sm font-semibold text-brand-700 hover:bg-brand-50"
                    >
                      ${t.playbook.whitepaperImportBtn}
                    </button>
                    <button
                      onClick=${onExportStoredWhitepaper}
                      className="rounded-md border border-brand-300 bg-white px-4 py-2 text-sm font-semibold text-brand-700 hover:bg-brand-50"
                    >
                      ${t.playbook.whitepaperExportStoredBtn}
                    </button>
                  </div>
                </div>

                <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                  <h4 className="text-sm font-semibold">${t.playbook.syncSummaryLabel}</h4>
                  <p className="mt-2 rounded-lg bg-brand-50 p-3 text-sm text-brand-800">
                    ${syncJobStatus
                      ? `${t.playbook.syncJobProgress}: ${syncJobStatus.progress_pct ?? 0}% | ${t.playbook.syncJobStage}: ${syncJobStatus.stage || '-'} | ${t.playbook.syncJobUpdatedAt}: ${
                          syncJobStatus.updated_at ? new Date(syncJobStatus.updated_at).toLocaleString() : '-'
                        } | ${
                          syncJobStatus.status === 'failed'
                            ? (syncJobStatus.message || t.playbook.syncJobFailed)
                            : syncJobStatus.status === 'succeeded'
                              ? (syncJobStatus.message || t.playbook.syncJobDone)
                              : syncJobStatus.status === 'running'
                                ? (syncJobStatus.message || t.playbook.syncJobRunning)
                                : (syncJobStatus.message || t.playbook.syncJobQueued)
                        }`
                      : t.playbook.syncJobIdle}
                  </p>
                  ${syncTotals
                    ? html`
                        <p className="mt-2 rounded-lg bg-brand-50 p-3 text-sm text-brand-800">
                          ${t.playbook.syncRangeTotals}: ${(syncTotals.start_date || syncWindow?.start_date || '-')} ~ ${(syncTotals.end_date || syncWindow?.end_date || '-')}
                          | ${t.playbook.syncTotalDays}: ${syncTotals.days ?? '-'}
                          | ${t.playbook.syncTotalClicks}: ${syncTotals.clicks ?? 0}
                          | ${t.playbook.syncTotalSpend}: ${fmtMoney(syncTotals.spend ?? 0, language)}
                          | ${t.playbook.syncTotalSales}: ${fmtMoney(syncTotals.sales ?? 0, language)}
                          | ${t.playbook.syncTotalAcos}: ${fmtPct(syncTotals.acos ?? 0)}
                          | ${t.playbook.syncTotalAdGroups}: ${syncTotals.ad_groups_with_spend ?? 0}
                        </p>
                      `
                    : null}
                  ${syncEmptyGuard?.has_warning
                    ? html`
                        <div className="mt-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                          <p className="font-semibold">${t.playbook.emptyGuardTitle}</p>
                          <p className="mt-1">
                            ${t.playbook.emptyGuardSummary}
                            | ${t.playbook.emptyGuardPending}: ${syncEmptyGuard.pending_total_days ?? 0}
                            | ${t.playbook.emptyGuardSuspect}: ${syncEmptyGuard.suspect_total_days ?? 0}
                            | ${t.playbook.emptyGuardConfirmed}: ${syncEmptyGuard.confirmed_total_days ?? 0}
                          </p>
                          ${Object.entries(syncEmptyGuard.by_category || {}).map(([category, detail]) => html`
                            <p key=${category} className="mt-1 text-xs">
                              ${(t.playbook.emptyGuardCategory || {})[category] || category}:
                              ${t.playbook.emptyGuardPending} ${detail?.pending_days ?? 0},
                              ${t.playbook.emptyGuardSuspect} ${detail?.suspect_days ?? 0},
                              ${t.playbook.emptyGuardConfirmed} ${detail?.confirmed_days ?? 0}
                            </p>
                          `)}
                        </div>
                      `
                    : null}
                  ${syncSummary
                    ? html`<${LingxingSyncTable}
                        rows=${syncRows}
                        t=${t}
                        language=${language}
                        sortKey=${syncSortKey}
                        sortDir=${syncSortDir}
                        onSort=${(key) => {
                          if (syncSortKey === key) {
                            setSyncSortDir((v) => (v === 'asc' ? 'desc' : 'asc'));
                            return;
                          }
                          setSyncSortKey(key);
                          setSyncSortDir('asc');
                        }}
                      />`
                    : html`<p className="mt-2 rounded-lg bg-brand-50 p-3 text-sm text-brand-800">${t.playbook.syncSummaryEmpty}</p>`}
                </div>

                <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                  <h4 className="text-sm font-semibold">${t.playbook.contextJobLabel}</h4>
                  <p className="mt-2 rounded-lg bg-brand-50 p-3 text-sm text-brand-800">
                    ${contextJobStatus
                      ? `${t.playbook.contextJobProgress}: ${contextJobStatus.progress_pct ?? 0}% | ${t.playbook.contextJobStage}: ${contextJobStatus.stage || '-'} | ${contextJobStatus.message || t.playbook.contextJobRunning}`
                      : t.playbook.contextJobIdle}
                  </p>
                </div>

                <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                  <h4 className="text-sm font-semibold">${t.playbook.uploadSummaryLabel}</h4>
                  <p className="mt-2 rounded-lg bg-brand-50 p-3 text-sm text-brand-800">${uploadSummary || t.playbook.uploadSummaryEmpty}</p>
                </div>

                <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                  <h4 className="text-sm font-semibold">${t.playbook.whitepaperStorageLabel}</h4>
                  <p className="mt-2 rounded-lg bg-brand-50 p-3 text-sm text-brand-800">${whitepaperStorageSummary || t.playbook.whitepaperStorageEmpty}</p>
                </div>

                <${TextOutputBlock}
                  title=${t.playbook.whitepaperOutput}
                  text=${whitepaper}
                  emptyText=${t.playbook.whitepaperEmpty}
                  meta=${whitepaperMeta}
                  expanded=${whitepaperExpanded}
                  onToggle=${() => setWhitepaperExpanded((v) => !v)}
                  onExport=${onExportWhitepaper}
                  t=${t}
                />

                <${TextOutputBlock}
                  title=${t.playbook.adviceOutput}
                  text=${advice}
                  emptyText=${t.playbook.adviceEmpty}
                  meta=${adviceMeta}
                  expanded=${adviceExpanded}
                  onToggle=${() => setAdviceExpanded((v) => !v)}
                  onExport=${onExportAdvice}
                  t=${t}
                />
              </section>
            `
          : null}

        ${view === 'autoReplyMail'
          ? html`
              <section className="space-y-4">
                <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                  <h3 className="text-base font-semibold">${t.autoReply.title}</h3>
                  <p className="mt-1 text-sm text-brand-600">${t.autoReply.subtitle}</p>
                  <p className="mt-3 rounded-lg bg-brand-50 p-3 text-sm text-brand-800">${t.autoReply.tip}</p>
                  <div className="mt-4 flex flex-wrap gap-2">
                    <button
                      onClick=${onFetchCustomerMessages}
                      disabled=${Boolean(mailLoading)}
                      className=${`rounded-md border px-4 py-2 text-sm font-semibold ${
                        mailLoading
                          ? 'cursor-not-allowed border-brand-100 bg-brand-50 text-brand-300'
                          : 'border-brand-300 bg-white text-brand-700 hover:bg-brand-50'
                      }`}
                    >
                      ${t.autoReply.fetchBtn}
                    </button>
                    <button
                      onClick=${loadCustomerMessages}
                      disabled=${Boolean(mailLoading)}
                      className=${`rounded-md border px-4 py-2 text-sm font-semibold ${
                        mailLoading
                          ? 'cursor-not-allowed border-brand-100 bg-brand-50 text-brand-300'
                          : 'border-brand-300 bg-white text-brand-700 hover:bg-brand-50'
                      }`}
                    >
                      ${t.autoReply.reloadBtn}
                    </button>
                  </div>
                  ${mailLoading ? html`<p className="mt-3 text-sm text-brand-600">${t.loading}</p>` : null}
                  ${mailNotice
                    ? html`<p className="mt-3 rounded-lg bg-brand-50 px-3 py-2 text-sm text-brand-800">${mailNotice}</p>`
                    : null}
                </div>

                <div className="overflow-hidden rounded-xl border border-brand-100 bg-white shadow-sm">
                  <div className="grid min-h-[680px] grid-cols-1 md:grid-cols-[320px_1fr]">
                    <aside className="border-b border-brand-100 bg-brand-50/40 md:border-b-0 md:border-r">
                      <div className="flex h-11 items-center justify-between border-b border-brand-100 px-3 text-xs font-semibold uppercase tracking-wide text-brand-600">
                        <span>${t.autoReply.leftListTitle}</span>
                        <span>${t.autoReply.selectedCount}: ${mailRows.length}</span>
                      </div>
                      <div className="max-h-[640px] overflow-y-auto">
                        ${mailRows.length
                          ? mailRows.map((row) => {
                              const detail = mailDetailMap[row.id] || null;
                              const title = detail?.from_name || detail?.from_address || row.conversation_id || `#${row.id}`;
                              const subject = detail?.subject || row.buyer_message || '-';
                              const isActive = selectedMailRow?.id === row.id;
                              return html`
                                <button
                                  key=${row.id}
                                  onClick=${() => onSelectMessage(row)}
                                  className=${`block w-full border-b border-brand-100 px-3 py-3 text-left ${isActive ? 'bg-brand-100/70' : 'hover:bg-brand-50'}`}
                                >
                                  <div className="flex items-start justify-between gap-3">
                                    <p className="truncate text-sm font-semibold text-brand-800">${title}</p>
                                    <p className="shrink-0 text-xs text-brand-500">${formatDateTime(row.created_at)}</p>
                                  </div>
                                  <p className="mt-1 truncate text-xs text-brand-600">${subject}</p>
                                  <span className="mt-2 inline-flex rounded-full border border-brand-200 bg-white px-2 py-0.5 text-[11px] font-semibold text-brand-700">${row.status}</span>
                                </button>
                              `;
                            })
                          : html`<p className="px-3 py-4 text-sm text-brand-600">${t.autoReply.empty}</p>`}
                      </div>
                    </aside>

                    <div className="flex min-w-0 flex-col">
                      ${selectedMailRow
                        ? html`
                            <div className="border-b border-brand-100 px-4 py-3">
                              <div className="flex flex-wrap items-center justify-between gap-2">
                                <div className="min-w-0">
                                  <p className="truncate text-base font-semibold text-brand-900">${selectedMailDetail?.subject || selectedMailRow.buyer_message || '-'}</p>
                                  <p className="mt-1 truncate text-xs text-brand-600">${t.autoReply.fromLabel}: ${selectedMailDetail?.from_address || selectedMailDetail?.from_name || '-'}</p>
                                </div>
                                <button
                                  onClick=${() => onProcessMessage(selectedMailRow.id)}
                                  disabled=${Boolean(mailLoading)}
                                  className="rounded-md border border-brand-300 bg-white px-3 py-1.5 text-xs font-semibold text-brand-700 hover:bg-brand-50 disabled:cursor-not-allowed disabled:border-brand-100 disabled:bg-brand-50 disabled:text-brand-300"
                                >
                                  ${t.autoReply.processBtn}
                                </button>
                              </div>
                            </div>

                            <div className="min-h-0 flex-1 space-y-3 overflow-y-auto bg-white px-4 py-4">
                              <div className="max-w-[85%] rounded-xl border border-brand-100 bg-brand-50 p-3">
                                <p className="mb-1 text-xs font-semibold text-brand-700">${t.autoReply.buyerLabel}</p>
                                ${selectedMailDetailLoading
                                  ? html`<p className="whitespace-pre-wrap break-words text-sm text-brand-600">${t.autoReply.detailLoading}</p>`
                                  : html`<p className="whitespace-pre-wrap break-words text-sm text-brand-800">${selectedBuyerText || selectedMailRow.buyer_message || '-'}</p>`}
                              </div>

                              ${selectedMailRow.status === 'sent' || selectedMailRow.status === 'auto_sent'
                                ? html`
                                    <div className="ml-auto max-w-[85%] rounded-xl border border-green-200 bg-green-50 p-3">
                                      <p className="mb-1 text-xs font-semibold text-green-700">${t.autoReply.sellerLabel} · ${t.autoReply.sentTag}</p>
                                      <p className="whitespace-pre-wrap break-words text-sm text-green-900">${selectedHistoryReply || '-'}</p>
                                    </div>
                                  `
                                : null}
                            </div>

                            <div className="border-t border-brand-100 bg-brand-50/30 p-3">
                              <div className="mb-2 flex flex-wrap items-center gap-2">
                                <label className="inline-flex cursor-pointer items-center rounded-md border border-brand-300 bg-white px-3 py-1.5 text-xs font-semibold text-brand-700 hover:bg-brand-50">
                                  <input
                                    type="file"
                                    multiple
                                    className="hidden"
                                    onChange=${(e) => {
                                      onUploadAttachments(selectedMailRow.id, e.target.files);
                                      e.target.value = '';
                                    }}
                                  />
                                  ${t.autoReply.uploadAttachment}
                                </label>
                                ${selectedAttachments.length
                                  ? selectedAttachments.map(
                                      (item, idx) => html`
                                        <span key=${`${selectedMailRow.id}_${idx}`} className="inline-flex items-center gap-1 rounded-md border border-brand-200 bg-white px-2 py-1 text-xs text-brand-700">
                                          <span className="max-w-[180px] truncate">${item.name}</span>
                                          <button
                                            onClick=${() => onRemoveAttachment(selectedMailRow.id, idx)}
                                            className="rounded px-1 text-brand-500 hover:bg-brand-50 hover:text-brand-700"
                                            title=${t.autoReply.removeAttachment}
                                          >
                                            ×
                                          </button>
                                        </span>
                                      `,
                                    )
                                  : html`<span className="text-xs text-brand-500">${t.autoReply.attachmentEmpty}</span>`}
                              </div>

                              <div className="flex items-end gap-2">
                                <textarea
                                  value=${selectedDraft}
                                  onChange=${(e) =>
                                    setReplyDrafts((prev) => ({ ...prev, [selectedMailRow.id]: e.target.value }))}
                                  rows="4"
                                  placeholder=${t.autoReply.draftPlaceholder}
                                  className="min-h-[96px] flex-1 rounded-md border border-brand-200 bg-white px-3 py-2 text-sm text-brand-800"
                                />
                                <div className="flex shrink-0 flex-col gap-2">
                                  <button
                                    onClick=${() => onSaveReply(selectedMailRow.id)}
                                    disabled=${Boolean(mailLoading) || !String(selectedDraft || '').trim()}
                                    className="rounded-md border border-brand-300 bg-white px-3 py-2 text-xs font-semibold text-brand-700 hover:bg-brand-50 disabled:cursor-not-allowed disabled:border-brand-100 disabled:bg-brand-50 disabled:text-brand-300"
                                  >
                                    ${t.autoReply.saveBtn}
                                  </button>
                                  <button
                                    onClick=${() => onSendMessage(selectedMailRow.id)}
                                    disabled=${Boolean(mailLoading) || !String(selectedDraft || '').trim()}
                                    className="rounded-md bg-brand-700 px-4 py-2 text-xs font-semibold text-white hover:bg-brand-800 disabled:cursor-not-allowed disabled:bg-brand-200"
                                  >
                                    ${t.autoReply.sendBtn}
                                  </button>
                                </div>
                              </div>
                              <p className="mt-2 text-[11px] text-brand-500">${t.autoReply.sendHint}</p>
                            </div>
                          `
                        : html`<div className="flex h-full items-center justify-center text-sm text-brand-500">${t.autoReply.empty}</div>`}
                    </div>
                  </div>
                </div>
              </section>
            `
          : null}

        ${view === 'userManagement'
          ? html`
              <section className="space-y-4">
                ${authUser?.role !== 'admin'
                  ? html`<div className="rounded-xl border border-blue-200 bg-blue-50 p-4 text-sm text-blue-800">${t.userMgmt.onlyAdmin}</div>`
                  : html`
                      <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                        <div className="flex items-center justify-between">
                          <h3 className="text-base font-semibold">${t.userMgmt.createTitle}</h3>
                          <button
                            onClick=${loadUserManagementData}
                            className="rounded-md border border-brand-300 bg-white px-3 py-2 text-sm font-semibold text-brand-700 hover:bg-brand-50"
                          >
                            ${t.userMgmt.reload}
                          </button>
                        </div>
                        <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                          <input
                            type="text"
                            name="new_user_username"
                            autoComplete="off"
                            value=${newUserUsername}
                            onChange=${(e) => setNewUserUsername(e.target.value)}
                            className="rounded-md border border-brand-200 px-3 py-2 text-sm"
                            placeholder=${t.userMgmt.username}
                          />
                          <input
                            type="password"
                            name="new_user_password"
                            autoComplete="new-password"
                            value=${newUserPassword}
                            onChange=${(e) => setNewUserPassword(e.target.value)}
                            className="rounded-md border border-brand-200 px-3 py-2 text-sm"
                            placeholder=${t.userMgmt.password}
                          />
                          <select
                            value=${newUserRole}
                            onChange=${(e) => setNewUserRole(e.target.value)}
                            className="rounded-md border border-brand-200 px-3 py-2 text-sm"
                          >
                            <option value="admin">admin</option>
                            <option value="manager">manager</option>
                            <option value="staff">staff</option>
                            <option value="viewer">viewer</option>
                          </select>
                        </div>
                        <button
                          onClick=${onCreateManagedUser}
                          disabled=${Boolean(adminLoading)}
                          className=${`mt-3 rounded-md px-4 py-2 text-sm font-semibold ${
                            adminLoading ? 'cursor-not-allowed bg-brand-200 text-white' : 'bg-brand-700 text-white hover:bg-brand-800'
                          }`}
                        >
                          ${t.userMgmt.createBtn}
                        </button>
                        ${adminNotice
                          ? html`<p className="mt-3 rounded-md bg-brand-50 px-3 py-2 text-sm text-brand-800">${adminNotice}</p>`
                          : null}
                        ${adminLoading ? html`<p className="mt-2 text-sm text-brand-600">${t.loading}</p>` : null}
                      </div>

                      <div className="rounded-xl border border-brand-100 bg-white p-4 shadow-sm">
                        <h3 className="text-base font-semibold">${t.userMgmt.listTitle}</h3>
                        <div className="mt-3 space-y-4">
                          ${adminUsers.map(
                            (user) => html`
                              <article key=${user.user_id} className="rounded-lg border border-brand-100 p-3">
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                  <div className="text-sm">
                                    <div className="font-semibold">${user.username}</div>
                                    <div className="text-brand-600">${t.userMgmt.status}: ${user.status}</div>
                                  </div>
                                  <div className="flex flex-wrap items-center gap-2">
                                    <select
                                      value=${userRoleDrafts[user.user_id] || user.role}
                                      onChange=${(e) =>
                                        setUserRoleDrafts((prev) => ({ ...prev, [user.user_id]: e.target.value }))}
                                      className="rounded-md border border-brand-200 px-2 py-1 text-sm"
                                    >
                                      <option value="admin">admin</option>
                                      <option value="manager">manager</option>
                                      <option value="staff">staff</option>
                                      <option value="viewer">viewer</option>
                                    </select>
                                    <button
                                      onClick=${() => onSaveManagedUserRole(user.user_id)}
                                      className="rounded-md border border-brand-300 bg-white px-3 py-1 text-sm font-semibold text-brand-700 hover:bg-brand-50"
                                    >
                                      ${t.userMgmt.saveRole}
                                    </button>
                                    <button
                                      onClick=${() => onToggleManagedUserStatus(user)}
                                      disabled=${authUser?.user_id === user.user_id}
                                      className=${`rounded-md border px-3 py-1 text-sm font-semibold ${
                                        authUser?.user_id === user.user_id
                                          ? 'cursor-not-allowed border-brand-100 bg-brand-50 text-brand-300'
                                          : 'border-brand-300 bg-white text-brand-700 hover:bg-brand-50'
                                      }`}
                                    >
                                      ${user.status === 'active' ? t.userMgmt.deactivate : t.userMgmt.activate}
                                    </button>
                                  </div>
                                </div>

                                <div className="mt-3 flex flex-wrap items-center gap-2">
                                  <input
                                    type="password"
                                    name=${`reset_user_password_${user.user_id}`}
                                    autoComplete="new-password"
                                    value=${userPasswordDrafts[user.user_id] || ''}
                                    onChange=${(e) =>
                                      setUserPasswordDrafts((prev) => ({ ...prev, [user.user_id]: e.target.value }))}
                                    className="rounded-md border border-brand-200 px-3 py-1 text-sm"
                                    placeholder=${t.userMgmt.password}
                                  />
                                  <button
                                    onClick=${() => onResetManagedUserPassword(user.user_id)}
                                    className="rounded-md border border-brand-300 bg-white px-3 py-1 text-sm font-semibold text-brand-700 hover:bg-brand-50"
                                  >
                                    ${t.userMgmt.resetPwd}
                                  </button>
                                </div>

                                <div className="mt-3">
                                  <p className="mb-2 text-xs font-semibold text-brand-700">${t.userMgmt.stores}</p>
                                  <div className="grid grid-cols-1 gap-2 md:grid-cols-3">
                                    ${adminStores.map(
                                      (store) => html`
                                        <label key=${`${user.user_id}_${store.store_id}`} className="inline-flex items-center gap-2 text-sm text-brand-800">
                                          <input
                                            type="checkbox"
                                            checked=${Boolean(userStoreAccessMap?.[user.user_id]?.[store.store_id])}
                                            onChange=${() => onToggleManagedStoreAccess(user.user_id, store)}
                                          />
                                          <span>${store.store_name}</span>
                                        </label>
                                      `,
                                    )}
                                  </div>
                                </div>
                              </article>
                            `,
                          )}
                        </div>
                      </div>
                    `}
              </section>
            `
          : null}
      </main>
    </div>
  `;
}

createRoot(document.getElementById('root')).render(html`<${App} />`);
