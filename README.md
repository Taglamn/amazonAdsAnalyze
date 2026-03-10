# Amazon Ads Analyze Dashboard (FastAPI + React + Tailwind)

一个多租户 Amazon 广告分析面板，包含：

- 每日表现数据看板（Date / Clicks / Spend / ACoS / Sales）
- 广告组调价建议（含 bid 建议）
- 调整历史与 7 天前后 ROI 影响分析
- 基于 Gemini API + 店铺 Playbook 的策略白皮书与建议生成
- 通过领星 ERP OpenAPI 自动同步多店铺广告数据与操作日志，并直接生成分析结果
- 支持上传广告组 Excel（广告日数据 + 操作历史）并交给 Gemini 自动分析

## 项目结构

- `app/main.py`: FastAPI 服务与 API 路由
- `app/data_access.py`: 多租户数据模型 `Store` 与仓储读取
- `app/analysis.py`: 逻辑对齐层，含 `analyze_impact`
- `app/gemini_bridge.py`: Playbook 加载、租户校验、Gemini 调用
- `app/lingxing_client.py`: 领星 OpenAPI 客户端（签名、鉴权、分页）
- `app/lingxing_sync.py`: 领星同步与分析编排（可落盘到本地 CSV）
- `app/context_export_jobs.py`: Context Package 异步导出任务管理（内存任务 + 磁盘文件）
- `app/upload_analysis.py`: 上传 Excel 解析与标准化
- `app/static/index.html`: 前端页面容器 + Tailwind
- `app/static/app.js`: React 管理面板
- `scripts/gemini_advice.py`: 独立 AI 决策脚本
- `scripts/lingxing_sync.py`: 独立领星同步 + 分析脚本
- `app/data/playbooks/store_playbook_{id}.json`: 店铺策略规则

## 快速启动

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 如果要调用 Gemini
export GEMINI_API_KEY="your_key_here"
export GEMINI_MODEL="gemini-2.5-flash"
export GEMINI_MAX_OUTPUT_TOKENS="4096"
export GEMINI_CONTINUATION_ROUNDS="2"

uvicorn app.main:app --reload
```

打开：`http://127.0.0.1:8000`

## Docker 部署（Alibaba Cloud Linux，公网访问 8080）

已提供：

- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`

### 1) 服务器准备

```bash
cd /path/to/Amazon_Ads_Analyze
cp .env.example .env
# 编辑 .env，填入 GEMINI / 领星配置
```

### 2) 启动容器（端口 8080）

```bash
docker compose up -d --build
docker compose ps
```

容器内服务监听：`0.0.0.0:8080`  
宿主机映射：`8080:8080`

访问地址：

`http://<你的公网IP>:8080`

### 3) 放行云上与系统防火墙端口

必须同时满足以下两项：

1. 阿里云安全组入方向放行 `TCP 8080`
2. 系统防火墙放行 `8080/tcp`（如启用了 `firewalld`）

`firewalld` 示例：

```bash
sudo firewall-cmd --permanent --add-port=8080/tcp
sudo firewall-cmd --reload
```

### 4) 常用运维命令

```bash
# 查看日志
docker compose logs -f

# 重启
docker compose restart

# 停止并删除容器
docker compose down
```

说明：

- `docker-compose.yml` 已挂载 `./app/data:/app/app/data`，白皮书与同步数据会持久化到宿主机。

## 领星 ERP 配置（手工）

按你的要求，需要先人工配置领星 ERP 用户名与密码，同时配置 OpenAPI 的 AppId / AppSecret。

可参考 `.env.example`：

```bash
cp .env.example .env
```

关键变量：

- `LINGXING_ERP_USERNAME`
- `LINGXING_ERP_PASSWORD`
- `LINGXING_APP_ID`
- `LINGXING_APP_SECRET`
- `LINGXING_BASE_URL`（默认 `https://openapi.lingxing.com`）
- `LINGXING_TIMEOUT_SECONDS`（默认 `90`）
- `LINGXING_MAX_RETRIES`（默认 `3`）
- `LINGXING_RETRY_BACKOFF_SECONDS`（默认 `1.5`）
- `LINGXING_TLS_MODE`（`default` 或 `tls1_2`）
- `LINGXING_PROXY_URL`（如 `http://proxy.company.com:8080`）
- `LINGXING_INSECURE_SKIP_VERIFY`（仅诊断用途，不建议生产开启）
- `CONTEXT_EXPORT_MAX_WORKERS`（Context Package 异步导出并发，默认 `1`）
- `CONTEXT_EXPORT_RETENTION_HOURS`（异步导出文件保留小时数，默认 `24`）

系统会自动读取项目根目录 `.env`（通过 `python-dotenv`），无需额外 `export`。

### 常见网络报错排查

如果出现 `Lingxing network error: ... The handshake operation timed out`：

1. 先确认当前机器可访问 `https://openapi.lingxing.com`（DNS、出网、防火墙、代理）。
2. 确认领星 OpenAPI 白名单中已添加当前出口 IP。
3. 适当调大：
   - `LINGXING_TIMEOUT_SECONDS=120`
   - `LINGXING_MAX_RETRIES=5`
   - `LINGXING_TLS_MODE=tls1_2`
4. 若公司网络需要代理，配置系统级 `HTTPS_PROXY` / `HTTP_PROXY` 后重试。
   或设置应用内代理：`LINGXING_PROXY_URL=http://proxy.company.com:8080`

可先运行网络自检：

```bash
python scripts/lingxing_network_check.py
```

## 关键 API

- `GET /api/stores`（默认会尝试拉取领星绑定店铺名并返回 `store_id + store_name`）
  - 可选查询参数：`include_bound=true|false`
- `GET /api/stores/{store_id}/performance`
- `GET /api/stores/{store_id}/ad-group-recommendations`
- `GET /api/stores/{store_id}/optimization-cases`
- `POST /api/stores/{store_id}/ai/advice`
- `POST /api/stores/{store_id}/ai/whitepaper`
- `GET /api/stores/{store_id}/whitepaper`（获取已保存白皮书）
- `POST /api/stores/{store_id}/whitepaper/import`（导入 `.txt/.md` 白皮书）
- `GET /api/stores/{store_id}/whitepaper/export`（导出白皮书）
- `POST /api/lingxing/sync`（自动同步领星广告数据+历史操作并分析）
- `POST /api/lingxing/context-package/jobs`（创建 Context Package 异步导出任务）
- `GET /api/lingxing/context-package/jobs/{job_id}`（查询导出任务状态/进度）
- `GET /api/lingxing/context-package/jobs/{job_id}/download`（任务完成后下载导出文件）
- `POST /api/lingxing/context-package/export`（导出当前店铺 365 天 Context Package，供 Gemini 使用）
- `POST /api/ai/upload-analysis`（上传 Excel 并调用 Gemini 分析）

Context Package 异步导出流程：

1. `POST /api/lingxing/context-package/jobs` 创建任务
2. `GET /api/lingxing/context-package/jobs/{job_id}` 轮询状态（`queued/running/succeeded/failed`）
3. `GET /api/lingxing/context-package/jobs/{job_id}/download` 下载结果文件

AI 接口支持 `lang` 参数（`zh` / `en`），例如：

```json
{ "lang": "zh" }
```

说明：

- `POST /api/stores/{store_id}/ai/advice` 会优先读取该店铺已保存白皮书，再生成建议。
- 若店铺白皮书不存在，会先自动生成并保存白皮书，再生成建议。

领星同步请求示例：

```json
{
  "store_id": "lingxing_123456",
  "start_date": "2026-03-01",
  "end_date": "2026-03-07",
  "persist": true
}
```

或单日报告：

```json
{
  "store_id": "lingxing_123456",
  "report_date": "2026-03-08",
  "persist": true
}
```

说明：

- `persist=true` 时会把同步结果落盘到 `app/data/performance/*.csv` 和 `app/data/history/*.csv`
- 并自动生成缺失的 `store_playbook_{id}.json`
- API 会直接返回每个店铺的最新表现、优化案例和 bid 建议

上传分析接口说明（`multipart/form-data`）：

- `file`: `.xlsx` / `.xls`
- `store_id`: 可选，默认 `uploaded_store`
- `lang`: `zh` / `en`
- `model`: Gemini 模型名（不传时使用 `GEMINI_MODEL`，默认 `gemini-2.5-flash`）
- `rules`: 可选，JSON 字符串或纯文本规则

工作簿要求：

- Sheet A（广告日数据）：至少含 `日期`、`点击`、`花费`、`广告销售额`（`ACoS` 可选）
- Sheet B（操作历史）：至少含 `操作时间`、`操作前的数据`、`操作后的数据`（会自动提取“竞价”变更）

## 独立 Gemini 脚本

```bash
export GEMINI_API_KEY="your_key_here"
python scripts/gemini_advice.py --store-id store_a
# 指定日期
python scripts/gemini_advice.py --store-id store_a --date 2026-03-05
# 指定英文输出
python scripts/gemini_advice.py --store-id store_a --lang en
```

该脚本会在调用 Gemini 前验证 `metrics.store_id == playbook.store_id`，防止租户数据泄露。

## 独立领星同步脚本

```bash
# 最近 14 天（默认），并落盘
python scripts/lingxing_sync.py

# 指定日期区间
python scripts/lingxing_sync.py --start-date 2026-03-01 --end-date 2026-03-07

# 仅查看分析结果，不落盘
python scripts/lingxing_sync.py --report-date 2026-03-08 --no-persist
```

## Context Package 导出脚本

```bash
# 默认过去 365 天
python scripts/lingxing_context_package.py --store-id lingxing_123456

# 指定时间范围
python scripts/lingxing_context_package.py --store-id lingxing_123456 --start-date 2025-03-01 --end-date 2026-02-28

# 指定输出文件
python scripts/lingxing_context_package.py --store-id lingxing_123456 --output /tmp/context_package.json
```

导出的 `ad_groups[]` 现包含 `placement_metrics[]`，用于给 Gemini 识别不同广告位对 ACoS 的影响，字段包括：

- `placement_type`
- `top_of_search_is`
- `clicks`
- `spend`
- `sales`
- `acos`
