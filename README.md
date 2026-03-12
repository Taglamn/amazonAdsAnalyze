# Amazon Ads Analyze 平台说明（给产品经理/项目经理）

> 文档目标：用一份 README 说明当前版本“已经实现了什么、怎么实现、如何部署、有什么风险与边界”。

## 1. 项目定位

Amazon 广告分析与运营辅助平台，核心覆盖：

- 多店铺（多租户）广告数据管理与隔离
- 领星 ERP 数据同步（广告报表 + 操作日志）
- Gemini 白皮书与广告组建议生成
- Context Package（365 天）导出，供 LLM 深度分析
- 运营策略白皮书（基于本地 SQLite 的年度因果分析）
- 客服 AI（Buyer Message 抓取/生成/审核/发送，后端 API 已集成）

---

## 2. 当前能力矩阵（状态总览）

| 模块 | 状态 | 入口 | 说明 |
|---|---|---|---|
| 多租户店铺切换 + 中英文 UI | 已上线 | Web 前端 | 店铺下拉优先展示绑定店铺名（`store_name`），中英文文案已本地化 |
| Dashboard / Ad Groups / Optimization History | 已上线 | Web 前端 | 每日表现、广告组建议、7 天前后 ROI 影响案例 |
| 领星同步（当前店铺 + 日期筛选） | 已上线 | Web 前端 + API | 支持按店铺同步，支持起止日期过滤 |
| 领星结果表格（Campaign 合并、排序） | 已上线 | Web 前端 | 输出列：广告组合、Campaign、Ad Group、当前/建议 bid、Clicks/Spend/Sales/ACOS |
| Gemini 白皮书生成 | 已上线 | Web 前端 + API | 文本白皮书，支持中英文 |
| Gemini 广告建议生成 | 已上线 | Web 前端 + API | 生成建议前会优先读取已保存白皮书 |
| 白皮书导入/导出 | 已上线 | Web 前端 + API | 支持 `.txt/.md` 导入覆盖、导出 |
| 上传 Excel 并分析 | 已上线 | Web 前端 + API | 两个 Sheet：广告日数据 + 操作历史，支持自动字段识别 |
| Context Package 异步导出 | 已上线 | Web 前端 + API | 任务化导出，轮询进度，完成后自动下载 |
| 运营白皮书（年度/14天因果） | 后端已完成 | API | 基于 SQLite 的增量数据与因果映射 |
| 运营周期建议（含 High Sensitivity） | 后端已完成 | API | 目标竞价/版位/否定词建议 + 价格库存偏离告警 |
| 客服 AI（Buyer Message） | 后端已完成 | API + Celery | 拉取消息、AI 回复、人工审核、发送，前端页面尚未接入 |

---

## 3. 用户侧功能（前端）

前端文件：`app/static/index.html`、`app/static/app.js`

### 3.1 导航与视图

- `Dashboard`
- `Playbook Logic`
- `Ad Groups`
- `Optimization History`

### 3.2 全局能力

- 中英文切换（自动跟随浏览器语言，可手动切换）
- 店铺切换（展示 `store_name`）
- 店铺隔离说明文案（Header 显示）

### 3.3 Playbook Logic 区功能

- 同步领星数据并分析（仅当前店铺）
- 日期筛选（`start_date` / `end_date`）
- 下载 Context Package（异步任务 + 进度）
- 上传广告组 Excel 并分析
- 生成白皮书（Gemini）
- 生成广告组建议（Gemini）
- 导入白皮书（`.txt/.md`）
- 导出白皮书
- 白皮书/建议全文展开显示

### 3.4 领星同步结果展示

- 表格支持排序
- Campaign 相同项合并显示（rowSpan）
- 仅输出有消耗（`spend > 0`）广告组
- Campaign 名来自领星 Campaign 接口映射

---

## 4. 技术架构

## 4.1 技术栈

- 前端：React（UMD）+ Tailwind CSS
- 后端：FastAPI
- 数据处理：pandas
- 外部系统：Lingxing OpenAPI、Gemini API、Amazon SP-API（客服模块）
- 存储：
  - CSV（历史兼容路径）
  - SQLite（运营分析主数据）
  - PostgreSQL（客服消息）
- 队列：Celery + Redis（客服异步任务）

## 4.2 目录与职责

- `app/main.py`：主 API 路由与应用启动
- `app/data_access.py`：CSV 多租户仓储（StoreRepository）
- `app/analysis.py`：7 天窗口影响分析 + 基础 bid 建议
- `app/gemini_bridge.py`：Gemini Prompt 构建、调用、语言控制
- `app/whitepaper_store.py`：文本白皮书保存/读取
- `app/upload_analysis.py`：上传 Excel 解析与标准化
- `app/lingxing_client.py`：领星鉴权、签名、分页、容错
- `app/lingxing_sync.py`：领星同步主流程（CSV 输出 + 前端表格数据）
- `app/lingxing_context_package.py`：365 天 Context Package 生成
- `app/context_export_jobs.py`：Context 异步任务管理（进程内）
- `app/ops_db.py`：SQLite 持久层（运营分析）
- `app/ops_sync.py`：运营数据增量同步（365 天）
- `app/ops_whitepaper.py`：年度运营白皮书合成（14 天因果）
- `app/ops_advisory.py`：运营周期建议（含高敏感标记）
- `app/ops_logger.py`：运营日志
- `app/customer_service_ai/*`：客服 AI 子系统（DB/API/LLM/Task/SP-API）

---

## 5. 数据与隔离设计

## 5.1 多租户隔离策略

- API 都要求显式 `store_id`
- `analysis.py` 使用 `_assert_store_scope()` 校验 `history_df/perf_df` 仅含当前店铺
- Gemini 调用前执行 `validate_metrics_store(metrics, store_id)`，防止跨店铺误传
- Playbook 文件按店铺拆分：`app/data/playbooks/store_playbook_{store_id}.json`

## 5.2 本地数据层

### A. CSV（兼容层）

- `app/data/performance/{store_id}.csv`
- `app/data/history/{store_id}.csv`

用于前端 Dashboard、Ad Groups、Optimization History 的现有读取链路。

### B. SQLite（运营分析层）

默认：`app/data/ops_data.db`（可由 `OPS_DB_PATH` 覆盖）

关键表：

- `performance_daily`
- `change_history`
- `placement_daily`
- `query_term_daily`
- `inventory_snapshot`
- `sync_coverage`（记录已同步日期，避免重复请求）

### C. 文件输出

- 文本白皮书：`app/data/whitepapers/{store_id}.md`
- 运营白皮书：`app/data/ops_whitepapers/{store_id}_ops_whitepaper.{json|md}`
- Context 导出：`app/data/context_packages/{job_id}.json`
- 运营日志：`app/data/logs/operations.log`

---

## 6. 核心业务流程（实现逻辑）

## 6.1 领星同步与前端展示（当前前端主流程）

1. 前端调用 `POST /api/lingxing/sync`
2. 后端按日期窗口拉取：
   - 广告组日报（SP/SB/SD）
   - 操作日志
   - Bid 快照
   - Campaign 基础信息（用于 name 映射）
3. 生成：
   - 每日汇总表现
   - 历史变更（bid 变化）
   - 优化案例（7 天前后）
   - 广告组建议
   - `lingxing_output_rows`（表格显示）
4. `persist=true` 时落盘 CSV，并自动补默认 playbook

## 6.2 Gemini 白皮书/建议

- 白皮书：`POST /api/stores/{store_id}/ai/whitepaper`
- 建议：`POST /api/stores/{store_id}/ai/advice`

建议生成逻辑：

1. 优先读取已保存白皮书
2. 如无白皮书，先自动生成并保存
3. 构造 Prompt（带店铺规则 + 昨日指标 + 白皮书上下文）
4. 输出多行可读文本；支持 continuation rounds 防截断

## 6.3 上传 Excel 分析

入口：`POST /api/ai/upload-analysis`（multipart）

- 自动识别两个 sheet（日报/操作历史）
- 自动匹配中英文字段名
- 提取 bid 变化
- 生成摘要 + 启发式建议 + Gemini 白皮书/建议

## 6.4 Context Package（给 Gemini 的结构化上下文）

入口：

- 创建任务：`POST /api/lingxing/context-package/jobs`
- 查状态：`GET /api/lingxing/context-package/jobs/{job_id}`
- 下载文件：`GET /api/lingxing/context-package/jobs/{job_id}/download`

实现要点：

- 任务在进程内 `ThreadPoolExecutor` 执行
- 前端 2 秒轮询
- 数据粒度：`date + ad_group`
- 融合：performance + query terms + placement + ASIN 价格/库存
- placement 输出嵌套到 ad_group 内，包含：
  - `placement_type`
  - `top_of_search_is`
  - `clicks/spend/sales/acos`

## 6.5 运营白皮书与周期建议（后端能力）

### 增量同步（365天）

入口：`POST /api/ops/sync/incremental`

- 读取 `sync_coverage` 找缺失日期
- 仅请求缺失天数据
- 写入 SQLite 并更新 coverage
- 可选导出回 CSV（兼容旧链路）

### 年度运营白皮书

入口：`POST /api/ops/whitepaper/synthesize`

- 分析近 12 个月 targeting bid / placement bid / negative targeting
- 对每次操作做前后 14 天因果映射
- 输出 success/failure pattern
- 生成 `master_strategy`（成功区间、placement benchmark、negative rule、库存价格假设）

### 周期建议

入口：`POST /api/ops/advisory`

- 基于最新广告组数据对齐 `master_strategy`
- 输出：
  - targeting bid 建议
  - placement multiplier 建议
  - negative targeting 候选词
- 若当日价格/库存偏离白皮书假设，标记：
  - `high_sensitivity = true`
  - `manual_review_required = true`

> 注意：当前前端按钮尚未接入 `/api/ops/*`，该能力目前主要用于 API/后台流程。

## 6.6 客服 AI 子系统（后端 API）

路由前缀：`/api/customer-service`

功能：

- 抓取 Buyer Messages（支持异步任务）
- AI 流水线处理（分类、情绪、风险、问题提取、场景回复）
- 自动回复引擎（低风险 + 指定类别自动发送）
- 人工编辑、审批、发送
- 状态流转：`new -> ai_generated -> auto_sent | waiting_review -> approved -> sent`

依赖：PostgreSQL + Redis + Celery + Amazon SP-API Token。

核心模块：

1. Message Sync
2. Message Storage
3. Message Classification
4. Sentiment Analysis
5. Risk Detection
6. Reply Generation
7. Auto Reply Engine
8. Human Review Interface
9. Message Send

---

## 7. API 清单（按域）

## 7.1 店铺与分析

- `GET /api/stores`
- `GET /api/stores/{store_id}/performance`
- `GET /api/stores/{store_id}/optimization-cases`
- `GET /api/stores/{store_id}/ad-group-recommendations`

## 7.2 Gemini（广告）

- `POST /api/stores/{store_id}/ai/whitepaper`
- `POST /api/stores/{store_id}/ai/advice`

## 7.3 白皮书管理

- `GET /api/stores/{store_id}/whitepaper`
- `POST /api/stores/{store_id}/whitepaper/import`
- `GET /api/stores/{store_id}/whitepaper/export`

## 7.4 领星同步

- `POST /api/lingxing/sync`

## 7.5 Context Package

- `POST /api/lingxing/context-package/jobs`
- `GET /api/lingxing/context-package/jobs/{job_id}`
- `GET /api/lingxing/context-package/jobs/{job_id}/download`
- `POST /api/lingxing/context-package/export`（同步版本，兼容保留）

## 7.6 运营策略（SQLite）

- `POST /api/ops/sync/incremental`
- `POST /api/ops/whitepaper/synthesize`
- `GET /api/ops/whitepaper/{store_id}`
- `POST /api/ops/advisory`

## 7.7 上传分析

- `POST /api/ai/upload-analysis`

## 7.8 客服 AI

- `POST /api/customer-service/messages/fetch`
- `GET /api/customer-service/messages`
- `POST /api/customer-service/messages/{message_id}/process`
- `POST /api/customer-service/messages/{message_id}/generate`
- `PATCH /api/customer-service/messages/{message_id}/reply`
- `POST /api/customer-service/messages/{message_id}/approve`
- `POST /api/customer-service/messages/{message_id}/send`

---

## 8. 配置项（`.env`）

完整示例见 `.env.example`。

### 8.1 Gemini

- `GEMINI_API_KEY`
- `GEMINI_MODEL`（默认 `gemini-2.5-flash`）
- `GEMINI_MAX_OUTPUT_TOKENS`
- `GEMINI_CONTINUATION_ROUNDS`

### 8.2 Lingxing

- `LINGXING_ERP_USERNAME`
- `LINGXING_ERP_PASSWORD`
- `LINGXING_APP_ID`
- `LINGXING_APP_SECRET`
- `LINGXING_BASE_URL`（默认 `https://openapi.lingxing.com`）
- `LINGXING_TIMEOUT_SECONDS`
- `LINGXING_MAX_RETRIES`
- `LINGXING_RETRY_BACKOFF_SECONDS`
- `LINGXING_TLS_MODE`（`default` / `tls1_2`）
- `LINGXING_PROXY_URL`
- `LINGXING_INSECURE_SKIP_VERIFY`（仅排障）

### 8.3 Context 导出

- `CONTEXT_EXPORT_MAX_WORKERS`（默认 1）
- `CONTEXT_EXPORT_RETENTION_HOURS`（默认 24）

### 8.4 运营分析

- `OPS_DB_PATH`（默认 `app/data/ops_data.db`）

### 8.5 客服 AI

- `CUSTOMER_SERVICE_DATABASE_URL`
- `CUSTOMER_SERVICE_REDIS_URL`
- `CUSTOMER_SERVICE_LLM_PROVIDER`（`gemini` / `openai`）
- `CUSTOMER_SERVICE_LLM_MODEL`
- `CUSTOMER_SERVICE_MAX_REPLY_CHARS`
- `CUSTOMER_SERVICE_SP_API_BASE_URL`
- `CUSTOMER_SERVICE_SP_API_ACCESS_TOKEN`
- `CUSTOMER_SERVICE_SP_API_MARKETPLACE_ID`
- `CUSTOMER_SERVICE_SP_API_LIST_MESSAGES_PATH`
- `CUSTOMER_SERVICE_SP_API_SEND_MESSAGE_PATH`
- `OPENAI_API_KEY`（当 provider=openai 时）

---

## 9. 本地开发与部署

## 9.1 本地启动

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

访问：`http://127.0.0.1:8000`

## 9.2 Docker（公网 8080）

```bash
cp .env.example .env
docker compose up -d --build
```

- Web：`http://<公网IP>:8080`
- 已包含服务：
  - `amazon-ads-analyze`（FastAPI）
  - `celery-worker`
  - `postgres`
  - `redis`

宿主机持久化：`./app/data:/app/app/data`

---

## 10. 脚本工具

- `scripts/lingxing_network_check.py`：领星网络/TLS/代理排障
- `scripts/lingxing_sync.py`：CLI 同步领星并输出 JSON
- `scripts/lingxing_context_package.py`：CLI 导出 Context Package
- `scripts/gemini_advice.py`：基于已保存白皮书生成建议（强校验 store_id）

---

## 11. 测试与质量

当前测试：

- `tests/test_context_export_jobs.py`
- `tests/test_ops_pipeline_unittest.py`

建议执行：

```bash
python -m compileall app tests
python -m unittest -q
```

---

## 12. 已知边界与风险（给项目排期用）

1. 前端与后端能力不完全对齐：
   - `/api/ops/*` 已实现，但前端按钮还未切到这套流程。
2. 客服 AI 目前是 API 能力：
   - 主站前端尚未有客服页面入口。
3. Context 导出任务是“进程内任务管理”：
   - 服务重启后任务状态会丢失（文件仍在磁盘）。
4. 领星网络依赖外网与白名单：
   - TLS/代理策略不通时会失败，需要网络层配合。
5. 当前存在双数据链路：
   - 旧链路（CSV）与新链路（SQLite）并存，后续可统一。

---

## 13. 建议下一步（产品/项目视角）

1. 前端接入运营白皮书链路：按钮改为 `ops/sync -> ops/whitepaper -> ops/advisory`。
2. 增加“运营白皮书/周期建议”可视化页面（结构化 JSON 卡片化，而非纯文本）。
3. 客服 AI 增加前端工作台（消息列表、编辑、审批、发送）。
4. 将 Context 异步任务迁移到 Redis/Celery，提升重启恢复能力。
5. 统一数据读取层，逐步从 CSV 迁移到 SQLite。

---

## 13. 用户与权限（新增）

本版本新增完整的多租户用户与店铺授权能力（FastAPI + PostgreSQL + JWT + bcrypt + Alembic）：

- 用户认证：注册、登录、JWT
- RBAC：`admin` / `manager` / `staff` / `viewer`
- 店铺授权：用户仅可访问授权店铺数据
- 多租户：核心表均带 `tenant_id`
- 客服消息：仅 `staff/manager/admin` 可访问，且按店铺授权隔离

### 13.1 新增核心表

- `tenants`
- `roles`
- `users`
- `stores`
- `user_store_mapping`
- `buyer_messages`

### 13.2 迁移

```bash
alembic upgrade head
```

### 13.3 启动

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

启动后会自动：

- 初始化 RBAC 表结构
- 初始化默认租户与默认管理员（由 `.env` 中 `BOOTSTRAP_*` 控制）
- 同步本地店铺目录到授权表

### 13.4 认证 API 示例

1. 登录获取 JWT

```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"account":"admin","password":"ChangeThisPassword123!"}'
```

2. 创建用户

```bash
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"staff1","email":"staff1@example.com","password":"StrongPass123!","tenant_id":1,"role":"staff"}'
```

3. 给用户授权店铺

```bash
curl -X POST http://localhost:8080/api/auth/users/2/stores \
  -H "Authorization: Bearer <JWT>" \
  -H "Content-Type: application/json" \
  -d '{"external_store_id":"store_a","store_name":"Store A"}'
```

4. 查询当前用户可见店铺

```bash
curl http://localhost:8080/api/auth/stores/me \
  -H "Authorization: Bearer <JWT>"
```

### 13.5 AI 客服 API（按店铺授权）

- 拉取消息：`POST /api/customer-service/stores/{store_id}/messages/fetch`
- 列表：`GET /api/customer-service/stores/{store_id}/messages`
- 生成回复：`POST /api/customer-service/stores/{store_id}/messages/{message_id}/generate`
- 编辑回复：`PATCH /api/customer-service/stores/{store_id}/messages/{message_id}/reply`
- 审批：`POST /api/customer-service/stores/{store_id}/messages/{message_id}/approve`
- 发送：`POST /api/customer-service/stores/{store_id}/messages/{message_id}/send`
- 审批并发送：`POST /api/customer-service/stores/{store_id}/messages/{message_id}/approve-send`

生成回复返回结构示例：

```json
{
  "category": "damage",
  "sentiment": "negative",
  "risk_level": "high",
  "product_issue": "broken leg",
  "reply": "Hello, we sincerely apologize..."
}
```
