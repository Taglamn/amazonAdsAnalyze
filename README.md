# Amazon Ads Analyze Dashboard (FastAPI + React + Tailwind)

一个多租户 Amazon 广告分析面板，包含：

- 每日表现数据看板（Date / Clicks / Spend / ACoS / Sales）
- 广告组调价建议（含 bid 建议）
- 调整历史与 7 天前后 ROI 影响分析
- 基于 Gemini API + 店铺 Playbook 的策略白皮书与建议生成

## 项目结构

- `app/main.py`: FastAPI 服务与 API 路由
- `app/data_access.py`: 多租户数据模型 `Store` 与仓储读取
- `app/analysis.py`: 逻辑对齐层，含 `analyze_impact`
- `app/gemini_bridge.py`: Playbook 加载、租户校验、Gemini 调用
- `app/static/index.html`: 前端页面容器 + Tailwind
- `app/static/app.js`: React 管理面板
- `scripts/gemini_advice.py`: 独立 AI 决策脚本
- `app/data/playbooks/store_playbook_{id}.json`: 店铺策略规则

## 快速启动

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 如果要调用 Gemini
export GEMINI_API_KEY="your_key_here"

uvicorn app.main:app --reload
```

打开：`http://127.0.0.1:8000`

## 关键 API

- `GET /api/stores`
- `GET /api/stores/{store_id}/performance`
- `GET /api/stores/{store_id}/ad-group-recommendations`
- `GET /api/stores/{store_id}/optimization-cases`
- `POST /api/stores/{store_id}/ai/advice`
- `POST /api/stores/{store_id}/ai/whitepaper`

AI 接口支持 `lang` 参数（`zh` / `en`），例如：

```json
{ "lang": "zh" }
```

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
