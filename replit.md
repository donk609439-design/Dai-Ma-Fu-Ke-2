# JetBrains AI Admin Panel Workspace

## Overview

pnpm workspace monorepo 1:1 复刻自 https://github.com/zzz609439-stack/Code-Replica-1。

一个完整的 JetBrains AI 账号管理与代理系统（中文界面）。

## 生产稳定性修复（2026-04-27）

### 一阶段修复：PoolTimeout 风暴

修复生产环境 `httpcore.PoolTimeout` 风暴（症状：`/admin/pending-nc` 卡 120s、`/key/discord-callback` 500、所有走全局 `http_client` 的接口逐步 500）。

根因：全局 `http_client = httpx.AsyncClient(timeout(read=None), max_connections=200)` + 长流式 SSE（如 claude opus thinking 模型）— 客户端断开 ASGI 连接后，`async for response.aiter_lines()` 在没有上游下一行字节前不会被取消，僵尸连接长时间占用池槽位，逐步耗尽 200 个连接。

修复（`jetbrainsai2api/main.py`）：
1. **3186 行 `http_client` 配置升级**：`read=None → 900.0`、`pool=5.0 → 30.0`、`max_connections=200 → 500`、`max_keepalive=50 → 100`；给僵尸连接一个最终回收兜底 + 池等待 30s 缓冲突发流量。
2. **457 行 `generic_exception_handler` 加 PoolTimeout 兜底**：识别 `httpx.PoolTimeout` / `httpcore.PoolTimeout` → 转 503 + `Retry-After: 5`（语义正确，便于客户端退避重试，不再 500 风暴）。
3. **3285 行 `_sse_with_keepalive` 加 `try/finally` 显式 `aclose`**：确保任何路径（正常/断连/异常）退出时显式 `await it.aclose()` 关闭下游生成器链；客户端断开时 starlette cancel stream task → GeneratorExit → finally → aclose → 上游 `async with http_client.stream(...)` 立刻退出释放连接。
4. **3765 行 `chat_completions`、4142 行 `messages_completions` 加 `http_request: Request` 形参**：传给 `_sse_with_keepalive(..., request=http_request)`。第三处调用（`/admin/activate`，8945 行）保持原签名向后兼容。

### 二阶段修复：撤销 ASGI 协议违规（关键回归）

第一次部署后用户报"能拉取到模型但是不出字"。根因：之前在 `_sse_with_keepalive` 的 25s 心跳分支里加了 `await request.is_disconnected()` —— 这会引入第二个并发的 ASGI `await receive()`，与 starlette `StreamingResponse` 内部的 `listen_for_disconnect` 协程抢消息，违反 ASGI 规范，导致整个 streaming task 在第一次心跳后被异常 cancel，于是流式响应"开了流不出字"。

修复：从 `_sse_with_keepalive` 移除 `is_disconnected()` 主动检测；保留 `request` 形参（兼容调用方）但不使用；保留 `try/finally + aclose`（无副作用，断连时 starlette 自己 cancel 仍会触发 GeneratorExit 走 finally，效果与主动检测等价但合规）。

经架构师审查通过（`evaluate_task` + `includeGitDiff`）。

### 三阶段修复：Express 代理超时升至 10 分钟

二阶段部署后用户再报"还是不出字"。根因（看生产日志直接命中）：
```
request aborted ... POST /v1/chat/completions ... statusCode=null responseTime=120002
```
**Node.js Express 代理层在 120 秒整点强制 abort 客户端**——`artifacts/api-server/src/routes/proxy.ts` 写死了 `proxyTimeout: 120_000` + `timeout: 120_000`，但下列场景轻松突破 2 分钟：
1. claude opus thinking 等慢推理模型首 token > 2 分钟；
2. `_stream_with_account_fallback` 在第一字节前需要切多个 JWT 失效账号（每个 JWT 刷新 ~10–30s，叠加 4 次即超 120s）；当前生产 grazie auth API 大量 401/500，触发概率极高；
3. SSE 心跳器每 25s 才发 keepalive 注释行（`: ping\n\n`），开始的几秒内若没数据 Node 仍把它当"无响应"。

与 Python 代码无关，纯 Node 代理配置问题。

修复（`artifacts/api-server/src/routes/proxy.ts`）：将 `pythonProxy` / `anthropicProxy` 的 `proxyTimeout` 与 `timeout` 从 `120_000` 升至 `600_000`（10 分钟），与 Python `http_client` 的 `read=900s` 对齐并保留 5 分钟余量。短请求自然在毫秒级关闭，对它们零影响；只把"长流式被错杀"这条路堵上。

最终生效改动总结：连接池配置升级 + PoolTimeout 转 503 + `_sse_with_keepalive` 的 finally aclose + Express 代理超时升至 10 分钟。

### 四阶段修复：启动检测减压（生产瘫痪止血）

三阶段部署后用户报"现在生产环境彻底炸了，根本进不去网站"。生产日志显示**所有路径**（`/v1/models`、`/admin/status`、`/key/usage`、`/admin/pending-nc` 等）均在 165–180s 后被 abort，不只是流式 chat。Python 后端整体瘫痪。

根因（不是代码 bug，是上游仓库设计 + 外部 API 异常的化学反应）：
1. 生产库 1864 个账号；
2. `_startup_quota_check`（main.py:2587）启动时**立即**对**全部 1864 个**做强制 JWT 刷新；
3. 当前 grazie auth API 大量 429/401，每次 JWT 刷新 hang 几秒到 timeout；
4. 这些 hang 住的请求把全局 `http_client` 连接池（500 connections）占满；
5. 真实用户的所有 Python 后端请求拿不到连接 → 全部 PoolTimeout / 卡死；
6. dev 环境同样代码、同样 1864 账号但不炸——因为 dev 没有真实用户流量挤压。

修复（`jetbrainsai2api/main.py:2590` `_startup_quota_check`）：
1. **延后开始**：函数体最前面 `await asyncio.sleep(300)`，让真实流量先获得 5 分钟连接池资源；
2. **降低并发**：`Semaphore(2) → Semaphore(1)`（串行化 JWT 刷新）；
3. **缩小分块**：`_CHUNK 20 → 10`；
4. **拉长块间休息**：`asyncio.sleep(0.3) → 1.5`。

附带：已移除旧的后台扫描机制，避免额外定时扫描与启动检测争用连接池。

代价：账号 has_quota 状态收敛变慢（约 30 → 60 分钟），但绝不会再因启动瞬间打爆连接池而瘫痪整个 Python 后端。

架构师审查指出**残余风险**：`get_next_jetbrains_account` 内的 `_first_ready` 仍在候选组上"全量并发探测 + 找到 winner 后不取消其余"，在 JWT 普遍陈旧时可触发大规模并发刷新。后续若复发，需要改为"有上限并发 + 早停取消"。本次按用户"最简改动"原则未动。

最终生效改动总结：上述三阶段 + 启动检测减压 + 后台扫描错开。

### 五阶段修复：`_first_ready` 滑动窗口分批并发（彻底拆 JWT 刷新洪水）

四阶段部署后用户再报"还是不出字"。生产库实查（重点：dev 1864、生产 **20534**，差 11 倍）：
- has_quota=True：19752、has_quota=False：811、no_jwt：69
- **last_quota_check 已 stale（>20 分钟）：19681（95.7%）**
- old_jwt_24h：179

部署后 `_startup_quota_check` 已成功延后 5 分钟（生效 ✓），但日志显示**第一个 chat 请求**（1777264602759）瞬间引爆几十条"正在为 licenseId XXX 刷新 JWT..."日志——正中四阶段架构师警告的残余风险点。

根因（main.py:2222 `_first_ready`，旧版）：
```python
tasks = [asyncio.create_task(_probe(acc)) for _, acc in group]
await done_event.wait()
return winner[0] if winner else None
```
- `group2`（公共池）在生产可达 **1.9 万账号**；
- 一次性 `create_task` 全部候选，每个 `_try_account` 内若 jwt stale（>12h）会触发 `_refresh_jetbrains_jwt`；
- 即使 winner 已选出，剩余上万 task 仍在后台并发刷 JWT → grazie auth 429 + 全局 `http_client` 连接池（500 connections）被全部占满 → Python 后端整体瘫痪。

修复（`jetbrainsai2api/main.py:2225` `_first_ready`）：**滑动窗口分批并发**。
- 新增 `batch_size=8` 默认参数；
- 外层 `for batch_start in range(0, len(group), batch_size)` 循环；
- 每批 8 个候选并发探测，逻辑与原版完全一致（done_event + winner + remaining）；
- 找到 winner 立即返回（该批最多遗留 7 个后台 task，可控）；
- 该批全失败 → 进入下一批。

效果：单次请求并发 JWT 刷新数从 ~1.9 万 收敛到 **≤ 8**；正常场景（has_quota=True 占 96%）winner 通常在第一批就出现。

架构师审查结论（evaluate_task + git diff）：
- 正确性 ✓：可正确返回 winner / None；nonlocal remaining 闭包绑定安全；winner/done_event 仅 append/方法调用不重绑定。
- 语义有意改变：从"组内全员竞争 first-ready"变为"按批顺序首个成功的批次胜"，引入顺序偏置；但这是**有意降载**，架构师同时确认"该改动直接切断'首个 chat 触发万级 create_task'的根因，是当前止血链路里最关键的一环"。
- 后续优化建议（按"最简改动"原则本次未做）：
  1. `_refresh_jetbrains_jwt` / `_check_quota` 外层加全局 `asyncio.Semaphore`（如 20–50），防多请求叠加击穿连接池；
  2. `_first_ready` 加"组级时间预算 / 最大批次数"，避免 group2 长时间失败时拖慢进入 group3；
  3. 若需完全保留原语义，可改为"有界并发竞争池（固定窗口补位）"代替严格按批串行。

最终生效改动总结：上述四阶段 + `_first_ready` 滑动窗口分批并发。

## LOW 专用 DB Pool 资源隔离（2026-04-27）

工单目标：**资源隔离，不限速**。LOW 批量激活写库走独立 DB Pool，防止挤占普通用户/admin 的主 DB Pool。

### 改动清单

**改动1：新增 `DB_POOL_LOW` 和 `_get_low_db_pool()`**
- 全局变量 line ~90：`DB_POOL_LOW: Optional[Any] = None`
- `_get_db_pool()`：max_size 从 20 → **15**（主池），command_timeout 30 → 60，statement_cache_size=0
- 新增 `_get_low_db_pool()`：min_size=1，max_size=**20**（LOW 专用），懒加载，失败返回 None

**改动2：shutdown 关闭 LOW Pool**
- `shutdown()` 追加 `if DB_POOL_LOW: await DB_POOL_LOW.close()`

**改动3：三个写库 helper 支持显式 pool 参数**
- `_save_account_to_db(account, pool=None)` — pool=None 时走 `_get_db_pool()`
- `_upsert_key_to_db(key, meta, pool=None)` — 同上
- `_delete_key_from_db(key, pool=None)` — 同上

**改动4：`admin_activate_stream` event_generator**
- 添加 `_is_low_task = bool(task.get("is_low_admin"))` 和嵌套 `async def _get_pool_for_task()`
- event_generator 内所有 `_get_db_pool()` 调用（`_cdb`、`_db_pool`、`_db2`、`_db3`、`_dup_db`、`_dup_db2`、`_db_p`）→ `_get_pool_for_task()`
- `_save_account_to_db(new/extra_account_obj, pool=await _get_pool_for_task())`

**改动5：`_retry_pending_nc_lids` Phase 3 按行选 pool**
- Phase 2（trusted 入池）：`_save_account_to_db(new_acc, pool=await _get_low_db_pool() if row_is_low else await _get_db_pool())`
- Phase 3 循环顶部：`_row_pool = await _get_low_db_pool() if row_is_low else db`
- Phase 3 内所有 `db.acquire()` / `await _get_db_pool()` → `_row_pool.acquire()` / `_row_pool`

**改动6：`/admin/status` 增加两个 pool 状态字段**
- `db_pool_main: {size, free, max=15}`（DB_POOL not None 时）
- `db_pool_low: {size, free, max=20}`（DB_POOL_LOW not None 时）
- LOW 池首次使用前为 null（懒加载），触发一次 LOW 激活后初始化

### 资源模型（修改后）

```text
普通用户 / admin / chat / 主后台任务 → DB_POOL_MAIN (max=15)
LOW 激活 / LOW pending retry        → DB_POOL_LOW  (max=20)
总连接数：约 35（原 20）
```

### 未改动
- `_get_low_executor()` LOW Worker 池不变
- 无 LOW 全局限速
- `_add_low_quota(api_key, ids, pool)` 已支持显式 pool，不需改

### 架构师审查结论

审查发现两个 Bug 已同步修复：

**Bug 1（严重）**：`shutdown()` 仅关闭了 LOW pool，丢失了主池 `DB_POOL` 的关闭逻辑 → 已补全 `if DB_POOL: await DB_POOL.close()`。

**Bug 2（崩溃风险）**：`_retry_pending_nc_lids` Phase 3 中 `_row_pool = await _get_low_db_pool() if row_is_low else db`，如果 LOW pool 初始化失败返回 None，后续 `_row_pool.acquire()` 会崩溃 → 已在 `_row_pool` 赋值后加 `if not _row_pool: continue` 保护。

**后续修改（2026-04-27）**：`_LOW_USER_INPUT_TOKENS` 改为 `2_000_000_000`（实际无限制），LOW 用户输入不再受长度限制。

**低优先级（未改，可接受）**：
- `_get_db_pool()` / `_get_low_db_pool()` 懒加载存在多协程竞争初始化，极低概率创建多余连接池（GC 最终回收）；startup 初始化可彻底消除此问题，但超出本工单最简改动原则。
- `/admin/status` pool 状态字段在 5s 缓存 TTL 内可能显示 null（LOW 池懒加载），可接受。

## LOW 用户 AI 响应缓存（2026-04-27）

工单级别：LOW。仅对 `is_low_admin_key=true` 的 key 生效。

### 数据库表

`ai_response_cache`（cache_key TEXT PK、scope、owner_discord_id、client_key、route、model、request_hash、response_json JSONB、prompt_tokens、completion_tokens、hit_count、created_at、expires_at）+ 两个索引。

`ai_idempotency_cache`（idempotency_key + owner_discord_id 复合 PK、client_key、body_hash、response_json JSONB、created_at、expires_at）+ 过期索引。

### 缓存条件（`_is_low_cacheable_request`）

stream=False、temperature≤0.3、无 tools/tool_choice/functions/function_call、无多模态 content list、body≤128KB，且调用方为 LOW key。

### 接入端点

- `/v1/chat/completions`（非流式路径）：缓存读取在 `get_next_jetbrains_account` 之前，写入在 `_append_log` 之后、`return resp` 之前（用 `resp.model_dump()` 序列化 Pydantic 对象）。
- `/v1/messages`（非流式路径）：缓存读取在 `get_next_jetbrains_account` 之前（基于 Anthropic 原始请求体），写入在 `convert_openai_to_anthropic_response` 之后（用 `anthropic_response.model_dump()`）。

### Idempotency-Key

同 LOW 用户同 key 同 body_hash → 直接 replay（HTTP 200）；同 key 不同 body_hash → 409。

### TTL

temperature=0 → 86400s（24h）；其他 → 3600s（1h）。

### 缓存命中行为

命中不调 `_consume_key_usage`，调用日志以 `exempt=True` 写入（不计费），响应体附加 `"cached": true, "cache_hit": true`。

### 后台清理

`_cleanup_ai_response_cache_loop`：每小时对两张表 DELETE WHERE expires_at < now()，在 `startup()` 中注册为 asyncio task。

### 架构师审查结论

- `_is_low_cacheable_request` 基本覆盖（细微遗漏：Anthropic `system` 字段可能为多模态 list，但极少见，风险低）。
- `_ai_cache_set` asyncpg JSONB 传递（`json.dumps` + `::jsonb` 强转）可行。
- **修复 Bug**：`chat_completions` 缓存写入原先 `isinstance(resp, dict)` 永远为 False（`resp` 是 `ChatCompletionResponse` Pydantic 对象）→ 已改为 `resp.model_dump()`。
- cleanup loop 正确清理两张表。

## 排队记录入队超时自动清理（2026-04-27）

新增机制：**超过 1 小时仍在排队的邮箱，自动从排队列表清除**。

实现位置：`jetbrainsai2api/main.py` `_retry_pending_nc_lids` 后台任务（每 5 分钟一轮）每轮重试**之前**先执行一次清理 SQL。

关键设计：
- **专用入队时间字段** `jb_accounts.pending_nc_enqueued_at`（DOUBLE PRECISION DEFAULT 0，epoch 秒）。**仅在入队瞬间写入，重试与 JWT 刷新都不会更新**，避免了一开始用 `last_updated` 的两个语义偏差（INSERT ON CONFLICT 旧时间戳误删 + JWT 刷新刷新 `last_updated` 延后清理）。
- schema 变更随 `init_db` ALTER TABLE IF NOT EXISTS 一并下发；同时一次性回填存量 pending 行（enqueued_at=0 → NOW()），让历史数据"从启动时刻开始计时"，不会被立即误判超时。
- 三处入队点全部同步写入 `time.time()`：批量激活 done 路径（line 8895）、批量激活 pending 路径（line 8922）、单激活 INSERT ON CONFLICT 路径（line 9029/9039/9057，且写入 EXCLUDED 列表保证冲突更新也刷新）。
- 阈值常量：`_PENDING_NC_MAX_AGE_SEC = 3600`（1 小时）。
- 自动清理（`_retry_pending_nc_lids` 每轮重试前执行）与手动清除（`admin_pending_nc_delete`）使用**完全相同**的字段重置：`pending_nc_lids = pending_nc_key = pending_nc_bound_ids = NULL`，`pending_nc_enqueued_at = 0`（避免历史脏值）。
- 自动清理通过 `RETURNING id, pending_nc_email, pending_nc_low_admin` 拿到记录，逐条写入 `_pending_nc_retry_log` / `_pending_nc_retry_log_low`，前端排队记录页（`PendingQueue.tsx`）自动可见。LOW 用户日志使用脱敏邮箱前缀 + 友好文案（"已自动取消激活"）。
- 清理代码包在 `try/except` 内，失败仅 print 不影响主重试循环。

经架构师两轮审查（第一轮指出 `last_updated` 不可靠 → 引入专用字段后第二轮 PASS）。

代价：被清理的邮箱后续若想继续激活需要用户手动重新发起激活流程。

## 复刻状态（2026-04-26）

源码已 1:1 复刻完成，三个工作流均正常运行：
- admin-panel（端口 20130，路径 /admin-panel/）渲染中文管理员登录页
- api-server（端口 8080）通过 spawn 子进程启动 Python 服务并代理转发
- jetbrainsai2api（Python FastAPI，端口 8000）已连数据库并就绪

### 数据库初始化注意

当前 Python 后端使用 PostgreSQL（`asyncpg`）作为主要持久化存储，表结构由 `jetbrainsai2api/main.py` 的 `_ensure_db_tables()` 在启动时通过 `CREATE TABLE IF NOT EXISTS` / `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` 自动补齐。

历史版本曾存在 `pending_nc_key` 迁移顺序问题：在添加 `jb_accounts.pending_nc_key` 列之前就执行了引用该列的回填 SQL。当前实现已修复：`pending_nc_key` 等 pending-NC 字段会先通过 `ALTER TABLE` 添加，再执行相关回填逻辑。因此全新 PostgreSQL 数据库不再需要手工预建 `pending_nc_key` 列。

重置数据库时只需确保：
1. `DATABASE_URL` 指向可连接的 PostgreSQL 数据库；
2. 启动应用，让 `_ensure_db_tables()` 自动创建/迁移表；
3. 如需保留生产数据，务必先使用 `/admin/db-export-stream` 导出 NDJSON 备份。

### 必需的环境变量

要使代理服务真正可用，需配置：
- `ADMIN_KEY` — 管理面板登录密钥
- `jetbrainsai2api/jetbrainsai.json` — JetBrains AI 账号配置
- `jetbrainsai2api/client_api_keys.json` — 客户端 API 密钥

未配置时服务仍会启动但接口返回 503/未配置错误。

## Architecture

### Artifacts

- **admin-panel** (`artifacts/admin-panel/`) — React + Vite frontend, port 20130, path `/admin-panel/`
- **api-server** (`artifacts/api-server/`) — Express API server, port 8080, path `/`
  - Proxies AI requests to Python FastAPI backend on port 8000
  - Spawns `jetbrainsai2api/main.py` via uvicorn at startup

### Key Directories

- `lib/db/` — `@workspace/db` — Drizzle ORM PostgreSQL client
- `lib/api-zod/` — `@workspace/api-zod` — Zod schemas for API validation
- `lib/api-client-react/` — `@workspace/api-client-react` — React API client
- `jetbrainsai2api/` — Python FastAPI backend for JetBrains AI activation
- `cf-worker.js` — Cloudflare Worker for CF proxy pool

### Frontend Pages (admin-panel)

Dashboard, Accounts, ApiKeys, Models, Stats, Logs, Docs, Prizes, Partners, DonatedAccounts, ProxyPool, Activate, PendingQueue, SelfRegister, KeyUsage, Backpack, Donate, Lottery

## Tech Stack

- **Frontend**: React 19, Vite 7, TailwindCSS 4, shadcn/ui, wouter, @tanstack/react-query
- **API Server**: Node.js + Express 5, pino logging, http-proxy-middleware
- **Python Backend**: FastAPI, uvicorn, httpx, cryptography
- **Database**: PostgreSQL（Node.js 侧通过 Drizzle ORM；Python 侧通过 asyncpg）
- **Language**: TypeScript + Python 3.11

## Environment Variables

- `ADMIN_KEY` — Admin authentication key
- `LOW_ADMIN_KEY` — Secondary admin key (low_admin role) — limited /admin/* whitelist (status, activate, low-cf-proxies, low-config, pending-nc/low, cf-proxies/test)
- `DATABASE_URL` — PostgreSQL connection string
- `SESSION_SECRET` — Session secret
- `PORT` — Server port (8080 for API server, 20130 for admin panel)

## LOW_ADMIN Subsystem

LOW_ADMIN_KEY users get a separate, isolated activation flow with their own per-tier limits:

- **Per-tier limits** — Constants in `main.py`: `_NORMAL_KEY_QUOTA=25`, `_LOW_USER_KEY_QUOTA=16`, `_LOW_USER_INPUT_TOKENS=2_000_000_000`（实际等同 LOW 输入无限制）, `_LOW_USER_OUTPUT_TOKENS=40_000`. Helpers `_key_tier()` and `_key_tier_limits()` resolve a key → its tier limits. `/v1/chat/completions` and `/v1/messages` enforce token caps per tier; `_activate_key_quota` upgrades quota to 16 (LOW) or 25 (normal) based on the `is_low_admin_key` column.
- **Per-key tagging** — `jb_client_keys.is_low_admin_key` (bool) marks LOW-issued keys at creation; load/upsert/bulk-save preserve it; cleanup uses `usage_limit > 0` (no longer hard-coded 25).
- **Dedicated CF pool, sharded by Discord ID** — `cf_proxy_pool` rows with `owner='low_admin'` are further split by `owner_discord_id` (TEXT NOT NULL DEFAULT ''). The unique key is `(url, owner, owner_discord_id)`. `jb_activate.LOW_CF_PROXY_POOL` is now `Dict[discord_id, list[url]]` and `_low_proxy_idx` is `Dict[discord_id, int]`; `_set_proxy_pool_context(use_low, discord_id)` is thread-local so `_get_proxy_url()` picks the LOW user's own sub-pool. `process_account(..., use_low_pool=True, low_discord_id=...)` propagates the routing key end-to-end. Pending-NC rows store the originating discord ID in `jb_accounts.pending_nc_discord_id`; `_retry_pending_nc_lids` reads it back and routes auto-retries to the matching sub-pool.
- **Forced Discord auth for LOW users** — `/admin/activate` and `/admin/activate-batch` now require LOW users to send a Discord-verified `discord_token` (same gate as guests; only the daily-20 cap is skipped for LOW). The verified Discord user_id becomes `low_discord_id` and is persisted alongside every pending-NC row, so auto-retries always replay through the same Discord sub-pool.
- **Per-Discord LOW pool admin sub-page** — `/admin/low-cf-proxies` is owner-aware: full admins see every sub-pool grouped by Discord ID and may target any bucket via `?discord_id=` (GET) or `discord_id` body field (POST). LOW users must send `X-Discord-Token` and are hard-pinned to their own bucket. New admin page `LowCfPoolAdmin.tsx` (route `/proxy-pool/low-users`) renders all sub-pools collapsibly. The LOW-user page `LowCfPool.tsx` shows a Discord login gate first, then injects the Discord token into every request.
- **Single executor for ALL LOW work** — `_low_executor` is a single shared `ThreadPoolExecutor` returned by `_get_low_executor()`. Both single LOW activation and batch LOW activation submit to it, so the global concurrency setting throttles BOTH paths. `_reset_low_executor()` is invoked on `PATCH /admin/low-config` to rebuild the pool with the new size.
- **Batch activate** — `POST /admin/activate-batch` accepts up to 50 `email:password` lines, enforces a 1 hour cooldown between batches, runs through `_get_low_executor()`. Each account gets a pre-issued NC key tagged `is_low_admin_key=true` and a normal `task_id`/SSE stream just like single activation.
- **Pending-NC retry concurrency** — `_retry_pending_nc_lids` uses TWO semaphores (`sem_main=10`, `sem_low=_low_admin_concurrency`); LOW rows compete only against other LOW rows. Pending-NC rows carry `pending_nc_discord_id` so retries replay through the correct Discord sub-pool.
- **LOW-only retry log** — `_pending_nc_retry_log_low` deque accumulates retry events for LOW rows only; surfaced via `logs_low` field on `/admin/pending-nc` (admin) and `/admin/pending-nc/low` (LOW or admin).
- **LOW retry log disguise** — `_log(msg, level, is_low, low_msg=None)` accepts an optional `low_msg`: when `is_low=True`, the LOW deque entry uses `low_msg` (falls back to `msg`). All 17 LOW-path log sites in `_retry_pending_nc_lids` now pass `low_msg` to hide internal terms (`trusted/Untrusted/licenseId/lid/信任凭证/NC/492/批量/LOW`) and re-phrase events to mimic the friendly tone of single-account activation SSE logs (e.g. `✓ {short_email} 账号激活成功，已入池`, `⏳ {short_email} 账号验证中，请稍候`, `🎉 {short_email} 激活成功！密钥额度已增加 +16（当前总额度 N）`). The Phase-2 result loop pre-extracts `_se = meta.get("short_email", "账号")` so per-result `low_msg` strings can use the email prefix instead of `lid`. Admin-side logs (`is_low=False/None`) still receive the original detailed `msg` (with `lid/HTTP code/exception text`) for troubleshooting; `_pending_nc_retry_log` (main deque) is unchanged.
- **Configuration** — `GET/PATCH /admin/low-config` returns/updates `concurrency` (1–50), persisted in the `jb_settings` k/v table and reloaded on startup.
- **Personal key accumulation** — `GET/POST/DELETE /admin/low-user-key`: LOW users create exactly ONE personal API key (stored in `jb_client_keys` with their `low_admin_discord_id`). All subsequent single and batch activations accumulate quota (+16 each) to that key instead of generating a new pre-issued key. Helper `_get_low_personal_key(discord_id)` / `_add_low_quota(api_key, new_acc_ids)` manage this in the stream handler and pending-NC path.
- **Personal key export / import** — `GET /admin/low-user-key/export` returns the user's personal key as a JSON payload (`version`, `owner_discord_id`, `key`, `usage_limit`, `usage_count`, `account_id` CSV, banned flags, plus an HMAC-SHA256 `signature`). `POST /admin/low-user-key/import` restores it. Security invariants enforced server-side:
  1. **HMAC signing (anti-forgery)** — Helper `_low_key_export_sign(payload)` computes HMAC-SHA256 over the canonical JSON of `_LOW_KEY_EXPORT_SIG_FIELDS` using **`SESSION_SECRET` only** (no fallback — fails closed if env var is missing). Import rejects (400) any payload with missing or mismatched signature via `hmac.compare_digest`, blocking client-side forgery of `usage_limit` / `usage_count` / `owner_discord_id` etc. Migrating between deployments requires the same `SESSION_SECRET`.
  2. **Auth gate** — Helper `_verify_low_user_discord(request)` requires `LOW_ADMIN_KEY` + a valid `X-Discord-Token` (admin key is rejected with 403).
  3. **Owner consistency (defense-in-depth)** — Even after signature verification, the JSON's `owner_discord_id` must equal the verifying Discord ID (403).
  4. **Per-Discord asyncio lock** — Helper `_get_low_user_key_lock(discord_id)` returns a per-user `asyncio.Lock`; the entire check-then-insert-then-audit sequence runs inside it. Concurrent imports for the same Discord ID are serialized — exactly one succeeds, the rest get 409.
  5. **Pre-existence guard** — Import is rejected (409) if the user already holds a personal key (must DELETE first), or if the key string already belongs to a different Discord ID (409).
  6. **Anti-replay audit (persistent)** — The `jb_settings` k/v table holds an audit row keyed `low_audit:{discord_id}` containing `last_usage_count`, `last_usage_limit`, `banned`, `banned_at`. Helpers `_low_audit_load` / `_low_audit_save` maintain it with monotonic-max merge for usage and sticky-True for banned (`_low_audit_save` itself wraps its load→merge→upsert in a separate `_low_audit_save_locks` per-Discord lock so concurrent saves cannot clobber each other). Write sites: `low_user_key_create` (baseline 0/0/false), `_add_low_quota` (every quota accumulation), `admin_ban_key` (manual ban), and `low_user_key_delete` (snapshots current `usage_count`/`usage_limit` before removal so delete-then-import can never roll back quota). The audit row is **NOT deleted on DELETE**. On import: (a) reject 409 if `payload.usage_count < audit.last_usage_count` (prevents quota rollback), (b) reject 409 if `audit.banned == True && payload.banned == False` (prevents self-unban via pre-ban backup), (c) compute `effective_banned = payload.banned OR audit.banned` so a ban is never lost. Ban-import mutex: helper `_persist_low_user_ban(discord_id, ts)` acquires the per-Discord import lock then writes audit AND re-applies `banned=True` to the current in-memory key (and `_upsert_key_to_db`), so a ban happening concurrently with an import can never produce a runtime state where audit says banned but the active key is unbanned. `admin_ban_key` awaits this helper synchronously. `low_user_key_delete` itself runs under the same per-Discord import lock for full mutex with import / ban paths.
  7. **Format / version guards** — `key` is regex-validated `[A-Za-z0-9_\-\.]{8,128}` (400); `version` > `_LOW_KEY_EXPORT_VERSION` (currently 1) is rejected (400).
  
  On success, the key is restored with `is_low_admin_key=True` forced and the audit row updated. UI: `LowPersonalKey` panel adds three buttons — **导出备份** (downloads `jbai-personal-key-YYYY-MM-DD.json`), **删除并重置** (with confirm dialog), and **从备份导入** (hidden `<input type=file>` → POST).
- **UI** — `Activate.tsx` renders a `LowPersonalKey` panel (LOW users see it after Discord login; shows key + usage stats + copy button), a `LowConcurrencyConfig` panel (visible to admin + low_admin), and `LowBatchPanel`. The concurrency input lives ONLY in the global panel. Tier-aware quota labels show "额度 16" / "额度 25". `PendingQueue.tsx` shows two tabs. Admin sees all LOW sub-pools grouped by Discord ID at `/proxy-pool/low-users` (`LowCfPoolAdmin.tsx`). LOW users manage their own sub-pool at `/my-cf-pool` (`LowCfPool.tsx`), which gates on Discord login and displays the user's Discord tag + ID in the header.

## Call Logs & Usage Exemption

- **Call log buffer** — `_call_logs` is an in-memory `deque(maxlen=500)`. Each entry now carries `discord_id` (resolved from `VALID_CLIENT_KEYS[key].low_admin_discord_id` at append time) and `exempt: bool`.
- **`GET /admin/logs`** — admin sees ALL logs; LOW user (`X-Admin-Key=LOW_ADMIN_KEY` + `X-Discord-Token`) sees only logs whose `discord_id` matches their verified Discord user_id (401 if no Discord verification). `DELETE /admin/logs` mirrors the same scoping (LOW only erases own rows).
- **Path whitelist** — `/admin/logs` added to `_LOW_ADMIN_ALLOWED_PREFIXES`; the endpoint internally re-verifies Discord, so LOW users cannot bypass scoping by omitting the header.
- **Frontend `Logs.tsx`** — admin-mode shows a `Discord` column; LOW-user mode renders a Discord-login gate first and auto-injects `X-Discord-Token` on every request. Added a "计费 / 豁免" column (amber badge for exempt rows). LOW users get a `/logs` nav entry under their sidebar (`lowAdminNavItems`) and a matching `<Route path="/logs">` in `LowAdminRoutes`.
- **Real token capture in streaming** — `openai_stream_adapter` accepts an optional `_usage_capture: Dict[str, int]` and writes the FinishMetadata-derived `prompt_tokens` / `completion_tokens` into it. `_tracked_stream` and `_stream_with_key_consume` now share the same `usage_capture` dict so log entries and consumption decisions both use real upstream tokens (falling back to char-based estimation when FinishMetadata is missing).
- **Usage exemption (applies to ALL keys)** — `_USAGE_EXEMPT_INPUT_TOKENS = 200`, `_USAGE_EXEMPT_OUTPUT_TOKENS = 200`; helper `_is_call_exempt(p, c)` returns `True` only when BOTH are below the threshold. Streaming consumption was refactored: `_stream_with_key_consume` no longer fires on the first content chunk — the entire `async for` is now wrapped in `try/finally` so the post-loop billing decision (consume vs exempt) ALSO runs when the client disconnects mid-stream (GeneratorExit / CancelledError), preventing billing-bypass via early disconnect. The inner `_consume_key_usage` call is itself wrapped in `try/except` so a billing failure cannot block resource cleanup. Non-streaming paths (`/v1/chat/completions` and `/v1/messages`) gate `_consume_key_usage` purely on `not is_exempt` (the previous `completion_text` precondition was removed because tool_calls-only responses are legitimate billable calls). All paths write `exempt=is_exempt` into the log entry. The exemption applies to non-LOW keys too — small probe/heartbeat-style calls do not count toward `usage_count`.
- **Token source parity** — `_tracked_stream` (which writes log rows) and `_stream_with_key_consume` (which charges) BOTH parse inline SSE `usage` chunks and use the same priority order: `FinishMetadata capture > inline SSE usage > char-based estimation`. This guarantees the token counts shown in 调用日志 match the tokens used for billing decisions, even in the fallback-estimation path.
- **管理面板"次级管理员 key"分组（`/admin/keys` + ApiKeys.tsx）** — `GET /admin/keys` 的 `keys_with_meta` 现在额外返回 `is_low_admin_key: bool` 与 `low_admin_discord_id: str`。前端 `ApiKeys.tsx` 在 useMemo 中先把 `is_low_admin_key=true` 的密钥从 `normalKeys/multiKeys/pendingKeys` 中剔除，单独装入 `lowAdminKeys` 并按 `low_admin_discord_id` 进一步分组；UI 上以橙色 `UserCog` 图标的折叠区域呈现，每个 Discord 用户一张子卡片，显示该用户名下密钥总数与累计 `已用/上限` 次数。搜索框现在也按 `low_admin_discord_id` 匹配。所有 LOW 密钥创建路径均已通过 `_admin_cache_invalidate("keys", "status")` 失效缓存（main.py 6173/6206），因此新建密钥后 15s TTL 不会出现脏数据。
- **Anthropic 流式计费修复（`/v1/messages` stream）** — `openai_to_anthropic_stream_adapter` 之前 `message_start` 与 `message_delta` 的 `usage` 都硬编码为 `{input_tokens: 0, output_tokens: 0}`，违反 Anthropic 规范且让客户端无法读到真实计费。现在签名扩展为 `(openai_stream, model_name, usage_capture, est_prompt_tokens)`：`message_start.usage.input_tokens` 用 `est_prompt_tokens`（与 chat_completions 保持一致），`message_delta.usage` 优先取 `usage_capture` 中由 `openai_stream_adapter` 写入的 FinishMetadata 真实 token；若上游未给（usage_capture 为空），则回退用 `est_prompt_tokens` 作 input、按 `len(content_text)//4` 估算 output（保证非零）。`messages_completions` 调用处同时传入 `usage_capture` 与 `prompt_tokens` 以打通这一链路。`/v1/chat/completions`（流式 + 非流式）以及 `/v1/messages` 非流式此前已通过 `aggregate_stream_for_non_stream_response` 与 `convert_openai_to_anthropic_response` 正确返回 `usage`，本次只补齐了 Anthropic 流式这条最后的漏洞。
- **LOW 预签 key 的 Discord 归属修复（架构师建议）** — `/admin/activate` 与 `/admin/activate-batch` 在为 LOW 用户预签发 0 额度 key 时，过去只写了 `is_low_admin_key=True` 但漏写 `low_admin_discord_id`，导致管理面板"次级管理员 key"分组里这些预签 key 被错误归入"未知"用户。现在两个路径都把 `dc_user_id` 写入 `low_admin_discord_id` 字段，并在写入 `VALID_CLIENT_KEYS` 后立即调用 `_admin_cache_invalidate("keys", "status")`，避免 15s TTL 下管理员看到陈旧分组。
- **Anthropic 流式 input_tokens 单调性保证** — `openai_to_anthropic_stream_adapter` 的最终 `message_delta.usage.input_tokens` 现在会被 clamp 到 `>= initial_input`（即 `message_start.usage.input_tokens` 用的估算值），防止上游 FinishMetadata 报告的 prompt_tokens 小于估算值时让严格的 Anthropic 客户端看到"input_tokens 中途下降"的非单调序列。单元测试 TEST C/D/E 全部通过：clamp 生效（10→100）、不必要时不 clamp（200 保持 200）、fallback 不为零。
- **激活流程 5 处直连改走 CF 代理池修复（2026-04-26）** — 此前 `jb_activate.py` 中虽然有 `_cf_post` wrapper，但仅 `oauth_pkce` 的 token 兑换、`register_grazie`、`get_jwt` 主路径使用了它。**5 个业务函数完全绕过代理池直接 `requests.post/get`**，导致批量激活时同一 IP 高频打 `account.jetbrains.com/lservice/*` 与 `api.jetbrains.ai/auth/*`，触发 429 限流。单账号最多 ~50 次直连请求，10 个账号并发激活 = 500 次同源直连。
  - **新增 `_cf_get(url, **kwargs)` wrapper（jb_activate.py 157）**：与 `_cf_post` 配套；GET 的 query string 用 `requests.models.PreparedRequest.prepare_url(url, params)` 预先拼接到 URL 后再传给 worker（worker 端按 `x-target-url` 透传），与原 `requests.get(url, params=...)` 行为等价。无代理时降级直连。
  - **5 处替换**：(a) `obtain_trial` 的 `obtainTrial.action`（line 348）→ `_cf_get`，每次激活循环 6 次；(b) `create_nc_licenses` 的 6 个 IDE `obtainFreeLicense.action`（line 773）+ AIP `obtainLicense.action`（line 797）→ `_cf_get`；(c) `get_jwt_from_ides_endpoint._try_ides`（line 586）→ `_cf_post`，每个 IDE 试 5 种格式；(d) `get_jwt_from_grazie_lite` 的 grazie-lite POST（line 835）+ v2 POST（line 853，含 4 次 429 重试）→ `_cf_post`；(e) `get_jwt_multiformat._try`（line 899）→ `_cf_post`，循环 7+ 种格式。所有 5 处都是高频且打的就是 CF 严限的两个域。
  - **未改动**：(1) `jba_login` 末尾主动卸载 `CFProxyAdapter` 的逻辑——OAuth PKCE 重定向链（`oauth_pkce` line 280 `s.get(allow_redirects=False)`）必须 Python 端 hop-by-hop 跟踪 Location，而当前 Worker `redirect: "follow"` 写死会把链吃掉。(2) 因此被一并波及的 `check_ai_status` / `fetch_aip_license_tokens` / `extract_license_ids` HTML 兜底 / `change_password_with_session` 仍然直连——需要后续 Worker 端加 `x-redirect-mode: manual` 支持才能彻底走代理。
  - **预期效果**：单账号直连请求数从 ~50 降到 ~16（仅剩 OAuth 重定向链 + 少量 session 调用）；高危的 `api.jetbrains.ai` 三个端点（grazie-lite、ides/provide-access、multiformat）全部走 worker 分散 IP，与已经走代理的 `register_grazie`/`get_jwt` 行为一致。架构师审查通过：批量场景从"直连流量为主"反转为"绝大多数走代理 + 自动降级"，且 `_cf_get` 的 PreparedRequest 拼接行为与原生 requests 等价、未引入安全风险。
- **OpenAI/Anthropic tools / function calling 三处缺口修复（2026-04-26）** — 此前的 `/v1/chat/completions` 与 `/v1/messages` 在以下 3 处与 OpenAI/Anthropic 协议存在偏差，已端到端测试修正：
  1. **多 tool_calls 全发送 + tool_choice 过滤** — 抽出共享辅助 `_convert_openai_messages_to_jetbrains(messages)`（main.py 3282）：assistant 消息若带多个 `tool_calls`，按 OpenAI 规范应作为多条独立 JetBrains `assistant_message` 下发（每条带一个 `functionCall`），第一条携带原 `content`、其余 `content=""`；之前的实现只取 `tool_calls[0]` 直接丢失并行调用。同时新增 `_filter_tools_by_choice(openai_tools, tool_choice)`（main.py 3342）实现代理层 `tool_choice` 语义近似：`"none"` → 返回 `[]`（不发 tools）；`{type:"function",function:{name:X}}` → 仅保留指定的那个；`"auto"`/`"required"`/未提供 → 全发（JetBrains 后端无 `tool_choice` 公开 hook，"required" 退化为 auto 是必要折衷）。OpenAI 端 `chat_completions` 与 Anthropic 端 `convert_anthropic_to_openai` 转换出的 jetbrains_messages 都改用同一对辅助函数，行为一致。
  2. **流式 `tool_calls.index` 修复** — `openai_stream_adapter`（main.py 3389）原本对每个 JetBrains `FunctionCall` 事件硬编码 `tool_id = 0`，导致：(a) 真正并行 tool_calls 时所有 delta 都堆到 index=0 客户端无法拆分；(b) 一个 tool_call 内 arguments 的多个增量 chunk 也都重复发送 `id`/`name`/`type`。新逻辑用 `current_tool_call_index` 跟踪状态：见到带 `name` 的事件 → 视为新的 tool_call（`current_tool_call_index += 1`，下发 `id`+`type`+`function.name`+初始 `arguments`）；见到 `name=null` 的事件 → 视为当前 tool_call 的 arguments 增量（沿用 index，仅下发 `function.arguments` 增量片段，不重发 id/name/type）。这样既正确处理 JetBrains 把单 tool_call 拆成多条 FunctionCall 事件的流送方式，也支持真正的并行 tool_calls 按 (index, id) 正确切分。端到端验证：单工具 `get_weather({"city":"Tokyo"})` 现在表现为 1 条带 id+name 的首块 + 5 条只带 arguments 增量的后续块，完全符合 OpenAI streaming 协议。
  3. **tool_choice 端到端验证** — 用临时 client_key 跑了三组 curl：(T1) 普通 tools 调用 → 正确返回单 tool_call 流；(T2) `tool_choice:"none"` + tools → 0 个 tool_calls 块（只输出文本，证明 tools 被过滤掉）；(T3) `tool_choice:{type:"function",function:{name:"get_weather"}}` + 提供 `[get_weather, get_news]` 两个 tools → 模型只能调用 `get_weather`（证明只发了指定的那个 tool）。
## 批量稳定性修复（2026-04-27）

以下 6 条改动经架构师两轮审查，最终 Pass：

1. **graceful shutdown（api-server/src/index.ts）**：将 `py` 提升为模块级 `pyProcess`；新增 `gracefulShutdown()` 先 `httpServer.close()` 再向 Python 发 SIGTERM（30s 后强 SIGKILL）；`process.on("SIGTERM"/"SIGINT")` 改为调用 `gracefulShutdown`；保留 `process.on("exit")` 作为意外退出兜底；`app.listen` 返回值保存为 `httpServer`。效果：长流式请求不再被 SIGTERM 硬切。

2. **JWT 刷新全局 Semaphore（main.py）**：新增 `_jwt_refresh_sem = asyncio.Semaphore(20)`，`_refresh_jetbrains_jwt` 整个函数体缩进至 `async with _jwt_refresh_sem:` 下。配合 `_first_ready` 批内 8 并发限制，高并发刷新数从潜在"8N"收敛到 ≤ 20，防止 grazie auth 接口被击穿。

3. **`_get_low_personal_key` 排序修复（main.py）**：原来返回第一个匹配 `low_admin_discord_id` 的 key（可能命中孤儿预签 key）；改为按 `usage_count DESC, usage_limit DESC` 排序，优先选已被真实使用/有真实额度的 key。修复"批量激活额度发错 Discord 账号"根因。

4. **per-Discord 批量激活 cooldown（main.py）**：`_low_admin_last_batch_at` 从 `float` 改为 `Dict[str, float]`（admin 用 `""` 键，LOW 用 `dc_user_id` 键）；`/admin/activate-batch` 与 `/admin/low-config` 均改为按各自 discord_id 读写，不再共享全局 cooldown。两个不同 Discord ID 的 LOW 用户可以独立触发批量激活。

5. **CF Worker 池健康检查（main.py）**：`cf_proxy_pool` 表加两列 `last_health_check`（DOUBLE PRECISION）和 `consecutive_failures`（INTEGER）；新增后台任务 `_cf_proxy_health_check_loop()`，每 5 分钟对所有 `is_active=TRUE` 的 Worker URL 发 `GET /health`，连续 3 次失败自动 `is_active=FALSE` 并打印日志，成功则重置计数；检查结束后调 `load_cf_proxies_from_db()` 刷内存池；已在 `startup()` 里注册。

6. **前端 retry jitter（admin-panel/src/App.tsx）**：503/502 重试次数从 5 降为 3；`retryDelay` 加随机抖动 `Math.min(1000 * 2^attempt + random()*1000, 15000)`，多 tab 并发重试不再同步打后端。

7. **调用日志持久化到 PostgreSQL（main.py 任务5）**：
   - 新增 `call_logs` 表（BIGSERIAL PK + ts/model/api_key/discord_id/prompt_tokens/completion_tokens/elapsed_ms/status/exempt/created_at），含 `idx_call_logs_ts(ts DESC)` 和 `idx_call_logs_discord_id(discord_id)` 两个索引。
   - `_persist_call_log(entry)` 异步辅助函数：从 pool acquire 连接后 INSERT 一行，失败静默。
   - `_cleanup_old_call_logs_loop()`：启动 60s 后首次运行，每 24h 清理 `ts < now - 7d` 的行；已在 `startup()` 注册。
   - `_append_log()`：追加到内存 deque 后，fire-and-forget `asyncio.get_running_loop().create_task(_persist_call_log(entry))`（RuntimeError 兜底）。
   - `GET /admin/logs`：limit 上限从 500 升为 5000；优先从数据库查询（admin 全量 / LOW 按 discord_id 过滤）；DB 失败回退内存 deque；字段 `api_key` 在 Python 层 rename 为 `key` 保持前端兼容。
   - `DELETE /admin/logs`：先 DELETE 数据库行，再清内存 deque（admin 全量 / LOW 按 discord_id 分隔）。

- **管理员整库迁移导出/导入补齐 `low_admin_discord_id`** — `/admin/accounts/export-all` 与 `_do_bulk_import`（被 `/admin/accounts/bulk-import` 与 `/admin/accounts/import-from-source` 复用）此前只携带 `is_low_admin_key`，漏掉 `low_admin_discord_id`，导致服务器迁移后所有 LOW key 都被前端归到「未知」分组里。现在导出 SQL 增加 `COALESCE(low_admin_discord_id, '') as low_admin_discord_id`，导出 JSON 多出该字段；`_do_bulk_import` 的 INSERT SQL 增加该列与 `$9` 参数，行构造从 payload 读 `low_admin_discord_id`，ON CONFLICT 用 `COALESCE(NULLIF(EXCLUDED.low_admin_discord_id,''), jb_client_keys.low_admin_discord_id)` 保证 upsert 不会用空值覆盖已有归属。注：LOW 用户的个人 key 单独导出/导入端点（`/admin/low-user-key/export|import`）不需要改 — 导入时已强制用调用者验证过的 Discord ID 写 `low_admin_discord_id`，不依赖备份文件里的字段（更安全）。Roundtrip 验证：导出 2 把 LOW key → 重新导入 → `/admin/keys` 显示 Discord ID 完整保留（`'1382720893633171578'` 与空串均正确）。

## 数据迁移重构（2026-04-27，流式 NDJSON）

### 后端重构（main.py）

删除全部旧的一次性迁移端点（共 785 行 + 99 行残留）：
- `_do_extra_import`、`_do_bulk_import`、`admin_bulk_import`
- `admin_extra_import`、`admin_extra_import_stream`、`admin_export_all`
- `admin_migration_probe`、`admin_import_from_source`
- 旧的 `admin_db_export`、`admin_db_import`

新增 5 个流式端点与辅助函数：
1. **`GET /admin/db-export-stream`** — asyncpg cursor 边查边写 NDJSON，内存稳定 ~10MB，支持 GB 级数据
2. **`_upsert_one_row(conn, table, row)`** — 单行 upsert 核心逻辑，被两个导入端点复用
3. **`POST /admin/db-import-stream`** — 浏览器上传 .ndjson 文件，流式 upsert，每 500 行 commit
4. **`POST /admin/import-from-source-stream`** — 从源端 `/admin/db-export-stream` 流式拉取并 upsert（15 分钟 read timeout，每 500 行 commit）
5. **`POST /admin/migration-probe-stream`** — 探测源端连通性，只读前 64KB

NDJSON 协议（version 3）：
```
{"_meta":{"version":3,"exported_at":1730000000}}
{"_table":"jb_accounts"}
{<row1>}
{<row2>}
{"_error":{"table":"xxx","error":"..."}}  # 可选错误行
```

`_EXPORT_TABLES` 新增 `jb_settings`（LOW 用户审计行 + 全局配置）。  
`_TABLE_CONFLICT_COL` 新增 `"jb_settings": "key"`。

### 前端重构（admin-panel）

1. **新建 `src/pages/MigrationPanel.tsx`** — 三个功能块的独立组件：
   - 📥 下载本实例完整 NDJSON 数据
   - 📤 上传 NDJSON 文件导入（`fetch` 流式提交，不在浏览器内存累积）
   - 🔄 从源端流式拉取（输入 URL + Admin Key，内置探测功能）

2. **重写 `Dashboard.tsx`** — 删除旧迁移 state/handler（~200 行）、删除旧的两个迁移 Card（"数据迁移"+ "JSON 粘贴导入"），替换为 `<MigrationPanel />`。

3. **修改 `Accounts.tsx`** — 删除 `syncFromSourceMutation`（调用旧端点 `/admin/accounts/import-from-source`）及其 state（`syncOpen`、`syncUrl`）和对应 Dialog UI；移除 `ArrowDownToLine` 未使用 import。

架构师审查：旧函数已全部删除，新 5 个端点存在且正确，_EXPORT_TABLES/_TABLE_CONFLICT_COL 包含 jb_settings，Python 语法检查 PASS（`python3 -m py_compile`），前端热更新无编译错误，API 服务正常启动（2166 账号）。

## Netlify Claude Proxy（2026-05-02 新增）

研究后选定 **Netlify AI Gateway + Agent Runners** 为 JB 关闭后的下一代渠道。判断依据：

| 维度 | 评估 |
|---|---|
| 免费额度 | 300 credits/月（账号级，邮箱注册无信用卡） |
| 模型 | Claude Sonnet 4.6 + Opus 4.6 全系 |
| 凭证机制 | **AI Gateway 自动注入 `ANTHROPIC_API_KEY` + `ANTHROPIC_BASE_URL`** 到 Functions/Edge Functions runtime，无需用户自带 Anthropic key |
| 月度续期 | ✅ |
| 充值不影响免费额 | ✅（与 Vercel 不同） |
| 现有反代项目 | ❌ 零 |
| 平台反爬 | 弱（Netlify 主业是部署，AI 是黏性工具） |

⚠️ 关键约束：300 credits 是**账号级**（团队共享），同账号开多 site 不增容量。多账号策略是必须的。

### 文件清单

**`netlify-claude-proxy/`** — 单账号可部署的 Netlify Edge Function 模板
- `netlify.toml` — Edge Function 路径映射（`/v1/chat/completions`、`/v1/models`、`/healthz`）
- `netlify/edge-functions/chat.ts` (416 行) — OpenAI ⇄ Anthropic Messages 双向协议转换
  - 系统消息提取到 Anthropic 顶层 `system` 字段
  - 合并连续相同角色消息（Anthropic 禁止）
  - 流式 SSE：Anthropic `message_start` / `content_block_delta` / `message_delta` / `message_stop` → OpenAI `chat.completion.chunk` + `[DONE]`
  - 模型别名映射（`claude-opus` → `claude-opus-4-6` 等）
  - 图像支持（base64 + URL 两种 source）
  - 多模态消息内容数组
  - Bearer token 鉴权（`PROXY_SECRET` 环境变量），防止陌生人偷烧免费额度
- `netlify/edge-functions/models.ts` — `/v1/models` 列表（含鉴权）
- `netlify/edge-functions/health.ts` — `/healthz` 诊断（输出 AI Gateway 是否真的注入了凭证，关键验证点）
- `public/index.html` — 装饰性首页，迷惑性掩盖代理用途
- `README.md` — 部署 + 验证指南（含 OpenAI SDK 使用示例）

**`scripts/src/deploy-netlify.ts`** (283 行) — Netlify REST API 批量部署器
- 子命令：`deploy <PAT> [siteName]` / `list <PAT>` / `status <PAT> <siteId>` / `verify <siteUrl> <PROXY_SECRET>`
- `deploy` 流程：
  1. 用 PAT 拉账号 slug
  2. POST `/sites` 创建新 site
  3. POST `/accounts/{slug}/env?site_id={id}` 注入 `PROXY_SECRET=<32 字节随机>`
  4. Python `zipfile`（系统 `zip` 二进制在本环境段错误，已避开）打包模板
  5. POST `/sites/{id}/deploys` 上传 zip
  6. 轮询 deploy state 至 `ready`
  7. 输出 JSON 含 `account_slug`、`site_id`、`site_url`、`proxy_secret`、`healthz`、`chat_endpoint`，可直接管道入 DB
- `verify` 子命令：依次跑 healthz → 非流式 chat → 流式 chat，端到端验证

**`scripts/package.json`** — 新增 npm 脚本：`netlify:deploy` / `netlify:list` / `netlify:status` / `netlify:verify`

### 待办

- 用户提供 PAT 后实测 healthz 返回 `anthropicKeyInjected: true`
- 实测 sonnet/opus 调用扣 credits 系数（用于本地余额估算）
- admin-panel 改造：账号池表 schema 重命名 + UI（沿用 JB 项目的池化 + 调度框架）
- api-server 改造：上游路由从 `localhost:python` 切到 Netlify site 池

### Netlify proxy 已知坑：Claude 4.x temperature ⊕ top_p 互斥

Claude 4.x 上游对 `temperature` 和 `top_p` 同时存在的请求会返 400 `invalid_request_error: temperature and top_p cannot both be specified`。许多客户端（包括 Cherry Studio 等）默认会把两者都填上，导致代理直通后被上游拒。

修复（`netlify-claude-proxy/netlify/edge-functions/chat.ts` `openaiToAnthropic`）：当 `temperature !== undefined` 时，把 `top_p` 强制置 `undefined`。即"两者择一保 temperature"。客户端无需改动。
