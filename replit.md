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

## 复刻状态（2026-04-26）

源码已 1:1 复刻完成，三个工作流均正常运行：
- admin-panel（端口 20130，路径 /admin-panel/）渲染中文管理员登录页
- api-server（端口 8080）通过 spawn 子进程启动 Python 服务并代理转发
- jetbrainsai2api（Python FastAPI，端口 8000）已连数据库并就绪

### 数据库初始化注意

源码 `_ensure_db_tables()` 中存在迁移顺序问题：在 `ALTER TABLE jb_accounts ADD COLUMN pending_nc_key` 之前就执行了 `SELECT pending_nc_key FROM jb_accounts` 的回填语句。
全新数据库必须预先创建带 `pending_nc_key` 列的 `jb_accounts` 表，否则启动会失败。当前数据库已完成预建。
若重置数据库，需重新执行：
```sql
DROP TABLE IF EXISTS jb_accounts CASCADE;
DROP TABLE IF EXISTS jb_client_keys CASCADE;
CREATE TABLE jb_accounts (
  id TEXT PRIMARY KEY, license_id TEXT, auth_token TEXT, jwt TEXT,
  last_updated DOUBLE PRECISION DEFAULT 0, last_quota_check DOUBLE PRECISION DEFAULT 0,
  has_quota BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT NOW(),
  pending_nc_key TEXT DEFAULT NULL
);
CREATE TABLE jb_client_keys (
  key TEXT PRIMARY KEY, usage_limit INTEGER, usage_count INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 必需的环境变量

要使代理服务真正可用，需配置：
- `ADMIN_KEY` 或 `ADMIN_KEYS` — 管理面板登录密钥
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
- **Database**: PostgreSQL via Drizzle ORM (for Node.js), SQLite (for Python via aiosqlite)
- **Language**: TypeScript + Python 3.11

## Environment Variables

- `ADMIN_KEY` / `ADMIN_KEYS` — Admin authentication key(s)
- `LOW_ADMIN_KEY` — Secondary admin key (low_admin role) — limited /admin/* whitelist (status, activate, low-cf-proxies, low-config, pending-nc/low, cf-proxies/test)
- `DATABASE_URL` — PostgreSQL connection string
- `SESSION_SECRET` — Session secret
- `PORT` — Server port (8080 for API server, 20130 for admin panel)

## LOW_ADMIN Subsystem

LOW_ADMIN_KEY users get a separate, isolated activation flow with their own per-tier limits:

- **Per-tier limits** — Constants in `main.py`: `_NORMAL_KEY_QUOTA=25`, `_LOW_USER_KEY_QUOTA=16`, `_LOW_USER_INPUT_TOKENS=300_000`, `_LOW_USER_OUTPUT_TOKENS=40_000`. Helpers `_key_tier()` and `_key_tier_limits()` resolve a key → its tier limits. `/v1/chat/completions` and `/v1/messages` enforce token caps per tier; `_activate_key_quota` upgrades quota to 16 (LOW) or 25 (normal) based on the `is_low_admin_key` column.
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
  6. **Anti-replay audit (persistent)** — The `jb_settings` k/v table holds an audit row keyed `low_audit:{discord_id}` containing `last_usage_count`, `last_usage_limit`, `banned`, `banned_at`. Helpers `_low_audit_load` / `_low_audit_save` maintain it with monotonic-max merge for usage and sticky-True for banned (`_low_audit_save` itself wraps its load→merge→upsert in a separate `_low_audit_save_locks` per-Discord lock so concurrent saves cannot clobber each other). Write sites: `low_user_key_create` (baseline 0/0/false), `_add_low_quota` (every quota accumulation), `admin_ban_key` (manual ban), the external-usage auto-ban loop in `_external_usage_check`, and `low_user_key_delete` (snapshots current `usage_count`/`usage_limit` before removal so delete-then-import can never roll back quota). The audit row is **NOT deleted on DELETE**. On import: (a) reject 409 if `payload.usage_count < audit.last_usage_count` (prevents quota rollback), (b) reject 409 if `audit.banned == True && payload.banned == False` (prevents self-unban via pre-ban backup), (c) compute `effective_banned = payload.banned OR audit.banned` so a ban is never lost. Ban-import mutex: helper `_persist_low_user_ban(discord_id, ts)` acquires the per-Discord import lock then writes audit AND re-applies `banned=True` to the current in-memory key (and `_upsert_key_to_db`), so a ban happening concurrently with an import can never produce a runtime state where audit says banned but the active key is unbanned. `admin_ban_key` awaits this helper synchronously; the external auto-ban loop spawns it via `create_task` (the helper acquires the same import lock, so any subsequent import for that user serializes behind it). `low_user_key_delete` itself runs under the same per-Discord import lock for full mutex with import / ban paths.
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
- **管理员整库迁移导出/导入补齐 `low_admin_discord_id`** — `/admin/accounts/export-all` 与 `_do_bulk_import`（被 `/admin/accounts/bulk-import` 与 `/admin/accounts/import-from-source` 复用）此前只携带 `is_low_admin_key`，漏掉 `low_admin_discord_id`，导致服务器迁移后所有 LOW key 都被前端归到「未知」分组里。现在导出 SQL 增加 `COALESCE(low_admin_discord_id, '') as low_admin_discord_id`，导出 JSON 多出该字段；`_do_bulk_import` 的 INSERT SQL 增加该列与 `$9` 参数，行构造从 payload 读 `low_admin_discord_id`，ON CONFLICT 用 `COALESCE(NULLIF(EXCLUDED.low_admin_discord_id,''), jb_client_keys.low_admin_discord_id)` 保证 upsert 不会用空值覆盖已有归属。注：LOW 用户的个人 key 单独导出/导入端点（`/admin/low-user-key/export|import`）不需要改 — 导入时已强制用调用者验证过的 Discord ID 写 `low_admin_discord_id`，不依赖备份文件里的字段（更安全）。Roundtrip 验证：导出 2 把 LOW key → 重新导入 → `/admin/keys` 显示 Discord ID 完整保留（`'1382720893633171578'` 与空串均正确）。
