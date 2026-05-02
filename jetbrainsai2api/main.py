import base64
import functools as _functools
import hashlib
import hmac as _hmac_mod
import json
import os
import queue
import random as _random
import re
import secrets
from datetime import datetime as _dt
import time
import uuid
import asyncio
import threading
from collections import deque as _deque
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, quote as _urlquote
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

import asyncpg
import httpx
import uvicorn
import aiofiles
from fastapi import FastAPI, HTTPException, Depends, Header, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, RedirectResponse, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, ConfigDict

# Configuration
DEFAULT_REQUEST_TIMEOUT = 30.0

# Global variables
VALID_CLIENT_KEYS: dict = {}  # key -> {"usage_limit": Optional[int], "usage_count": int}
POKEBALL_KEYS: dict = {}      # ball_key -> {"id", "name", "capacity", "total_used", "rr_index", "members": [key,...]}
JETBRAINS_ACCOUNTS: list = []
current_account_index: int = 0
account_rotation_lock = asyncio.Lock()
# JWT 刷新全局节流：防止高并发时 grazie auth 接口被击穿
# 即使 _first_ready 给每个请求做了批内并发限制（8），多个用户同时请求时累积仍会超过上游能承受的速率。
_jwt_refresh_sem = asyncio.Semaphore(20)
# 每个账号 ID 对应一把 JWT 刷新锁，防止并发请求重复刷新同一账号
_jwt_refresh_locks: Dict[str, asyncio.Lock] = {}
# 当前正在进行 _check_quota 的账号 ID 集合（防止同一账号并发重入）
_quota_check_in_progress: set = set()
# JWT 刷新间隔：仅在 JWT 缺失或距上次成功刷新超过此秒数时才重新刷新
_JWT_REFRESH_INTERVAL_SECS: int = 1800  # 30 分钟

# 全量重检进度跟踪（线程安全：asyncio 单线程）
_bulk_recheck_state: Dict[str, Any] = {
    "running": False,
    "total": 0,
    "done": 0,
    "task": None,   # asyncio.Task，用于取消
}

class _AccountFailed(Exception):
    """当前账号不可用（477 配额耗尽 / 401 持久失效），触发自动切换到下一个账号"""
    pass

file_write_lock = asyncio.Lock()
_running_bind_tasks: set = set()  # 当前正在运行的 bind_task email 集合，防并发重复
_self_register_user_locks: Dict[str, asyncio.Lock] = {}  # per discord_user_id 锁，防日限并发绕过
_self_register_inflight: Dict[str, int] = {}              # 各用户当前 in-flight 提交数
_SELF_REGISTER_LOCKS_MAX = 5_000                          # 锁表最大条目数，防无限增长

# 自助绑卡 precheck 拒绝原因映射（模块级常量，避免每请求重建）
_PRECHECK_MESSAGES: Dict[str, str] = {
    "invalid_login":            "账号或密码验证失败，请检查后重试",
    "invalid_proof":            "该账号需要先绑定支付方式才能激活试用，不符合申请条件",
    "invalid_country":          "该账号所在地区受限，不符合申请条件",
    "invalid_grazie_untrusted": "该账号暂不受信任，不符合申请条件",
    "precheck_error":           "账号验证过程中出现异常，请稍后重试",
}

# 第二对端配置缓存（env var 在进程内不变，加密只做一次）
# 值为 dict（有效）、None（已解析但配置缺失/加密失败）或 _UNSET（未初始化）
_UNSET = object()
_secondary_push_cfg_cache: object = _UNSET
models_data: Dict[str, Any] = {}
_model_dict: Dict[str, Dict] = {}        # model_id -> model_info（O(1) 查找缓存）
anthropic_model_mappings: Dict[str, str] = {}
MODEL_COSTS: Dict[str, float] = {}       # model_id -> 单次调用消耗的 key 用量（支持小数）
_key_fractional_usage: Dict[str, float] = {}  # key -> 累计小数部分
_pending_key_increments: Dict[str, int] = {}  # key -> 待刷新到 DB 的累计增量（批量写入，减少连接）
http_client: Optional[httpx.AsyncClient] = None
DB_POOL: Optional[Any] = None       # 主池：普通用户 / admin / chat / 主后台任务
DB_POOL_LOW: Optional[Any] = None   # LOW 专用池：LOW 激活 / LOW pending retry

# ==================== 用量统计 ====================
service_start_time: float = time.time()
# 结构: { account_id: { calls, errors, prompt_tokens, completion_tokens, ttft_ms: deque, total_ms: deque } }
account_stats: Dict[str, Dict[str, Any]] = {}

# ── 管理接口响应缓存（避免高并发时大型 JSON 反复序列化阻塞事件循环）──
# 结构: { cache_key: (timestamp, json_bytes) }
_admin_cache: Dict[str, tuple] = {}
_ADMIN_CACHE_TTL: Dict[str, float] = {
    "status":      5.0,   # /admin/status         —— 纯内存读，5s 足够
    "accounts":   15.0,   # /admin/accounts       —— 4000+ 条，15s 缓存
    "keys":       15.0,   # /admin/keys           —— 2000+ 条，15s 缓存
    "leaderboard": 30.0,  # /key/saint-leaderboard —— 排行榜每 30s 刷新
    "prizes":      30.0,  # /key/prizes            —— 奖品列表每 30s 刷新
}

def _admin_cache_get(key: str) -> "bytes | None":
    entry = _admin_cache.get(key)
    if entry and time.time() - entry[0] < _ADMIN_CACHE_TTL.get(key, 10.0):
        return entry[1]
    return None

def _admin_cache_set(key: str, body: bytes) -> None:
    _admin_cache[key] = (time.time(), body)

def _admin_cache_invalidate(*keys: str) -> None:
    """写操作后调用，使对应缓存立即失效"""
    for k in keys:
        _admin_cache.pop(k, None)

# ==================== 调用日志（环形缓冲，最近 500 条）====================
_call_logs: _deque = _deque(maxlen=500)
_log_id_counter: int = 0

# ==================== 少量调用豁免阈值 ====================
# 当一次调用的输入 < 阈值 且 输出 < 阈值 时，不消耗 key 用量（不计入 usage_count）。
# 同时满足两个条件才豁免（OR 不行）；用于过滤心跳、ping、补全候选触发等极小请求。
_USAGE_EXEMPT_INPUT_TOKENS = 200
_USAGE_EXEMPT_OUTPUT_TOKENS = 200

def _is_call_exempt(prompt_tokens: int, completion_tokens: int) -> bool:
    """少量调用豁免：输入/输出均 < 200 token 时，不记次数（同时满足两个条件）。"""
    return (
        int(prompt_tokens or 0) < _USAGE_EXEMPT_INPUT_TOKENS
        and int(completion_tokens or 0) < _USAGE_EXEMPT_OUTPUT_TOKENS
    )

def _append_log(model: str, client_key: str, prompt_tokens: int,
                completion_tokens: int, elapsed_ms: float, status: str,
                exempt: bool = False) -> None:
    global _log_id_counter
    _log_id_counter += 1
    masked_key = (client_key[:10] + "…") if len(client_key) > 10 else client_key
    # 解析 key → Discord ID（用于 LOW 用户私有日志过滤）
    meta = VALID_CLIENT_KEYS.get(client_key) or {}
    discord_id = str(meta.get("low_admin_discord_id", "") or "")
    entry: Dict[str, Any] = {
        "id":               _log_id_counter,
        "ts":               time.time(),
        "model":            model,
        "key":              masked_key,
        "discord_id":       discord_id,   # 内部字段：LOW 用户日志按此过滤
        "prompt_tokens":    int(prompt_tokens or 0),
        "completion_tokens": int(completion_tokens or 0),
        "elapsed_ms":       round(elapsed_ms),
        "status":           status,
        "exempt":           bool(exempt),  # True 表示本次未计费（豁免）
    }
    _call_logs.append(entry)
    # 同时异步持久化到数据库（fire-and-forget，失败不影响响应）
    try:
        asyncio.get_running_loop().create_task(_persist_call_log(entry))
    except RuntimeError:
        pass

def _account_id(account: dict) -> str:
    """生成账户唯一标识（优先用 licenseId，否则用 JWT 的 SHA256 哈希）。
    结果缓存在 account["_id"] 中，避免热路径重复哈希。"""
    cached = account.get("_id")
    if cached:
        return cached
    if account.get("licenseId"):
        account["_id"] = account["licenseId"]
        return account["_id"]
    jwt = account.get("jwt", "unknown")
    account["_id"] = "jwt:" + hashlib.sha256(jwt.encode()).hexdigest()[:32]
    return account["_id"]

def _ensure_stats(account_id: str):
    if account_id not in account_stats:
        account_stats[account_id] = {
            "calls": 0,
            "errors": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "ttft_ms": _deque(maxlen=200),   # 自动逐出旧条目，O(1) append
            "total_ms": _deque(maxlen=200),
        }

def _estimate_tokens(text: str) -> int:
    """粗略估算 token 数（无官方接口，按字符数/4 近似）"""
    return max(1, len(text) // 4)

def _estimate_messages_tokens(messages) -> int:
    total = 0
    for msg in messages:
        content = msg.content
        if isinstance(content, str):
            total += _estimate_tokens(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    total += _estimate_tokens(block.get("text", "") or block.get("content", ""))
    return total

def _record_stats(account: dict, prompt_tokens: int, completion_tokens: int,
                  start_time: float, first_token_time: Optional[float] = None,
                  error: bool = False):
    """更新账户统计（asyncio 单线程，无需加锁；deque 自动淘汰旧条目）"""
    account_id = _account_id(account)
    _ensure_stats(account_id)
    s = account_stats[account_id]
    if error:
        s["errors"] += 1
    else:
        s["calls"] += 1
        s["prompt_tokens"] += prompt_tokens
        s["completion_tokens"] += completion_tokens
        s["total_ms"].append(round((time.time() - start_time) * 1000, 1))
        if first_token_time is not None:
            s["ttft_ms"].append(round((first_token_time - start_time) * 1000, 1))

async def _tracked_stream(
    stream: AsyncGenerator[str, None],
    account: dict,
    prompt_tokens: int,
    start_time: float,
    model: str = "",
    client_key: str = "",
    usage_capture: Optional[Dict[str, int]] = None,
) -> AsyncGenerator[str, None]:
    """包装流式生成器，在其中追踪 TTFT、总耗时、token 数和错误。
    Token 来源优先级与 _stream_with_key_consume 保持一致：
      FinishMetadata 捕获 (usage_capture) > SSE 内联 usage 块 > 字符估算
    这样日志条目里的 token 数和实际计费用的 token 数严格对齐。
    """
    first_token_time: Optional[float] = None
    completion_chars = 0
    inline_usage_p = 0
    inline_usage_c = 0
    try:
        async for chunk in stream:
            if chunk.startswith("data: ") and chunk.strip() != "data: [DONE]":
                if first_token_time is None:
                    first_token_time = time.time()
                try:
                    data = json.loads(chunk[6:].strip())
                    for choice in data.get("choices", []):
                        content = choice.get("delta", {}).get("content") or ""
                        completion_chars += len(content)
                    # 与 _stream_with_key_consume 一致：兼容内联 usage 块
                    u = data.get("usage") or {}
                    if u:
                        if u.get("prompt_tokens"):
                            inline_usage_p = int(u["prompt_tokens"])
                        if u.get("completion_tokens"):
                            inline_usage_c = int(u["completion_tokens"])
                except Exception:
                    pass
            yield chunk
        # 优先使用 FinishMetadata 捕获的真实 token，其次 SSE 内联 usage，最后字符估算
        cap = usage_capture or {}
        real_p = cap.get("prompt_tokens") or inline_usage_p or 0
        real_c = cap.get("completion_tokens") or inline_usage_c or 0
        actual_prompt = real_p if real_p else prompt_tokens
        actual_completion = real_c if real_c else max(1, completion_chars // 4)
        _record_stats(account, actual_prompt, actual_completion, start_time, first_token_time)
        # 是否豁免：流式分支由 _stream_with_key_consume 实际决定计费；此处按相同条件标记日志
        is_exempt = _is_call_exempt(actual_prompt, actual_completion)
        _append_log(model, client_key, actual_prompt, actual_completion,
                    (time.time() - start_time) * 1000, "ok", exempt=is_exempt)
    except Exception as e:
        _record_stats(account, 0, 0, start_time, error=True)
        _append_log(model, client_key, prompt_tokens, 0,
                    (time.time() - start_time) * 1000, "error")
        raise

# Pydantic Models
class ChatMessage(BaseModel):
    model_config = ConfigDict(extra='ignore')

    role: str
    content: Optional[Union[str, List[Dict[str, Any]]]] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_call_id: Optional[str] = None
    name: Optional[str] = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra='ignore')

    model: str
    messages: List[ChatMessage]
    stream: bool = False
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    top_p: Optional[float] = None
    tools: Optional[List[Dict[str, Any]]] = None
    tool_choice: Optional[Any] = None
    stop: Optional[Union[str, List[str]]] = None
    # SillyTavern / OpenAI 兼容字段（接收但不传递给 JetBrains）
    frequency_penalty: Optional[float] = None
    presence_penalty: Optional[float] = None
    logit_bias: Optional[Dict[str, Any]] = None
    n: Optional[int] = None
    user: Optional[str] = None
    seed: Optional[int] = None
    stream_options: Optional[Dict[str, Any]] = None
    response_format: Optional[Dict[str, Any]] = None
    top_k: Optional[int] = None
    repetition_penalty: Optional[float] = None
    min_p: Optional[float] = None


# --- Anthropic-Compatible Models ---


class AnthropicContentBlock(BaseModel):
    model_config = ConfigDict(extra='ignore')

    type: str
    text: Optional[str] = None
    tool_use_id: Optional[str] = None
    content: Optional[Union[str, List[Dict[str, Any]]]] = None
    id: Optional[str] = None
    name: Optional[str] = None
    input: Optional[Dict[str, Any]] = None


class AnthropicMessage(BaseModel):
    model_config = ConfigDict(extra='ignore')

    role: str
    content: Union[str, List[AnthropicContentBlock]]


class AnthropicTool(BaseModel):
    model_config = ConfigDict(extra='ignore')

    name: str
    description: Optional[str] = None
    input_schema: Dict[str, Any]


class AnthropicMessageRequest(BaseModel):
    model_config = ConfigDict(extra='ignore')

    model: str
    messages: List[AnthropicMessage]
    system: Optional[Union[str, List[Dict[str, Any]]]] = None
    max_tokens: int
    stream: bool = False
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    tools: Optional[List[AnthropicTool]] = None
    stop_sequences: Optional[List[str]] = None
    top_k: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


# --- Anthropic-Compatible Response Models ---


class AnthropicUsage(BaseModel):
    input_tokens: int
    output_tokens: int


class AnthropicResponseContent(BaseModel):
    type: str
    id: Optional[str] = None
    name: Optional[str] = None
    input: Optional[Dict[str, Any]] = None
    text: Optional[str] = None


class AnthropicResponseMessage(BaseModel):
    id: str
    type: str = "message"
    role: str = "assistant"
    model: str
    content: List[AnthropicResponseContent]
    stop_reason: Optional[str]
    stop_sequence: Optional[str] = None
    usage: AnthropicUsage


# --- End Anthropic Models ---


class ModelInfo(BaseModel):
    id: str
    object: str = "model"
    created: int
    owned_by: str


class ModelList(BaseModel):
    object: str = "list"
    data: List[ModelInfo]


class ChatCompletionChoice(BaseModel):
    message: ChatMessage
    index: int = 0
    finish_reason: str = "stop"


class ChatCompletionResponse(BaseModel):
    id: str = Field(default_factory=lambda: f"chatcmpl-{uuid.uuid4().hex}")
    object: str = "chat.completion"
    created: int = Field(default_factory=lambda: int(time.time()))
    model: str
    system_fingerprint: str = "fp_jetbrains"
    choices: List[ChatCompletionChoice]
    usage: Dict[str, int] = Field(
        default_factory=lambda: {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
    )


class StreamChoice(BaseModel):
    delta: Dict[str, Any] = Field(default_factory=dict)
    index: int = 0
    finish_reason: Optional[str] = None


class StreamResponse(BaseModel):
    id: str = Field(default_factory=lambda: f"chatcmpl-{uuid.uuid4().hex}")
    object: str = "chat.completion.chunk"
    created: int = Field(default_factory=lambda: int(time.time()))
    model: str
    system_fingerprint: str = "fp_jetbrains"
    choices: List[StreamChoice]


# FastAPI App
app = FastAPI(title="JetBrains AI OpenAI Compatible API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.exception_handler(HTTPException)
async def openai_http_exception_handler(request: Request, exc: HTTPException):
    """将 FastAPI HTTPException 转为 OpenAI 兼容的错误格式，使 SillyTavern 等客户端能正确显示错误信息"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "message": exc.detail,
                "type": "invalid_request_error" if exc.status_code < 500 else "server_error",
                "code": str(exc.status_code),
            }
        },
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """捕获所有未被 HTTPException handler 处理的异常，防止 httpx 内部错误字符串泄漏给客户端"""
    if isinstance(exc, httpx.HTTPStatusError):
        upstream_body = getattr(exc.response, "_content", b"").decode("utf-8", errors="replace")
        upstream_status = exc.response.status_code
        print(f"[未处理 httpx 错误] status={upstream_status} body={upstream_body[:500]}")
        return JSONResponse(
            status_code=502,
            content={
                "error": {
                    "message": f"上游 JetBrains API 错误 ({upstream_status}): {upstream_body}",
                    "type": "server_error",
                    "code": str(upstream_status),
                }
            },
        )
    # 连接池耗尽：转 503 + Retry-After，让客户端能够退避重试，避免 500 风暴
    # （httpx.PoolTimeout 是 httpx.TimeoutException 子类；底层 httpcore.PoolTimeout 偶尔会原样冒泡）
    pool_timeout_types = (httpx.PoolTimeout,)
    try:
        import httpcore  # type: ignore
        pool_timeout_types = (httpx.PoolTimeout, httpcore.PoolTimeout)
    except Exception:
        pass
    if isinstance(exc, pool_timeout_types):
        print(f"[未处理异常] 连接池超时 PoolTimeout: 全局 http_client 池已饱和")
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "message": "上游连接池繁忙，请稍后重试",
                    "type": "server_error",
                    "code": "503",
                }
            },
            headers={"Retry-After": "5"},
        )
    print(f"[未处理异常] {type(exc).__name__}: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "message": "Internal server error",
                "type": "server_error",
                "code": "500",
            }
        },
    )


security = HTTPBearer(auto_error=False)


@app.get("/health")
async def health_check():
    """无需鉴权的健康检查接口（供 Express 代理探测使用，不消耗任何资源）"""
    return {"status": "ok"}


# ==================== 后台管理鉴权 ====================
ADMIN_KEY: str = os.environ.get("ADMIN_KEY", "")
# 次级管理员（LOW_ADMIN）：登录后落入用户面板，不受激活每日 20 次限制，
# 拥有专属隔离的 CF 代理池。前端通过 /admin/status 返回的 role 判定。
LOW_ADMIN_KEY: str = os.environ.get("LOW_ADMIN_KEY", "") or os.environ.get("LOW-ADMIN_KEY", "")

# ==================== Partner API AES-GCM 加密工具 ====================
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM
    _PARTNER_AES_KEY: bytes = hashlib.sha256(ADMIN_KEY.encode()).digest()  # 32-byte key

    def _partner_encrypt(plaintext: str) -> str:
        nonce = os.urandom(12)
        ct = _AESGCM(_PARTNER_AES_KEY).encrypt(nonce, plaintext.encode(), None)
        return "v1:" + base64.b64encode(nonce + ct).decode()

    def _partner_decrypt(ciphertext: str) -> str:
        assert ciphertext.startswith("v1:"), "invalid prefix"
        data = base64.b64decode(ciphertext[3:])
        nonce, ct = data[:12], data[12:]
        return _AESGCM(_PARTNER_AES_KEY).decrypt(nonce, ct, None).decode()

    _PARTNER_CRYPTO_OK = True
except ImportError:
    _PARTNER_CRYPTO_OK = False
    def _partner_encrypt(plaintext: str) -> str:
        raise RuntimeError("cryptography not installed")
    def _partner_decrypt(ciphertext: str) -> str:
        raise RuntimeError("cryptography not installed")

# HMAC secret 解密缓存：同一密文在进程内只解密一次（最多缓存 8 条）
_partner_decrypt_cached = _functools.lru_cache(maxsize=8)(_partner_decrypt)

# 不需要 ADMIN_KEY 的 /admin 路径前缀（账号激活相关）
_ADMIN_PUBLIC_PREFIXES = (
    "/admin/activate",
)

# LOW_ADMIN_KEY 允许访问的 /admin/* 端点白名单（前缀匹配）
# 不在此列表的 /admin/* 路径，LOW key 一律 403。
# 列表内容：状态、激活（含批量）、专属 CF 池、CF 连通性测试、LOW 配置、LOW 排队记录。
_LOW_ADMIN_ALLOWED_PREFIXES = (
    "/admin/status",
    "/admin/activate",          # 含 /admin/activate-batch
    "/admin/low-cf-proxies",
    "/admin/low-config",        # LOW 用户的批量并发配置 / 冷却信息
    "/admin/pending-nc/low",    # LOW 用户专属排队队列只读视图
    "/admin/cf-proxies/test",   # 仅测试连通性，不写主池
    "/admin/low-user-key",      # LOW 用户个人专属密钥管理
    "/admin/logs",              # LOW 用户调用日志（按 Discord ID 过滤；端点内部强制鉴权）
)

@app.middleware("http")
async def admin_key_middleware(request: Request, call_next):
    """对所有 /admin/* 路径（账号激活除外）校验 X-Admin-Key 请求头。
    接受两类 key：
      - ADMIN_KEY      → 管理员，全部权限
      - LOW_ADMIN_KEY  → 次级管理员（用户面板），仅放行白名单内端点（_LOW_ADMIN_ALLOWED_PREFIXES）
    """
    path = request.url.path
    if path.startswith("/admin/") or path == "/admin":
        is_public = any(path.startswith(p) for p in _ADMIN_PUBLIC_PREFIXES)
        if not is_public:
            provided = request.headers.get("X-Admin-Key", "")
            if not ADMIN_KEY:
                # 未配置 ADMIN_KEY，拒绝所有请求
                return JSONResponse(status_code=503, content={"detail": "服务未配置 ADMIN_KEY"})
            if provided == ADMIN_KEY:
                pass  # 完整管理员，放行
            elif LOW_ADMIN_KEY and provided == LOW_ADMIN_KEY:
                # 次级管理员：仅允许白名单端点
                allowed = any(path.startswith(p) for p in _LOW_ADMIN_ALLOWED_PREFIXES)
                if not allowed:
                    return JSONResponse(
                        status_code=403,
                        content={"detail": "次级管理员无权访问该接口"},
                    )
            else:
                return JSONResponse(status_code=401, content={"detail": "ADMIN_KEY 不正确"})
    return await call_next(request)


def _request_role(request: Request) -> str:
    """根据 X-Admin-Key 判定调用者身份：'admin' / 'low_admin' / 'none'"""
    provided = request.headers.get("X-Admin-Key", "")
    if ADMIN_KEY and provided == ADMIN_KEY:
        return "admin"
    if LOW_ADMIN_KEY and provided == LOW_ADMIN_KEY:
        return "low_admin"
    return "none"


# Helper functions
def load_models():
    """加载模型配置和映射规则"""
    global anthropic_model_mappings, MODEL_COSTS, _model_dict
    try:
        with open("models.json", "r", encoding="utf-8") as f:
            config = json.load(f)

        # 支持新格式（包含 models 和 anthropic_model_mappings）
        if isinstance(config, dict):
            if "models" in config:
                model_ids = config["models"]
                # 加载模型映射配置
                anthropic_model_mappings = config.get("anthropic_model_mappings", {})
                # 加载模型调用成本（支持小数，如 1.5 表示消耗 1.5 次 key 用量）
                MODEL_COSTS = {k: float(v) for k, v in config.get("model_costs", {}).items()}
                if MODEL_COSTS:
                    print(f"模型调用成本配置: {MODEL_COSTS}")
                print(f"从 models.json 加载了 {len(anthropic_model_mappings)} 个模型映射规则")
            else:
                # 处理旧格式的字典（如果有其他字段但没有 models）
                model_ids = []
                anthropic_model_mappings = {}
                print("警告: models.json 使用非标准格式，没有找到 models 字段")
        # 支持旧格式（仅包含模型列表）
        elif isinstance(config, list):
            model_ids = config
            anthropic_model_mappings = {}
            print("警告: models.json 使用旧格式，没有找到模型映射配置")
        else:
            print("错误: models.json 格式不正确")
            return {"data": []}

        processed_models = []
        if isinstance(model_ids, list):
            for model_id in model_ids:
                if isinstance(model_id, str):
                    processed_models.append(
                        {
                            "id": model_id,
                            "object": "model",
                            "created": int(time.time()),
                            "owned_by": "jetbrains-ai",
                        }
                    )

        # 构建 O(1) 查找字典
        _model_dict = {m["id"]: m for m in processed_models}
        return {"data": processed_models}
    except Exception as e:
        print(f"加载 models.json 时出错: {e}")
        anthropic_model_mappings = {}
        _model_dict = {}
        return {"data": []}


async def _get_db_pool():
    """主 DB 连接池：普通用户、admin、chat、主后台任务使用。"""
    global DB_POOL
    if DB_POOL is not None:
        return DB_POOL
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return None
    try:
        DB_POOL = await asyncpg.create_pool(database_url, min_size=2, max_size=15,
                                             command_timeout=60, statement_cache_size=0)
        print("数据库连接池已初始化")
    except Exception as e:
        print(f"数据库连接失败，将使用文件存储作为回退: {e}")
        DB_POOL = None
    return DB_POOL


async def _get_low_db_pool():
    """LOW 专用 DB 连接池：LOW 激活与 LOW pending retry 使用。

    不限制 LOW 激活速度，只把 LOW 写库压力隔离到独立 DB pool，
    避免 LOW 批量激活占满普通用户/admin 的主 DB pool。
    """
    global DB_POOL_LOW
    if DB_POOL_LOW is not None:
        return DB_POOL_LOW
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return None
    try:
        DB_POOL_LOW = await asyncpg.create_pool(database_url, min_size=1, max_size=20,
                                                 command_timeout=60, statement_cache_size=0)
        print("[DB] LOW 专用连接池已初始化")
    except Exception as e:
        print(f"[DB] LOW 池初始化失败: {e}")
        DB_POOL_LOW = None
    return DB_POOL_LOW


async def _ensure_db_tables():
    """确保数据库表存在"""
    pool = await _get_db_pool()
    if not pool:
        return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS jb_accounts (
                id               TEXT PRIMARY KEY,
                license_id       TEXT,
                auth_token       TEXT,
                jwt              TEXT,
                last_updated     DOUBLE PRECISION DEFAULT 0,
                last_quota_check DOUBLE PRECISION DEFAULT 0,
                has_quota        BOOLEAN DEFAULT TRUE,
                created_at       TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS jb_client_keys (
                key         TEXT PRIMARY KEY,
                usage_limit INTEGER,
                usage_count INTEGER DEFAULT 0,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # 兼容旧表：若列不存在则添加
        await conn.execute("ALTER TABLE jb_client_keys ADD COLUMN IF NOT EXISTS usage_limit INTEGER")
        await conn.execute("ALTER TABLE jb_client_keys ADD COLUMN IF NOT EXISTS usage_count INTEGER DEFAULT 0")
        await conn.execute("ALTER TABLE jb_client_keys ADD COLUMN IF NOT EXISTS account_id TEXT")
        await conn.execute("ALTER TABLE jb_client_keys ADD COLUMN IF NOT EXISTS banned BOOLEAN DEFAULT FALSE")
        await conn.execute("ALTER TABLE jb_client_keys ADD COLUMN IF NOT EXISTS banned_at DOUBLE PRECISION DEFAULT NULL")
        await conn.execute("ALTER TABLE jb_client_keys ADD COLUMN IF NOT EXISTS is_nc_key BOOLEAN DEFAULT FALSE")
        # LOW_ADMIN_KEY 专属密钥标记：升级后额度 16（而非 25），输入/输出限制更宽松（300k/40k vs 150k/15k）
        await conn.execute("ALTER TABLE jb_client_keys ADD COLUMN IF NOT EXISTS is_low_admin_key BOOLEAN DEFAULT FALSE")
        # LOW 用户个人专属密钥：绑定 Discord ID，多次激活累加配额到同一把 key
        await conn.execute("ALTER TABLE jb_client_keys ADD COLUMN IF NOT EXISTS low_admin_discord_id TEXT DEFAULT ''")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_jb_client_keys_discord ON jb_client_keys(low_admin_discord_id) WHERE low_admin_discord_id IS NOT NULL AND low_admin_discord_id <> ''")
        # 注意：将 pending_nc_key 引用的 key 标记 is_nc_key=TRUE 的回填，
        # 必须放在下方 ALTER TABLE jb_accounts ADD COLUMN pending_nc_key 之后，
        # 否则新建数据库时该列还不存在会报错。已挪至 ALTER 序列结束后。
        # 每日用量展示
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS daily_used INTEGER DEFAULT NULL")
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS daily_total INTEGER DEFAULT NULL")
        # NC 许可证 492 重试队列（新建 NC 需约 30-60 分钟被 Grazie 信任）
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_lids TEXT DEFAULT NULL")
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_email TEXT DEFAULT NULL")
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_pass TEXT DEFAULT NULL")
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_key TEXT DEFAULT NULL")
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_bound_ids TEXT DEFAULT NULL")
        # 入队时间戳（epoch 秒）——仅在"入队瞬间"写入，重试与 JWT 刷新都不会更新它，
        # 用于"超过 1 小时还在排队的邮箱自动清除"机制的精确入队年龄判定
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_enqueued_at DOUBLE PRECISION DEFAULT 0")
        # 一次性回填：历史 pending 行（已在排队但未记录入队时间）从当前时间开始计时，
        # 避免新机制把存量数据立即误判为超时
        await conn.execute(
            """UPDATE jb_accounts
               SET pending_nc_enqueued_at = EXTRACT(EPOCH FROM NOW())
             WHERE pending_nc_lids IS NOT NULL
               AND pending_nc_lids <> '[]'
               AND (pending_nc_enqueued_at IS NULL OR pending_nc_enqueued_at = 0)"""
        )
        # 该邮箱（行）是否已为 LOW 用户的 key 贡献过 +16 额度
        # —— 同一邮箱凑够 4 个信任凭证只会触发一次 +16，不会重复计入
        await conn.execute("ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_quota_granted BOOLEAN DEFAULT FALSE")
        # 一次性回填：将 jb_accounts.pending_nc_key 引用的 key 标记为 is_nc_key=TRUE
        # （必须在 jb_accounts 的 pending_nc_key 列被 ALTER 添加之后执行）
        await conn.execute(
            """UPDATE jb_client_keys SET is_nc_key = TRUE
               WHERE key IN (SELECT pending_nc_key FROM jb_accounts WHERE pending_nc_key IS NOT NULL)
                 AND (is_nc_key IS NULL OR is_nc_key = FALSE)"""
        )
        # 注意：LOW NC 配额历史回填依赖 jb_settings 表，已挪到下方 jb_settings CREATE 之后。
        # 抽奖奖品表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lottery_prizes (
                id         SERIAL PRIMARY KEY,
                name       TEXT NOT NULL,
                quantity   INTEGER DEFAULT -1,
                weight     INTEGER DEFAULT 10,
                is_active  BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # 用户密码表（密码作为用户唯一标识，不可重复）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_passwords (
                password    TEXT PRIMARY KEY,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # 圣人点数表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS saint_points (
                password    TEXT PRIMARY KEY,
                points      INTEGER NOT NULL DEFAULT 0,
                updated_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Discord 会话持久化表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS discord_sessions (
                token    TEXT PRIMARY KEY,
                user_id  TEXT NOT NULL,
                user_tag TEXT NOT NULL,
                ts       DOUBLE PRECISION NOT NULL
            )
        """)
        # 迁移：补充 total_earned 列（累计获得，永不减少）
        await conn.execute("""
            ALTER TABLE saint_points ADD COLUMN IF NOT EXISTS
                total_earned INTEGER NOT NULL DEFAULT 0
        """)
        # 对已有数据：用现有 points 初始化 total_earned（一次性补录）
        await conn.execute("""
            UPDATE saint_points SET total_earned = points
            WHERE total_earned = 0 AND points > 0
        """)
        # 迁移：补充 dc_tag 列（Discord 用户名，用于排行榜展示）
        await conn.execute("""
            ALTER TABLE saint_points ADD COLUMN IF NOT EXISTS
                dc_tag TEXT NOT NULL DEFAULT ''
        """)
        # 迁移：首次捐 key 赠宝可梦球标志（防重领）
        await conn.execute("""
            ALTER TABLE saint_points ADD COLUMN IF NOT EXISTS
                dc_pokeball_rewarded BOOLEAN NOT NULL DEFAULT FALSE
        """)
        # Discord 账号每日激活次数限制
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dc_activate_limits (
                dc_user_id  TEXT NOT NULL,
                date        DATE NOT NULL DEFAULT CURRENT_DATE,
                count       INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (dc_user_id, date)
            )
        """)
        # 通用设置 K-V 表（目前用于 LOW_ADMIN 批量激活并发数）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS jb_settings (
                k TEXT PRIMARY KEY,
                v TEXT NOT NULL
            )
        """)
        # 必须在下方回填 SQL 引用前提前添加（回填 SQL 引用此列）
        await conn.execute(
            "ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_low_admin BOOLEAN DEFAULT FALSE"
        )
        await conn.execute(
            "ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_discord_id TEXT NOT NULL DEFAULT ''"
        )
        # 一次性回填：旧机制下"全部信任后一次性升 16/25"会导致 LOW key.usage_limit > 0；
        # 切到新机制后，把"已经领过那次 +16 的最早一行"标记为 granted=TRUE，
        # 否则重启后会被当作"未贡献"再次触发 +16。用 jb_settings 加幂等标记，确保只跑一次。
        backfill_done = await conn.fetchval(
            "SELECT v FROM jb_settings WHERE k = 'low_nc_quota_granted_backfill_v1'"
        )
        if not backfill_done:
            await conn.execute(
                """
                WITH first_row_per_low_key AS (
                    SELECT DISTINCT ON (a.pending_nc_key) a.id
                    FROM jb_accounts a
                    JOIN jb_client_keys k ON a.pending_nc_key = k.key
                    WHERE a.pending_nc_low_admin = TRUE
                      AND k.is_low_admin_key = TRUE
                      AND COALESCE(k.usage_limit, 0) > 0
                      AND a.pending_nc_quota_granted = FALSE
                      AND a.pending_nc_key IS NOT NULL
                      AND a.pending_nc_key <> ''
                    ORDER BY a.pending_nc_key, a.id
                )
                UPDATE jb_accounts SET pending_nc_quota_granted = TRUE
                WHERE id IN (SELECT id FROM first_row_per_low_key)
                """
            )
            await conn.execute(
                "INSERT INTO jb_settings (k, v) VALUES ('low_nc_quota_granted_backfill_v1', '1') "
                "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v"
            )
            logger.info("✅ LOW NC 配额历史回填完成：已为已升级 LOW key 的最早一行标记 granted=TRUE")
        # 对已有数据：从 donated_jb_accounts 预填 dc_tag（仅表存在时执行）
        await conn.execute("""
            DO $$ BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'donated_jb_accounts'
                ) THEN
                    UPDATE saint_points sp
                    SET dc_tag = dja.dc_tag
                    FROM donated_jb_accounts dja
                    WHERE dja.dc_password = sp.password
                      AND sp.dc_tag = ''
                      AND dja.dc_tag <> '';
                END IF;
            END $$;
        """)
        # 捐献记录表：每个 account_id 只能捐一次
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS saint_donations (
                account_id  TEXT PRIMARY KEY,
                password    TEXT NOT NULL,
                donated_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # 后备隐藏能源：用户捐献的 JetBrains 账号邮密（待/已审核）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS donated_jb_accounts (
                id           SERIAL PRIMARY KEY,
                jb_email     TEXT NOT NULL,
                jb_password  TEXT NOT NULL,
                dc_password  TEXT NOT NULL,
                dc_tag       TEXT NOT NULL DEFAULT '',
                status       TEXT NOT NULL DEFAULT 'pending',
                submitted_at TIMESTAMPTZ DEFAULT NOW(),
                reviewed_at  TIMESTAMPTZ
            )
        """)
        # 迁移：补充 admin_used 列（标记管理员已实际使用该邮密）
        await conn.execute("""
            ALTER TABLE donated_jb_accounts
            ADD COLUMN IF NOT EXISTS admin_used BOOLEAN NOT NULL DEFAULT FALSE
        """)
        await conn.execute("""
            ALTER TABLE donated_jb_accounts
            ADD COLUMN IF NOT EXISTS admin_used_at TIMESTAMPTZ
        """)
        # 迁移：为 jb_email 加唯一约束前，先去重（保留最新 id），避免重复数据导致 ALTER TABLE 失败
        await conn.execute("""
            DELETE FROM donated_jb_accounts
            WHERE id NOT IN (
                SELECT MAX(id) FROM donated_jb_accounts
                WHERE jb_email IS NOT NULL AND jb_email <> ''
                GROUP BY jb_email
            )
            AND jb_email IS NOT NULL AND jb_email <> '';
        """)
        await conn.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'donated_jb_accounts_jb_email_key'
                ) THEN
                    ALTER TABLE donated_jb_accounts ADD CONSTRAINT donated_jb_accounts_jb_email_key UNIQUE (jb_email);
                END IF;
            END $$;
        """)
        # 用户背包物品表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_items (
                id          SERIAL PRIMARY KEY,
                owner_key   TEXT NOT NULL,
                prize_name  TEXT NOT NULL,
                metadata    JSONB DEFAULT '{}',
                used        BOOLEAN DEFAULT FALSE,
                used_at     TIMESTAMPTZ,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_items_owner ON user_items(owner_key)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_items_owner_used ON user_items(owner_key, used)")
        # jb_client_keys: account_id 用于捐献时检查重复捐献
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_jb_keys_account ON jb_client_keys(account_id)")
        # saint_donations: password 用于排行榜 JOIN 和查询
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_saint_donations_pw ON saint_donations(password)")
        # saint_points: total_earned 降序用于排行榜 ORDER BY（偏索引，排除 0 分行）
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_saint_points_earned ON saint_points(total_earned DESC) WHERE total_earned > 0")
        # self_register_jobs 的索引在该表 CREATE TABLE 之后执行（见下方）
        # lottery_prizes.name 唯一索引（幂等建立，先去重避免冲突）
        await conn.execute("""
            DELETE FROM lottery_prizes
            WHERE id NOT IN (
                SELECT MIN(id) FROM lottery_prizes GROUP BY name
            )
        """)
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_lottery_prizes_name ON lottery_prizes(name)"
        )
        # 宝可梦球（虚拟聚合 key）表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pokeballs (
                id          SERIAL PRIMARY KEY,
                ball_key    TEXT UNIQUE NOT NULL,
                name        TEXT NOT NULL,
                capacity    INTEGER NOT NULL,
                total_used  INTEGER DEFAULT 0,
                rr_index    INTEGER DEFAULT 0,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # 宝可梦球成员 key
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pokeball_members (
                id          SERIAL PRIMARY KEY,
                pokeball_id INTEGER NOT NULL REFERENCES pokeballs(id) ON DELETE CASCADE,
                member_key  TEXT NOT NULL,
                UNIQUE(pokeball_id, member_key)
            )
        """)
        # ==================== 合作伙伴 API 相关表 ====================
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS partner_keys (
                id              TEXT PRIMARY KEY,
                hmac_secret_enc TEXT NOT NULL,
                enabled         BOOLEAN DEFAULT TRUE,
                notes           TEXT DEFAULT '',
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS account_contributions (
                id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                partner_id              TEXT NOT NULL,
                email                   TEXT NOT NULL,
                password_enc            TEXT NOT NULL,
                status                  TEXT DEFAULT 'pending',
                activation_mode         TEXT DEFAULT 'immediate',
                idempotency_key         TEXT,
                custom_note             TEXT DEFAULT '',
                activation_attempts     INTEGER DEFAULT 0,
                activation_error        TEXT DEFAULT '',
                aif_license_count       INTEGER DEFAULT 0,
                linked_credential_ids   TEXT DEFAULT '[]',
                created_at_ms           BIGINT DEFAULT 0,
                updated_at_ms           BIGINT DEFAULT 0,
                activation_started_at   BIGINT DEFAULT 0,
                activation_completed_at BIGINT DEFAULT 0
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ac_email ON account_contributions(email)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ac_partner ON account_contributions(partner_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ac_idempotency ON account_contributions(idempotency_key, partner_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ac_status ON account_contributions(status)")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS partner_api_audit (
                id              SERIAL PRIMARY KEY,
                partner_id      TEXT,
                method          TEXT,
                path            TEXT,
                status_code     INTEGER,
                body_hash       TEXT,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS partner_idempotency (
                idempotency_key TEXT NOT NULL,
                partner_id      TEXT NOT NULL,
                body_hash       TEXT NOT NULL,
                response_json   TEXT NOT NULL,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (idempotency_key, partner_id)
            )
        """)
        # 合规检测拒绝记录表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS partner_precheck_rejections (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                partner_id      TEXT NOT NULL,
                email           TEXT NOT NULL,
                precheck_result TEXT NOT NULL,
                error_detail    TEXT DEFAULT '',
                created_at_ms   BIGINT DEFAULT 0
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ppr_partner ON partner_precheck_rejections(partner_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ppr_result  ON partner_precheck_rejections(precheck_result)")
        # 合作方客户端配置（我方作为推送方时的对端信息）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS partner_client_config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        # 自助绑卡任务记录
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS self_register_jobs (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email           TEXT UNIQUE NOT NULL,
                status          TEXT DEFAULT 'pending',
                result_keys     TEXT DEFAULT '',
                error_msg       TEXT DEFAULT '',
                discord_user_id TEXT DEFAULT '',
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # 兼容旧表：补充新列
        await conn.execute("""
            ALTER TABLE self_register_jobs
            ADD COLUMN IF NOT EXISTS discord_user_id TEXT DEFAULT ''
        """)
        # self_register_jobs: result_keys 等值查找（放在 CREATE TABLE 之后）
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sr_result_keys ON self_register_jobs(result_keys)")
        # CF 代理池表（Cloudflare Worker URLs，用于规避 provide-access 429 限流）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS cf_proxy_pool (
                id         SERIAL PRIMARY KEY,
                url        TEXT NOT NULL UNIQUE,
                label      TEXT DEFAULT '',
                is_active  BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # 兼容旧表：补 owner 列（'admin' = 主池 / 'low_admin' = LOW 用户专属池）
        await conn.execute(
            "ALTER TABLE cf_proxy_pool ADD COLUMN IF NOT EXISTS owner TEXT NOT NULL DEFAULT 'admin'"
        )
        # LOW 池按 Discord 账号分桶：owner='low_admin' 的行用 owner_discord_id 区分子池
        # （admin 主池行该字段恒为 ''）
        await conn.execute(
            "ALTER TABLE cf_proxy_pool ADD COLUMN IF NOT EXISTS owner_discord_id TEXT NOT NULL DEFAULT ''"
        )
        # 把唯一约束从 url 改成 (url, owner, owner_discord_id)：
        # 允许同一 URL 同时出现在主池、不同 Discord LOW 子池中
        await conn.execute("ALTER TABLE cf_proxy_pool DROP CONSTRAINT IF EXISTS cf_proxy_pool_url_key")
        await conn.execute("ALTER TABLE cf_proxy_pool DROP CONSTRAINT IF EXISTS cf_proxy_pool_url_owner_key")
        await conn.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'cf_proxy_pool_url_owner_dc_key'
                ) THEN
                    ALTER TABLE cf_proxy_pool
                        ADD CONSTRAINT cf_proxy_pool_url_owner_dc_key UNIQUE (url, owner, owner_discord_id);
                END IF;
            END $$;
        """)
        await conn.execute(
            "ALTER TABLE cf_proxy_pool ADD COLUMN IF NOT EXISTS last_health_check DOUBLE PRECISION DEFAULT 0"
        )
        await conn.execute(
            "ALTER TABLE cf_proxy_pool ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER DEFAULT 0"
        )
        # pending NC 任务的 LOW 标记：True 时该任务由 LOW_ADMIN 触发，
        # 后台重试探测时必须使用 LOW_CF_PROXY_POOL。
        await conn.execute(
            "ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_low_admin BOOLEAN DEFAULT FALSE"
        )
        # pending NC 任务的 Discord 归属：用于 LOW 重试时定位正确的子池
        await conn.execute(
            "ALTER TABLE jb_accounts ADD COLUMN IF NOT EXISTS pending_nc_discord_id TEXT NOT NULL DEFAULT ''"
        )
        # 调用日志持久化表（任务5）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS call_logs (
                id             BIGSERIAL PRIMARY KEY,
                ts             DOUBLE PRECISION NOT NULL,
                model          TEXT NOT NULL DEFAULT '',
                api_key        TEXT NOT NULL DEFAULT '',
                discord_id     TEXT NOT NULL DEFAULT '',
                prompt_tokens  INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                elapsed_ms     INTEGER NOT NULL DEFAULT 0,
                status         TEXT NOT NULL DEFAULT '',
                exempt         BOOLEAN NOT NULL DEFAULT FALSE,
                created_at     TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_call_logs_ts ON call_logs(ts DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_call_logs_discord_id ON call_logs(discord_id)")
        # ── AI 响应缓存（仅 LOW 用户）──────────────────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_response_cache (
                cache_key         TEXT PRIMARY KEY,
                scope             TEXT NOT NULL DEFAULT 'low',
                owner_discord_id  TEXT NOT NULL DEFAULT '',
                client_key        TEXT NOT NULL DEFAULT '',
                route             TEXT NOT NULL,
                model             TEXT NOT NULL,
                request_hash      TEXT NOT NULL,
                response_json     JSONB NOT NULL,
                prompt_tokens     INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                hit_count         INTEGER NOT NULL DEFAULT 0,
                created_at        DOUBLE PRECISION NOT NULL,
                expires_at        DOUBLE PRECISION NOT NULL
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_response_cache_owner ON ai_response_cache(owner_discord_id, expires_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_response_cache_expires ON ai_response_cache(expires_at)")
        # ── Idempotency-Key 缓存（仅 LOW 用户）────────────────────────────────
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_idempotency_cache (
                idempotency_key  TEXT NOT NULL,
                owner_discord_id TEXT NOT NULL DEFAULT '',
                client_key       TEXT NOT NULL DEFAULT '',
                body_hash        TEXT NOT NULL,
                response_json    JSONB NOT NULL,
                created_at       DOUBLE PRECISION NOT NULL,
                expires_at       DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (owner_discord_id, idempotency_key)
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_ai_idempotency_expires ON ai_idempotency_cache(expires_at)")
    print("数据库表已就绪")


async def _save_accounts_to_db():
    """将当前账户列表持久化到数据库（无 DB 时回退到文件）；失败时抛出异常"""
    pool = await _get_db_pool()
    if not pool:
        async with file_write_lock:
            async with aiofiles.open("jetbrainsai.json", "w", encoding="utf-8") as f:
                await f.write(json.dumps(JETBRAINS_ACCOUNTS, indent=2))
        return
    async with pool.acquire() as conn:
        for acc in JETBRAINS_ACCOUNTS:
            acc_id = _account_id(acc)
            await conn.execute(
                """
                INSERT INTO jb_accounts
                    (id, license_id, auth_token, jwt, last_updated, last_quota_check, has_quota)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO UPDATE SET
                    license_id       = EXCLUDED.license_id,
                    auth_token       = EXCLUDED.auth_token,
                    jwt              = EXCLUDED.jwt,
                    last_updated     = EXCLUDED.last_updated,
                    last_quota_check = EXCLUDED.last_quota_check,
                    has_quota        = EXCLUDED.has_quota
                """,
                acc_id,
                acc.get("licenseId"),
                acc.get("authorization"),
                acc.get("jwt"),
                float(acc.get("last_updated") or 0),
                float(acc.get("last_quota_check") or 0),
                bool(acc.get("has_quota", True)),
            )
    print(f"已保存 {len(JETBRAINS_ACCOUNTS)} 个账户到数据库")


async def _delete_account_from_db(acc_id: str):
    """从数据库中删除指定 ID 的账户；失败时抛出异常"""
    pool = await _get_db_pool()
    if not pool:
        return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM jb_accounts WHERE id = $1", acc_id)


async def _batch_delete_accounts_from_db(ids: List[str]):
    """单条 SQL 批量删除多个账户（DELETE WHERE id = ANY(...)），远快于串行调用"""
    if not ids:
        return
    pool = await _get_db_pool()
    if not pool:
        return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM jb_accounts WHERE id = ANY($1::text[])", ids)


async def _batch_save_accounts_to_db(accounts: List[dict]):
    """批量 UPSERT 多个账户（executemany，单次连接完成），用于大量账号状态更新后的持久化"""
    if not accounts:
        return
    pool = await _get_db_pool()
    if not pool:
        return
    rows = [
        (
            _account_id(acc),
            acc.get("licenseId"),
            acc.get("authorization"),
            acc.get("jwt"),
            float(acc.get("last_updated") or 0),
            float(acc.get("last_quota_check") or 0),
            bool(acc.get("has_quota", True)),
            acc.get("daily_used"),
            acc.get("daily_total"),
        )
        for acc in accounts
    ]
    try:
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO jb_accounts
                    (id, license_id, auth_token, jwt, last_updated, last_quota_check, has_quota,
                     daily_used, daily_total)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (id) DO UPDATE SET
                    license_id                  = EXCLUDED.license_id,
                    auth_token                  = EXCLUDED.auth_token,
                    jwt                         = EXCLUDED.jwt,
                    last_updated                = EXCLUDED.last_updated,
                    last_quota_check            = EXCLUDED.last_quota_check,
                    has_quota                   = EXCLUDED.has_quota,
                    daily_used                  = EXCLUDED.daily_used,
                    daily_total                 = EXCLUDED.daily_total
                """,
                rows,
            )
    except Exception as e:
        print(f"[批量保存] 写入数据库出错: {e}")


async def _save_keys_to_db():
    """将客户端 API 密钥持久化到数据库（无 DB 时回退到文件）"""
    pool = await _get_db_pool()
    if not pool:
        async with file_write_lock:
            try:
                async with aiofiles.open("client_api_keys.json", "w", encoding="utf-8") as f:
                    await f.write(json.dumps(list(VALID_CLIENT_KEYS.keys()), indent=2))
            except Exception as e:
                print(f"保存 client_api_keys.json 时出错: {e}")
        return
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                current_keys = list(VALID_CLIENT_KEYS.keys())
                if current_keys:
                    await conn.execute(
                        "DELETE FROM jb_client_keys WHERE key != ALL($1::text[])",
                        current_keys,
                    )
                else:
                    await conn.execute("DELETE FROM jb_client_keys")
                rows = [
                    (
                        key,
                        meta.get("usage_limit"),
                        meta.get("usage_count", 0),
                        meta.get("account_id"),
                        bool(meta.get("banned", False)),
                        meta.get("banned_at"),
                        bool(meta.get("is_nc_key", False)),
                        bool(meta.get("is_low_admin_key", False)),
                        meta.get("low_admin_discord_id", "") or "",
                    )
                    for key, meta in VALID_CLIENT_KEYS.items()
                ]
                if rows:
                    await conn.executemany(
                        """INSERT INTO jb_client_keys (key, usage_limit, usage_count, account_id, banned, banned_at, is_nc_key, is_low_admin_key, low_admin_discord_id)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                           ON CONFLICT (key) DO UPDATE
                           SET usage_limit            = EXCLUDED.usage_limit,
                               usage_count            = EXCLUDED.usage_count,
                               account_id             = COALESCE(EXCLUDED.account_id, jb_client_keys.account_id),
                               banned                 = EXCLUDED.banned,
                               banned_at              = EXCLUDED.banned_at,
                               is_nc_key              = GREATEST(jb_client_keys.is_nc_key, EXCLUDED.is_nc_key),
                               is_low_admin_key       = GREATEST(jb_client_keys.is_low_admin_key, EXCLUDED.is_low_admin_key),
                               low_admin_discord_id   = COALESCE(NULLIF(EXCLUDED.low_admin_discord_id,''), jb_client_keys.low_admin_discord_id)""",
                        rows,
                    )
    except Exception as e:
        print(f"保存密钥到数据库时出错: {e}")


async def _flush_key_increments_to_db():
    """将内存中积累的 key 用量增量批量写入 DB（executemany，单次连接完成）。
    由后台任务周期调用，也在 shutdown 时主动调用确保不丢数据。
    """
    global _pending_key_increments
    if not _pending_key_increments:
        return
    # 原子交换：立刻清空全局 dict，避免写 DB 期间漏掉新增的增量
    batch, _pending_key_increments = _pending_key_increments, {}
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.executemany(
                "UPDATE jb_client_keys SET usage_count = usage_count + $2 WHERE key = $1",
                list(batch.items()),
            )
    except Exception as e:
        print(f"[批量] 刷新 key 用量到 DB 失败: {e}")
        # 写入失败：把这批增量合并回 pending dict，下次重试
        for k, v in batch.items():
            _pending_key_increments[k] = _pending_key_increments.get(k, 0) + v


async def _flush_key_increments_loop():
    """后台周期任务：每 5 秒将累积的 key 用量增量刷新到 DB。"""
    while True:
        await asyncio.sleep(5)
        try:
            await _flush_key_increments_to_db()
        except Exception as e:
            print(f"[批量] key 用量定时刷新异常: {e}")


async def _save_account_to_db(account: dict, pool=None):
    """精准更新单个账户到数据库（避免全量重写）。pool 为 None 时使用主 DB 池。"""
    # 账号已被标记删除，跳过写入，防止 fire-and-forget 任务在删除后重新写回
    if account.get("_deleted"):
        return
    if pool is None:
        pool = await _get_db_pool()
    if not pool:
        # 无 DB 时回退到全量文件写入
        await _save_accounts_to_db()
        return
    try:
        acc_id = _account_id(account)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO jb_accounts
                    (id, license_id, auth_token, jwt, last_updated, last_quota_check, has_quota,
                     daily_used, daily_total)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (id) DO UPDATE SET
                    license_id                  = EXCLUDED.license_id,
                    auth_token                  = EXCLUDED.auth_token,
                    jwt                         = EXCLUDED.jwt,
                    last_updated                = EXCLUDED.last_updated,
                    last_quota_check            = EXCLUDED.last_quota_check,
                    has_quota                   = EXCLUDED.has_quota,
                    daily_used                  = EXCLUDED.daily_used,
                    daily_total                 = EXCLUDED.daily_total
                """,
                acc_id,
                account.get("licenseId"),
                account.get("authorization"),
                account.get("jwt"),
                float(account.get("last_updated") or 0),
                float(account.get("last_quota_check") or 0),
                bool(account.get("has_quota", True)),
                account.get("daily_used"),
                account.get("daily_total"),
            )
    except Exception as e:
        print(f"保存单个账户到数据库时出错: {e}")


async def _upsert_key_to_db(key: str, meta: dict, pool=None):
    """精准插入或更新单个 API 密钥到数据库（避免全量重写）。pool 为 None 时使用主 DB 池。"""
    if pool is None:
        pool = await _get_db_pool()
    if not pool:
        await _save_keys_to_db()
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jb_client_keys (key, usage_limit, usage_count, account_id, banned, banned_at, is_nc_key, is_low_admin_key, low_admin_discord_id)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                   ON CONFLICT (key) DO UPDATE
                   SET usage_limit            = EXCLUDED.usage_limit,
                       usage_count            = EXCLUDED.usage_count,
                       account_id             = COALESCE(EXCLUDED.account_id, jb_client_keys.account_id),
                       banned                 = EXCLUDED.banned,
                       banned_at              = EXCLUDED.banned_at,
                       is_nc_key              = GREATEST(jb_client_keys.is_nc_key, EXCLUDED.is_nc_key),
                       is_low_admin_key       = GREATEST(jb_client_keys.is_low_admin_key, EXCLUDED.is_low_admin_key),
                       low_admin_discord_id   = COALESCE(NULLIF(EXCLUDED.low_admin_discord_id,''), jb_client_keys.low_admin_discord_id)""",
                key,
                meta.get("usage_limit"),
                meta.get("usage_count", 0),
                meta.get("account_id"),
                bool(meta.get("banned", False)),
                meta.get("banned_at"),
                bool(meta.get("is_nc_key", False)),
                bool(meta.get("is_low_admin_key", False)),
                meta.get("low_admin_discord_id", "") or "",
            )
    except Exception as e:
        print(f"精准写入密钥到数据库时出错: {e}")


async def _delete_key_from_db(key: str, pool=None):
    """精准删除单个 API 密钥（避免全量重写）。pool 为 None 时使用主 DB 池。"""
    if pool is None:
        pool = await _get_db_pool()
    if not pool:
        await _save_keys_to_db()
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM jb_client_keys WHERE key = $1", key)
    except Exception as e:
        print(f"精准删除密钥时出错: {e}")


def load_client_api_keys():
    """加载客户端 API 密钥（文件回退，支持旧格式 list[str]）"""
    global VALID_CLIENT_KEYS
    try:
        with open("client_api_keys.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                # 旧格式：字符串列表，迁移为无限制密钥
                VALID_CLIENT_KEYS = {k: {"usage_limit": None, "usage_count": 0} for k in data if k}
            else:
                print("警告: client_api_keys.json 格式不正确")
                VALID_CLIENT_KEYS = {}
                return
            if not VALID_CLIENT_KEYS:
                print("警告: client_api_keys.json 为空")
            else:
                print(f"成功加载 {len(VALID_CLIENT_KEYS)} 个客户端 API 密钥")
    except FileNotFoundError:
        print("错误: 未找到 client_api_keys.json")
        VALID_CLIENT_KEYS = {}
    except Exception as e:
        print(f"加载 client_api_keys.json 时出错: {e}")
        VALID_CLIENT_KEYS = {}


def load_jetbrains_accounts():
    """加载 JetBrains AI 认证信息"""
    global JETBRAINS_ACCOUNTS
    try:
        with open("jetbrainsai.json", "r", encoding="utf-8") as f:
            accounts_data = json.load(f)

        if not isinstance(accounts_data, list):
            print("警告: jetbrainsai.json 格式不正确，应为对象列表")
            JETBRAINS_ACCOUNTS = []
            return

        processed_accounts = []
        for account in accounts_data:
            processed_accounts.append(
                {
                    "licenseId": account.get("licenseId"),
                    "authorization": account.get("authorization"),
                    "jwt": account.get("jwt"),
                    "last_updated": account.get("last_updated", 0),
                    "has_quota": account.get("has_quota", True),
                    "last_quota_check": account.get("last_quota_check", 0),
                }
            )

        JETBRAINS_ACCOUNTS = processed_accounts
        if not JETBRAINS_ACCOUNTS:
            print("警告: jetbrainsai.json 中未找到有效的认证信息")
        else:
            print(f"成功加载 {len(JETBRAINS_ACCOUNTS)} 个 JetBrains AI 账户")

    except FileNotFoundError:
        print("错误: 未找到 jetbrainsai.json 文件")
        JETBRAINS_ACCOUNTS = []
    except Exception as e:
        print(f"加载 jetbrainsai.json 时出错: {e}")
        JETBRAINS_ACCOUNTS = []


async def load_cf_proxies_from_db():
    """从数据库加载两套 CF 代理池：
       - owner='admin'     → jb_activate.CF_PROXY_POOL（主池）
       - owner='low_admin' → jb_activate.LOW_CF_PROXY_POOL（LOW 用户专属，与主池完全隔离）
    """
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        import importlib
        jb_mod = importlib.import_module("jb_activate")
        admin_rows = await pool.fetch(
            "SELECT url FROM cf_proxy_pool WHERE is_active=TRUE AND owner='admin' ORDER BY id"
        )
        low_rows = await pool.fetch(
            "SELECT url, owner_discord_id FROM cf_proxy_pool "
            "WHERE is_active=TRUE AND owner='low_admin' ORDER BY owner_discord_id, id"
        )
        jb_mod.CF_PROXY_POOL = [r["url"] for r in admin_rows]
        # 按 Discord ID 分桶；空字符串桶代表未绑定 Discord 的兜底子池
        low_buckets: Dict[str, list] = {}
        for r in low_rows:
            dc = str(r["owner_discord_id"] or "")
            low_buckets.setdefault(dc, []).append(r["url"])
        jb_mod.LOW_CF_PROXY_POOL = low_buckets
        # 重置轮询游标（旧 Discord ID 的游标可保留，但避免遗留指向已删除子池）
        jb_mod._low_proxy_idx = {dc: 0 for dc in low_buckets.keys()}
        if jb_mod.CF_PROXY_POOL:
            print(f"[CF代理池-主] 已加载 {len(jb_mod.CF_PROXY_POOL)} 个代理")
        else:
            print("[CF代理池-主] 未配置代理，使用直连模式")
        if low_buckets:
            total = sum(len(v) for v in low_buckets.values())
            print(f"[CF代理池-LOW] 已加载 {total} 个代理，覆盖 {len(low_buckets)} 个 Discord 子池")
        else:
            print("[CF代理池-LOW] 未配置专属代理（LOW 激活将走直连）")
    except Exception as e:
        print(f"[CF代理池] 加载失败: {e}")


async def load_accounts_from_db():
    """从数据库加载账户列表，若 DB 为空则从 JSON 迁移"""
    global JETBRAINS_ACCOUNTS
    pool = await _get_db_pool()
    if not pool:
        load_jetbrains_accounts()
        return
    try:
        rows = await pool.fetch(
            "SELECT id, jwt, auth_token, license_id, last_updated, has_quota, last_quota_check,"
            " daily_used, daily_total"
            " FROM jb_accounts WHERE jwt IS NOT NULL ORDER BY created_at"
        )
        if rows:
            def _row_to_account(row) -> dict:
                acc = {
                    "jwt": row["jwt"],
                    "last_updated": row["last_updated"] or 0,
                    "has_quota": row["has_quota"],
                    "last_quota_check": row["last_quota_check"] or 0,
                    "daily_used": int(row["daily_used"]) if row["daily_used"] is not None else None,
                    "daily_total": int(row["daily_total"]) if row["daily_total"] is not None else None,
                }
                if row["license_id"] is not None:
                    acc["licenseId"] = row["license_id"]
                if row["auth_token"] is not None:
                    acc["authorization"] = row["auth_token"]
                return acc
            JETBRAINS_ACCOUNTS = [_row_to_account(row) for row in rows]
            print(f"从数据库加载 {len(JETBRAINS_ACCOUNTS)} 个账户")
            # 迁移旧格式 ID（jwt:前16字符）→ 新格式（jwt:SHA256哈希）
            old_ids = [
                row["id"] for row in rows
                if row["id"].startswith("jwt:") and len(row["id"]) <= 20
            ]
            if old_ids:
                print(f"检测到 {len(old_ids)} 个旧格式账户 ID，正在迁移...")
                try:
                    await _batch_delete_accounts_from_db(old_ids)
                    await _batch_save_accounts_to_db(JETBRAINS_ACCOUNTS)
                    print(f"账户 ID 迁移完成")
                except Exception as migrate_err:
                    print(f"账户 ID 迁移失败（不影响运行）: {migrate_err}")
        else:
            load_jetbrains_accounts()
            if JETBRAINS_ACCOUNTS:
                print(f"从 JSON 迁移 {len(JETBRAINS_ACCOUNTS)} 个账户到数据库...")
                try:
                    await _batch_save_accounts_to_db(JETBRAINS_ACCOUNTS)
                    print(f"迁移成功，已写入数据库")
                except Exception as migrate_err:
                    print(f"迁移到数据库失败（将只在内存中运行）: {migrate_err}")
    except Exception as e:
        print(f"从数据库加载账户失败，回退到文件: {e}")
        load_jetbrains_accounts()


async def load_keys_from_db():
    """从数据库加载客户端 API 密钥（含 usage_limit/usage_count），若 DB 为空则从 JSON 迁移"""
    global VALID_CLIENT_KEYS
    pool = await _get_db_pool()
    if not pool:
        load_client_api_keys()
        return
    try:
        rows = await pool.fetch("SELECT key, usage_limit, usage_count, account_id, banned, banned_at, is_nc_key, is_low_admin_key, low_admin_discord_id FROM jb_client_keys")
        if rows:
            VALID_CLIENT_KEYS = {
                row["key"]: {
                    "usage_limit": row["usage_limit"],
                    "usage_count": row["usage_count"] or 0,
                    "account_id": row["account_id"],
                    "banned": bool(row["banned"]) if row["banned"] is not None else False,
                    "banned_at": float(row["banned_at"]) if row["banned_at"] is not None else None,
                    "is_nc_key": bool(row["is_nc_key"]) if row["is_nc_key"] is not None else False,
                    "is_low_admin_key": bool(row["is_low_admin_key"]) if row["is_low_admin_key"] is not None else False,
                    "low_admin_discord_id": row["low_admin_discord_id"] or "",
                }
                for row in rows
            }
            print(f"从数据库加载 {len(VALID_CLIENT_KEYS)} 个客户端 API 密钥")
        else:
            load_client_api_keys()
            if VALID_CLIENT_KEYS:
                print(f"从 JSON 迁移 {len(VALID_CLIENT_KEYS)} 个密钥到数据库...")
                await _save_keys_to_db()
    except Exception as e:
        print(f"从数据库加载密钥失败，回退到文件: {e}")
        load_client_api_keys()


async def load_pokeball_keys_from_db():
    """从数据库加载宝可梦球虚拟 key 到内存（单次 JOIN 查询，消除 N+1）"""
    global POKEBALL_KEYS
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT p.ball_key, p.id, p.name, p.capacity, p.total_used, p.rr_index,
                       pm.member_key
                FROM pokeballs p
                LEFT JOIN pokeball_members pm ON pm.pokeball_id = p.id
                ORDER BY p.id, pm.id
            """)
        new_map: Dict[str, dict] = {}
        for row in rows:
            bk = row["ball_key"]
            if bk not in new_map:
                new_map[bk] = {
                    "id":         row["id"],
                    "name":       row["name"],
                    "capacity":   row["capacity"],
                    "total_used": row["total_used"],
                    "rr_index":   row["rr_index"],
                    "members":    [],
                }
            if row["member_key"]:
                new_map[bk]["members"].append(row["member_key"])
        POKEBALL_KEYS = new_map
        if POKEBALL_KEYS:
            print(f"从数据库加载 {len(POKEBALL_KEYS)} 个宝可梦球 key")
    except Exception as e:
        print(f"加载宝可梦球 key 失败: {e}")


def get_model_item(model_id: str) -> Optional[Dict]:
    """根据模型ID获取模型配置（O(1) 字典查找）"""
    return _model_dict.get(model_id)


def _validate_key(key: str) -> None:
    """仅验证密钥有效性和用量上限（不消耗用量）"""
    # 宝可梦球虚拟 key
    pb = POKEBALL_KEYS.get(key)
    if pb is not None:
        if pb["total_used"] >= pb["capacity"]:
            raise HTTPException(status_code=429, detail="宝可梦球额度已耗尽")
        # 检查是否有成员 key 还有余量
        for mk in pb["members"]:
            meta = VALID_CLIENT_KEYS.get(mk)
            if meta and not meta.get("banned"):
                lim = meta.get("usage_limit")
                cnt = meta.get("usage_count", 0)
                if lim is None or cnt < lim:
                    return
        raise HTTPException(status_code=429, detail="宝可梦球内所有密钥用量已耗尽")

    meta = VALID_CLIENT_KEYS.get(key)
    if meta is None:
        raise HTTPException(status_code=403, detail="无效的客户端 API 密钥")
    if meta.get("banned"):
        raise HTTPException(
            status_code=403,
            detail="This API key has been banned. Please contact the administrator.",
        )
    limit = meta.get("usage_limit")
    count = meta.get("usage_count", 0)
    if limit is not None and count >= limit:
        raise HTTPException(
            status_code=429,
            detail=f"API 密钥使用次数已达上限（{limit} 次）",
        )


async def _increment_pokeball_usage_in_db(ball_id: int, rr_index: int):
    """在数据库中更新宝可梦球总用量和轮询指针（fire-and-forget）"""
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE pokeballs SET total_used = total_used + 1, rr_index = $1 WHERE id = $2",
                rr_index, ball_id,
            )
    except Exception as e:
        print(f"更新宝可梦球用量失败: {e}")


def _pokeball_pick_member(ball_key: str) -> Optional[str]:
    """轮询选取宝可梦球中下一个有余量的成员 key（不消耗用量）"""
    pb = POKEBALL_KEYS.get(ball_key)
    if not pb or not pb["members"]:
        return None
    members = pb["members"]
    n = len(members)
    start = pb["rr_index"]
    for i in range(n):
        mk = members[(start + i) % n]
        meta = VALID_CLIENT_KEYS.get(mk)
        if meta and not meta.get("banned"):
            lim = meta.get("usage_limit")
            cnt = meta.get("usage_count", 0)
            if lim is None or cnt < lim:
                new_rr = (start + i + 1) % n
                pb["rr_index"] = new_rr
                pb["total_used"] = pb.get("total_used", 0) + 1
                asyncio.create_task(_increment_pokeball_usage_in_db(pb["id"], new_rr))
                return mk
    return None


def _consume_key_usage(key: str, cost: float = 1.0) -> None:
    """消耗密钥用量（支持小数成本，通过累积器实现精确的小数消耗）。
    DB 写入不再 fire-and-forget，而是累积到 _pending_key_increments 由后台批量刷新。
    """
    # 宝可梦球：轮询选取成员 key 并消耗其用量
    if key in POKEBALL_KEYS:
        member = _pokeball_pick_member(key)
        if member:
            _consume_key_usage(member, cost)
        return

    meta = VALID_CLIENT_KEYS.get(key)
    if meta is None:
        return

    # 整数部分直接消耗
    integer_part = int(cost)
    fractional_part = cost - integer_part

    # 累积小数部分，凑满 1 才额外消耗一次
    if fractional_part > 0:
        _key_fractional_usage[key] = _key_fractional_usage.get(key, 0.0) + fractional_part
        if _key_fractional_usage[key] >= 1.0:
            integer_part += 1
            _key_fractional_usage[key] -= 1.0

    increment = max(1, integer_part)
    meta["usage_count"] = meta.get("usage_count", 0) + increment
    # 累积到 pending dict，由 _flush_key_increments_loop 批量写 DB（每 5 秒一次）
    _pending_key_increments[key] = _pending_key_increments.get(key, 0) + increment


async def _stream_with_key_consume(
    stream: AsyncGenerator[str, None], key: str, model: str = "",
    usage_capture: Optional[Dict[str, int]] = None,
    est_prompt_tokens: int = 0,
) -> AsyncGenerator[str, None]:
    """包装流式生成器：在流结束时按真实/估算 token 决定是否计费。
    满足 _is_call_exempt（输入/输出均 < 阈值）则不消耗 key 用量（豁免）。
    usage_capture: 由 openai_stream_adapter 写入的真实 token；若无则使用估算值。

    使用 try/finally：客户端中途断开时（GeneratorExit/CancelledError），
    仍按已累积的输出做出计费决策，避免「断流跳过计费」的滥用。
    """
    cost = MODEL_COSTS.get(model, 1.0)
    has_content = False
    completion_chars = 0
    inline_usage_p = 0
    inline_usage_c = 0
    try:
        async for chunk in stream:
            if chunk.startswith("data: ") and chunk.strip() != "data: [DONE]":
                try:
                    obj = json.loads(chunk[6:].strip())
                    for choice in obj.get("choices", []):
                        delta = choice.get("delta", {})
                        if delta.get("content") or delta.get("tool_calls"):
                            has_content = True
                        content = delta.get("content") or ""
                        if content:
                            completion_chars += len(content)
                    # 兼容：若 SSE 中带了 usage 块（include_usage=True 时），也尝试采集
                    u = obj.get("usage") or {}
                    if u:
                        if u.get("prompt_tokens"):
                            inline_usage_p = int(u["prompt_tokens"])
                        if u.get("completion_tokens"):
                            inline_usage_c = int(u["completion_tokens"])
                except Exception:
                    pass
            yield chunk
    finally:
        # 无论是正常结束还是被取消（客户端断开），都按已累积内容做计费决策
        if has_content:
            cap = usage_capture or {}
            final_p = cap.get("prompt_tokens") or inline_usage_p or est_prompt_tokens or 0
            final_c = cap.get("completion_tokens") or inline_usage_c or max(1, completion_chars // 4)
            if not _is_call_exempt(final_p, final_c):
                try:
                    _consume_key_usage(key, cost=cost)
                except Exception as _e:
                    # 计费不能阻止资源清理；若失败仅记录
                    print(f"警告: _stream_with_key_consume finally 计费失败: {_e}")


async def authenticate_client(
    auth: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> str:
    """客户端认证 (OpenAI-style)，返回验证通过的 key"""
    if not VALID_CLIENT_KEYS:
        raise HTTPException(status_code=503, detail="服务不可用: 未配置客户端 API 密钥")

    if not auth or not auth.credentials:
        raise HTTPException(
            status_code=401,
            detail="需要在 Authorization header 中提供 API 密钥",
            headers={"WWW-Authenticate": "Bearer"},
        )

    _validate_key(auth.credentials)
    return auth.credentials


async def authenticate_any_client(
    auth: Optional[HTTPAuthorizationCredentials] = Depends(security),
    api_key: Optional[str] = Header(None, alias="x-api-key"),
) -> str:
    """客户端认证 (支持 OpenAI 和 Anthropic 风格)，返回验证通过的 key"""
    if not VALID_CLIENT_KEYS:
        raise HTTPException(status_code=503, detail="服务不可用: 未配置客户端 API 密钥")

    # 优先检查 x-api-key
    if api_key:
        _validate_key(api_key)
        return api_key

    # 其次检查 Authorization header
    if auth and auth.credentials:
        _validate_key(auth.credentials)
        return auth.credentials

    # 如果两者都未提供
    raise HTTPException(
        status_code=401,
        detail="需要在 Authorization header (Bearer) 或 x-api-key header 中提供 API 密钥",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def authenticate_anthropic_client(
    api_key: Optional[str] = Header(None, alias="x-api-key"),
) -> str:
    """客户端认证 (Anthropic-style)，返回验证通过的 key"""
    if not VALID_CLIENT_KEYS:
        raise HTTPException(status_code=503, detail="服务不可用: 未配置客户端 API 密钥")

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="需要在 x-api-key header 中提供 API 密钥",
        )

    _validate_key(api_key)
    return api_key



async def _check_quota(account: dict):
    """检查指定账户的配额。
    - 重入保护：同一账号已有检测在运行时直接跳过，防止并发重复刷新 JWT
    - JWT 刷新节流：仅在 JWT 缺失或超过 _JWT_REFRESH_INTERVAL_SECS 时才刷新
    """
    if not http_client:
        raise HTTPException(status_code=500, detail="HTTP 客户端未初始化")

    acc_id = _account_id(account)
    # ── 重入保护：同一账号并发调用时直接跳过 ──
    if acc_id in _quota_check_in_progress:
        return
    _quota_check_in_progress.add(acc_id)

    try:
        # ── JWT 刷新节流：仅在 JWT 缺失或超过刷新间隔时才调用，避免批量检测时 429 ──
        # pre_refresh_attempted=True 表示本次检测前已经尝试过刷新（无论结果），
        # 若 quota API 之后仍返回 401，不再二次刷新（避免 429→旧JWT→401→429 循环）
        pre_refresh_attempted = False
        if account.get("licenseId") and account.get("authorization"):
            jwt_missing = not account.get("jwt")
            last_upd = float(account.get("last_updated") or 0)
            jwt_stale = time.time() - last_upd > _JWT_REFRESH_INTERVAL_SECS
            if jwt_missing or jwt_stale:
                pre_refresh_attempted = True
                try:
                    await _refresh_jetbrains_jwt(account)
                except Exception as _e:
                    print(f"检查配额时 JWT 刷新失败（将用现有 JWT 继续）: {_e}")

        if not account.get("jwt"):
            print(f"账号 {account.get('licenseId', '?')} 无 JWT，无法查询配额")
            account["has_quota"] = False
            return

        headers = {
            "User-Agent": "ktor-client",
            "Content-Length": "0",
            "Accept-Charset": "UTF-8",
            "grazie-agent": '{"name":"aia:pycharm","version":"251.26094.80.13:251.26094.141"}',
            "grazie-authenticate-jwt": account["jwt"],
        }
        response = await http_client.post(
            "https://api.jetbrains.ai/user/v5/quota/get", headers=headers, timeout=10.0
        )

        if response.status_code == 401 and account.get("licenseId"):
            # 若本轮已尝试过刷新（无论成功/429/其他失败），不再二次尝试
            if pre_refresh_attempted:
                # JWT刷新失败 + quota API 401 ≠ 配额耗尽
                # 可能是 auth 端点临时异常（批量 401 通常是服务端问题而非账号问题）
                # 策略：不更新 has_quota，保留现状；若账号有 JWT，给一次实际 API 调用的机会
                # 实际 API 返回 477 才是真正无配额；401 可在 _make_jetbrains_raw_stream 里重试
                old_has_quota = account.get("has_quota")
                if account.get("jwt"):
                    # 有现存 JWT → 标记为"待验证"（True），让实际 API 调用做最终裁定
                    account["has_quota"] = True
                    account["quota_status_reason"] = "jwt_401_indeterminate_has_jwt"
                    print(f"Account {account.get('licenseId')} quota API 401 after JWT refresh, "
                          f"has existing JWT → set has_quota=True (was {old_has_quota}), "
                          f"will verify on next real API call")
                else:
                    # 无 JWT 且刷新失败，确实无法使用
                    account["has_quota"] = False
                    account["quota_status_reason"] = "jwt_401_no_jwt"
                    print(f"Account {account.get('licenseId')} quota API 401 after JWT refresh, no JWT → marking no quota")
                return
            # JWT 仍新鲜（未触发预刷新），但 quota API 返回 401 → JWT 在远端已过期，立即刷新一次
            print(f"JWT for {account['licenseId']} expired (still fresh locally), refreshing...")
            old_jwt = account.get("jwt")
            try:
                await _refresh_jetbrains_jwt(account)
            except Exception:
                pass
            new_jwt = account.get("jwt")
            if new_jwt and new_jwt != old_jwt:
                headers["grazie-authenticate-jwt"] = new_jwt
                response = await http_client.post(
                    "https://api.jetbrains.ai/user/v5/quota/get", headers=headers, timeout=10.0
                )
            else:
                print(f"Account {account.get('licenseId')} JWT refresh returned NONE state, marking no quota.")
                account["has_quota"] = False
                account["quota_status_reason"] = "jwt_state_none"
                if account.get("daily_total"):
                    account["daily_used"] = account["daily_total"]
                return

        if response.status_code == 401:
            print(f"Account {account.get('licenseId') or 'with static JWT'} quota check got persistent 401, marking no quota.")
            account["has_quota"] = False
            account["quota_status_reason"] = "jwt_401_unrecoverable"
            if account.get("daily_total"):
                account["daily_used"] = account["daily_total"]
            return

        response.raise_for_status()
        quota_data = response.json()

        quota_obj = quota_data.get("current", {})
        tariff = quota_obj.get("tariffQuota", {})

        tariff_used      = float(tariff.get("current",   {}).get("amount", 0) or 0)
        tariff_total     = float(tariff.get("maximum",   {}).get("amount", 0) or 0)
        tariff_available = float(tariff.get("available", {}).get("amount", 0) or 0)

        if tariff_total == 0:
            has_quota = True
        else:
            has_quota = tariff_available > 0

        account["has_quota"]   = has_quota
        account["daily_used"]  = int(tariff_total - tariff_available) if tariff_total > 0 else 0
        account["daily_total"] = int(tariff_total)
        # 成功确认配额 → 清除 JWT 冷却标记，账号立即重新参与轮询
        if has_quota:
            account.pop("last_jwt_fail", None)
            account.pop("quota_status_reason", None)
        print(f"Account {account.get('licenseId') or 'with static JWT'} "
              f"AI Credits: {int(tariff_available):,}/{int(tariff_total):,} "
              f"(used {int(tariff_used):,}) → has_quota={has_quota}")

    except Exception as e:
        print(f"Error checking quota for account {acc_id}: {e}")
        # 异常时保留现有状态，只更新检查时间
    finally:
        _quota_check_in_progress.discard(acc_id)
        account["last_quota_check"] = time.time()
        try:
            await _save_account_to_db(account)
        except Exception as e:
            print(f"[后台] 保存账户配额状态到数据库时出错: {e}")


async def _check_quota_fast(account: dict) -> bool:
    """极速配额检查：直接用现有 JWT 查询 /quota/get，全程不做 JWT 刷新。

    设计原则：
    - 不触发 JWT 刷新（保持极速，无论账号数量多少均不增加刷新开销）
    - 不调用 _save_account_to_db（由调用方批量写库）
    - 429 限流：原地等待 2s 后重试一次，仍 429 则保留现有 has_quota 不变
    - 401 处理分两类：
        * 普通账号（无 jwt_ 误标原因）→ 标记 has_quota=False
        * 被旧 bug 误标的账号（quota_status_reason 以 jwt_ 开头，且账号仍有 JWT）
          → 保留 has_quota 不变（不降级），等下次真实请求时懒刷新 JWT
    - 配额确认有效时：清除 last_jwt_fail 冷却标记，账号立即重新参与轮询
    适用场景：全量极速扫描，保证速度的同时不误伤被旧 bug 标错的账号。
    """
    acc_id = _account_id(account)
    if acc_id in _quota_check_in_progress:
        return account.get("has_quota", True)

    jwt_val = account.get("jwt")
    if not jwt_val:
        account["has_quota"] = False
        account["last_quota_check"] = time.time()
        return False

    _quota_check_in_progress.add(acc_id)
    try:
        headers = {
            "User-Agent": "ktor-client",
            "Content-Length": "0",
            "Accept-Charset": "UTF-8",
            "grazie-agent": '{"name":"aia:pycharm","version":"251.26094.80.13:251.26094.141"}',
            "grazie-authenticate-jwt": jwt_val,
        }
        for _attempt in range(2):
            try:
                response = await http_client.post(
                    "https://api.jetbrains.ai/user/v5/quota/get", headers=headers, timeout=8.0
                )
            except Exception as req_e:
                print(f"[fast-recheck] {acc_id} 网络异常: {req_e}")
                account["last_quota_check"] = time.time()
                return account.get("has_quota", True)

            sc = response.status_code
            if sc == 429:
                if _attempt == 0:
                    await asyncio.sleep(2.0)
                    continue
                # 二次仍 429：保留现有状态
                account["last_quota_check"] = time.time()
                return account.get("has_quota", True)

            if sc == 401:
                # 被旧 bug 误标的账号（quota_status_reason 以 jwt_ 开头）且仍有 JWT：
                # 不做降级，保留现有状态；JWT 刷新留给下次真实请求懒处理
                _is_jwt_mismarked = str(account.get("quota_status_reason", "")).startswith("jwt_")
                if _is_jwt_mismarked and account.get("jwt"):
                    account["last_quota_check"] = time.time()
                    return account.get("has_quota", True)
                # 普通账号 401 → 确认无配额
                account["has_quota"] = False
                account["last_quota_check"] = time.time()
                return False

            if sc != 200:
                account["last_quota_check"] = time.time()
                return account.get("has_quota", True)

            try:
                data = response.json()
            except Exception:
                account["last_quota_check"] = time.time()
                return account.get("has_quota", True)

            quota_obj = data.get("current", {})
            tariff = quota_obj.get("tariffQuota", {})
            tariff_total     = float(tariff.get("maximum",   {}).get("amount", 0) or 0)
            tariff_available = float(tariff.get("available", {}).get("amount", 0) or 0)
            has_q = (tariff_total == 0) or (tariff_available > 0)
            account["has_quota"]   = has_q
            account["daily_used"]  = int(tariff_total - tariff_available) if tariff_total > 0 else 0
            account["daily_total"] = int(tariff_total)
            account["last_quota_check"] = time.time()
            # 成功确认有配额 → 清除 JWT 冷却标记，账号立即重新参与轮询
            if has_q:
                account.pop("last_jwt_fail", None)
                account.pop("quota_status_reason", None)
            return has_q
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[fast-recheck] {acc_id} 异常: {e}")
        account["last_quota_check"] = time.time()
        return account.get("has_quota", True)
    finally:
        _quota_check_in_progress.discard(acc_id)


async def _refresh_jetbrains_jwt(account: dict):
    async with _jwt_refresh_sem:
        """使用 licenseId 和 authorization 刷新 JWT"""
        if not http_client:
            raise HTTPException(status_code=500, detail="HTTP 客户端未初始化")

        license_id = account.get("licenseId", "<unknown>")
        auth_token = account.get("authorization")

        if not auth_token:
            print(f"无法刷新 licenseId {license_id} 的 JWT：账号缺少 authorization 令牌（需重新激活）")
            raise HTTPException(
                status_code=503,
                detail=f"账号 {license_id} 的 JWT 已过期且无法自动刷新，请在管理面板重新激活此账号",
            )

        print(f"正在为 licenseId {license_id} 刷新 JWT...")
        try:
            headers = {
                "User-Agent": "ktor-client",
                "Content-Type": "application/json",
                "Accept-Charset": "UTF-8",
                "authorization": f"Bearer {auth_token}",
            }

            _lite_url = "https://api.jetbrains.ai/auth/jetbrains-jwt/license/obtain/grazie-lite"
            _jwt_url = "https://api.jetbrains.ai/auth/jetbrains-jwt/provide-access/license/v2"
            stored_lid = account.get("licenseId", "")
            actual_license_id = stored_lid or license_id

            # ★ free-tier 账号（非 AIP- 开头，且已有存储的 licenseId）直接使用存储的 licenseId 刷新，
            # 跳过 obtain/grazie-lite，防止覆盖为 grazie-lite（10K）licenseId
            is_free_tier = bool(stored_lid) and not stored_lid.startswith("AIP")
            if not is_free_tier:
                # grazie.individual.lite / AIP 账号：先 obtain/grazie-lite 确认最新 licenseId
                lite_response = await http_client.post(_lite_url, headers=headers, timeout=DEFAULT_REQUEST_TIMEOUT)
                if lite_response.status_code == 200:
                    lite_data = lite_response.json()
                    lite_lic = lite_data.get("license", {})
                    if isinstance(lite_lic, dict) and lite_lic.get("licenseId"):
                        actual_license_id = lite_lic["licenseId"]
                        print(f"  [refresh] grazie-lite → licenseId={actual_license_id}")
            else:
                print(f"  [refresh] free-tier 账号，直接使用存储 licenseId={actual_license_id}")

            payload = {"licenseId": actual_license_id}
            response = await http_client.post(_jwt_url, json=payload, headers=headers, timeout=DEFAULT_REQUEST_TIMEOUT)
            # 429 限流：等待后重试一次
            if response.status_code == 429:
                await asyncio.sleep(3 + (hash(license_id) % 5))   # 3~7s 抖动，避免雷同账号同时重试
                response = await http_client.post(_jwt_url, json=payload, headers=headers, timeout=DEFAULT_REQUEST_TIMEOUT)
            response.raise_for_status()

            data = response.json()
            new_token = data.get("token")
            state = data.get("state", "")
            # grazie.individual.lite 账号的 state 始终为 NONE，但 token 完全有效
            VALID_STATES = {"ACTIVE", "PAID", "TRIAL", "FULL", "NONE"}
            if new_token and state in VALID_STATES:
                account["jwt"] = new_token
                account["last_updated"] = time.time()
                print(f"成功刷新 licenseId {account['licenseId']} 的 JWT (state={state})")
                try:
                    await _save_account_to_db(account)
                except Exception as e:
                    print(f"[后台] 保存刷新后 JWT 到数据库时出错: {e}")
            else:
                print(f"刷新 JWT 失败: 响应中无 token 或 state 未知，state={state}")
                raise HTTPException(status_code=500, detail=f"刷新 JWT 失败: {data}")

        except httpx.HTTPStatusError as e:
            print(f"刷新 JWT 时 HTTP 错误: {e.response.status_code} {e.response.text}")
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"刷新 JWT 失败: {e.response.text}",
            )
        except Exception as e:
            print(f"刷新 JWT 时发生未知错误: {e}")
            raise HTTPException(status_code=500, detail=f"刷新 JWT 时发生未知错误: {e}")


async def get_next_jetbrains_account(client_key: Optional[str] = None) -> dict:
    """按优先级轮询 JetBrains 账户（has_quota=False 的账号始终跳过）：
    1. 当前请求 key 绑定的账号（优先保障专属配额）
    2. 没有任何 key 绑定的账号（公共池，轮询）
    3. 有其他 key 绑定的账号（兜底，轮询）
    """
    global current_account_index

    if not JETBRAINS_ACCOUNTS:
        raise HTTPException(status_code=503, detail="服务不可用: 未配置 JetBrains 账户")

    # 取出当前 key 绑定的 account_id（支持逗号分隔多个账号）
    preferred_account_ids: set = set()
    if client_key:
        key_meta = VALID_CLIENT_KEYS.get(client_key)
        if key_meta:
            raw_aid = key_meta.get("account_id") or ""
            preferred_account_ids = {x.strip() for x in raw_aid.split(",") if x.strip()}

    async def _try_account(account: dict) -> bool:
        """尝试使 account 就绪（刷新 JWT / 检查配额），返回是否可用。

        优化点：
        - 配额重查窗口缩短至 120 秒（原 300 秒），更快感知账号恢复
        - JWT 刷新使用每账号独立锁，防止并发请求重复发起刷新（刷新后双重检查）
        """
        # ── JWT 冷却期检查：401-jwt-cooldown 后 300s 内直接跳过 ──
        last_jwt_fail = float(account.get("last_jwt_fail") or 0)
        if last_jwt_fail and time.time() - last_jwt_fail < 300:
            return False

        if not account.get("has_quota"):
            last_check = account.get("last_quota_check", 0)
            if time.time() - last_check >= 120:   # 缩短至 2 分钟，更快恢复
                try:
                    await _check_quota(account)   # finally 块内已调用 _save_account_to_db，无需重复
                except Exception as _qe:
                    print(f"[轮询重查] 账号 {_account_id(account)} 配额重查失败: {_qe}")
            if not account.get("has_quota"):
                return False

        if account.get("licenseId"):
            is_jwt_stale = time.time() - account.get("last_updated", 0) > 12 * 3600
            if not account.get("jwt") or is_jwt_stale:
                if not account.get("authorization"):
                    if account.get("jwt"):
                        account["last_updated"] = time.time()
                        print(f"账号 {account.get('licenseId')} 缺少 authorization，跳过刷新，使用现有 JWT")
                    else:
                        return False
                else:
                    # ── JWT 刷新去重：每账号一把锁，避免 N 个并发请求重复刷新 ──
                    acc_id = _account_id(account)
                    jwt_lock = _jwt_refresh_locks.setdefault(acc_id, asyncio.Lock())
                    async with jwt_lock:
                        # 双重检查：等待锁期间可能已被其他协程刷新
                        still_stale = time.time() - account.get("last_updated", 0) > 12 * 3600
                        if still_stale:
                            try:
                                await _refresh_jetbrains_jwt(account)
                            except Exception as _e:
                                print(f"账号 {account.get('licenseId')} JWT 刷新失败: {_e}")
                                # JWT 刷新失败 ≠ 配额耗尽；若有现存 JWT 仍可尝试使用，
                                # 避免 auth 端点临时故障时误伤整个账号池。
                                # 更新 last_updated 防止本次请求内再次触发刷新。
                                if account.get("jwt"):
                                    account["last_updated"] = time.time()
                                    print(f"账号 {account.get('licenseId')} 使用现有 JWT 继续（刷新失败但 JWT 存在）")
                                else:
                                    return False

                if not account.get("has_quota"):
                    await _check_quota(account)
                    if not account.get("has_quota"):
                        return False

        return bool(account.get("jwt"))

    # ── 第一步：持锁构建候选列表，并立即推进索引（防止并发请求堆在同一账号）──
    async with account_rotation_lock:
        n = len(JETBRAINS_ACCOUNTS)
        if n == 0:
            raise HTTPException(status_code=503, detail="服务不可用: 未配置 JetBrains 账户")

        # 主组：has_quota=True（或未检测过）的账号，优先处理，无需网络调用即可判断可用性
        group1: list = []  # 绑定了本次请求 key 的账号（最高优先）
        group2: list = []  # 没有任何 key 绑定的账号（公共池）
        group3: list = []  # 有其他 key 绑定的账号（兜底）
        # 重检组：has_quota=False 但 last_quota_check 已超 120s 的账号（可能已恢复）
        # 仅在主组全部耗尽时才进入，避免每次请求都触发大量 _check_quota 网络调用
        retry1: list = []
        retry2: list = []
        retry3: list = []

        # 预构建「所有已绑定账号 ID」集合：O(keys) 一次，避免 O(accounts×keys)
        bound_account_ids: set = set()
        for _v in VALID_CLIENT_KEYS.values():
            _raw = _v.get("account_id") or ""
            bound_account_ids.update(x.strip() for x in _raw.split(",") if x.strip())

        now_ts = time.time()
        _JWT_COOLDOWN = 300  # 与 _try_account 保持一致
        for i in range(n):
            idx = (current_account_index + i) % n
            acc = JETBRAINS_ACCOUNTS[idx]

            # 跳过 JWT 冷却期内的账号（_try_account 也会跳过，提前过滤节省并发槽）
            _last_jwt_fail = float(acc.get("last_jwt_fail") or 0)
            if _last_jwt_fail and now_ts - _last_jwt_fail < _JWT_COOLDOWN:
                continue

            is_confirmed_no_quota = acc.get("has_quota") is False
            if is_confirmed_no_quota:
                # 近期已确认无配额 → 完全跳过
                if now_ts - acc.get("last_quota_check", 0) < 120:
                    continue
                # 超过 120s 的无配额账号 → 进入重检组（仅兜底使用）
                acc_id = _account_id(acc)
                if preferred_account_ids and acc_id in preferred_account_ids:
                    retry1.append((idx, acc))
                elif acc_id not in bound_account_ids:
                    retry2.append((idx, acc))
                else:
                    retry3.append((idx, acc))
                continue

            # has_quota=True 或 None（从未检测过）→ 进入主组
            acc_id = _account_id(acc)
            if preferred_account_ids and acc_id in preferred_account_ids:
                group1.append((idx, acc))
            elif acc_id not in bound_account_ids:
                group2.append((idx, acc))
            else:
                group3.append((idx, acc))

        # 主组候选优先；重检组只在主组全部失败后才处理
        primary_candidates = group1 + group2 + group3
        retry_candidates = retry1 + retry2 + retry3
        candidates = primary_candidates + retry_candidates

        # 关键：立即推进索引到第一候选位，后续并发请求从下一个账号开始，分散负载
        if primary_candidates:
            current_account_index = (primary_candidates[0][0] + 1) % n
        elif retry_candidates:
            current_account_index = (retry_candidates[0][0] + 1) % n

    if not candidates:
        raise HTTPException(status_code=429, detail="所有 JetBrains 账户均已超出配额或无效")

    # ── 第二步：组内并行验证，组间保持优先顺序 ──
    # group1 > group2 > group3，同一组内分批并发就绪检查，取最先通过的账号。
    async def _first_ready(group: list, batch_size: int = 8) -> Optional[dict]:
        """分批并发验证同一优先组内的所有账号，返回第一个就绪的，或 None。

        生产事故修复（2026-04-27）：旧版一次性 create_task 全部候选（生产 group2
        可达 1.9 万账号），其中部分账号 jwt stale 会触发并发 JWT 刷新洪水 →
        grazie auth 429 + 全局 http_client 连接池（500 connections）被全部占满 →
        Python 后端整体瘫痪，所有路径 165–180s 后被客户端 abort。
        新版采用滑动窗口分批并发：每批最多 batch_size 个候选，找到 winner 立即返回。
        单次请求引发的并发 JWT 刷新上限收敛到 batch_size，可控且语义不变。
        """
        if not group:
            return None
        if len(group) == 1:
            _, acc = group[0]
            return acc if await _try_account(acc) else None

        for batch_start in range(0, len(group), batch_size):
            batch = group[batch_start: batch_start + batch_size]
            done_event = asyncio.Event()
            winner: list = []
            remaining = len(batch)

            async def _probe(acc: dict):
                nonlocal remaining
                try:
                    if await _try_account(acc):
                        if not done_event.is_set():
                            winner.append(acc)
                            done_event.set()
                except Exception:
                    pass
                finally:
                    remaining -= 1
                    if remaining <= 0:
                        done_event.set()  # 该批全部探测完成，解除阻塞

            tasks = [asyncio.create_task(_probe(acc)) for _, acc in batch]
            await done_event.wait()
            if winner:
                # 找到 winner，立即返回；该批剩余 task（≤ batch_size-1 个）后台继续完成
                return winner[0]
            # 该批全部失败（remaining 在 finally 里降到 0 触发 done_event）→ 进入下一批
        return None

    # 先处理主组（has_quota=True），按 key 绑定优先级；主组全部失败才尝试重检组
    for grp in (group1, group2, group3):
        chosen = await _first_ready(grp)
        if chosen:
            return chosen

    # 主组无可用账号 → 尝试重检组（has_quota=False 但超过 120s，可能已恢复）
    for grp in (retry1, retry2, retry3):
        chosen = await _first_ready(grp)
        if chosen:
            return chosen

    raise HTTPException(status_code=429, detail="所有 JetBrains 账户均已超出配额或无效")


_JB_STREAM_URL = "https://api.jetbrains.ai/user/v5/llm/chat/stream/v7"
_JB_STREAM_HEADERS_BASE = {
    "User-Agent": "ktor-client",
    "Accept": "text/event-stream",
    "Content-Type": "application/json",
    "Accept-Charset": "UTF-8",
    "Cache-Control": "no-cache",
    "grazie-agent": '{"name":"aia:pycharm","version":"251.26094.80.13:251.26094.141"}',
}


async def _make_jetbrains_raw_stream(account: dict, payload: dict, extra_headers: dict):
    """
    向 JetBrains AI 发起流式请求，逐行 yield 原始 SSE 文本。

    - 477（配额耗尽）→ 标记账号 has_quota=False，raise _AccountFailed
    - 401（JWT 失效，且刷新后仍无效）→ 标记账号 has_quota=False，raise _AccountFailed
    - 400（请求格式错误）→ yield 规范错误 SSE，return（不切换账号）
    - 其他错误 → raise（由上层决定处理方式）
    """
    acc_id = account.get("licenseId") or "static-jwt"
    current_jwt = account.get("jwt") or extra_headers.get("grazie-authenticate-jwt", "")

    def _mark_no_quota():
        account["has_quota"] = False
        now_ts = time.time()
        # 仅当距上次检测超过 60 秒时才安排后台配额重检（防止 477/401 响应风暴触发无限任务）
        if now_ts - account.get("last_quota_check", 0) > 60 and acc_id not in _quota_check_in_progress:
            account["last_quota_check"] = now_ts
            asyncio.create_task(_check_quota(account))
        asyncio.create_task(_save_account_to_db(account))

    for attempt in range(2):
        req_headers = {
            **_JB_STREAM_HEADERS_BASE,
            **extra_headers,
            "grazie-authenticate-jwt": current_jwt,
        }
        try:
            async with http_client.stream(
                "POST", _JB_STREAM_URL, json=payload, headers=req_headers
            ) as response:
                status = response.status_code
                if status == 477:
                    print(f"[477] Account {acc_id} has no quota.")
                    _mark_no_quota()
                    raise _AccountFailed(f"477:{acc_id}")
                if status == 401:
                    print(f"[401] Account {acc_id} JWT 已过期（第 {attempt+1} 次尝试）")
                    # 交给 except 分支里的 JWT 刷新逻辑处理
                    response.raise_for_status()
                if status == 400:
                    body = await response.aread()
                    body_str = body.decode("utf-8", errors="replace")
                    print(f"[400] Account {acc_id} bad request: {body_str[:300]}")
                    err = json.dumps({"error": {"message": f"JetBrains API 拒绝请求 (400): {body_str}", "type": "invalid_request_error", "code": "400"}})
                    yield f"data: {err}\n\n"
                    yield "data: [DONE]\n\n"
                    return
                if status == 413:
                    body = await response.aread()
                    body_str = body.decode("utf-8", errors="replace")
                    print(f"[413] Account {acc_id} payload too large: {body_str[:300]}")
                    err = json.dumps({"error": {"message": "请求内容过长，超出 JetBrains AI 单次请求限制，请缩短对话历史或 prompt 后重试", "type": "invalid_request_error", "code": "413"}})
                    yield f"data: {err}\n\n"
                    yield "data: [DONE]\n\n"
                    return
                response.raise_for_status()
                async for line in response.aiter_lines():
                    yield line
                return  # 成功
        except _AccountFailed:
            raise
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 477:
                print(f"[477] Account {acc_id} has no quota (except).")
                _mark_no_quota()
                raise _AccountFailed(f"477:{acc_id}") from e
            if code == 401:
                if attempt == 0 and account.get("licenseId") and account.get("authorization"):
                    # 有 authorization，尝试刷新 JWT 后重试
                    old_jwt = current_jwt
                    try:
                        await _refresh_jetbrains_jwt(account)
                        new_jwt = account.get("jwt")
                        if new_jwt and new_jwt != old_jwt:
                            current_jwt = new_jwt
                            print(f"[401] JWT 刷新成功，重试 ({acc_id})")
                            continue
                        else:
                            print(f"[401] JWT 未更新（state=NONE），账号无 AI 权限 ({acc_id})")
                    except _AccountFailed:
                        raise
                    except Exception as _re:
                        print(f"[401] JWT 刷新失败 ({acc_id}): {_re}")
                # 兜底：401 无法恢复（attempt=1 / 无 authorization / refresh 失败）
                # 401 = JWT 失效，不等于配额耗尽；不标 has_quota=False，
                # 改为打一个 JWT 冷却标记（300 秒内跳过此账号），避免误伤整个池子。
                account["last_jwt_fail"] = time.time()
                asyncio.create_task(_save_account_to_db(account))
                print(f"[401] 账号 {acc_id} JWT 失效，进入 300s 冷却（不标无配额）")
                raise _AccountFailed(f"401-jwt-cooldown:{acc_id}") from e
            if code == 400:
                body = getattr(e.response, "_content", b"")
                body_str = body.decode("utf-8", errors="replace")
                err = json.dumps({"error": {"message": f"JetBrains API 拒绝请求 (400): {body_str}", "type": "invalid_request_error", "code": "400"}})
                yield f"data: {err}\n\n"
                yield "data: [DONE]\n\n"
                return
            if code == 413:
                body = getattr(e.response, "_content", b"")
                body_str = body.decode("utf-8", errors="replace")
                print(f"[413] Account {acc_id} payload too large (except): {body_str[:300]}")
                err = json.dumps({"error": {"message": "请求内容过长，超出 JetBrains AI 单次请求限制，请缩短对话历史或 prompt 后重试", "type": "invalid_request_error", "code": "413"}})
                yield f"data: {err}\n\n"
                yield "data: [DONE]\n\n"
                return
            raise


async def _stream_with_account_fallback(
    first_account: dict,
    payload: dict,
    extra_headers: dict,
    client_key: Optional[str],
    max_fallbacks: int = 4,
):
    """
    Raw JetBrains SSE 流，带自动账号切换：
    - 当前账号返回 477/401 时，自动从池中挑选下一个有配额的账号重试。
    - 最多尝试 max_fallbacks+1 个账号；全部失败时向客户端推送规范的错误 SSE。
    - 切换动作发生在第一个 yield 之前（HTTP 级别错误），客户端不会察觉。
    """
    account = first_account
    tried_ids: set = {_account_id(account)}

    for _attempt in range(max_fallbacks + 1):
        try:
            async for line in _make_jetbrains_raw_stream(account, payload, extra_headers):
                yield line
            return  # 成功
        except _AccountFailed as af:
            print(f"[fallback] {af}，尝试下一个账号（已试 {len(tried_ids)} 个）")
            # 从剩余账号中挑一个有配额的（不再经过 lock + JWT 刷新，快速切换）
            # 从 current_account_index 开始迭代，配合全局轮询分散负载
            next_acc = None
            async with account_rotation_lock:
                _n = len(JETBRAINS_ACCOUNTS)
                for _i in range(_n):
                    _acc = JETBRAINS_ACCOUNTS[(current_account_index + _i) % _n]
                    if _account_id(_acc) not in tried_ids and _acc.get("has_quota", True) and _acc.get("jwt"):
                        next_acc = _acc
                        break
            if next_acc is None:
                # 所有快速候选用尽，尝试走完整路径再选一次
                try:
                    next_acc = await get_next_jetbrains_account(client_key=client_key)
                    if _account_id(next_acc) in tried_ids:
                        next_acc = None
                except HTTPException:
                    next_acc = None
            if next_acc is None:
                break
            tried_ids.add(_account_id(next_acc))
            account = next_acc

    # 所有账号均失败，向客户端推送规范错误事件
    err_payload = json.dumps({
        "error": {
            "message": "所有 JetBrains 账户配额已耗尽，请稍后重试",
            "type": "rate_limit_error",
            "code": "quota_exhausted",
        }
    })
    yield f"data: {err_payload}\n\n"
    yield "data: [DONE]\n\n"


# FastAPI 生命周期事件

async def _do_cleanup_pending_param_keys() -> dict:
    """手动触发：清除【等待返回参数】中不在排队行列里的 key。
    ① 删除已超过 40 分钟仍未绑定参数的密钥（usage_limit=0/NULL）。
    ② 清除 jb_accounts 中"僵尸 NC key"：
       pending_nc_key IS NOT NULL 但 pending_nc_lids 为空/NULL，
       说明该 key 不在任何排队行列里，永远不会被升额，直接清除。
    超时/僵尸 key 均从数据库和内存（VALID_CLIENT_KEYS）同步删除，并使管理缓存失效。
    返回清理结果 dict。"""
    result = {"expired_keys": 0, "zombie_keys": 0, "memory_removed": 0}
    pool = await _get_db_pool()
    if not pool:
        return result
    async with pool.acquire() as conn:
        # ① 超时待参数密钥（jb_client_keys）
        rows = await conn.fetch(
            """DELETE FROM jb_client_keys
               WHERE (usage_limit IS NULL OR usage_limit = 0)
                 AND (banned IS NULL OR banned = FALSE)
                 AND created_at < NOW() - INTERVAL '40 minutes'
                 AND (low_admin_discord_id IS NULL OR low_admin_discord_id = '')
               RETURNING key"""
        )
        if rows:
            expired_keys = [r["key"] for r in rows]
            removed = 0
            for k in expired_keys:
                if VALID_CLIENT_KEYS.pop(k, None) is not None:
                    removed += 1
            _admin_cache_invalidate("keys", "status")
            result["expired_keys"] = len(expired_keys)
            result["memory_removed"] += removed
            print(f"[待参数清理] 已删除 {len(expired_keys)} 个超时待参数密钥"
                  f"（其中 {removed} 个在内存中）")

        # ② 僵尸 NC key：有 pending_nc_key 但不在排队行列（lids 为空）
        #    且 key 尚未升额（usage_limit IS NULL 或 0 表示预签未升级），已升额（普通 25 / LOW 16）的不清理
        zombie_rows = await conn.fetch(
            """WITH target AS (
                   SELECT a.id, a.pending_nc_key AS old_key
                   FROM jb_accounts a
                   WHERE a.pending_nc_key IS NOT NULL
                     AND (a.pending_nc_lids IS NULL OR a.pending_nc_lids = '[]')
                     AND NOT EXISTS (
                         SELECT 1 FROM jb_client_keys k
                         WHERE k.key = a.pending_nc_key
                           AND k.usage_limit IS NOT NULL
                           AND k.usage_limit > 0
                     )
               ),
               cleared AS (
                   UPDATE jb_accounts
                   SET pending_nc_key       = NULL,
                       pending_nc_lids      = NULL,
                       pending_nc_bound_ids = NULL,
                       pending_nc_email     = NULL,
                       pending_nc_pass      = NULL
                   WHERE id IN (SELECT id FROM target)
                   RETURNING id
               )
               SELECT t.old_key FROM target t
               JOIN cleared c ON t.id = c.id"""
        )
        if zombie_rows:
            zombie_keys = [r["old_key"] for r in zombie_rows if r["old_key"]]
            zm_removed = 0
            if zombie_keys:
                await conn.execute(
                    """DELETE FROM jb_client_keys
                       WHERE key = ANY($1::text[])
                         AND (usage_limit IS NULL OR usage_limit = 0)""",
                    zombie_keys,
                )
                for k in zombie_keys:
                    # 双重保护：内存中 usage_limit 已 > 0（已升额，普通 25 / LOW 16）的不弹出
                    if VALID_CLIENT_KEYS.get(k, {}).get("usage_limit", 0) <= 0:
                        if VALID_CLIENT_KEYS.pop(k, None) is not None:
                            zm_removed += 1
            _admin_cache_invalidate("keys", "status")
            result["zombie_keys"] = len(zombie_rows)
            result["memory_removed"] += zm_removed
            print(f"[待参数清理] 已清除 {len(zombie_rows)} 条僵尸 NC key"
                  f"（其中 {zm_removed} 个同步从内存删除）")
    return result


async def _cleanup_pending_prizes_loop():
    """后台定时清理 _PENDING_PRIZES（每 5 分钟）。
    防止低流量时 spin_token 长期占用内存（超过 10 分钟视为过期）。"""
    while True:
        await asyncio.sleep(300)  # 5 分钟
        try:
            now = time.time()
            expired = [k for k, v in list(_PENDING_PRIZES.items()) if now - v["ts"] > 600]
            for k in expired:
                _PENDING_PRIZES.pop(k, None)
            if expired:
                print(f"[pending_prizes_gc] 清理 {len(expired)} 个过期 spin_token")
        except Exception as e:
            print(f"[pending_prizes_gc] 出错: {e}")


async def _startup_quota_check():
    """启动后对所有账号做一次强制配额检测（不受 last_quota_check 限制），
    确保服务重启后 has_quota 状态与实际 AI Credits 保持一致。

    生产事故减压（2026-04-27）：
      之前"启动立即扫全量 1864 账号" + grazie auth API 大量 429/401 →
      所有 JWT 刷新挤占全局 http_client 连接池 → 真实用户请求全部 PoolTimeout，
      症状："网站根本进不去，特别卡"。
      改为：① 启动后等 5 分钟（让真实流量先获得连接池资源）
            ② 并发降为 1、分块缩小为 10、块间休 1.5s
      影响：账号 has_quota 状态收敛变慢（约 30 → 60 分钟），
            但绝不会再因启动瞬间打爆连接池而瘫痪整个 Python 后端。
    """
    global current_account_index
    if not JETBRAINS_ACCOUNTS:
        return
    # 启动后让出 5 分钟给真实流量预热（admin-panel/v1 等接口先恢复响应）
    await asyncio.sleep(300)
    total_acc = len(JETBRAINS_ACCOUNTS)
    print(f"[启动检测] 启动 5 分钟缓冲后开始对 {total_acc} 个账号配额刷新（慢节奏，不抢真实流量）...")
    _CHUNK = 10
    semaphore = asyncio.Semaphore(1)   # 串行化，最大限度避免 JWT 刷新 429 + 池占用
    async def _check_one(acc: dict):
        async with semaphore:
            try:
                await _check_quota(acc)
            except Exception as e:
                print(f"[启动检测] 账号 {_account_id(acc)} 检测失败: {e}")
    snapshot = list(JETBRAINS_ACCOUNTS)
    for chunk_start in range(0, len(snapshot), _CHUNK):
        chunk = snapshot[chunk_start: chunk_start + _CHUNK]
        await asyncio.gather(*[_check_one(acc) for acc in chunk])
        # _check_quota.finally 已逐条保存，此处无需冗余 batch save
        await asyncio.sleep(1.5)  # 块间显著休息，给远端速率限制充分恢复时间

    # 删除确认无配额的账号（内存 + 数据库）
    no_quota_accs_startup = [a for a in JETBRAINS_ACCOUNTS if not a.get("has_quota")]
    no_quota_ids = [_account_id(a) for a in no_quota_accs_startup]
    if no_quota_ids:
        try:
            # 先打标，防止后续 fire-and-forget 任务重新写回
            for acc in no_quota_accs_startup:
                acc["_deleted"] = True
            await _batch_delete_accounts_from_db(no_quota_ids)
            delete_set = set(no_quota_ids)
            async with account_rotation_lock:
                JETBRAINS_ACCOUNTS[:] = [
                    a for a in JETBRAINS_ACCOUNTS if _account_id(a) not in delete_set
                ]
                if JETBRAINS_ACCOUNTS and current_account_index >= len(JETBRAINS_ACCOUNTS):
                    current_account_index = 0
            print(f"[启动检测] 已删除 {len(no_quota_ids)} 个无配额账号")
        except Exception as e:
            print(f"[启动检测] 删除无配额账号失败: {e}")

    has = sum(1 for a in JETBRAINS_ACCOUNTS if a.get("has_quota"))
    print(f"[启动检测] 完成：{has}/{total_acc} 个账号有配额，剩余 {len(JETBRAINS_ACCOUNTS)} 个")


def _schedule_quota_checks_for_ids(acc_ids: set, *, label: str = "入池检测") -> int:
    """对指定 acc_id 集合中、尚未做过配额检测（last_quota_check==0）的账号，
    用 semaphore(2) 串行受控地在单个后台 task 内执行 _check_quota。
    返回实际安排的账号数量。"""
    targets = [
        acc for acc in list(JETBRAINS_ACCOUNTS)
        if _account_id(acc) in acc_ids and acc.get("last_quota_check", 0) == 0
    ]
    if not targets:
        return 0

    async def _run():
        sem = asyncio.Semaphore(2)
        async def _one(acc: dict):
            async with sem:
                try:
                    await _check_quota(acc)
                except Exception as e:
                    print(f"[{label}] 检测失败 {_account_id(acc)}: {e}")
        await asyncio.gather(*[_one(acc) for acc in targets])

    asyncio.create_task(_run())
    print(f"[{label}] 已为 {len(targets)} 个新账号安排后台配额检测（并发=2）")
    return len(targets)


async def _startup_resume_bind_tasks():
    """启动时恢复所有 processing 状态的 self_register 任务；
    如果 result_keys 为空（旧 job），先补签一个 limit=0 的 key。"""
    await asyncio.sleep(3)  # 等 DB 池就绪
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT email, result_keys FROM self_register_jobs WHERE status = 'processing'"
            )
        if not rows:
            return
        cfg = await _get_client_cfg_for_push()
        if not cfg:
            print("[startup_resume] 无合作方配置，跳过恢复 bind_task")
            return
        for row in rows:
            email = row["email"]
            if email in _running_bind_tasks:
                print(f"[startup_resume] {email} bind_task 已在运行，跳过重复恢复")
                continue
            existing_key = (row["result_keys"] or "").strip()
            if not existing_key:
                # 旧 job 没有预签 key，补签一个 limit=0 的
                new_key = f"sk-jb-{secrets.token_hex(24)}"
                key_meta: dict = {"usage_limit": 0, "usage_count": 0, "account_id": ""}
                VALID_CLIENT_KEYS[new_key] = key_meta
                await _upsert_key_to_db(new_key, key_meta)
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE self_register_jobs SET result_keys=$2 WHERE email=$1 AND (result_keys IS NULL OR result_keys='')",
                        email, new_key
                    )
                print(f"[startup_resume] {email} 补签预签 key {new_key[:16]}…")
            print(f"[startup_resume] 恢复 bind_task: {email}")
            asyncio.create_task(_bind_accounts_for_key(email, cfg))
    except Exception as exc:
        print(f"[startup_resume] 异常: {exc}")


_pending_nc_last_retry_at: float = 0.0   # 上次批量重试的时间戳
_PENDING_NC_INTERVAL: int = 300          # 重试间隔（秒）
from collections import deque as _deque
_pending_nc_retry_log: _deque = _deque(maxlen=100)         # 主池滚动日志（普通 / 完整管理员激活的账号）
_pending_nc_retry_log_low: _deque = _deque(maxlen=100)     # LOW 用户专属滚动日志

# ────────── LOW_ADMIN 批量激活 / 并发配置 ──────────
_LOW_BATCH_MAX: int = 50                 # 单次批量最多账号数
_LOW_BATCH_COOLDOWN: int = 3600          # 批量激活冷却（秒）
# Per-Discord 批量激活冷却时间戳（dc_user_id → epoch_seconds）
# admin 用空字符串 "" 作为 key
_low_admin_last_batch_at: Dict[str, float] = {}

# LOW 用户并发：同时控制 (a) pending-nc 重试中 LOW 行的并发探测数；
# (b) 批量激活时一次启动多少个账号并行处理。与普通用户的固定 10 区分。
# 现在支持 per-Discord-account 独立配置：_low_discord_concurrency[discord_id] → int
# 未配置的 discord_id 回退到全局默认 _low_admin_concurrency。
_low_admin_concurrency: int = 3          # 全局默认（admin 发起时 / discord_id 未设置时使用）
_low_discord_concurrency: Dict[str, int] = {}  # per-Discord 并发覆盖
_LOW_CONCURRENCY_MAX: int = 50


def _get_low_concurrency(discord_id: str = "") -> int:
    """返回指定 Discord 账号的并发配置；未设置则回退到全局默认。"""
    if discord_id and discord_id in _low_discord_concurrency:
        return _low_discord_concurrency[discord_id]
    return _low_admin_concurrency


async def _load_low_admin_settings():
    """从 jb_settings 加载 LOW_ADMIN 全局并发 + 所有 per-Discord 并发配置"""
    global _low_admin_concurrency, _low_discord_concurrency
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT k, v FROM jb_settings "
                "WHERE k = 'low_admin_concurrency' OR k LIKE 'low_concurrency:%'"
            )
        new_dc_conc: Dict[str, int] = {}
        for row in rows:
            k, v = row["k"], (row["v"] or "").strip()
            if not v:
                continue
            try:
                val = int(v)
                if not (1 <= val <= _LOW_CONCURRENCY_MAX):
                    continue
                if k == "low_admin_concurrency":
                    _low_admin_concurrency = val
                elif k.startswith("low_concurrency:"):
                    dc_id = k[len("low_concurrency:"):]
                    if dc_id:
                        new_dc_conc[dc_id] = val
            except ValueError:
                pass
        _low_discord_concurrency = new_dc_conc
    except Exception as _e:
        print(f"[LOW] 加载并发配置失败（使用默认 {_low_admin_concurrency}）: {_e}")


async def _retry_pending_nc_lids():
    """
    后台任务：每 5 分钟扫描所有带 pending_nc_lids 的账号，
    尝试 provide-access/license/v2。一旦某个 licenseId 变为 trusted（200 OK），
    将其作为新账号加入池并清除 pending 记录。
    """
    global _pending_nc_last_retry_at, _pending_nc_retry_log, _pending_nc_retry_log_low
    import importlib, itertools as _itertools

    # ── 并发参数 ──
    _CONCURRENCY = 10
    AI_URL = "https://api.jetbrains.ai/auth/jetbrains-jwt/provide-access/license/v2"

    await asyncio.sleep(120)  # 首次延迟 2 分钟，等服务完全启动

    # 全局 proxy 轮询计数器（线程安全不需要，asyncio 单线程）
    _pnc_proxy_counter = 0

    # id_token 内存缓存：避免每轮都重新登录触发 JetBrains 限速
    # 格式：{email: {"token": str, "cached_at": float}}
    _id_token_cache: dict = {}
    _TOKEN_TTL = 55 * 60  # 55 分钟，JetBrains id_token 通常 1 小时有效

    def _log(msg: str, level: str = "info", is_low=None, low_msg: str = None):
        """
        is_low: None=全局事件（写主日志） / True=LOW 行事件（仅写 LOW 日志） / False=主行事件（仅写主日志）
        low_msg: 当日志会写入 LOW 用户面板时（is_low=True），优先使用此简化文案。
                 不暴露内部术语（trusted/Untrusted/licenseId/信任凭证/NC/492/批量等），
                 让 LOW 用户看到的日志与"普通用户单账号激活"的友好提示风格一致。
                 不传则用 msg。主管理员侧（is_low=False/None）始终用 msg，便于排查。
        """
        if is_low is True:
            shown = low_msg if low_msg else msg
            entry = {"ts": time.time(), "msg": shown, "level": level}
            _pending_nc_retry_log_low.append(entry)
            print(f"[pending-nc:LOW] {shown}")
        else:
            entry = {"ts": time.time(), "msg": msg, "level": level}
            _pending_nc_retry_log.append(entry)
            print(f"[pending-nc] {msg}")

    async def _probe_lid(lid: str, id_token: str, semaphore: asyncio.Semaphore,
                          proxy_pool: list) -> dict:
        """对单个 licenseId 发起探测，通过给定的 CF 代理池轮询，返回结果字典。
        proxy_pool 由调用方传入：LOW 任务传 LOW_CF_PROXY_POOL，普通任务传 CF_PROXY_POOL。
        """
        nonlocal _pnc_proxy_counter
        async with semaphore:
            try:
                hdrs = {"Authorization": f"Bearer {id_token}",
                        "User-Agent": "ktor-client", "Content-Type": "application/json"}
                via_proxy = False
                if proxy_pool:
                    proxy_url = proxy_pool[_pnc_proxy_counter % len(proxy_pool)]
                    _pnc_proxy_counter += 1
                    req_url = proxy_url
                    hdrs["x-target-url"] = AI_URL
                    via_proxy = True
                else:
                    req_url = AI_URL
                async with httpx.AsyncClient(timeout=20) as hc:
                    r = await hc.post(req_url, json={"licenseId": lid}, headers=hdrs)
                body_txt = r.text
                # 若通过代理且得到 492，做一次直连对比验证（直连结果最权威）
                if via_proxy and r.status_code == 492:
                    try:
                        direct_hdrs = {"Authorization": f"Bearer {id_token}",
                                       "User-Agent": "ktor-client", "Content-Type": "application/json"}
                        async with httpx.AsyncClient(timeout=20) as hc2:
                            r2 = await hc2.post(AI_URL, json={"licenseId": lid}, headers=direct_hdrs)
                        direct_body = r2.text[:300].replace("\n", " ")
                        if r2.status_code == 200:
                            _log(f"  [probe] {lid[:8]} 代理492 直连200 trusted，以直连为准", "info")
                            return {"lid": lid, "status": 200, "body": r2.text}
                        # 直连非200：打印真实返回，以直连结果为准
                        _log(
                            f"  [probe] {lid[:8]} 代理492 直连{r2.status_code}  直连body={direct_body!r}",
                            "warn",
                        )
                        return {"lid": lid, "status": r2.status_code, "body": r2.text}
                    except Exception as de:
                        _log(f"  [probe] {lid[:8]} 直连对比失败: {de}", "warn")
                return {"lid": lid, "status": r.status_code, "body": body_txt}
            except Exception as e:
                return {"lid": lid, "status": -1, "error": str(e)}

    # 排队记录的最长保留时间：超过此值未被任何激活流程触碰即自动清除
    # （last_updated 在 _retry_pending_nc_lids 内不会被刷新，因此可作为"入队时间"近似）
    _PENDING_NC_MAX_AGE_SEC = 3600  # 1 小时

    while True:
        try:
            _pending_nc_last_retry_at = time.time()
            db = await _get_db_pool()
            if not db:
                await asyncio.sleep(600)
                continue

            # ── 入队超时清理：排队 > 1 小时仍未完成的邮箱自动从排队列表移除 ──
            # 与手动清除接口（admin_pending_nc_delete）使用完全相同的字段重置，
            # 通过 RETURNING 拿到被清理的邮箱列表写入日志，前端排队记录页可见。
            # 用专用字段 pending_nc_enqueued_at 作为入队时间，仅在入队瞬间写入，
            # 重试与 JWT 刷新都不会更新它，避免误删/延后清理的语义偏差。
            try:
                now_ts = time.time()
                async with db.acquire() as conn:
                    expired_rows = await conn.fetch(
                        """
                        UPDATE jb_accounts
                           SET pending_nc_lids        = NULL,
                               pending_nc_key         = NULL,
                               pending_nc_bound_ids   = NULL,
                               pending_nc_enqueued_at = 0
                         WHERE pending_nc_lids IS NOT NULL
                           AND pending_nc_lids <> '[]'
                           AND pending_nc_enqueued_at > 0
                           AND ($1 - pending_nc_enqueued_at) > $2
                        RETURNING id, pending_nc_email,
                                  COALESCE(pending_nc_low_admin, FALSE) AS is_low
                        """,
                        now_ts, _PENDING_NC_MAX_AGE_SEC,
                    )
                if expired_rows:
                    age_min = _PENDING_NC_MAX_AGE_SEC // 60
                    for er in expired_rows:
                        em = er["pending_nc_email"] or er["id"]
                        short_em = em[:em.index("@")] + "@..." if "@" in em else em
                        is_low_row = bool(er["is_low"])
                        _log(
                            f"⏰ {short_em} 排队超过 {age_min} 分钟未完成，已自动从排队列表清除",
                            "warn", is_low=is_low_row,
                            low_msg=f"⏰ {short_em} 排队超过 {age_min} 分钟未完成，已自动取消激活",
                        )
            except Exception as _ce:
                print(f"[pending-nc] 入队超时清理失败: {_ce}")

            async with db.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id, pending_nc_lids, pending_nc_email, pending_nc_pass, "
                    "pending_nc_key, pending_nc_bound_ids, pending_nc_low_admin, "
                    "pending_nc_discord_id, pending_nc_quota_granted, "
                    "auth_token, license_id FROM jb_accounts "
                    "WHERE pending_nc_lids IS NOT NULL AND pending_nc_lids != '[]'"
                )

            if not rows:
                _log("✅ 无排队记录，跳过本轮", "info")
                await asyncio.sleep(300)
                continue

            # ── Phase 1：并发登录，获取所有账号的 id_token ──
            loop = asyncio.get_event_loop()
            valid_rows = []  # [(row, id_token, short_email)]

            async def _login_row(row):
                email = row["pending_nc_email"]
                password = row["pending_nc_pass"]
                row_is_low = bool(row["pending_nc_low_admin"]) if "pending_nc_low_admin" in row.keys() else False
                row_dc_id = (
                    str(row["pending_nc_discord_id"] or "")
                    if "pending_nc_discord_id" in row.keys() else ""
                )
                if not email or not password:
                    _log(
                        f"⚠ {row['id']} 缺少邮箱/密码，跳过", "warn", is_low=row_is_low,
                        low_msg="⚠ 账号信息不完整，跳过本次激活",
                    )
                    return None
                short_email = email[:email.index("@")] + "@..." if "@" in email else email

                # ── 优先使用缓存的 id_token，避免频繁登录触发限速 ──
                cached = _id_token_cache.get(email)
                if cached and (time.time() - cached["cached_at"]) < _TOKEN_TTL:
                    return (row, cached["token"], short_email)

                def _relogin():
                    try:
                        jb_mod = importlib.import_module("jb_activate")
                        jb_mod._set_proxy_pool_context(row_is_low, row_dc_id)
                        try:
                            s, _ = jb_mod.jba_login(email, password)
                            if s is None:
                                print(f"[pending-nc] {email} 重登录失败: jba_login 返回 None session")
                                return None
                            id_token, _ = jb_mod.oauth_pkce(s)
                            return id_token
                        finally:
                            jb_mod._clear_proxy_pool_context()
                    except Exception as e:
                        print(f"[pending-nc] {email} 重登录失败: {e}")
                        return None
                id_token = await loop.run_in_executor(None, _relogin)
                if not id_token:
                    _log(
                        f"❌ {short_email} 登录失败，跳过本轮", "error", is_low=row_is_low,
                        low_msg=f"❌ {short_email} 账号登录失败，稍后会自动重试",
                    )
                    return None
                # 缓存成功获取的 id_token
                _id_token_cache[email] = {"token": id_token, "cached_at": time.time()}
                _log(
                    f"🔑 {short_email} 登录成功", "info", is_low=row_is_low,
                    low_msg=f"✓ {short_email} 账号登录成功",
                )
                return (row, id_token, short_email)

            login_results = await asyncio.gather(*[_login_row(row) for row in rows])
            valid_rows = [r for r in login_results if r is not None]

            total_lids = sum(len(json.loads(row["pending_nc_lids"] or "[]")) for row, _, _ in valid_rows)
            if total_lids == 0:
                await asyncio.sleep(300)
                continue

            # 双信号量策略：主池固定 10 并发；LOW 行按各自 Discord 账号的独立并发配置。
            _log(
                f"🔄 开始并发探测，{len(valid_rows)} 个账号 / 共 {total_lids} 个 licenseId "
                f"（主池并发 {_CONCURRENCY}，LOW 按 Discord 账号独立并发，CF 代理轮询）",
                "info",
            )

            # ── Phase 2：全量并发探测，按 is_low 分别走两条 semaphore，物理隔离 ──
            sem_main = asyncio.Semaphore(_CONCURRENCY)
            # per-Discord LOW 信号量字典（惰性创建，同一 discord_id 共享同一把 semaphore）
            _sem_low_dict: Dict[str, asyncio.Semaphore] = {}

            def _get_sem_low(dc_id: str) -> asyncio.Semaphore:
                if dc_id not in _sem_low_dict:
                    _sem_low_dict[dc_id] = asyncio.Semaphore(
                        max(1, _get_low_concurrency(dc_id))
                    )
                return _sem_low_dict[dc_id]

            # 构造 (row_id → {meta, tasks}) 映射
            row_meta = {}  # row_id → {row, short_email, pending_nc_key, pending_nc_bound_ids_str, pending_lids}
            probe_tasks = []  # [(row_id, lid, coroutine)]
            jb_mod = importlib.import_module("jb_activate")
            for row, id_token, short_email in valid_rows:
                pending_lids = json.loads(row["pending_nc_lids"] or "[]")
                if not pending_lids:
                    continue
                # 按行的 LOW 标记选择对应的 CF 池：LOW 任务严格走 LOW 池，绝不与主池混用
                is_low = bool(row["pending_nc_low_admin"]) if "pending_nc_low_admin" in row.keys() else False
                row_dc_id = (
                    str(row["pending_nc_discord_id"] or "")
                    if "pending_nc_discord_id" in row.keys() else ""
                )
                if is_low:
                    # LOW 行按 Discord 子池取代理；LOW_CF_PROXY_POOL 现在是 Dict[discord_id, list]
                    low_buckets = jb_mod.LOW_CF_PROXY_POOL or {}
                    pool_for_row = list(low_buckets.get(row_dc_id, []) or [])
                else:
                    pool_for_row = list(jb_mod.CF_PROXY_POOL)
                sem_for_row = _get_sem_low(row_dc_id) if is_low else sem_main
                row_meta[row["id"]] = {
                    "row": row,
                    "id_token": id_token,
                    "short_email": short_email,
                    "pending_nc_key": row["pending_nc_key"] or "",
                    "pending_nc_bound_ids_str": row["pending_nc_bound_ids"] or "",
                    "pending_lids": pending_lids,
                    "is_low": is_low,
                    "discord_id": row_dc_id,
                    # 该邮箱是否已为 LOW 用户的 key 贡献过 +16 额度（防重复计入）
                    "nc_quota_granted": (
                        bool(row["pending_nc_quota_granted"])
                        if "pending_nc_quota_granted" in row.keys() else False
                    ),
                }
                for lid in pending_lids:
                    probe_tasks.append((row["id"], lid, _probe_lid(lid, id_token, sem_for_row, pool_for_row)))

            # 并发执行全部探测
            coros = [t[2] for t in probe_tasks]
            results = await asyncio.gather(*coros)

            # 按 row_id 分组收集结果
            row_results: dict = {rid: {"still": [], "trusted": []} for rid in row_meta}
            for (row_id, lid, _), res in zip(probe_tasks, results):
                meta = row_meta[row_id]
                id_token = meta["id_token"]
                row_is_low = meta.get("is_low", False)
                _se = meta.get("short_email", "账号")  # LOW 面板用邮箱前缀代替 lid
                status_code = res.get("status", -1)
                if status_code == -1:
                    row_results[row_id]["still"].append(lid)
                    _log(
                        f"❌ {lid} → 网络异常: {res.get('error', '?')}", "error", is_low=row_is_low,
                        low_msg=f"❌ {_se} 网络暂时不稳定，稍后会自动重试",
                    )
                elif status_code == 200:
                    try:
                        body = json.loads(res["body"])
                        tok = body.get("token", "")
                        if tok:
                            import base64 as _b64
                            _pl = json.loads(_b64.urlsafe_b64decode(tok.split(".")[1] + "=="))
                            ltype = _pl.get("license_type", "")
                            real_lid = _pl.get("license", lid)
                            if "free-tier" in ltype:
                                new_acc = {
                                    "jwt": tok, "has_quota": True,
                                    "last_updated": time.time(),
                                    "licenseId": real_lid,
                                    "authorization": id_token,
                                }
                                await _check_quota(new_acc)
                                already = any(
                                    a.get("licenseId") == real_lid or a.get("jwt") == tok
                                    for a in JETBRAINS_ACCOUNTS
                                )
                                if not already:
                                    async with account_rotation_lock:
                                        JETBRAINS_ACCOUNTS.append(new_acc)
                                    _save_pool = await _get_low_db_pool() if row_is_low else await _get_db_pool()
                                    await _save_account_to_db(new_acc, pool=_save_pool)
                                    _log(
                                        f"✅ {lid} → trusted，已入池", "success", is_low=row_is_low,
                                        low_msg=f"✓ {_se} 账号激活成功，已入池",
                                    )
                                else:
                                    _log(
                                        f"✅ {lid} → trusted（已在池中，跳过重复）", "success", is_low=row_is_low,
                                        low_msg=f"✓ {_se} 账号已存在，跳过重复添加",
                                    )
                                row_results[row_id]["trusted"].append(real_lid)
                            else:
                                row_results[row_id]["still"].append(lid)
                                _log(
                                    f"⏳ {lid} → 200 但 license_type 不含 free-tier，继续等待", "warn", is_low=row_is_low,
                                    low_msg=f"⏳ {_se} 账号验证中，请稍候",
                                )
                        else:
                            row_results[row_id]["still"].append(lid)
                            _log(
                                f"⏳ {lid} → 200 但无 token，继续等待", "warn", is_low=row_is_low,
                                low_msg=f"⏳ {_se} 账号验证中，请稍候",
                            )
                    except Exception as e:
                        row_results[row_id]["still"].append(lid)
                        _log(
                            f"⚠ {lid} → 解析异常: {e}", "warn", is_low=row_is_low,
                            low_msg=f"⚠ {_se} 账号信息解析异常，稍后重试",
                        )
                elif status_code == 492:
                    row_results[row_id]["still"].append(lid)
                    body_snippet = (res.get("body") or "")[:250].replace("\n", " ")
                    _log(
                        f"⏳ {lid} → 492 Untrusted，继续等待  body={body_snippet!r}", "pending", is_low=row_is_low,
                        low_msg=f"⏳ {_se} 账号验证中，请稍候",
                    )
                elif status_code == 429:
                    row_results[row_id]["still"].append(lid)
                    _log(
                        f"⚠ {lid} → 429 限流，保留下次重试", "warn", is_low=row_is_low,
                        low_msg=f"⏳ {_se} 系统繁忙，稍后会自动重试",
                    )
                elif status_code == 401:
                    # token 过期，清除缓存，保留 lid 等下一轮重新登录
                    row_email = meta["row"]["pending_nc_email"] if "row" in meta else ""
                    if row_email and row_email in _id_token_cache:
                        del _id_token_cache[row_email]
                    row_results[row_id]["still"].append(lid)
                    _log(
                        f"⚠ {lid} → 401 token 已过期，已清除缓存，下一轮重新登录", "warn", is_low=row_is_low,
                        low_msg=f"⏳ {_se} 账号重新验证中，请稍候",
                    )
                else:
                    _log(
                        f"❌ {lid} → HTTP {status_code}，放弃重试", "error", is_low=row_is_low,
                        low_msg=f"❌ {_se} 账号激活失败，已停止重试",
                    )

            # ── Phase 3：更新 DB、按需升级密钥 ──
            for row_id, rr in row_results.items():
                meta = row_meta[row_id]
                still_pending = rr["still"]
                newly_trusted_ids = rr["trusted"]
                short_email = meta["short_email"]
                pending_lids = meta["pending_lids"]
                pending_nc_key = meta["pending_nc_key"]
                pending_nc_bound_ids_str = meta["pending_nc_bound_ids_str"]
                row_is_low = meta.get("is_low", False)
                # LOW 行写库走 LOW 专用池；普通行继续使用主池（db）
                _row_pool = await _get_low_db_pool() if row_is_low else db
                if not _row_pool:
                    _log(f"⚠ {meta.get('short_email','?')} DB pool 不可用，跳过本行写库", "warn")
                    continue

                async with _row_pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE jb_accounts SET pending_nc_lids=$1 WHERE id=$2",
                        json.dumps(still_pending), row_id
                    )
                resolved = len(pending_lids) - len(still_pending)
                if resolved > 0:
                    _log(
                        f"📊 {short_email}：本轮 {resolved} 个已信任，{len(still_pending)} 个仍等待",
                        "info", is_low=row_is_low,
                        low_msg=f"📊 {short_email}：本轮成功激活 {resolved} 个账号，{len(still_pending)} 个仍在验证中",
                    )
                else:
                    _log(
                        f"📊 {short_email}：全部 {len(still_pending)} 个仍在等待",
                        "info", is_low=row_is_low,
                        low_msg=f"📊 {short_email}：账号仍在验证中，请耐心等待（共 {len(still_pending)} 个）",
                    )

                NC_QUOTA_THRESHOLD = 4
                already_bound = [x for x in pending_nc_bound_ids_str.split(",") if x.strip()]
                all_ids = list(dict.fromkeys(already_bound + newly_trusted_ids))  # 去重保序
                # 同一邮箱（行）只能贡献一次额度
                row_already_granted = bool(meta.get("nc_quota_granted", False))

                # ── LOW 用户：每邮箱独立判定，凑够 4 个信任凭证就给该 LOW 用户的 key 追加 +16；
                #            已贡献过的邮箱不会重复计入。
                if row_is_low:
                    if (
                        pending_nc_key
                        and len(all_ids) >= NC_QUOTA_THRESHOLD
                        and not row_already_granted
                    ):
                        _upg_db = _row_pool
                        if _upg_db:
                            new_quota = await _add_low_quota(pending_nc_key, all_ids, _upg_db)
                            _log(
                                f"🎉 {short_email} 已达 {NC_QUOTA_THRESHOLD} 个信任凭证，"
                                f"LOW 用户密钥额度 +{_LOW_USER_KEY_QUOTA}（当前总额度 {new_quota}，"
                                f"本邮箱贡献 {len(all_ids)} 个账号）",
                                "success", is_low=True,
                                low_msg=(
                                    f"🎉 {short_email} 激活成功！"
                                    f"密钥额度已增加 +{_LOW_USER_KEY_QUOTA}（当前总额度 {new_quota}）"
                                ),
                            )
                            # 同时持久化：bound_ids 累积、本行 granted 置 TRUE
                            async with _row_pool.acquire() as _ub:
                                await _ub.execute(
                                    "UPDATE jb_accounts "
                                    "SET pending_nc_bound_ids=$1, pending_nc_quota_granted=TRUE "
                                    "WHERE id=$2",
                                    ",".join(all_ids), row_id,
                                )
                    elif row_already_granted and newly_trusted_ids:
                        # 已贡献过额度的邮箱：仅累积 bound_ids（用于审计/账号去重），不再加额度
                        async with _row_pool.acquire() as _ub:
                            await _ub.execute(
                                "UPDATE jb_accounts SET pending_nc_bound_ids=$1 WHERE id=$2",
                                ",".join(all_ids), row_id,
                            )
                        _log(
                            f"ℹ {short_email} 本轮新增 {len(newly_trusted_ids)} 个信任凭证，"
                            f"但本邮箱已贡献过 +{_LOW_USER_KEY_QUOTA} 额度，不重复计入",
                            "info", is_low=True,
                            low_msg=(
                                f"ℹ {short_email} 本轮新增 {len(newly_trusted_ids)} 个账号，"
                                f"该批次额度已发放，不重复计入"
                            ),
                        )
                    elif newly_trusted_ids:
                        # 未达阈值：仅累积 bound_ids，等待下轮
                        async with _row_pool.acquire() as _ub:
                            await _ub.execute(
                                "UPDATE jb_accounts SET pending_nc_bound_ids=$1 WHERE id=$2",
                                ",".join(all_ids), row_id,
                            )
                        _log(
                            f"📊 {short_email} 当前 {len(all_ids)}/{NC_QUOTA_THRESHOLD} 个信任凭证，"
                            f"未达阈值",
                            "info", is_low=True,
                            low_msg=(
                                f"📊 {short_email} 当前已激活 {len(all_ids)}/{NC_QUOTA_THRESHOLD} 个账号，"
                                f"继续验证中"
                            ),
                        )
                else:
                    # ── 普通用户（非 LOW）：保持原"全部信任后一次性升至 25"逻辑 ──
                    key_already_upgraded = (
                        VALID_CLIENT_KEYS.get(pending_nc_key, {}).get("usage_limit", 0) > 0
                        if pending_nc_key else False
                    )
                    if pending_nc_key and len(all_ids) >= NC_QUOTA_THRESHOLD and not key_already_upgraded:
                        _upg_db = _row_pool
                        if _upg_db:
                            await _activate_key_quota(pending_nc_key, all_ids, _upg_db)
                            _log(
                                f"🎉 {short_email} 已达 {NC_QUOTA_THRESHOLD} 个信任凭证，"
                                f"密钥升级为额度 {_NORMAL_KEY_QUOTA}（共 {len(all_ids)} 个）",
                                "success", is_low=False,
                            )
                        async with _row_pool.acquire() as _ub:
                            await _ub.execute(
                                "UPDATE jb_accounts SET pending_nc_bound_ids=$1 WHERE id=$2",
                                ",".join(all_ids), row_id,
                            )
                    elif len(still_pending) == 0 and pending_nc_key and not key_already_upgraded:
                        if all_ids:
                            _upg_db = _row_pool
                            if _upg_db:
                                await _activate_key_quota(pending_nc_key, all_ids, _upg_db)
                                _log(f"🎉 {short_email} 全部 trusted（共 {len(all_ids)} 个），密钥已升级", "success", is_low=False)
                        else:
                            _log(f"⚠ {short_email} 无有效账号可绑定，密钥保持 0 额度", "warn", is_low=False)

        except Exception as e:
            _log(f"❌ 扫描循环异常: {e}", "error")
        await asyncio.sleep(300)  # 每 5 分钟重试一次


async def _persist_call_log(entry: dict) -> None:
    """将单条调用日志写入 call_logs 表（fire-and-forget，失败静默）。"""
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO call_logs (ts,model,api_key,discord_id,prompt_tokens,completion_tokens,elapsed_ms,status,exempt) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                entry["ts"], entry["model"], entry["key"], entry["discord_id"],
                int(entry["prompt_tokens"] or 0), int(entry["completion_tokens"] or 0),
                int(entry["elapsed_ms"] or 0), entry["status"], bool(entry["exempt"]),
            )
    except Exception:
        pass


# ==================== LOW 用户 AI 响应缓存 ====================

def _canonical_json(obj: Any) -> str:
    """确定性 JSON 序列化（用于 hash）"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _is_low_cacheable_request(api_key: str, body: dict) -> tuple:
    """判断本次请求是否满足 LOW 缓存条件。返回 (cacheable: bool, reason: str)"""
    meta = VALID_CLIENT_KEYS.get(api_key, {})
    if not meta.get("is_low_admin_key"):
        return False, "not_low_key"
    if body.get("stream") is True:
        return False, "stream_not_cached"
    try:
        temperature = float(body.get("temperature", 1) or 1)
    except Exception:
        temperature = 1.0
    if temperature > 0.3:
        return False, "temperature_too_high"
    if body.get("tools") or body.get("tool_choice") or body.get("functions") or body.get("function_call"):
        return False, "tools_not_cached"
    if len(json.dumps(body, ensure_ascii=False)) > 128 * 1024:
        return False, "request_too_large"
    for m in (body.get("messages") or []):
        if isinstance(m.get("content"), list):
            return False, "multimodal_not_cached"
    return True, "ok"


def _build_ai_cache_key(route: str, api_key: str, body: dict) -> tuple:
    """生成缓存 key 与 request_hash。返回 (cache_key: str, request_hash: str)"""
    meta = VALID_CLIENT_KEYS.get(api_key, {})
    owner = str(meta.get("low_admin_discord_id") or "")
    owner_scope = owner or api_key
    normalized = dict(body)
    normalized["stream"] = False     # 确保 key 与 stream 参数无关
    raw = _canonical_json({"route": route, "owner_scope": owner_scope, "body": normalized})
    request_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    cache_key = f"low:{owner_scope}:{route}:{request_hash}"
    return cache_key, request_hash


def _is_valid_openai_chat_response(resp: Any) -> bool:
    """校验缓存里的 OpenAI chat 响应是否含有效 assistant message。
    旧版本可能缓存过 content=None / 无 choices 的坏响应，会触发 VS Code
    “语言模型未提供任何辅助消息”。命中缓存前必须过滤掉。
    """
    if not isinstance(resp, dict) or "error" in resp:
        return False
    choices = resp.get("choices")
    if not isinstance(choices, list) or not choices:
        return False
    msg = (choices[0] or {}).get("message")
    if not isinstance(msg, dict) or msg.get("role") != "assistant":
        return False
    # content="" 是合法空响应；None 才是不兼容风险。
    return msg.get("content") is not None or bool(msg.get("tool_calls"))


def _is_valid_anthropic_message_response(resp: Any) -> bool:
    """校验缓存里的 Anthropic message 响应是否含有效 assistant content。"""
    if not isinstance(resp, dict) or "error" in resp:
        return False
    if resp.get("role") != "assistant":
        return False
    content = resp.get("content")
    return isinstance(content, list) and len(content) > 0


async def _ai_cache_delete(cache_key: str) -> None:
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM ai_response_cache WHERE cache_key=$1", cache_key)
    except Exception:
        pass


async def _ai_idem_delete(owner_scope: str, idem_key: str) -> None:
    pool = await _get_db_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM ai_idempotency_cache WHERE owner_discord_id=$1 AND idempotency_key=$2",
                owner_scope, idem_key,
            )
    except Exception:
        pass


async def _ai_cache_get(cache_key: str) -> Optional[dict]:
    """从 DB 读取缓存行，命中则自动 hit_count +1。未命中/过期返回 None。"""
    pool = await _get_db_pool()
    if not pool:
        return None
    now = time.time()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT response_json FROM ai_response_cache WHERE cache_key=$1 AND expires_at > $2",
                cache_key, now,
            )
            if not row:
                return None
            await conn.execute(
                "UPDATE ai_response_cache SET hit_count = hit_count + 1 WHERE cache_key=$1",
                cache_key,
            )
            rj = row["response_json"]
            return dict(rj) if isinstance(rj, dict) else json.loads(rj)
    except Exception:
        return None


async def _ai_cache_set(
    *,
    cache_key: str,
    owner_discord_id: str,
    client_key: str,
    route: str,
    model: str,
    request_hash: str,
    response_json: dict,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    ttl_sec: int = 3600,
) -> None:
    """写入/更新缓存行。失败静默。"""
    pool = await _get_db_pool()
    if not pool:
        return
    now = time.time()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ai_response_cache (
                    cache_key, scope, owner_discord_id, client_key,
                    route, model, request_hash, response_json,
                    prompt_tokens, completion_tokens,
                    hit_count, created_at, expires_at
                )
                VALUES ($1,'low',$2,$3,$4,$5,$6,$7::jsonb,$8,$9,0,$10,$11)
                ON CONFLICT (cache_key) DO UPDATE SET
                    response_json     = EXCLUDED.response_json,
                    prompt_tokens     = EXCLUDED.prompt_tokens,
                    completion_tokens = EXCLUDED.completion_tokens,
                    expires_at        = EXCLUDED.expires_at
                """,
                cache_key, owner_discord_id, client_key,
                route, model, request_hash,
                json.dumps(response_json, ensure_ascii=False),
                prompt_tokens, completion_tokens,
                now, now + ttl_sec,
            )
    except Exception as e:
        print(f"[ai-cache] set error: {e}")


async def _ai_idem_get(owner_scope: str, idem_key: str, body_hash: str) -> tuple:
    """查询 Idempotency-Key 缓存。返回 (status, response_or_None)
       status: 'miss' | 'hit' | 'conflict'
    """
    pool = await _get_db_pool()
    if not pool:
        return "miss", None
    now = time.time()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT body_hash, response_json FROM ai_idempotency_cache
                   WHERE owner_discord_id=$1 AND idempotency_key=$2 AND expires_at > $3""",
                owner_scope, idem_key, now,
            )
            if not row:
                return "miss", None
            if row["body_hash"] != body_hash:
                return "conflict", None
            rj = row["response_json"]
            return "hit", (dict(rj) if isinstance(rj, dict) else json.loads(rj))
    except Exception:
        return "miss", None


async def _ai_idem_set(
    owner_scope: str, idem_key: str, client_key: str,
    body_hash: str, response_json: dict, ttl_sec: int = 86400,
) -> None:
    """写入 Idempotency-Key 缓存。失败静默。"""
    pool = await _get_db_pool()
    if not pool:
        return
    now = time.time()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ai_idempotency_cache
                    (idempotency_key, owner_discord_id, client_key, body_hash, response_json, created_at, expires_at)
                VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7)
                ON CONFLICT (owner_discord_id, idempotency_key) DO NOTHING
                """,
                idem_key, owner_scope, client_key,
                body_hash, json.dumps(response_json, ensure_ascii=False),
                now, now + ttl_sec,
            )
    except Exception as e:
        print(f"[ai-idem] set error: {e}")


async def _cleanup_ai_response_cache_loop() -> None:
    """每小时清理过期的 ai_response_cache 和 ai_idempotency_cache 行"""
    await asyncio.sleep(3600)
    while True:
        try:
            pool = await _get_db_pool()
            if pool:
                now = time.time()
                async with pool.acquire() as conn:
                    r1 = await conn.execute("DELETE FROM ai_response_cache WHERE expires_at < $1", now)
                    r2 = await conn.execute("DELETE FROM ai_idempotency_cache WHERE expires_at < $1", now)
                    print(f"[ai-cache] 清理完成：response_cache={r1}, idem={r2}")
        except Exception as e:
            print(f"[ai-cache] cleanup error: {e}")
        await asyncio.sleep(3600)


async def _cleanup_old_call_logs_loop() -> None:
    """每 24 小时清理 7 天前的调用日志记录。"""
    await asyncio.sleep(60)   # 启动后稍等，待 DB 池就绪
    while True:
        try:
            pool = await _get_db_pool()
            if pool:
                cutoff = time.time() - 7 * 24 * 3600
                async with pool.acquire() as conn:
                    result = await conn.execute("DELETE FROM call_logs WHERE ts < $1", cutoff)
                print(f"[日志清理] 已清理 7 天前调用日志，受影响行：{result}")
        except Exception as e:
            print(f"[日志清理] 失败：{e}")
        await asyncio.sleep(24 * 3600)


async def _cf_proxy_health_check_loop():
    """每 5 分钟探活所有 CF Worker URL，连续 3 次失败自动 is_active=false"""
    while True:
        try:
            await asyncio.sleep(300)
            pool = await _get_db_pool()
            if not pool:
                continue
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id, url, consecutive_failures FROM cf_proxy_pool WHERE is_active = TRUE"
                )

            async with httpx.AsyncClient(timeout=8) as client:
                for row in rows:
                    ok = False
                    try:
                        r = await client.get(f"{row['url']}/health")
                        ok = (r.status_code == 200)
                    except Exception:
                        ok = False

                    async with pool.acquire() as conn:
                        if ok:
                            await conn.execute(
                                "UPDATE cf_proxy_pool SET last_health_check=$1, consecutive_failures=0 WHERE id=$2",
                                time.time(), row["id"]
                            )
                        else:
                            new_fails = (row["consecutive_failures"] or 0) + 1
                            if new_fails >= 3:
                                await conn.execute(
                                    "UPDATE cf_proxy_pool SET is_active=FALSE, last_health_check=$1, consecutive_failures=$2 WHERE id=$3",
                                    time.time(), new_fails, row["id"]
                                )
                                print(f"[cf-health] 自动停用 {row['url']}（连续失败 {new_fails} 次）")
                            else:
                                await conn.execute(
                                    "UPDATE cf_proxy_pool SET last_health_check=$1, consecutive_failures=$2 WHERE id=$3",
                                    time.time(), new_fails, row["id"]
                                )

            await load_cf_proxies_from_db()
        except Exception as e:
            print(f"[cf-health] loop error: {e}")


@app.on_event("startup")
async def startup():
    global models_data, http_client
    models_data = load_models()
    await _ensure_db_tables()
    await load_keys_from_db()
    await load_accounts_from_db()
    await load_cf_proxies_from_db()
    await load_pokeball_keys_from_db()
    await _load_discord_sessions_from_db()
    await _load_low_admin_settings()
    http_client = httpx.AsyncClient(
        # 连接阶段最多等 15 秒；流式读取兜底 900 秒（避免僵尸 SSE 永久占用连接池连接）；
        # 池等待 30 秒（应对突发流量排队，比直接 PoolTimeout 友好）
        timeout=httpx.Timeout(connect=15.0, read=900.0, write=15.0, pool=30.0),
        limits=httpx.Limits(
            max_connections=500,
            max_keepalive_connections=100,
            keepalive_expiry=60,
        ),
    )
    asyncio.create_task(_startup_quota_check())
    asyncio.create_task(_cleanup_pending_prizes_loop())
    asyncio.create_task(_partner_credentials_poller())
    asyncio.create_task(_startup_resume_bind_tasks())
    asyncio.create_task(_flush_key_increments_loop())   # 批量刷新 key 用量到 DB
    asyncio.create_task(_retry_pending_nc_lids())       # NC 492 重试队列
    asyncio.create_task(_cf_proxy_health_check_loop())  # CF Worker 池健康检查
    asyncio.create_task(_cleanup_old_call_logs_loop())  # 调用日志 7 天自动清理
    asyncio.create_task(_cleanup_ai_response_cache_loop())  # AI 响应缓存定期清理
    print("JetBrains AI OpenAI Compatible API 服务器已启动")


async def _migrate_key_limits():
    """迁移 key 上限：30→25；40→25（超出的标记用完，未超的降低上限）"""
    NEW_LIMIT = 25
    try:
        pool = await _get_db_pool()
        async with pool.acquire() as conn:
            # 30 → 25（之前的旧迁移，一并处理）
            r30 = await conn.execute(
                "UPDATE jb_client_keys SET usage_limit = $1 WHERE usage_limit = 30 AND usage_count < $1",
                NEW_LIMIT,
            )
            # 40 → 25（未超 25 次的，只降上限）
            r40_ok = await conn.execute(
                "UPDATE jb_client_keys SET usage_limit = $1 WHERE usage_limit = 40 AND usage_count < $1",
                NEW_LIMIT,
            )
            # 40 → 25（已超 25 次的，上限+用量都设为 25，标记为用完）
            r40_ex = await conn.execute(
                "UPDATE jb_client_keys SET usage_limit = $1, usage_count = $1 WHERE usage_limit = 40 AND usage_count >= $1",
                NEW_LIMIT,
            )
            total = sum(int(r.split()[-1]) for r in [r30, r40_ok, r40_ex] if r)
            if total:
                print(f"[迁移] 已将 {total} 个 API 密钥的使用次数上限统一调整为 {NEW_LIMIT}")
    except Exception as e:
        print(f"[迁移] 升级 key 上限时出错（不影响启动）: {e}")


@app.on_event("shutdown")
async def shutdown():
    global http_client, DB_POOL, DB_POOL_LOW
    # 关机前主动刷新剩余的 key 用量，避免因 5s 周期未到而丢失数据
    try:
        await _flush_key_increments_to_db()
    except Exception:
        pass
    if http_client:
        await http_client.aclose()
    if DB_POOL is not None:
        try:
            await DB_POOL.close()
        except Exception:
            pass
    if DB_POOL_LOW is not None:
        try:
            await DB_POOL_LOW.close()
        except Exception:
            pass


# API 端点
@app.get("/v1/models", response_model=ModelList)
async def list_models(_: str = Depends(authenticate_any_client)):
    """列出可用模型"""
    model_list = [
        ModelInfo(
            id=model.get("id", ""),
            created=model.get("created", int(time.time())),
            owned_by=model.get("owned_by", "jetbrains-ai"),
        )
        for model in models_data.get("data", [])
    ]
    return ModelList(data=model_list)


async def _sse_with_keepalive(
    gen: AsyncGenerator[str, None],
    interval: float = 25.0,
    request: Optional[Request] = None,  # 保留形参以兼容调用方，当前不使用（见下方注释）
) -> AsyncGenerator[str, None]:
    """SSE 心跳包装器：超过 interval 秒无数据时注入 SSE 注释行 ': ping'，
    防止 Replit 反向代理的 idle timeout（默认 300s）截断长流式响应。
    SSE 注释行以冒号开头，OpenAI/Anthropic 兼容客户端会将其忽略。

    断连处理由 starlette ``StreamingResponse`` 内部的 ``listen_for_disconnect`` 协程负责
    （它在 ASGI ``receive`` 上监听 ``http.disconnect``）。我们 **不能** 在这里再调用
    ``request.is_disconnected()``：那会引入第二个并发的 ``await receive()``，违反 ASGI
    规范，导致消息被错误派发、整个流式响应被异常 cancel（症状：开了流不出字）。

    客户端断连时，starlette 会 cancel 包裹我们的 stream task，从而抛出
    ``GeneratorExit`` 进入下方 ``finally`` —— 显式 ``await it.aclose()`` 会沿生成器链
    向上传播，让 ``async with http_client.stream(...)`` 立即退出并归还连接池连接，
    不依赖 GC，避免僵尸 SSE 连接耗尽全局池。
    """
    it = gen.__aiter__()
    try:
        while True:
            try:
                chunk = await asyncio.wait_for(it.__anext__(), timeout=interval)
                yield chunk
            except asyncio.TimeoutError:
                yield ": ping\n\n"
            except StopAsyncIteration:
                break
    finally:
        # 显式关闭下游生成器链（含 http_client.stream 的 async with），
        # 确保不依赖 GC 即可立刻归还连接池连接
        try:
            aclose = getattr(it, "aclose", None)
            if aclose is not None:
                await aclose()
        except Exception as _close_err:
            print(f"[SSE] 关闭下游生成器时出错（忽略）: {_close_err}")


def _convert_openai_messages_to_jetbrains(
    messages: "List[ChatMessage]",
    tool_id_to_func_name_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    """OpenAI 历史消息 → JetBrains 内部 messages 格式。

    要点：
    - 过滤空白 user/system 普通文本消息，避免 JetBrains 上游报错：
      ``system: text content blocks must contain non-whitespace text``。
    - assistant 一次返回多个 tool_calls 时，全部都要保留：第一个 tool_call 跟随原
      assistant 文本作为一条 assistant_message 发出；后续每个 tool_call 单独追加一条
      带非空占位文本的 assistant_message，避免上游历史丢失并行调用上下文。
    - 同时把所有 tool_call.id → function.name 写入映射表，供后续 role='tool' 消息
      转 function_message 时回查 functionName。
    """
    jetbrains_messages: List[Dict[str, Any]] = []
    for msg in messages:
        text_content = extract_text_content(msg.content)
        has_text = bool(text_content.strip())

        if msg.role in ("user", "system"):
            if not has_text:
                continue
            jetbrains_messages.append(
                {"type": f"{msg.role}_message", "content": text_content}
            )
        elif msg.role == "assistant":
            if msg.tool_calls:
                for idx, tc in enumerate(msg.tool_calls):
                    fn = tc.get("function", {}) or {}
                    fn_name = fn.get("name") or ""
                    fn_args = fn.get("arguments") or ""
                    if tc.get("id") and fn_name:
                        tool_id_to_func_name_map[tc["id"]] = fn_name
                    jetbrains_messages.append({
                        "type": "assistant_message",
                        # JetBrains 不接受空白-only text block；tool_call 历史无文本时给非空占位
                        "content": text_content if (idx == 0 and has_text) else "(tool call)",
                        "functionCall": {
                            "functionName": fn_name,
                            "content": fn_args,
                        },
                    })
            elif has_text:
                jetbrains_messages.append(
                    {"type": "assistant_message", "content": text_content}
                )
        elif msg.role == "tool":
            function_name = tool_id_to_func_name_map.get(msg.tool_call_id)
            if function_name:
                jetbrains_messages.append({
                    "type": "function_message",
                    "content": text_content if has_text else "(empty)",
                    "functionName": function_name,
                })
            else:
                print(
                    f"警告: 无法为 tool_call_id {msg.tool_call_id} 找到对应的函数调用"
                )
        else:
            if has_text:
                jetbrains_messages.append(
                    {"type": "user_message", "content": text_content}
                )

    if not jetbrains_messages:
        # 防御性兜底：所有输入消息都是空白时，不把空 messages 发给上游
        jetbrains_messages.append({"type": "user_message", "content": "继续"})

    # JetBrains 要求对话最后一条必须是 user_message；
    # 如果客户端发来末尾 assistant 消息（prefill 技巧），直接移除，防止 400。
    while jetbrains_messages and jetbrains_messages[-1].get("type") == "assistant_message":
        jetbrains_messages.pop()

    # 移除后若列表为空，补一条 user 消息兜底
    if not jetbrains_messages:
        jetbrains_messages.append({"type": "user_message", "content": "继续"})

    return jetbrains_messages


def _jetbrains_profile_supports_functions(model_name: str) -> bool:
    """JetBrains 聚合网关里 OpenAI provider 不支持 llm.parameters.functions。
    VS Code/Cline/Roo 常会默认携带 tools；如果原样转发到 openai-* / gpt-* profile，
    上游会返回 400: Configuration parameters [functions] are not supported for chat OpenAI provider。
    """
    m = (model_name or "").lower()
    if m.startswith("openai-") or m.startswith("gpt-") or m.startswith("o1") or m.startswith("o3") or m.startswith("o4"):
        return False
    return True


def _filter_tools_by_choice(
    tools: List[Dict[str, Any]],
    tool_choice: Any,
) -> List[Dict[str, Any]]:
    """按 OpenAI tool_choice 语义在代理层裁剪 tools 列表。

    JetBrains 后端没有公开的"强制调用"开关，因此我们在代理层做语义近似：
      - None / "auto" / "required" → 原样返回（required 退化为 auto，把候选集发过去）
      - "none"                     → 返回 []，调用方据此完全不发送 tools 字段
      - {"type":"function", "function":{"name":"X"}} → 仅保留名为 X 的那一个 function
        若指定的 function 不在 tools 中则原样返回（避免误把列表清空，更接近"宽松匹配"）
    其它未识别值（如旧版 "function_call":{...} 字符串等）→ 视为 auto，不裁剪。
    """
    if not tools:
        return []
    if tool_choice is None:
        return tools
    if isinstance(tool_choice, str):
        if tool_choice == "none":
            return []
        # "auto" / "required" / 任何未知字符串 → 不裁剪
        return tools
    if isinstance(tool_choice, dict):
        if tool_choice.get("type") == "function":
            target_name = (tool_choice.get("function") or {}).get("name")
            if target_name:
                picked = [
                    t for t in tools
                    if (t.get("function") or {}).get("name") == target_name
                ]
                if picked:
                    return picked
        # 其它形态（如 anthropic 风格 {"type":"tool","name":"X"} 等）尽量宽松解析
        target_name = tool_choice.get("name")
        if target_name:
            picked = [
                t for t in tools
                if (t.get("function") or {}).get("name") == target_name
            ]
            if picked:
                return picked
    return tools


async def openai_stream_adapter(
    api_stream_generator: AsyncGenerator[str, None],
    model_name: str,
    tools: Optional[List[Dict[str, Any]]],
    include_usage: bool = False,
    _usage_capture: Optional[Dict[str, int]] = None,
) -> AsyncGenerator[str, None]:
    """将 JetBrains API 的流转换为 OpenAI 格式的 SSE。
    _usage_capture: 若提供（dict），FinishMetadata 的真实 token 数会被写入该字典，
                    供上层 wrapper（如 _stream_with_key_consume）读取。不会改变 SSE 输出。
    """
    stream_id = f"chatcmpl-{uuid.uuid4().hex}"
    first_chunk_sent = False
    # JetBrains 把同一个 tool_call 的 arguments 拆成多个 FunctionCall 事件流送：
    #   第一条带 name（=新的 tool_call 起点）；后续每个 token 增量再发一条 name=null
    # OpenAI 客户端按 (index, id) 拼接 arguments，所以：
    #   - 见到带 name 的事件 → 视为新的 tool_call，分配新 index 与新 id
    #   - 见到 name=null 的事件 → 视为当前 tool_call 的 arguments 增量，复用 index、不再下发 id
    # 这样既正确处理单 tool_call 的多片增量，也能在真正出现并行 tool_calls
    # （多个带 name 的事件）时按顺序分配独立 index，避免它们撞到同一个 index=0。
    current_tool_call_index = -1
    completion_chars = 0
    api_prompt_tokens: int = 0
    api_completion_tokens: int = 0

    try:
        async for line in api_stream_generator:
            if not line or line == "data: end":
                continue

            if line.startswith("data: "):
                try:
                    data = json.loads(line[6:])
                except json.JSONDecodeError:
                    print(f"警告: 无法解析的 JSON 行: {line}")
                    continue

                try:
                    # 流式接口里不要直接透传无 choices 的 error 事件：
                    # VS Code/Cline/Roo 等严格客户端会把这种 200 SSE 判定为
                    # “语言模型未提供任何辅助消息”。改为发送一条 assistant 文本
                    # chunk，再正常 stop，保证协议层始终有 assistant delta。
                    if "error" in data:
                        err = data.get("error") or {}
                        err_msg = err.get("message") if isinstance(err, dict) else str(err)
                        if not err_msg:
                            err_msg = "上游 JetBrains API 返回错误"
                        chunk = {
                            "id": stream_id, "object": "chat.completion.chunk",
                            "created": int(time.time()), "model": model_name,
                            "system_fingerprint": "fp_jetbrains",
                            "choices": [{
                                "delta": {"role": "assistant", "content": f"[上游错误] {err_msg}"},
                                "index": 0,
                                "finish_reason": None,
                            }],
                        }
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                        finish_chunk = {
                            "id": stream_id, "object": "chat.completion.chunk",
                            "created": int(time.time()), "model": model_name,
                            "system_fingerprint": "fp_jetbrains",
                            "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}],
                        }
                        yield f"data: {json.dumps(finish_chunk, ensure_ascii=False)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

                    event_type = data.get("type")

                    if event_type == "Content":
                        content = data.get("content", "")
                        if not content:
                            continue

                        completion_chars += len(content)
                        # 直接构造 dict 而非创建 Pydantic 对象，快 3-5×
                        if not first_chunk_sent:
                            delta = {"role": "assistant", "content": content}
                            first_chunk_sent = True
                        else:
                            delta = {"content": content}
                        chunk = {
                            "id": stream_id, "object": "chat.completion.chunk",
                            "created": int(time.time()), "model": model_name,
                            "system_fingerprint": "fp_jetbrains",
                            "choices": [{"delta": delta, "index": 0, "finish_reason": None}],
                        }
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

                    elif event_type == "FunctionCall":
                        func_name = data.get("name", None)
                        func_argu = data.get("content", None)
                        if func_name:
                            # 新的 tool_call 起点：分配新 index 与新 id，函数名只在首块下发
                            current_tool_call_index += 1
                            tc_delta = {
                                "index": current_tool_call_index,
                                "id": f"call_{uuid.uuid4().hex}",
                                "type": "function",
                                "function": {
                                    "name": func_name,
                                    "arguments": func_argu or "",
                                },
                            }
                        else:
                            # 当前 tool_call 的 arguments 增量；index 沿用，不重发 id/name/type
                            if current_tool_call_index < 0:
                                # 罕见：上游先发了无 name 的增量。兜底建立一个匿名 tool_call
                                current_tool_call_index = 0
                                tc_delta = {
                                    "index": 0,
                                    "id": f"call_{uuid.uuid4().hex}",
                                    "type": "function",
                                    "function": {"name": "", "arguments": func_argu or ""},
                                }
                            else:
                                tc_delta = {
                                    "index": current_tool_call_index,
                                    "function": {"arguments": func_argu or ""},
                                }
                        delta = {"tool_calls": [tc_delta]}
                        # 严格 OpenAI 兼容客户端要求整条响应流中至少出现一次
                        # assistant role；当模型首个输出就是 tool_call 时，也要在首包补 role。
                        if not first_chunk_sent:
                            delta["role"] = "assistant"
                            first_chunk_sent = True
                        chunk = {
                            "id": stream_id, "object": "chat.completion.chunk",
                            "created": int(time.time()), "model": model_name,
                            "system_fingerprint": "fp_jetbrains",
                            "choices": [{
                                "delta": delta,
                                "index": 0,
                                "finish_reason": None,
                            }],
                        }
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

                    elif event_type == "FinishMetadata":
                        # 尝试从 FinishMetadata 提取真实 token 数（API 字段名不固定，全部尝试）
                        api_prompt_tokens = (
                            data.get("promptTokens")
                            or data.get("prompt_tokens")
                            or data.get("inputTokens")
                            or data.get("input_tokens")
                            or 0
                        )
                        api_completion_tokens = (
                            data.get("completionTokens")
                            or data.get("completion_tokens")
                            or data.get("outputTokens")
                            or data.get("output_tokens")
                            or 0
                        )
                        if api_prompt_tokens or api_completion_tokens:
                            print(f"[FinishMetadata] 真实 token: prompt={api_prompt_tokens}, completion={api_completion_tokens}")
                        else:
                            print(f"[FinishMetadata] 未找到 token 字段，完整数据: {data}")
                        # 把真实 token 写入捕获字典（供 _stream_with_key_consume / _tracked_stream 读取）
                        if _usage_capture is not None:
                            if api_prompt_tokens:
                                _usage_capture["prompt_tokens"] = int(api_prompt_tokens)
                            if api_completion_tokens:
                                _usage_capture["completion_tokens"] = int(api_completion_tokens)
                        if not first_chunk_sent:
                            role_chunk = {
                                "id": stream_id, "object": "chat.completion.chunk",
                                "created": int(time.time()), "model": model_name,
                                "system_fingerprint": "fp_jetbrains",
                                "choices": [{"delta": {"role": "assistant", "content": ""}, "index": 0, "finish_reason": None}],
                            }
                            yield f"data: {json.dumps(role_chunk, ensure_ascii=False)}\n\n"
                            first_chunk_sent = True
                        finish_chunk = {
                            "id": stream_id, "object": "chat.completion.chunk",
                            "created": int(time.time()), "model": model_name,
                            "system_fingerprint": "fp_jetbrains",
                            "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}],
                        }
                        yield f"data: {json.dumps(finish_chunk, ensure_ascii=False)}\n\n"
                        break
                except Exception as _ev_err:
                    print(f"警告: 处理事件时异常: {_ev_err} | 行: {line[:200]}")
                    continue

        # 计算最终 token 数：优先使用 API 返回的真实值，否则用字符估算
        final_prompt_tokens = api_prompt_tokens if api_prompt_tokens else 0
        final_completion_tokens = (
            api_completion_tokens if api_completion_tokens
            else max(1, completion_chars // 4)
        )
        final_total_tokens = final_prompt_tokens + final_completion_tokens

        # 兜底：上游可能空流结束（没有 Content/FunctionCall/FinishMetadata/error）。
        # VS Code/Cline/Roo 等严格客户端要求流里至少出现一次 assistant delta；
        # 否则会报“语言模型未提供任何辅助消息”。
        if not first_chunk_sent:
            role_chunk = {
                "id": stream_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model_name,
                "system_fingerprint": "fp_jetbrains",
                "choices": [{
                    "delta": {"role": "assistant", "content": ""},
                    "index": 0,
                    "finish_reason": None,
                }],
            }
            yield f"data: {json.dumps(role_chunk, ensure_ascii=False)}\n\n"
            finish_chunk = {
                "id": stream_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model_name,
                "system_fingerprint": "fp_jetbrains",
                "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}],
            }
            yield f"data: {json.dumps(finish_chunk, ensure_ascii=False)}\n\n"
            first_chunk_sent = True

        # 如果客户端请求了流式 usage（SillyTavern stream_options.include_usage）
        if include_usage:
            usage_resp = {
                "id": stream_id,
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model_name,
                "system_fingerprint": "fp_jetbrains",
                "choices": [],
                "usage": {
                    "prompt_tokens": final_prompt_tokens,
                    "completion_tokens": final_completion_tokens,
                    "total_tokens": final_total_tokens,
                },
            }
            yield f"data: {json.dumps(usage_resp)}\n\n"

        yield "data: [DONE]\n\n"

    except Exception as e:
        print(f"流式适配器错误: {e}")
        # 使用 OpenAI 标准 error 事件格式，不含 content/tool_calls，
        # 这样 _stream_with_key_consume 不会将此次错误计入用量
        err_msg = str(e) or "上游流式适配器错误"
        chunk = {
            "id": stream_id, "object": "chat.completion.chunk",
            "created": int(time.time()), "model": model_name,
            "system_fingerprint": "fp_jetbrains",
            "choices": [{
                "delta": {"role": "assistant", "content": f"[上游错误] {err_msg}"},
                "index": 0,
                "finish_reason": None,
            }],
        }
        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
        finish_chunk = {
            "id": stream_id, "object": "chat.completion.chunk",
            "created": int(time.time()), "model": model_name,
            "system_fingerprint": "fp_jetbrains",
            "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}],
        }
        yield f"data: {json.dumps(finish_chunk, ensure_ascii=False)}\n\n"
        yield "data: [DONE]\n\n"


async def aggregate_stream_for_non_stream_response(
    openai_sse_stream: AsyncGenerator[str, None], model_name: str
) -> ChatCompletionResponse:
    """聚合流式响应为完整响应（含真实 token 统计）"""
    content_parts = []
    tool_calls_map = {}
    final_finish_reason = "stop"
    captured_usage: Dict[str, int] = {}

    async for sse_line in openai_sse_stream:
        if sse_line.startswith("data: ") and sse_line.strip() != "data: [DONE]":
            try:
                data = json.loads(sse_line[6:].strip())

                if "error" in data:
                    err = data.get("error") or {}
                    message = err.get("message") if isinstance(err, dict) else str(err)
                    err_type = err.get("type", "") if isinstance(err, dict) else ""
                    err_code = str(err.get("code", "")) if isinstance(err, dict) else ""
                    status_code = 502
                    if err_type == "rate_limit_error" or err_code in {"429", "quota_exhausted"}:
                        status_code = 429
                    elif err_type == "invalid_request_error" or err_code == "400":
                        status_code = 400
                    raise HTTPException(
                        status_code=status_code,
                        detail=message or "上游 JetBrains API 返回错误",
                    )

                # 捕获 usage 块（choices 为空列表时是 usage-only 块）
                if "usage" in data and data.get("choices") == []:
                    captured_usage = data["usage"]
                    continue

                if not data.get("choices"):
                    continue

                choice = data["choices"][0]
                delta = choice.get("delta", {})

                if choice.get("finish_reason"):
                    final_finish_reason = choice.get("finish_reason")

                if delta.get("content"):
                    content_parts.append(delta["content"])

                if "tool_calls" in delta:
                    for tc_chunk in delta["tool_calls"]:
                        idx = tc_chunk["index"]
                        if idx not in tool_calls_map:
                            tool_calls_map[idx] = {
                                "type": "function",
                                "function": {"name": "", "arguments": ""},
                            }

                        if tc_chunk.get("id"):
                            tool_calls_map[idx]["id"] = tc_chunk["id"]

                        func_chunk = tc_chunk.get("function", {})
                        if func_chunk.get("name"):
                            tool_calls_map[idx]["function"]["name"] = func_chunk["name"]
                        if func_chunk.get("arguments"):
                            tool_calls_map[idx]["function"]["arguments"] += func_chunk[
                                "arguments"
                            ]
            except json.JSONDecodeError:
                print(f"警告: 聚合时无法解析的 JSON 行: {sse_line}")

    final_tool_calls = []
    for k, v in sorted(tool_calls_map.items()):
        if "id" not in v:
            v["id"] = f"call_{uuid.uuid4().hex}"
        final_tool_calls.append(v)

    full_content = "".join(content_parts)

    if final_tool_calls:
        message = ChatMessage(
            role="assistant", content=full_content or None, tool_calls=final_tool_calls
        )
        final_finish_reason = "tool_calls"
    else:
        # 非流式 OpenAI 响应必须显式返回 assistant 消息；content 用空串而不是 None，
        # 避免严格客户端报“语言模型未提供任何辅助消息/assistant message”。
        message = ChatMessage(role="assistant", content=full_content)

    # 使用捕获到的真实 token 统计（如果有）；估算时复用已 join 的 full_content
    if captured_usage:
        usage = captured_usage
    else:
        char_count = len(full_content) if full_content else 0
        est = max(1, char_count // 4)
        usage = {"prompt_tokens": 0, "completion_tokens": est, "total_tokens": est}

    return ChatCompletionResponse(
        model=model_name,
        choices=[
            ChatCompletionChoice(
                message=message,
                finish_reason=final_finish_reason,
            )
        ],
        usage=usage,
    )


def extract_text_content(content: Optional[Union[str, List[Dict[str, Any]]]]) -> str:
    """从消息内容中提取文本内容；空白-only 文本规整为空串，避免上游 400。"""
    if isinstance(content, str):
        return content if content.strip() else ""
    elif isinstance(content, list):
        # 处理多模态消息格式，提取所有非空白文本内容
        text_parts = []
        for item in content:
            text = ""
            if isinstance(item, dict) and item.get("type") == "text":
                text = str(item.get("text", "") or "")
            elif getattr(item, "type", None) == "text":
                text = str(getattr(item, "text", "") or "")
            if text.strip():
                text_parts.append(text)
        return " ".join(text_parts)
    return ""


MAX_INPUT_TOKENS = 150_000
MAX_OUTPUT_TOKENS = 15_000

# LOW_ADMIN_KEY 用户专属密钥的更宽松限制
_LOW_USER_INPUT_TOKENS = 2_000_000_000  # 实际无限制
_LOW_USER_OUTPUT_TOKENS = 40_000
# 普通密钥升级后总额度
_NORMAL_KEY_QUOTA = 25
# LOW 用户密钥升级后总额度
_LOW_USER_KEY_QUOTA = 16


def _key_tier(api_key: Optional[str]) -> str:
    """返回该 client key 的等级：'low' = LOW_ADMIN 用户预签/升级的密钥；'normal' = 其他。
    供请求时按等级决定 max_input/max_output；找不到 key 时按 normal 处理。"""
    if not api_key:
        return "normal"
    meta = VALID_CLIENT_KEYS.get(api_key)
    if meta and meta.get("is_low_admin_key"):
        return "low"
    return "normal"


def _key_tier_limits(api_key: Optional[str]) -> tuple:
    """根据 client key 的等级返回 (max_input_tokens, max_output_tokens, key_quota)"""
    if _key_tier(api_key) == "low":
        return _LOW_USER_INPUT_TOKENS, _LOW_USER_OUTPUT_TOKENS, _LOW_USER_KEY_QUOTA
    return MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, _NORMAL_KEY_QUOTA


def _estimate_input_tokens(messages) -> int:
    """估算消息列表的 token 数量（用于输入限制检查）
    
    采用字节长度除以 3 的保守估算：
    - 英文约 4 字符/token，UTF-8 编码 4 字节/3 ≈ 1.3 token（略偏高，偏安全）
    - 中文约 1-2 字符/token，UTF-8 编码 3 字节/3 = 1 token（合理）
    """
    total_bytes = 0
    for msg in messages:
        content = msg.content if hasattr(msg, "content") else (msg.get("content", "") if isinstance(msg, dict) else "")
        if isinstance(content, str):
            total_bytes += len(content.encode("utf-8"))
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    text = block.get("text", "") or ""
                    total_bytes += len(str(text).encode("utf-8"))
                elif hasattr(block, "text") and block.text:
                    total_bytes += len(str(block.text).encode("utf-8"))
    return total_bytes // 3


@app.post("/v1/chat/completions")
async def chat_completions(
    request: ChatCompletionRequest,
    http_request: Request,
    client_key: str = Depends(authenticate_client),
):
    """创建聊天完成"""
    max_in, max_out, _ = _key_tier_limits(client_key)
    estimated = _estimate_input_tokens(request.messages)
    if estimated > max_in:
        raise HTTPException(
            status_code=400,
            detail=f"输入内容过长：估算约 {estimated:,} tokens，超过单次限制 {max_in:,} tokens",
        )

    if request.max_tokens is not None and request.max_tokens > max_out:
        print(f"[限制] max_tokens {request.max_tokens} 超过上限，已截断至 {max_out}")
        request.max_tokens = max_out

    # 宽松的模型检查：找不到时允许继续，直接使用请求中的模型名（SillyTavern 兼容）
    model_info = get_model_item(request.model)
    if not model_info and not models_data.get("data"):
        raise HTTPException(status_code=503, detail="服务未配置任何模型")

    # ── LOW 用户缓存读取 ────────────────────────────────────────────────────
    _req_body = request.model_dump(exclude_none=True)
    _ai_cacheable, _cache_reason = _is_low_cacheable_request(client_key, _req_body)
    _ai_cache_key: Optional[str] = None
    _ai_cache_req_hash: Optional[str] = None
    _meta = VALID_CLIENT_KEYS.get(client_key, {})
    _is_low_key = bool(_meta.get("is_low_admin_key"))
    _ai_cache_owner: str = str(_meta.get("low_admin_discord_id") or "") or client_key

    # Idempotency-Key 是防重复提交/网络重试机制：
    # 只要是 LOW key + 非流式请求就生效，不受 exact-cache 的 temperature/tools 限制影响。
    _idem_header = http_request.headers.get("Idempotency-Key") if (_is_low_key and not request.stream) else None
    _idem_body_hash = hashlib.sha256(_canonical_json(_req_body).encode()).hexdigest() if _idem_header else ""
    if _idem_header:
        _idem_status, _idem_resp = await _ai_idem_get(_ai_cache_owner, _idem_header, _idem_body_hash)
        if _idem_status == "hit" and _idem_resp:
            if _is_valid_openai_chat_response(_idem_resp):
                _idem_resp["cached"] = True
                _idem_resp["cache_hit"] = True
                _append_log(request.model, client_key, 0, 0, 0, "ok", exempt=True)
                return JSONResponse(_idem_resp)
            await _ai_idem_delete(_ai_cache_owner, _idem_header)
            print("[ai-idem] 丢弃旧版坏缓存：OpenAI 响应缺少有效 assistant message")
        elif _idem_status == "conflict":
            return JSONResponse(
                {"error": {"message": "Idempotency-Key conflict: body changed", "type": "conflict", "code": "409"}},
                status_code=409,
            )

    if _ai_cacheable:
        _ai_cache_key, _ai_cache_req_hash = _build_ai_cache_key("/v1/chat/completions", client_key, _req_body)

        # Exact match 缓存
        _cached_resp = await _ai_cache_get(_ai_cache_key)
        if _cached_resp:
            if _is_valid_openai_chat_response(_cached_resp):
                _cached_resp["cached"] = True
                _cached_resp["cache_hit"] = True
                _append_log(request.model, client_key, 0, 0, 0, "ok", exempt=True)
                return JSONResponse(_cached_resp)
            await _ai_cache_delete(_ai_cache_key)
            print("[ai-cache] 丢弃旧版坏缓存：OpenAI 响应缺少有效 assistant message")
    # ────────────────────────────────────────────────────────────────────────

    account = await get_next_jetbrains_account(client_key=client_key)

    # 从历史消息中创建 tool_call_id 到 function_name 的映射
    tool_id_to_func_name_map = {}
    for m in request.messages:
        if m.role == "assistant" and m.tool_calls:
            for tc in m.tool_calls:
                if tc.get("id") and tc.get("function", {}).get("name"):
                    tool_id_to_func_name_map[tc["id"]] = tc["function"]["name"]

    # 将 OpenAI 格式的消息转换为 JetBrains 格式
    jetbrains_messages = _convert_openai_messages_to_jetbrains(
        request.messages, tool_id_to_func_name_map
    )

    # 解释 tool_choice：
    #   None / "auto" / "required" → 全部 tools 发给 JetBrains（required 退化为 auto，
    #     JetBrains 后端无强制 hook，能做到的最接近语义就是仍把候选工具集发过去）
    #   "none" → 完全不发送 tools，模型按纯文本生成
    #   {"type":"function","function":{"name":"X"}} → 只发送指定的那一个 function
    data = []
    tools = None
    if request.tools:
        if _jetbrains_profile_supports_functions(request.model):
            tools_filtered = _filter_tools_by_choice(request.tools, request.tool_choice)
            if tools_filtered:
                data.append({"type": "json", "fqdn": "llm.parameters.functions"})
                tools = [t["function"] for t in tools_filtered]
                data.append({"type": "json", "value": json.dumps(tools)})
        else:
            # OpenAI provider 不支持 functions 参数；忽略 VS Code 自动附带的 tools，
            # 避免上游 400 导致客户端报“没有辅助消息”。
            print(f"[tools] model={request.model} 使用 OpenAI provider，已忽略 {len(request.tools)} 个 tools")

    payload = {
        "prompt": "ij.chat.request.new-chat-on-start",
        "profile": request.model,
        "chat": {"messages": jetbrains_messages},
        "parameters": {"data": data},
    }

    # 创建 OpenAI 格式的流（带自动账号切换：477/401 时最多再试 4 个账号）
    client_wants_usage = bool(
        request.stream_options and request.stream_options.get("include_usage")
    )
    include_usage = True if not request.stream else client_wants_usage
    raw_stream = _stream_with_account_fallback(account, payload, {}, client_key)
    # 共享 dict：openai_stream_adapter 写真实 token，下游 wrapper 读取以判断豁免/记录日志
    usage_capture: Dict[str, int] = {}
    openai_sse_stream = openai_stream_adapter(
        raw_stream, request.model, tools or [], include_usage=include_usage,
        _usage_capture=usage_capture,
    )

    # 返回流式或非流式响应（统计用量）
    prompt_tokens = _estimate_messages_tokens(request.messages)
    req_start = time.time()
    if request.stream:
        tracked = _tracked_stream(openai_sse_stream, account, prompt_tokens, req_start,
                                  model=request.model, client_key=client_key,
                                  usage_capture=usage_capture)
        # 成功出字 + 不满足豁免条件时才计入 key 用量
        final_stream = _stream_with_key_consume(
            tracked, client_key, model=request.model,
            usage_capture=usage_capture, est_prompt_tokens=prompt_tokens,
        )
        return StreamingResponse(
            _sse_with_keepalive(final_stream, request=http_request),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
        )
    else:
        try:
            resp = await aggregate_stream_for_non_stream_response(
                openai_sse_stream, request.model
            )
            completion_text = resp.choices[0].message.content or ""
            # 优先使用响应中捕获的真实 token 数，否则回退到估算
            real_prompt = resp.usage.get("prompt_tokens", 0)
            real_completion = resp.usage.get("completion_tokens", 0)
            actual_prompt = real_prompt if real_prompt else prompt_tokens
            actual_completion = real_completion if real_completion else _estimate_tokens(completion_text)
            # 豁免判断：输入/输出均 < 阈值则不计费
            # 注意：成功返回（未抛异常）即视为一次有效调用；不再用 completion_text 作为门槛，
            # 否则纯 tool_calls / 空文本的合法响应会被错误地豁免计费。
            is_exempt = _is_call_exempt(actual_prompt, actual_completion)
            if not is_exempt:
                _consume_key_usage(client_key, cost=MODEL_COSTS.get(request.model, 1.0))
            _record_stats(account, actual_prompt, actual_completion, req_start)
            _append_log(request.model, client_key, actual_prompt, actual_completion,
                        (time.time() - req_start) * 1000, "ok", exempt=is_exempt)
            # ── LOW 缓存写入 ────────────────────────────────────────────────
            if _ai_cacheable and _ai_cache_key:
                try:
                    _resp_dict = resp.model_dump()
                except Exception:
                    _resp_dict = None
            else:
                _resp_dict = None
            if _ai_cacheable and _ai_cache_key and _resp_dict:
                _ttl = 86400 if float(_req_body.get("temperature", 1) or 1) == 0 else 3600
                asyncio.get_running_loop().create_task(_ai_cache_set(
                    cache_key=_ai_cache_key,
                    owner_discord_id=_ai_cache_owner,
                    client_key=client_key,
                    route="/v1/chat/completions",
                    model=request.model,
                    request_hash=_ai_cache_req_hash or "",
                    response_json=_resp_dict,
                    prompt_tokens=actual_prompt,
                    completion_tokens=actual_completion,
                    ttl_sec=_ttl,
                ))
            # Idempotency-Key 写入（如果有且本次未命中）：比 exact cache 更宽松，
            # 只要求 LOW key + 非流式，并缓存本次成功响应，避免客户端 retry 重复消耗。
            if _idem_header:
                try:
                    _idem_resp_dict = resp.model_dump()
                except Exception:
                    _idem_resp_dict = None
                if _idem_resp_dict:
                    asyncio.get_running_loop().create_task(
                        _ai_idem_set(_ai_cache_owner, _idem_header, client_key, _idem_body_hash, _idem_resp_dict)
                    )
            # ────────────────────────────────────────────────────────────────
            return resp
        except Exception:
            _record_stats(account, 0, 0, req_start, error=True)
            _append_log(request.model, client_key, prompt_tokens, 0,
                        (time.time() - req_start) * 1000, "error")
            raise


# ═══════════════ OpenAI Responses API (/v1/responses) ═══════════════
# Codex CLI 及部分新版 OpenAI 客户端使用此接口格式

def _responses_convert_tools(tools_raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Responses API tool 格式 → chat completions tool 格式。

    Responses API: {"type":"function","name":"...","description":"...","parameters":{...}}
    Chat compl.:   {"type":"function","function":{"name":"...","description":"...","parameters":{...}}}
    其他内置 tool 类型（computer_use_preview / web_search_preview / code_interpreter）
    JetBrains 不支持，直接忽略。
    """
    result = []
    for t in (tools_raw or []):
        if t.get("type") != "function":
            continue
        fn: Dict[str, Any] = {
            "name": t.get("name", ""),
            "description": t.get("description", ""),
            "parameters": t.get("parameters", {}),
        }
        if "strict" in t:
            fn["strict"] = t["strict"]
        result.append({"type": "function", "function": fn})
    return result


def _responses_input_to_chat_messages(
    input_val: Any,
) -> tuple:
    """Responses API input → (List[ChatMessage], tool_id_map: Dict[str,str])

    input 可以是字符串或数组。数组中每个 item 可能是：
      - 标准消息  {"role":"user"|"assistant"|"system","content": str|list}
      - 函数调用  {"type":"function_call","call_id":"...","name":"...","arguments":"..."}
      - 工具结果  {"type":"function_call_output","call_id":"...","output":"..."}
    content 数组里的 part type 包括:
      input_text / output_text / text → 普通文本
      function_call / function_call_output → 嵌在 content 里的工具调用/结果
    """
    tool_id_map: Dict[str, str] = {}

    if isinstance(input_val, str):
        return [ChatMessage(role="user", content=input_val)], tool_id_map

    messages: List[ChatMessage] = []
    for item in (input_val or []):
        item_type = item.get("type", "")
        role = item.get("role", "")

        # ── 顶层工具结果 ──
        if item_type == "function_call_output":
            messages.append(ChatMessage(
                role="tool",
                tool_call_id=item.get("call_id", ""),
                content=str(item.get("output", "")),
            ))
            continue

        # ── 顶层函数调用（assistant 历史轮次的 function_call 输出） ──
        if item_type == "function_call":
            call_id = item.get("call_id") or item.get("id", "")
            name = item.get("name", "")
            arguments = item.get("arguments", "{}")
            if call_id and name:
                tool_id_map[call_id] = name
            messages.append(ChatMessage(
                role="assistant",
                content=None,
                tool_calls=[{"id": call_id, "type": "function",
                             "function": {"name": name, "arguments": arguments}}],
            ))
            continue

        # ── 标准消息 ──
        if item_type == "message" or role in ("user", "assistant", "system"):
            if not role:
                role = "user"
            content = item.get("content", "")

            if isinstance(content, str):
                messages.append(ChatMessage(role=role, content=content))

            elif isinstance(content, list):
                text_parts: List[str] = []
                tool_calls_list: List[Dict] = []
                tool_results: List[ChatMessage] = []

                for part in content:
                    ptype = part.get("type", "")
                    if ptype in ("input_text", "output_text", "text"):
                        txt = part.get("text", "")
                        if txt:
                            text_parts.append(txt)
                    elif ptype == "function_call_output":
                        tool_results.append(ChatMessage(
                            role="tool",
                            tool_call_id=part.get("call_id", ""),
                            content=str(part.get("output", "")),
                        ))
                    elif ptype == "function_call":
                        cid = part.get("call_id") or part.get("id", "")
                        nm = part.get("name", "")
                        args = part.get("arguments", "{}")
                        if cid and nm:
                            tool_id_map[cid] = nm
                        tool_calls_list.append({
                            "id": cid, "type": "function",
                            "function": {"name": nm, "arguments": args},
                        })

                messages.extend(tool_results)
                if tool_calls_list:
                    messages.append(ChatMessage(
                        role="assistant",
                        content=" ".join(text_parts) if text_parts else None,
                        tool_calls=tool_calls_list,
                    ))
                elif text_parts:
                    messages.append(ChatMessage(role=role, content=" ".join(text_parts)))

    return messages, tool_id_map


async def _responses_sse_adapter(
    openai_sse_stream: AsyncGenerator[str, None],
    model_name: str,
    resp_id: str,
    created_at: int,
    usage_capture: Optional[Dict[str, int]] = None,
) -> AsyncGenerator[str, None]:
    """OpenAI chat.completion.chunk SSE 流 → Responses API SSE 事件流。"""

    def _e(obj: dict) -> str:
        return f"event: {obj['type']}\ndata: {json.dumps(obj, ensure_ascii=False)}\n\n"

    # response.created
    yield _e({
        "type": "response.created",
        "response": {
            "id": resp_id, "object": "response",
            "created_at": created_at, "model": model_name,
            "output": [], "status": "in_progress",
        },
    })

    next_oi = 0  # 下一个可用 output_index

    # 文本消息状态
    msg_id = f"msg_{uuid.uuid4().hex[:24]}"
    msg_oi: Optional[int] = None   # None = 尚未分配
    text_started = False
    full_text = ""

    # 工具调用状态：按 chunk index 分组
    tc_st: Dict[int, Dict[str, Any]] = {}

    final_items: List[Dict] = []
    usage_data: Optional[Dict] = None

    try:
        async for line in openai_sse_stream:
            if not line.startswith("data: ") or line.strip() == "data: [DONE]":
                continue
            try:
                data = json.loads(line[6:].strip())
            except Exception:
                continue

            # usage-only 块
            if "usage" in data and data.get("choices") == []:
                usage_data = data["usage"]
                continue
            if not data.get("choices"):
                continue

            choice = data["choices"][0]
            delta = choice.get("delta", {})

            # ── 文本增量 ──
            text = delta.get("content") or ""
            if text:
                full_text += text
                if msg_oi is None:
                    msg_oi = next_oi
                    next_oi += 1
                    yield _e({
                        "type": "response.output_item.added",
                        "output_index": msg_oi,
                        "item": {"id": msg_id, "type": "message", "role": "assistant",
                                 "content": [], "status": "in_progress"},
                    })
                if not text_started:
                    text_started = True
                    yield _e({
                        "type": "response.content_part.added",
                        "item_id": msg_id, "output_index": msg_oi, "content_index": 0,
                        "part": {"type": "output_text", "text": ""},
                    })
                yield _e({
                    "type": "response.output_text.delta",
                    "item_id": msg_id, "output_index": msg_oi,
                    "content_index": 0, "delta": text,
                })

            # ── 工具调用增量 ──
            for tc_chunk in (delta.get("tool_calls") or []):
                idx = tc_chunk.get("index", 0)
                func = tc_chunk.get("function", {})

                if idx not in tc_st:
                    fc_id = f"fc_{uuid.uuid4().hex[:24]}"
                    call_id = tc_chunk.get("id") or f"call_{uuid.uuid4().hex}"
                    name = func.get("name", "")
                    args_init = func.get("arguments", "")
                    oi = next_oi
                    next_oi += 1
                    tc_st[idx] = {
                        "fc_id": fc_id, "call_id": call_id,
                        "name": name, "arguments": args_init, "output_index": oi,
                    }
                    yield _e({
                        "type": "response.output_item.added",
                        "output_index": oi,
                        "item": {"type": "function_call", "id": fc_id,
                                 "call_id": call_id, "name": name,
                                 "arguments": "", "status": "in_progress"},
                    })
                    if args_init:
                        yield _e({
                            "type": "response.function_call_arguments.delta",
                            "item_id": fc_id, "output_index": oi, "delta": args_init,
                        })
                else:
                    args_d = func.get("arguments", "")
                    if args_d:
                        s = tc_st[idx]
                        s["arguments"] += args_d
                        yield _e({
                            "type": "response.function_call_arguments.delta",
                            "item_id": s["fc_id"], "output_index": s["output_index"],
                            "delta": args_d,
                        })
    except Exception as _ex:
        print(f"[/v1/responses] SSE 适配器错误: {_ex}")

    # ── 关闭文本消息 ──
    if msg_oi is not None and text_started:
        yield _e({
            "type": "response.output_text.done",
            "item_id": msg_id, "output_index": msg_oi, "content_index": 0,
            "text": full_text,
        })
        yield _e({
            "type": "response.content_part.done",
            "item_id": msg_id, "output_index": msg_oi, "content_index": 0,
            "part": {"type": "output_text", "text": full_text},
        })
        done_msg: Dict[str, Any] = {
            "id": msg_id, "type": "message", "role": "assistant",
            "content": [{"type": "output_text", "text": full_text}],
            "status": "completed",
        }
        yield _e({"type": "response.output_item.done",
                  "output_index": msg_oi, "item": done_msg})
        final_items.append(done_msg)

    # ── 关闭工具调用 ──
    for idx, s in sorted(tc_st.items()):
        fc_id = s["fc_id"]
        oi = s["output_index"]
        arguments = s["arguments"]
        yield _e({
            "type": "response.function_call_arguments.done",
            "item_id": fc_id, "output_index": oi, "arguments": arguments,
        })
        done_tc: Dict[str, Any] = {
            "type": "function_call", "id": fc_id,
            "call_id": s["call_id"], "name": s["name"],
            "arguments": arguments, "status": "completed",
        }
        yield _e({"type": "response.output_item.done",
                  "output_index": oi, "item": done_tc})
        final_items.append(done_tc)

    # ── response.completed ──
    final_resp: Dict[str, Any] = {
        "id": resp_id, "object": "response",
        "created_at": created_at, "model": model_name,
        "output": final_items, "status": "completed",
    }
    # 优先用 FinishMetadata 捕获的真实 token；其次用最后一个 usage-only 块
    cap = usage_capture or {}
    real_p = cap.get("prompt_tokens") or (usage_data or {}).get("prompt_tokens") or 0
    real_c = cap.get("completion_tokens") or (usage_data or {}).get("completion_tokens") or 0
    if real_p or real_c:
        final_resp["usage"] = {
            "input_tokens": real_p,
            "output_tokens": real_c,
            "total_tokens": real_p + real_c,
        }
    yield _e({"type": "response.completed", "response": final_resp})


@app.post("/v1/responses")
async def responses_api(
    http_request: Request,
    client_key: str = Depends(authenticate_client),
):
    """OpenAI Responses API 兼容端点（Codex CLI / 新版 OpenAI SDK 使用）。

    将 Responses API 请求格式转换为 JetBrains 内部格式，
    复用与 /v1/chat/completions 完全相同的上游调用链路。
    """
    body = await http_request.json()

    model_name: str = body.get("model", "")
    input_val = body.get("input") or []
    tools_raw: List[Dict] = body.get("tools") or []
    do_stream: bool = bool(body.get("stream", False))
    max_output: Optional[int] = body.get("max_output_tokens")

    # ── 输入转换 ──
    messages, tool_id_map = _responses_input_to_chat_messages(input_val)
    if not messages:
        raise HTTPException(status_code=400, detail="input 不能为空")

    # ── 令牌限额检查（与 chat/completions 相同逻辑） ──
    max_in, max_out, _ = _key_tier_limits(client_key)
    estimated = _estimate_input_tokens(messages)
    if estimated > max_in:
        raise HTTPException(
            status_code=400,
            detail=f"输入内容过长：估算约 {estimated:,} tokens，超过限制 {max_in:,} tokens",
        )
    if max_output is not None and max_output > max_out:
        max_output = max_out

    # ── 工具格式转换 ──
    tools_chat = _responses_convert_tools(tools_raw)

    # ── 补全 tool_id_map（从历史消息中收集） ──
    for m in messages:
        if m.role == "assistant" and m.tool_calls:
            for tc in m.tool_calls:
                if tc.get("id") and tc.get("function", {}).get("name"):
                    tool_id_map[tc["id"]] = tc["function"]["name"]

    # ── JetBrains 消息格式转换 ──
    jetbrains_messages = _convert_openai_messages_to_jetbrains(messages, tool_id_map)

    # ── 工具参数 ──
    data_params: List[Dict] = []
    jb_tools: Optional[List[Dict]] = None
    if tools_chat and _jetbrains_profile_supports_functions(model_name):
        jb_tools = [t["function"] for t in tools_chat]
        data_params.append({"type": "json", "fqdn": "llm.parameters.functions"})
        data_params.append({"type": "json", "value": json.dumps(jb_tools)})
    else:
        if tools_chat:
            print(f"[/v1/responses] model={model_name} 使用 OpenAI provider，已忽略 {len(tools_chat)} 个 tools")

    payload = {
        "prompt": "ij.chat.request.new-chat-on-start",
        "profile": model_name,
        "chat": {"messages": jetbrains_messages},
        "parameters": {"data": data_params},
    }

    account = await get_next_jetbrains_account(client_key=client_key)
    usage_capture: Dict[str, int] = {}
    raw_stream = _stream_with_account_fallback(account, payload, {}, client_key)
    openai_sse = openai_stream_adapter(
        raw_stream, model_name, jb_tools or [],
        include_usage=True, _usage_capture=usage_capture,
    )

    resp_id = f"resp_{uuid.uuid4().hex}"
    created_at = int(time.time())
    prompt_tokens = _estimate_messages_tokens(messages)
    req_start = time.time()

    if do_stream:
        responses_stream = _responses_sse_adapter(
            openai_sse, model_name, resp_id, created_at, usage_capture,
        )

        async def _tracked_responses():
            has_content = False
            completion_chars = 0
            try:
                async for chunk in responses_stream:
                    yield chunk
                    if ('"response.output_text.delta"' in chunk
                            or '"response.function_call_arguments.delta"' in chunk):
                        has_content = True
                        completion_chars += len(chunk)
            finally:
                if has_content:
                    real_p = usage_capture.get("prompt_tokens") or prompt_tokens
                    real_c = usage_capture.get("completion_tokens") or max(1, completion_chars // 20)
                    is_exempt = _is_call_exempt(real_p, real_c)
                    if not is_exempt:
                        _consume_key_usage(client_key, cost=MODEL_COSTS.get(model_name, 1.0))
                    _record_stats(account, real_p, real_c, req_start)
                    _append_log(model_name, client_key, real_p, real_c,
                                (time.time() - req_start) * 1000, "ok", exempt=is_exempt)

        return StreamingResponse(
            _tracked_responses(),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
        )

    # ── 非流式：聚合 OpenAI SSE → Responses API JSON ──
    try:
        chat_resp = await aggregate_stream_for_non_stream_response(openai_sse, model_name)

        output_items: List[Dict] = []
        message = chat_resp.choices[0].message
        if message.content:
            output_items.append({
                "id": f"msg_{uuid.uuid4().hex[:24]}",
                "type": "message", "role": "assistant",
                "content": [{"type": "output_text", "text": message.content}],
                "status": "completed",
            })
        for tc in (message.tool_calls or []):
            output_items.append({
                "type": "function_call",
                "id": f"fc_{uuid.uuid4().hex[:24]}",
                "call_id": tc.get("id", f"call_{uuid.uuid4().hex}"),
                "name": tc.get("function", {}).get("name", ""),
                "arguments": tc.get("function", {}).get("arguments", "{}"),
                "status": "completed",
            })

        usage = chat_resp.usage or {}
        real_p = usage.get("prompt_tokens") or 0
        real_c = usage.get("completion_tokens") or 0
        actual_p = real_p if real_p else prompt_tokens
        actual_c = real_c if real_c else _estimate_tokens(message.content or "")

        is_exempt = _is_call_exempt(actual_p, actual_c)
        if not is_exempt:
            _consume_key_usage(client_key, cost=MODEL_COSTS.get(model_name, 1.0))
        _record_stats(account, actual_p, actual_c, req_start)
        _append_log(model_name, client_key, actual_p, actual_c,
                    (time.time() - req_start) * 1000, "ok", exempt=is_exempt)

        return JSONResponse({
            "id": resp_id,
            "object": "response",
            "created_at": created_at,
            "model": model_name,
            "output": output_items,
            "status": "completed",
            "usage": {
                "input_tokens": actual_p,
                "output_tokens": actual_c,
                "total_tokens": actual_p + actual_c,
            },
        })
    except Exception:
        _record_stats(account, 0, 0, req_start, error=True)
        _append_log(model_name, client_key, prompt_tokens, 0,
                    (time.time() - req_start) * 1000, "error")
        raise


def convert_anthropic_to_openai(
    anthropic_req: AnthropicMessageRequest,
) -> ChatCompletionRequest:
    openai_messages = []
    tool_id_to_func_name_map = {}

    if anthropic_req.system:
        system_prompt = ""
        if isinstance(anthropic_req.system, str):
            system_prompt = anthropic_req.system
        elif isinstance(anthropic_req.system, list):
            system_prompt = " ".join(
                [
                    item.get("text", "")
                    for item in anthropic_req.system
                    if isinstance(item, dict) and item.get("type") == "text"
                ]
            )
        if system_prompt:
            openai_messages.append(ChatMessage(role="system", content=system_prompt))

    for msg in anthropic_req.messages:
        if msg.role == "user":
            text_parts = []
            if isinstance(msg.content, str):
                text_parts.append(msg.content)
            else:
                for block in msg.content:
                    if block.type == "text":
                        text_parts.append(block.text)
                    elif block.type == "tool_result" and block.tool_use_id:
                        content_str = (
                            block.content
                            if isinstance(block.content, str)
                            else json.dumps(block.content)
                        )
                        openai_messages.append(
                            ChatMessage(
                                role="tool",
                                tool_call_id=block.tool_use_id,
                                content=content_str,
                            )
                        )

            if text_parts:
                openai_messages.append(
                    ChatMessage(role="user", content=" ".join(text_parts))
                )

        elif msg.role == "assistant":
            text_parts = []
            tool_calls = []
            if isinstance(msg.content, list):
                for block in msg.content:
                    if block.type == "text":
                        text_parts.append(block.text)
                    elif block.type == "tool_use" and block.id and block.name:
                        arguments = (
                            json.dumps(block.input) if block.input is not None else "{}"
                        )
                        tool_calls.append(
                            {
                                "id": block.id,
                                "type": "function",
                                "function": {
                                    "name": block.name,
                                    "arguments": arguments,
                                },
                            }
                        )
                        tool_id_to_func_name_map[block.id] = block.name

            content_text = " ".join(text_parts) if text_parts else None
            openai_messages.append(
                ChatMessage(
                    role="assistant",
                    content=content_text,
                    tool_calls=tool_calls if tool_calls else None,
                )
            )

    openai_tools = None
    if anthropic_req.tools:
        openai_tools = [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.input_schema,
                },
            }
            for t in anthropic_req.tools
        ]

    return ChatCompletionRequest(
        model=anthropic_req.model,
        messages=openai_messages,
        stream=anthropic_req.stream,
        temperature=anthropic_req.temperature,
        max_tokens=anthropic_req.max_tokens,
        top_p=anthropic_req.top_p,
        tools=openai_tools,
        stop=anthropic_req.stop_sequences,
    )


def map_finish_reason(finish_reason: Optional[str]) -> Optional[str]:
    if finish_reason == "stop":
        return "end_turn"
    if finish_reason == "length":
        return "max_tokens"
    if finish_reason == "tool_calls":
        return "tool_use"
    return finish_reason


async def openai_to_anthropic_stream_adapter(
    openai_stream: AsyncGenerator[str, None],
    model_name: str,
    usage_capture: Optional[Dict[str, int]] = None,
    est_prompt_tokens: int = 0,
) -> AsyncGenerator[str, None]:
    """OpenAI SSE → Anthropic SSE 转换。
    usage_capture 由上游 openai_stream_adapter 写入真实 token；emit 真实 input/output。
    若 FinishMetadata 缺失，message_delta 回退用估算值，保证 Anthropic 客户端始终拿到非零 usage。
    """
    message_id = f"msg_{uuid.uuid4().hex.replace('-', '')}"
    # message_start 中先发估算 input_tokens（部分客户端在此读取上下文用量）
    initial_input = int(est_prompt_tokens or 0)
    yield f"event: message_start\ndata: {json.dumps({'type': 'message_start', 'message': {'id': message_id, 'type': 'message', 'role': 'assistant', 'model': model_name, 'content': [], 'stop_reason': None, 'stop_sequence': None, 'usage': {'input_tokens': initial_input, 'output_tokens': 0}}})}\n\n"
    yield f"event: ping\ndata: {json.dumps({'type': 'ping'})}\n\n"

    content_block_index = 0
    text_block_open = False
    tool_blocks = {}  # index -> {id, name, args}
    completion_text_buf: List[str] = []  # 用于无 FinishMetadata 时回退估算 output_tokens

    async for sse_line in openai_stream:
        if not sse_line.startswith("data:") or sse_line.strip() == "data: [DONE]":
            continue

        data_str = sse_line[6:].strip()
        try:
            data = json.loads(data_str)
            if not data.get("choices"):
                continue

            delta = data["choices"][0].get("delta", {})
            finish_reason = data["choices"][0].get("finish_reason")

            if delta.get("content"):
                if not text_block_open:
                    yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': content_block_index, 'content_block': {'type': 'text', 'text': ''}})}\n\n"
                    text_block_open = True

                completion_text_buf.append(delta["content"])
                yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': content_block_index, 'delta': {'type': 'text_delta', 'text': delta['content']}})}\n\n"

            if delta.get("tool_calls"):
                if text_block_open:
                    yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': content_block_index})}\n\n"
                    text_block_open = False
                    content_block_index += 1

                for tc in delta["tool_calls"]:
                    idx = tc["index"]
                    if idx not in tool_blocks:
                        tool_blocks[idx] = {
                            "id": tc.get("id"),
                            "name": tc.get("function", {}).get("name"),
                            "args": "",
                        }
                        yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': content_block_index + idx, 'content_block': {'type': 'tool_use', 'id': tc.get('id'), 'name': tc.get('function', {}).get('name'), 'input': {}}})}\n\n"

                    if tc.get("function", {}).get("arguments"):
                        args_delta = tc["function"]["arguments"]
                        tool_blocks[idx]["args"] += args_delta
                        yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': content_block_index + idx, 'delta': {'type': 'input_json_delta', 'partial_json': args_delta}})}\n\n"

            if finish_reason:
                if text_block_open:
                    yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': content_block_index})}\n\n"

                for i in range(len(tool_blocks)):
                    yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': content_block_index + i})}\n\n"

                # 计费：优先用上游 FinishMetadata 捕获到的真实 token，否则估算
                cap = usage_capture or {}
                real_in = int(cap.get("prompt_tokens") or 0)
                real_out = int(cap.get("completion_tokens") or 0)
                final_in = real_in if real_in else int(est_prompt_tokens or 0)
                # 单调性：保证 message_delta.input_tokens >= message_start.input_tokens
                # 兼容严格的 Anthropic 客户端（防止 input_tokens 在中途下降）
                if initial_input and final_in < initial_input:
                    final_in = initial_input
                if real_out:
                    final_out = real_out
                else:
                    completion_text = "".join(completion_text_buf)
                    final_out = max(1, len(completion_text) // 4) if completion_text else 0
                message_delta_data = {
                    "type": "message_delta",
                    "delta": {
                        "stop_reason": map_finish_reason(finish_reason),
                        "stop_sequence": None,
                    },
                    "usage": {"input_tokens": final_in, "output_tokens": final_out},
                }
                yield f"event: message_delta\ndata: {json.dumps(message_delta_data)}\n\n"
                break
        except json.JSONDecodeError:
            print(f"Anthropic adapter JSON decode error: {data_str}")
            continue

    yield f"event: message_stop\ndata: {json.dumps({'type': 'message_stop'})}\n\n"


def convert_openai_to_anthropic_response(
    resp: ChatCompletionResponse,
) -> AnthropicResponseMessage:
    message = resp.choices[0].message
    content_blocks = []

    if message.content:
        content_blocks.append(
            AnthropicResponseContent(type="text", text=message.content)
        )

    if message.tool_calls:
        for tc in message.tool_calls:
            try:
                tool_input = json.loads(tc["function"]["arguments"])
            except json.JSONDecodeError:
                tool_input = {
                    "error": "invalid JSON in arguments",
                    "arguments": tc["function"]["arguments"],
                }
            content_blocks.append(
                AnthropicResponseContent(
                    type="tool_use",
                    id=tc["id"],
                    name=tc["function"]["name"],
                    input=tool_input,
                )
            )

    if not content_blocks:
        # Anthropic 严格客户端不接受空 content 数组；纯空响应兜底为一个空文本块。
        content_blocks.append(AnthropicResponseContent(type="text", text=""))

    return AnthropicResponseMessage(
        id=resp.id.replace("chatcmpl-", "msg_"),
        model=resp.model,
        content=content_blocks,
        stop_reason=map_finish_reason(resp.choices[0].finish_reason),
        usage=AnthropicUsage(
            input_tokens=resp.usage.get("prompt_tokens", 0),
            output_tokens=resp.usage.get("completion_tokens", 0),
        ),
    )


@app.post("/v1/messages", response_model=None)
async def messages_completions(
    request: AnthropicMessageRequest,
    http_request: Request,
    client_key: str = Depends(authenticate_anthropic_client),
):
    """创建符合 Anthropic 规范的聊天完成"""
    # 估算 token 数：消息 + system prompt
    system_text = ""
    if isinstance(request.system, str):
        system_text = request.system
    elif isinstance(request.system, list):
        system_text = " ".join(
            b.get("text", "") for b in request.system if isinstance(b, dict)
        )
    system_tokens = len(system_text.encode("utf-8")) // 3
    msg_tokens = _estimate_input_tokens(request.messages)
    estimated = system_tokens + msg_tokens
    max_in, max_out, _ = _key_tier_limits(client_key)
    if estimated > max_in:
        raise HTTPException(
            status_code=400,
            detail=f"输入内容过长：估算约 {estimated:,} tokens，超过单次限制 {max_in:,} tokens",
        )

    if request.max_tokens > max_out:
        print(f"[限制] max_tokens {request.max_tokens} 超过上限，已截断至 {max_out}")
        request.max_tokens = max_out

    openai_request = convert_anthropic_to_openai(request)

    # Apply model mapping specifically for /v1/messages endpoint using config from models.json
    if openai_request.model in anthropic_model_mappings:
        original_model = openai_request.model
        openai_request.model = anthropic_model_mappings[openai_request.model]
        print(f"Model mapping applied: {original_model} -> {openai_request.model}")

    model_config = get_model_item(openai_request.model)
    if not model_config:
        raise HTTPException(
            status_code=404, detail=f"模型 {openai_request.model} 未找到"
        )

    # ── LOW 用户缓存读取（/v1/messages） ─────────────────────────────────────
    _msg_body = request.model_dump(exclude_none=True)
    _msg_cacheable, _ = _is_low_cacheable_request(client_key, _msg_body)
    _msg_cache_key: Optional[str] = None
    _msg_cache_req_hash: Optional[str] = None
    _msg_meta = VALID_CLIENT_KEYS.get(client_key, {})
    _msg_is_low_key = bool(_msg_meta.get("is_low_admin_key"))
    _msg_cache_owner: str = str(_msg_meta.get("low_admin_discord_id") or "") or client_key

    # Idempotency-Key 是防重复提交/网络重试机制：
    # 只要是 LOW key + 非流式请求就生效，不受 exact-cache 的 temperature/tools 限制影响。
    _idem_hdr_msg = http_request.headers.get("Idempotency-Key") if (_msg_is_low_key and not request.stream) else None
    _idem_body_hash_msg = hashlib.sha256(_canonical_json(_msg_body).encode()).hexdigest() if _idem_hdr_msg else ""
    if _idem_hdr_msg:
        _is_msg, _ir_msg = await _ai_idem_get(_msg_cache_owner, _idem_hdr_msg, _idem_body_hash_msg)
        if _is_msg == "hit" and _ir_msg:
            if _is_valid_anthropic_message_response(_ir_msg):
                _ir_msg["cached"] = True
                _ir_msg["cache_hit"] = True
                _append_log(openai_request.model, client_key, 0, 0, 0, "ok", exempt=True)
                return JSONResponse(_ir_msg)
            await _ai_idem_delete(_msg_cache_owner, _idem_hdr_msg)
            print("[ai-idem] 丢弃旧版坏缓存：Anthropic 响应缺少有效 assistant content")
        elif _is_msg == "conflict":
            return JSONResponse(
                {"error": {"type": "invalid_request_error", "message": "Idempotency-Key conflict: body changed"}},
                status_code=409,
            )

    if _msg_cacheable:
        _msg_cache_key, _msg_cache_req_hash = _build_ai_cache_key("/v1/messages", client_key, _msg_body)

        _cached_msg = await _ai_cache_get(_msg_cache_key)
        if _cached_msg:
            if _is_valid_anthropic_message_response(_cached_msg):
                _cached_msg["cached"] = True
                _cached_msg["cache_hit"] = True
                _append_log(openai_request.model, client_key, 0, 0, 0, "ok", exempt=True)
                return JSONResponse(_cached_msg)
            await _ai_cache_delete(_msg_cache_key)
            print("[ai-cache] 丢弃旧版坏缓存：Anthropic 响应缺少有效 assistant content")
    # ────────────────────────────────────────────────────────────────────────

    account = await get_next_jetbrains_account(client_key=client_key)

    tool_id_to_func_name_map = {}
    for m in openai_request.messages:
        if m.role == "assistant" and m.tool_calls:
            for tc in m.tool_calls:
                if tc.get("id") and tc.get("function", {}).get("name"):
                    tool_id_to_func_name_map[tc["id"]] = tc["function"]["name"]

    jetbrains_messages = _convert_openai_messages_to_jetbrains(
        openai_request.messages, tool_id_to_func_name_map
    )

    # tool_choice：与 OpenAI 端口一致的语义近似（none → 不发；指定 function → 仅发那一个）
    data = []
    tools = None
    if openai_request.tools:
        if _jetbrains_profile_supports_functions(openai_request.model):
            tools_filtered = _filter_tools_by_choice(openai_request.tools, openai_request.tool_choice)
            if tools_filtered:
                data.append({"type": "json", "fqdn": "llm.parameters.functions"})
                tools = [t["function"] for t in tools_filtered]
                data.append({"type": "json", "value": json.dumps(tools)})
        else:
            # OpenAI provider 不支持 functions 参数；忽略 Anthropic→OpenAI 后携带的 tools。
            print(f"[tools] model={openai_request.model} 使用 OpenAI provider，已忽略 {len(openai_request.tools)} 个 tools")

    payload = {
        "prompt": "ij.chat.request.new-chat-on-start",
        "profile": openai_request.model,
        "chat": {"messages": jetbrains_messages},
        "parameters": {"data": data},
    }

    # 带自动账号切换的 raw 流 → 转 OpenAI SSE
    raw_stream = _stream_with_account_fallback(account, payload, {}, client_key)
    usage_capture: Dict[str, int] = {}
    openai_sse_stream = openai_stream_adapter(
        raw_stream, openai_request.model, tools or [], include_usage=False,
        _usage_capture=usage_capture,
    )

    prompt_tokens = _estimate_messages_tokens(openai_request.messages)
    req_start = time.time()
    if openai_request.stream:
        tracked = _tracked_stream(openai_sse_stream, account, prompt_tokens, req_start,
                                  model=openai_request.model, client_key=client_key,
                                  usage_capture=usage_capture)
        # 成功出字 + 不满足豁免条件时才计入 key 用量
        key_consumed = _stream_with_key_consume(
            tracked, client_key, model=openai_request.model,
            usage_capture=usage_capture, est_prompt_tokens=prompt_tokens,
        )
        anthropic_stream = openai_to_anthropic_stream_adapter(
            key_consumed, openai_request.model,
            usage_capture=usage_capture, est_prompt_tokens=prompt_tokens,
        )
        return StreamingResponse(
            _sse_with_keepalive(anthropic_stream, request=http_request),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
        )
    else:
        try:
            openai_response = await aggregate_stream_for_non_stream_response(
                openai_sse_stream, openai_request.model
            )
            completion_text = openai_response.choices[0].message.content or ""
            # 优先使用响应中真实 token，否则估算
            real_prompt = (openai_response.usage or {}).get("prompt_tokens", 0)
            real_completion = (openai_response.usage or {}).get("completion_tokens", 0)
            actual_prompt = real_prompt if real_prompt else prompt_tokens
            actual_completion = real_completion if real_completion else _estimate_tokens(completion_text)
            is_exempt = _is_call_exempt(actual_prompt, actual_completion)
            # 同 chat 路径：不再以 completion_text 为门槛，避免 tool_calls 类响应漏计费
            if not is_exempt:
                _consume_key_usage(client_key, cost=MODEL_COSTS.get(openai_request.model, 1.0))
            _record_stats(account, actual_prompt, actual_completion, req_start)
            _append_log(openai_request.model, client_key, actual_prompt, actual_completion,
                        (time.time() - req_start) * 1000, "ok", exempt=is_exempt)
            anthropic_response = convert_openai_to_anthropic_response(openai_response)
            # ── LOW 缓存写入（/v1/messages） ─────────────────────────────────
            if _msg_cacheable and _msg_cache_key:
                try:
                    _resp_dict_msg = anthropic_response.model_dump()
                except Exception:
                    _resp_dict_msg = None
                if _resp_dict_msg:
                    _ttl_msg = 86400 if float(_msg_body.get("temperature", 1) or 1) == 0 else 3600
                    asyncio.get_running_loop().create_task(_ai_cache_set(
                        cache_key=_msg_cache_key,
                        owner_discord_id=_msg_cache_owner,
                        client_key=client_key,
                        route="/v1/messages",
                        model=openai_request.model,
                        request_hash=_msg_cache_req_hash or "",
                        response_json=_resp_dict_msg,
                        prompt_tokens=actual_prompt,
                        completion_tokens=actual_completion,
                        ttl_sec=_ttl_msg,
                    ))
            # Idempotency-Key 写入（如果有且本次未命中）：比 exact cache 更宽松，
            # 只要求 LOW key + 非流式，并缓存本次成功响应，避免客户端 retry 重复消耗。
            if _idem_hdr_msg:
                try:
                    _idem_resp_dict_msg = anthropic_response.model_dump()
                except Exception:
                    _idem_resp_dict_msg = None
                if _idem_resp_dict_msg:
                    asyncio.get_running_loop().create_task(
                        _ai_idem_set(_msg_cache_owner, _idem_hdr_msg, client_key, _idem_body_hash_msg, _idem_resp_dict_msg)
                    )
            # ─────────────────────────────────────────────────────────────────
            return anthropic_response
        except Exception:
            _record_stats(account, 0, 0, req_start, error=True)
            _append_log(openai_request.model, client_key, prompt_tokens, 0,
                        (time.time() - req_start) * 1000, "error")
            raise


# ==================== 调用日志 API ====================

@app.get("/admin/logs")
async def admin_get_logs(request: Request, limit: int = 200):
    """返回最近调用日志（默认 200，最多 5000，倒序排列）。
    - ADMIN_KEY：返回全部日志。
    - LOW_ADMIN_KEY + X-Discord-Token：仅返回该 Discord 用户名下密钥的日志。
    优先从数据库查询，无 DB 时回退内存 deque。
    """
    limit = max(1, min(int(limit), 5000))
    # 区分调用者角色：LOW 用户必须按 Discord ID 过滤
    provided = request.headers.get("X-Admin-Key", "")
    is_low = bool(LOW_ADMIN_KEY and provided == LOW_ADMIN_KEY and provided != ADMIN_KEY)
    discord_id = ""
    if is_low:
        token = request.headers.get("X-Discord-Token", "").strip()
        dc_info = _DISCORD_VERIFIED.get(token) if token else None
        if not dc_info or time.time() - dc_info.get("ts", 0) > 1800:
            raise HTTPException(status_code=401, detail="请先完成 Discord 验证后再查看调用日志")
        discord_id = str(dc_info.get("user_id", "") or "")

    pool = await _get_db_pool()
    if pool:
        try:
            async with pool.acquire() as conn:
                if is_low:
                    rows = await conn.fetch(
                        "SELECT * FROM call_logs WHERE discord_id=$1 ORDER BY ts DESC LIMIT $2",
                        discord_id, limit,
                    )
                else:
                    rows = await conn.fetch(
                        "SELECT * FROM call_logs ORDER BY ts DESC LIMIT $1", limit
                    )
            logs = []
            for r in rows:
                row = dict(r)
                row["key"] = row.pop("api_key", "")
                logs.append(row)
            return {"logs": logs, "total": len(logs)}
        except Exception:
            pass  # DB 失败时回退内存

    # 内存 deque 回退
    logs = list(_call_logs)
    logs.reverse()
    if is_low:
        logs = [lg for lg in logs if str(lg.get("discord_id") or "") == discord_id]
    return {"logs": logs[:limit], "total": len(logs)}

@app.delete("/admin/logs")
async def admin_clear_logs(request: Request):
    """清空调用日志（同时清数据库和内存 deque）。
    - ADMIN_KEY：清空全部。
    - LOW_ADMIN_KEY + X-Discord-Token：仅清空该 Discord 用户名下的日志。
    """
    provided = request.headers.get("X-Admin-Key", "")
    is_low = bool(LOW_ADMIN_KEY and provided == LOW_ADMIN_KEY and provided != ADMIN_KEY)
    if is_low:
        token = request.headers.get("X-Discord-Token", "").strip()
        dc_info = _DISCORD_VERIFIED.get(token) if token else None
        if not dc_info or time.time() - dc_info.get("ts", 0) > 1800:
            raise HTTPException(status_code=401, detail="请先完成 Discord 验证后再清空日志")
        discord_id = str(dc_info.get("user_id", "") or "")
        # 数据库删除
        pool = await _get_db_pool()
        if pool:
            try:
                async with pool.acquire() as conn:
                    await conn.execute("DELETE FROM call_logs WHERE discord_id=$1", discord_id)
            except Exception:
                pass
        # 内存 deque 同步
        kept = [lg for lg in _call_logs if str(lg.get("discord_id") or "") != discord_id]
        _call_logs.clear()
        for lg in kept:
            _call_logs.append(lg)
        return {"success": True, "scope": "low_user", "discord_id": discord_id}
    # 管理员清空全部
    pool = await _get_db_pool()
    if pool:
        try:
            async with pool.acquire() as conn:
                await conn.execute("DELETE FROM call_logs")
        except Exception:
            pass
    _call_logs.clear()
    return {"success": True, "scope": "all"}


# ==================== 管理面板 API ====================

class AddAccountRequest(BaseModel):
    jwt: Optional[str] = None
    licenseId: Optional[str] = None
    authorization: Optional[str] = None

class AddKeyRequest(BaseModel):
    key: str
    usage_limit: Optional[int] = None

class UpdateModelsRequest(BaseModel):
    models: List[str]
    anthropic_model_mappings: Optional[Dict[str, str]] = {}

def _build_stats_response():
    """构建统计响应数据（asyncio 单线程，无需加锁）"""
    uptime = round(time.time() - service_start_time, 1)
    accounts_out = {}
    for account_id, s in account_stats.items():
        ttft_list = s["ttft_ms"]
        total_list = s["total_ms"]
        accounts_out[account_id] = {
            "calls": s["calls"],
            "errors": s["errors"],
            "prompt_tokens": s["prompt_tokens"],
            "completion_tokens": s["completion_tokens"],
            "avg_ttft_ms": round(sum(ttft_list) / len(ttft_list), 1) if ttft_list else None,
            "avg_total_ms": round(sum(total_list) / len(total_list), 1) if total_list else None,
            "p90_ttft_ms": round(sorted(ttft_list)[int(len(ttft_list) * 0.9)], 1) if len(ttft_list) >= 10 else None,
            "p90_total_ms": round(sorted(total_list)[int(len(total_list) * 0.9)], 1) if len(total_list) >= 10 else None,
        }
    total_calls = sum(v["calls"] for v in accounts_out.values())
    total_errors = sum(v["errors"] for v in accounts_out.values())
    total_prompt = sum(v["prompt_tokens"] for v in accounts_out.values())
    total_completion = sum(v["completion_tokens"] for v in accounts_out.values())
    return {
        "uptime_seconds": uptime,
        "total": {
            "calls": total_calls,
            "errors": total_errors,
            "prompt_tokens": total_prompt,
            "completion_tokens": total_completion,
        },
        "accounts": accounts_out,
    }

@app.get("/admin/stats")
async def admin_stats():
    """获取各账户后端用量统计（管理接口，无需鉴权）"""
    return _build_stats_response()

@app.get("/v1/stats")
async def v1_stats(_: str = Depends(authenticate_client)):
    """获取各账户后端用量统计 - /v1 路径别名（需鉴权）"""
    return _build_stats_response()

@app.get("/admin/status")
async def admin_status(request: Request):
    """获取服务状态（5s TTL 缓存）。响应里附带 role 字段供前端判定身份：
       - 'admin'     → 完整管理员
       - 'low_admin' → 次级管理员（用户面板）
    role 不进入缓存，每次根据请求头实时计算。
    """
    role = _request_role(request)
    cached = _admin_cache_get("status")
    if cached is not None:
        try:
            data = json.loads(cached)
        except Exception:
            data = {}
        data["role"] = role
        return Response(
            content=json.dumps(data, ensure_ascii=False).encode(),
            media_type="application/json",
        )
    base = {
        "status": "running",
        "accounts_count": len(JETBRAINS_ACCOUNTS),
        "keys_count": len(VALID_CLIENT_KEYS),
        "models_count": len(models_data.get("data", [])),
        "current_account_index": current_account_index,
        "db_pool_main": {
            "size": DB_POOL.get_size() if DB_POOL else 0,
            "free": DB_POOL.get_idle_size() if DB_POOL else 0,
            "max": 15,
        } if DB_POOL else None,
        "db_pool_low": {
            "size": DB_POOL_LOW.get_size() if DB_POOL_LOW else 0,
            "free": DB_POOL_LOW.get_idle_size() if DB_POOL_LOW else 0,
            "max": 20,
        } if DB_POOL_LOW else None,
    }
    _admin_cache_set("status", json.dumps(base, ensure_ascii=False).encode())
    base["role"] = role
    return Response(
        content=json.dumps(base, ensure_ascii=False).encode(),
        media_type="application/json",
    )

@app.get("/admin/accounts")
async def admin_list_accounts():
    """列出所有 JetBrains 账户（脱敏，15s TTL 缓存）"""
    cached = _admin_cache_get("accounts")
    if cached is not None:
        return Response(content=cached, media_type="application/json")
    result = []
    for i, acc in enumerate(JETBRAINS_ACCOUNTS):
        result.append({
            "index": i,
            "has_jwt": bool(acc.get("jwt")),
            "has_quota": acc.get("has_quota", True),
            "licenseId": acc.get("licenseId", ""),
            "jwt_preview": (acc.get("jwt", "")[:12] + "...") if acc.get("jwt") else "",
            "auth_preview": (acc.get("authorization", "")[:12] + "...") if acc.get("authorization") else "",
            "daily_used": acc.get("daily_used", None),
            "daily_total": acc.get("daily_total", None),
            "last_quota_check": acc.get("last_quota_check", 0),
            "account_id": _account_id(acc),
            "quota_status_reason": acc.get("quota_status_reason", None),
        })
    body = json.dumps({"accounts": result}, ensure_ascii=False).encode()
    _admin_cache_set("accounts", body)
    return Response(content=body, media_type="application/json")

@app.get("/admin/accounts/{index}/jwt")
async def admin_get_account_jwt(index: int):
    """获取指定账户的完整 JWT（用于复制）"""
    if index < 0 or index >= len(JETBRAINS_ACCOUNTS):
        raise HTTPException(status_code=404, detail="账户不存在")
    jwt = JETBRAINS_ACCOUNTS[index].get("jwt")
    if not jwt:
        raise HTTPException(status_code=404, detail="该账户暂无 JWT")
    return {"jwt": jwt}

@app.post("/admin/accounts")
async def admin_add_account(req: AddAccountRequest):
    """添加 JetBrains 账户（添加前验证凭据有效性）"""
    global JETBRAINS_ACCOUNTS
    if not req.jwt and not (req.licenseId and req.authorization):
        raise HTTPException(status_code=400, detail="需要提供 jwt 或 (licenseId + authorization)")

    # 重复检测：按 licenseId 或 JWT 判断是否已存在
    # 若已存在，返回该账号绑定的密钥（而不是报错），方便找回
    def _keys_for_account(acc: dict) -> list:
        acc_id = _account_id(acc)
        return [
            {"key": k, "usage_limit": v.get("usage_limit", 40), "usage_count": v.get("usage_count", 0)}
            for k, v in VALID_CLIENT_KEYS.items()
            if v.get("account_id") == acc_id
        ]

    if req.licenseId:
        for acc in JETBRAINS_ACCOUNTS:
            if acc.get("licenseId") and acc["licenseId"] == req.licenseId:
                return {"already_exists": True, "keys": _keys_for_account(acc)}
    elif req.jwt:
        for acc in JETBRAINS_ACCOUNTS:
            if acc.get("jwt") and acc["jwt"] == req.jwt:
                return {"already_exists": True, "keys": _keys_for_account(acc)}

    # 先构建账户对象，但不加入列表
    new_account: Dict[str, Any] = {"has_quota": True}
    if req.jwt:
        new_account["jwt"] = req.jwt
    if req.licenseId:
        new_account["licenseId"] = req.licenseId
    if req.authorization:
        new_account["authorization"] = req.authorization

    # 实际调用 JetBrains AI 接口验证凭据
    # 对于自动刷新模式，会先尝试获取 JWT；对于静态 JWT，直接调用配额接口
    await _check_quota(new_account)

    # 验证失败：自动刷新模式下无法获取 JWT（licenseId/authorization 无效）
    if not new_account.get("jwt"):
        raise HTTPException(
            status_code=422,
            detail="无法获取 JWT，请检查 licenseId 和 authorization 是否正确"
        )

    # 验证失败：JWT 无效或当日配额已耗尽
    if not new_account.get("has_quota"):
        raise HTTPException(
            status_code=422,
            detail="JWT 验证失败或当日配额已耗尽，请检查凭据是否正确且账户可用"
        )

    # 验证通过，加入账户列表并保存（持锁避免并发写 JETBRAINS_ACCOUNTS）
    async with account_rotation_lock:
        JETBRAINS_ACCOUNTS.append(new_account)
    try:
        await _save_account_to_db(new_account)
    except Exception as e:
        async with account_rotation_lock:
            try:
                JETBRAINS_ACCOUNTS.remove(new_account)
            except ValueError:
                pass
        print(f"保存账户到数据库失败: {e}")
        raise HTTPException(status_code=500, detail=f"账户验证成功，但保存到数据库失败: {e}")
    _admin_cache_invalidate("accounts", "status")
    return {"success": True, "accounts_count": len(JETBRAINS_ACCOUNTS)}

@app.delete("/admin/accounts/exhausted")
async def admin_delete_exhausted_accounts():
    """一键删除所有 has_quota=False 的账户（单条 SQL 批量删除，O(n) 内存过滤）"""
    global JETBRAINS_ACCOUNTS, current_account_index
    accs_to_delete = [acc for acc in JETBRAINS_ACCOUNTS if not acc.get("has_quota", True)]
    ids_to_delete = [_account_id(acc) for acc in accs_to_delete]
    if not ids_to_delete:
        return {"success": True, "deleted_accounts": 0, "remaining": len(JETBRAINS_ACCOUNTS)}

    # 先打标，防止 fire-and-forget 任务在删除后重新写回
    for acc in accs_to_delete:
        acc["_deleted"] = True
    # 单条 SQL 批量删除（远快于 N 次串行 DELETE）
    await _batch_delete_accounts_from_db(ids_to_delete)

    # O(n) 内存过滤（替换原来的 O(n²) list.remove 循环）
    delete_set = set(ids_to_delete)
    JETBRAINS_ACCOUNTS = [acc for acc in JETBRAINS_ACCOUNTS if _account_id(acc) not in delete_set]

    # 修正轮询索引
    if JETBRAINS_ACCOUNTS:
        current_account_index = current_account_index % len(JETBRAINS_ACCOUNTS)
    else:
        current_account_index = 0

    deleted_accounts = len(ids_to_delete)
    print(f"[手动清理] 已删除 {deleted_accounts} 个无配额账户（绑定密钥保留）")
    _admin_cache_invalidate("accounts", "status")
    return {"success": True, "deleted_accounts": deleted_accounts, "remaining": len(JETBRAINS_ACCOUNTS)}


@app.delete("/admin/accounts/{index}")
async def admin_delete_account(index: int):
    """删除指定索引的 JetBrains 账户"""
    global JETBRAINS_ACCOUNTS, current_account_index
    if index < 0 or index >= len(JETBRAINS_ACCOUNTS):
        raise HTTPException(status_code=404, detail="账户不存在")
    acc_id = _account_id(JETBRAINS_ACCOUNTS[index])
    # 先打标，防止 fire-and-forget 任务在删除后重新写回
    JETBRAINS_ACCOUNTS[index]["_deleted"] = True
    # 先删数据库；失败则直接返回错误，不修改内存
    try:
        await _delete_account_from_db(acc_id)
    except Exception as e:
        JETBRAINS_ACCOUNTS[index].pop("_deleted", None)
        print(f"删除账户 {acc_id} 失败: {e}")
        raise HTTPException(status_code=500, detail=f"数据库删除失败: {e}")
    # 数据库删除成功后再修改内存
    JETBRAINS_ACCOUNTS.pop(index)
    if current_account_index >= len(JETBRAINS_ACCOUNTS) and JETBRAINS_ACCOUNTS:
        current_account_index = 0
    _admin_cache_invalidate("accounts", "status")
    return {"success": True, "accounts_count": len(JETBRAINS_ACCOUNTS)}

@app.post("/admin/accounts/{index}/reset-quota")
async def admin_reset_account_quota(index: int):
    """手动重置指定账户的配额状态（清除 has_quota=False 标记并立即重新检查）"""
    if index < 0 or index >= len(JETBRAINS_ACCOUNTS):
        raise HTTPException(status_code=404, detail="账户不存在")
    account = JETBRAINS_ACCOUNTS[index]
    acc_id = _account_id(account)
    diag: dict = {"account_id": acc_id}

    # 若为许可证账号且有 authorization，先强制刷新 JWT
    if account.get("licenseId") and account.get("authorization"):
        old_jwt = account.get("jwt")
        try:
            await _refresh_jetbrains_jwt(account)
            new_jwt = account.get("jwt")
            diag["jwt_refresh"] = "renewed" if new_jwt != old_jwt else "unchanged"
        except Exception as jwt_err:
            diag["jwt_refresh"] = f"failed: {jwt_err}"

    # 清除旧配额状态，强制重新检查
    account["has_quota"] = True
    account["last_quota_check"] = 0
    try:
        await _check_quota(account)
        diag["quota_check"] = "ok"
    except Exception as qe:
        diag["quota_check"] = f"error: {qe}"

    diag["has_quota"]           = account.get("has_quota")
    diag["quota_status_reason"] = account.get("quota_status_reason")
    diag["daily_used"]          = account.get("daily_used")
    diag["daily_total"]         = account.get("daily_total")
    diag["success"] = True
    return diag

@app.post("/admin/accounts/reset-quota-all")
async def admin_reset_all_quota():
    """重置所有账户的配额状态并在后台分批重新检查（立即返回，可通过 /recheck-progress 查询进度）"""
    global _bulk_recheck_state

    # 若已有任务在跑，先取消并等待其 finally 执行完毕（最多 3 秒）
    if _bulk_recheck_state["running"] and _bulk_recheck_state["task"]:
        old_task = _bulk_recheck_state["task"]
        old_task.cancel()
        await asyncio.wait({old_task}, timeout=3.0)

    total = len(JETBRAINS_ACCOUNTS)
    if total == 0:
        return {"success": True, "message": "当前没有账号，无需重检", "total": 0}
    for account in JETBRAINS_ACCOUNTS:
        account["has_quota"] = True
        account["last_quota_check"] = 0

    _bulk_recheck_state.update({"running": True, "total": total, "done": 0, "task": None})

    async def _bg_recheck_all():
        """分块并发检查，每块完成后批量写 DB，全程让出事件循环给正常请求"""
        _CHUNK = 50        # 每块账号数：控制同时创建的协程数量
        _CONCURRENCY = 8   # 块内最大并发 HTTP 请求数
        semaphore = asyncio.Semaphore(_CONCURRENCY)

        async def _check_one(acc: dict):
            async with semaphore:
                try:
                    await _check_quota(acc)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[批量重检] 账号 {_account_id(acc)} 检测出错: {e}")
            _bulk_recheck_state["done"] += 1

        try:
            snapshot = list(JETBRAINS_ACCOUNTS)
            for chunk_start in range(0, len(snapshot), _CHUNK):
                chunk = snapshot[chunk_start: chunk_start + _CHUNK]
                await asyncio.gather(*[_check_one(acc) for acc in chunk])
                # 每块完成后批量写 DB（一次连接代替 N 次单独写入）
                await _batch_save_accounts_to_db(chunk)
                # 让出事件循环，避免长时间霸占调度器
                await asyncio.sleep(0)

            has_q = sum(1 for a in JETBRAINS_ACCOUNTS if a.get("has_quota"))
            print(f"[批量重检] 完成：{has_q}/{total} 个账号有配额")
        except asyncio.CancelledError:
            print(f"[批量重检] 已取消（已完成 {_bulk_recheck_state['done']}/{total}）")
        finally:
            _bulk_recheck_state["running"] = False
            _bulk_recheck_state["task"] = None

    task = asyncio.create_task(_bg_recheck_all())
    _bulk_recheck_state["task"] = task
    return {
        "success": True,
        "message": f"已在后台启动对 {total} 个账号的配额重检，可通过 /admin/accounts/recheck-progress 查询进度",
        "total": total,
    }


@app.get("/admin/accounts/recheck-progress")
async def admin_recheck_progress():
    """查询全量重检任务的当前进度"""
    s = _bulk_recheck_state
    total = s["total"] or 0
    done = s["done"]
    return {
        "running": s["running"],
        "total": total,
        "done": done,
        "percent": round(done / total * 100, 1) if total else 0,
    }


@app.post("/admin/accounts/recheck-cancel")
async def admin_recheck_cancel():
    """取消正在进行的全量重检任务"""
    if not _bulk_recheck_state["running"]:
        return {"success": False, "message": "当前没有运行中的重检任务"}
    task = _bulk_recheck_state.get("task")
    if task:
        task.cancel()
    return {"success": True, "message": "取消信号已发送"}


@app.post("/admin/accounts/turbo-recheck")
async def admin_turbo_recheck(concurrency: int = 120, delete_empty: bool = False):
    """极速全量配额重检。

    与 reset-quota-all 的区别：
    - 跳过 JWT 刷新（用现有 JWT 直查 /quota/get，省去 1-2 个额外 HTTP 往返）
    - 并发默认 120（原来 8），可通过 ?concurrency=N 调节，最大 300
    - 每 500 账号批量写一次 DB（原来每账号单独写）
    - 401 账号标记 has_quota=False 但不从内存池删除（等扫完后统一清理）
    - delete_empty=true（默认）：扫完后从内存+DB 删除确认无配额的账号
    预计完成时间：~5-10 分钟（62K 账号）
    """
    global _bulk_recheck_state

    _concurrency = max(1, min(concurrency, 300))

    # 若已有任务在跑，先取消
    if _bulk_recheck_state["running"] and _bulk_recheck_state["task"]:
        old_task = _bulk_recheck_state["task"]
        old_task.cancel()
        await asyncio.wait({old_task}, timeout=3.0)

    total = len(JETBRAINS_ACCOUNTS)
    if total == 0:
        return {"success": True, "message": "当前没有账号，无需重检", "total": 0}

    _bulk_recheck_state.update({"running": True, "total": total, "done": 0, "task": None})
    _t_start = time.time()

    async def _bg_turbo():
        global current_account_index
        semaphore = asyncio.Semaphore(_concurrency)
        _BATCH_SAVE = 500   # 每满 500 个账号批量写 DB 一次
        pending_save: list = []
        _done_count = 0
        _429_count  = 0

        async def _one(acc: dict):
            nonlocal pending_save, _done_count, _429_count
            async with semaphore:
                try:
                    result = await _check_quota_fast(acc)
                    if not result:
                        pass  # has_quota 已在函数内更新
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"[turbo-recheck] 账号 {_account_id(acc)} 异常: {e}")
                pending_save.append(acc)
                _done_count += 1
                _bulk_recheck_state["done"] = _done_count
                if len(pending_save) >= _BATCH_SAVE:
                    batch, pending_save = pending_save[:], []
                    try:
                        await _batch_save_accounts_to_db(batch)
                    except Exception as e:
                        print(f"[turbo-recheck] 批量写 DB 失败: {e}")

        try:
            snapshot = list(JETBRAINS_ACCOUNTS)
            await asyncio.gather(*[_one(acc) for acc in snapshot])
            # 写入剩余未满一批的账号
            if pending_save:
                try:
                    await _batch_save_accounts_to_db(pending_save)
                except Exception as e:
                    print(f"[turbo-recheck] 最终批量写 DB 失败: {e}")

            elapsed = time.time() - _t_start
            has_q   = sum(1 for a in JETBRAINS_ACCOUNTS if a.get("has_quota"))
            no_q    = total - has_q
            print(f"[turbo-recheck] 完成：{has_q} 有配额，{no_q} 无配额，耗时 {elapsed:.0f}s")

            # 可选：删除确认无配额的账号（内存 + DB）
            if delete_empty and no_q > 0:
                no_q_accs = [a for a in JETBRAINS_ACCOUNTS if not a.get("has_quota")]
                no_q_ids  = [_account_id(a) for a in no_q_accs]
                try:
                    for a in no_q_accs:
                        a["_deleted"] = True
                    await _batch_delete_accounts_from_db(no_q_ids)
                    delete_set = set(no_q_ids)
                    async with account_rotation_lock:
                        JETBRAINS_ACCOUNTS[:] = [
                            a for a in JETBRAINS_ACCOUNTS if _account_id(a) not in delete_set
                        ]
                        if JETBRAINS_ACCOUNTS and current_account_index >= len(JETBRAINS_ACCOUNTS):
                            current_account_index = 0
                    print(f"[turbo-recheck] 已从内存+DB 删除 {len(no_q_ids)} 个无配额账号，"
                          f"剩余 {len(JETBRAINS_ACCOUNTS)} 个")
                except Exception as e:
                    print(f"[turbo-recheck] 删除无配额账号失败: {e}")

        except asyncio.CancelledError:
            print(f"[turbo-recheck] 已取消（已完成 {_bulk_recheck_state['done']}/{total}）")
            if pending_save:
                try:
                    await _batch_save_accounts_to_db(pending_save)
                except Exception:
                    pass
        finally:
            _bulk_recheck_state["running"] = False
            _bulk_recheck_state["task"] = None

    task = asyncio.create_task(_bg_turbo())
    _bulk_recheck_state["task"] = task
    eta_minutes = round(total / _concurrency * 0.8 / 60, 1)
    return {
        "success": True,
        "message": (
            f"已启动极速重检：{total} 个账号，并发 {_concurrency}，"
            f"预计 {eta_minutes} 分钟完成。"
            f"通过 /admin/accounts/recheck-progress 查询进度，"
            f"/admin/accounts/recheck-cancel 取消。"
        ),
        "total": total,
        "concurrency": _concurrency,
        "eta_minutes": eta_minutes,
        "delete_empty": delete_empty,
    }


@app.get("/admin/keys")
async def admin_list_keys():
    """列出所有客户端 API 密钥（含用量和封禁状态，15s TTL 缓存）"""
    cached = _admin_cache_get("keys")
    if cached is not None:
        return Response(content=cached, media_type="application/json")
    keys_list = []
    keys_masked = []
    for k, meta in VALID_CLIENT_KEYS.items():
        if not meta.get("banned"):
            keys_list.append(k)
            keys_masked.append((k[:8] + "*" * (len(k) - 8)) if len(k) > 8 else "***")
    keys_with_meta = [
        {
            "key": k,
            "masked": (k[:8] + "*" * (len(k) - 8)) if len(k) > 8 else "***",
            "usage_limit": VALID_CLIENT_KEYS[k].get("usage_limit"),
            "usage_count": VALID_CLIENT_KEYS[k].get("usage_count", 0),
            "usage_cost": round(
                VALID_CLIENT_KEYS[k].get("usage_count", 0) + _key_fractional_usage.get(k, 0.0),
                2,
            ),
            "account_id": VALID_CLIENT_KEYS[k].get("account_id"),
            "banned": bool(VALID_CLIENT_KEYS[k].get("banned", False)),
            "banned_at": VALID_CLIENT_KEYS[k].get("banned_at"),
            "is_nc_key": bool(VALID_CLIENT_KEYS[k].get("is_nc_key", False)),
            "is_low_admin_key": bool(VALID_CLIENT_KEYS[k].get("is_low_admin_key", False)),
            "low_admin_discord_id": str(VALID_CLIENT_KEYS[k].get("low_admin_discord_id", "") or ""),
        }
        for k in VALID_CLIENT_KEYS
    ]
    banned_count = sum(1 for v in VALID_CLIENT_KEYS.values() if v.get("banned"))
    body = json.dumps({
        "keys": keys_list,
        "keys_masked": keys_masked,
        "keys_with_meta": keys_with_meta,
        "count": len(VALID_CLIENT_KEYS) - banned_count,
        "banned_count": banned_count,
    }, ensure_ascii=False).encode()
    _admin_cache_set("keys", body)
    return Response(content=body, media_type="application/json")


@app.post("/admin/keys/{key}/ban")
async def admin_ban_key(key: str):
    """手动封禁一个 API 密钥"""
    if key not in VALID_CLIENT_KEYS:
        raise HTTPException(status_code=404, detail="密钥不存在")
    if VALID_CLIENT_KEYS[key].get("banned"):
        return {"success": True, "already_banned": True}
    ban_ts = time.time()
    VALID_CLIENT_KEYS[key]["banned"] = True
    VALID_CLIENT_KEYS[key]["banned_at"] = ban_ts
    await _upsert_key_to_db(key, VALID_CLIENT_KEYS[key])
    # 若是 LOW 用户的个人 key，持久化封禁审计；同时与 import 互斥，
    # 防止"管理员封禁中"与"用户导入中"交错产生 banned=False 的内存状态
    _low_did = str(VALID_CLIENT_KEYS[key].get("low_admin_discord_id") or "")
    if _low_did:
        await _persist_low_user_ban(_low_did, ban_ts)
    print(f"[管理员] 手动封禁密钥 {key[:20]}...")
    _admin_cache_invalidate("keys", "status")
    return {"success": True, "already_banned": False}


@app.post("/admin/keys/{key}/unban")
async def admin_unban_key(key: str):
    """解封一个被封禁的 API 密钥"""
    if key not in VALID_CLIENT_KEYS:
        raise HTTPException(status_code=404, detail="密钥不存在")
    VALID_CLIENT_KEYS[key]["banned"] = False
    VALID_CLIENT_KEYS[key]["banned_at"] = None
    await _upsert_key_to_db(key, VALID_CLIENT_KEYS[key])
    print(f"[管理员] 已解封密钥 {key[:20]}...")
    _admin_cache_invalidate("keys", "status")
    return {"success": True}


@app.post("/admin/keys/unban-all")
async def admin_unban_all_keys():
    """一键解封所有被封禁的 API 密钥"""
    banned_keys = [(k, v) for k, v in VALID_CLIENT_KEYS.items() if v.get("banned")]
    if not banned_keys:
        return {"success": True, "unbanned_count": 0}

    for k, v in banned_keys:
        v["banned"] = False
        v["banned_at"] = None
        asyncio.create_task(_upsert_key_to_db(k, dict(v)))

    print(f"[管理员] 一键解封 {len(banned_keys)} 个密钥")
    _admin_cache_invalidate("keys", "status")
    return {"success": True, "unbanned_count": len(banned_keys)}


@app.post("/admin/keys")
async def admin_add_key(req: AddKeyRequest):
    """添加客户端 API 密钥（可选 usage_limit 限制使用次数）"""
    global VALID_CLIENT_KEYS
    if not req.key or len(req.key) < 4:
        raise HTTPException(status_code=400, detail="密钥长度不能小于 4 个字符")
    meta = {"usage_limit": req.usage_limit, "usage_count": 0}
    VALID_CLIENT_KEYS[req.key] = meta
    await _upsert_key_to_db(req.key, meta)
    _admin_cache_invalidate("keys", "status")
    return {"success": True, "keys_count": len(VALID_CLIENT_KEYS)}

@app.post("/admin/keys/cleanup-pending")
async def admin_cleanup_pending_keys(request: Request):
    """手动清除【等待返回参数】中不在排队行列里的 key：
    ① 超时 >40 分钟且仍未绑定额度的密钥；
    ② jb_accounts 中 pending_nc_key 存在但 pending_nc_lids 为空的僵尸 NC key。"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    try:
        result = await _do_cleanup_pending_param_keys()
        return {
            "success": True,
            "expired_keys_deleted": result["expired_keys"],
            "zombie_nc_keys_cleared": result["zombie_keys"],
            "memory_removed": result["memory_removed"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/admin/keys/exhausted")
async def admin_delete_exhausted_keys():
    """一键删除所有 usage_count >= usage_limit 的已耗尽密钥（排除等待绑定中的预签 key）"""
    # usage_limit=0 的 key 是等待激活的预签 key，不算耗尽，不得删除
    exhausted = [k for k, v in list(VALID_CLIENT_KEYS.items())
                 if v.get("usage_limit") is not None
                 and v["usage_limit"] > 0
                 and v.get("usage_count", 0) >= v["usage_limit"]]
    for k in exhausted:
        VALID_CLIENT_KEYS.pop(k, None)
        await _delete_key_from_db(k)
    print(f"[手动清理] 已删除 {len(exhausted)} 个已耗尽的 API 密钥")
    _admin_cache_invalidate("keys", "status")
    return {"success": True, "deleted_keys": len(exhausted), "remaining": len(VALID_CLIENT_KEYS)}


@app.patch("/admin/keys/{key}/set-usage")
async def admin_set_key_usage(key: str, request: Request):
    """静默修改密钥的已用量（usage_count）"""
    body = await request.json()
    if key not in VALID_CLIENT_KEYS:
        raise HTTPException(status_code=404, detail="密钥不存在")
    new_count = body.get("usage_count")
    if new_count is None or not isinstance(new_count, (int, float)) or int(new_count) < 0:
        raise HTTPException(status_code=400, detail="usage_count 必须为非负整数")
    new_count = int(new_count)
    VALID_CLIENT_KEYS[key]["usage_count"] = new_count
    # 同步清理小数余量
    _key_fractional_usage.pop(key, None)
    if DB_POOL:
        async with DB_POOL.acquire() as conn:
            await conn.execute(
                "UPDATE jb_client_keys SET usage_count = $1 WHERE key = $2",
                new_count, key,
            )
    _admin_cache_invalidate("keys")
    return {"success": True, "key": key[:8] + "***", "usage_count": new_count}


@app.patch("/admin/keys/{key}/set-limit")
async def admin_set_key_limit(key: str, request: Request):
    """修改密钥的额度上限（usage_limit）。传 null 或省略 → 不限次数。"""
    body = await request.json()
    if key not in VALID_CLIENT_KEYS:
        raise HTTPException(status_code=404, detail="密钥不存在")
    raw_limit = body.get("usage_limit", None)
    new_limit: Optional[int]
    if raw_limit is None:
        new_limit = None
    else:
        if not isinstance(raw_limit, (int, float)) or int(raw_limit) < 0:
            raise HTTPException(status_code=400, detail="usage_limit 必须为非负整数或 null")
        new_limit = int(raw_limit)
    VALID_CLIENT_KEYS[key]["usage_limit"] = new_limit
    if DB_POOL:
        async with DB_POOL.acquire() as conn:
            await conn.execute(
                "UPDATE jb_client_keys SET usage_limit = $1 WHERE key = $2",
                new_limit, key,
            )
    _admin_cache_invalidate("keys")
    return {"success": True, "key": key[:8] + "***", "usage_limit": new_limit}


@app.delete("/admin/keys/bulk")
async def admin_delete_keys_bulk(request: Request):
    """批量删除指定密钥列表"""
    global VALID_CLIENT_KEYS
    body = await request.json()
    keys_to_delete = body.get("keys", [])
    if not isinstance(keys_to_delete, list) or not keys_to_delete:
        raise HTTPException(status_code=400, detail="keys 必须为非空数组")
    deleted = []
    for k in keys_to_delete:
        if k in VALID_CLIENT_KEYS:
            del VALID_CLIENT_KEYS[k]
            deleted.append(k)
    if deleted:
        if DB_POOL:
            try:
                async with DB_POOL.acquire() as conn:
                    await conn.execute(
                        "DELETE FROM jb_client_keys WHERE key = ANY($1::text[])",
                        deleted,
                    )
            except Exception as e:
                print(f"批量删除密钥时出错: {e}")
        else:
            await _save_keys_to_db()
    print(f"[批量删除] 已删除 {len(deleted)} 个密钥，剩余 {len(VALID_CLIENT_KEYS)} 个")
    _admin_cache_invalidate("keys", "status")
    return {"success": True, "deleted": len(deleted), "remaining": len(VALID_CLIENT_KEYS)}


@app.delete("/admin/keys/{key}")
async def admin_delete_key(key: str):
    """删除客户端 API 密钥"""
    global VALID_CLIENT_KEYS
    if key not in VALID_CLIENT_KEYS:
        raise HTTPException(status_code=404, detail="密钥不存在")
    del VALID_CLIENT_KEYS[key]
    await _delete_key_from_db(key)
    _admin_cache_invalidate("keys", "status")
    return {"success": True, "keys_count": len(VALID_CLIENT_KEYS)}

@app.get("/admin/prizes")
async def admin_list_prizes():
    """获取所有奖品"""
    pool = await _get_db_pool()
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, quantity, weight, is_active, created_at FROM lottery_prizes ORDER BY id")
    return [dict(r) for r in rows]


@app.post("/admin/prizes")
async def admin_create_prize(request: Request):
    """新增奖品"""
    data = await request.json()
    name = data.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="奖品名不能为空")
    quantity = int(data.get("quantity", -1))
    weight = max(1, int(data.get("weight", 10)))
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO lottery_prizes (name, quantity, weight) VALUES ($1, $2, $3) RETURNING id, name, quantity, weight, is_active, created_at",
            name, quantity, weight
        )
    _admin_cache_invalidate("prizes")
    return dict(row)


@app.put("/admin/prizes/{prize_id}")
async def admin_update_prize(prize_id: int, request: Request):
    """更新奖品"""
    data = await request.json()
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT id FROM lottery_prizes WHERE id=$1", prize_id)
        if not existing:
            raise HTTPException(status_code=404, detail="奖品不存在")
        fields, vals, idx = [], [], 1
        for col in ("name", "quantity", "weight", "is_active"):
            if col in data:
                v = data[col]
                if col == "name":
                    v = str(v).strip()
                    if not v:
                        continue
                if col == "weight":
                    v = max(1, int(v))
                if col == "quantity":
                    v = int(v)
                fields.append(f"{col}=${idx}")
                vals.append(v)
                idx += 1
        if not fields:
            raise HTTPException(status_code=400, detail="无有效字段")
        vals.append(prize_id)
        row = await conn.fetchrow(
            f"UPDATE lottery_prizes SET {', '.join(fields)} WHERE id=${idx} RETURNING id, name, quantity, weight, is_active, created_at",
            *vals
        )
    _admin_cache_invalidate("prizes")
    return dict(row)


@app.delete("/admin/prizes/{prize_id}")
async def admin_delete_prize(prize_id: int):
    """删除奖品"""
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM lottery_prizes WHERE id=$1", prize_id)
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="奖品不存在")
    _admin_cache_invalidate("prizes")
    return {"success": True}


@app.post("/admin/award-prize")
async def admin_award_prize(request: Request):
    """管理员手动给指定 DC 账号发放奖品。

    入参 ``dc_identifier`` 支持三种形态：
      1. Discord 用户 ID（纯数字）—— 直接拼成 ``dc_{id}`` 作为 owner_key。
      2. ``dc_<id>`` 格式 —— 直接作为 owner_key。
      3. Discord 用户名 / dc_tag —— 在 saint_points / donated_jb_accounts 表中
         反查得到对应的 owner_key（不区分大小写）。
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    dc_identifier = (body.get("dc_identifier") or "").strip()
    prize_name = (body.get("prize_name") or "").strip()
    if not dc_identifier or not prize_name:
        raise HTTPException(status_code=400, detail="缺少 dc_identifier 或 prize_name")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    owner_key: Optional[str] = None
    resolved_tag = ""

    # 优先级：dc_<id> 完整格式 > 真实 dc_tag 反查 > 纯数字按 Discord 用户 ID 处理。
    # 这样既兼容用户名是数字串的少数情况，又支持直接用 Discord 用户 ID 发奖。
    if dc_identifier.startswith("dc_") and dc_identifier[3:].isdigit():
        owner_key = dc_identifier
    else:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT password, dc_tag FROM saint_points "
                "WHERE LOWER(dc_tag)=LOWER($1) AND password LIKE 'dc_%' LIMIT 1",
                dc_identifier,
            )
            if row:
                owner_key = row["password"]
                resolved_tag = row["dc_tag"] or ""
            else:
                row2 = await conn.fetchrow(
                    "SELECT dc_password, dc_tag FROM donated_jb_accounts "
                    "WHERE LOWER(dc_tag)=LOWER($1) AND COALESCE(dc_password,'')<>'' LIMIT 1",
                    dc_identifier,
                )
                if row2:
                    owner_key = row2["dc_password"]
                    resolved_tag = row2["dc_tag"] or ""
        # 反查未命中且为纯数字 → 按 Discord 用户 ID 处理（典型长度 17-20 位）
        if not owner_key and dc_identifier.isdigit() and len(dc_identifier) >= 17:
            owner_key = f"dc_{dc_identifier}"

    if not owner_key:
        raise HTTPException(
            status_code=404,
            detail=(
                f"未找到 DC 账号「{dc_identifier}」"
                "（可输入 Discord 用户名 或 17 位以上的 Discord 用户 ID）"
            ),
        )

    # 反查最新 dc_tag（即便是按 ID 命中也尽量带上展示名）
    if not resolved_tag:
        async with pool.acquire() as conn:
            tag_row = await conn.fetchrow(
                "SELECT dc_tag FROM saint_points WHERE password=$1 AND dc_tag<>'' LIMIT 1",
                owner_key,
            )
            if tag_row:
                resolved_tag = tag_row["dc_tag"]

    cap = _parse_pokeball_capacity(prize_name)
    quota_amt = _parse_quota_amount(prize_name)
    metadata: dict = {}
    if cap is not None:
        metadata["pokeball_capacity"] = cap
    if quota_amt is not None:
        metadata["quota_amount"] = quota_amt

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO user_items (owner_key, prize_name, metadata) VALUES ($1,$2,$3) RETURNING id, created_at",
            owner_key, prize_name, json.dumps(metadata),
        )
    return {
        "success": True,
        "item_id": row["id"],
        "owner_key": owner_key,
        "dc_tag": resolved_tag,
        "prize_name": prize_name,
    }


@app.post("/admin/award-prize-all")
async def admin_award_prize_all(request: Request):
    """管理员给所有已注册 DC 用户批量发放同一奖品。

    从 saint_points（password LIKE 'dc_%'）收集所有 owner_key，逐条插入 user_items。
    返回成功发放人数。
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    prize_name = (body.get("prize_name") or "").strip()
    if not prize_name:
        raise HTTPException(status_code=400, detail="缺少 prize_name")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    cap = _parse_pokeball_capacity(prize_name)
    quota_amt = _parse_quota_amount(prize_name)
    metadata: dict = {}
    if cap is not None:
        metadata["pokeball_capacity"] = cap
    if quota_amt is not None:
        metadata["quota_amount"] = quota_amt
    meta_json = json.dumps(metadata)

    async with pool.acquire() as conn:
        # 取所有已注册 DC 用户的唯一 owner_key（去重）
        rows = await conn.fetch(
            "SELECT DISTINCT password AS owner_key FROM saint_points "
            "WHERE password LIKE 'dc_%' AND password <> ''"
        )
        if not rows:
            return {"success": True, "awarded_count": 0, "prize_name": prize_name, "detail": "没有可发放的用户"}

        owner_keys = [r["owner_key"] for r in rows]
        # 批量插入（executemany 在单连接内串行，但避免 N 次网络往返）
        await conn.executemany(
            "INSERT INTO user_items (owner_key, prize_name, metadata) VALUES ($1, $2, $3)",
            [(k, prize_name, meta_json) for k in owner_keys],
        )

    print(f"[管理员] 给全体 {len(owner_keys)} 名用户批量发放「{prize_name}」")
    return {
        "success": True,
        "awarded_count": len(owner_keys),
        "prize_name": prize_name,
    }


@app.get("/admin/saint-points-export")
async def admin_saint_points_export(request: Request):
    """批量导出圣人积分（供跨环境迁移使用）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT password, points, total_earned, dc_tag FROM saint_points ORDER BY password"
        )
    return [dict(r) for r in rows]


@app.get("/admin/saint-donations-export")
async def admin_saint_donations_export(request: Request):
    """批量导出捐献记录（供跨环境迁移使用）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT account_id, password, donated_at FROM saint_donations ORDER BY account_id"
        )
    return [dict(r) for r in rows]


@app.get("/admin/user-passwords-export")
async def admin_user_passwords_export(request: Request):
    """批量导出用户密码列表（DC 绑定关键，供跨环境迁移使用）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT password FROM user_passwords ORDER BY password")
    return [r["password"] for r in rows]


@app.get("/admin/user-items-export")
async def admin_user_items_export(request: Request):
    """批量导出背包物品（含时间戳，供跨环境迁移使用）"""
    import json as _json
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT owner_key, prize_name, metadata::text as metadata, used, used_at, created_at "
            "FROM user_items ORDER BY id"
        )
    return [
        {
            "owner_key":  r["owner_key"],
            "prize_name": r["prize_name"],
            "metadata":   _json.loads(r["metadata"]) if r["metadata"] else {},
            "used":       bool(r["used"]),
            "used_at":    r["used_at"].isoformat() if r["used_at"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


@app.post("/admin/reset-sequences")
async def admin_reset_sequences(request: Request):
    """一次性修复：重置所有 SERIAL 序列，解决迁移后新行 id 冲突问题。"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        return JSONResponse(status_code=503, content={"error": "DB unavailable"})
    async with pool.acquire() as conn:
        msgs = await _reset_serial_sequences(conn)
    for m in msgs:
        print(m, flush=True)
    return {"ok": True, "results": msgs}


@app.get("/admin/pokeballs-export")
async def admin_pokeballs_export(request: Request):
    """批量导出宝可梦球及成员（供跨环境迁移使用）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    async with pool.acquire() as conn:
        pbs = await conn.fetch(
            "SELECT id, ball_key, name, capacity, total_used, rr_index FROM pokeballs ORDER BY id"
        )
        members_rows = await conn.fetch(
            "SELECT p.ball_key, pm.member_key FROM pokeball_members pm "
            "JOIN pokeballs p ON p.id = pm.pokeball_id ORDER BY pm.id"
        )
    pb_members: dict = {}
    for row in members_rows:
        pb_members.setdefault(row["ball_key"], []).append(row["member_key"])
    return [
        {**dict(pb), "members": pb_members.get(pb["ball_key"], [])}
        for pb in pbs
    ]


@app.get("/admin/backpack-pokeball-repair-preview")
async def backpack_pokeball_repair_preview(request: Request):
    """
    预览宝可梦球背包修复情况（不执行任何写操作）。
    返回：
      - auto_fix: 可自动修复的条目（1对1唯一容量匹配）
      - ambiguous: 有歧义需要手动指定的条目
      - orphan_pokeballs: 找不到任何候选断链道具的孤儿球
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        # 断链道具：used=TRUE 且 metadata 里没有 ball_key
        broken_rows = await conn.fetch("""
            SELECT id, owner_key, prize_name, metadata::text AS metadata
            FROM user_items
            WHERE used = TRUE
              AND metadata->>'ball_key' IS NULL
              AND (
                  prize_name LIKE '%宝可梦球%'
                  OR metadata->>'pokeball_id' IS NOT NULL
              )
            ORDER BY id
        """)
        # 孤儿球：没有任何 user_items.metadata.ball_key 指向它
        orphan_rows = await conn.fetch("""
            SELECT id, ball_key, name, capacity
            FROM pokeballs
            WHERE ball_key NOT IN (
                SELECT metadata->>'ball_key'
                FROM user_items
                WHERE metadata->>'ball_key' IS NOT NULL
            )
            ORDER BY id
        """)

    import json as _json

    # 解析断链道具的容量
    broken = []
    for r in broken_rows:
        meta = _json.loads(r["metadata"]) if r["metadata"] else {}
        cap = _parse_pokeball_capacity(r["prize_name"]) or meta.get("capacity")
        broken.append({
            "item_id":    r["id"],
            "owner_key":  r["owner_key"],
            "prize_name": r["prize_name"],
            "capacity":   cap,
            "old_pokeball_id": meta.get("pokeball_id"),
        })

    orphans = [
        {"ball_key": r["ball_key"], "name": r["name"], "capacity": r["capacity"], "db_id": r["id"]}
        for r in orphan_rows
    ]

    # 按容量分组
    from collections import defaultdict
    broken_by_cap: dict = defaultdict(list)   # capacity → [broken_item]
    orphan_by_cap: dict = defaultdict(list)   # capacity → [orphan_pb]
    for b in broken:
        broken_by_cap[b["capacity"]].append(b)
    for o in orphans:
        orphan_by_cap[o["capacity"]].append(o)

    auto_fix = []
    ambiguous = []
    leftover_orphans = []

    all_caps = set(broken_by_cap.keys()) | set(orphan_by_cap.keys())
    for cap in sorted(all_caps, key=lambda x: (x is None, x)):
        b_list = broken_by_cap.get(cap, [])
        o_list = orphan_by_cap.get(cap, [])
        if not b_list:
            leftover_orphans.extend(o_list)
            continue
        if not o_list:
            for b in b_list:
                ambiguous.append({"broken_item": b, "candidates": [], "reason": "无匹配容量的孤儿球"})
            continue
        if len(b_list) == 1 and len(o_list) == 1:
            # 唯一匹配，可自动修复
            auto_fix.append({"broken_item": b_list[0], "assign_ball_key": o_list[0]["ball_key"],
                              "pokeball_name": o_list[0]["name"]})
        else:
            # 数量相等但有多个，或不等，需要手动指定
            reason = "多对多匹配，需手动指定" if len(b_list) == len(o_list) else f"断链道具{len(b_list)}个 vs 孤儿球{len(o_list)}个，数量不符"
            for b in b_list:
                ambiguous.append({"broken_item": b, "candidates": o_list, "reason": reason})

    return {
        "auto_fix_count":  len(auto_fix),
        "ambiguous_count": len(ambiguous),
        "orphan_only_count": len(leftover_orphans),
        "auto_fix":        auto_fix,
        "ambiguous":       ambiguous,
        "orphan_pokeballs_no_broken_item": leftover_orphans,
        "hint": "对 ambiguous 条目，请用 POST /admin/backpack-pokeball-repair 的 force_mapping 参数手动指定 item_id→ball_key",
    }


@app.post("/admin/backpack-pokeball-repair")
async def backpack_pokeball_repair(request: Request):
    """
    执行宝可梦球背包修复：
    1. 自动处理所有唯一容量匹配（同 preview 的 auto_fix 列表）
    2. 执行 force_mapping 中手动指定的映射（优先级更高）

    请求体（可选）:
    {
        "force_mapping": [
            {"item_id": 123, "ball_key": "jb-pb-xxxx"},
            ...
        ],
        "dry_run": false
    }
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    dry_run = bool(body.get("dry_run", False))
    force_mapping: list = body.get("force_mapping", [])   # [{item_id, ball_key}, ...]

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    import json as _json
    from collections import defaultdict

    async with pool.acquire() as conn:
        broken_rows = await conn.fetch("""
            SELECT id, owner_key, prize_name, metadata::text AS metadata
            FROM user_items
            WHERE used = TRUE
              AND metadata->>'ball_key' IS NULL
              AND (prize_name LIKE '%宝可梦球%' OR metadata->>'pokeball_id' IS NOT NULL)
            ORDER BY id
        """)
        orphan_rows = await conn.fetch("""
            SELECT id, ball_key, name, capacity
            FROM pokeballs
            WHERE ball_key NOT IN (
                SELECT metadata->>'ball_key'
                FROM user_items
                WHERE metadata->>'ball_key' IS NOT NULL
            )
            ORDER BY id
        """)

        broken = []
        for r in broken_rows:
            meta = _json.loads(r["metadata"]) if r["metadata"] else {}
            cap = _parse_pokeball_capacity(r["prize_name"]) or meta.get("capacity")
            broken.append({"item_id": r["id"], "owner_key": r["owner_key"],
                           "prize_name": r["prize_name"], "capacity": cap})

        orphans = {r["ball_key"]: {"capacity": r["capacity"], "name": r["name"]} for r in orphan_rows}
        broken_by_cap: dict = defaultdict(list)
        orphan_by_cap: dict = defaultdict(list)
        for b in broken:
            broken_by_cap[b["capacity"]].append(b)
        for bk, info in orphans.items():
            orphan_by_cap[info["capacity"]].append(bk)

        # 构建修复计划
        repair_plan: list = []       # [(item_id, ball_key, owner_key)]
        used_ball_keys: set = set()  # 已被计划使用的球，避免重复分配

        # 手动强制映射优先
        force_item_ids = {fm["item_id"] for fm in force_mapping if "item_id" in fm and "ball_key" in fm}
        for fm in force_mapping:
            iid = fm.get("item_id")
            bk  = fm.get("ball_key")
            if not iid or not bk:
                continue
            if bk not in orphans and bk not in {bk2 for bk2 in used_ball_keys}:
                # 允许强制映射到任意球（含已链接球，管理员手动覆盖）
                pass
            # 查找 owner_key
            owner = next((b["owner_key"] for b in broken if b["item_id"] == iid), None)
            repair_plan.append((iid, bk, owner or "?"))
            used_ball_keys.add(bk)

        # 自动匹配：唯一容量 1对1
        for cap, b_list in broken_by_cap.items():
            o_list = [bk for bk in orphan_by_cap.get(cap, []) if bk not in used_ball_keys]
            # 过滤掉已被 force_mapping 处理的
            b_list_remaining = [b for b in b_list if b["item_id"] not in force_item_ids]
            if len(b_list_remaining) == 1 and len(o_list) == 1:
                repair_plan.append((b_list_remaining[0]["item_id"], o_list[0], b_list_remaining[0]["owner_key"]))
                used_ball_keys.add(o_list[0])

        # 执行修复
        fixed = []
        skipped = []
        for item_id, ball_key, owner_key in repair_plan:
            # 验证 ball_key 存在
            pb_check = await conn.fetchrow("SELECT id FROM pokeballs WHERE ball_key=$1", ball_key)
            if not pb_check:
                skipped.append({"item_id": item_id, "ball_key": ball_key, "reason": "ball_key 不存在"})
                continue
            if not dry_run:
                await conn.execute("""
                    UPDATE user_items
                    SET metadata = jsonb_set(metadata, '{ball_key}', to_jsonb($1::text))
                    WHERE id = $2 AND metadata->>'ball_key' IS NULL
                """, ball_key, item_id)
            fixed.append({"item_id": item_id, "ball_key": ball_key, "owner_key": owner_key,
                          "pokeball_name": orphans.get(ball_key, {}).get("name", "?")})

    return {
        "dry_run": dry_run,
        "fixed_count": len(fixed),
        "skipped_count": len(skipped),
        "fixed": fixed,
        "skipped": skipped,
    }


@app.post("/admin/user-item-set-ball-key")
async def admin_user_item_set_ball_key(request: Request):
    """强制设置或清除指定 user_items 记录的 ball_key（管理员修复用）
    Body: {"item_id": 123, "ball_key": "jb-pb-xxx" | null}
    ball_key=null 表示清除（从 metadata 中删除 ball_key 字段）
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    item_id = body.get("item_id")
    ball_key = body.get("ball_key")  # None 表示清除
    if not item_id:
        raise HTTPException(status_code=400, detail="缺少 item_id")
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, owner_key, prize_name, metadata FROM user_items WHERE id=$1", item_id
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"item_id={item_id} 不存在")
        old_meta = json.loads(row["metadata"]) if row["metadata"] else {}
        old_ball_key = old_meta.get("ball_key")
        if ball_key is None:
            await conn.execute(
                "UPDATE user_items SET metadata = metadata - 'ball_key' WHERE id=$1", item_id
            )
            action = "cleared"
        else:
            pb = await conn.fetchrow("SELECT id FROM pokeballs WHERE ball_key=$1", ball_key)
            if not pb:
                raise HTTPException(status_code=404, detail=f"ball_key={ball_key} 不存在")
            await conn.execute(
                "UPDATE user_items SET metadata = jsonb_set(metadata, '{ball_key}', to_jsonb($1::text)) WHERE id=$2",
                ball_key, item_id
            )
            action = "set"
        return {
            "item_id": item_id,
            "owner_key": row["owner_key"],
            "prize_name": row["prize_name"],
            "action": action,
            "old_ball_key": old_ball_key,
            "new_ball_key": ball_key,
        }


@app.get("/admin/donated-accounts-export")
async def admin_donated_accounts_export(request: Request):
    """批量导出后备隐藏能源（JB 捐献账号，含状态/DC绑定，供跨环境迁移使用）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT jb_email, jb_password, dc_password, dc_tag, status, submitted_at, reviewed_at "
            "FROM donated_jb_accounts ORDER BY id"
        )
    return [
        {
            "jb_email":    r["jb_email"],
            "jb_password": r["jb_password"],
            "dc_password": r["dc_password"],
            "dc_tag":      r["dc_tag"],
            "status":      r["status"],
            "submitted_at": r["submitted_at"].isoformat() if r["submitted_at"] else None,
            "reviewed_at":  r["reviewed_at"].isoformat() if r["reviewed_at"] else None,
        }
        for r in rows
    ]


# ──────────────────────────────────────────────────────────────
# 排队记录 API（pending NC licenseId）
# ──────────────────────────────────────────────────────────────

@app.get("/admin/pending-nc")
async def admin_pending_nc_list(request: Request):
    """列出所有排队中的 NC licenseId 记录（jb_accounts.pending_nc_lids 非空）。仅完整管理员可访问。"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="无权限")
    db = await _get_db_pool()
    if not db:
        return {"records": []}
    async with db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, pending_nc_lids, pending_nc_email, pending_nc_key,
                   pending_nc_bound_ids, pending_nc_low_admin, last_updated
            FROM jb_accounts
            WHERE pending_nc_lids IS NOT NULL AND pending_nc_lids != '[]'
            ORDER BY last_updated DESC
            """
        )
    result = []
    for row in rows:
        lids = []
        try:
            lids = json.loads(row["pending_nc_lids"] or "[]")
        except Exception:
            pass
        bound_ids = [x for x in (row["pending_nc_bound_ids"] or "").split(",") if x.strip()]
        result.append({
            "id": row["id"],
            "email": row["pending_nc_email"] or "",
            "pending_lids": lids,
            "pending_count": len(lids),
            "bound_ids": bound_ids,
            "bound_count": len(bound_ids),
            "pending_nc_key": row["pending_nc_key"] or "",
            "is_low_admin": bool(row["pending_nc_low_admin"]) if "pending_nc_low_admin" in row.keys() else False,
            "last_updated": row["last_updated"],
        })
    now = time.time()
    last_retry = _pending_nc_last_retry_at
    next_retry = last_retry + _PENDING_NC_INTERVAL if last_retry > 0 else now + 120
    # 主面板：返回主池日志 + LOW 日志（分两个字段，前端可独立展示 / Tab 切换）
    return {
        "records": result,
        "last_retry_at": last_retry,
        "next_retry_at": next_retry,
        "interval": _PENDING_NC_INTERVAL,
        "server_time": now,
        "logs": list(_pending_nc_retry_log)[-60:],
        "logs_low": list(_pending_nc_retry_log_low)[-60:],
    }


@app.get("/admin/pending-nc/low")
async def admin_pending_nc_list_low(request: Request):
    """LOW 用户专属排队记录视图：仅返回 is_low_admin=TRUE 的记录与 LOW 滚动日志。
    完整管理员（ADMIN_KEY）与 LOW_ADMIN_KEY 均可访问，便于 LOW 用户在用户面板自查。"""
    provided = request.headers.get("X-Admin-Key", "")
    if provided != ADMIN_KEY and (not LOW_ADMIN_KEY or provided != LOW_ADMIN_KEY):
        raise HTTPException(status_code=403, detail="无权限")
    db = await _get_db_pool()
    if not db:
        return {"records": []}
    async with db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, pending_nc_lids, pending_nc_email, pending_nc_key,
                   pending_nc_bound_ids, last_updated
            FROM jb_accounts
            WHERE pending_nc_lids IS NOT NULL AND pending_nc_lids != '[]'
              AND pending_nc_low_admin = TRUE
            ORDER BY last_updated DESC
            """
        )
    result = []
    is_full_admin = (provided == ADMIN_KEY)
    for row in rows:
        lids = []
        try:
            lids = json.loads(row["pending_nc_lids"] or "[]")
        except Exception:
            pass
        bound_ids = [x for x in (row["pending_nc_bound_ids"] or "").split(",") if x.strip()]
        # LOW 用户视角：脱敏邮箱（仅显示前 3 字符 + ***）
        email_raw = row["pending_nc_email"] or ""
        if is_full_admin:
            email_show = email_raw
        else:
            if "@" in email_raw:
                local, _, dom = email_raw.partition("@")
                email_show = (local[:3] + "***@" + dom) if len(local) > 3 else (local + "***@" + dom)
            else:
                email_show = "***"
        result.append({
            "id": row["id"],
            "email": email_show,
            "pending_lids": lids,
            "pending_count": len(lids),
            "bound_ids": bound_ids,
            "bound_count": len(bound_ids),
            "pending_nc_key": row["pending_nc_key"] or "",
            "is_low_admin": True,
            "last_updated": row["last_updated"],
        })
    now = time.time()
    last_retry = _pending_nc_last_retry_at
    next_retry = last_retry + _PENDING_NC_INTERVAL if last_retry > 0 else now + 120
    return {
        "records": result,
        "last_retry_at": last_retry,
        "next_retry_at": next_retry,
        "interval": _PENDING_NC_INTERVAL,
        "server_time": now,
        "logs": list(_pending_nc_retry_log_low)[-60:],
    }


@app.delete("/admin/pending-nc")
async def admin_pending_nc_clear_all(request: Request, is_low: Optional[int] = None):
    """一键清空所有排队记录（is_low=1 只清 LOW 用户；is_low=0 只清普通用户；省略清全部）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="无权限")
    db = await _get_db_pool()
    if not db:
        raise HTTPException(status_code=503, detail="DB 不可用")
    async with db.acquire() as conn:
        if is_low is None:
            result = await conn.execute(
                "UPDATE jb_accounts SET pending_nc_lids=NULL, pending_nc_key=NULL, "
                "pending_nc_bound_ids=NULL, pending_nc_enqueued_at=0 "
                "WHERE pending_nc_lids IS NOT NULL AND pending_nc_lids != '[]'"
            )
        else:
            result = await conn.execute(
                "UPDATE jb_accounts SET pending_nc_lids=NULL, pending_nc_key=NULL, "
                "pending_nc_bound_ids=NULL, pending_nc_enqueued_at=0 "
                "WHERE pending_nc_lids IS NOT NULL AND pending_nc_lids != '[]' "
                "AND COALESCE(pending_nc_low_admin, FALSE) = $1",
                bool(is_low)
            )
    cleared = int(result.split()[-1]) if result else 0
    return {"ok": True, "cleared": cleared}


@app.delete("/admin/pending-nc/{row_id}")
async def admin_pending_nc_delete(row_id: str, request: Request):
    """手动清除某条排队记录（重置 pending_nc_lids 为空）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="无权限")
    db = await _get_db_pool()
    if not db:
        raise HTTPException(status_code=503, detail="DB 不可用")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jb_accounts SET pending_nc_lids=NULL, pending_nc_key=NULL, "
            "pending_nc_bound_ids=NULL, pending_nc_enqueued_at=0 WHERE id=$1",
            row_id
        )
    return {"ok": True}


# ──────────────────────────────────────────────────────────────
# LOW_ADMIN 并发配置 / 批量激活 API
# ──────────────────────────────────────────────────────────────

def _check_low_or_admin(request: Request) -> str:
    """校验调用者：返回 'admin' | 'low_admin'，否则抛 403。"""
    provided = request.headers.get("X-Admin-Key", "")
    if ADMIN_KEY and provided == ADMIN_KEY:
        return "admin"
    if LOW_ADMIN_KEY and provided == LOW_ADMIN_KEY:
        return "low_admin"
    raise HTTPException(status_code=403, detail="无权限")


@app.get("/admin/low-config")
async def admin_low_config_get(request: Request):
    """读取 LOW 用户并发配置。
    - 完整管理员：?discord_id= 查询指定账号的并发（省略则返回全局默认及所有 Discord 账号设置）。
    - LOW 用户：须在 Header 带 X-Discord-Token，自动返回自己账号的并发。
    """
    _check_low_or_admin(request)
    role = _request_role(request)
    is_admin = (role == "admin")

    discord_id = ""
    if is_admin:
        discord_id = (request.query_params.get("discord_id", "") or "").strip()
    else:
        token = (
            request.headers.get("X-Discord-Token", "")
            or request.query_params.get("discord_token", "")
        ).strip()
        if token:
            info = _DISCORD_VERIFIED.get(token)
            if info and time.time() - info["ts"] <= 1800:
                discord_id = str(info.get("user_id", "") or "")

    now = time.time()
    # per-Discord cooldown：LOW 用户按 discord_id 查，admin 用 ""
    _cfg_cooldown_key = discord_id if (not is_admin) else ""
    _cfg_last_at = _low_admin_last_batch_at.get(_cfg_cooldown_key, 0.0)
    cd_remaining = max(0, int(_cfg_last_at + _LOW_BATCH_COOLDOWN - now))
    concurrency = _get_low_concurrency(discord_id)
    resp: Dict[str, Any] = {
        "concurrency":         concurrency,
        "concurrency_default": _low_admin_concurrency,
        "concurrency_max":     _LOW_CONCURRENCY_MAX,
        "batch_max":           _LOW_BATCH_MAX,
        "cooldown_seconds":    _LOW_BATCH_COOLDOWN,
        "last_batch_at":       _cfg_last_at,
        "cooldown_remaining":  cd_remaining,
        "server_time":         now,
        "discord_id":          discord_id,
    }
    if is_admin:
        resp["discord_settings"] = dict(_low_discord_concurrency)
    return resp


class LowConfigPatch(BaseModel):
    concurrency: int
    discord_id: str = ""  # admin 指定目标 Discord 账号；LOW 用户由 Header 自动推断


@app.patch("/admin/low-config")
async def admin_low_config_patch(req: LowConfigPatch, request: Request):
    """更新 LOW 用户并发配置。
    - 完整管理员：body.discord_id 非空 → 设置该 Discord 账号的独立并发；空 → 更新全局默认。
    - LOW 用户：须在 Header 带 X-Discord-Token，仅可更新自己账号的并发。
    持久化到 jb_settings 表，下次重启自动加载。
    """
    global _low_admin_concurrency, _low_discord_concurrency
    _check_low_or_admin(request)
    role = _request_role(request)
    is_admin = (role == "admin")

    val = int(req.concurrency)
    if val < 1 or val > _LOW_CONCURRENCY_MAX:
        raise HTTPException(status_code=400, detail=f"并发数须在 1 - {_LOW_CONCURRENCY_MAX} 之间")

    discord_id = ""
    if is_admin:
        discord_id = (req.discord_id or "").strip()
    else:
        token = (
            request.headers.get("X-Discord-Token", "")
            or request.query_params.get("discord_token", "")
        ).strip()
        if token:
            info = _DISCORD_VERIFIED.get(token)
            if info and time.time() - info["ts"] <= 1800:
                discord_id = str(info.get("user_id", "") or "")
        if not discord_id:
            raise HTTPException(status_code=401, detail="LOW 用户须先通过 Discord 验证才能设置并发数")

    if discord_id:
        _low_discord_concurrency[discord_id] = val
        settings_key = f"low_concurrency:{discord_id}"
    else:
        _low_admin_concurrency = val
        settings_key = "low_admin_concurrency"

    # 重建该 Discord 账号（或全部）的线程池以立即生效
    _reset_low_executor(discord_id if discord_id else None)

    db_pool = await _get_db_pool()
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO jb_settings (k, v) VALUES ($1, $2) "
                "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                settings_key, str(val),
            )
    return {"ok": True, "concurrency": val, "discord_id": discord_id}


# LOW 用户专属线程池：per-Discord 独立线程池，并发数互不干扰。
# "" 键作为 admin 发起（无 discord_id）时的兜底池。
# PATCH /admin/low-config 时延迟回收旧池，避免 "cannot schedule new futures after shutdown" 竞态。
_low_executor_dict: Dict[str, Optional[ThreadPoolExecutor]] = {}
_low_executor_lock = threading.Lock()
_low_executor_old: list = []  # 旧池暂存，待在飞任务自然结束后 GC


def _get_low_executor(discord_id: str = "") -> ThreadPoolExecutor:
    """惰性创建/获取 per-Discord LOW 用户激活线程池。"""
    global _low_executor_dict
    with _low_executor_lock:
        ex = _low_executor_dict.get(discord_id)
        if ex is None:
            size = max(1, min(_get_low_concurrency(discord_id), _LOW_CONCURRENCY_MAX))
            ex = ThreadPoolExecutor(max_workers=size)
            _low_executor_dict[discord_id] = ex
        return ex


def _reset_low_executor(discord_id: Optional[str] = None) -> None:
    """重建指定 Discord 账号的线程池（discord_id=None → 重建所有）。
    旧池暂存于 _low_executor_old，在飞任务跑完后由 GC 回收。
    """
    global _low_executor_dict
    with _low_executor_lock:
        targets = (
            [discord_id] if discord_id is not None and discord_id in _low_executor_dict
            else list(_low_executor_dict.keys())
        )
        for dc_id in targets:
            ex = _low_executor_dict.pop(dc_id, None)
            if ex is not None:
                try:
                    ex.shutdown(wait=False)
                except Exception:
                    pass
                _low_executor_old.append(ex)


@app.get("/admin/low-user-key")
async def low_user_key_get(request: Request):
    """查询 LOW 用户的个人专属密钥。
    - LOW_ADMIN_KEY + X-Discord-Token：返回自己的 key 信息
    - ADMIN_KEY：返回全部 LOW 用户个人 key 列表"""
    role = _check_low_or_admin(request)
    if role == "admin":
        keys = [
            {
                "key": k,
                "usage_limit": v.get("usage_limit", 0) or 0,
                "usage_count": v.get("usage_count", 0) or 0,
                "account_id": v.get("account_id", "") or "",
                "low_admin_discord_id": v.get("low_admin_discord_id", ""),
            }
            for k, v in VALID_CLIENT_KEYS.items()
            if v.get("low_admin_discord_id")
        ]
        return {"keys": keys}
    # LOW 用户：须 Discord 验证
    token = request.headers.get("X-Discord-Token", "").strip()
    dc_info = _DISCORD_VERIFIED.get(token) if token else None
    if not dc_info or time.time() - dc_info.get("ts", 0) > 1800:
        return {"key": None, "reason": "未登录 Discord 或验证已过期"}
    discord_id = str(dc_info.get("user_id", ""))
    existing = _get_low_personal_key(discord_id)
    if not existing:
        return {"key": None}
    meta = VALID_CLIENT_KEYS[existing]
    return {
        "key": existing,
        "usage_limit": meta.get("usage_limit", 0) or 0,
        "usage_count": meta.get("usage_count", 0) or 0,
    }


@app.post("/admin/low-user-key")
async def low_user_key_create(request: Request):
    """创建 LOW 用户个人专属密钥（每人唯一，已存在则直接返回现有 key）。
    需要 LOW_ADMIN_KEY + X-Discord-Token 请求头。"""
    role = _check_low_or_admin(request)
    if role == "admin":
        raise HTTPException(status_code=403, detail="管理员无需创建个人密钥，请使用 /admin/keys 端点")
    token = request.headers.get("X-Discord-Token", "").strip()
    dc_info = _DISCORD_VERIFIED.get(token) if token else None
    if not dc_info or time.time() - dc_info.get("ts", 0) > 1800:
        raise HTTPException(status_code=401, detail="请先完成 Discord 验证后再创建个人密钥")
    discord_id = str(dc_info.get("user_id", ""))
    # 已有个人 key → 直接返回
    existing = _get_low_personal_key(discord_id)
    if existing:
        meta = VALID_CLIENT_KEYS[existing]
        return {
            "key": existing,
            "created": False,
            "usage_limit": meta.get("usage_limit", 0) or 0,
            "usage_count": meta.get("usage_count", 0) or 0,
        }
    # 新建
    new_key = f"sk-jb-{secrets.token_hex(24)}"
    meta: Dict[str, Any] = {
        "usage_limit": 0,
        "usage_count": 0,
        "account_id": "",
        "is_nc_key": True,
        "is_low_admin_key": True,
        "low_admin_discord_id": discord_id,
    }
    VALID_CLIENT_KEYS[new_key] = meta
    _pool = await _get_db_pool()
    if _pool:
        await _upsert_key_to_db(new_key, meta)
    # 创建即写入审计行（建立基线，便于后续防回滚检测）
    await _low_audit_save(discord_id, usage_limit=0, usage_count=0, banned=False)
    _admin_cache_invalidate("keys", "status")
    return {"key": new_key, "created": True, "usage_limit": 0, "usage_count": 0}


@app.delete("/admin/low-user-key")
async def low_user_key_delete(request: Request):
    """删除 LOW 用户自己的个人专属密钥（慎用：额度清零）。
    删除前会把当前 usage_count / usage_limit / banned 持久化到 audit 行，
    防止"删除→导入老备份"的额度回滚（关键防御层）。"""
    role = _check_low_or_admin(request)
    if role == "admin":
        raise HTTPException(status_code=403, detail="请使用 /admin/keys/{key} 端点删除指定密钥")
    token = request.headers.get("X-Discord-Token", "").strip()
    dc_info = _DISCORD_VERIFIED.get(token) if token else None
    if not dc_info or time.time() - dc_info.get("ts", 0) > 1800:
        raise HTTPException(status_code=401, detail="请先完成 Discord 验证")
    discord_id = str(dc_info.get("user_id", ""))
    # 与 import / 持久化封禁互斥
    async with _get_low_user_key_lock(discord_id):
        existing = _get_low_personal_key(discord_id)
        if not existing:
            return {"deleted": False, "reason": "未找到个人密钥"}
        # 删除前快照写入 audit（防回滚关键步骤）
        meta_snapshot = dict(VALID_CLIENT_KEYS.get(existing, {}))
        await _low_audit_save(
            discord_id,
            usage_limit=int(meta_snapshot.get("usage_limit") or 0),
            usage_count=int(meta_snapshot.get("usage_count") or 0),
            banned=True if bool(meta_snapshot.get("banned")) else None,
            banned_at=meta_snapshot.get("banned_at"),
        )
        VALID_CLIENT_KEYS.pop(existing, None)
        await _delete_key_from_db(existing)
        _admin_cache_invalidate("keys", "status")
    return {"deleted": True, "key": existing}


# ──────────────────────────────────────────────────────────────────────────
# LOW 用户个人 key 导出 / 导入
# 设计原则：
#   1. 仅本人可导出/导入（依赖 X-Discord-Token 验证）
#   2. 导出文件含服务器 HMAC-SHA256 签名；导入时必须验签通过
#      → 防止用户篡改 owner_discord_id / usage_limit / usage_count 等字段
#      → 实际上这意味着只能在 SESSION_SECRET 相同的部署间互相迁移
#   3. 导入时若用户已有个人 key，必须先 DELETE 再导入（避免同一 Discord 持多 key）
#   4. 导入时若 key 字符串与他人 key 冲突，拒绝
#   5. 导入操作通过 per-Discord asyncio.Lock 串行化，防止并发导入产生多 key
# ──────────────────────────────────────────────────────────────────────────

_LOW_KEY_EXPORT_VERSION = 1
_LOW_KEY_EXPORT_SIG_FIELDS = (
    "version", "exported_at", "owner_discord_id", "key",
    "usage_limit", "usage_count", "account_id",
    "is_nc_key", "is_low_admin_key", "banned", "banned_at",
)
_low_user_key_locks: Dict[str, asyncio.Lock] = {}


def _get_low_user_key_lock(discord_id: str) -> asyncio.Lock:
    """返回 per-Discord-ID 的 asyncio.Lock；按需创建。
    asyncio 事件循环单线程模型保证 dict 的 if-not-in/赋值序列在协程间是原子的（不含 await）。"""
    lock = _low_user_key_locks.get(discord_id)
    if lock is None:
        lock = asyncio.Lock()
        _low_user_key_locks[discord_id] = lock
    return lock


def _low_key_signing_secret() -> bytes:
    """计算签名用密钥。仅使用 SESSION_SECRET（专用），不接受其他回退以减小密钥耦合。"""
    secret = (os.environ.get("SESSION_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("无可用的导出签名密钥（SESSION_SECRET 未配置）")
    return secret.encode("utf-8")


def _low_key_export_sign(payload: dict) -> str:
    """对导出 payload 计算 HMAC-SHA256 签名（取规范化的字段子集）。"""
    canonical = {k: payload.get(k) for k in _LOW_KEY_EXPORT_SIG_FIELDS}
    msg = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return _hmac_mod.new(_low_key_signing_secret(), msg, hashlib.sha256).hexdigest()


# ──────────────────────────────────────────────────────────────────────────
# 持久化审计：每个 Discord ID 的"已知最大用量 + 封禁状态"
# 即使用户 DELETE 了 key，audit 行也保留，用于：
#   - 防止导入更旧的备份（usage_count 回滚 → 额度盗取）
#   - 防止已封禁用户通过导入封禁前快照实现自助解封
# 写入：create / _add_low_quota / 任何对 LOW key 的 ban
# 存储：jb_settings 表（key 形如 "low_audit:{discord_id}"，value 为 JSON 字符串）
# ──────────────────────────────────────────────────────────────────────────

def _low_audit_key(discord_id: str) -> str:
    return f"low_audit:{discord_id}"


# 独立的 audit 写入锁（与 import 锁分离，避免持有 import 锁时调用 _low_audit_save 死锁）
_low_audit_save_locks: Dict[str, asyncio.Lock] = {}


def _get_low_audit_save_lock(discord_id: str) -> asyncio.Lock:
    lock = _low_audit_save_locks.get(discord_id)
    if lock is None:
        lock = asyncio.Lock()
        _low_audit_save_locks[discord_id] = lock
    return lock


async def _low_audit_load(discord_id: str) -> Dict[str, Any]:
    """读取该 Discord ID 的审计快照；不存在则返回空 dict。"""
    if not discord_id:
        return {}
    pool = await _get_db_pool()
    if not pool:
        return {}
    try:
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT v FROM jb_settings WHERE k=$1", _low_audit_key(discord_id))
        if not row:
            return {}
        try:
            return json.loads(row["v"]) or {}
        except Exception:
            return {}
    except Exception as e:
        print(f"[low_audit] 读取失败 discord_id={discord_id}: {e}")
        return {}


async def _low_audit_save(discord_id: str, *, usage_limit: Optional[int] = None,
                          usage_count: Optional[int] = None,
                          banned: Optional[bool] = None,
                          banned_at: Optional[float] = None) -> None:
    """以"单调最大值 + 一次封禁不可撤销"的语义合并写入审计行。
    传入 None 的字段保持原值不变。**整个 load-merge-store 在 per-discord 锁内串行化**，
    保证对同一 Discord ID 的并发审计写入不会互相覆盖。"""
    if not discord_id:
        return
    pool = await _get_db_pool()
    if not pool:
        return
    async with _get_low_audit_save_lock(discord_id):
        try:
            prev = await _low_audit_load(discord_id)
            new_doc = dict(prev) if prev else {}
            # 单调递增：只升不降
            if usage_limit is not None:
                new_doc["last_usage_limit"] = max(int(prev.get("last_usage_limit", 0) or 0), int(usage_limit or 0))
            if usage_count is not None:
                new_doc["last_usage_count"] = max(int(prev.get("last_usage_count", 0) or 0), int(usage_count or 0))
            # 封禁是 sticky：一旦 True 就永远 True
            if banned is True:
                new_doc["banned"] = True
                if banned_at is not None and not new_doc.get("banned_at"):
                    new_doc["banned_at"] = float(banned_at)
            elif banned is False and not prev.get("banned"):
                # 仅在从未被封禁过时才允许显式 False（保持字段存在）
                new_doc["banned"] = False
            new_doc["updated_at"] = int(time.time())
            async with pool.acquire() as c:
                await c.execute(
                    "INSERT INTO jb_settings (k, v) VALUES ($1, $2) "
                    "ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                    _low_audit_key(discord_id), json.dumps(new_doc, ensure_ascii=False),
                )
        except Exception as e:
            print(f"[low_audit] 写入失败 discord_id={discord_id}: {e}")


async def _persist_low_user_ban(discord_id: str, ban_ts: float) -> None:
    """对 LOW 用户个人 key 的封禁，统一持久化 audit 行；与 import 互斥串行化。
    在同一把 import 锁内既写 audit 又强制把当前内存中的个人 key 标 banned=True，
    防止"封禁中"与"导入中"交错产生 audit=banned 但内存=未封禁的运行时漂移。
    后台调用方应 await 此函数（或对其 create_task 后再 await gather），以保证封禁可持久。"""
    if not discord_id:
        return
    async with _get_low_user_key_lock(discord_id):
        # 1) 写 audit（防回滚）
        await _low_audit_save(discord_id, banned=True, banned_at=ban_ts)
        # 2) 同步把当前内存中的 LOW key 强制标 banned（防止 import 抢锁后写入 banned=False）
        cur_key = _get_low_personal_key(discord_id)
        if cur_key and cur_key in VALID_CLIENT_KEYS:
            meta = VALID_CLIENT_KEYS[cur_key]
            if not meta.get("banned"):
                meta["banned"] = True
                meta["banned_at"] = meta.get("banned_at") or ban_ts
                try:
                    await _upsert_key_to_db(cur_key, meta)
                except Exception as e:
                    print(f"[low_ban] DB 持久化失败 key={cur_key[:20]}…: {e}")


def _verify_low_user_discord(request: Request) -> str:
    """统一的 LOW 用户 Discord 验证入口；返回验证通过的 discord_id。"""
    role = _check_low_or_admin(request)
    if role == "admin":
        raise HTTPException(status_code=403, detail="此端点仅供 LOW 用户使用")
    token = request.headers.get("X-Discord-Token", "").strip()
    dc_info = _DISCORD_VERIFIED.get(token) if token else None
    if not dc_info or time.time() - dc_info.get("ts", 0) > 1800:
        raise HTTPException(status_code=401, detail="请先完成 Discord 验证")
    discord_id = str(dc_info.get("user_id", "")).strip()
    if not discord_id:
        raise HTTPException(status_code=401, detail="Discord 验证信息缺失 user_id")
    return discord_id


@app.get("/admin/low-user-key/export")
async def low_user_key_export(request: Request):
    """导出 LOW 用户自己的个人专属密钥（含额度、累积的账号 ID 列表等）。
    返回带 HMAC 签名的 JSON，可保存为本地文件后通过 /admin/low-user-key/import 恢复。"""
    discord_id = _verify_low_user_discord(request)
    existing = _get_low_personal_key(discord_id)
    if not existing:
        raise HTTPException(status_code=404, detail="未找到个人密钥，请先创建后再导出")
    meta = VALID_CLIENT_KEYS.get(existing) or {}
    payload = {
        "version": _LOW_KEY_EXPORT_VERSION,
        "exported_at": int(time.time()),
        "owner_discord_id": discord_id,
        "key": existing,
        "usage_limit": int(meta.get("usage_limit") or 0),
        "usage_count": int(meta.get("usage_count") or 0),
        "account_id": str(meta.get("account_id") or ""),
        "is_nc_key": bool(meta.get("is_nc_key", True)),
        "is_low_admin_key": bool(meta.get("is_low_admin_key", True)),
        "banned": bool(meta.get("banned", False)),
        "banned_at": meta.get("banned_at"),
    }
    payload["signature"] = _low_key_export_sign(payload)
    return payload


class LowUserKeyImportPayload(BaseModel):
    model_config = ConfigDict(extra='ignore')
    version: int = _LOW_KEY_EXPORT_VERSION
    exported_at: Optional[int] = None
    owner_discord_id: str
    key: str
    usage_limit: int = 0
    usage_count: int = 0
    account_id: str = ""
    is_nc_key: bool = True
    is_low_admin_key: bool = True
    banned: bool = False
    banned_at: Optional[float] = None
    signature: str = ""


@app.post("/admin/low-user-key/import")
async def low_user_key_import(request: Request, payload: LowUserKeyImportPayload):
    """导入之前导出的个人 key JSON。安全约束：
      - HMAC 签名校验：拒绝任何被篡改或非本服务器签发的 payload
      - JSON 中 owner_discord_id 必须等于当前 Discord 验证身份（双重保险）
      - 当前用户尚未持有个人 key（已有则必须先 DELETE）
      - key 字符串不与他人现有 key 冲突
      - 整个验证 + 写入流程在 per-Discord asyncio.Lock 内串行化
    """
    discord_id = _verify_low_user_discord(request)

    # 版本校验（先做，签名包含 version，过新版本可能字段不同）
    if int(payload.version or 0) > _LOW_KEY_EXPORT_VERSION:
        raise HTTPException(status_code=400, detail=f"导出文件版本过新（{payload.version}），当前服务端仅支持 ≤ {_LOW_KEY_EXPORT_VERSION}")

    # 签名校验：使用服务器密钥重算签名并常量时间比对
    incoming_sig = (payload.signature or "").strip()
    if not incoming_sig:
        raise HTTPException(status_code=400, detail="导出文件缺少 signature 字段，无法验证完整性")
    try:
        expected_sig = _low_key_export_sign(payload.model_dump())
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not _hmac_mod.compare_digest(expected_sig, incoming_sig):
        raise HTTPException(status_code=400, detail="导出文件签名校验失败：内容已被篡改、来自不同部署、或损坏")

    # owner 一致性校验（双重保险；签名通过的话理论上一致）
    incoming_owner = str(payload.owner_discord_id or "").strip()
    if not incoming_owner or incoming_owner != discord_id:
        raise HTTPException(status_code=403, detail="导出文件归属的 Discord 账号与当前登录账号不一致，禁止导入")

    # key 字符串基本校验（避免注入异常字符）
    incoming_key = str(payload.key or "").strip()
    if not incoming_key or not re.fullmatch(r"[A-Za-z0-9_\-\.]{8,128}", incoming_key):
        raise HTTPException(status_code=400, detail="导入的 key 字符串格式不合法")

    # 串行化：同 Discord ID 的 import 一次只能进行一次
    async with _get_low_user_key_lock(discord_id):
        # 当前用户已有个人 key → 拒绝（避免同 Discord 持多 key、合并额度）
        existing_self = _get_low_personal_key(discord_id)
        if existing_self:
            raise HTTPException(status_code=409, detail=f"您当前已有个人密钥（{existing_self[:12]}...），请先删除再导入")

        # key 字符串若已存在但属于他人 → 拒绝
        other_meta = VALID_CLIENT_KEYS.get(incoming_key)
        if other_meta is not None:
            other_owner = str(other_meta.get("low_admin_discord_id") or "")
            if other_owner and other_owner != discord_id:
                raise HTTPException(status_code=409, detail="该 key 已被其他 Discord 账号占用，无法导入")
            # 如果该 key 已在系统中且属于自己，正常情况下 _get_low_personal_key 应返回非空，
            # 走到这里说明系统状态不一致；为安全起见拒绝并提示
            raise HTTPException(status_code=409, detail="该 key 已存在于系统中，建议先 DELETE 再导入")

        # ── 审计行检查：防止"老备份回滚 → 偷额度 / 自助解封" ──
        audit = await _low_audit_load(discord_id)
        srv_used = int(audit.get("last_usage_count", 0) or 0) if audit else 0
        srv_banned = bool(audit.get("banned", False)) if audit else False
        incoming_used = max(0, int(payload.usage_count or 0))
        if srv_used > incoming_used:
            raise HTTPException(
                status_code=409,
                detail=f"该备份的用量计数（{incoming_used}）低于服务器记录的最近值（{srv_used}），拒绝导入以防止额度回滚。请使用更新的备份。",
            )
        if srv_banned and not bool(payload.banned):
            raise HTTPException(
                status_code=409,
                detail="您的账号曾被封禁，无法通过导入封禁前的备份解除封禁状态。",
            )
        # 强制把封禁状态合并为 sticky：服务器封禁过 → 一定保留封禁
        effective_banned = bool(payload.banned) or srv_banned
        effective_banned_at = payload.banned_at if bool(payload.banned) else (
            float(audit.get("banned_at")) if (srv_banned and audit.get("banned_at")) else None
        )

        # 写入内存 + DB
        new_meta: Dict[str, Any] = {
            "usage_limit": max(0, int(payload.usage_limit or 0)),
            "usage_count": incoming_used,
            "account_id": str(payload.account_id or ""),
            "is_nc_key": bool(payload.is_nc_key),
            "is_low_admin_key": True,  # 强制：导入到 LOW 池只能是 LOW key
            "banned": effective_banned,
            "banned_at": effective_banned_at,
            "low_admin_discord_id": discord_id,
        }
        VALID_CLIENT_KEYS[incoming_key] = new_meta
        _pool = await _get_db_pool()
        if _pool:
            await _upsert_key_to_db(incoming_key, new_meta)
        # 同步审计行（usage_limit/count 取最大值，banned 一旦 True 永久保留）
        await _low_audit_save(
            discord_id,
            usage_limit=new_meta["usage_limit"],
            usage_count=new_meta["usage_count"],
            banned=effective_banned if effective_banned else None,
            banned_at=effective_banned_at,
        )
        _admin_cache_invalidate("keys", "status")
    return {
        "imported": True,
        "key": incoming_key,
        "usage_limit": new_meta["usage_limit"],
        "usage_count": new_meta["usage_count"],
        "account_count": len([x for x in new_meta["account_id"].split(",") if x]),
    }


class BatchActivateAccount(BaseModel):
    email: str
    password: str


class BatchActivateRequest(BaseModel):
    accounts: List[BatchActivateAccount]
    discord_token: str = ""   # LOW_ADMIN 用户必填：用于 Discord 验证 + 划分 LOW CF 子池


@app.post("/admin/activate-batch")
async def admin_activate_batch(req: BatchActivateRequest, request: Request):
    """LOW_ADMIN（或完整管理员）批量激活账号：
       - 单批最多 _LOW_BATCH_MAX 个；
       - 与上次批量启动须间隔 _LOW_BATCH_COOLDOWN 秒；
       - 内部按 _low_admin_concurrency 控制并行度；
       - LOW_ADMIN 用户走 LOW CF 池，按 Discord 账号划分子池；必须先 Discord 登录；
       - 复用 _activate_tasks，前端可对每个 task_id 用 /admin/activate/{id}/stream 流式查看；
       - 与单条激活逻辑完全一致：预签 key → process_account → 凭证到位升级配额 → 入池。
    """
    role = _check_low_or_admin(request)
    is_low_admin = (role == "low_admin")

    # ── LOW 用户必须 Discord 验证 ──（admin 直通）
    dc_user_id = ""
    if is_low_admin:
        token = (req.discord_token or "").strip()
        if not token:
            raise HTTPException(status_code=401, detail="请先通过 Discord 验证后再批量激活账号")
        dc_info = _DISCORD_VERIFIED.get(token)
        if not dc_info:
            raise HTTPException(status_code=401, detail="Discord 验证无效或已过期，请重新授权")
        if time.time() - dc_info["ts"] > 1800:
            _DISCORD_VERIFIED.pop(token, None)
            raise HTTPException(status_code=401, detail="Discord 验证已过期（30 分钟），请重新授权")
        dc_user_id = str(dc_info.get("user_id", "") or "")

    accounts = [a for a in (req.accounts or []) if a.email.strip() and a.password]
    if not accounts:
        raise HTTPException(status_code=400, detail="账号列表为空")
    if len(accounts) > _LOW_BATCH_MAX:
        raise HTTPException(status_code=400, detail=f"单次最多 {_LOW_BATCH_MAX} 个账号（实际 {len(accounts)}）")

    # per-Discord 冷却：admin 走 "" key，LOW 用户走自己的 dc_user_id
    _cooldown_key = dc_user_id if is_low_admin else ""
    now = time.time()
    last_at = _low_admin_last_batch_at.get(_cooldown_key, 0.0)
    cd_remaining = int(last_at + _LOW_BATCH_COOLDOWN - now)
    if cd_remaining > 0:
        raise HTTPException(
            status_code=429,
            detail=f"距离上次批量激活仅 {int(now - last_at)} 秒，请 {cd_remaining} 秒后再试",
        )
    _low_admin_last_batch_at[_cooldown_key] = now

    # 决定执行器：LOW 用户走 per-Discord 独立线程池；完整管理员用主激活池
    if is_low_admin:
        executor = _get_low_executor(dc_user_id)
        pool_size = executor._max_workers  # type: ignore[attr-defined]
    else:
        executor = _activate_executor
        pool_size = executor._max_workers  # type: ignore[attr-defined]

    from jb_activate import process_account as _process_account

    # LOW 用户优先使用已创建的个人专属密钥（整批共用同一把 key，每个账号成功后累加配额）
    batch_personal_key = _get_low_personal_key(dc_user_id) if (is_low_admin and dc_user_id) else ""

    started: list = []
    for acct in accounts:
        task_id = str(uuid.uuid4())
        log_queue: queue.Queue = queue.Queue()

        if batch_personal_key:
            # 有个人 key → 激活成功后 stream handler 累加配额，无需 preissued
            preissued_key = ""
        else:
            # 无个人 key：为每个账号预签一把 0 额度 key（与单条激活逻辑一致）
            preissued_key = f"sk-jb-{secrets.token_hex(24)}"
            preissued_meta: Dict[str, Any] = {
                "usage_limit": 0, "usage_count": 0, "account_id": "",
                "is_nc_key": True, "is_low_admin_key": is_low_admin,
                # 把创建者 Discord ID 写入 LOW 预签 key，确保 /admin/keys 按 Discord 分组归属正确
                "low_admin_discord_id": str(dc_user_id or "") if is_low_admin else "",
            }
            VALID_CLIENT_KEYS[preissued_key] = preissued_meta
            _admin_cache_invalidate("keys", "status")
            _pre_db = await _get_db_pool()
            if _pre_db:
                await _upsert_key_to_db(preissued_key, preissued_meta)

        _activate_tasks[task_id] = {
            "status": "running",
            "email": acct.email,
            "password": acct.password,
            "logs": [],
            "result": None,
            "log_queue": log_queue,
            "preissued_key": preissued_key,       # 无个人 key 时有值
            "personal_key": batch_personal_key,   # LOW 用户个人 key（整批共用）
            "is_low_admin": is_low_admin,
            "discord_user_id": dc_user_id,    # ★ stream handler 据此写 pending_nc_discord_id
        }

        def _make_runner(_task_id: str, _email: str, _password: str, _q: queue.Queue):
            def _log_cb(msg: str):
                _activate_tasks[_task_id]["logs"].append(msg)
                _q.put(msg)
            def _run():
                try:
                    result = _process_account(
                        _email, _password, log_cb=_log_cb,
                        use_low_pool=is_low_admin,
                        low_discord_id=(dc_user_id if is_low_admin else ""),
                    )
                    _activate_tasks[_task_id]["result"] = result
                    if result.get("new_password"):
                        _activate_tasks[_task_id]["password"] = result["new_password"]
                    if result.get("jwt") or result.get("pending_nc_lids"):
                        _activate_tasks[_task_id]["status"] = "success"
                    else:
                        _activate_tasks[_task_id]["status"] = "failed"
                except Exception as e:
                    _activate_tasks[_task_id]["result"] = {"error": str(e)}
                    _activate_tasks[_task_id]["status"] = "failed"
                    _log_cb(f"[EXCEPTION] {e}")
                finally:
                    _q.put(None)
            return _run

        executor.submit(_make_runner(task_id, acct.email, acct.password, log_queue))
        started.append({
            "email": acct.email,
            "task_id": task_id,
            "preissued_key": preissued_key,
            "personal_key": batch_personal_key,
        })

    return {
        "started": started,
        "count": len(started),
        "concurrency": pool_size,
        "next_allowed_at": now + _LOW_BATCH_COOLDOWN,
        "personal_key": batch_personal_key,   # 前端展示用
    }


# ──────────────────────────────────────────────────────────────
# CF 代理池管理 API
# ──────────────────────────────────────────────────────────────

@app.get("/admin/cf-proxies")
async def admin_cf_proxies_list(request: Request):
    """列出主池 CF 代理 URL（仅 owner='admin'）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, url, label, is_active, created_at FROM cf_proxy_pool "
            "WHERE owner='admin' ORDER BY id"
        )
    # 读取当前内存中的代理池
    import importlib
    jb_mod = importlib.import_module("jb_activate")
    active_pool = list(jb_mod.CF_PROXY_POOL)
    return {
        "proxies": [
            {
                "id": r["id"],
                "url": r["url"],
                "label": r["label"] or "",
                "is_active": r["is_active"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
        "loaded_count": len(active_pool),
        "loaded_urls": active_pool,
    }


@app.post("/admin/cf-proxies")
async def admin_cf_proxies_add(request: Request):
    """添加新 CF 代理 URL 到主池"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    url = (body.get("url") or "").strip().rstrip("/")
    label = (body.get("label") or "").strip()
    if not url.startswith("https://"):
        raise HTTPException(status_code=400, detail="URL 必须以 https:// 开头")
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        try:
            # owner_discord_id='' 既是 admin 主池的固定取值，也是新唯一键
            # (url, owner, owner_discord_id) 所必需的 conflict target
            row = await conn.fetchrow(
                "INSERT INTO cf_proxy_pool (url, label, owner, owner_discord_id) "
                "VALUES ($1, $2, 'admin', '') "
                "ON CONFLICT (url, owner, owner_discord_id) "
                "DO UPDATE SET is_active=TRUE, label=EXCLUDED.label "
                "RETURNING id, url, label, is_active",
                url, label,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    # 刷新内存代理池
    await load_cf_proxies_from_db()
    return {"ok": True, "proxy": {"id": row["id"], "url": row["url"], "label": row["label"]}}


@app.delete("/admin/cf-proxies/{proxy_id}")
async def admin_cf_proxies_delete(proxy_id: int, request: Request):
    """删除指定主池 CF 代理"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM cf_proxy_pool WHERE id=$1 AND owner='admin'", proxy_id)
    await load_cf_proxies_from_db()
    return {"ok": True}


@app.patch("/admin/cf-proxies/{proxy_id}")
async def admin_cf_proxies_toggle(proxy_id: int, request: Request):
    """切换主池代理启用/禁用状态"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    is_active = bool(body.get("is_active", True))
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE cf_proxy_pool SET is_active=$1 WHERE id=$2 AND owner='admin'",
            is_active, proxy_id,
        )
    await load_cf_proxies_from_db()
    return {"ok": True, "is_active": is_active}


@app.post("/admin/cf-proxies/test")
async def admin_cf_proxies_test(request: Request):
    """测试 CF 代理 URL 是否可达（GET /health）。
    主池/低池都可调用此接口（只是发个网络请求测试连通性，不写库）。
    """
    if _request_role(request) == "none":
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    url = (body.get("url") or "").strip().rstrip("/")
    if not url.startswith("https://"):
        raise HTTPException(status_code=400, detail="URL 必须以 https:// 开头")
    try:
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{url}/health")
            return {"ok": r.status_code == 200, "status": r.status_code, "body": r.text[:200]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/admin/cf-proxies/export")
async def admin_cf_proxies_export(request: Request):
    """导出主池（owner='admin'）全部 CF 代理 URL（含 URL、备注、启用状态）。
    仅完整管理员可用（X-Admin-Key = ADMIN_KEY）。
    供跨实例迁移或备份使用，可与 POST /admin/cf-proxies/import 配合。
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "仅完整管理员可导出代理池"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT url, label, is_active, owner_discord_id, created_at "
            "FROM cf_proxy_pool WHERE owner='admin' ORDER BY id"
        )
    return {
        "proxies": [
            {
                "url":              r["url"],
                "label":            r["label"] or "",
                "owner":            "admin",
                "owner_discord_id": "",
                "is_active":        bool(r["is_active"]),
                "created_at":       r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
        "count": len(rows),
        "exported_at": int(time.time()),
    }


@app.post("/admin/cf-proxies/import")
async def admin_cf_proxies_import(request: Request):
    """批量导入主池 CF 代理 URL（幂等 upsert，冲突时更新备注和启用状态）。
    仅完整管理员可用。
    请求体：{ "proxies": [{ "url": "...", "label": "...", "is_active": true }, ...] }
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "仅完整管理员可导入代理池"})
    body = await request.json()
    proxies_in = body.get("proxies", [])
    if not isinstance(proxies_in, list):
        raise HTTPException(status_code=400, detail="proxies 必须是数组")
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    imported = 0
    skipped = 0
    async with pool.acquire() as conn:
        for item in proxies_in:
            url = (item.get("url") or "").strip().rstrip("/")
            if not url.startswith("https://"):
                skipped += 1
                continue
            label = (item.get("label") or "").strip()
            is_active = bool(item.get("is_active", True))
            try:
                await conn.execute(
                    "INSERT INTO cf_proxy_pool (url, label, owner, owner_discord_id, is_active) "
                    "VALUES ($1, $2, 'admin', '', $3) "
                    "ON CONFLICT (url, owner, owner_discord_id) DO UPDATE SET "
                    "label=EXCLUDED.label, is_active=EXCLUDED.is_active",
                    url, label, is_active,
                )
                imported += 1
            except Exception as e:
                print(f"[cf-proxies/import] 跳过 {url}: {e}")
                skipped += 1
    await load_cf_proxies_from_db()
    return {"ok": True, "imported": imported, "skipped": skipped}


# ──────────────────────────────────────────────────────────────
# LOW_ADMIN 专属 CF 代理池：与主池物理隔离（owner='low_admin'）
# 鉴权：接受 LOW_ADMIN_KEY 或 ADMIN_KEY（管理员可代为查看/管理）
# ──────────────────────────────────────────────────────────────

def _can_manage_low_pool(request: Request) -> bool:
    role = _request_role(request)
    return role in ("admin", "low_admin")


def _resolve_low_pool_scope(request: Request, body_discord_id: str = "") -> tuple:
    """统一解析 LOW CF 子池操作的访问范围：
       返回 (is_full_admin, scope_discord_id, dc_tag)
       - admin 可指定任何 discord_id（query 或 body 提供，未提供 → ''，即兜底子池）
       - low_admin 必须随请求带 X-Discord-Token，scope_discord_id 强制为其 Discord user_id
       - 其他身份 → 抛 403
    """
    role = _request_role(request)
    if role == "admin":
        qid = (
            request.query_params.get("discord_id", "")
            or body_discord_id
            or ""
        ).strip()
        return (True, qid, "")
    if role == "low_admin":
        token = (
            request.headers.get("X-Discord-Token", "")
            or request.query_params.get("discord_token", "")
        ).strip()
        if not token:
            raise HTTPException(status_code=401, detail="请先通过 Discord 验证后再管理 LOW CF 子池")
        info = _DISCORD_VERIFIED.get(token)
        if not info:
            raise HTTPException(status_code=401, detail="Discord 验证无效或已过期，请重新授权")
        if time.time() - info["ts"] > 1800:
            _DISCORD_VERIFIED.pop(token, None)
            raise HTTPException(status_code=401, detail="Discord 验证已过期（30 分钟），请重新授权")
        return (False, str(info.get("user_id", "") or ""), str(info.get("tag", "") or ""))
    raise HTTPException(status_code=403, detail="无权限")


@app.get("/admin/low-cf-proxies")
async def low_cf_proxies_list(request: Request):
    """列出 LOW 专属池 CF 代理 URL（按 Discord 子池分桶）。
    - 完整管理员：可通过 ?discord_id=xxx 过滤；不传则返回全部子池
    - LOW 用户：必须带 X-Discord-Token，仅返回自己 Discord ID 的子池
    """
    is_admin, scope_dc, dc_tag = _resolve_low_pool_scope(request)
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    if is_admin and not scope_dc:
        # admin 不指定 discord_id：返回所有 LOW 行
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, url, label, is_active, owner_discord_id, created_at "
                "FROM cf_proxy_pool WHERE owner='low_admin' ORDER BY owner_discord_id, id"
            )
    else:
        # admin 指定了 discord_id 或 LOW 用户：仅该子池
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, url, label, is_active, owner_discord_id, created_at "
                "FROM cf_proxy_pool WHERE owner='low_admin' AND owner_discord_id=$1 ORDER BY id",
                scope_dc,
            )
    import importlib
    jb_mod = importlib.import_module("jb_activate")
    low_buckets = jb_mod.LOW_CF_PROXY_POOL or {}
    if is_admin and not scope_dc:
        # 全部子池视图：返回每个桶的统计
        loaded_urls: List[str] = []
        for v in low_buckets.values():
            loaded_urls.extend(v)
        loaded_count = len(loaded_urls)
    else:
        sub = low_buckets.get(scope_dc, []) or []
        loaded_urls = list(sub)
        loaded_count = len(loaded_urls)
    return {
        "proxies": [
            {
                "id": r["id"],
                "url": r["url"],
                "label": r["label"] or "",
                "is_active": r["is_active"],
                "discord_id": r["owner_discord_id"] or "",
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
        "loaded_count": loaded_count,
        "loaded_urls": loaded_urls,
        "scope": "admin_all" if (is_admin and not scope_dc) else "single",
        "scope_discord_id": scope_dc,
        "scope_discord_tag": dc_tag,
    }


@app.post("/admin/low-cf-proxies")
async def low_cf_proxies_add(request: Request):
    """添加 CF 代理 URL 到 LOW 专属池。
    - admin：body 须带 discord_id（指定写入哪个子池；空字符串视作兜底子池）
    - LOW 用户：必须带 X-Discord-Token，强制写入自己的子池
    """
    body = await request.json()
    is_admin, scope_dc, _ = _resolve_low_pool_scope(request, body_discord_id=str(body.get("discord_id", "") or ""))
    url = (body.get("url") or "").strip().rstrip("/")
    label = (body.get("label") or "").strip()
    if not url.startswith("https://"):
        raise HTTPException(status_code=400, detail="URL 必须以 https:// 开头")
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "INSERT INTO cf_proxy_pool (url, label, owner, owner_discord_id) "
                "VALUES ($1, $2, 'low_admin', $3) "
                "ON CONFLICT (url, owner, owner_discord_id) "
                "DO UPDATE SET is_active=TRUE, label=EXCLUDED.label "
                "RETURNING id, url, label, is_active, owner_discord_id",
                url, label, scope_dc,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    await load_cf_proxies_from_db()
    return {
        "ok": True,
        "proxy": {
            "id": row["id"], "url": row["url"], "label": row["label"],
            "discord_id": row["owner_discord_id"] or "",
        },
    }


@app.delete("/admin/low-cf-proxies/{proxy_id}")
async def low_cf_proxies_delete(proxy_id: int, request: Request):
    """删除 LOW 专属池中的代理。LOW 用户仅可删除自己 Discord 子池的行。"""
    is_admin, scope_dc, _ = _resolve_low_pool_scope(request)
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        if is_admin and not scope_dc:
            # admin 未指定 discord_id：允许跨子池删除（按 id）
            await conn.execute(
                "DELETE FROM cf_proxy_pool WHERE id=$1 AND owner='low_admin'", proxy_id,
            )
        else:
            await conn.execute(
                "DELETE FROM cf_proxy_pool "
                "WHERE id=$1 AND owner='low_admin' AND owner_discord_id=$2",
                proxy_id, scope_dc,
            )
    await load_cf_proxies_from_db()
    return {"ok": True}


@app.patch("/admin/low-cf-proxies/{proxy_id}")
async def low_cf_proxies_toggle(proxy_id: int, request: Request):
    """切换 LOW 专属池代理启用/禁用状态。LOW 用户仅可操作自己子池。"""
    body = await request.json()
    is_admin, scope_dc, _ = _resolve_low_pool_scope(request)
    is_active = bool(body.get("is_active", True))
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        if is_admin and not scope_dc:
            await conn.execute(
                "UPDATE cf_proxy_pool SET is_active=$1 WHERE id=$2 AND owner='low_admin'",
                is_active, proxy_id,
            )
        else:
            await conn.execute(
                "UPDATE cf_proxy_pool SET is_active=$1 "
                "WHERE id=$2 AND owner='low_admin' AND owner_discord_id=$3",
                is_active, proxy_id, scope_dc,
            )
    await load_cf_proxies_from_db()
    return {"ok": True, "is_active": is_active}


@app.get("/admin/low-cf-proxies/export")
async def low_cf_proxies_export(request: Request):
    """导出 LOW 专属 CF 代理池。
    - 完整管理员：导出所有 Discord 子池（含 owner_discord_id）；可用 ?discord_id= 筛选单个子池。
    - LOW 用户：必须带 X-Discord-Token，仅导出自己 Discord ID 的子池。
    供跨实例迁移或备份使用，可与 POST /admin/low-cf-proxies/import 配合。
    """
    is_admin, scope_dc, dc_tag = _resolve_low_pool_scope(request)
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    if is_admin and not scope_dc:
        # admin 不指定 discord_id：导出全部 LOW 行
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT url, label, owner_discord_id, is_active, created_at "
                "FROM cf_proxy_pool WHERE owner='low_admin' ORDER BY owner_discord_id, id"
            )
    else:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT url, label, owner_discord_id, is_active, created_at "
                "FROM cf_proxy_pool WHERE owner='low_admin' AND owner_discord_id=$1 ORDER BY id",
                scope_dc,
            )
    return {
        "proxies": [
            {
                "url":              r["url"],
                "label":            r["label"] or "",
                "owner":            "low_admin",
                "owner_discord_id": r["owner_discord_id"] or "",
                "is_active":        bool(r["is_active"]),
                "created_at":       r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
        "count":            len(rows),
        "scope":            "admin_all" if (is_admin and not scope_dc) else "single",
        "scope_discord_id": scope_dc,
        "scope_discord_tag": dc_tag,
        "exported_at":      int(time.time()),
    }


@app.post("/admin/low-cf-proxies/import")
async def low_cf_proxies_import(request: Request):
    """批量导入 LOW 专属 CF 代理 URL（幂等 upsert）。
    - 完整管理员：body 中每条记录的 owner_discord_id 决定写入哪个子池；
      也可在 body 顶层提供 discord_id 作为统一目标（优先级低于行内字段）。
    - LOW 用户：必须带 X-Discord-Token，所有行强制写入自己的 Discord 子池，忽略行内 owner_discord_id。
    请求体：{ "proxies": [{ "url": "...", "label": "...", "owner_discord_id": "...", "is_active": true }] }
    """
    body = await request.json()
    is_admin, scope_dc, _ = _resolve_low_pool_scope(
        request, body_discord_id=str(body.get("discord_id", "") or "")
    )
    proxies_in = body.get("proxies", [])
    if not isinstance(proxies_in, list):
        raise HTTPException(status_code=400, detail="proxies 必须是数组")
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    imported = 0
    skipped = 0
    async with pool.acquire() as conn:
        for item in proxies_in:
            url = (item.get("url") or "").strip().rstrip("/")
            if not url.startswith("https://"):
                skipped += 1
                continue
            label = (item.get("label") or "").strip()
            is_active = bool(item.get("is_active", True))
            # LOW 用户强制用自己的 Discord ID；admin 允许行内指定
            if is_admin and not scope_dc:
                dc_id = str(item.get("owner_discord_id") or "")
            else:
                dc_id = scope_dc   # LOW 用户或 admin 已指定 discord_id
            try:
                await conn.execute(
                    "INSERT INTO cf_proxy_pool (url, label, owner, owner_discord_id, is_active) "
                    "VALUES ($1, $2, 'low_admin', $3, $4) "
                    "ON CONFLICT (url, owner, owner_discord_id) DO UPDATE SET "
                    "label=EXCLUDED.label, is_active=EXCLUDED.is_active",
                    url, label, dc_id, is_active,
                )
                imported += 1
            except Exception as e:
                print(f"[low-cf-proxies/import] 跳过 {url}: {e}")
                skipped += 1
    await load_cf_proxies_from_db()
    return {"ok": True, "imported": imported, "skipped": skipped}


@app.get("/prizes")
@app.get("/key/prizes")
async def public_list_prizes():
    """公开接口：获取启用中的奖品（供抽奖轮使用，30s 缓存）"""
    cached = _admin_cache_get("prizes")
    if cached is not None:
        return Response(content=cached, media_type="application/json")
    pool = await _get_db_pool()
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, quantity, weight FROM lottery_prizes WHERE is_active=TRUE ORDER BY id"
        )
    body = json.dumps([dict(r) for r in rows], ensure_ascii=False).encode()
    _admin_cache_set("prizes", body)
    return Response(content=body, media_type="application/json")


@app.get("/key/usage")
async def key_usage_query(key: str):
    """公开接口：用 API 密钥查询自己的用量（不返回完整密钥）"""
    if not key or (key not in VALID_CLIENT_KEYS and key not in POKEBALL_KEYS):
        raise HTTPException(status_code=404, detail="密钥不存在或无效")
    # 宝可梦球 key
    if key in POKEBALL_KEYS:
        pb = POKEBALL_KEYS[key]
        masked = (key[:10] + "*" * max(0, len(key) - 14) + key[-4:]) if len(key) > 14 else (key[:4] + "****")
        return {
            "masked": masked,
            "usage_count": pb["total_used"],
            "usage_cost": pb["total_used"],
            "usage_limit": pb["capacity"],
            "is_pokeball": True,
            "name": pb["name"],
        }
    meta = VALID_CLIENT_KEYS[key]
    masked = (key[:10] + "*" * max(0, len(key) - 14) + key[-4:]) if len(key) > 14 else (key[:4] + "****")
    return {
        "masked": masked,
        "usage_count": meta.get("usage_count", 0),
        "usage_cost": round(meta.get("usage_count", 0) + _key_fractional_usage.get(key, 0.0), 2),
        "usage_limit": meta.get("usage_limit"),
        "is_pokeball": False,
    }


# ─── 背包 & 宝可梦球 接口 ─────────────────────────────────────────

POKEBALL_CAPACITIES = [50, 75, 100, 200]


_RE_QUOTA_AMOUNT = re.compile(r"(\d+)")
_RE_POKEBALL_CAPS = [
    re.compile(r'宝可梦球?[^0-9]*[【\[\(（]容量[：:]?\s*(\d+)[】\]\)）]'),
    re.compile(r'宝可梦球?[^0-9]*[【\[\(（]\s*(\d+)\s*[】\]\)）]'),
    re.compile(r'宝可梦球?[^0-9]*容量[：:]?\s*(\d+)'),
    re.compile(r'宝可梦球?[^0-9]*(\d+)'),
]


def _parse_ts(s) -> "Optional[datetime.datetime]":
    """将导出的 ISO 时间戳字符串解析为 Python datetime；None/空值返回 None。
    asyncpg 要求 TIMESTAMPTZ 参数为 datetime 对象，不接受字符串。"""
    if not s:
        return None
    try:
        import datetime as _dt
        return _dt.datetime.fromisoformat(str(s))
    except (ValueError, TypeError):
        return None


def _parse_quota_amount(prize_name: str) -> Optional[int]:
    """从额度奖品名称解析充值次数，如「额度【100次】」→ 100（预编译正则）"""
    if "额度" not in prize_name:
        return None
    m = _RE_QUOTA_AMOUNT.search(prize_name)
    return int(m.group(1)) if m else None


def _parse_pokeball_capacity(prize_name: str) -> Optional[int]:
    """从奖品名称解析宝可梦球容量，支持多种括号格式（预编译正则）"""
    for pat in _RE_POKEBALL_CAPS:
        m = pat.search(prize_name)
        if m:
            cap = int(m.group(1))
            if cap > 0:
                return cap
    return None


@app.get("/key/check-password")
async def check_password(password: str):
    """检查密码是否已被占用（公开接口）"""
    if not password:
        raise HTTPException(status_code=400, detail="缺少 password")
    pool = await _get_db_pool()
    if not pool:
        return {"exists": False}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM user_passwords WHERE password=$1", password)
    return {"exists": row is not None}


@app.post("/key/claim-prize")
async def claim_prize(request: Request):
    """用户领取抽奖奖品到背包（使用自定义密码标识身份）"""
    body = await request.json()
    password = body.get("password", "").strip()
    prize_name = body.get("prize_name", "").strip()
    force = bool(body.get("force", False))   # 密码已占用时强制继续（老用户再次领奖）
    if not password or not prize_name:
        raise HTTPException(status_code=400, detail="缺少 password 或 prize_name")
    if len(password) < 4:
        raise HTTPException(status_code=400, detail="密码至少需要 4 个字符")

    # 解析 metadata
    metadata: dict = body.get("metadata", {})
    cap = _parse_pokeball_capacity(prize_name)
    if cap:
        metadata["capacity"] = cap
    quota_amt = _parse_quota_amount(prize_name)
    if quota_amt:
        metadata["quota_amount"] = quota_amt

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT 1 FROM user_passwords WHERE password=$1", password)
        if existing and not force:
            # 密码已被占用，且用户未确认是自己的
            raise HTTPException(status_code=409, detail="该密码已被占用，请换一个；或者点击「这是我的密码」继续领取到该账号")
        if not existing:
            # 注册新密码
            await conn.execute("INSERT INTO user_passwords (password) VALUES ($1)", password)
        # 写入背包
        row = await conn.fetchrow(
            "INSERT INTO user_items (owner_key, prize_name, metadata) VALUES ($1,$2,$3) RETURNING id, created_at",
            password, prize_name, json.dumps(metadata)
        )
    return {"success": True, "item_id": row["id"]}


@app.post("/key/quota-claim")
async def quota_claim(request: Request):
    """领取额度类奖品：向指定 key 充值 usage_limit 并记录背包"""
    body = await request.json()
    password   = (body.get("password")   or "").strip()
    prize_name = (body.get("prize_name") or "").strip()
    target_key = (body.get("target_key") or "").strip()
    force      = bool(body.get("force", False))

    if not password or not prize_name or not target_key:
        raise HTTPException(status_code=400, detail="缺少 password、prize_name 或 target_key")
    if len(password) < 4:
        raise HTTPException(status_code=400, detail="密码至少 4 位")

    amount = _parse_quota_amount(prize_name)
    if not amount:
        raise HTTPException(status_code=400, detail="无法从奖品名称解析额度数量")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        # 注册密码
        existing = await conn.fetchrow("SELECT 1 FROM user_passwords WHERE password=$1", password)
        if existing and not force:
            raise HTTPException(
                status_code=409,
                detail="该密码已被占用，请换一个；或者点击「这是我的密码」继续领取到该账号"
            )
        if not existing:
            await conn.execute("INSERT INTO user_passwords (password) VALUES ($1)", password)

        # 验证目标 key 存在
        key_row = await conn.fetchrow(
            "SELECT key, usage_limit FROM jb_client_keys WHERE key=$1", target_key
        )
        if not key_row:
            raise HTTPException(status_code=404, detail="目标 Key 不存在，请确认 Key 正确")

        # 写入背包（立即标记为已使用）
        masked = target_key[:8] + "…" + target_key[-4:] if len(target_key) > 12 else target_key
        metadata = {"amount": amount, "target_key_masked": masked}
        await conn.execute(
            """INSERT INTO user_items (owner_key, prize_name, metadata, used, used_at)
               VALUES ($1, $2, $3, TRUE, NOW())""",
            password, prize_name, json.dumps(metadata)
        )

        # 充值 usage_limit
        await conn.execute(
            "UPDATE jb_client_keys SET usage_limit = COALESCE(usage_limit, 0) + $1 WHERE key = $2",
            amount, target_key
        )

    # 同步内存
    if target_key in VALID_CLIENT_KEYS:
        old = VALID_CLIENT_KEYS[target_key].get("usage_limit") or 0
        VALID_CLIENT_KEYS[target_key]["usage_limit"] = old + amount

    return {"success": True, "amount": amount}


@app.post("/key/quota-redeem")
async def quota_redeem(request: Request):
    """从背包使用额度道具：标记已使用 + 充值目标 Key 用量上限"""
    body = await request.json()
    password = (body.get("password") or "").strip()
    item_id = body.get("item_id")
    target_key = (body.get("target_key") or "").strip()

    if not password or not item_id or not target_key:
        raise HTTPException(status_code=400, detail="缺少参数")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        item = await conn.fetchrow(
            "SELECT id, prize_name, metadata, used FROM user_items WHERE id=$1 AND owner_key=$2",
            item_id, password
        )
        if not item:
            raise HTTPException(status_code=404, detail="奖品不存在或无权访问")
        if item["used"]:
            raise HTTPException(status_code=400, detail="该额度道具已使用")

        meta = json.loads(item["metadata"]) if item["metadata"] else {}
        amount = _parse_quota_amount(item["prize_name"]) or meta.get("quota_amount")
        if not amount:
            raise HTTPException(status_code=400, detail="无法解析额度数量")

        key_row = await conn.fetchrow("SELECT key FROM jb_client_keys WHERE key=$1", target_key)
        if not key_row:
            raise HTTPException(status_code=404, detail="目标 Key 不存在，请确认 Key 正确")

        await conn.execute(
            "UPDATE user_items SET used=TRUE, used_at=NOW() WHERE id=$1", item_id
        )
        await conn.execute(
            "UPDATE jb_client_keys SET usage_limit = COALESCE(usage_limit, 0) + $1 WHERE key=$2",
            amount, target_key
        )

    if target_key in VALID_CLIENT_KEYS:
        old = VALID_CLIENT_KEYS[target_key].get("usage_limit") or 0
        VALID_CLIENT_KEYS[target_key]["usage_limit"] = old + amount

    return {"success": True, "amount": amount}


@app.get("/key/backpack")
async def get_backpack(password: str):
    """查询用户背包物品列表（按密码查询）"""
    if not password:
        raise HTTPException(status_code=400, detail="缺少 password")
    pool = await _get_db_pool()
    if not pool:
        return {"items": [], "pokeballs": []}
    # 单次连接完成两次查询，减少连接池争用
    async with pool.acquire() as conn:
        pw_row = await conn.fetchrow("SELECT 1 FROM user_passwords WHERE password=$1", password)
        if not pw_row:
            raise HTTPException(status_code=404, detail="密码不存在，请先通过领奖设置密码")
        # asyncpg 同一连接不支持并发查询，必须顺序执行
        rows = await conn.fetch(
            "SELECT id, prize_name, metadata, used, used_at, created_at FROM user_items WHERE owner_key=$1 ORDER BY created_at DESC",
            password,
        )
        pb_rows = await conn.fetch(
            """SELECT p.id, p.ball_key, p.name, p.capacity, p.total_used, p.created_at,
                      ARRAY(SELECT member_key FROM pokeball_members WHERE pokeball_id=p.id) AS members
               FROM pokeballs p
               JOIN user_items ui ON ui.owner_key=$1
                   AND ui.used=TRUE
                   AND (
                       CASE WHEN ui.metadata->>'ball_key' IS NOT NULL
                            THEN ui.metadata->>'ball_key' = p.ball_key
                            ELSE (ui.metadata->>'pokeball_id')::int = p.id
                       END
                   )
               GROUP BY p.id
               ORDER BY p.created_at DESC""",
            password,
        )
    result = [
        {
            "id": r["id"],
            "prize_name": r["prize_name"],
            "metadata": json.loads(r["metadata"]) if r["metadata"] else {},
            "used": r["used"],
            "used_at": r["used_at"].isoformat() if r["used_at"] else None,
            "created_at": r["created_at"].isoformat(),
        }
        for r in rows
    ]
    pokeballs = [
        {
            "id": pb["id"],
            "ball_key": pb["ball_key"],
            "name": pb["name"],
            "capacity": pb["capacity"],
            "total_used": pb["total_used"],
            "members": list(pb["members"]),
            "created_at": pb["created_at"].isoformat(),
        }
        for pb in pb_rows
    ]
    return {"items": result, "pokeballs": pokeballs}


@app.post("/key/pokeball/create")
async def create_pokeball(request: Request):
    """激活背包中的宝可梦球道具，创建虚拟聚合 key"""
    body = await request.json()
    password = body.get("password", "").strip()
    item_id = body.get("item_id")
    name = body.get("name", "").strip()
    if not password or not item_id or not name:
        raise HTTPException(status_code=400, detail="缺少 password、item_id 或 name")

    manual_capacity: Optional[int] = body.get("capacity")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        item = await conn.fetchrow(
            "SELECT id, prize_name, metadata, used FROM user_items WHERE id=$1 AND owner_key=$2",
            item_id, password
        )
        if not item:
            raise HTTPException(status_code=404, detail="道具不存在或不属于该账号")
        if item["used"]:
            raise HTTPException(status_code=409, detail="该道具已被使用")
        meta = json.loads(item["metadata"]) if item["metadata"] else {}
        capacity = (
            meta.get("capacity")
            or _parse_pokeball_capacity(item["prize_name"])
            or manual_capacity
        )
        if not capacity or capacity <= 0:
            raise HTTPException(status_code=400, detail="无法识别宝可梦球容量，请联系管理员")

        # 生成唯一 ball_key
        import secrets
        ball_key = "jb-pb-" + secrets.token_hex(10)

        pb_row = await conn.fetchrow(
            "INSERT INTO pokeballs (ball_key, name, capacity) VALUES ($1,$2,$3) RETURNING id",
            ball_key, name, capacity
        )
        pb_id = pb_row["id"]

        # 标记道具已使用，记录 pokeball_id 和 ball_key（ball_key 跨环境迁移时稳定不变）
        meta["pokeball_id"] = pb_id
        meta["ball_key"] = ball_key
        await conn.execute(
            "UPDATE user_items SET used=TRUE, used_at=NOW(), metadata=$1 WHERE id=$2",
            json.dumps(meta), item_id
        )

    # 加载到内存
    POKEBALL_KEYS[ball_key] = {
        "id": pb_id,
        "name": name,
        "capacity": capacity,
        "total_used": 0,
        "rr_index": 0,
        "members": [],
    }
    return {"success": True, "ball_key": ball_key, "name": name, "capacity": capacity}


@app.get("/key/pokeball/{ball_key}")
async def get_pokeball_info(ball_key: str, password: str):
    """查询宝可梦球状态（需要所有者密码）"""
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        owner_check = await conn.fetchrow(
            """SELECT p.id, p.name, p.capacity, p.total_used
               FROM pokeballs p
               WHERE p.ball_key=$1
                 AND EXISTS (
                     SELECT 1 FROM user_items ui
                     WHERE ui.owner_key=$2
                       AND ui.used=TRUE
                       AND (
                           CASE WHEN ui.metadata->>'ball_key' IS NOT NULL
                                THEN ui.metadata->>'ball_key' = p.ball_key
                                ELSE (ui.metadata->>'pokeball_id')::int = p.id
                           END
                       )
                 )""",
            ball_key, password
        )
        if not owner_check:
            raise HTTPException(status_code=403, detail="宝可梦球不存在或无权访问")
        members = await conn.fetch(
            "SELECT member_key FROM pokeball_members WHERE pokeball_id=$1", owner_check["id"]
        )
    member_info = []
    for m in members:
        mk = m["member_key"]
        meta = VALID_CLIENT_KEYS.get(mk, {})
        masked = (mk[:8] + "****" + mk[-4:]) if len(mk) > 12 else mk[:4] + "****"
        member_info.append({
            "masked": masked,
            "usage_count": meta.get("usage_count", 0),
            "usage_limit": meta.get("usage_limit"),
        })
    return {
        "ball_key": ball_key,
        "name": owner_check["name"],
        "capacity": owner_check["capacity"],
        "total_used": owner_check["total_used"],
        "members": member_info,
    }


@app.post("/key/pokeball/{ball_key}/members")
async def add_pokeball_member(ball_key: str, request: Request):
    """向宝可梦球中添加成员 key（需要所有者密码验证）"""
    body = await request.json()
    password = body.get("password", "").strip()
    member_key = body.get("member_key", "").strip()
    if not password or not member_key:
        raise HTTPException(status_code=400, detail="缺少 password 或 member_key")

    # 验证成员 key 有效性
    if member_key not in VALID_CLIENT_KEYS:
        raise HTTPException(status_code=404, detail="成员 key 不存在")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        pb = await conn.fetchrow(
            """SELECT p.id, p.capacity, p.total_used
               FROM pokeballs p
               WHERE p.ball_key=$1
                 AND EXISTS (
                     SELECT 1 FROM user_items ui
                     WHERE ui.owner_key=$2 AND ui.used=TRUE
                       AND (
                           CASE WHEN ui.metadata->>'ball_key' IS NOT NULL
                                THEN ui.metadata->>'ball_key' = p.ball_key
                                ELSE (ui.metadata->>'pokeball_id')::int = p.id
                           END
                       )
                 )""",
            ball_key, password
        )
        if not pb:
            raise HTTPException(status_code=403, detail="宝可梦球不存在或无权访问")

        # 检查成员 key 剩余额度
        member_meta = VALID_CLIENT_KEYS[member_key]
        member_remaining = (member_meta.get("usage_limit") or 0) - member_meta.get("usage_count", 0)
        if member_remaining <= 0:
            raise HTTPException(status_code=400, detail="该成员 key 已无剩余额度")

        # 检查是否已加入 + 已用数量（asyncpg 同一连接不可并发，顺序执行）
        dup_check = await conn.fetchrow(
            "SELECT 1 FROM pokeball_members WHERE pokeball_id=$1 AND member_key=$2",
            pb["id"], member_key
        )
        alloc_row = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt FROM pokeball_members WHERE pokeball_id=$1",
            pb["id"]
        )
        if dup_check:
            raise HTTPException(status_code=409, detail="该 key 已在宝可梦球中")

        # 剩余容量 = 总容量 - 已用 - 现有成员数（简化：每个成员占1槽，capacity 即成员上限）
        # 注意：原逻辑是按剩余额度计算，保留原语义用 total_used 检查即可
        remaining_cap = pb["capacity"] - pb["total_used"]
        if remaining_cap <= 0:
            raise HTTPException(status_code=400, detail="宝可梦球已无剩余容量")

        await conn.execute(
            "INSERT INTO pokeball_members (pokeball_id, member_key) VALUES ($1,$2)",
            pb["id"], member_key
        )

    pb_mem = POKEBALL_KEYS.get(ball_key)
    if pb_mem and member_key not in pb_mem["members"]:
        pb_mem["members"].append(member_key)

    return {"success": True}


@app.delete("/key/pokeball/{ball_key}")
async def delete_pokeball(ball_key: str, password: str):
    """删除整个宝可梦球（含所有成员 key 记录）"""
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        pb = await conn.fetchrow(
            """SELECT p.id FROM pokeballs p
               WHERE p.ball_key=$1
                 AND EXISTS (
                     SELECT 1 FROM user_items ui
                     WHERE ui.owner_key=$2 AND ui.used=TRUE
                       AND (
                           CASE WHEN ui.metadata->>'ball_key' IS NOT NULL
                                THEN ui.metadata->>'ball_key' = p.ball_key
                                ELSE (ui.metadata->>'pokeball_id')::int = p.id
                           END
                       )
                 )""",
            ball_key, password,
        )
        if not pb:
            raise HTTPException(status_code=403, detail="宝可梦球不存在或无权访问")
        await conn.execute("DELETE FROM pokeball_members WHERE pokeball_id=$1", pb["id"])
        await conn.execute("DELETE FROM pokeballs WHERE id=$1", pb["id"])
    POKEBALL_KEYS.pop(ball_key, None)
    return {"success": True}


@app.delete("/key/pokeball/{ball_key}/members/{member_key}")
async def remove_pokeball_member(ball_key: str, member_key: str, password: str):
    """从宝可梦球中移除成员 key"""
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        pb = await conn.fetchrow(
            """SELECT p.id FROM pokeballs p
               WHERE p.ball_key=$1
                 AND EXISTS (
                     SELECT 1 FROM user_items ui
                     WHERE ui.owner_key=$2 AND ui.used=TRUE
                       AND (
                           CASE WHEN ui.metadata->>'ball_key' IS NOT NULL
                                THEN ui.metadata->>'ball_key' = p.ball_key
                                ELSE (ui.metadata->>'pokeball_id')::int = p.id
                           END
                       )
                 )""",
            ball_key, password
        )
        if not pb:
            raise HTTPException(status_code=403, detail="宝可梦球不存在或无权访问")
        await conn.execute(
            "DELETE FROM pokeball_members WHERE pokeball_id=$1 AND member_key=$2",
            pb["id"], member_key
        )
    pb_mem = POKEBALL_KEYS.get(ball_key)
    if pb_mem:
        pb_mem["members"] = [mk for mk in pb_mem["members"] if mk != member_key]
    return {"success": True}


@app.get("/key/saint-points")
async def get_saint_points(password: str):
    """查询圣人点数"""
    if not password:
        raise HTTPException(status_code=400, detail="缺少 password")
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT points FROM saint_points WHERE password=$1", password)
    return {"points": row["points"] if row else 0}


@app.post("/key/saint-donate")
async def saint_donate(request: Request):
    """捐献一个 JB client key，换取 1 个圣人点数"""
    body = await request.json()
    password = (body.get("password") or "").strip()
    api_key = (body.get("api_key") or "").strip()
    if not password or not api_key:
        raise HTTPException(status_code=400, detail="缺少 password 或 api_key")
    if len(password) < 4:
        raise HTTPException(status_code=400, detail="密码至少 4 位")

    # 验证 key 存在于 client keys
    if api_key not in VALID_CLIENT_KEYS:
        raise HTTPException(status_code=404, detail="Key 不存在或已被删除，无法捐献")

    # 检查剩余用量：必须 >= 90%（即已用 <= 10%）
    key_meta = VALID_CLIENT_KEYS[api_key]
    usage_limit_raw = key_meta.get("usage_limit")   # None=无限, 0=待激活, >0=正常
    usage_limit = usage_limit_raw or 0
    usage_count = key_meta.get("usage_count") or 0

    # 额度为 0 的 key（NC 待激活 或 等待返回参数的预签 key）一律拒绝捐献
    if usage_limit_raw is not None and usage_limit_raw == 0:
        raise HTTPException(
            status_code=403,
            detail="此 Key 额度为 0（尚未激活），请等待激活后再捐献",
        )

    if usage_limit > 0 and usage_count / usage_limit > 0.10:
        raise HTTPException(status_code=400, detail="佛说，无元")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        # 保留兼容：拦截自助绑卡流程中尚未完成的预签 key
        sr_row = await conn.fetchrow(
            "SELECT status FROM self_register_jobs WHERE result_keys=$1", api_key
        )
        if sr_row and sr_row["status"] in ("processing", "pending"):
            raise HTTPException(status_code=403, detail="此 Key 尚未激活（等待返回参数），请等待激活后再捐献")

        # 确认 key 在数据库中，并取 account_id
        key_row = await conn.fetchrow(
            "SELECT key, account_id FROM jb_client_keys WHERE key=$1", api_key
        )
        if not key_row:
            raise HTTPException(status_code=404, detail="Key 不在数据库中，无法捐献")

        account_id = key_row["account_id"]

        # 每个账号只能捐一次：拆分逗号分隔的多账号 ID，逐一检查
        acc_ids = [a.strip() for a in (account_id or "").split(",") if a.strip()]
        if acc_ids:
            existing = await conn.fetchrow(
                "SELECT 1 FROM saint_donations WHERE account_id = ANY($1::text[])", acc_ids
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail="此账号已捐献过 Key，不可重复捐献（佛渡有缘人）"
                )

        # 写操作全部包裹在事务中，避免崩溃导致 Key 被删但点数未到账
        async with conn.transaction():
            await conn.execute("DELETE FROM jb_client_keys WHERE key=$1", api_key)
            await conn.execute(
                "INSERT INTO user_passwords (password) VALUES ($1) ON CONFLICT DO NOTHING",
                password
            )
            if acc_ids:
                await conn.executemany(
                    "INSERT INTO saint_donations (account_id, password) VALUES ($1, $2) ON CONFLICT (account_id) DO NOTHING",
                    [(aid, password) for aid in acc_ids]
                )
            row = await conn.fetchrow(
                """INSERT INTO saint_points (password, points, total_earned, updated_at)
                   VALUES ($1, 1, 1, NOW())
                   ON CONFLICT (password) DO UPDATE
                   SET points = saint_points.points + 1,
                       total_earned = saint_points.total_earned + 1,
                       updated_at = NOW()
                   RETURNING points, total_earned""",
                password,
            )

    # 从内存中删除
    VALID_CLIENT_KEYS.pop(api_key, None)
    _admin_cache_invalidate("leaderboard")
    return {"success": True, "points": row["points"]}


@app.post("/key/saint-spin")
async def saint_spin(request: Request):
    """消耗 1 个圣人点数，在服务端抽取奖品并扣减库存"""
    body = await request.json()
    password = (body.get("password") or "").strip()
    if not password:
        raise HTTPException(status_code=400, detail="缺少 password")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        async with conn.transaction():
            # 1. 原子扣减圣人点数（CAS 防并发超扣）
            updated = await conn.fetchrow(
                """UPDATE saint_points SET points = points - 1, updated_at = NOW()
                   WHERE password=$1 AND points >= 1
                   RETURNING points""",
                password,
            )
            if not updated:
                raise HTTPException(status_code=402, detail="圣人点数不足，请先捐献 Key")

            # 2. 查询有效奖品（库存 > 0 或无限）
            prize_rows = await conn.fetch(
                "SELECT id, name, quantity, weight FROM lottery_prizes WHERE is_active=TRUE AND (quantity=-1 OR quantity>0) ORDER BY id"
            )

            if not prize_rows:
                # 没有可用奖品：回滚自动退还点数
                raise HTTPException(status_code=503, detail="暂无可用奖品，请联系管理员")

            # 3. 加权随机抽取（使用索引直接定位，无需二次 O(n) 搜索）
            weights = [r["weight"] for r in prize_rows]
            idx = _random.choices(range(len(prize_rows)), weights=weights, k=1)[0]
            selected = prize_rows[idx]
            prize_name = selected["name"]

            # 4. 库存 -1（非无限时）
            if selected["quantity"] != -1:
                res = await conn.fetchrow(
                    "UPDATE lottery_prizes SET quantity = quantity - 1 WHERE id=$1 AND quantity > 0 RETURNING id",
                    selected["id"],
                )
                if not res:
                    # 并发导致库存刚好耗尽，回滚退还点数
                    raise HTTPException(status_code=409, detail="奖品库存不足，点数已退还，请重新抽取")

    _admin_cache_invalidate("prizes")
    return {"success": True, "points": updated["points"], "prize": prize_name}


@app.get("/admin/models")
async def admin_list_models():
    """获取模型配置"""
    with open("models.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    return config

@app.post("/admin/models")
async def admin_update_models(req: UpdateModelsRequest):
    """更新模型配置"""
    global models_data, anthropic_model_mappings
    config = {
        "models": req.models,
        "anthropic_model_mappings": req.anthropic_model_mappings or {}
    }
    async with file_write_lock:
        async with aiofiles.open("models.json", "w", encoding="utf-8") as f:
            await f.write(json.dumps(config, indent=2, ensure_ascii=False))
    models_data = load_models()
    return {"success": True, "models_count": len(req.models)}

@app.post("/admin/reload")
async def admin_reload():
    """重新加载所有配置（从数据库）"""
    global models_data
    models_data = load_models()
    await load_keys_from_db()
    await load_accounts_from_db()
    return {
        "success": True,
        "accounts_count": len(JETBRAINS_ACCOUNTS),
        "keys_count": len(VALID_CLIENT_KEYS),
        "models_count": len(models_data.get("data", [])),
    }


# ==================== 账号激活（邮箱+密码自动获取JWT）====================

# 激活任务注册表：task_id -> {status, logs, result}
_activate_tasks: Dict[str, Dict] = {}
_activate_executor = ThreadPoolExecutor(max_workers=5)

class ActivateRequest(BaseModel):
    email: str
    password: str
    discord_token: str = ""  # 非管理员用户须提供，验证 Discord 服务器成员资格

@app.post("/admin/activate")
async def admin_start_activate(req: ActivateRequest, request: Request):
    """启动账号激活任务（无卡激活：依次尝试 RustRover/CLion/RubyMine/DataGrip/WebStorm/Rider 获取 JWT）
    - 管理员（X-Admin-Key 正确）：直接激活，无需 Discord 验证
    - 普通用户：需提供有效的 Discord token（仅验服务器成员资格，不限身份组）
    """
    from jb_activate import process_account as _process_account

    # ── 鉴权：三档身份 ──
    #   admin     → 直通，无 Discord、无每日限额
    #   low_admin → 必须 Discord 登录（用于按 Discord 账号划分 LOW CF 子池），但跳过每日 20 次限额
    #   none      → 普通用户：须验 Discord + 每日 20 次限额
    provided_key = request.headers.get("X-Admin-Key", "")
    is_admin = bool(ADMIN_KEY) and provided_key == ADMIN_KEY
    is_low_admin = (not is_admin) and bool(LOW_ADMIN_KEY) and provided_key == LOW_ADMIN_KEY
    dc_user_id = ""  # 普通 + low_admin 用户均会赋值；admin 保持 ''
    # 仅 admin 直通，low_admin 与普通用户都需 Discord 验证
    if not is_admin:
        token = req.discord_token.strip()
        if not token:
            raise HTTPException(status_code=401, detail="请先通过 Discord 验证后再激活账号")
        dc_info = _DISCORD_VERIFIED.get(token)
        if not dc_info:
            raise HTTPException(status_code=401, detail="Discord 验证无效或已过期，请重新授权")
        if time.time() - dc_info["ts"] > 1800:
            _DISCORD_VERIFIED.pop(token, None)
            raise HTTPException(status_code=401, detail="Discord 验证已过期（30 分钟），请重新授权")
        dc_user_id = str(dc_info.get("user_id", "") or "")

        # ── 每日激活次数限制：仅普通用户受限；low_admin 跳过 ──
        if not is_low_admin and dc_user_id:
            _rl_pool = await _get_db_pool()
            if _rl_pool:
                async with _rl_pool.acquire() as _rl_conn:
                    row = await _rl_conn.fetchrow(
                        """
                        INSERT INTO dc_activate_limits (dc_user_id, date, count)
                        VALUES ($1, CURRENT_DATE, 1)
                        ON CONFLICT (dc_user_id, date)
                        DO UPDATE SET count = dc_activate_limits.count + 1
                        RETURNING count
                        """,
                        dc_user_id,
                    )
                    if row and row["count"] > 20:
                        raise HTTPException(
                            status_code=429,
                            detail=f"今日激活次数已达上限（20 次/天），请明天再试（已用 {row['count']} 次）",
                        )

    task_id = str(uuid.uuid4())
    log_queue: queue.Queue = queue.Queue()

    # LOW 用户优先使用已创建的个人专属密钥；管理员或无个人 key 时沿用 preissued 机制
    personal_key = _get_low_personal_key(dc_user_id) if (is_low_admin and dc_user_id) else ""

    if personal_key:
        # 有个人 key → 激活成功后累加配额，无需预签新 key
        preissued_key = ""
    else:
        # ★ 预签发 0 额度 key（凭证全部到位后自动升级：普通 25，LOW 用户 16）
        # is_nc_key=True 保护其不被清理任务误删；is_low_admin_key 决定升级后配额和请求时输入/输出限制
        preissued_key = f"sk-jb-{secrets.token_hex(24)}"
        preissued_meta: Dict[str, Any] = {
            "usage_limit": 0, "usage_count": 0, "account_id": "",
            "is_nc_key": True, "is_low_admin_key": is_low_admin,
            # 把创建者 Discord ID 写入 LOW 预签 key，确保 /admin/keys 按 Discord 分组归属正确
            "low_admin_discord_id": str(dc_user_id or "") if is_low_admin else "",
        }
        VALID_CLIENT_KEYS[preissued_key] = preissued_meta
        _admin_cache_invalidate("keys", "status")
        _pre_db = await _get_db_pool()
        if _pre_db:
            await _upsert_key_to_db(preissued_key, preissued_meta)

    _activate_tasks[task_id] = {
        "status": "running",
        "email": req.email,
        "password": req.password,   # ★ 供 stream handler 保存 pending_nc_pass 用
        "logs": [],
        "result": None,
        "log_queue": log_queue,
        "preissued_key": preissued_key,   # ★ 预签 key，有个人 key 时为空
        "personal_key": personal_key,     # ★ LOW 用户个人 key（累加配额用），无则为空
        "is_low_admin": is_low_admin,     # ★ stream handler 据此写 pending_nc_low_admin
        "discord_user_id": dc_user_id,    # ★ stream handler 据此写 pending_nc_discord_id
    }

    def _log_cb(msg: str):
        _activate_tasks[task_id]["logs"].append(msg)
        log_queue.put(msg)

    def _run():
        try:
            # LOW_ADMIN 用户的激活全程使用 LOW CF 池，按 Discord 账号挑选对应子池
            result = _process_account(
                req.email, req.password, log_cb=_log_cb,
                use_low_pool=is_low_admin,
                low_discord_id=(dc_user_id if is_low_admin else ""),
            )
            _activate_tasks[task_id]["result"] = result
            # 若激活后自动修改了密码，更新任务里的 password，让后续 pending_nc_pass 存的是新密码
            if result.get("new_password"):
                _activate_tasks[task_id]["password"] = result["new_password"]
            if result.get("jwt") or result.get("pending_nc_lids"):
                # jwt 已就绪，或全部 pending（492 Untrusted，等待信任）都算成功
                _activate_tasks[task_id]["status"] = "success"
            else:
                _activate_tasks[task_id]["status"] = "failed"
        except Exception as e:
            _activate_tasks[task_id]["result"] = {"error": str(e)}
            _activate_tasks[task_id]["status"] = "failed"
            _log_cb(f"[EXCEPTION] {e}")
        finally:
            log_queue.put(None)  # sentinel: 结束信号

    # LOW_ADMIN 用户单条激活走 per-Discord 独立线程池，不同 Discord 账号互不干扰；
    # 完整管理员的单条激活继续使用主激活池（max_workers=5）。
    loop = asyncio.get_event_loop()
    runner_executor = _get_low_executor(dc_user_id) if is_low_admin else _activate_executor
    loop.run_in_executor(runner_executor, _run)

    return {
        "task_id": task_id,
        "preissued_key": preissued_key,        # 无个人 key 时有值；有个人 key 时为空
        "personal_key": personal_key,          # LOW 用户个人 key（有则复用，无则空）
    }

@app.get("/admin/activate/{task_id}/stream")
async def admin_activate_stream(task_id: str):
    """SSE 流式返回激活日志，任务完成后自动将账号加入系统"""
    if task_id not in _activate_tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    task = _activate_tasks[task_id]
    log_queue: queue.Queue = task["log_queue"]

    async def event_generator():
        # 取出个人 key（有则复用，无则沿用 preissued_key 机制）
        _personal_key = task.get("personal_key", "") or ""

        # ── LOW 任务身份：使用 LOW 专用 DB Pool，普通任务使用主 DB Pool ──
        _is_low_task = bool(task.get("is_low_admin"))

        async def _get_pool_for_task():
            """LOW 任务使用 LOW DB 池；普通/admin 任务使用主 DB 池。"""
            return await _get_low_db_pool() if _is_low_task else await _get_db_pool()

        # ── 失败时删除零额度预签 key，避免僵尸积压 ──
        # 使用个人 key 时无需删除（个人 key 长期存在）
        async def _discard_preissued():
            if _personal_key:
                return  # 个人 key 不删除
            _bk = task.get("preissued_key", "")
            if _bk and VALID_CLIENT_KEYS.get(_bk, {}).get("usage_limit", 0) == 0:
                VALID_CLIENT_KEYS.pop(_bk, None)
                try:
                    _cdb = await _get_pool_for_task()
                    if _cdb:
                        async with _cdb.acquire() as _cc:
                            await _cc.execute(
                                "DELETE FROM jb_client_keys WHERE key=$1 AND usage_limit=0", _bk
                            )
                except Exception:
                    pass

        # 首先发送历史日志（重连时补全）
        for old_log in task["logs"]:
            yield f"data: {json.dumps({'type': 'log', 'msg': old_log})}\n\n"

        if task["status"] != "running":
            # 任务已完成 —— 幂等守卫：若已完成 key 升级，直接返回
            if task.get("stream_processed"):
                # stream 已处理过（有个人 key 时的标记），直接带出对应 key
                _done_key = _personal_key or task.get("preissued_key", "")
                yield f"data: {json.dumps({'type': 'done', 'status': task['status'], 'result': task.get('result'), 'generated_key': _done_key, 'is_existing_key': False})}\n\n"
                return
            if not _personal_key:
                # 无个人 key：检查 preissued_key 是否已被升级
                _pk_done = task.get("preissued_key", "")
                if _pk_done and VALID_CLIENT_KEYS.get(_pk_done, {}).get("usage_limit", 0) > 0:
                    yield f"data: {json.dumps({'type': 'done', 'status': task['status'], 'result': task.get('result'), 'generated_key': _pk_done, 'is_existing_key': False})}\n\n"
                    return
            # 继续走完整账号入池流程（队列已空，while 立即 break）

        # 流式读取新日志
        while True:
            try:
                msg = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: log_queue.get(timeout=1)
                )
                if msg is None:
                    break
                yield f"data: {json.dumps({'type': 'log', 'msg': msg})}\n\n"
            except Exception:
                if task["status"] != "running":
                    break
                continue

        # 任务完成，如果成功则自动添加账号并生成限量密钥
        result = task.get("result") or {}
        generated_key = None
        duplicate = False
        if result.get("jwt") and task["status"] == "success":
            try:
                new_account: Dict[str, Any] = {
                    "jwt": result["jwt"],
                    "has_quota": True,
                    "last_updated": time.time(),  # 激活时记录时间，避免立即触发 JWT 刷新
                }
                if result.get("license_id"):
                    new_account["licenseId"] = result["license_id"]
                # 保存 OAuth id_token 作为 authorization，供 JWT 过期后自动刷新使用
                if result.get("id_token"):
                    new_account["authorization"] = result["id_token"]

                # ── 捐献账号封锁：曾捐献过 Key 的账号不允许重新激活 ──
                if result.get("license_id"):
                    _db_pool = await _get_pool_for_task()
                    if _db_pool:
                        async with _db_pool.acquire() as _conn:
                            _donated = await _conn.fetchrow(
                                "SELECT 1 FROM saint_donations WHERE account_id=$1",
                                result["license_id"],
                            )
                        if _donated:
                            task["status"] = "donated_blocked"
                            await _discard_preissued()
                            yield f"data: {json.dumps({'type': 'log', 'msg': '✗ 此账号曾捐献过 Key，已被封锁，无法重新激活获取新密钥'})}\n\n"
                            yield f"data: {json.dumps({'type': 'done', 'status': 'donated_blocked', 'result': result, 'generated_key': None, 'is_existing_key': False})}\n\n"
                            return

                # 检查是否重复：1) 先查活跃池  2) 再查密钥表（账号可能已被删除但 key 保留）
                duplicate = False
                existing_acc = None
                for acc in JETBRAINS_ACCOUNTS:
                    if result.get("license_id") and acc.get("licenseId") == result["license_id"]:
                        duplicate = True
                        existing_acc = acc
                        break
                    if acc.get("jwt") == result["jwt"]:
                        duplicate = True
                        existing_acc = acc
                        break

                # 活跃池未找到时，检查密钥表中是否存在该 license_id 绑定的密钥
                # （账号可能因额度耗尽已被删除，但 key 的 account_id 仍保留）
                # 优先返回未封禁的 key，找不到未封禁的才返回封禁的 key
                if not duplicate and result.get("license_id"):
                    first_banned = None
                    for k, v in VALID_CLIENT_KEYS.items():
                        if v.get("account_id") == result["license_id"]:
                            if not v.get("banned"):
                                duplicate = True
                                generated_key = k  # 优先使用未封禁的 key
                                break
                            elif first_banned is None:
                                first_banned = k  # 记录第一个封禁的 key 作为备选
                    if not duplicate and first_banned:
                        duplicate = True
                        generated_key = first_banned  # 只有封禁 key 时才返回

                if not duplicate:
                    # 检查额度是否满额（未使用过）
                    # ★ 先用 process_account 已知的 daily_total 作兜底（grazie-lite=10K，NC=300K）
                    # 这样即使 quota API 返回 404（grazie-lite JWT 不支持该端点），也不会误拒入池
                    if result.get("daily_total") and "daily_total" not in new_account:
                        new_account["daily_total"] = result["daily_total"]
                    yield f"data: {json.dumps({'type': 'log', 'msg': '正在检查账号额度...'})}\n\n"
                    await _check_quota(new_account)
                    # 若配额 API 和兜底值均无 daily_total → 拒绝入池
                    if "daily_total" not in new_account:
                        await _discard_preissued()
                        yield f"data: {json.dumps({'type': 'log', 'msg': '✗ 配额查询失败（API 暂时不可用），拒绝入池'})}\n\n"
                        task["status"] = "quota_check_failed"
                        yield f"data: {json.dumps({'type': 'done', 'status': 'quota_check_failed', 'result': result, 'generated_key': None, 'is_existing_key': False})}\n\n"
                        return
                    daily_used = new_account.get("daily_used", 0) or 0
                    daily_total = new_account.get("daily_total", 0) or 0
                    # daily_total=0 表示无限制套餐，允许入池；否则剩余须 ≥ 90% 才允许
                    MIN_REMAINING_PCT = 0.90
                    if daily_total > 0:
                        remaining_pct = (daily_total - daily_used) / daily_total
                        used_pct = round(daily_used / daily_total * 100, 1)
                        if remaining_pct < MIN_REMAINING_PCT:
                            await _discard_preissued()
                            yield f"data: {json.dumps({'type': 'log', 'msg': f'✗ 账号额度剩余不足 90%（已用 {used_pct}%，{daily_used:,}/{daily_total:,} tokens），拒绝入池'})}\n\n"
                            task["status"] = "quota_rejected"
                            yield f"data: {json.dumps({'type': 'done', 'status': 'quota_rejected', 'result': result, 'generated_key': None, 'is_existing_key': False})}\n\n"
                            return
                        yield f"data: {json.dumps({'type': 'log', 'msg': f'✓ 额度充足（剩余 {100-used_pct}%），允许入池'})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'log', 'msg': '✓ 无限制套餐，允许入池'})}\n\n"

                    async with account_rotation_lock:
                        JETBRAINS_ACCOUNTS.append(new_account)
                    await _save_account_to_db(new_account, pool=await _get_pool_for_task())
                    yield f"data: {json.dumps({'type': 'log', 'msg': '✓ 账号已自动添加到系统'})}\n\n"

                    # ★ 个人 key 模式复用已有 key；否则使用预签 key
                    generated_key = _personal_key or task.get("preissued_key", "")
                    new_acc_id = _account_id(new_account)
                    all_bound_ids: list = [new_acc_id]  # 收集全部绑定账号 ID

                    # ★ 额外账号：把 extra_accounts 也入池（不单独生成 key，统一绑定到预签 key）
                    extra_accounts_list = result.get("extra_accounts", [])
                    extra_added = 0
                    for extra_acc_data in extra_accounts_list:
                        extra_lid = extra_acc_data.get("license_id")
                        extra_jwt = extra_acc_data.get("jwt")
                        if not extra_lid or not extra_jwt:
                            continue
                        already_exists = any(
                            a.get("licenseId") == extra_lid or a.get("jwt") == extra_jwt
                            for a in JETBRAINS_ACCOUNTS
                        )
                        if already_exists:
                            continue
                        extra_account_obj: Dict[str, Any] = {
                            "jwt": extra_jwt,
                            "has_quota": True,
                            "last_updated": time.time(),
                            "licenseId": extra_lid,
                        }
                        if extra_acc_data.get("id_token"):
                            extra_account_obj["authorization"] = extra_acc_data["id_token"]
                        await _check_quota(extra_account_obj)
                        async with account_rotation_lock:
                            JETBRAINS_ACCOUNTS.append(extra_account_obj)
                        await _save_account_to_db(extra_account_obj, pool=await _get_pool_for_task())
                        all_bound_ids.append(extra_lid)
                        extra_added += 1

                    # ★ 保存 pending NC licenseId；已绑定 ≥ 4 个立即升额度，否则等重试任务补齐
                    NC_QUOTA_THRESHOLD = 4
                    pending_nc = result.get("pending_nc_lids", [])
                    if len(all_bound_ids) >= NC_QUOTA_THRESHOLD:
                        # 已绑定账号达到阈值，立即升级，剩余 pending 继续后台入池
                        _db3 = await _get_pool_for_task()
                        if _db3 and generated_key:
                            if _personal_key:
                                await _add_low_quota(_personal_key, all_bound_ids, _db3)
                            else:
                                await _activate_key_quota(generated_key, all_bound_ids, _db3)
                        if pending_nc:
                            try:
                                _db2 = await _get_pool_for_task()
                                if _db2:
                                    async with _db2.acquire() as _c:
                                        await _c.execute(
                                            "UPDATE jb_accounts SET pending_nc_lids=$1, "
                                            "pending_nc_email=$2, pending_nc_pass=$3, "
                                            "pending_nc_key=$4, pending_nc_bound_ids=$5, "
                                            "pending_nc_low_admin=$6, pending_nc_discord_id=$7, "
                                            "pending_nc_enqueued_at=$8 "
                                            "WHERE id=$9",
                                            json.dumps(pending_nc),
                                            task.get("email", ""), task.get("password", ""),
                                            generated_key, ",".join(all_bound_ids),
                                            bool(task.get("is_low_admin")),
                                            str(task.get("discord_user_id", "") or ""),
                                            time.time(),
                                            new_acc_id
                                        )
                            except Exception as _e:
                                yield f"data: {json.dumps({'type': 'log', 'msg': f'[WARN] 保存 pending NC 失败: {_e}'})}\n\n"
                        _q_show = _LOW_USER_KEY_QUOTA if task.get("is_low_admin") else _NORMAL_KEY_QUOTA
                        _new_lim = VALID_CLIENT_KEYS.get(generated_key, {}).get("usage_limit", _q_show)
                        _lim_msg = f"当前累计额度 {_new_lim}" if _personal_key else f"额度 {_q_show}"
                        yield f"data: {json.dumps({'type': 'log', 'msg': f'✓ 已为您激活专属密钥（{_lim_msg}）'})}\n\n"
                    elif pending_nc:
                        # 当前绑定不足阈值，保存 pending，等重试任务凑够后升级
                        try:
                            _db2 = await _get_pool_for_task()
                            if _db2:
                                async with _db2.acquire() as _c:
                                    await _c.execute(
                                        "UPDATE jb_accounts SET pending_nc_lids=$1, "
                                        "pending_nc_email=$2, pending_nc_pass=$3, "
                                        "pending_nc_key=$4, pending_nc_bound_ids=$5, "
                                        "pending_nc_low_admin=$6, pending_nc_discord_id=$7, "
                                        "pending_nc_enqueued_at=$8 "
                                        "WHERE id=$9",
                                        json.dumps(pending_nc),
                                        task.get("email", ""), task.get("password", ""),
                                        generated_key, ",".join(all_bound_ids),
                                        bool(task.get("is_low_admin")),
                                        str(task.get("discord_user_id", "") or ""),
                                        time.time(),
                                        new_acc_id
                                    )
                            yield f"data: {json.dumps({'type': 'log', 'msg': f'⏳ NC 许可证已创建并记录，约30-60分钟后后台自动入池'})}\n\n"
                            _q_show = _LOW_USER_KEY_QUOTA if task.get("is_low_admin") else _NORMAL_KEY_QUOTA
                            if _personal_key:
                                yield f"data: {json.dumps({'type': 'log', 'msg': f'⏳ 凭证到位后额度将自动累加到您的专属密钥（每账号 +{_q_show}）'})}\n\n"
                            else:
                                yield f"data: {json.dumps({'type': 'log', 'msg': f'⏳ 已为您生成专属 API 密钥（额度 0，凭证到位后自动升为 {_q_show}）：{generated_key}'})}\n\n"
                        except Exception as _e:
                            yield f"data: {json.dumps({'type': 'log', 'msg': f'[WARN] 保存 pending NC 失败: {_e}'})}\n\n"
                    else:
                        # 无 pending 且未达阈值（极少见）：直接升级
                        _db3 = await _get_pool_for_task()
                        if _db3 and generated_key:
                            if _personal_key:
                                await _add_low_quota(_personal_key, all_bound_ids, _db3)
                            else:
                                await _activate_key_quota(generated_key, all_bound_ids, _db3)
                        _q_show = _LOW_USER_KEY_QUOTA if task.get("is_low_admin") else _NORMAL_KEY_QUOTA
                        _new_lim = VALID_CLIENT_KEYS.get(generated_key, {}).get("usage_limit", _q_show)
                        _lim_msg = f"当前累计额度 {_new_lim}" if _personal_key else f"额度 {_q_show}"
                        yield f"data: {json.dumps({'type': 'log', 'msg': f'✓ 已为您激活专属密钥（{_lim_msg}）'})}\n\n"
                    task["stream_processed"] = True  # 幂等标记
                else:
                    # 账号重复处理
                    if _personal_key:
                        # 个人 key 模式：直接返回个人 key
                        generated_key = _personal_key
                        yield f"data: {json.dumps({'type': 'log', 'msg': '⚠ 账号已存在，将使用您的个人专属密钥'})}\n\n"
                    else:
                        # 清理无效预签 key，找回已绑定密钥
                        _preissued_dup = task.get("preissued_key", "")
                        if _preissued_dup and VALID_CLIENT_KEYS.get(_preissued_dup, {}).get("usage_limit", 1) == 0:
                            VALID_CLIENT_KEYS.pop(_preissued_dup, None)
                            try:
                                _dup_db = await _get_pool_for_task()
                                if _dup_db:
                                    async with _dup_db.acquire() as _dc:
                                        await _dc.execute("DELETE FROM jb_client_keys WHERE key=$1", _preissued_dup)
                            except Exception:
                                pass
                        if generated_key:
                            yield f"data: {json.dumps({'type': 'log', 'msg': '⚠ 检测到该账号曾经激活（已从池中删除），已为您找回绑定的 API 密钥'})}\n\n"
                        elif existing_acc:
                            acc_id = _account_id(existing_acc)
                            existing_keys_found = [
                                k for k, v in VALID_CLIENT_KEYS.items()
                                if v.get("account_id") == acc_id
                            ]
                            if existing_keys_found:
                                generated_key = existing_keys_found[0]
                                yield f"data: {json.dumps({'type': 'log', 'msg': '⚠ 账号已存在（在活跃池中），已为您找回绑定的 API 密钥'})}\n\n"
                            else:
                                yield f"data: {json.dumps({'type': 'log', 'msg': '正在检查账号额度...'})}\n\n"
                                await _check_quota(existing_acc)
                                ex_daily_used = existing_acc.get("daily_used", 0) or 0
                                ex_daily_total = existing_acc.get("daily_total", 0) or 0
                                MIN_REMAINING_PCT = 0.90
                                if ex_daily_total > 0:
                                    ex_remaining_pct = (ex_daily_total - ex_daily_used) / ex_daily_total
                                    ex_used_pct = round(ex_daily_used / ex_daily_total * 100, 1)
                                    if ex_remaining_pct < MIN_REMAINING_PCT:
                                        yield f"data: {json.dumps({'type': 'log', 'msg': f'✗ 账号额度剩余不足 90%（已用 {ex_used_pct}%，{ex_daily_used:,}/{ex_daily_total:,} tokens），拒绝入池'})}\n\n"
                                        task["status"] = "quota_rejected"
                                        yield f"data: {json.dumps({'type': 'done', 'status': 'quota_rejected', 'result': result, 'generated_key': None, 'is_existing_key': False})}\n\n"
                                        return
                                    yield f"data: {json.dumps({'type': 'log', 'msg': f'✓ 额度充足（剩余 {100-ex_used_pct}%），允许入池'})}\n\n"
                                else:
                                    yield f"data: {json.dumps({'type': 'log', 'msg': '✓ 无限制套餐，允许入池'})}\n\n"
                                generated_key = task.get("preissued_key", "")
                                _dup_db2 = await _get_pool_for_task()
                                if _dup_db2 and generated_key:
                                    await _activate_key_quota(generated_key, [acc_id], _dup_db2)
                                _q_show = _LOW_USER_KEY_QUOTA if task.get("is_low_admin") else _NORMAL_KEY_QUOTA
                                yield f"data: {json.dumps({'type': 'log', 'msg': f'✓ 已激活专属 API 密钥（额度 {_q_show}）'})}\n\n"
                        else:
                            yield f"data: {json.dumps({'type': 'log', 'msg': '⚠ 账号已存在，但未找到绑定密钥（请联系管理员）'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'log', 'msg': f'[WARN] 后处理失败: {e}'})}\n\n"

        # ★ 全部 pending（NC 已创建但尚未被 Grazie 信任，无立即可用 JWT）
        elif (result.get("pending_nc_lids") and not result.get("jwt")
              and task["status"] == "success"):
            # 有个人 key → 使用个人 key；否则使用预签 key
            generated_key = _personal_key or task.get("preissued_key", "")
            try:
                pending_nc = result["pending_nc_lids"]
                _db_p = await _get_pool_for_task()
                if _db_p and pending_nc:
                    # 以第一个 pending lid 作为占位 id 存入 DB，供后台重试任务使用
                    placeholder_id = pending_nc[0]
                    async with _db_p.acquire() as _cp:
                        await _cp.execute(
                            """
                            INSERT INTO jb_accounts
                                (id, license_id, auth_token, jwt, last_updated,
                                 last_quota_check, has_quota, daily_total, daily_used,
                                 pending_nc_lids, pending_nc_email, pending_nc_pass,
                                 pending_nc_key, pending_nc_bound_ids, pending_nc_low_admin,
                                 pending_nc_discord_id, pending_nc_enqueued_at)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
                            ON CONFLICT (id) DO UPDATE SET
                                pending_nc_lids       = EXCLUDED.pending_nc_lids,
                                pending_nc_email      = EXCLUDED.pending_nc_email,
                                pending_nc_pass       = EXCLUDED.pending_nc_pass,
                                pending_nc_key        = EXCLUDED.pending_nc_key,
                                pending_nc_bound_ids  = EXCLUDED.pending_nc_bound_ids,
                                pending_nc_low_admin  = EXCLUDED.pending_nc_low_admin,
                                pending_nc_discord_id = EXCLUDED.pending_nc_discord_id,
                                pending_nc_enqueued_at = EXCLUDED.pending_nc_enqueued_at
                            """,
                            placeholder_id,
                            placeholder_id,
                            result.get("id_token", ""),
                            None,          # jwt = NULL（待信任后填入）
                            time.time(),
                            0.0,
                            False,
                            0,             # daily_total = 0（待升级）
                            0,
                            json.dumps(pending_nc),
                            task.get("email", ""),
                            task.get("password", ""),
                            generated_key,   # 个人 key 或 preissued_key
                            "",            # bound_ids 为空（全部待激活）
                            bool(task.get("is_low_admin")),
                            str(task.get("discord_user_id", "") or ""),
                            time.time(),   # pending_nc_enqueued_at：入队瞬间记录
                        )
                _q_show = _LOW_USER_KEY_QUOTA if task.get("is_low_admin") else _NORMAL_KEY_QUOTA
                yield f"data: {json.dumps({'type': 'log', 'msg': f'⏳ NC 许可证已创建并记录（共 {len(pending_nc)} 个），全部凭证到位后自动激活密钥（额度 {_q_show}）'})}\n\n"
                if _personal_key:
                    yield f"data: {json.dumps({'type': 'log', 'msg': f'⏳ 凭证到位后额度将自动累加到您的专属密钥（每账号 +{_q_show}）'})}\n\n"
                elif generated_key:
                    yield f"data: {json.dumps({'type': 'log', 'msg': f'⏳ 您的密钥（当前额度 0，激活后为 {_q_show}）：{generated_key}'})}\n\n"
                yield f"data: {json.dumps({'type': 'log', 'msg': f'pending licenseId: {pending_nc}'})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'type': 'log', 'msg': f'[WARN] 保存 pending 记录失败: {e}'})}\n\n"

        # 兜底：任务失败时（status=failed/exception）清除未用的预签 key
        if task["status"] not in ("success",):
            await _discard_preissued()

        yield f"data: {json.dumps({'type': 'done', 'status': task['status'], 'result': result, 'generated_key': generated_key, 'is_existing_key': duplicate})}\n\n"

    return StreamingResponse(
        _sse_with_keepalive(event_generator()),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )

@app.get("/admin/activate/{task_id}")
async def admin_activate_status(task_id: str):
    """查询激活任务状态"""
    if task_id not in _activate_tasks:
        raise HTTPException(status_code=404, detail="任务不存在")
    task = _activate_tasks[task_id]
    return {
        "task_id": task_id,
        "email": task["email"],
        "status": task["status"],
        "logs": task["logs"],
        "result": task["result"] if task["status"] != "running" else None,
    }


class DirectJwtRequest(BaseModel):
    email: str
    jwt: str
    license_id: Optional[str] = ""
    auth_token: Optional[str] = ""


@app.post("/admin/activate/direct-jwt")
async def admin_direct_jwt(req: DirectJwtRequest):
    """直接导入 JWT —— 跳过自动激活，由用户粘贴从 IDE/浏览器获取的 Grazie JWT。
    系统会先验证 JWT 是否有效（调用 quota/get），有效则存入账号池。"""
    # 验证 JWT 有效性
    test_headers = {
        "User-Agent": "ktor-client",
        "Content-Length": "0",
        "Accept-Charset": "UTF-8",
        "grazie-agent": '{"name":"aia:pycharm","version":"251.26094.80.13:251.26094.141"}',
        "grazie-authenticate-jwt": req.jwt,
    }
    try:
        resp = await http_client.post(
            "https://api.jetbrains.ai/user/v5/quota/get",
            headers=test_headers, timeout=15.0
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"无法连接 JetBrains AI 验证服务: {e}")

    if resp.status_code == 401:
        raise HTTPException(status_code=400, detail="JWT 无效或已过期（401），请重新获取后再导入")
    if resp.status_code not in (200, 204):
        raise HTTPException(status_code=400, detail=f"JWT 验证失败（HTTP {resp.status_code}）: {resp.text[:200]}")

    # JWT 有效，提取配额信息
    quota_data = {}
    daily_total = 300000
    try:
        quota_data = resp.json()
        quota_obj = quota_data.get("current", {})
        tariff = quota_obj.get("tariffQuota", {})
        daily_total = tariff.get("total", 300000)
    except Exception:
        pass

    # 构建账号对象
    new_account: Dict[str, Any] = {
        "jwt": req.jwt,
        "has_quota": True,
        "last_updated": time.time(),
        "daily_total": daily_total,
        "daily_used": 0,
        "quota_status_reason": "direct_import",
    }
    if req.email:
        new_account["email"] = req.email
    if req.license_id:
        new_account["licenseId"] = req.license_id
    if req.auth_token:
        new_account["authorization"] = req.auth_token

    # 检查是否重复
    for acc in JETBRAINS_ACCOUNTS:
        if req.email and acc.get("email") == req.email:
            # 更新现有账号的 JWT
            acc.update(new_account)
            _admin_cache_invalidate("status")
            try:
                await _save_account_to_db(acc)
            except Exception:
                pass
            return {"status": "updated", "email": req.email, "daily_total": daily_total}

    # 新账号
    JETBRAINS_ACCOUNTS.append(new_account)
    _admin_cache_invalidate("status")
    try:
        await _save_account_to_db(new_account)
    except Exception:
        pass

    return {"status": "added", "email": req.email or "(无邮箱)", "daily_total": daily_total}


# ==================== 自助绑卡（公开端点）====================

async def _get_client_cfg_for_push() -> dict | None:
    pool = await _get_db_pool()
    if not pool:
        return None
    async with pool.acquire() as conn:
        return await _get_client_config(conn)


def _get_secondary_push_cfg() -> dict | None:
    """读取第二对端配置（仅从环境变量：PARTNER2_ENDPOINT / PARTNER2_ID / PARTNER2_HMAC_SECRET）。
    结果在进程内缓存，AES 加密只在首次调用时执行一次。"""
    global _secondary_push_cfg_cache
    if _secondary_push_cfg_cache is not _UNSET:
        return _secondary_push_cfg_cache  # type: ignore[return-value]
    ep = os.environ.get("PARTNER2_ENDPOINT", "").strip()
    pid = os.environ.get("PARTNER2_ID", "").strip()
    raw_secret = os.environ.get("PARTNER2_HMAC_SECRET", "").strip()
    if not ep or not pid or not raw_secret:
        _secondary_push_cfg_cache = None
        return None
    hmac_secret_enc = _partner_encrypt(raw_secret)
    if not hmac_secret_enc:
        _secondary_push_cfg_cache = None
        return None
    _secondary_push_cfg_cache = {"endpoint": ep, "partner_id": pid, "hmac_secret_enc": hmac_secret_enc}
    return _secondary_push_cfg_cache  # type: ignore[return-value]


async def _get_push_cfg_for_self_register() -> dict | None:
    """自助绑卡专用：在主配置和第二配置之间随机均衡。
    两个配置均可用时各 50% 概率；只有一个可用时直接返回该配置。"""
    primary = await _get_client_cfg_for_push()
    secondary = _get_secondary_push_cfg()

    primary_ok = bool(primary and primary.get("endpoint") and primary.get("partner_id") and primary.get("hmac_secret_enc"))
    secondary_ok = bool(secondary)

    if primary_ok and secondary_ok:
        return primary if _random.random() < 0.5 else secondary
    if primary_ok:
        return primary
    if secondary_ok:
        return secondary
    return None


async def _import_partner_credential(cred: dict) -> str | None:
    """将合作方单条凭证（license_id / jwt / auth_token）写入 jb_accounts 和内存池，返回 acc_id 或 None"""
    license_id = cred.get("license_id") or ""
    jwt_val = cred.get("jwt") or ""
    auth_token = cred.get("auth_token") or ""
    if not jwt_val:
        return None
    acc_id = license_id if license_id else ("jwt:" + hashlib.sha256(jwt_val.encode()).hexdigest()[:32])

    # 快速去重：如果内存池中已存在该账号且 JWT 完全一致，直接返回，
    # 避免对端重复发送旧参数时触发多余的 DB 写入和配额检测（防 429 风暴）
    existing = next((a for a in JETBRAINS_ACCOUNTS if _account_id(a) == acc_id), None)
    if existing and existing.get("jwt", "") == jwt_val:
        return acc_id

    pool = await _get_db_pool()
    if not pool:
        return None
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO jb_accounts (id, license_id, auth_token, jwt, last_updated, last_quota_check, has_quota)
               VALUES ($1,$2,$3,$4,$5,$6,$7)
               ON CONFLICT (id) DO UPDATE SET
                 auth_token=EXCLUDED.auth_token, jwt=EXCLUDED.jwt, last_updated=EXCLUDED.last_updated""",
            acc_id, license_id, auth_token, jwt_val, time.time(), 0, True
        )
    # 同步更新内存池（DB 写入后立刻对轮询可见）
    if existing:
        existing["jwt"] = jwt_val
        existing["last_updated"] = time.time()
        if auth_token:
            existing["authorization"] = auth_token
        existing["has_quota"] = True
    else:
        new_acc: dict = {
            "jwt": jwt_val,
            "last_updated": time.time(),
            "has_quota": True,
            "last_quota_check": 0,
            "daily_used": None,
            "daily_total": None,
        }
        if license_id:
            new_acc["licenseId"] = license_id
        if auth_token:
            new_acc["authorization"] = auth_token
        async with account_rotation_lock:
            JETBRAINS_ACCOUNTS.append(new_acc)
    # JWT 有变化（新账号或更新）才安排配额检测，避免重复导入触发 429
    _schedule_quota_checks_for_ids({acc_id}, label="partner凭证入池检测")
    return acc_id


async def _run_partner_credentials_poll() -> dict:
    """一次性拉取合作方新增凭证（游标分页），写入 jb_accounts。

    安全启动策略：
    - 正常情况下从 DB 中持久化的 poll_cursor 继续拉取；
    - 如果 poll_cursor 缺失（例如清库/新库首次启动），绝不能从 0 开始导入历史凭证，
      否则会把合作方很久以前的全部凭证重新拉入本实例；
    - 此时将游标 bootstrap 到当前时间戳（毫秒）并跳过本轮，后续只拉取此刻之后的新凭证。
      合作方 poll 游标按时间/自增位置单调推进；即使远端无新增，本地也会持久化该基线。
    """
    cfg = await _get_client_cfg_for_push()
    if not cfg or not cfg.get("endpoint") or not cfg.get("hmac_secret_enc"):
        return {"skipped": True, "reason": "配置不完整"}
    _ep_parsed = urlparse(cfg["endpoint"]); base_url = f"{_ep_parsed.scheme}://{_ep_parsed.netloc}"
    pool = await _get_db_pool()
    if not pool:
        return {"skipped": True, "reason": "DB不可用"}
    async with pool.acquire() as conn:
        cursor_row = await conn.fetchrow("SELECT value FROM partner_client_config WHERE key='poll_cursor'")
        if not cursor_row or not str(cursor_row["value"] or "").isdigit():
            bootstrap_cursor = int(time.time() * 1000)
            await conn.execute(
                "INSERT INTO partner_client_config (key,value) VALUES ('poll_cursor',$1) "
                "ON CONFLICT (key) DO UPDATE SET value=$1",
                str(bootstrap_cursor),
            )
            print(
                f"[partner_poll] poll_cursor 缺失/无效，已初始化为当前时间 {bootstrap_cursor}，"
                "本轮跳过历史凭证导入"
            )
            return {
                "skipped": True,
                "reason": "poll_cursor missing; bootstrapped to current time",
                "cursor": bootstrap_cursor,
            }
        raw_cursor = str(cursor_row["value"])
    cursor = int(raw_cursor)
    imported = 0
    while True:
        body_obj = {"cursor": cursor, "limit": 200}
        body_bytes = json.dumps(body_obj, separators=(",", ":")).encode()
        path = "/api/partner/contribute/credentials/poll"
        headers = _partner_headers(cfg, "POST", path, body_bytes)
        try:
            resp = await http_client.post(f"{base_url}{path}", content=body_bytes, headers=headers, timeout=20)
            print(f"[partner_poll] credentials/poll HTTP {resp.status_code}, cursor={cursor}")
            if resp.status_code != 200:
                print(f"[partner_poll] 非200响应: {resp.text[:200]}")
                return {"error": f"HTTP {resp.status_code}", "imported": imported}
            data = resp.json()
            print(f"[partner_poll] 返回 credentials={len(data.get('credentials',[]))}, has_more={data.get('has_more')}, next_cursor={data.get('next_cursor')}")
        except Exception as e:
            print(f"[partner_poll] 请求异常: {e}")
            return {"error": str(e), "imported": imported}
        for cred in data.get("credentials", []):
            acc_id = await _import_partner_credential(cred)
            if acc_id:
                imported += 1
                email = cred.get("email")
                if email:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """UPDATE account_contributions SET status='active', updated_at_ms=$1
                               WHERE email=$2 AND status NOT IN ('active','retired')""",
                            _ms(), email
                        )
                    # 同步更新 self_register_jobs：处理新流程（processing→active）和补绑旧流程（active补绑）
                    async with pool.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT status, result_keys FROM self_register_jobs WHERE email=$1", email
                        )
                    if row:
                        st = row["status"]
                        if st in ("processing", "failed"):
                            # 凭证到位（含 bind_task 超时后迟到的情况）：激活预签发 key 的额度
                            api_key = (row["result_keys"] or "").strip()
                            if api_key and await _activate_key_quota(api_key, [acc_id], pool):
                                async with pool.acquire() as conn:
                                    await conn.execute(
                                        "UPDATE self_register_jobs SET status='active', error_msg='' WHERE email=$1",
                                        email
                                    )
                                late = " (迟到恢复)" if st == "failed" else ""
                                print(f"[partner_poll] {email} 凭证到位{late}，key {api_key[:16]}… 额度已激活，绑定账号 {acc_id}")
                            else:
                                # 兜底：预签 key 丢失，重新生成
                                api_key = f"sk-jb-{secrets.token_hex(24)}"
                                key_meta: dict = {"usage_limit": 25, "usage_count": 0, "account_id": acc_id}
                                VALID_CLIENT_KEYS[api_key] = key_meta
                                await _upsert_key_to_db(api_key, key_meta)
                                async with pool.acquire() as conn:
                                    await conn.execute(
                                        "UPDATE self_register_jobs SET status='active', result_keys=$2, error_msg='' WHERE email=$1",
                                        email, api_key
                                    )
                                print(f"[partner_poll] {email} 预签 key 丢失，兜底重发 key {api_key[:16]}…")
        next_cursor = data.get("next_cursor") or cursor
        # 每页处理完立即持久化 cursor，避免服务重启后从旧位置重拉历史凭证
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO partner_client_config (key,value) VALUES ('poll_cursor',$1) ON CONFLICT (key) DO UPDATE SET value=$1",
                str(next_cursor)
            )
        if not data.get("has_more", False):
            break
        cursor = next_cursor
    return {"success": True, "imported": imported}


async def _partner_credentials_poller():
    """后台任务：每 60 秒向合作方拉取一次最新凭证写入 jb_accounts"""
    await asyncio.sleep(15)
    while True:
        try:
            result = await _run_partner_credentials_poll()
            n = result.get("imported", 0)
            skipped = result.get("skipped", False)
            err = result.get("error")
            if skipped:
                print(f"[partner_poll] 跳过: {result.get('reason', '未知')}")
            elif err:
                print(f"[partner_poll] 请求异常: {err}")
            else:
                print(f"[partner_poll] 本轮完成，导入 {n} 条凭证")
        except Exception as e:
            print(f"[partner_poll] 轮询异常: {e}")
        await asyncio.sleep(60)


def _partner_headers(cfg: dict, method: str, path: str, body: bytes) -> dict:
    """生成合作方 HMAC 请求头（HMAC secret 解密结果在进程内缓存）"""
    ts_str = str(int(time.time()))
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = f"{ts_str}\n{method}\n{path}\n{body_hash}"
    hmac_secret = _partner_decrypt_cached(cfg["hmac_secret_enc"])
    sig = _hmac_mod.new(hmac_secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    return {
        "X-Partner-Key-Id": cfg["partner_id"],
        "X-Partner-Timestamp": ts_str,
        "X-Partner-Signature": sig,
        "Content-Type": "application/json",
    }


@app.post("/key/self-register")
async def key_self_register(request: Request):
    """自助绑卡：用户提交 JB 账密，自动推送激活并返回处理结果"""
    body = await request.json()
    email = str(body.get("email", "")).strip().lower()
    password = str(body.get("password", "")).strip()
    discord_token = str(body.get("discord_token", "")).strip()

    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="邮箱格式不正确")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="密码过短")

    # ── Discord 验证 & 每日限额检查 ──
    if not discord_token or discord_token not in _DISCORD_VERIFIED:
        raise HTTPException(status_code=401, detail="Discord 验证无效或已过期，请重新授权")
    discord_info = _DISCORD_VERIFIED[discord_token]
    if time.time() - discord_info["ts"] > 1800:
        _DISCORD_VERIFIED.pop(discord_token, None)
        raise HTTPException(status_code=401, detail="Discord 验证已过期，请重新授权")
    discord_user_id = discord_info.get("user_id", "")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="服务暂时不可用，请稍后重试")

    # ── 步骤 1：先查现有记录，active/processing 直接返回（无需占用名额）──
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT status, result_keys FROM self_register_jobs WHERE email=$1", email
        )
    if existing:
        st = existing["status"]
        if st == "active":
            keys = [k for k in (existing["result_keys"] or "").split(",") if k.strip()]
            return {"status": "active", "keys": keys}
        if st in ("pending", "processing"):
            pre_key = (existing["result_keys"] or "").strip()
            return {"status": "processing", "key": pre_key, "message": "您的账号正在处理中，凭证确认后即可使用"}
        # st == "failed" → 允许重新提交，继续往下走

    # ── 步骤 2：每日限额 + 并发防刷 ──
    # 持锁范围仅为 DB 查询（约 50ms），不影响 precheck 吞吐。
    # inflight 计数器防止同一用户并发提交绕过限额。
    _DAILY_LIMIT = 5
    if discord_user_id:
        # 锁表过大时修剪（移除不在 inflight 中的条目），防无限增长
        if len(_self_register_user_locks) >= _SELF_REGISTER_LOCKS_MAX:
            stale = [k for k in list(_self_register_user_locks) if _self_register_inflight.get(k, 0) == 0]
            for k in stale[:max(1, len(stale) // 2)]:
                _self_register_user_locks.pop(k, None)
        user_lock = _self_register_user_locks.setdefault(discord_user_id, asyncio.Lock())
    else:
        user_lock = None

    if user_lock:
        async with user_lock:
            inflight = _self_register_inflight.get(discord_user_id, 0)
            async with pool.acquire() as conn:
                today_count = await conn.fetchval(
                    """SELECT COUNT(*) FROM self_register_jobs
                       WHERE discord_user_id=$1
                         AND status IN ('processing', 'active')
                         AND updated_at >= CURRENT_DATE
                         AND updated_at <  CURRENT_DATE + INTERVAL '1 day'""",
                    discord_user_id
                ) or 0
            if today_count + inflight >= _DAILY_LIMIT:
                raise HTTPException(
                    status_code=429,
                    detail=f"您今天已领取 {today_count + inflight} 个 key（含处理中），"
                           f"每个 Discord 账号每天最多可领取 {_DAILY_LIMIT} 个，请明天再试",
                )
            _self_register_inflight[discord_user_id] = inflight + 1

    try:
        # ── 步骤 3：获取合作方配置（两个对端均衡） ──
        cfg = await _get_push_cfg_for_self_register()
        if not cfg or not cfg.get("endpoint") or not cfg.get("partner_id") or not cfg.get("hmac_secret_enc"):
            raise HTTPException(status_code=503, detail="服务暂未开放，请联系管理员")

        # ── 步骤 4：推送到合作方（precheck，最多 60s）──
        submit_path = "/api/partner/contribute/submit"
        req_body = json.dumps({
            "idempotency_key": str(uuid.uuid4()),
            "activation_mode": "immediate",
            "accounts": [{"email": email, "password": password}],
        }, ensure_ascii=False).encode()

        _ep = cfg["endpoint"]
        _parsed = urlparse(_ep)
        base_url = f"{_parsed.scheme}://{_parsed.netloc}"
        submit_url = f"{base_url}{submit_path}"

        try:
            headers = _partner_headers(cfg, "POST", submit_path, req_body)
            # precheck 单账号最多约 25s，留 60s 余量
            resp = await http_client.post(submit_url, content=req_body, headers=headers, timeout=60)
            resp_data = resp.json()
        except Exception:
            raise HTTPException(status_code=502, detail="系统繁忙，请稍后重试")

        if resp.status_code != 200:
            raise HTTPException(status_code=502, detail="系统繁忙，请稍后重试")

        results = resp_data.get("results", [])
        acc_result = next((r for r in results if r.get("email") == email), None)
        if acc_result and acc_result.get("status") == "rejected":
            reason = acc_result.get("reason", "")
            precheck_result = acc_result.get("precheck_result", "")

            if "duplicate" in reason.lower():
                # 账号曾提交过，检查库里状态
                async with pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT status, result_keys FROM self_register_jobs WHERE email=$1", email)
                if row:
                    if row["status"] == "active" and (row["result_keys"] or "").strip():
                        return {"status": "active", "keys": [row["result_keys"].strip()]}
                    if row["status"] in ("pending", "processing"):
                        pre_key = (row["result_keys"] or "").strip()
                        return {"status": "processing", "key": pre_key, "message": "您的账号正在处理中，凭证确认后即可使用"}
                # 无记录或已失败 → 继续进入处理流程

            elif precheck_result:
                detail = _PRECHECK_MESSAGES.get(precheck_result, f"账号验证不通过（{precheck_result}）")
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO self_register_jobs (email, status, error_msg, discord_user_id) VALUES ($1,'failed',$2,$3) "
                        "ON CONFLICT (email) DO UPDATE SET status='failed', error_msg=$2, discord_user_id=$3, updated_at=NOW()",
                        email, detail, discord_user_id
                    )
                raise HTTPException(status_code=400, detail=detail)

            else:
                detail = "账号或密码校验失败，请检查后重试"
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO self_register_jobs (email, status, error_msg, discord_user_id) VALUES ($1,'failed',$2,$3) "
                        "ON CONFLICT (email) DO UPDATE SET status='failed', error_msg=$2, discord_user_id=$3, updated_at=NOW()",
                        email, detail, discord_user_id
                    )
                raise HTTPException(status_code=400, detail=detail)

        # ── 步骤 5：合作方已接受，预签发 key（额度=0，凭证确认后升为 25）──
        api_key = f"sk-jb-{secrets.token_hex(24)}"
        key_meta: dict = {"usage_limit": 0, "usage_count": 0, "account_id": ""}
        VALID_CLIENT_KEYS[api_key] = key_meta
        await _upsert_key_to_db(api_key, key_meta)

        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO self_register_jobs (email, status, result_keys, error_msg, discord_user_id) VALUES ($1,'processing',$2,'',$3) "
                "ON CONFLICT (email) DO UPDATE SET status='processing', result_keys=$2, error_msg='', discord_user_id=$3, updated_at=NOW()",
                email, api_key, discord_user_id
            )

        asyncio.create_task(_bind_accounts_for_key(email, cfg))
        return {"status": "processing", "key": api_key, "message": "账号已提交，凭证确认后 key 即可使用"}

    finally:
        # 释放 inflight 预占名额（成功后由 DB 记录接管；失败则归还名额供下次使用）
        if user_lock and discord_user_id:
            cnt = _self_register_inflight.get(discord_user_id, 0)
            if cnt <= 1:
                _self_register_inflight.pop(discord_user_id, None)
            else:
                _self_register_inflight[discord_user_id] = cnt - 1


async def _activate_key_quota(api_key: str, acc_ids: list, pool) -> bool:
    """激活 processing 阶段预签发的 key：绑定账号 ID，将 usage_limit 从 0 升到对应等级配额。
    普通密钥升至 25；LOW_ADMIN 用户预签的密钥升至 16。"""
    if not api_key:
        return False
    if api_key not in VALID_CLIENT_KEYS:
        # 重启后内存里没有，从 DB 补加
        async with pool.acquire() as c:
            row = await c.fetchrow(
                "SELECT usage_limit, usage_count, account_id, banned, is_low_admin_key FROM jb_client_keys WHERE key=$1", api_key
            )
        if not row:
            return False
        VALID_CLIENT_KEYS[api_key] = {
            "usage_limit": row["usage_limit"],
            "usage_count": row["usage_count"],
            "account_id": row["account_id"] or "",
            "banned": bool(row["banned"]),
            "is_low_admin_key": bool(row["is_low_admin_key"]) if row["is_low_admin_key"] is not None else False,
        }
    bound_ids = ",".join(acc_ids)
    target_quota = _LOW_USER_KEY_QUOTA if VALID_CLIENT_KEYS[api_key].get("is_low_admin_key") else _NORMAL_KEY_QUOTA
    VALID_CLIENT_KEYS[api_key]["usage_limit"] = target_quota
    VALID_CLIENT_KEYS[api_key]["account_id"] = bound_ids
    VALID_CLIENT_KEYS[api_key]["is_nc_key"] = True  # 持久化标记：NC key 升级后依然保留标识
    await _upsert_key_to_db(api_key, VALID_CLIENT_KEYS[api_key])
    return True


def _get_low_personal_key(discord_id: str) -> str:
    """从内存中查找 LOW 用户的个人专属密钥（按 low_admin_discord_id 匹配）。
    返回 key 字符串，未找到返回空字符串。

    优先级（避免命中预签 key 等孤儿）：
      1. usage_count > 0（已被使用过的 key 更可能是真个人 key）
      2. usage_limit > 0（有真实额度）
      3. 没有上述特征则按字典顺序兜底
    """
    if not discord_id:
        return ""
    candidates = [
        (k, v) for k, v in VALID_CLIENT_KEYS.items()
        if v.get("low_admin_discord_id") == discord_id
    ]
    if not candidates:
        return ""
    candidates.sort(key=lambda kv: (
        -int(kv[1].get("usage_count") or 0),
        -int(kv[1].get("usage_limit") or 0),
    ))
    return candidates[0][0]


async def _add_low_quota(api_key: str, new_acc_ids: list, pool) -> int:
    """为 LOW 用户个人 key 累加激活配额（每成功激活一个账号 += _LOW_USER_KEY_QUOTA）。
    返回累加后的新 usage_limit。"""
    if not api_key or api_key not in VALID_CLIENT_KEYS:
        return 0
    meta = VALID_CLIENT_KEYS[api_key]
    current = meta.get("usage_limit") or 0
    new_quota = current + _LOW_USER_KEY_QUOTA
    # 合并 account_id（去重）
    existing_ids = [x for x in (meta.get("account_id") or "").split(",") if x]
    all_ids = list(dict.fromkeys(existing_ids + [i for i in new_acc_ids if i]))
    VALID_CLIENT_KEYS[api_key]["usage_limit"] = new_quota
    VALID_CLIENT_KEYS[api_key]["account_id"] = ",".join(all_ids)
    VALID_CLIENT_KEYS[api_key]["is_nc_key"] = True
    await _upsert_key_to_db(api_key, VALID_CLIENT_KEYS[api_key])
    # 同步审计行（防回滚导入）
    discord_id = str(meta.get("low_admin_discord_id") or "")
    if discord_id:
        await _low_audit_save(
            discord_id,
            usage_limit=new_quota,
            usage_count=int(meta.get("usage_count") or 0),
        )
    return new_quota


async def _bind_accounts_for_key(email: str, cfg: dict, max_tries: int = 120, interval: int = 10):
    """后台轮询合作方账号状态。
    凭证确认后将预签发 key 的额度从 0 升为 25，并将状态更新为 active。
    未能完成则将状态更新为 failed。
    """
    global _running_bind_tasks
    if email in _running_bind_tasks:
        print(f"[bind_task] {email} 已有任务在运行，跳过重复启动")
        return
    _running_bind_tasks.add(email)

    pool = await _get_db_pool()
    if not pool:
        _running_bind_tasks.discard(email)
        return

    async def _mark_failed(msg: str):
        try:
            async with pool.acquire() as c:
                row = await c.fetchrow(
                    "SELECT result_keys FROM self_register_jobs WHERE email=$1", email
                )
                preissued_key = (row["result_keys"] or "").strip() if row else ""
                await c.execute(
                    "UPDATE self_register_jobs SET status='failed', error_msg=$2 WHERE email=$1",
                    email, msg
                )
                if preissued_key:
                    await c.execute(
                        "DELETE FROM jb_client_keys WHERE key=$1 AND usage_limit=0",
                        preissued_key
                    )
                    VALID_CLIENT_KEYS.pop(preissued_key, None)
                    print(f"[bind_task] {email} 失败，已删除预签 key {preissued_key[:16]}…")
        except Exception as exc:
            print(f"[bind_task] _mark_failed DB 异常: {exc}")

    async def _get_preissued_key() -> str:
        """从 DB 取出预签发的 key（processing 阶段写入的）"""
        try:
            async with pool.acquire() as c:
                row = await c.fetchrow(
                    "SELECT result_keys FROM self_register_jobs WHERE email=$1", email
                )
            return (row["result_keys"] or "").strip() if row else ""
        except Exception as exc:
            print(f"[bind_task] 读取预签 key 异常: {exc}")
            return ""

    _ep_parsed = urlparse(cfg["endpoint"]); base_url = f"{_ep_parsed.scheme}://{_ep_parsed.netloc}"
    path_with_qs = f"/api/partner/contribute/account?email={_urlquote(email, safe='')}"

    print(f"[bind_task] {email} 开始轮询，最多 {max_tries} 次，间隔 {interval}s")
    active_no_cred_retries = 0
    active_no_cred_max = 5
    try:
        for attempt in range(max_tries):
            await asyncio.sleep(interval)
            # 每轮先确认 job 仍在 processing，否则提前退出（防止重复任务无限续跑）
            try:
                async with pool.acquire() as _sc:
                    _job_row = await _sc.fetchrow(
                        "SELECT status FROM self_register_jobs WHERE email=$1", email
                    )
                _job_st = (_job_row["status"] if _job_row else None)
                if _job_st not in ("processing", "pending", None):
                    print(f"[bind_task] {email} job 状态已变为 {_job_st!r}，终止轮询")
                    return
            except Exception as _sc_exc:
                print(f"[bind_task] {email} 状态检查异常（继续）: {_sc_exc}")
            try:
                headers = _partner_headers(cfg, "GET", path_with_qs, b"")
                resp = await http_client.get(
                    f"{base_url}{path_with_qs}",
                    headers={k: v for k, v in headers.items() if k != "Content-Type"},
                    timeout=15,
                )
                print(f"[bind_task] {email} 第{attempt+1}次 HTTP {resp.status_code}")
                if resp.status_code != 200:
                    continue
                data = resp.json()
            except Exception as exc:
                print(f"[bind_task] {email} 第{attempt+1}次请求异常: {exc}")
                continue

            account = data.get("account") or data
            status = account.get("status", "")
            cred_count = data.get("credentials_count", 0) or len(data.get("credentials") or [])
            print(f"[bind_task] {email} 第{attempt+1}次 status={status!r}, credentials_count={cred_count}")

            if status in ("failed", "rejected", "error"):
                err_msg = account.get("activation_error") or account.get("error") or "账号激活失败，请重新提交"
                print(f"[bind_task] {email} 账号激活失败: {err_msg}")
                await _mark_failed(err_msg)
                return

            if cred_count > 0 or status == "active":
                credentials = data.get("credentials") or account.get("credentials") or []
                print(f"[bind_task] {email} 尝试导入 {len(credentials)} 条凭证 (status={status!r})")
                acc_ids = []
                for cred in credentials:
                    acc_id = await _import_partner_credential(cred)
                    if acc_id:
                        acc_ids.append(acc_id)

                if acc_ids:
                    api_key = await _get_preissued_key()
                    if api_key and await _activate_key_quota(api_key, acc_ids, pool):
                        async with pool.acquire() as c:
                            await c.execute(
                                "UPDATE self_register_jobs SET status='active', error_msg='' WHERE email=$1",
                                email
                            )
                        print(f"[bind_task] {email} 凭证确认，key {api_key[:16]}… 额度已激活，绑定 {len(acc_ids)} 个账号")
                    else:
                        # 预签 key 丢失（异常情况），兜底：重新生成
                        api_key = f"sk-jb-{secrets.token_hex(24)}"
                        bound_ids = ",".join(acc_ids)
                        key_meta: dict = {"usage_limit": 25, "usage_count": 0, "account_id": bound_ids}
                        VALID_CLIENT_KEYS[api_key] = key_meta
                        await _upsert_key_to_db(api_key, key_meta)
                        async with pool.acquire() as c:
                            await c.execute(
                                "UPDATE self_register_jobs SET status='active', result_keys=$2, error_msg='' WHERE email=$1",
                                email, api_key
                            )
                        print(f"[bind_task] {email} 预签 key 丢失，兜底重发 key {api_key[:16]}…，绑定 {len(acc_ids)} 个账号")
                    return

                if status == "active":
                    active_no_cred_retries += 1
                    print(f"[bind_task] {email} 已 active 但凭证均无效（第{active_no_cred_retries}/{active_no_cred_max}次），继续等待")
                    if active_no_cred_retries >= active_no_cred_max:
                        await _mark_failed("绑定完成但未返回有效凭证，请联系管理员")
                        print(f"[bind_task] {email} active 无凭证重试已达上限，标记失败")
                        return
                    continue
                print(f"[bind_task] {email} credentials_count={cred_count} 但凭证均无效（无jwt），继续轮询")

        print(f"[bind_task] {email} 账号绑定超时，标记失败")
        await _mark_failed("绑定超时，请重新提交")
    finally:
        _running_bind_tasks.discard(email)


@app.get("/key/self-register-status")
async def key_self_register_status(email: str):
    """查询自助绑卡进度"""
    email = email.strip().lower()
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="服务暂时不可用")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, result_keys, error_msg FROM self_register_jobs WHERE email=$1", email
        )
    if not row:
        return {"status": "not_found"}
    st = row["status"]
    pre_key = (row["result_keys"] or "").strip()
    if st == "active":
        return {"status": "active", "keys": [k for k in pre_key.split(",") if k.strip()]}
    if st == "failed":
        return {"status": "failed", "message": row["error_msg"] or "激活失败"}
    # processing / pending：返回预签发的 key（额度=0，等待激活）
    return {"status": "processing", "key": pre_key, "message": "账号正在处理中，凭证确认后即可使用"}



# ==================== Discord OAuth 成员验证 ====================

DISCORD_CLIENT_ID = "1495093746474156164"
DISCORD_GUILD_ID = "1134557553011998840"
DISCORD_REQUIRED_ROLES = {"1134611078203052122", "1383835063455842395", "1383835973384802396"}
DISCORD_CLIENT_SECRET = os.environ.get("DISCORD_CLIENT_SECRET", "")
_DISCORD_STATES: dict = {}   # state -> {"ts": float}
_DISCORD_VERIFIED: dict = {}  # verify_token -> {"user_id": str, "user_tag": str, "ts": float}
_DC_SESSION_TTL = 7 * 86400   # Discord 会话有效期：7 天（604800 秒）
# spin_token -> {"password": str, "prize_name": str, "ts": float} — 一次性领取令牌
_PENDING_PRIZES: dict = {}


async def _load_discord_sessions_from_db():
    """启动时从数据库加载 Discord 持久会话（7 天内有效的自动恢复，过期的直接删除）"""
    try:
        pool = await _get_db_pool()
        if not pool:
            return
        cutoff = time.time() - _DC_SESSION_TTL
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM discord_sessions WHERE ts < $1", cutoff)
            rows = await conn.fetch("SELECT token, user_id, user_tag, ts FROM discord_sessions")
            for row in rows:
                _DISCORD_VERIFIED[row["token"]] = {
                    "user_id": row["user_id"],
                    "user_tag": row["user_tag"],
                    "ts": row["ts"],
                }
        print(f"[Discord] 已从数据库恢复 {len(rows)} 个持久会话")
    except Exception as e:
        print(f"[Discord] 加载持久会话失败（不影响启动）: {e}")


def _discord_redirect_uri(request: Request) -> str:
    """构造 OAuth redirect_uri，始终使用外部域名"""
    # 1. 显式配置优先（管理员手动设定，兼容自定义域名）
    base = os.environ.get("DISCORD_CALLBACK_BASE", "").rstrip("/")
    if base:
        return f"{base}/key/discord-callback"
    # 2. Replit 生产域名（发布后自动注入，取第一个）
    replit_domains = os.environ.get("REPLIT_DOMAINS", "")
    if replit_domains:
        first_domain = replit_domains.split(",")[0].strip()
        if first_domain:
            return f"https://{first_domain}/key/discord-callback"
    # 3. Replit 开发域名
    dev_domain = os.environ.get("REPLIT_DEV_DOMAIN", "")
    if dev_domain:
        return f"https://{dev_domain}/key/discord-callback"
    # 4. fallback: 从请求头推断
    scheme = request.headers.get("x-forwarded-proto", "https")
    host = request.headers.get("x-forwarded-host", request.headers.get("host", "localhost"))
    return f"{scheme}://{host}/key/discord-callback"


@app.get("/key/discord-auth")
async def discord_auth(request: Request, mode: str = "register", redirect_to: str = ""):
    """发起 Discord OAuth 授权，跳转到 Discord 授权页。
    mode=register: 需要身份组验证（用于自助注册）
    mode=pack: 只验服务器成员资格（用于背包/抽奖机）
    redirect_to: 登录完成后跳回的前端子路径（如 lottery、backpack）
    """
    state = secrets.token_urlsafe(16)
    _DISCORD_STATES[state] = {"ts": time.time(), "mode": mode, "redirect_to": redirect_to}
    # 清理超过 10 分钟的旧 state
    expired = [k for k, v in _DISCORD_STATES.items() if time.time() - v["ts"] > 600]
    for k in expired:
        _DISCORD_STATES.pop(k, None)
    # 防 DDoS 撑大：若仍超上限则淘汰最旧条目
    if len(_DISCORD_STATES) > 500:
        oldest = sorted(_DISCORD_STATES.items(), key=lambda kv: kv[1]["ts"])
        for k, _ in oldest[:len(_DISCORD_STATES) - 400]:
            _DISCORD_STATES.pop(k, None)
    redirect_uri = _discord_redirect_uri(request)
    params = (
        f"?client_id={DISCORD_CLIENT_ID}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
        f"&scope=identify%20guilds.members.read"
        f"&state={state}"
    )
    return RedirectResponse(f"https://discord.com/api/oauth2/authorize{params}")


@app.get("/key/discord-callback")
async def discord_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    """Discord OAuth 回调：换取 token，验证服务器成员资格，跳回前端"""
    state_data = _DISCORD_STATES.pop(state, None) if state else None
    dc_mode = state_data.get("mode", "register") if state_data else "register"
    redirect_to = state_data.get("redirect_to", "") if state_data else ""
    frontend_base = f"/admin-panel/{redirect_to}" if (dc_mode == "pack" and redirect_to) else "/admin-panel/self-register"
    if error:
        return RedirectResponse(f"{frontend_base}?discord_error={error}")
    if not code or not state_data:
        return RedirectResponse(f"{frontend_base}?discord_error=invalid_state")

    redirect_uri = _discord_redirect_uri(request)
    # 1. 换取 access_token（复用全局连接池，避免每次新建 TCP）
    token_resp = await http_client.post(
        "https://discord.com/api/oauth2/token",
        data={
            "client_id": DISCORD_CLIENT_ID,
            "client_secret": DISCORD_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    if token_resp.status_code != 200:
        return RedirectResponse(f"{frontend_base}?discord_error=token_failed")
    token_data = token_resp.json()
    access_token = token_data.get("access_token", "")
    if not access_token:
        return RedirectResponse(f"{frontend_base}?discord_error=no_token")

    # 2. 获取用户信息
    user_resp = await http_client.get(
        "https://discord.com/api/users/@me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    if user_resp.status_code != 200:
        return RedirectResponse(f"{frontend_base}?discord_error=user_failed")
    user_data = user_resp.json()
    discord_user_id = user_data.get("id", "")
    username = user_data.get("username", "unknown")
    # 2023+ 新版 Discord 用户名体系：优先使用 global_name（昵称），回退到 username
    global_name = (user_data.get("global_name") or "").strip()
    display_name = global_name or username
    discriminator = user_data.get("discriminator", "0")
    user_tag = f"{username}#{discriminator}" if discriminator != "0" else username

    # 3. 验证服务器成员资格（pack 模式只验服务器，不验身份组）
    member_resp = await http_client.get(
        f"https://discord.com/api/users/@me/guilds/{DISCORD_GUILD_ID}/member",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    if member_resp.status_code == 404:
        return RedirectResponse(f"{frontend_base}?discord_error=not_member&tag={user_tag}")
    if member_resp.status_code != 200:
        return RedirectResponse(f"{frontend_base}?discord_error=member_check_failed")
    member_data = member_resp.json()
    # 仅在 register 模式下检查身份组
    if dc_mode != "pack":
        member_roles = set(member_data.get("roles", []))
        if not (member_roles & DISCORD_REQUIRED_ROLES):
            return RedirectResponse(f"{frontend_base}?discord_error=no_required_role&tag={user_tag}")

    # 验证通过 —— 幂等持久化 dc_tag ↔ password 映射，
    # 让从未捐献过的 DC 用户也能被管理员手动发奖反查到。
    # 仅在 dc_tag 真正变化时更新，points/total_earned 永远保留。
    if discord_user_id:
        try:
            pool = await _get_db_pool()
            if pool:
                tag_to_persist = display_name  # 优先全局昵称，回退用户名
                async with pool.acquire() as conn:
                    # 用 RETURNING xmax 区分插入 (xmax=0) vs 更新 (xmax<>0)
                    # 仅在新增、或 dc_tag 实际被改写时才使排行榜缓存失效。
                    row = await conn.fetchrow(
                        "INSERT INTO saint_points (password, points, total_earned, dc_tag) "
                        "VALUES ($1, 0, 0, $2) "
                        "ON CONFLICT (password) DO UPDATE SET "
                        "  dc_tag = CASE WHEN EXCLUDED.dc_tag <> '' "
                        "                 AND EXCLUDED.dc_tag <> saint_points.dc_tag "
                        "              THEN EXCLUDED.dc_tag ELSE saint_points.dc_tag END "
                        "RETURNING (xmax = 0) AS is_new, dc_tag",
                        f"dc_{discord_user_id}", tag_to_persist,
                    )
                # 新增 或 名字真的变了 → 失效排行榜缓存
                if row and (row["is_new"] or row["dc_tag"] == tag_to_persist):
                    _admin_cache_invalidate("leaderboard")
        except Exception as e:
            print(f"[Discord 验证] dc_tag 持久化失败（不影响登录）: {e}")

    # 写入内存，颁发持久 token 返回给前端（7 天有效）
    verify_token = secrets.token_urlsafe(24)
    now_ts = time.time()
    _DISCORD_VERIFIED[verify_token] = {
        "user_id": discord_user_id,
        "user_tag": user_tag,
        "ts": now_ts,
    }
    # 持久化到数据库（重启后可恢复）
    try:
        _pool = await _get_db_pool()
        if _pool:
            async with _pool.acquire() as _conn:
                await _conn.execute(
                    "INSERT INTO discord_sessions (token, user_id, user_tag, ts) "
                    "VALUES ($1, $2, $3, $4) ON CONFLICT (token) DO UPDATE SET ts = EXCLUDED.ts",
                    verify_token, str(discord_user_id), user_tag, now_ts,
                )
    except Exception as _e:
        print(f"[Discord] 保存会话到数据库失败（不影响登录）: {_e}")
    # 清理超过 7 天的旧记录，同时限制总容量
    now_vt = time.time()
    expired_vt = [k for k, v in _DISCORD_VERIFIED.items() if now_vt - v["ts"] > _DC_SESSION_TTL]
    for k in expired_vt:
        _DISCORD_VERIFIED.pop(k, None)
    # 若仍超过上限（1000），淘汰最旧的条目
    if len(_DISCORD_VERIFIED) > 1000:
        oldest = sorted(_DISCORD_VERIFIED.items(), key=lambda kv: kv[1]["ts"])
        for k, _ in oldest[:len(_DISCORD_VERIFIED) - 800]:
            _DISCORD_VERIFIED.pop(k, None)

    return RedirectResponse(f"{frontend_base}?discord_token={verify_token}&tag={user_tag}")


@app.get("/key/discord-verify")
async def discord_verify(token: str = ""):
    """前端轮询验证 discord token 有效性"""
    if not token or token not in _DISCORD_VERIFIED:
        raise HTTPException(status_code=401, detail="无效或已过期的 Discord 验证令牌")
    data = _DISCORD_VERIFIED[token]
    if time.time() - data["ts"] > _DC_SESSION_TTL:
        _DISCORD_VERIFIED.pop(token, None)
        raise HTTPException(status_code=401, detail="Discord 验证已过期，请重新授权")
    return {"valid": True, "user_tag": data["user_tag"], "user_id": data["user_id"]}


# ==================== Discord 账号 背包/抽奖 专用接口 ====================

def _require_dc_token(discord_token: str) -> tuple:
    """校验 discord_token 并返回 (dc_password, user_info)。校验失败直接抛 HTTP 401。"""
    if not discord_token or discord_token not in _DISCORD_VERIFIED:
        raise HTTPException(status_code=401, detail="Discord 验证无效，请重新登录")
    data = _DISCORD_VERIFIED[discord_token]
    if time.time() - data["ts"] > _DC_SESSION_TTL:
        _DISCORD_VERIFIED.pop(discord_token, None)
        raise HTTPException(status_code=401, detail="Discord 验证已过期，请重新登录")
    return f"dc_{data['user_id']}", data


@app.get("/key/dc-saint-points")
async def dc_saint_points(discord_token: str = ""):
    """查询 Discord 账号的圣人点数（token 无效时优雅返回而非 401，避免前端误判）"""
    try:
        password, dc_info = _require_dc_token(discord_token)
    except HTTPException:
        return {"points": 0, "authed": False}
    pool = await _get_db_pool()
    if not pool:
        return {"points": 0, "authed": True}
    tag = dc_info.get("user_tag", "")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT points FROM saint_points WHERE password=$1", password)
        # 顺手更新/写入 dc_tag（用户登录时的最新用户名）
        if tag:
            await conn.execute(
                "UPDATE saint_points SET dc_tag=$1 WHERE password=$2 AND dc_tag<>$1",
                tag, password,
            )
    return {"points": row["points"] if row else 0, "authed": True}


@app.get("/key/saint-leaderboard")
async def saint_leaderboard():
    """圣人点数累计排行榜（公开接口，30s 缓存）"""
    cached = _admin_cache_get("leaderboard")
    if cached is not None:
        return Response(content=cached, media_type="application/json")

    pool = await _get_db_pool()
    if not pool:
        return {"entries": []}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT password, dc_tag, total_earned
               FROM saint_points
               WHERE total_earned > 0
               ORDER BY total_earned DESC
               LIMIT 50"""
        )

    def _display(pw: str, tag: str) -> str:
        if tag:
            return tag  # 优先用 Discord 用户名
        if pw.startswith("dc_"):
            return pw[3:]  # 降级到数字 user_id
        return f"···{pw[-4:]}" if len(pw) >= 4 else "****"

    entries = [
        {"rank": i + 1, "name": _display(r["password"], r["dc_tag"]), "total_earned": r["total_earned"]}
        for i, r in enumerate(rows)
    ]
    body = json.dumps({"entries": entries}, ensure_ascii=False).encode()
    _admin_cache_set("leaderboard", body)
    return Response(content=body, media_type="application/json")


@app.post("/key/dc-saint-donate")
async def dc_saint_donate(request: Request):
    """Discord 账号捐献一个 JB Key，换取 1 个圣人点数"""
    body = await request.json()
    discord_token = (body.get("discord_token") or "").strip()
    api_key = (body.get("api_key") or "").strip()
    password, _ = _require_dc_token(discord_token)

    if not api_key:
        raise HTTPException(status_code=400, detail="缺少 api_key")
    if api_key not in VALID_CLIENT_KEYS:
        raise HTTPException(status_code=404, detail="Key 不存在或已被删除，无法捐献")

    key_meta = VALID_CLIENT_KEYS[api_key]
    usage_limit_raw = key_meta.get("usage_limit")   # None=无限, 0=待激活, >0=正常
    usage_limit = usage_limit_raw or 0
    usage_count = key_meta.get("usage_count") or 0

    # 额度为 0 的 key（NC 待激活 或 等待返回参数的预签 key）一律拒绝捐献
    if usage_limit_raw is not None and usage_limit_raw == 0:
        raise HTTPException(
            status_code=403,
            detail="此 Key 额度为 0（尚未激活），请等待激活后再捐献",
        )

    if usage_limit > 0 and usage_count / usage_limit > 0.10:
        raise HTTPException(status_code=400, detail="佛说，无元")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        # 保留兼容：拦截自助绑卡流程中尚未完成的预签 key
        sr_row = await conn.fetchrow("SELECT status FROM self_register_jobs WHERE result_keys=$1", api_key)
        if sr_row and sr_row["status"] in ("processing", "pending"):
            raise HTTPException(status_code=403, detail="此 Key 尚未激活（等待返回参数），请等待激活后再捐献")
        key_row = await conn.fetchrow("SELECT key, account_id FROM jb_client_keys WHERE key=$1", api_key)
        if not key_row:
            raise HTTPException(status_code=404, detail="Key 不在数据库中，无法捐献")
        account_id = key_row["account_id"]
        # 拆分逗号分隔的多账号 ID，逐一检查是否已捐献
        acc_ids = [a.strip() for a in (account_id or "").split(",") if a.strip()]
        if acc_ids:
            existing = await conn.fetchrow(
                "SELECT 1 FROM saint_donations WHERE account_id = ANY($1::text[])", acc_ids
            )
            if existing:
                raise HTTPException(status_code=409, detail="此账号已捐献过 Key，不可重复捐献（佛渡有缘人）")

        dc_tag = _DISCORD_VERIFIED.get(discord_token, {}).get("user_tag", "")

        # 写操作全部包裹在事务中，避免崩溃导致 Key 被删但点数未到账
        pokeball_item_id = None
        async with conn.transaction():
            await conn.execute("DELETE FROM jb_client_keys WHERE key=$1", api_key)
            await conn.execute("INSERT INTO user_passwords (password) VALUES ($1) ON CONFLICT DO NOTHING", password)
            if acc_ids:
                await conn.executemany(
                    "INSERT INTO saint_donations (account_id, password) VALUES ($1, $2) ON CONFLICT (account_id) DO NOTHING",
                    [(aid, password) for aid in acc_ids],
                )
            row = await conn.fetchrow(
                """INSERT INTO saint_points (password, points, total_earned, dc_tag, updated_at)
                   VALUES ($1, 1, 1, $2, NOW())
                   ON CONFLICT (password) DO UPDATE
                   SET points = saint_points.points + 1,
                       total_earned = saint_points.total_earned + 1,
                       dc_tag = CASE WHEN $2 <> '' THEN $2 ELSE saint_points.dc_tag END,
                       updated_at = NOW()
                   RETURNING points, total_earned, dc_pokeball_rewarded""",
                password, dc_tag,
            )
            # 首次捐 key：自动发放宝可梦球【容量10000】，每个 Discord 账号限一次
            if row["total_earned"] == 1 and not row["dc_pokeball_rewarded"]:
                pb_item = await conn.fetchrow(
                    """INSERT INTO user_items (owner_key, prize_name, metadata)
                       VALUES ($1, '宝可梦球【容量10000】', $2)
                       RETURNING id""",
                    password,
                    json.dumps({"capacity": 10000, "source": "first_donate_reward"}),
                )
                pokeball_item_id = pb_item["id"]
                await conn.execute(
                    "UPDATE saint_points SET dc_pokeball_rewarded=TRUE WHERE password=$1",
                    password,
                )
    VALID_CLIENT_KEYS.pop(api_key, None)
    _admin_cache_invalidate("leaderboard")
    resp: dict = {"success": True, "points": row["points"]}
    if pokeball_item_id is not None:
        resp["pokeball_awarded"] = True
        resp["pokeball_item_id"] = pokeball_item_id
        resp["pokeball_name"] = "宝可梦球【容量10000】"
    return resp


@app.post("/key/dc-saint-spin")
async def dc_saint_spin(request: Request):
    """Discord 账号消耗 1 个圣人点数抽奖（事务保护，spin_token 防重领）"""
    body = await request.json()
    discord_token = (body.get("discord_token") or "").strip()
    password, _ = _require_dc_token(discord_token)

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    # 清理过期 pending prizes（> 10 分钟）
    now = time.time()
    expired = [k for k, v in list(_PENDING_PRIZES.items()) if now - v["ts"] > 600]
    for k in expired:
        _PENDING_PRIZES.pop(k, None)

    async with pool.acquire() as conn:
        async with conn.transaction():
            updated = await conn.fetchrow(
                "UPDATE saint_points SET points = points - 1, updated_at = NOW() WHERE password=$1 AND points >= 1 RETURNING points",
                password,
            )
            if not updated:
                raise HTTPException(status_code=402, detail="圣人点数不足，请先捐献 Key")

            prize_rows = await conn.fetch(
                "SELECT id, name, quantity, weight FROM lottery_prizes WHERE is_active=TRUE AND (quantity=-1 OR quantity>0) ORDER BY id"
            )
            if not prize_rows:
                # 回滚会自动退还点数
                raise HTTPException(status_code=503, detail="暂无可用奖品，请联系管理员")

            weights = [r["weight"] for r in prize_rows]
            idx = _random.choices(range(len(prize_rows)), weights=weights, k=1)[0]
            chosen = prize_rows[idx]

            if chosen["quantity"] != -1:
                res = await conn.fetchrow(
                    "UPDATE lottery_prizes SET quantity = quantity - 1 WHERE id=$1 AND quantity > 0 RETURNING id",
                    chosen["id"],
                )
                if not res:
                    raise HTTPException(status_code=409, detail="奖品已被抢完，请再试一次")

    # 生成一次性领取令牌（仅对需要入背包的奖品有意义）
    spin_token = str(uuid.uuid4())
    _PENDING_PRIZES[spin_token] = {
        "password": password,
        "prize_name": chosen["name"],
        "ts": time.time(),
    }

    _admin_cache_invalidate("prizes")
    return {"success": True, "prize": chosen["name"], "points": updated["points"], "spin_token": spin_token}


@app.post("/key/dc-claim-prize")
async def dc_claim_prize(request: Request):
    """Discord 账号领取奖品到背包（spin_token 保证每次抽奖只能领取一次）"""
    body = await request.json()
    discord_token = (body.get("discord_token") or "").strip()
    spin_token = (body.get("spin_token") or "").strip()
    password, _ = _require_dc_token(discord_token)

    if not spin_token:
        raise HTTPException(status_code=400, detail="缺少 spin_token")

    # 原子性地弹出 pending prize，防止重复领取
    pending = _PENDING_PRIZES.pop(spin_token, None)
    if not pending:
        raise HTTPException(status_code=409, detail="无效或已使用的 spin_token，请勿重复领取")
    if pending["password"] != password:
        raise HTTPException(status_code=403, detail="spin_token 与当前账号不匹配")

    prize_name = pending["prize_name"]

    metadata: dict = body.get("metadata", {})
    cap = _parse_pokeball_capacity(prize_name)
    if cap:
        metadata["capacity"] = cap
    quota_amt = _parse_quota_amount(prize_name)
    if quota_amt:
        metadata["quota_amount"] = quota_amt

    pool = await _get_db_pool()
    if not pool:
        # DB 不可用，将 token 放回去供用户重试
        _PENDING_PRIZES[spin_token] = pending
        raise HTTPException(status_code=503, detail="数据库不可用，请稍后重试")

    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO user_passwords (password) VALUES ($1) ON CONFLICT DO NOTHING", password)
            row = await conn.fetchrow(
                "INSERT INTO user_items (owner_key, prize_name, metadata) VALUES ($1,$2,$3) RETURNING id, created_at",
                password, prize_name, json.dumps(metadata),
            )
    except Exception:
        # DB 操作失败，将 token 放回去供用户重试
        _PENDING_PRIZES[spin_token] = pending
        raise HTTPException(status_code=503, detail="写入背包失败，请稍后重试（spin_token 已保留）")
    return {"success": True, "item_id": row["id"]}


@app.get("/key/dc-backpack")
async def dc_backpack(discord_token: str = ""):
    """查询 Discord 账号的背包物品列表"""
    password, _ = _require_dc_token(discord_token)
    pool = await _get_db_pool()
    if not pool:
        return {"items": [], "pokeballs": []}
    async with pool.acquire() as conn:
        pw_row = await conn.fetchrow("SELECT 1 FROM user_passwords WHERE password=$1", password)
        if not pw_row:
            return {"items": [], "pokeballs": []}
        rows = await conn.fetch(
            "SELECT id, prize_name, metadata, used, used_at, created_at FROM user_items WHERE owner_key=$1 ORDER BY created_at DESC",
            password,
        )
        pb_rows = await conn.fetch(
            """SELECT p.id, p.ball_key, p.name, p.capacity, p.total_used, p.created_at,
                      ARRAY(SELECT member_key FROM pokeball_members WHERE pokeball_id=p.id) AS members
               FROM pokeballs p
               JOIN user_items ui ON ui.owner_key=$1
                   AND ui.used=TRUE
                   AND (
                       CASE WHEN ui.metadata->>'ball_key' IS NOT NULL
                            THEN ui.metadata->>'ball_key' = p.ball_key
                            ELSE (ui.metadata->>'pokeball_id')::int = p.id
                       END
                   )
               GROUP BY p.id
               ORDER BY p.created_at DESC""",
            password,
        )
    result = [
        {
            "id": r["id"],
            "prize_name": r["prize_name"],
            "metadata": json.loads(r["metadata"]) if r["metadata"] else {},
            "used": r["used"],
            "used_at": r["used_at"].isoformat() if r["used_at"] else None,
            "created_at": r["created_at"].isoformat(),
        }
        for r in rows
    ]
    pokeballs = [
        {
            "id": pb["id"],
            "ball_key": pb["ball_key"],
            "name": pb["name"],
            "capacity": pb["capacity"],
            "total_used": pb["total_used"],
            "members": list(pb["members"]),
            "created_at": pb["created_at"].isoformat(),
        }
        for pb in pb_rows
    ]
    return {"items": result, "pokeballs": pokeballs, "owner_key": password}


@app.post("/key/dc-import-from-password")
async def dc_import_from_password(request: Request):
    """将旧密码账号的背包物品和圣人点数合并到 Discord 账号"""
    body = await request.json()
    discord_token = (body.get("discord_token") or "").strip()
    old_password = (body.get("password") or "").strip()
    dc_password, dc_info = _require_dc_token(discord_token)

    if not old_password:
        raise HTTPException(status_code=400, detail="缺少 password")
    if old_password == dc_password:
        raise HTTPException(status_code=400, detail="不能导入到自身账号")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")

    async with pool.acquire() as conn:
        old_pw_row = await conn.fetchrow("SELECT 1 FROM user_passwords WHERE password=$1", old_password)
        if not old_pw_row:
            raise HTTPException(status_code=404, detail="密码账号不存在，请确认密码正确")

        # 确保 DC 账号存在
        await conn.execute("INSERT INTO user_passwords (password) VALUES ($1) ON CONFLICT DO NOTHING", dc_password)

        # 转移背包物品
        await conn.execute(
            "UPDATE user_items SET owner_key=$1 WHERE owner_key=$2",
            dc_password, old_password,
        )
        moved_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_items WHERE owner_key=$1", dc_password
        )

        # 合并圣人点数（包括 points 和 total_earned）
        old_sp_row = await conn.fetchrow(
            "SELECT points, total_earned FROM saint_points WHERE password=$1", old_password
        )
        old_points = old_sp_row["points"] if old_sp_row else 0
        old_earned = old_sp_row["total_earned"] if old_sp_row else 0
        if old_points > 0 or old_earned > 0:
            await conn.execute(
                """INSERT INTO saint_points (password, points, total_earned, updated_at)
                   VALUES ($1, $2, $3, NOW())
                   ON CONFLICT (password) DO UPDATE
                   SET points        = saint_points.points + $2,
                       total_earned  = saint_points.total_earned + $3,
                       updated_at    = NOW()""",
                dc_password, old_points, old_earned,
            )
            await conn.execute(
                "UPDATE saint_points SET points=0, total_earned=0, updated_at=NOW() WHERE password=$1",
                old_password,
            )

        new_points_row = await conn.fetchrow("SELECT points FROM saint_points WHERE password=$1", dc_password)
        new_points = new_points_row["points"] if new_points_row else 0

        # 将旧宝可梦球关联到新 owner（pokeballs 通过 user_items 关联，已随物品转移）

    return {
        "success": True,
        "imported_items": int(moved_count or 0),
        "imported_points": old_points,
        "new_total_points": new_points,
        "dc_tag": dc_info.get("user_tag", ""),
    }


# ==================== 合作方客户端推送（我方→对端）====================

async def _get_client_config(conn) -> dict:
    rows = await conn.fetch("SELECT key, value FROM partner_client_config")
    cfg = {r["key"]: r["value"] for r in rows}
    # 环境变量兜底：DB 里没有配置时从环境变量读取
    endpoint = cfg.get("endpoint", "") or os.environ.get("PARTNER_ENDPOINT", "")
    partner_id = cfg.get("partner_id", "") or os.environ.get("PARTNER_ID", "")
    hmac_secret_enc = cfg.get("hmac_secret_enc", "")
    if not hmac_secret_enc:
        raw_secret = os.environ.get("PARTNER_HMAC_SECRET", "")
        if raw_secret:
            hmac_secret_enc = _partner_encrypt(raw_secret)
    return {
        "endpoint":   endpoint,
        "partner_id": partner_id,
        "hmac_secret_enc": hmac_secret_enc,
    }


@app.get("/admin/partner-client-config")
async def admin_get_client_config(request: Request):
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        cfg = await _get_client_config(conn)
    return {
        "endpoint":   cfg["endpoint"],
        "partner_id": cfg["partner_id"],
        "has_secret": bool(cfg["hmac_secret_enc"]),
    }


@app.post("/admin/partner-client-config")
async def admin_set_client_config(request: Request):
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    upsert_sql = "INSERT INTO partner_client_config (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2"
    async with pool.acquire() as conn:
        async with conn.transaction():
            # asyncpg 同一连接不支持并发写入，顺序执行
            if "endpoint" in body:
                await conn.execute(upsert_sql, "endpoint", str(body["endpoint"]).strip())
            if "partner_id" in body:
                await conn.execute(upsert_sql, "partner_id", str(body["partner_id"]).strip())
            if "hmac_secret" in body and body["hmac_secret"]:
                enc = _partner_encrypt(str(body["hmac_secret"]).strip())
                await conn.execute(upsert_sql, "hmac_secret_enc", enc)
    return {"success": True}


@app.get("/admin/partner-client-config2")
async def admin_get_client_config2(request: Request):
    """返回第二对端配置状态（仅读，来自环境变量 PARTNER2_*）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    ep  = os.environ.get("PARTNER2_ENDPOINT", "").strip()
    pid = os.environ.get("PARTNER2_ID", "").strip()
    has = bool(os.environ.get("PARTNER2_HMAC_SECRET", "").strip())
    return {
        "endpoint":   ep,
        "partner_id": pid,
        "has_secret": has,
        "configured": bool(ep and pid and has),
    }


@app.get("/admin/partner-account-status")
async def admin_partner_account_status(request: Request, email: str):
    """查询合作方账号激活状态及密钥"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        cfg = await _get_client_config(conn)
    if not cfg["endpoint"] or not cfg["partner_id"] or not cfg["hmac_secret_enc"]:
        raise HTTPException(status_code=400, detail="合作方配置不完整")
    try:
        hmac_secret = _partner_decrypt_cached(cfg["hmac_secret_enc"])
    except Exception:
        raise HTTPException(status_code=500, detail="HMAC Secret 解密失败")
    # 从 endpoint 提取 base URL
    parsed = urlparse(cfg["endpoint"])
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path_with_qs = f"/api/partner/contribute/account?email={email}"
    ts_str = str(int(time.time()))
    body_hash = hashlib.sha256(b"").hexdigest()
    canonical = f"{ts_str}\nGET\n{path_with_qs}\n{body_hash}"
    sig = _hmac_mod.new(hmac_secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    headers = {
        "X-Partner-Key-Id": cfg["partner_id"],
        "X-Partner-Timestamp": ts_str,
        "X-Partner-Signature": sig,
    }
    try:
        resp = await http_client.get(f"{base_url}{path_with_qs}", headers=headers, timeout=15)
        if resp.status_code == 404:
            return {"status": "not_found", "message": "对方未找到该账号记录"}
        data = resp.json()
        # 规整响应：对方将数据嵌套在 account 字段里，且 linked_credential_ids 是 JSON 字符串
        account = data.get("account") or data
        raw_ids = account.get("linked_credential_ids", [])
        if isinstance(raw_ids, str):
            try:
                raw_ids = json.loads(raw_ids)
            except Exception:
                raw_ids = []
        return {
            "status": account.get("status", "unknown"),
            "email": account.get("email", email),
            "linked_credential_ids": raw_ids,
            "aif_license_count": account.get("aif_license_count", 0),
            "activation_error": account.get("activation_error", ""),
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"查询失败: {e}")


@app.post("/admin/partner-poll-credentials")
async def admin_partner_poll_credentials(request: Request):
    """立即执行一次合作方凭证轮询，将新凭证写入 jb_accounts"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    try:
        result = await _run_partner_credentials_poll()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"轮询失败: {e}")
    return {"success": True, **result}


@app.post("/admin/partner-import-by-email")
async def admin_partner_import_by_email(request: Request):
    """向合作方查询指定 email 的凭证并直接写入 jb_accounts"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    email = str(body.get("email", "")).strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="email required")
    cfg = await _get_client_cfg_for_push()
    if not cfg or not cfg.get("endpoint") or not cfg.get("hmac_secret_enc"):
        raise HTTPException(status_code=400, detail="合作方配置不完整")
    _ep_parsed = urlparse(cfg["endpoint"]); base_url = f"{_ep_parsed.scheme}://{_ep_parsed.netloc}"
    path_with_qs = f"/api/partner/contribute/account?email={_urlquote(email, safe='')}"
    try:
        headers = _partner_headers(cfg, "GET", path_with_qs, b"")
        resp = await http_client.get(
            f"{base_url}{path_with_qs}",
            headers={k: v for k, v in headers.items() if k != "Content-Type"},
            timeout=20,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"查询失败: {e}")
    if resp.status_code == 404:
        return {"success": False, "partner_status": "not_found", "imported": 0, "acc_ids": []}
    try:
        data = resp.json()
    except Exception:
        raise HTTPException(status_code=502, detail="合作方响应解析失败")
    account = data.get("account") or data
    partner_status = account.get("status", "unknown")
    credentials = account.get("credentials") or data.get("credentials") or []
    if not credentials:
        return {"success": False, "partner_status": partner_status,
                "imported": 0, "acc_ids": [],
                "message": account.get("activation_error") or f"对方状态: {partner_status}，暂无凭证"}
    acc_ids = []
    for cred in credentials:
        acc_id = await _import_partner_credential(cred)
        if acc_id:
            acc_ids.append(acc_id)
    return {"success": bool(acc_ids), "partner_status": partner_status,
            "imported": len(acc_ids), "acc_ids": acc_ids}


@app.post("/admin/self-register-force-complete")
async def admin_self_register_force_complete(request: Request):
    """管理员强制完成指定邮箱的 processing 任务：查询合作方凭证 → 导入 → 签发 key"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    email = str(body.get("email", "")).strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="email required")

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")

    # 确认 job 存在
    async with pool.acquire() as conn:
        job = await conn.fetchrow(
            "SELECT status, result_keys FROM self_register_jobs WHERE email=$1", email
        )
    if not job:
        raise HTTPException(status_code=404, detail="找不到该邮箱的申请记录")
    if job["status"] == "active":
        return {"success": True, "message": "已是 active", "keys": [k for k in (job["result_keys"] or "").split(",") if k.strip()]}

    # 调合作方接口
    cfg = await _get_client_cfg_for_push()
    if not cfg or not cfg.get("endpoint") or not cfg.get("hmac_secret_enc"):
        raise HTTPException(status_code=400, detail="合作方配置不完整")

    _ep_parsed = urlparse(cfg["endpoint"]); base_url = f"{_ep_parsed.scheme}://{_ep_parsed.netloc}"
    path_with_qs = f"/api/partner/contribute/account?email={_urlquote(email, safe='')}"
    try:
        headers = _partner_headers(cfg, "GET", path_with_qs, b"")
        resp = await http_client.get(
            f"{base_url}{path_with_qs}",
            headers={k: v for k, v in headers.items() if k != "Content-Type"},
            timeout=20,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"查询失败: {e}")

    try:
        data = resp.json()
    except Exception:
        raise HTTPException(status_code=502, detail="合作方响应解析失败")

    account = data.get("account") or data
    partner_status = account.get("status", "unknown")
    credentials = account.get("credentials") or data.get("credentials") or []

    if not credentials:
        return {
            "success": False,
            "partner_status": partner_status,
            "message": account.get("activation_error") or f"合作方状态: {partner_status}，暂无凭证，无法强制完成",
        }

    # 导入凭证
    acc_ids = []
    for cred in credentials:
        acc_id = await _import_partner_credential(cred)
        if acc_id:
            acc_ids.append(acc_id)

    if not acc_ids:
        return {"success": False, "partner_status": partner_status, "message": "凭证导入均失败（无有效 JWT）"}

    # 激活预签发 key 的额度（或兜底重新生成）
    api_key = (job["result_keys"] or "").strip()
    if api_key and await _activate_key_quota(api_key, acc_ids, pool):
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE self_register_jobs SET status='active', error_msg='' WHERE email=$1", email
            )
        print(f"[admin_force_complete] {email} 完成，key {api_key[:16]}… 额度已激活，绑定 acc_ids={acc_ids}")
    else:
        api_key = f"sk-jb-{secrets.token_hex(24)}"
        bound_ids = ",".join(acc_ids)
        key_meta: dict = {"usage_limit": 25, "usage_count": 0, "account_id": bound_ids}
        VALID_CLIENT_KEYS[api_key] = key_meta
        await _upsert_key_to_db(api_key, key_meta)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE self_register_jobs SET status='active', result_keys=$2, error_msg='' WHERE email=$1",
                email, api_key
            )
        print(f"[admin_force_complete] {email} 预签 key 丢失，兜底重发 key {api_key[:16]}…")

    return {"success": True, "partner_status": partner_status, "key": api_key, "acc_ids": acc_ids}


@app.post("/admin/partner-client-push")
async def admin_partner_client_push(request: Request):
    """将账号批量推送到合作方主端点"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    accounts = body.get("accounts", [])
    activation_mode = body.get("activation_mode", "immediate")
    idempotency_key = body.get("idempotency_key") or str(uuid.uuid4())

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        cfg = await _get_client_config(conn)

    if not cfg["endpoint"] or not cfg["partner_id"] or not cfg["hmac_secret_enc"]:
        raise HTTPException(status_code=400, detail="合作方配置不完整，请先填写 Endpoint、Partner ID 和 HMAC Secret")

    try:
        hmac_secret = _partner_decrypt_cached(cfg["hmac_secret_enc"])
    except Exception:
        raise HTTPException(status_code=500, detail="HMAC Secret 解密失败")

    path = "/api/partner/contribute/submit"
    req_body = json.dumps({
        "idempotency_key": idempotency_key,
        "activation_mode": activation_mode,
        "accounts": accounts,
    }, ensure_ascii=False)

    _parsed2 = urlparse(cfg["endpoint"])
    submit_url2 = f"{_parsed2.scheme}://{_parsed2.netloc}{path}"

    ts_str = str(int(time.time()))
    body_hash = hashlib.sha256(req_body.encode()).hexdigest()
    canonical = f"{ts_str}\nPOST\n{path}\n{body_hash}"
    sig = _hmac_mod.new(hmac_secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()

    headers = {
        "X-Partner-Key-Id": cfg["partner_id"],
        "X-Partner-Timestamp": ts_str,
        "X-Partner-Signature": sig,
        "Content-Type": "application/json",
    }

    try:
        resp = await http_client.post(submit_url2, content=req_body.encode(), headers=headers, timeout=30)
        return {"status_code": resp.status_code, "response": resp.json() if resp.headers.get("content-type","").startswith("application/json") else resp.text}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"请求失败: {e}")


# ==================== 合作伙伴 API ====================

def _ms() -> int:
    return int(time.time() * 1000)


async def _partner_auth(request: Request) -> tuple:
    """验证 Partner HMAC 签名，返回 (partner_row_dict, raw_body_str)"""
    key_id = request.headers.get("X-Partner-Key-Id", "")
    ts_str = request.headers.get("X-Partner-Timestamp", "")
    sig    = request.headers.get("X-Partner-Signature", "")
    if not key_id or not ts_str or not sig:
        raise HTTPException(status_code=401, detail={"error": "unauthorized", "msg": "missing headers"})
    try:
        ts = int(ts_str)
    except ValueError:
        raise HTTPException(status_code=401, detail={"error": "unauthorized", "msg": "invalid timestamp"})
    if abs(time.time() - ts) > 300:
        raise HTTPException(status_code=401, detail={"error": "unauthorized", "msg": "timestamp out of window"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=500, detail={"error": "internal_error"})
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM partner_keys WHERE id=$1", key_id)
    if not row:
        raise HTTPException(status_code=401, detail={"error": "unauthorized", "msg": "unknown partner"})
    if not row["enabled"]:
        raise HTTPException(status_code=403, detail={"error": "key_disabled"})
    try:
        hmac_secret = _partner_decrypt_cached(row["hmac_secret_enc"])
    except Exception:
        raise HTTPException(status_code=500, detail={"error": "internal_error", "msg": "decrypt failed"})
    raw_body = await request.body()
    body_str = raw_body.decode()
    method = request.method.upper()
    path = request.url.path
    if request.url.query:
        path += "?" + request.url.query
    body_hash = hashlib.sha256(body_str.encode()).hexdigest()
    canonical = f"{ts_str}\n{method}\n{path}\n{body_hash}"
    expected = _hmac_mod.new(hmac_secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    if not _hmac_mod.compare_digest(expected, sig.lower()):
        raise HTTPException(status_code=401, detail={"error": "unauthorized", "msg": "invalid signature"})
    return dict(row), body_str


async def _audit_log(partner_id: str, method: str, path: str, status_code: int, body_hash: str = ""):
    try:
        pool = await _get_db_pool()
        if not pool:
            return
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO partner_api_audit (partner_id,method,path,status_code,body_hash) VALUES ($1,$2,$3,$4,$5)",
                partner_id, method, path, status_code, body_hash
            )
    except Exception:
        pass


def _contribution_to_dict(r) -> dict:
    return {
        "id": str(r["id"]),
        "email": r["email"],
        "status": r["status"],
        "activation_attempts": r["activation_attempts"],
        "activation_error": r["activation_error"] or "",
        "aif_license_count": r["aif_license_count"],
        "linked_credential_ids": r["linked_credential_ids"] or "[]",
        "activation_mode": r["activation_mode"],
        "created_at": r["created_at_ms"],
        "updated_at": r["updated_at_ms"],
        "activation_started_at": r["activation_started_at"],
        "activation_completed_at": r["activation_completed_at"],
    }


# ---- Admin: Partner key management ----

@app.get("/admin/partner-keys")
async def admin_list_partner_keys(request: Request):
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, enabled, notes, created_at FROM partner_keys ORDER BY created_at DESC"
        )
        counts = await conn.fetch(
            "SELECT partner_id, COUNT(*) as total, SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active FROM account_contributions GROUP BY partner_id"
        )
    count_map = {r["partner_id"]: {"total": r["total"], "active": r["active"]} for r in counts}
    result = []
    for r in rows:
        stats = count_map.get(r["id"], {"total": 0, "active": 0})
        result.append({
            "id": r["id"],
            "enabled": r["enabled"],
            "notes": r["notes"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "total_contributions": stats["total"],
            "active_contributions": stats["active"],
        })
    return result


@app.post("/admin/partner-keys")
async def admin_create_partner_key(request: Request):
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    if not _PARTNER_CRYPTO_OK:
        raise HTTPException(status_code=500, detail="crypto not available")
    body = await request.json()
    name = re.sub(r"[^a-z0-9]", "", (body.get("name") or "partner").lower())[:20] or "partner"
    notes = (body.get("notes") or "")[:200]
    suffix = secrets.token_hex(8)
    partner_id = f"partner-{name}-{suffix}"
    hmac_secret_plain = secrets.token_hex(32)  # 64-hex = 32-byte secret
    hmac_secret_enc = _partner_encrypt(hmac_secret_plain)
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO partner_keys (id, hmac_secret_enc, notes) VALUES ($1, $2, $3)",
            partner_id, hmac_secret_enc, notes
        )
    return {
        "partner_id": partner_id,
        "hmac_secret": hmac_secret_plain,
        "note": "hmac_secret 仅显示一次，请立刻保存",
    }


@app.put("/admin/partner-keys/{pid}")
async def admin_update_partner_key(pid: str, request: Request):
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    body = await request.json()
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        if "enabled" in body:
            await conn.execute("UPDATE partner_keys SET enabled=$1 WHERE id=$2", bool(body["enabled"]), pid)
        if "notes" in body:
            await conn.execute("UPDATE partner_keys SET notes=$1 WHERE id=$2", str(body["notes"])[:200], pid)
    return {"success": True}


@app.delete("/admin/partner-keys/{pid}")
async def admin_delete_partner_key(pid: str, request: Request):
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM partner_keys WHERE id=$1", pid)
    return {"success": True}


@app.get("/admin/contributions")
async def admin_list_contributions(request: Request, status: str = "", partner_id: str = "", limit: int = 100, offset: int = 0):
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    where_clauses = []
    vals: list = []
    if status:
        vals.append(status)
        where_clauses.append(f"status=${len(vals)}")
    if partner_id:
        vals.append(partner_id)
        where_clauses.append(f"partner_id=${len(vals)}")
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    vals += [min(limit, 500), offset]
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM account_contributions {where} ORDER BY created_at_ms DESC LIMIT ${len(vals)-1} OFFSET ${len(vals)}",
            *vals
        )
        total = await conn.fetchval(f"SELECT COUNT(*) FROM account_contributions {where}", *vals[:-2])
    return {
        "total": total,
        "accounts": [_contribution_to_dict(r) for r in rows],
    }


@app.get("/admin/contributions/{cid}")
async def admin_get_contribution(cid: str, request: Request):
    """查询单条 contribution 详情"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM account_contributions WHERE id=$1", cid)
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    return _contribution_to_dict(row)


@app.post("/admin/contributions/{cid}/activate")
async def admin_manually_activate_contribution(cid: str, request: Request):
    """手动触发单个 contribution 激活"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM account_contributions WHERE id=$1", cid)
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    asyncio.create_task(_activate_contribution(str(row["id"]), row["email"], row["password_enc"]))
    return {"success": True, "message": "激活任务已提交"}


# ==================== 数据库全量导出 / 导入（数据迁移用）====================

_EXPORT_TABLES = [
    "jb_accounts",
    "jb_client_keys",
    "jb_settings",                   # ★ LOW 用户审计行 + 全局配置（关键安全表）
    "partner_client_config",
    "partner_keys",
    "partner_precheck_rejections",
    "partner_api_audit",
    "partner_idempotency",
    "self_register_jobs",
    "account_contributions",
    "user_passwords",
    "saint_points",
    "saint_donations",
    "donated_jb_accounts",
    "pokeballs",
    "pokeball_members",
    "user_items",
    "lottery_prizes",
    "cf_proxy_pool",            # ★ CF 代理池（含 owner='admin' 主池 + owner='low_admin' LOW 子池）
]

# 各表的冲突主键（单列）；未列出的表导入时用 ON CONFLICT DO NOTHING
_TABLE_CONFLICT_COL: dict[str, str] = {
    "jb_accounts":                 "id",
    "jb_client_keys":              "key",
    "jb_settings":                 "key",           # ★ 新加：LOW 用户审计行 + 全局配置
    "partner_client_config":       "key",
    "partner_keys":                "id",
    "partner_precheck_rejections": "id",
    "self_register_jobs":          "email",
    "account_contributions":       "id",
    # 以下表支持源覆盖（冲突时用源数据覆盖）
    "user_passwords":              "password",
    "saint_points":                "password",
    "saint_donations":             "account_id",
    "donated_jb_accounts":        "jb_email",
    "pokeballs":                   "ball_key",
    "lottery_prizes":              "name",
}

# 复合唯一键表（db-import 时需要多列 ON CONFLICT 表达式）
_TABLE_CONFLICT_MULTI: dict[str, list] = {
    "cf_proxy_pool":      ["url", "owner", "owner_discord_id"],
    "pokeball_members":   ["pokeball_id", "member_key"],
}


# ==================== 流式数据迁移（NDJSON）====================
# 替代旧的一次性 export/import 端点，避免内存峰值和超时
#
# NDJSON 协议：每行一个独立 JSON 对象
#   {"_meta":{"version":3,"exported_at":1730000000}}
#   {"_table":"jb_accounts"}
#   {<row1>}
#   {<row2>}
#   {"_table":"jb_client_keys"}
#   {<row1>}
#   {"_error":{"table":"xxx","error":"..."}}    可选错误行

@app.get("/admin/db-export-stream")
async def admin_db_export_stream(request: Request):
    """流式导出全表为 NDJSON。响应体每行一个独立 JSON 对象。

    优势：服务端用 PG asyncpg cursor 边查边写，内存稳定 ~10MB（无论数据量）。
    支持任意大数据量（GB 级也不会 OOM）。
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "仅完整管理员可使用数据迁移接口"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")

    async def stream_rows():
        # 1. 元数据行
        yield (json.dumps(
            {"_meta": {"version": 3, "exported_at": int(time.time())}},
            ensure_ascii=False,
        ) + "\n").encode("utf-8")

        # 2. 逐表流式输出
        for table in _EXPORT_TABLES:
            yield (json.dumps({"_table": table}, ensure_ascii=False) + "\n").encode("utf-8")
            try:
                # asyncpg cursor 必须在 transaction 内
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        # pokeball_members 附带 ball_key 以便跨环境 id 映射
                        if table == "pokeball_members":
                            _export_q = (
                                "SELECT pm.id, pm.pokeball_id, pm.member_key, p.ball_key"
                                " FROM pokeball_members pm"
                                " JOIN pokeballs p ON p.id = pm.pokeball_id"
                            )
                        else:
                            _export_q = f'SELECT * FROM "{table}"'
                        async for row in conn.cursor(_export_q, prefetch=1000):
                            yield (json.dumps(
                                dict(row),
                                default=str,        # datetime/Decimal 自动转 str
                                ensure_ascii=False,
                            ) + "\n").encode("utf-8")
            except Exception as e:
                yield (json.dumps(
                    {"_error": {"table": table, "error": str(e)[:300]}},
                    ensure_ascii=False,
                ) + "\n").encode("utf-8")

    return StreamingResponse(
        stream_rows(),
        media_type="application/x-ndjson",
        headers={
            "Content-Disposition": f'attachment; filename="db-export-{int(time.time())}.ndjson"',
            "X-Accel-Buffering":   "no",
            "Cache-Control":       "no-cache",
        },
    )


# 目标表列名缓存（进程级，避免每行查 pg_attribute）
_table_columns_cache: Dict[str, set] = {}

# 时间戳字符串正则（json.dumps(default=str) 会把 datetime 转为字符串）
_TS_RE = re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}')


def _coerce_json_value(v):
    """将 JSON 导出中被序列化为字符串的 datetime 还原为 datetime 对象。
    asyncpg 不接受字符串形式的时间戳，必须传真正的 datetime 实例。
    """
    if isinstance(v, str) and _TS_RE.match(v):
        try:
            return _dt.fromisoformat(v)
        except ValueError:
            pass
    return v


async def _get_table_columns(conn, table: str) -> set:
    """查询并缓存目标表实际存在的列名集合。"""
    if table not in _table_columns_cache:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
            table,
        )
        cols = {r["column_name"] for r in rows}
        _table_columns_cache[table] = cols
        print(f"[schema-cache] {table}: {len(cols)} 列", flush=True)
    return _table_columns_cache[table]


async def _upsert_one_row(conn, table: str, row: dict) -> None:
    """单行 upsert 核心逻辑（被 db-import-stream 和 import-from-source-stream 复用）。
    自动过滤目标表不存在的列，防止因 schema 差异导致 UndefinedColumnError。
    """
    # 过滤掉目标表不存在的列（源端可能有目标端尚未迁移的字段）
    known = await _get_table_columns(conn, table)
    if known:
        original_keys = set(row.keys())
        row = {k: v for k, v in row.items() if k in known}
        dropped = original_keys - set(row.keys())
        if dropped and table not in getattr(_upsert_one_row, "_logged_drops", set()):
            if not hasattr(_upsert_one_row, "_logged_drops"):
                _upsert_one_row._logged_drops = set()
            _upsert_one_row._logged_drops.add(table)
            print(f"[schema-filter] {table}: 过滤掉源端多余列 {sorted(dropped)}", flush=True)

    cols = list(row.keys())
    if not cols:
        return
    placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
    col_names    = ", ".join(f'"{c}"' for c in cols)
    # _coerce_json_value 将时间戳字符串还原为 datetime 对象（json.dumps 序列化时变为字符串）
    values       = [_coerce_json_value(row[c]) for c in cols]

    conflict_col   = _TABLE_CONFLICT_COL.get(table)
    conflict_multi = _TABLE_CONFLICT_MULTI.get(table)

    if conflict_multi and all(c in cols for c in conflict_multi):
        target = "(" + ", ".join(f'"{c}"' for c in conflict_multi) + ")"
        update_cols = [c for c in cols if c not in conflict_multi]
        if update_cols:
            set_clause = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in update_cols)
            sql = (f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders}) '
                   f'ON CONFLICT {target} DO UPDATE SET {set_clause}')
        else:
            sql = (f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders}) '
                   f'ON CONFLICT {target} DO NOTHING')
    elif conflict_col and conflict_col in cols:
        update_cols = [c for c in cols if c != conflict_col]
        if update_cols:
            set_clause = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in update_cols)
            sql = (f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders}) '
                   f'ON CONFLICT ("{conflict_col}") DO UPDATE SET {set_clause}')
        else:
            sql = (f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders}) '
                   f'ON CONFLICT ("{conflict_col}") DO NOTHING')
    else:
        sql = (f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders}) '
               f'ON CONFLICT DO NOTHING')

    await conn.execute(sql, *values)


@app.post("/admin/db-import-stream")
async def admin_db_import_stream(request: Request):
    """接收 NDJSON 文件上传，文件收完后立刻启动后台导入任务并返回 job_id。
    客户端通过 GET /admin/migration-job/{job_id} 轮询导入进度。
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "仅完整管理员可使用数据迁移接口"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")

    # 接收完整文件（上传阶段，前端 XHR 可在此期间显示上传进度）
    chunks: list = []
    async for chunk in request.stream():
        if chunk:
            chunks.append(chunk)
    data = b"".join(chunks)
    bytes_received = len(data)

    job_id = uuid.uuid4().hex[:8]
    _migration_jobs[job_id] = {
        "status": "running",
        "started_at": time.time(),
        "counts": {},
        "bytes_read": 0,
        "total_file_bytes": bytes_received,
        "errors": [],
        "finished": False,
    }
    print(f"[bg-import:{job_id}] 启动，文件大小={bytes_received/1024/1024:.1f}MB", flush=True)
    asyncio.ensure_future(_run_import_bg(job_id, data))
    return {"job_id": job_id, "bytes_received": bytes_received}


_SERIAL_TABLES: list[tuple[str, str]] = [
    ("user_items",                "id"),
    ("pokeballs",                 "id"),
    ("pokeball_members",          "id"),
    ("account_contributions",     "id"),
    ("partner_precheck_rejections","id"),
    ("partner_api_audit",         "id"),
    ("partner_idempotency",       "id"),
    ("saint_donations",           "id"),
    ("self_register_jobs",        "id"),
]

async def _reset_serial_sequences(conn) -> list[str]:
    """导入完成后重置所有 SERIAL 序列，防止新行 id 冲突。返回执行日志。"""
    msgs: list[str] = []
    for tbl, col in _SERIAL_TABLES:
        try:
            row = await conn.fetchrow(
                f'SELECT COALESCE(MAX("{col}"), 0) AS mx FROM "{tbl}"'
            )
            mx = row["mx"]
            seq_row = await conn.fetchrow(
                "SELECT pg_get_serial_sequence($1, $2) AS seq", tbl, col
            )
            seq = seq_row["seq"] if seq_row else None
            if seq:
                await conn.execute(f"SELECT setval('{seq}', $1, true)", max(mx, 1))
                msgs.append(f"[seq-reset] {tbl}.{col} → {mx}")
        except Exception as ex:
            msgs.append(f"[seq-reset] {tbl}.{col} 跳过: {ex}")
    return msgs


async def _run_import_bg(job_id: str, data: bytes) -> None:
    """后台处理 NDJSON 文件导入，进度实时写入 _migration_jobs[job_id]。"""
    job = _migration_jobs[job_id]
    pool = await _get_db_pool()
    if not pool:
        job.update({"status": "failed", "error": "DB unavailable", "finished": True})
        return

    BATCH_INSERT = 500
    BATCH_COMMIT = 5000

    conn = await pool.acquire()
    current_table: Optional[str] = None
    row_buffer: list = []
    tx_box = [conn.transaction()]
    await tx_box[0].start()
    rows_in_tx = [0]

    async def _flush(tbl: Optional[str]) -> None:
        if not row_buffer or not tbl:
            row_buffer.clear()
            return

        # ── pokeball_members 特殊处理：用 ball_key JOIN 查目标库 id，避免跨环境 id 不一致 ──
        if tbl == "pokeball_members":
            if "ball_key" in row_buffer[0]:
                # 新格式：用 ball_key 解析目标库 pokeball_id（最安全）
                sql_pm = (
                    "INSERT INTO pokeball_members (pokeball_id, member_key)"
                    " SELECT p.id, $2 FROM pokeballs p WHERE p.ball_key = $1"
                    " ON CONFLICT (pokeball_id, member_key) DO NOTHING"
                )
                pm_args = [(r.get("ball_key"), r.get("member_key")) for r in row_buffer]
            else:
                # 旧格式：跳过 id 列（避免 PK 冲突），直接用 pokeball_id + member_key
                sql_pm = (
                    "INSERT INTO pokeball_members (pokeball_id, member_key)"
                    " VALUES ($1, $2)"
                    " ON CONFLICT (pokeball_id, member_key) DO NOTHING"
                )
                pm_args = [(r.get("pokeball_id"), r.get("member_key")) for r in row_buffer]
            try:
                await conn.executemany(sql_pm, pm_args)
                n = len(row_buffer)
                job["counts"][tbl] = job["counts"].get(tbl, 0) + n
                rows_in_tx[0] += n
            except Exception as ex:
                job["errors"].append(f"{tbl} batch({len(row_buffer)}): {type(ex).__name__}: {ex}")
                try: await tx_box[0].rollback()
                except Exception: pass
                tx_box[0] = conn.transaction()
                await tx_box[0].start()
                rows_in_tx[0] = 0
            row_buffer.clear()
            if rows_in_tx[0] >= BATCH_COMMIT:
                await tx_box[0].commit()
                tx_box[0] = conn.transaction()
                await tx_box[0].start()
                rows_in_tx[0] = 0
            return

        known = await _get_table_columns(conn, tbl)
        sample = {k: v for k, v in row_buffer[0].items() if k in known}
        cols = list(sample.keys())
        if not cols:
            row_buffer.clear()
            return

        conflict_col   = _TABLE_CONFLICT_COL.get(tbl)
        conflict_multi = _TABLE_CONFLICT_MULTI.get(tbl)
        ph = "(" + ", ".join(f"${i+1}" for i in range(len(cols))) + ")"
        cn = ", ".join(f'"{c}"' for c in cols)

        if conflict_multi and all(c in cols for c in conflict_multi):
            target = "(" + ", ".join(f'"{c}"' for c in conflict_multi) + ")"
            uc = [c for c in cols if c not in conflict_multi]
            sc = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in uc)
            sql = (f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT {target} '
                   f'DO UPDATE SET {sc}') if uc else (
                   f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT {target} DO NOTHING')
        elif conflict_col and conflict_col in cols:
            uc = [c for c in cols if c != conflict_col]
            sc = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in uc)
            sql = (f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT ("{conflict_col}") '
                   f'DO UPDATE SET {sc}') if uc else (
                   f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT ("{conflict_col}") DO NOTHING')
        else:
            sql = f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT DO NOTHING'

        args = [tuple(_coerce_json_value(r.get(c)) for c in cols) for r in row_buffer]
        try:
            await conn.executemany(sql, args)
            n = len(row_buffer)
            job["counts"][tbl] = job["counts"].get(tbl, 0) + n
            rows_in_tx[0] += n
        except Exception as ex:
            job["errors"].append(f"{tbl} batch({len(row_buffer)}): {type(ex).__name__}: {ex}")
            try: await tx_box[0].rollback()
            except Exception: pass
            tx_box[0] = conn.transaction()
            await tx_box[0].start()
            rows_in_tx[0] = 0
        row_buffer.clear()

        if rows_in_tx[0] >= BATCH_COMMIT:
            await tx_box[0].commit()
            tx_box[0] = conn.transaction()
            await tx_box[0].start()
            rows_in_tx[0] = 0

    try:
        for raw_line in data.split(b"\n"):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            job["bytes_read"] += len(raw_line) + 1
            try:
                obj = json.loads(raw_line)
            except Exception as e:
                job["errors"].append(f"JSON parse: {type(e).__name__}: {raw_line[:80]!r}")
                continue

            if "_meta" in obj:
                continue
            if "_table" in obj:
                await _flush(current_table)
                tbl = obj["_table"]
                current_table = tbl if tbl in _EXPORT_TABLES else None
                if current_table:
                    job["counts"].setdefault(current_table, 0)
                continue
            if "_error" in obj:
                job["errors"].append(f"source error: {obj['_error']}")
                continue
            if not current_table:
                continue

            row_buffer.append(obj)
            if len(row_buffer) >= BATCH_INSERT:
                await _flush(current_table)

        await _flush(current_table)
        await tx_box[0].commit()
        # 重置所有 SERIAL 序列，防止新行与已导入数据 id 冲突
        seq_msgs = await _reset_serial_sequences(conn)
        for m in seq_msgs:
            print(m, flush=True)
        job["errors"] = [e for e in job.get("errors", []) if not e.startswith("[seq-reset]")]
    except Exception as e:
        try: await tx_box[0].rollback()
        except Exception: pass
        job.update({
            "status": "failed",
            "error": f"导入失败: {type(e).__name__}: {e}",
            "finished": True,
            "elapsed_sec": round(time.time() - job["started_at"], 1),
        })
        return
    finally:
        await pool.release(conn)

    try: await load_accounts_from_db()
    except: pass
    try: await load_keys_from_db()
    except: pass
    try: await load_cf_proxies_from_db()
    except: pass

    if job["counts"].get("jb_accounts", 0) > 0:
        try:
            async with pool.acquire() as conn2:
                rows2 = await conn2.fetch(
                    "SELECT id FROM jb_accounts WHERE COALESCE(last_quota_check, 0) = 0 LIMIT 5000"
                )
                _schedule_quota_checks_for_ids({r["id"] for r in rows2}, label="bg-import入池检测")
        except Exception:
            pass

    total = sum(job["counts"].values())
    elapsed = round(time.time() - job["started_at"], 1)
    print(f"[bg-import:{job_id}] 完成：{total}行 / {elapsed}s / 错误{len(job['errors'])}个", flush=True)
    job.update({"status": "completed", "finished": True, "elapsed_sec": elapsed})


# ── 分块上传三步接口（解决 Cloud Run ≤32 MB 单请求限制）────────────────────

@app.post("/admin/db-import-start")
async def admin_db_import_start(request: Request, compressed: Optional[str] = None):
    """第一步：创建上传会话，返回 session_id。
    ?compressed=gzip 时 finish 阶段自动解压。
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "forbidden"})
    session_id = uuid.uuid4().hex[:12]
    # chunks dict 按索引存储，支持并发乱序到达
    _import_sessions[session_id] = {"chunks": {}, "compressed": compressed}
    return {"session_id": session_id}


@app.post("/admin/db-import-chunk/{session_id}/{chunk_index}")
async def admin_db_import_chunk(session_id: str, chunk_index: int, request: Request):
    """第二步（多个块可并发发送）：按索引存储数据块，无需串行等待。"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "forbidden"})
    sess = _import_sessions.get(session_id)
    if sess is None:
        return JSONResponse(status_code=404, content={"error": "session not found (server may have restarted)"})
    pieces: list = []
    async for chunk in request.stream():
        if chunk:
            pieces.append(chunk)
    sess["chunks"][chunk_index] = b"".join(pieces)
    total_buf = sum(len(v) for v in sess["chunks"].values())
    return {"session_id": session_id, "chunk_index": chunk_index, "buffered_bytes": total_buf}


@app.post("/admin/db-import-finish/{session_id}")
async def admin_db_import_finish(session_id: str, request: Request):
    """第三步：所有块上传完毕后调用。按索引重组数据，可选 gzip 解压，启动后台导入。"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "forbidden"})
    sess = _import_sessions.pop(session_id, None)
    if sess is None:
        return JSONResponse(status_code=404, content={"error": "session not found"})
    chunks_dict: dict = sess["chunks"]
    # 按块索引顺序重组
    data_bytes = b"".join(chunks_dict[i] for i in sorted(chunks_dict.keys()))
    # 按需解压
    if sess.get("compressed") == "gzip":
        import gzip as _gzip
        try:
            data_bytes = _gzip.decompress(data_bytes)
        except Exception as gz_err:
            return JSONResponse(status_code=400, content={"error": f"gzip decompress failed: {gz_err}"})
    job_id = uuid.uuid4().hex[:8]
    _migration_jobs[job_id] = {
        "status": "running",
        "started_at": time.time(),
        "counts": {},
        "bytes_read": 0,
        "total_file_bytes": len(data_bytes),
        "errors": [],
        "finished": False,
    }
    print(f"[bg-import:{job_id}] session={session_id} {len(chunks_dict)}块 "
          f"{len(data_bytes)/1024/1024:.1f}MB compressed={sess.get('compressed')}", flush=True)
    asyncio.ensure_future(_run_import_bg(job_id, data_bytes))
    return {"job_id": job_id, "bytes": len(data_bytes)}


@app.post("/admin/import-from-source-stream")
async def admin_import_from_source_stream(request: Request, data: dict = Body(...)):
    """从源端 /admin/db-export-stream 流式拉取并导入。

    请求体：{ "source_url": "https://xxx.replit.dev", "source_admin_key": "xxx" }

    关键：read timeout=15 分钟、内存稳定 ~10MB、每 500 行 commit 一次。
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "仅完整管理员可使用数据迁移接口"})
    source_url = (data.get("source_url") or "").rstrip("/")
    source_key = data.get("source_admin_key") or ADMIN_KEY
    if not source_url:
        return JSONResponse(status_code=400, content={"error": "source_url required"})

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="DB unavailable")

    counts: Dict[str, int] = {}
    errors: list = []
    current_table: Optional[str] = None
    started_at = time.time()
    bytes_read = 0
    BATCH_COMMIT = 500

    timeout = httpx.Timeout(connect=30.0, read=900.0, write=30.0, pool=300.0)
    async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
        try:
            async with client.stream(
                "GET",
                f"{source_url}/admin/db-export-stream",
                headers={"X-Admin-Key": source_key},
            ) as resp:
                if resp.status_code != 200:
                    body_preview = await resp.aread()
                    return JSONResponse(status_code=502, content={
                        "error": f"源端返回 HTTP {resp.status_code}",
                        "body":  body_preview[:500].decode("utf-8", errors="replace"),
                    })

                conn = await pool.acquire()
                try:
                    tx = conn.transaction()
                    await tx.start()
                    rows_in_tx = 0

                    async for line in resp.aiter_lines():
                        if not line or not line.strip():
                            continue
                        bytes_read += len(line) + 1
                        try:
                            obj = json.loads(line)
                        except Exception as e:
                            errors.append(f"JSON parse: {type(e).__name__}: {line[:120]}")
                            continue

                        if "_meta" in obj:
                            continue
                        if "_table" in obj:
                            tbl = obj["_table"]
                            current_table = tbl if tbl in _EXPORT_TABLES else None
                            if current_table:
                                counts.setdefault(current_table, 0)
                            continue
                        if "_error" in obj:
                            errors.append(f"source error: {obj['_error']}")
                            continue
                        if not current_table:
                            continue
                        try:
                            await _upsert_one_row(conn, current_table, obj)
                            counts[current_table] = counts.get(current_table, 0) + 1
                            rows_in_tx += 1
                        except Exception as e:
                            errors.append(f"{current_table} row failed: {type(e).__name__}: {e}")

                        if rows_in_tx >= BATCH_COMMIT:
                            await tx.commit()
                            tx = conn.transaction()
                            await tx.start()
                            rows_in_tx = 0

                    await tx.commit()
                finally:
                    await pool.release(conn)

        except httpx.ReadTimeout:
            return JSONResponse(status_code=504, content={
                "error": "源端响应读超时（>15 分钟），可能数据量过大或网络中断",
                "imported_so_far": counts,
                "bytes_read": bytes_read,
            })
        except Exception as e:
            return JSONResponse(status_code=500, content={
                "error": f"流式拉取失败: {type(e).__name__}: {e}",
                "imported_so_far": counts,
                "bytes_read": bytes_read,
            })

    try: await load_accounts_from_db()
    except: pass
    try: await load_keys_from_db()
    except: pass
    try: await load_cf_proxies_from_db()
    except: pass

    if counts.get("jb_accounts", 0) > 0:
        try:
            async with pool.acquire() as conn2:
                rows = await conn2.fetch(
                    "SELECT id FROM jb_accounts WHERE COALESCE(last_quota_check, 0) = 0 LIMIT 5000"
                )
                _schedule_quota_checks_for_ids({r["id"] for r in rows}, label="stream-import入池检测")
        except Exception:
            pass

    return {
        "success":  True,
        "imported": counts,
        "errors":   errors[:50],
        "stats": {
            "elapsed_sec":  round(time.time() - started_at, 1),
            "bytes_read":   bytes_read,
            "errors_total": len(errors),
        },
    }


# ── 后台迁移任务（绕过 Autoscale 5 分钟代理超时）────────────────────────
_migration_jobs: Dict[str, dict] = {}
# ── 分块上传会话缓冲（Cloud Run 单请求 ≤32 MB，大文件必须分块）───────────
_import_sessions: Dict[str, bytearray] = {}


async def _run_migration_bg(job_id: str, source_url: str, source_key: str) -> None:
    """后台执行迁移，进度实时写入 _migration_jobs[job_id]，不占用 HTTP 连接。"""
    print(f"[bg-migration:{job_id}] 启动，source={source_url}", flush=True)
    job = _migration_jobs[job_id]
    pool = await _get_db_pool()
    if not pool:
        job.update({"status": "failed", "error": "DB unavailable", "finished": True})
        print(f"[bg-migration:{job_id}] 失败：DB unavailable", flush=True)
        return

    BATCH_INSERT = 500   # 每次 executemany 的行数
    BATCH_COMMIT = 5000  # 每次 COMMIT 的行数（executemany 后才计数）

    timeout = httpx.Timeout(connect=30.0, read=900.0, write=30.0, pool=300.0)
    async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
        try:
            async with client.stream(
                "GET",
                f"{source_url}/admin/db-export-stream",
                headers={"X-Admin-Key": source_key},
            ) as resp:
                if resp.status_code != 200:
                    body_preview = await resp.aread()
                    job.update({
                        "status": "failed",
                        "error": f"源端返回 HTTP {resp.status_code}: {body_preview[:200].decode('utf-8', errors='replace')}",
                        "finished": True,
                    })
                    return

                conn = await pool.acquire()
                current_table: Optional[str] = None
                row_buffer: list = []
                # 用列表包装事务对象，让内嵌函数可以替换它
                tx_box = [conn.transaction()]
                await tx_box[0].start()
                rows_in_tx = [0]

                async def _flush(tbl: Optional[str]) -> None:
                    """将 row_buffer 批量写入 tbl，清空 buffer。"""
                    if not row_buffer or not tbl:
                        row_buffer.clear()
                        return

                    # ── pokeball_members 特殊处理：用 ball_key JOIN 查目标库 id ──
                    if tbl == "pokeball_members":
                        if "ball_key" in row_buffer[0]:
                            sql_pm = (
                                "INSERT INTO pokeball_members (pokeball_id, member_key)"
                                " SELECT p.id, $2 FROM pokeballs p WHERE p.ball_key = $1"
                                " ON CONFLICT (pokeball_id, member_key) DO NOTHING"
                            )
                            pm_args = [(r.get("ball_key"), r.get("member_key")) for r in row_buffer]
                        else:
                            # 旧格式：跳过 id 列（避免 PK 冲突）
                            sql_pm = (
                                "INSERT INTO pokeball_members (pokeball_id, member_key)"
                                " VALUES ($1, $2)"
                                " ON CONFLICT (pokeball_id, member_key) DO NOTHING"
                            )
                            pm_args = [(r.get("pokeball_id"), r.get("member_key")) for r in row_buffer]
                        try:
                            await conn.executemany(sql_pm, pm_args)
                            n = len(row_buffer)
                            job["counts"][tbl] = job["counts"].get(tbl, 0) + n
                            rows_in_tx[0] += n
                        except Exception as ex:
                            job["errors"].append(f"{tbl} batch({len(row_buffer)}): {type(ex).__name__}: {ex}")
                            try: await tx_box[0].rollback()
                            except Exception: pass
                            tx_box[0] = conn.transaction()
                            await tx_box[0].start()
                            rows_in_tx[0] = 0
                        row_buffer.clear()
                        if rows_in_tx[0] >= BATCH_COMMIT:
                            await tx_box[0].commit()
                            tx_box[0] = conn.transaction()
                            await tx_box[0].start()
                            rows_in_tx[0] = 0
                        return

                    known = await _get_table_columns(conn, tbl)
                    # 用第一行确定列集合（所有行过滤到相同 known 列）
                    sample = {k: v for k, v in row_buffer[0].items() if k in known}
                    cols = list(sample.keys())
                    if not cols:
                        row_buffer.clear()
                        return

                    conflict_col   = _TABLE_CONFLICT_COL.get(tbl)
                    conflict_multi = _TABLE_CONFLICT_MULTI.get(tbl)
                    ph = "(" + ", ".join(f"${i+1}" for i in range(len(cols))) + ")"
                    cn = ", ".join(f'"{c}"' for c in cols)

                    if conflict_multi and all(c in cols for c in conflict_multi):
                        target = "(" + ", ".join(f'"{c}"' for c in conflict_multi) + ")"
                        uc = [c for c in cols if c not in conflict_multi]
                        sc = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in uc)
                        sql = (f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT {target} '
                               f'DO UPDATE SET {sc}') if uc else (
                               f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT {target} DO NOTHING')
                    elif conflict_col and conflict_col in cols:
                        uc = [c for c in cols if c != conflict_col]
                        sc = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in uc)
                        sql = (f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT ("{conflict_col}") '
                               f'DO UPDATE SET {sc}') if uc else (
                               f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT ("{conflict_col}") DO NOTHING')
                    else:
                        sql = f'INSERT INTO "{tbl}" ({cn}) VALUES {ph} ON CONFLICT DO NOTHING'

                    args = [
                        tuple(_coerce_json_value(r.get(c)) for c in cols)
                        for r in row_buffer
                    ]
                    try:
                        await conn.executemany(sql, args)
                        n = len(row_buffer)
                        job["counts"][tbl] = job["counts"].get(tbl, 0) + n
                        rows_in_tx[0] += n
                    except Exception as ex:
                        job["errors"].append(f"{tbl} batch({len(row_buffer)}): {type(ex).__name__}: {ex}")
                        try: await tx_box[0].rollback()
                        except Exception: pass
                        tx_box[0] = conn.transaction()
                        await tx_box[0].start()
                        rows_in_tx[0] = 0
                    row_buffer.clear()

                    if rows_in_tx[0] >= BATCH_COMMIT:
                        await tx_box[0].commit()
                        tx_box[0] = conn.transaction()
                        await tx_box[0].start()
                        rows_in_tx[0] = 0

                try:
                    async for line in resp.aiter_lines():
                        if not line or not line.strip():
                            continue
                        job["bytes_read"] += len(line) + 1
                        try:
                            obj = json.loads(line)
                        except Exception as e:
                            job["errors"].append(f"JSON parse: {type(e).__name__}: {line[:120]}")
                            continue

                        if "_meta" in obj:
                            continue
                        if "_table" in obj:
                            await _flush(current_table)
                            tbl = obj["_table"]
                            current_table = tbl if tbl in _EXPORT_TABLES else None
                            if current_table:
                                job["counts"].setdefault(current_table, 0)
                            continue
                        if "_error" in obj:
                            job["errors"].append(f"source error: {obj['_error']}")
                            continue
                        if not current_table:
                            continue

                        row_buffer.append(obj)
                        if len(row_buffer) >= BATCH_INSERT:
                            await _flush(current_table)

                    await _flush(current_table)
                    await tx_box[0].commit()
                    # 重置所有 SERIAL 序列，防止新行与已导入数据 id 冲突
                    seq_msgs = await _reset_serial_sequences(conn)
                    for m in seq_msgs:
                        print(m, flush=True)
                finally:
                    await pool.release(conn)

        except httpx.ReadTimeout:
            job.update({
                "status": "failed",
                "error": "源端响应读超时（>15 分钟），数据量过大或网络中断",
                "finished": True,
                "elapsed_sec": round(time.time() - job["started_at"], 1),
            })
            return
        except Exception as e:
            job.update({
                "status": "failed",
                "error": f"流式拉取失败: {type(e).__name__}: {e}",
                "finished": True,
                "elapsed_sec": round(time.time() - job["started_at"], 1),
            })
            return

    try: await load_accounts_from_db()
    except: pass
    try: await load_keys_from_db()
    except: pass
    try: await load_cf_proxies_from_db()
    except: pass

    if job["counts"].get("jb_accounts", 0) > 0:
        try:
            async with pool.acquire() as conn2:
                rows = await conn2.fetch(
                    "SELECT id FROM jb_accounts WHERE COALESCE(last_quota_check, 0) = 0 LIMIT 5000"
                )
                _schedule_quota_checks_for_ids({r["id"] for r in rows}, label="bg-migration入池检测")
        except Exception:
            pass

    total = sum(job["counts"].values())
    elapsed = round(time.time() - job["started_at"], 1)
    print(f"[bg-migration:{job_id}] 完成：{total} 行 / {elapsed}s / 错误 {len(job['errors'])} 个", flush=True)
    job.update({
        "status": "completed",
        "finished": True,
        "elapsed_sec": elapsed,
    })


@app.post("/admin/start-migration-bg")
async def admin_start_migration_bg(request: Request, data: dict = Body(...)):
    """启动后台迁移，立即返回 job_id（不阻塞 HTTP 连接，绕过 5 分钟代理超时）。
    通过 GET /admin/migration-job/{job_id} 轮询进度。
    """
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "仅完整管理员可使用数据迁移接口"})
    source_url = (data.get("source_url") or "").rstrip("/")
    source_key = data.get("source_admin_key") or ADMIN_KEY
    if not source_url:
        return JSONResponse(status_code=400, content={"error": "source_url required"})

    job_id = uuid.uuid4().hex[:8]
    _migration_jobs[job_id] = {
        "status": "running",
        "started_at": time.time(),
        "counts": {},
        "bytes_read": 0,
        "errors": [],
        "finished": False,
    }
    asyncio.ensure_future(_run_migration_bg(job_id, source_url, source_key))
    return {"job_id": job_id, "status": "started"}


@app.get("/admin/migration-job/{job_id}")
async def admin_migration_job_status(job_id: str, request: Request):
    """轮询后台迁移任务进度（供前端每 2 秒调用一次）。"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"detail": "权限不足"})
    job = _migration_jobs.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"error": "任务不存在或服务已重启（状态丢失）"})
    total_rows = sum(job["counts"].values())
    elapsed = job.get("elapsed_sec") or round(time.time() - job["started_at"], 1)
    return {**job, "total_rows": total_rows, "elapsed_sec": elapsed}


@app.post("/admin/migration-probe-stream")
async def admin_migration_probe_stream(request: Request, data: dict = Body(...)):
    """诊断模式：探测源端 /admin/db-export-stream 是否可达，仅读前 64 KB 用于排错"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    source_url = (data.get("source_url") or "").rstrip("/")
    source_key = data.get("source_admin_key") or ADMIN_KEY
    if not source_url:
        return JSONResponse(status_code=400, content={"error": "source_url required"})

    timeout = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=30.0)
    try:
        async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
            async with client.stream(
                "GET",
                f"{source_url}/admin/db-export-stream",
                headers={"X-Admin-Key": source_key},
            ) as resp:
                preview = b""
                async for chunk in resp.aiter_bytes(chunk_size=8192):
                    preview += chunk
                    if len(preview) >= 64 * 1024:
                        break
                return {
                    "status_code":   resp.status_code,
                    "headers":       dict(resp.headers),
                    "body_preview":  preview[:64 * 1024].decode("utf-8", errors="replace"),
                    "bytes_sampled": len(preview),
                }
    except Exception as e:
        return JSONResponse(status_code=502, content={"error": f"{type(e).__name__}: {e}"})


# ---- Partner API: contribution endpoints ----

_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


async def _run_precheck(email: str, password: str) -> tuple:
    """
    同步探测账号合规性：login → obtainFreeLicense(RD) → provide-access/license/v2
    耗时约 10-25s/账号，在线程池中执行。
    返回 (precheck_result: str, credentials: dict)
      precheck_result 枚举: valid / invalid_login / invalid_country /
                            invalid_proof / invalid_grazie_untrusted / precheck_error
      credentials: 仅 valid 时非空，包含 jwt/license_id/id_token/refresh_token
    """
    from jb_activate import process_account as _pa
    loop = asyncio.get_event_loop()
    logs: list = []

    def _log_cb(msg: str):
        logs.append(msg)

    try:
        result = await loop.run_in_executor(None, lambda: _pa(email, password, log_cb=_log_cb))
    except Exception as exc:
        return "precheck_error", {"error_detail": str(exc)[:300]}

    if result.get("jwt"):
        # 探测成功：返回凭证
        return "valid", {
            "jwt":           result["jwt"],
            "license_id":    result.get("license_id", ""),
            "auth_token":    result.get("id_token", ""),   # id_token 用于 auth_token
            "refresh_token": result.get("refresh_token", ""),
        }

    error = result.get("error", "")
    full_log = "\n".join(logs)

    # 映射错误类型
    if ("登录失败" in error or "登录异常" in error
            or "login" in error.lower()
            or not result.get("id_token")):
        return "invalid_login", {"error_detail": error[:300]}
    if "PAYMENT_PROOF_REQUIRED" in error or "payment_proof" in error.lower():
        return "invalid_proof", {"error_detail": error[:300]}
    if "COUNTRY_IS_RESTRICTED" in error or "country" in error.lower():
        return "invalid_country", {"error_detail": error[:300]}
    if "所有 licenseId 都无法获取 JWT" in error:
        # 检查日志是否全是 492/Untrusted
        if "492" in full_log or "untrusted" in full_log.lower():
            return "invalid_grazie_untrusted", {"error_detail": error[:300]}
        return "precheck_error", {"error_detail": error[:300]}
    return "precheck_error", {"error_detail": error[:300]}


@app.post("/api/partner/contribute/submit")
async def partner_submit(request: Request):
    partner_row, body_str = await _partner_auth(request)
    partner_id = partner_row["id"]
    body_hash = hashlib.sha256(body_str.encode()).hexdigest()
    try:
        body = json.loads(body_str)
    except Exception:
        await _audit_log(partner_id, "POST", "/api/partner/contribute/submit", 400, body_hash)
        raise HTTPException(status_code=400, detail={"error": "invalid_json"})

    idempotency_key = (body.get("idempotency_key") or "")[:128]
    activation_mode = body.get("activation_mode", "immediate")
    if activation_mode not in ("immediate", "stockpile"):
        activation_mode = "immediate"
    accounts_raw = body.get("accounts")
    if not isinstance(accounts_raw, list) or len(accounts_raw) == 0:
        await _audit_log(partner_id, "POST", "/api/partner/contribute/submit", 400, body_hash)
        raise HTTPException(status_code=400, detail={"error": "empty_accounts_array"})
    if len(accounts_raw) > 20:
        # 每号 10-25s，限制批量以防超时
        await _audit_log(partner_id, "POST", "/api/partner/contribute/submit", 413, body_hash)
        raise HTTPException(status_code=413, detail={"error": "batch_too_large", "max": 20})

    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=500, detail={"error": "internal_error"})

    # Idempotency check
    if idempotency_key:
        async with pool.acquire() as conn:
            cached = await conn.fetchrow(
                "SELECT body_hash, response_json FROM partner_idempotency WHERE idempotency_key=$1 AND partner_id=$2",
                idempotency_key, partner_id
            )
        if cached:
            if cached["body_hash"] != body_hash:
                raise HTTPException(status_code=409, detail={"error": "idempotency_mismatch"})
            resp = json.loads(cached["response_json"])
            resp["idempotent_replay"] = True
            await _audit_log(partner_id, "POST", "/api/partner/contribute/submit", 200, body_hash)
            return JSONResponse(content=resp)

    results = []
    precheck_rejections_by_result: dict = {}
    seen_emails: set = set()
    now_ms = _ms()

    for entry in accounts_raw:
        if not isinstance(entry, dict):
            results.append({"email": "", "status": "rejected", "reason": "invalid_entry"})
            continue
        email = (entry.get("email") or "").lower().strip()
        password = (entry.get("password") or "")
        note = (entry.get("note") or "")[:200]
        if not _EMAIL_RE.match(email):
            results.append({"email": email, "status": "rejected", "reason": "invalid_email"})
            continue
        if not (6 <= len(password) <= 200):
            results.append({"email": email, "status": "rejected", "reason": "invalid_password"})
            continue
        if email in seen_emails:
            results.append({"email": email, "status": "rejected", "reason": "duplicate_in_batch"})
            continue
        seen_emails.add(email)

        # 重复检查
        try:
            async with pool.acquire() as conn:
                dup_contrib = await conn.fetchval(
                    "SELECT 1 FROM account_contributions WHERE email=$1", email)
            if dup_contrib:
                results.append({"email": email, "status": "rejected", "reason": "duplicate_in_contributions"})
                continue
        except Exception:
            results.append({"email": email, "status": "rejected", "reason": "lookup_error"})
            continue

        # ── 同步合规检测（10-25s/号）──
        print(f"[precheck] 开始探测 {email}")
        pr_result, pr_creds = await _run_precheck(email, password)
        print(f"[precheck] {email} → {pr_result}")

        if pr_result != "valid":
            # 拒绝：落 partner_precheck_rejections 表
            precheck_rejections_by_result[pr_result] = precheck_rejections_by_result.get(pr_result, 0) + 1
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        """INSERT INTO partner_precheck_rejections
                           (partner_id, email, precheck_result, error_detail, created_at_ms)
                           VALUES ($1,$2,$3,$4,$5)""",
                        partner_id, email, pr_result,
                        pr_creds.get("error_detail", "")[:500], now_ms
                    )
            except Exception as e:
                print(f"[precheck] 记录拒绝失败: {e}")
            results.append({
                "email": email,
                "status": "rejected",
                "precheck_result": pr_result,
            })
            continue

        # ── 通过检测：加密密码 + 入库 account_contributions（active）+ 导入凭证 ──
        try:
            password_enc = _partner_encrypt(password)
        except Exception:
            results.append({"email": email, "status": "rejected", "reason": "encryption_failed"})
            continue

        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    """INSERT INTO account_contributions
                       (partner_id, email, password_enc, status, activation_mode,
                        idempotency_key, custom_note, created_at_ms, updated_at_ms,
                        activation_completed_at)
                       VALUES ($1,$2,$3,'active',$4,$5,$6,$7,$8,$9) RETURNING id""",
                    partner_id, email, password_enc, activation_mode,
                    idempotency_key or None, note, now_ms, now_ms, now_ms
                )
            contrib_id = str(row["id"])
        except Exception as e:
            results.append({"email": email, "status": "rejected", "reason": "insert_error"})
            continue

        # 将凭证写入 jb_accounts 和内存池
        acc_id = await _import_partner_credential(pr_creds)
        if acc_id:
            try:
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE account_contributions SET linked_credential_ids=$1, aif_license_count=1 WHERE id=$2",
                        json.dumps([acc_id]), contrib_id
                    )
            except Exception:
                pass

        results.append({"email": email, "status": "accepted", "id": contrib_id})

    accepted = [r for r in results if r["status"] == "accepted"]
    precheck_rejected = [r for r in results if r.get("precheck_result")]

    response = {
        "success": True,
        "partner_id": partner_id,
        "accepted": len(accepted),
        "rejected_count": len(results) - len(accepted),
        "precheck_rejections_total": len(precheck_rejected),
        "precheck_rejections_by_result": precheck_rejections_by_result,
        "activation_mode": activation_mode,
        "results": results,
        "submitted_at": now_ms,
    }

    # Cache idempotency
    if idempotency_key:
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO partner_idempotency (idempotency_key, partner_id, body_hash, response_json) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                    idempotency_key, partner_id, body_hash, json.dumps(response)
                )
        except Exception:
            pass

    await _audit_log(partner_id, "POST", "/api/partner/contribute/submit", 200, body_hash)
    return JSONResponse(content=response)


@app.post("/api/partner/contribute/batch")
async def partner_batch_query(request: Request):
    partner_row, body_str = await _partner_auth(request)
    partner_id = partner_row["id"]
    try:
        body = json.loads(body_str)
    except Exception:
        raise HTTPException(status_code=400, detail={"error": "invalid_json"})
    idempotency_key = (body.get("idempotency_key") or "").strip()
    if not idempotency_key:
        raise HTTPException(status_code=400, detail={"error": "missing_idempotency_key"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=500, detail={"error": "internal_error"})
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM account_contributions WHERE idempotency_key=$1 AND partner_id=$2 ORDER BY created_at_ms",
            idempotency_key, partner_id
        )
    await _audit_log(partner_id, "POST", "/api/partner/contribute/batch", 200)
    return {
        "success": True,
        "idempotency_key": idempotency_key,
        "total": len(rows),
        "accounts": [_contribution_to_dict(r) for r in rows],
    }


@app.get("/api/partner/contribute/account")
async def partner_account_query(request: Request, email: str = ""):
    partner_row, _ = await _partner_auth(request)
    partner_id = partner_row["id"]
    email = email.lower().strip()
    if not email or not _EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail={"error": "invalid_email"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=500, detail={"error": "internal_error"})
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM account_contributions WHERE email=$1 AND partner_id=$2",
            email, partner_id
        )
    if not row:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    d = _contribution_to_dict(row)
    d["partner_id"] = partner_id

    # 若已 active，顺带返回实际凭证（JWT + license_id），供对端 bind task 导入
    if row["status"] == "active":
        try:
            linked_ids = json.loads(row["linked_credential_ids"] or "[]")
        except Exception:
            linked_ids = []
        if linked_ids:
            async with pool.acquire() as conn:
                cred_rows = await conn.fetch(
                    "SELECT id, license_id, jwt, auth_token FROM jb_accounts WHERE id = ANY($1::text[])",
                    linked_ids
                )
            credentials = [
                {
                    "license_id":  r["license_id"] or r["id"],
                    "jwt":         r["jwt"] or "",
                    "auth_token":  r["auth_token"] or "",
                }
                for r in cred_rows if r["jwt"]
            ]
            d["credentials"] = credentials
            d["credentials_count"] = len(credentials)
        else:
            d["credentials"] = []
            d["credentials_count"] = 0

    await _audit_log(partner_id, "GET", f"/api/partner/contribute/account", 200)
    return {"success": True, "account": d}


# ---- Background activation task ----

async def _activate_contribution_by_email(email: str):
    """查找 pending 的 contribution 并激活"""
    pool = await _get_db_pool()
    if not pool:
        return
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM account_contributions WHERE email=$1 AND status='pending' LIMIT 1", email
        )
    if row:
        await _activate_contribution(str(row["id"]), row["email"], row["password_enc"])


async def _activate_contribution(cid: str, email: str, password_enc: str):
    """激活单个 contribution：向合作方查询凭证并直接写入 jb_accounts"""
    pool = await _get_db_pool()
    if not pool:
        return
    now_ms = _ms()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE account_contributions SET status='activating', activation_started_at=$1, updated_at_ms=$2, activation_attempts=activation_attempts+1 WHERE id=$3",
            now_ms, now_ms, cid
        )
    cfg = await _get_client_cfg_for_push()
    if not cfg or not cfg.get("endpoint") or not cfg.get("hmac_secret_enc"):
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE account_contributions SET status='failed', activation_error='合作方配置不完整', updated_at_ms=$1 WHERE id=$2",
                _ms(), cid
            )
        return
    _ep_parsed = urlparse(cfg["endpoint"]); base_url = f"{_ep_parsed.scheme}://{_ep_parsed.netloc}"
    path_with_qs = f"/api/partner/contribute/account?email={_urlquote(email, safe='')}"
    try:
        headers = _partner_headers(cfg, "GET", path_with_qs, b"")
        resp = await http_client.get(f"{base_url}{path_with_qs}", headers=headers, timeout=20)
    except Exception as e:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE account_contributions SET status='failed', activation_error=$1, updated_at_ms=$2 WHERE id=$3",
                f"查询失败: {e}", _ms(), cid
            )
        return
    if resp.status_code == 404:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE account_contributions SET status='failed', activation_error='合作方未找到该账号', updated_at_ms=$1 WHERE id=$2",
                _ms(), cid
            )
        return
    try:
        data = resp.json()
    except Exception:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE account_contributions SET status='failed', activation_error='合作方响应解析失败', updated_at_ms=$1 WHERE id=$2",
                _ms(), cid
            )
        return
    account = data.get("account") or data
    credentials = account.get("credentials") or data.get("credentials") or []
    if not credentials:
        partner_status = account.get("status", "unknown")
        if partner_status in ("pending", "activating", "stockpiled"):
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE account_contributions SET status='pending', activation_error='', updated_at_ms=$1 WHERE id=$2",
                    _ms(), cid
                )
        else:
            error_msg = account.get("activation_error") or f"对方状态: {partner_status}"
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE account_contributions SET status='failed', activation_error=$1, updated_at_ms=$2 WHERE id=$3",
                    error_msg, _ms(), cid
                )
        return
    done_ms = _ms()
    acc_ids = []
    for cred in credentials:
        acc_id = await _import_partner_credential(cred)
        if acc_id:
            acc_ids.append(acc_id)
    if acc_ids:
        async with pool.acquire() as conn:
            await conn.execute(
                """UPDATE account_contributions SET status='active', aif_license_count=$1, linked_credential_ids=$2,
                   activation_completed_at=$3, updated_at_ms=$4 WHERE id=$5""",
                len(acc_ids), json.dumps(acc_ids), done_ms, done_ms, cid
            )
    else:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE account_contributions SET status='failed', activation_error='凭证数据不完整', updated_at_ms=$1 WHERE id=$2",
                _ms(), cid
            )


# ==================== 后备隐藏能源 ====================

@app.post("/key/donate-jb-account")
async def donate_jb_account(request: Request):
    """用户捐献 JetBrains 账号邮密（需 Discord 登录），审核通过后奖励 10 圣人点数"""
    body = await request.json()
    discord_token = (body.get("discord_token") or "").strip()
    jb_email = (body.get("email") or "").strip()
    jb_password = (body.get("password") or "").strip()
    if not jb_email or not jb_password:
        raise HTTPException(status_code=400, detail="邮箱或密码不能为空")
    dc_password, dc_info = _require_dc_token(discord_token)
    dc_tag = dc_info.get("user_tag", "")
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO donated_jb_accounts (jb_email, jb_password, dc_password, dc_tag)
                   VALUES ($1, $2, $3, $4)""",
                jb_email, jb_password, dc_password, dc_tag,
            )
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="该 JetBrains 邮箱已提交过，请勿重复捐献")
    return {"success": True, "message": "提交成功，等待管理员审核"}


@app.get("/admin/donate-accounts")
async def admin_list_donate_accounts(request: Request):
    """管理员查看后备隐藏能源列表（待验证 / 已验证）"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        return {"pending": [], "approved": []}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, jb_email, jb_password, dc_tag, status,
                      submitted_at, reviewed_at,
                      admin_used, admin_used_at
               FROM donated_jb_accounts ORDER BY submitted_at DESC"""
        )
    pending, approved = [], []
    for r in rows:
        item = {
            "id":           r["id"],
            "jb_email":     r["jb_email"],
            "jb_password":  r["jb_password"],
            "dc_tag":       r["dc_tag"],
            "submitted_at": r["submitted_at"].isoformat() if r["submitted_at"] else None,
            "reviewed_at":  r["reviewed_at"].isoformat() if r["reviewed_at"] else None,
            "admin_used":     bool(r["admin_used"]),
            "admin_used_at":  r["admin_used_at"].isoformat() if r["admin_used_at"] else None,
        }
        (approved if r["status"] == "approved" else pending).append(item)
    return {"pending": pending, "approved": approved}


@app.post("/admin/donate-accounts/{account_id}/approve")
async def admin_approve_donate_account(account_id: int, request: Request):
    """管理员审核通过：奖励 10 圣人点数并移入已验证区"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT dc_password, dc_tag, status FROM donated_jb_accounts WHERE id=$1", account_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="记录不存在")
        if row["status"] == "approved":
            raise HTTPException(status_code=409, detail="已审核通过，不可重复操作")
        await conn.execute(
            "UPDATE donated_jb_accounts SET status='approved', reviewed_at=NOW() WHERE id=$1",
            account_id,
        )
        dc_password = row["dc_password"]
        dc_tag = row["dc_tag"] or ""
        await conn.execute(
            "INSERT INTO user_passwords (password) VALUES ($1) ON CONFLICT DO NOTHING",
            dc_password,
        )
        pts_row = await conn.fetchrow(
            """INSERT INTO saint_points (password, points, total_earned, dc_tag, updated_at)
               VALUES ($1, 10, 10, $2, NOW())
               ON CONFLICT (password) DO UPDATE
               SET points = saint_points.points + 10,
                   total_earned = saint_points.total_earned + 10,
                   dc_tag = CASE WHEN $2 <> '' THEN $2 ELSE saint_points.dc_tag END,
                   updated_at = NOW()
               RETURNING points, total_earned""",
            dc_password, dc_tag,
        )
    return {"success": True, "message": "已审核通过，已奖励 10 圣人点数", "points": pts_row["points"]}


@app.delete("/admin/donate-accounts/{account_id}/reject")
async def admin_reject_donate_account(account_id: int, request: Request):
    """管理员拒绝：从数据库删除该条捐献记录"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM donated_jb_accounts WHERE id=$1", account_id
        )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"success": True}


@app.post("/admin/donate-accounts/{account_id}/mark-used")
async def admin_mark_donate_account_used(account_id: int, request: Request):
    """管理员标记已审核的邮密为「已使用」（幂等，可重复调用，不可撤销）。"""
    if request.headers.get("X-Admin-Key", "") != ADMIN_KEY:
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    pool = await _get_db_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="数据库不可用")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, admin_used FROM donated_jb_accounts WHERE id=$1", account_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="记录不存在")
        if row["status"] != "approved":
            raise HTTPException(status_code=400, detail="只有已审核通过的记录才能标记为已使用")
        result = await conn.fetchrow(
            """UPDATE donated_jb_accounts
               SET admin_used=TRUE, admin_used_at=COALESCE(admin_used_at, NOW())
               WHERE id=$1
               RETURNING admin_used, admin_used_at""",
            account_id,
        )
    was_already = bool(row["admin_used"])
    return {
        "success": True,
        "already_marked": was_already,
        "admin_used_at": result["admin_used_at"].isoformat() if result["admin_used_at"] else None,
    }


class GrazieProbeRequest(BaseModel):
    email: str
    password: str


@app.post("/admin/activate/grazie-probe")
async def admin_grazie_probe(req: GrazieProbeRequest):
    """诊断端点：用真实账号测试所有 Grazie 认证格式，找出哪种格式能成功获取 JWT。
    返回每种格式的测试结果，帮助确定正确的激活方法。"""
    import concurrent.futures, base64 as _b64, re as _re

    results = {}

    def sync_probe(email, password):
        from jb_activate import jba_login, oauth_pkce, decode_id_token, register_grazie, obtain_trial, \
            ENCRYPTED_HOSTNAME, ENCRYPTED_USERNAME, MACHINE_ID, MACHINE_UUID
        import time, requests as _req

        JB = "https://account.jetbrains.com"
        AI = "https://api.jetbrains.ai"

        out = {}
        # 登录
        s, h = jba_login(email, password)
        if not s:
            return {"error": "登录失败"}

        # OAuth
        id_token, refresh_token = oauth_pkce(s)
        if not id_token:
            return {"error": "OAuth失败"}
        out["id_token_prefix"] = id_token[:40] + "..."

        claims = decode_id_token(id_token)
        user_id = claims.get("user_id", "")
        out["user_id"] = user_id

        # 注册 Grazie
        reg_status, reg_body = register_grazie(id_token)
        out["register"] = f"HTTP {reg_status} | {reg_body}"

        # 获取 RR EncodedAsset
        salt = str(int(time.time() * 1000))
        r_rr = _req.get(f"{JB}/lservice/rpc/obtainTrial.action", params={
            "productFamilyId": "RR", "userId": user_id,
            "hostName": ENCRYPTED_HOSTNAME, "salt": salt,
            "ideProductCode": "RR", "buildDate": "20250416", "clientVersion": "21",
            "secure": "false", "userName": ENCRYPTED_USERNAME,
            "buildNumber": "2025.1.1 Build RR-251.25410.100", "version": "2025100",
            "machineId": MACHINE_ID, "productCode": "RR",
            "expiredLicenseDays": "0", "machineUUID": MACHINE_UUID, "checkedOption": "AGREEMENT",
        }, headers={"User-Agent": "local"}, timeout=15)
        asset_m = _re.search(r'<data>(.*?)</data>', r_rr.text, _re.DOTALL)
        encoded_asset = asset_m.group(1).strip() if asset_m else ""
        out["encoded_asset_len"] = len(encoded_asset)

        # 方法1: /licenses/tokens REST API（最准确）
        for params in [{"productCode": "AIP"}, {"product": "AIP"}, {}]:
            r_tok = s.get(f"{JB}/licenses/tokens", params=params, timeout=15)
            out[f"tokens_api_{list(params.values())[0] if params else 'noparams'}"] = (
                f"HTTP {r_tok.status_code} | {r_tok.text[:300]}"
            )
            if r_tok.status_code == 200:
                break

        # 方法2: /licenses HTML 解析
        r_lic = s.get(f"{JB}/licenses", timeout=15)
        ids1 = _re.findall(r'id="license-([A-Z0-9\-]{4,20})"', r_lic.text)
        ids2 = _re.findall(r'"licenseId"\s*:\s*"([A-Z0-9\-]{4,20})"', r_lic.text)
        license_ids_html = list(dict.fromkeys(ids1 + ids2))
        out["license_ids_html"] = license_ids_html
        
        # 合并所有 licenseId
        try:
            tok_data = r_tok.json()
            tok_lics = tok_data.get("licenseList", tok_data.get("licenses", tok_data if isinstance(tok_data, list) else []))
            api_ids = [l.get("licenseId", "") for l in tok_lics if l.get("licenseId")]
        except Exception:
            api_ids = []
        license_ids = list(dict.fromkeys(api_ids + license_ids_html))
        out["license_ids"] = license_ids
        out["license_ids_api"] = api_ids

        hdr = {
            "Authorization": f"Bearer {id_token}",
            "User-Agent": "ktor-client",
            "Content-Type": "application/json",
        }
        url_v2 = f"{AI}/auth/jetbrains-jwt/provide-access/license/v2"
        url_v1 = f"{AI}/auth/jetbrains-jwt/provide-access/license"

        probe_results = {}
        # 格式 A: 标准空 licenseId (free tier)
        r = _req.post(url_v2, json={"licenseId": ""}, headers=hdr, timeout=15)
        probe_results["A_empty_licenseId"] = f"HTTP {r.status_code} | {r.text[:120]}"

        # 格式 B: 标准 licenseId (从页面提取)
        for lid in license_ids[:3]:
            r = _req.post(url_v2, json={"licenseId": lid}, headers=hdr, timeout=15)
            probe_results[f"B_licenseId_{lid}"] = f"HTTP {r.status_code} | {r.text[:120]}"

        # 格式 C: licenseId + certificate (EncodedAsset)
        for lid in license_ids[:2]:
            r = _req.post(url_v2, json={"licenseId": lid, "certificate": encoded_asset}, headers=hdr, timeout=15)
            probe_results[f"C_licenseId+cert_{lid}"] = f"HTTP {r.status_code} | {r.text[:120]}"

        # 格式 D: licenseKey = EncodedAsset as licenseId
        if encoded_asset:
            r = _req.post(url_v2, json={"licenseId": encoded_asset[:100]}, headers=hdr, timeout=15)
            probe_results["D_encoded_as_id"] = f"HTTP {r.status_code} | {r.text[:120]}"

        # 格式 E: v1 端点 (without /v2) + licenseId
        for lid in license_ids[:2]:
            r = _req.post(url_v1, json={"licenseId": lid}, headers=hdr, timeout=15)
            probe_results[f"E_v1_{lid}"] = f"HTTP {r.status_code} | {r.text[:120]}"

        # 格式 F: v1 空 licenseId
        r = _req.post(url_v1, json={"licenseId": ""}, headers=hdr, timeout=15)
        probe_results["F_v1_empty"] = f"HTTP {r.status_code} | {r.text[:120]}"

        # 格式 G: 产品码前缀格式 "RR-XXXXXXXXXX"
        for lid in license_ids[:2]:
            for prefix in ["RR", "CL", "WS"]:
                full_id = f"{prefix}-{lid}"
                r = _req.post(url_v2, json={"licenseId": full_id}, headers=hdr, timeout=15)
                probe_results[f"G_prefixed_{full_id}"] = f"HTTP {r.status_code} | {r.text[:80]}"

        # 格式 H: 直接用 id_token 作为 grazie-authenticate-jwt
        hdr2 = {"grazie-authenticate-jwt": id_token, "User-Agent": "ktor-client", "Content-Length": "0"}
        r = _req.post(f"{AI}/user/v5/quota/get", headers=hdr2, timeout=15)
        probe_results["H_id_token_as_jwt"] = f"HTTP {r.status_code} | {r.text[:120]}"

        # 格式 I: /auth/jetbrains-jwt/license/obtain/grazie-lite（★ 最关键链路！）
        # 流程: obtain/grazie-lite → JBALicense.licenseId → provide-access/license/v2 → Grazie JWT
        grazie_lite_url = f"{AI}/auth/jetbrains-jwt/license/obtain/grazie-lite"
        grazie_lite_obtained_id = None
        # I1: 无 body（最可能）
        r = _req.post(grazie_lite_url, headers=hdr, timeout=15)
        probe_results["I1_grazie_lite_no_body"] = f"HTTP {r.status_code} | {r.text[:400]}"
        if r.status_code == 200:
            try:
                data_i1 = r.json()
                lic_obj = data_i1.get("license", data_i1)
                probe_results["I1_grazie_lite_license_obj"] = json.dumps(lic_obj)[:400]
                if isinstance(lic_obj, dict):
                    grazie_lite_obtained_id = lic_obj.get("licenseId", None)
                    probe_results["I1_grazie_lite_licenseId"] = str(grazie_lite_obtained_id)
                    # 核心链路：用 licenseId 调 v2 端点
                    if grazie_lite_obtained_id:
                        r_v2 = _req.post(url_v2, json={"licenseId": grazie_lite_obtained_id}, headers=hdr, timeout=15)
                        probe_results["I1_grazie_lite→v2"] = f"HTTP {r_v2.status_code} | {r_v2.text[:300]}"
                        # 也尝试 v1 端点
                        r_v1 = _req.post(url_v1, json={"licenseId": grazie_lite_obtained_id}, headers=hdr, timeout=15)
                        probe_results["I1_grazie_lite→v1"] = f"HTTP {r_v1.status_code} | {r_v1.text[:300]}"
            except Exception as e:
                probe_results["I1_grazie_lite_parse_err"] = str(e)
        # I2: 空 JSON body
        r = _req.post(grazie_lite_url, json={}, headers=hdr, timeout=15)
        probe_results["I2_grazie_lite_empty_json"] = f"HTTP {r.status_code} | {r.text[:300]}"

        # 格式 J: /auth/ls/provide-access（票据格式：ticket + sign）
        # 先检测端点是否存在，再尝试测试
        ls_url = f"{AI}/auth/ls/provide-access"
        r = _req.post(ls_url, json={"ticket": "test", "sign": "test"}, headers=hdr, timeout=15)
        probe_results["J1_ls_ticket_test"] = f"HTTP {r.status_code} | {r.text[:200]}"

        # 格式 K: /auth/jetbrains/provide-access（通用 JBA 端点）
        jb_generic_url = f"{AI}/auth/jetbrains/provide-access"
        r = _req.post(jb_generic_url, json={}, headers=hdr, timeout=15)
        probe_results["K1_jetbrains_provide_access"] = f"HTTP {r.status_code} | {r.text[:200]}"
        # K2: GET
        r = _req.get(jb_generic_url, headers=hdr, timeout=15)
        probe_results["K2_jetbrains_provide_access_GET"] = f"HTTP {r.status_code} | {r.text[:200]}"

        out["probe_results"] = probe_results

        # 格式 Z: /auth/ides/provide-access（全新端点！无需 Bearer token）
        ides_url = f"{AI}/auth/ides/provide-access"
        ides_hdr = {
            "Content-Type": "application/json",
            "User-Agent": "IntelliJIdea/251.25410.109 (JetBrains s.r.o.)",
            "Accept": "application/json",
        }
        if encoded_asset:
            # 解码 EncodedAsset 的各种格式
            try:
                raw_bytes = bytes.fromhex(encoded_asset.strip())
                b64_str = _b64.b64encode(raw_bytes).decode()
                is_printable = all(0x20 <= c < 0x7F or c in (9, 10, 13) for c in raw_bytes[:64])
                out["encoded_asset_first_bytes_hex"] = encoded_asset[:64] + "..."
                out["encoded_asset_b64_preview"] = b64_str[:80] + "..."
                out["encoded_asset_is_printable"] = is_printable
                if raw_bytes[:5] in (b"<?xml", b"<lice"):
                    out["encoded_asset_format"] = "XML"
                elif raw_bytes[:2] == b"PK":
                    out["encoded_asset_format"] = "ZIP"
                elif raw_bytes[0] == 0x30:
                    out["encoded_asset_format"] = "DER/ASN.1"
                elif raw_bytes[:4] == b"\x1f\x8b\x08\x00":
                    out["encoded_asset_format"] = "GZIP"
                else:
                    out["encoded_asset_format"] = f"unknown(0x{raw_bytes[:4].hex()})"
            except Exception as e:
                raw_bytes = None
                b64_str = encoded_asset
                out["encoded_asset_decode_error"] = str(e)

            ides_tests = {}
            # Z1: hex 原文
            r = _req.post(ides_url, json={"license": encoded_asset}, headers=ides_hdr, timeout=15)
            ides_tests["Z1_hex_raw"] = f"HTTP {r.status_code} | {r.text[:200]}"

            # Z2: base64 编码
            if raw_bytes:
                r = _req.post(ides_url, json={"license": b64_str}, headers=ides_hdr, timeout=15)
                ides_tests["Z2_b64"] = f"HTTP {r.status_code} | {r.text[:200]}"

            # Z3: 文本（如果可打印）
            if raw_bytes and is_printable:
                text_val = raw_bytes.decode("utf-8", errors="replace")
                r = _req.post(ides_url, json={"license": text_val}, headers=ides_hdr, timeout=15)
                ides_tests["Z3_text"] = f"HTTP {r.status_code} | {r.text[:200]}"

            # Z4: 带 Bearer id_token 的 hex
            ides_hdr_auth = {**ides_hdr, "Authorization": f"Bearer {id_token}"}
            r = _req.post(ides_url, json={"license": encoded_asset}, headers=ides_hdr_auth, timeout=15)
            ides_tests["Z4_hex_with_bearer"] = f"HTTP {r.status_code} | {r.text[:200]}"

            # Z5: 带 Bearer id_token 的 b64
            if raw_bytes:
                r = _req.post(ides_url, json={"license": b64_str}, headers=ides_hdr_auth, timeout=15)
                ides_tests["Z5_b64_with_bearer"] = f"HTTP {r.status_code} | {r.text[:200]}"

            out["ides_probe_results"] = ides_tests
        else:
            out["ides_probe_results"] = {"error": "无 EncodedAsset（obtainTrial.action 未返回数据）"}

        return out

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, sync_probe, req.email, req.password)
    return result


@app.post("/admin/activate/ides-probe")
async def admin_ides_probe(req: GrazieProbeRequest):
    """
    专项诊断：测试 /auth/ides/provide-access 端点。
    使用真实账号获取 EncodedAsset，测试各种格式发送到 ides 端点，
    同时也尝试使用所有 6 个 IDE (CL/WS/RM/DB/RD/RR) 的 EncodedAsset。
    """
    def sync_ides_probe(email, password):
        from jb_activate import (jba_login, oauth_pkce, decode_id_token,
                                  register_grazie, obtain_trial_nocard,
                                  get_jwt_from_ides_endpoint, _decode_encoded_asset, JB, AI)
        import time, requests as _req, re as _re, base64 as _b64, json as _json

        out = {"email": email}

        # 步骤1: 登录
        s, h = jba_login(email, password)
        if not s:
            return {"error": "登录失败"}

        # 步骤2: OAuth
        id_token, _ = oauth_pkce(s)
        if not id_token:
            return {"error": "OAuth 失败"}
        claims = decode_id_token(id_token)
        user_id = claims.get("user_id", "")
        out["user_id"] = user_id
        out["id_token_prefix"] = id_token[:40] + "..."

        # 步骤3: 注册 Grazie
        reg_status, reg_body = register_grazie(id_token)
        out["register"] = f"HTTP {reg_status} | {reg_body}"

        # 步骤4: 获取所有 6 个 IDE 的 EncodedAsset
        out["trial_results"] = {}
        successful_assets = []
        for ide_code, prod_code, family_id, build_num, ver in [
            ("CL", "CL", "CL", "2025.1.1 Build CL-251.25410.105", "2025100"),
            ("WS", "WS", "WS", "2025.1.1 Build WS-251.25410.103", "2025100"),
            ("RM", "RM", "RM", "2025.1.1 Build RM-251.25410.102", "2025100"),
            ("DB", "DB", "DB", "2025.1.1 Build DB-251.25410.107", "2025100"),
            ("RD", "RD", "RD", "2025.1.1 Build RD-251.25410.108", "2025100"),
            ("RR", "RR", "RR", "2025.1.1 Build RR-251.25410.100", "2025100"),
        ]:
            try:
                from jb_activate import obtain_trial
                rc, rr, asset = obtain_trial(user_id, ide_code, build_num, ver, prod_code, family_id)
                out["trial_results"][ide_code] = f"{rc}" + (f"({rr})" if rr else "") + f" asset={'✓' if asset else '✗'}"
                if rc in ("OK", "ALREADY_OBTAINED") and asset:
                    successful_assets.append((ide_code, asset))
            except Exception as e:
                out["trial_results"][ide_code] = f"异常: {e}"
            time.sleep(0.3)

        out["successful_assets_count"] = len(successful_assets)
        out["successful_ides"] = [x[0] for x in successful_assets]

        if not successful_assets:
            out["ides_probe"] = {"error": "没有可用的 EncodedAsset"}
            return out

        # 步骤5: 测试 /auth/ides/provide-access
        ides_url = f"{AI}/auth/ides/provide-access"
        ides_hdr = {
            "Content-Type": "application/json",
            "User-Agent": "IntelliJIdea/251.25410.109 (JetBrains s.r.o.)",
            "Accept": "application/json",
        }
        ides_hdr_auth = {**ides_hdr, "Authorization": f"Bearer {id_token}"}

        probe = {}
        for ide_code, asset_hex in successful_assets:
            raw, b64, text = _decode_encoded_asset(asset_hex)

            # 探测资产格式
            fmt = "unknown"
            if raw:
                if raw[:5] in (b"<?xml", b"<lice"):
                    fmt = "XML"
                elif raw[:2] == b"PK":
                    fmt = "ZIP"
                elif raw[0] == 0x30:
                    fmt = "DER"
                else:
                    fmt = f"bin({raw[:4].hex()})"
            probe[f"{ide_code}_format"] = fmt
            probe[f"{ide_code}_raw_len"] = len(raw) if raw else 0
            probe[f"{ide_code}_b64_len"] = len(b64)

            # 不带 Bearer
            r = _req.post(ides_url, json={"license": asset_hex}, headers=ides_hdr, timeout=15)
            probe[f"{ide_code}_hex_noauth"] = f"HTTP {r.status_code} | {r.text[:200]}"

            r = _req.post(ides_url, json={"license": b64}, headers=ides_hdr, timeout=15)
            probe[f"{ide_code}_b64_noauth"] = f"HTTP {r.status_code} | {r.text[:200]}"

            if text:
                r = _req.post(ides_url, json={"license": text}, headers=ides_hdr, timeout=15)
                probe[f"{ide_code}_text_noauth"] = f"HTTP {r.status_code} | {r.text[:200]}"

            # 带 Bearer id_token
            r = _req.post(ides_url, json={"license": b64}, headers=ides_hdr_auth, timeout=15)
            probe[f"{ide_code}_b64_withauth"] = f"HTTP {r.status_code} | {r.text[:200]}"

            # 如果 raw 是 DER/ASN.1，尝试 PEM 格式
            if raw and raw[0] == 0x30:
                pem = "-----BEGIN CERTIFICATE-----\n" + b64 + "\n-----END CERTIFICATE-----"
                r = _req.post(ides_url, json={"license": pem}, headers=ides_hdr, timeout=15)
                probe[f"{ide_code}_pem_noauth"] = f"HTTP {r.status_code} | {r.text[:200]}"

            time.sleep(0.2)

        out["ides_probe"] = probe

        # 步骤6: 也尝试用 get_jwt_from_ides_endpoint 函数
        jwt_token, desc = get_jwt_from_ides_endpoint(successful_assets)
        out["ides_jwt_result"] = {"token": jwt_token[:30] + "..." if jwt_token else None, "desc": desc}

        # 步骤7: 尝试 /auth/jetbrains-jwt/license/obtain/grazie-lite 端点
        grazie_lite_url = f"{AI}/auth/jetbrains-jwt/license/obtain/grazie-lite"
        hdr_bearer = {"Authorization": f"Bearer {id_token}", "User-Agent": "ktor-client",
                      "Content-Type": "application/json"}
        grazie_lite_results = {}
        for body_desc, body in [
            ("no_body", None),
            ("empty_json", {}),
        ]:
            try:
                if body is None:
                    r = _req.post(grazie_lite_url, headers=hdr_bearer, timeout=15)
                else:
                    r = _req.post(grazie_lite_url, json=body, headers=hdr_bearer, timeout=15)
                grazie_lite_results[body_desc] = f"HTTP {r.status_code} | {r.text[:400]}"
                if r.status_code == 200:
                    try:
                        lic_data = r.json()
                        lic_obj = lic_data.get("license", lic_data)
                        lic_str = _json.dumps(lic_obj) if isinstance(lic_obj, dict) else str(lic_obj)
                        # 尝试用这个 license 去 ides 端点
                        ides_hdr2 = {"Content-Type": "application/json",
                                     "User-Agent": "IntelliJIdea/251.25410.109 (JetBrains s.r.o.)"}
                        r2 = _req.post(f"{AI}/auth/ides/provide-access",
                                       json={"license": lic_str}, headers=ides_hdr2, timeout=15)
                        grazie_lite_results[f"{body_desc}_→ides"] = f"HTTP {r2.status_code} | {r2.text[:300]}"
                        # ★ 关键链路：用 JBALicense.licenseId → provide-access/license/v2
                        if isinstance(lic_obj, dict):
                            jba_lic_id = lic_obj.get("licenseId")
                            grazie_lite_results[f"{body_desc}_licenseId"] = str(jba_lic_id)
                            if jba_lic_id:
                                rv2 = _req.post(f"{AI}/auth/jetbrains-jwt/provide-access/license/v2",
                                                json={"licenseId": jba_lic_id}, headers=hdr_bearer, timeout=15)
                                grazie_lite_results[f"{body_desc}_licenseId→v2"] = f"HTTP {rv2.status_code} | {rv2.text[:300]}"
                                rv1 = _req.post(f"{AI}/auth/jetbrains-jwt/provide-access/license",
                                                json={"licenseId": jba_lic_id}, headers=hdr_bearer, timeout=15)
                                grazie_lite_results[f"{body_desc}_licenseId→v1"] = f"HTTP {rv1.status_code} | {rv1.text[:300]}"
                    except Exception as e:
                        grazie_lite_results[f"{body_desc}_parse_err"] = str(e)
            except Exception as e:
                grazie_lite_results[body_desc] = f"异常: {e}"
        out["grazie_lite_probe"] = grazie_lite_results

        # 步骤8: 尝试直接对 AIP 产品码发起试用（可能在无卡情况下获得 AIP license）
        aip_trial_results = {}
        try:
            from jb_activate import obtain_trial
            # 8a: obtainTrial 产品码 AIP
            rc, rr, asset = obtain_trial(user_id, "IU", "2025.1.1 Build IU-251.25410.109", "2025100",
                                          "AIP", "AIP")
            aip_trial_results["obtain_trial_AIP"] = f"rc={rc} reason={rr} asset={'✓' if asset else '✗'}"
        except Exception as e:
            aip_trial_results["obtain_trial_AIP"] = f"异常: {e}"
        # 8b: 检查 JBA licenses/tokens?productCode=AIP
        try:
            r = _req.get(f"{JB}/api/v1/licenses/tokens", params={"productCode": "AIP"},
                         cookies=s.cookies, timeout=15)
            aip_trial_results["jba_licenses_tokens_AIP"] = f"HTTP {r.status_code} | {r.text[:400]}"
        except Exception as e:
            aip_trial_results["jba_licenses_tokens_AIP"] = f"异常: {e}"
        # 8c: 检查 JBA licenses 页面
        try:
            r = _req.get(f"{JB}/api/v1/products/AIP", cookies=s.cookies, timeout=15)
            aip_trial_results["jba_products_AIP"] = f"HTTP {r.status_code} | {r.text[:300]}"
        except Exception as e:
            aip_trial_results["jba_products_AIP"] = f"异常: {e}"
        # 8d: 访问 JBA Account API
        try:
            r = _req.get(f"{JB}/api/v1/licenses", cookies=s.cookies, timeout=15)
            aip_trial_results["jba_api_v1_licenses"] = f"HTTP {r.status_code} | {r.text[:400]}"
        except Exception as e:
            aip_trial_results["jba_api_v1_licenses"] = f"异常: {e}"
        out["aip_trial_probe"] = aip_trial_results

        return out

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, sync_ides_probe, req.email, req.password)
    return result


# 主程序入口
if __name__ == "__main__":
    import os

    # 创建示例配置文件（如果不存在）
    if not os.path.exists("client_api_keys.json"):
        with open("client_api_keys.json", "w", encoding="utf-8") as f:
            json.dump(["sk-your-custom-key-here"], f, indent=2)
        print("已创建示例 client_api_keys.json 文件")

    if not os.path.exists("jetbrainsai.json"):
        with open("jetbrainsai.json", "w", encoding="utf-8") as f:
            json.dump([{"jwt": "your-jwt-here"}], f, indent=2)
        print("已创建示例 jetbrainsai.json 文件")

    if not os.path.exists("models.json"):
        with open("models.json", "w", encoding="utf-8") as f:
            example_config = {
                "models": ["anthropic-claude-3.5-sonnet"],
                "anthropic_model_mappings": {
                    "claude-3.5-sonnet": "anthropic-claude-3.5-sonnet",
                    "sonnet": "anthropic-claude-3.5-sonnet"
                }
            }
            json.dump(example_config, f, indent=2)
        print("已创建示例 models.json 文件")

    print("正在启动 JetBrains AI OpenAI Compatible API 服务器...")
    print("端点:")
    print("  GET  /v1/models")
    print("  POST /v1/chat/completions")
    print("  POST /v1/messages")
    print("\n在 Authorization header 中使用客户端 API 密钥 (Bearer sk-xxx)")

    uvicorn.run(app, host="0.0.0.0", port=8000)
