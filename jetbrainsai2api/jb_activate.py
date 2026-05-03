#!/usr/bin/env python3
"""
JetBrains AI 账号激活模块
通过邮箱+密码自动完成激活流程，获取 JWT 和完整凭证

流程：
1. JBA 登录（API）
2. 检查 AI 状态（是否已激活）
3. OAuth PKCE（获取 id_token + refresh_token）
4. 从 id_token 提取 user_id（Hub ring ID）
5. 调 obtainTrial.action 激活试用（优先无卡 IDE，依次回退）
6. 检查 /licenses 页面提取 licenseId
7. 调 register 注册 Grazie 用户
8. 调 provide-access 获取完整 JWT
"""
import json, re, time, base64, hashlib, os, secrets, urllib.parse, threading
import datetime
import concurrent.futures
from typing import Callable, Optional

try:
    import requests.adapters as _ra
    class CFProxyAdapter(_ra.HTTPAdapter):
        """
        将 JetBrains 相关域名的请求透明转发到 CF Worker，分散登录 IP。

        核心设计：
        - requests.Session 在 send() 之前就已按原始 URL 装配 Cookie 头，
          直接改写 URL 会让 Session 的 cookie jar 完全失效（domain 不匹配）。
        - 因此 Adapter 自己维护 _cookies 字典：
          · send() 前：把 _cookies 注入 Cookie 头（覆盖 Session 的空 jar）
          · send() 后：从 resp.raw 直接解析 Set-Cookie（绕开 domain 策略），
            更新 _cookies；无论 Worker 新旧版本均可正确提取 cookie 值。
        - CSRF token 通过 get_cookie('_st-JBA') 对外暴露，jba_login 用于
          构造 h['X-XSRF-TOKEN']。
        """
        _JB_PREFIXES = (
            "https://account.jetbrains.com",
            "https://oauth.account.jetbrains.com",
            "https://api.jetbrains.ai",
        )

        def __init__(self, proxy_url: str, *args, **kwargs):
            self._cf_url = proxy_url
            self._cookies: dict = {}   # 跨请求持久化的 cookie 字典
            super().__init__(*args, **kwargs)

        def get_cookie(self, name: str, default=None):
            return self._cookies.get(name, default)

        def send(self, prepared_request, **kwargs):
            is_jb = bool(prepared_request.url) and any(
                prepared_request.url.startswith(p) for p in self._JB_PREFIXES
            )
            if is_jb:
                # 1. 记录原始目标，改写 URL 到 Worker
                prepared_request.headers["x-target-url"] = prepared_request.url
                prepared_request.url = self._cf_url
                # 2. 手动注入已积累的 cookie（Session jar 因 domain 不匹配不会自动注入）
                if self._cookies:
                    existing = prepared_request.headers.get("Cookie", "")
                    extra = "; ".join(f"{k}={v}" for k, v in self._cookies.items())
                    prepared_request.headers["Cookie"] = (
                        f"{existing}; {extra}" if existing else extra
                    )

            resp = super().send(prepared_request, **kwargs)

            if is_jb:
                # 3. 从原始响应头直接解析 Set-Cookie（绕开 http.cookiejar domain 策略）
                #    无论 Worker 是否剥离 Domain 属性均可正确提取 cookie 值
                try:
                    raw_scs = resp.raw.headers.getlist("set-cookie")
                except Exception:
                    raw_scs = []
                for sc in raw_scs:
                    nv = sc.split(";")[0].strip()
                    if "=" in nv:
                        k, v = nv.split("=", 1)
                        self._cookies[k.strip()] = v.strip()

            return resp
except ImportError:
    CFProxyAdapter = None

try:
    import requests
except ImportError:
    requests = None

# ──────────────────────────────────────────────────────────────
# CF 代理池（Cloudflare Worker 反向代理）
# 由 main.py 在启动时注入，通过轮询分散 provide-access 请求 IP，缓解 429 限流
#   - CF_PROXY_POOL    : 主池，普通用户/管理员激活共用（list[str]）
#   - LOW_CF_PROXY_POOL: LOW_ADMIN_KEY 用户专属池，按 Discord user_id 划分多个独立子池
#                        Dict[discord_user_id, list[url]]；空 key '' 视为无 Discord 绑定的兜底池。
# 通过 thread-local 在调用栈上下文中切换池：process_account 入口处设置 use_low + low_discord_id，
# 内部所有 _cf_post / _get_proxy_url 调用自动遵循。
# ──────────────────────────────────────────────────────────────
from typing import Dict as _Dict  # 局部别名，避免覆盖文件顶部 typing 导入
CF_PROXY_POOL: list = []                        # 主池
LOW_CF_PROXY_POOL: _Dict[str, list] = {}        # LOW 池：按 Discord ID 分桶
_proxy_idx = 0                                  # 主池轮询游标
_low_proxy_idx: _Dict[str, int] = {}            # LOW 池轮询游标（每个 Discord 一个）
_proxy_lock = threading.Lock()
_low_proxy_lock = threading.Lock()
_proxy_ctx = threading.local()  # 线程局部：pool 选择上下文 + LOW 子池 Discord ID


def _set_proxy_pool_context(use_low: bool, discord_id: str = "") -> None:
    """在当前线程上设定要使用的 CF 池（True=LOW 池，False=主池）；
    use_low=True 时 discord_id 决定使用哪一个 LOW 子池（按 Discord 账号隔离）。"""
    _proxy_ctx.use_low = bool(use_low)
    _proxy_ctx.low_discord_id = str(discord_id or "")


def _clear_proxy_pool_context() -> None:
    for attr in ("use_low", "low_discord_id"):
        if hasattr(_proxy_ctx, attr):
            delattr(_proxy_ctx, attr)


def _get_current_proxy_pool_context() -> tuple:
    """读取当前线程的代理池上下文，用于把 LOW/主池选择透传到并发 worker。"""
    return (
        bool(getattr(_proxy_ctx, "use_low", False)),
        str(getattr(_proxy_ctx, "low_discord_id", "") or ""),
    )


def _apply_proxy_pool_context(ctx: tuple) -> None:
    """在并发 worker 中恢复父线程代理池上下文，避免 LOW 激活误走主池。"""
    use_low, discord_id = ctx
    _set_proxy_pool_context(bool(use_low), str(discord_id or ""))


def _current_proxy_pool_size() -> int:
    """当前上下文实际可用的 CF Worker 数量；无代理返回 0。"""
    use_low = bool(getattr(_proxy_ctx, "use_low", False))
    if use_low:
        dc_id = str(getattr(_proxy_ctx, "low_discord_id", "") or "")
        return len(LOW_CF_PROXY_POOL.get(dc_id) or [])
    return len(CF_PROXY_POOL or [])


def _activation_parallel_workers(task_count: int, no_proxy_workers: int = 2) -> int:
    """激活阶段并发度：有 CF 池时按池大小放开，无代理时保守并发，避免单 IP 限流。"""
    pool_size = _current_proxy_pool_size()
    if pool_size > 0:
        return max(1, min(task_count, pool_size, 6))
    return max(1, min(task_count, no_proxy_workers))


def _get_proxy_url() -> Optional[str]:
    """根据当前线程上下文返回 CF 代理 URL（轮询）。LOW 用户按 Discord ID 选子池。"""
    global _proxy_idx
    use_low = bool(getattr(_proxy_ctx, "use_low", False))
    if use_low:
        dc_id = str(getattr(_proxy_ctx, "low_discord_id", "") or "")
        sub_pool = LOW_CF_PROXY_POOL.get(dc_id) or []
        if not sub_pool:
            return None
        with _low_proxy_lock:
            cur = _low_proxy_idx.get(dc_id, 0)
            url = sub_pool[cur % len(sub_pool)]
            _low_proxy_idx[dc_id] = cur + 1
        return url
    if not CF_PROXY_POOL:
        return None
    with _proxy_lock:
        url = CF_PROXY_POOL[_proxy_idx % len(CF_PROXY_POOL)]
        _proxy_idx += 1
    return url


def _cf_post(url: str, **kwargs) -> "requests.Response":
    """通过 CF 代理池发送 POST 请求（无代理则直连）"""
    proxy = _get_proxy_url()
    if proxy:
        headers = dict(kwargs.pop("headers", {}))
        headers["x-target-url"] = url
        return requests.post(proxy, headers=headers, **kwargs)
    return requests.post(url, **kwargs)


def _cf_get(url: str, **kwargs) -> "requests.Response":
    """通过 CF 代理池发送 GET 请求（无代理则直连）。
    Worker 端按原 URL 透传 method/headers/body，因此 GET 的 query string 必须
    预先合并到 url 里再传给 worker——这里复用 requests 自身的 params 编码：
    若调用方传了 params=，先用一个 PreparedRequest 把它拼到 url 上，再走 worker。
    """
    proxy = _get_proxy_url()
    if proxy:
        params = kwargs.pop("params", None)
        if params:
            # 用 PreparedRequest 拼接 query string，与 requests.get(url, params=...) 等价
            from requests.models import PreparedRequest as _PR
            _pr = _PR()
            _pr.prepare_url(url, params)
            url = _pr.url
        headers = dict(kwargs.pop("headers", {}))
        headers["x-target-url"] = url
        return requests.get(proxy, headers=headers, **kwargs)
    return requests.get(url, **kwargs)

ENCRYPTED_HOSTNAME = "837dXi0iwT8bX6hyYx/jj8C3zRdOhXGfldH6IDWxUGxhR+uNhgtqr0mXpXf/nJd5ieCAGcQXo2XtV2lzBdTEDA=="
ENCRYPTED_USERNAME = "2iPzpOCWsIFuwgcAUOrGzZJDJA2tC1zeZXPkHWhSk5rFRoqp2BtfvhVv6yMaBp9a/opRRmMKvHgHseDc2usEmg=="
MACHINE_ID = "17ff7a9c-ee0d-409f-a556-a85e43c4097a"
MACHINE_UUID = "1-15f741da-48f2-3a49-a2a0-0d45352d1eb6"

JB = "https://account.jetbrains.com"
HUB = "https://oauth.account.jetbrains.com"
AI = "https://api.jetbrains.ai"

# 六个 IDE 试用配置（全部从 Replit IP 验证可返回 OK）
# 格式: (ideProductCode, productCode, productFamilyId, buildNumber, version)
# 重要：2026.1 版本的 IDE（CL/WS/RM/DB/RD/RR）均为非商业免费许可证（NC），
#        Grazie 识别 NC licenseId → jetbrains-ai.individual.free-tier → 300K 配额！
NOCARD_IDES = [
    ("CL", "CL",  "CL",  "2026.1.1 Build CL-261.23567.135", "2026100"),   # CLion 2026.1（NC，含 AI）
    ("WS", "WS",  "WS",  "2026.1.1 Build WS-261.23567.141", "2026100"),   # WebStorm 2026.1（NC，含 AI）
    ("RM", "RM",  "RM",  "2026.1.1 Build RM-261.23567.142", "2026100"),   # RubyMine 2026.1（NC，含 AI）
    ("DB", "DB",  "DB",  "2026.1.2 Build DB-261.23567.23",  "2026100"),   # DataGrip 2026.1（NC，含 AI）
    ("RD", "RD",  "RD",  "2026.1.0.1 Build RD-261.22158.394","2026100"),  # Rider 2026.1（NC，含 AI）
    ("RR", "RR",  "RR",  "2026.1.1 Build RR-261.23567.140", "2026100"),   # RustRover 2026.1（NC，含 AI）
]


def _log(msg: str, log_cb: Optional[Callable] = None):
    print(msg)
    if log_cb:
        log_cb(msg)


def jba_login(email, password, log_cb=None):
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    # CF 代理池可用时，挂载 CFProxyAdapter，将所有 JetBrains 域请求路由到 Worker
    # Adapter 自带 cookie 字典，在 send() 内手动注入/提取（绕开 Session 的 domain 策略）
    _adapter = None
    _login_proxy = _get_proxy_url()
    if _login_proxy and CFProxyAdapter is not None:
        _adapter = CFProxyAdapter(_login_proxy)
        s.mount("https://account.jetbrains.com", _adapter)
        s.mount("https://oauth.account.jetbrains.com", _adapter)
        s.mount("https://api.jetbrains.ai", _adapter)
        _log(f"  [代理] 登录通过 CF Worker 转发（{_login_proxy.split('/')[2]}）", log_cb)

    s.get(f"{JB}/login", timeout=15)
    # CSRF 优先从 Adapter 自管字典取（代理模式）；直连模式从 Session jar 取
    csrf = (
        _adapter.get_cookie("_st-JBA")
        if _adapter is not None
        else next((c.value for c in s.cookies if c.name == "_st-JBA"), None)
    )

    h = {"X-XSRF-TOKEN": csrf, "X-Requested-With": "XMLHttpRequest",
         "Content-Type": "application/json", "Origin": JB}
    # 429 速率限制：最多重试 3 次，间隔 20s
    for _attempt in range(3):
        r = s.post(f"{JB}/api/auth/sessions", headers=h, timeout=15)
        if r.status_code != 429:
            break
        wait = 20 * (_attempt + 1)
        _log(f"  [WARN] JetBrains 登录接口触发速率限制，等待 {wait}s 后重试（第{_attempt+1}次）...", log_cb)
        time.sleep(wait)
    if r.status_code == 429:
        raise Exception(f"[RATE_LIMIT] JetBrains 登录接口速率限制，请稍后再试")
    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    sid = data.get("id")
    if not sid:
        _log(f"  [FAIL] 创建登录会话失败 (HTTP {r.status_code}): {r.text[:200]}", log_cb)
        return None, None
    r_email = s.post(f"{JB}/api/auth/sessions/{sid}/email/login", headers=h, json={"email": email}, timeout=15)
    _log(f"  [DEBUG] email步骤: HTTP {r_email.status_code} | {r_email.text[:200]}", log_cb)
    r2 = s.post(f"{JB}/api/auth/sessions/{sid}/password", headers=h, json={"password": password}, timeout=15)
    _log(f"  [DEBUG] password步骤: HTTP {r2.status_code} | {r2.text[:300]}", log_cb)
    try:
        r2_json = r2.json()
    except Exception:
        r2_json = {}
    state = r2_json.get("state", "")
    if state != "REDIRECT_TO_RETURN_URL":
        _log(f"  [FAIL] 登录失败: state={state}", log_cb)
        return None, None
    # 登录后 CSRF 可能刷新，再取一次
    csrf = (
        _adapter.get_cookie("_st-JBA", csrf)
        if _adapter is not None
        else next((c.value for c in s.cookies if c.name == "_st-JBA"), csrf)
    )
    h["X-XSRF-TOKEN"] = csrf

    # 登录完成后卸载代理 Adapter：
    # OAuth PKCE 需要手动跟踪重定向链（allow_redirects=False），
    # 但 Worker 内部使用 redirect:follow 会把整个重定向链吃掉，导致 Python
    # 拿不到中间的 Location 头，授权码永远获取失败。
    # 解决方案：把 Adapter 收集的 cookie 写回 Session jar（绑定到 JB 域），
    # 然后换回标准直连 Adapter，让 OAuth 及后续步骤直连 JetBrains。
    # 登录 IP 分散的目的已经达到，后续步骤不需要再走代理。
    if _adapter is not None:
        for _k, _v in _adapter._cookies.items():
            s.cookies.set(_k, _v, domain="account.jetbrains.com")
        _direct = _ra.HTTPAdapter()
        s.mount("https://account.jetbrains.com", _direct)
        s.mount("https://oauth.account.jetbrains.com", _direct)
        s.mount("https://api.jetbrains.ai", _direct)

    return s, h


def check_ai_status(s):
    r = s.get(f"{JB}/api/ai/account/settings", timeout=15)
    if r.status_code != 200:
        return True, False
    ai = r.json()
    show = ai.get("personal", {}).get("showAIPlans", True)
    return show, not show


def oauth_pkce(s, log_cb=None):
    cv = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()
    cc = base64.urlsafe_b64encode(hashlib.sha256(cv.encode()).digest()).rstrip(b"=").decode()
    st = secrets.token_hex(16)
    url = (f"{JB}/oauth/login?client_id=ide&scope=openid+offline_access+r_ide_auth"
           f"&code_challenge={cc}&code_challenge_method=S256&state={st}"
           f"&redirect_uri={JB}/oauth2/ide/callback&response_type=code"
           f"&client_info=eyJwcm9kdWN0IjoiUFkiLCJidWlsZCI6IjI2MS4yMjE1OC4zNDAifQ")
    code = None
    i = 0
    for i in range(15):
        r = s.get(url, allow_redirects=False, timeout=15)
        loc = r.headers.get("Location", "")
        if not loc:
            if "code=" in str(r.url):
                code = urllib.parse.parse_qs(urllib.parse.urlparse(str(r.url)).query).get("code", [""])[0]
            break
        if "oauth2/ide/callback" in loc and "code=" in loc:
            code = urllib.parse.parse_qs(urllib.parse.urlparse(loc).query).get("code", [""])[0]
            break
        if loc.startswith("/"):
            parsed_url = urllib.parse.urlparse(str(r.url))
            url = f"{parsed_url.scheme}://{parsed_url.netloc}{loc}"
        else:
            url = loc
    if not code:
        _log(f"  [FAIL] OAuth 授权码获取失败（{i+1} 次重定向后）", log_cb)
        return None, None
    _log(f"  授权码获取成功（{i+1} 次重定向）", log_cb)
    _log("  正在交换 token...", log_cb)
    r = _cf_post(f"{HUB}/api/rest/oauth2/token", data={
        "grant_type": "authorization_code", "code": code,
        "code_verifier": cv, "client_id": "ide",
        "redirect_uri": f"{JB}/oauth2/ide/callback",
    }, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=15)
    tokens = r.json()
    return tokens.get("id_token", ""), tokens.get("refresh_token", "")


def decode_id_token(id_token):
    parts = id_token.split(".")
    payload = parts[1] + "==="
    claims = json.loads(base64.urlsafe_b64decode(payload))
    return claims


def obtain_trial(user_id, ide_product_code="II", build_number="2025.1.1 Build IU-251.25410.109",
                 version="2025100", product_code="AIP", product_family_id="AIP"):
    """调用 obtainTrial.action，支持指定产品码和 IDE 产品码。
    返回: (responseCode, reason, encoded_asset)
      encoded_asset: 成功时 EncodedAsset base64 数据，失败时为空字符串。
    """
    salt = str(int(time.time() * 1000))
    # 走 CF 代理池：obtainTrial.action 在批量激活时会被快速触发 6×N 次，
    # 直连容易被 account.jetbrains.com 限流；无代理时 _cf_get 自动降级为直连。
    r = _cf_get(f"{JB}/lservice/rpc/obtainTrial.action", params={
        "productFamilyId": product_family_id,
        "userId": user_id,
        "hostName": ENCRYPTED_HOSTNAME,
        "salt": salt,
        "ideProductCode": ide_product_code,
        "buildDate": "20260423",
        "clientVersion": "21",
        "secure": "false",
        "userName": ENCRYPTED_USERNAME,
        "buildNumber": build_number,
        "version": version,
        "machineId": MACHINE_ID,
        "productCode": product_code,
        "expiredLicenseDays": "0",
        "machineUUID": MACHINE_UUID,
        "checkedOption": "AGREEMENT",
    }, headers={"User-Agent": "local"}, timeout=15)
    code = re.search(r"<responseCode>(\w+)</responseCode>", r.text)
    reason = re.search(r"<trialRejectedReason>(.*?)</trialRejectedReason>", r.text)
    asset_m = re.search(r"<data>(.*?)</data>", r.text, re.DOTALL)
    rc = code.group(1) if code else "UNKNOWN"
    rr = reason.group(1) if reason else ""
    encoded_asset = asset_m.group(1).strip() if asset_m else ""
    return rc, rr, encoded_asset


def obtain_trial_nocard(user_id, log_cb=None):
    """
    无卡激活：尝试所有 IDE（CL/WS/RM/DB/RD/RR）并收集全部成功的试用。

    关键：付费 IDE（CL/WS/RM/DB/RD）的 30 天试用会自动在账号中生成
    JetBrains AI Pro 捆绑 license（AIP-XXXX），Grazie 可以识别该 ID。
    不再尝试 AIP productCode（会被 COUNTRY_IS_UNKNOWN 阻断）。

    返回: (successful_ides: list, any_success: bool)
    """
    successful = []       # [(ide_code, encoded_asset), ...]
    ctx = _get_current_proxy_pool_context()
    workers = _activation_parallel_workers(len(NOCARD_IDES), no_proxy_workers=2)
    _log(f"  [无卡] 并发尝试 {len(NOCARD_IDES)} 个 IDE（并发={workers}）...", log_cb)

    def _try_one(item):
        _apply_proxy_pool_context(ctx)
        ide_code, prod_code, family_id, build_num, ver = item
        try:
            rc, rr, encoded_asset = obtain_trial(
                user_id,
                ide_product_code=ide_code,
                build_number=build_num,
                version=ver,
                product_code=prod_code,
                product_family_id=family_id,
            )
            return ide_code, rc, rr, encoded_asset, None
        except Exception as e:
            return ide_code, None, "", "", e
        finally:
            _clear_proxy_pool_context()

    results_by_ide = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_try_one, item) for item in NOCARD_IDES]
        for fut in concurrent.futures.as_completed(futures):
            ide_code, rc, rr, encoded_asset, err = fut.result()
            if err is not None:
                _log(f"  [无卡] {ide_code} 请求异常: {err}", log_cb)
                continue
            _log(f"  [无卡] {ide_code}: {rc}" + (f" ({rr})" if rr else "") +
                 (f" [EncodedAsset {len(encoded_asset)}字符]" if encoded_asset else ""), log_cb)
            if rc in ("OK", "ALREADY_OBTAINED", "TRIAL_AVAILABLE"):
                _log(f"  ✓ {ide_code} 许可证获取成功！", log_cb)
                results_by_ide[ide_code] = encoded_asset
            else:
                _log(f"  [无卡] {ide_code} 返回 {rc}，继续...", log_cb)

    # 保持返回顺序稳定，便于后续日志与排查
    for ide_code, *_ in NOCARD_IDES:
        if ide_code in results_by_ide:
            successful.append((ide_code, results_by_ide[ide_code]))

    if successful:
        _log(f"  ✓ 共获得 {len(successful)} 个 IDE 许可证: {', '.join(x[0] for x in successful)}", log_cb)
    else:
        _log("  [无卡] 所有 IDE 许可证均未成功", log_cb)

    return successful, len(successful) > 0


def fetch_aip_license_tokens(s, log_cb=None):
    """
    调用 JBA /licenses/tokens REST API 获取 AIP 许可证列表。
    这是 IDE 内部使用的 API（JBAccountInfoService.getAvailableLicenses）。
    
    返回: [licenseId, ...] 按优先级排序（paid > trial, individual > company）
    """
    # 尝试多种格式调用 /licenses/tokens
    aip_ids = []
    
    # 方法1: GET /licenses/tokens（可能有 productCode 参数）
    for params in [{"productCode": "AIP"}, {"product": "AIP"}, {}]:
        try:
            r = s.get(f"{JB}/licenses/tokens", params=params, timeout=15)
            if log_cb:
                log_cb(f"  [tokens] GET /licenses/tokens{params}: HTTP {r.status_code} | {r.text[:150]}")
            if r.status_code == 200:
                try:
                    data = r.json()
                    # 可能返回 {"licenseList": [...]} 或直接 [...]
                    licenses = data.get("licenseList", data.get("licenses", data if isinstance(data, list) else []))
                    for lic in licenses:
                        lid = lic.get("licenseId", "")
                        kind = lic.get("licenseKind", "")
                        ltype = lic.get("licenseeType", "")
                        if lid:
                            aip_ids.append((lid, kind, ltype))
                            if log_cb:
                                log_cb(f"  [tokens] 找到 license: {lid} kind={kind} type={ltype}")
                except Exception as e:
                    if log_cb:
                        log_cb(f"  [tokens] JSON解析失败: {e} | raw: {r.text[:200]}")
                break
        except Exception as e:
            if log_cb:
                log_cb(f"  [tokens] 请求异常: {e}")
    
    # 按优先级排序：paid > trial, individual > company
    priority = {"COMMERCIAL": 0, "ALL_PRODUCTS": 1, "TRIAL": 2, "": 3}
    ind_priority = {"INDIVIDUAL": 0, "PERSONAL": 0, "COMPANY": 1, "": 2}
    aip_ids.sort(key=lambda x: (priority.get(x[1].upper(), 99), ind_priority.get(x[2].upper(), 99)))
    
    result = [x[0] for x in aip_ids]
    if log_cb:
        log_cb(f"  [tokens] 获得 {len(result)} 个 AIP license: {result}")
    return result


def extract_license_ids(s, log_cb=None):
    """从 JBA 账号获取所有 AIP license IDs。
    优先使用 /licenses/tokens API，回退到 /licenses HTML 解析。"""
    
    # 方法1: 使用 /licenses/tokens REST API（最准确）
    api_ids = fetch_aip_license_tokens(s, log_cb)
    if api_ids:
        if log_cb:
            log_cb(f"  [licenses] /tokens API 返回 {len(api_ids)} 个 AIP license")
        return api_ids
    
    # 方法2: 回退到 HTML 页面解析
    r = s.get(f"{JB}/licenses", timeout=15)
    # 宽泛匹配：id="license-XXXX"（原格式），data-id="XXXX"，以及 JSON 格式 licenseId/id
    ids1 = re.findall(r'id="license-([A-Z0-9\-]{4,20})"', r.text)
    ids2 = re.findall(r'data-license-id="([A-Z0-9\-]{4,20})"', r.text)
    ids3 = re.findall(r'"licenseId"\s*:\s*"([A-Z0-9\-]{4,20})"', r.text)
    ids4 = re.findall(r'data-id="([A-Z0-9\-]{8,20})"', r.text)
    # 合并去重，保持顺序
    seen = set()
    all_ids = []
    for lid in ids1 + ids2 + ids3 + ids4:
        lid = lid.strip("-")
        if lid and lid not in seen and len(lid) >= 6:
            seen.add(lid)
            all_ids.append(lid)
    # AIP 前缀 license 优先（Grazie 能直接识别）
    aip_ids = [lid for lid in all_ids if lid.startswith("AIP")]
    other_ids = [lid for lid in all_ids if not lid.startswith("AIP")]
    result = aip_ids + other_ids
    if log_cb:
        log_cb(f"  [licenses] HTML解析: 页面长度={len(r.text)}, AIP={aip_ids}, 其他={other_ids}")
    return result


def register_grazie(id_token):
    # 走 CF 代理池，避免 api.jetbrains.ai 直连限流
    r = _cf_post(f"{AI}/auth/jetbrains-jwt/register",
        headers={"Authorization": f"Bearer {id_token}", "User-Agent": "ktor-client"},
        timeout=15)
    return r.status_code, r.text[:200]


def get_jwt(id_token, license_id, retries=3, retry_delay=5, log_cb=None):
    """获取 Grazie JWT，失败时最多重试 retries 次（间隔 retry_delay 秒）"""
    url = f"{AI}/auth/jetbrains-jwt/provide-access/license/v2"
    headers = {"Authorization": f"Bearer {id_token}",
               "User-Agent": "ktor-client", "Content-Type": "application/json"}
    last_err = ""
    for attempt in range(1, retries + 1):
        try:
            # 走 CF 代理池，避免 api.jetbrains.ai 直连限流（无代理时自动降级直连）
            r = _cf_post(url, json={"licenseId": license_id}, headers=headers, timeout=15)
        except Exception as e:
            last_err = f"请求异常: {e}"
            if attempt < retries:
                if log_cb:
                    log_cb(f"  [get_jwt] 第{attempt}次请求异常，{retry_delay}s 后重试: {e}")
                time.sleep(retry_delay)
            continue
        if r.status_code == 200:
            data = r.json()
            token = data.get("token", "")
            state = data.get("state", "")
            if token:
                return token, state
            last_err = f"state={state}, no token in response"
            # state=NONE 且无 token，无需重试
            break
        last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        # 429 限流：多等一会儿
        if r.status_code == 429:
            wait = retry_delay * 2
            if log_cb:
                log_cb(f"  [get_jwt] 429 限流，等待 {wait}s 后第{attempt}次重试...")
            time.sleep(wait)
        elif attempt < retries:
            if log_cb:
                log_cb(f"  [get_jwt] HTTP {r.status_code}，{retry_delay}s 后第{attempt}次重试: {r.text[:100]}")
            time.sleep(retry_delay)
    return None, last_err


def _decode_encoded_asset(hex_str):
    """
    解码 obtainTrial.action 返回的 EncodedAsset hex 字符串，
    返回 (raw_bytes, b64_str, text_if_printable) 三元组。
    """
    try:
        raw = bytes.fromhex(hex_str.replace(" ", "").strip())
        b64 = base64.b64encode(raw).decode()
        # 尝试作为 UTF-8 或 Latin-1 文本
        try:
            text = raw.decode("utf-8")
        except Exception:
            text = raw.decode("latin-1")
        printable = all(0x20 <= c < 0x7F or c in (0x09, 0x0A, 0x0D) for c in raw[:64])
        return raw, b64, text if printable else None
    except Exception:
        # 如果不是 hex，尝试作为 base64
        try:
            raw = base64.b64decode(hex_str + "==")
            b64 = base64.b64encode(raw).decode()
            return raw, b64, None
        except Exception:
            return None, hex_str, None


def get_jwt_from_ides_endpoint(encoded_assets, log_cb=None):
    """
    尝试 /auth/ides/provide-access 端点（无需 Bearer token！）
    发送各种格式的 EncodedAsset 作为 license 字段。
    
    返回: (token, description) 或 (None, error_msg)
    """
    url = f"{AI}/auth/ides/provide-access"
    hdrs = {"Content-Type": "application/json",
            "User-Agent": "IntelliJIdea/251.25410.109 (JetBrains s.r.o.)",
            "Accept": "application/json"}

    def _try_ides(label, license_val):
        try:
            # 走 CF 代理池：api.jetbrains.ai 是限流核心域，直连极易 429
            r = _cf_post(url, json={"license": license_val}, headers=hdrs, timeout=15)
            if r.status_code == 200:
                try:
                    data = r.json()
                    token = data.get("token", "")
                    _log(f"  [ides/{label}] HTTP 200 token={'✓ ' + token[:20] if token else '✗ (no token)'}", log_cb)
                    return token or None, "200-no-token"
                except Exception:
                    _log(f"  [ides/{label}] HTTP 200 raw: {r.text[:100]}", log_cb)
                    return None, None
            else:
                _log(f"  [ides/{label}] HTTP {r.status_code}: {r.text[:120]}", log_cb)
        except Exception as e:
            _log(f"  [ides/{label}] 异常: {e}", log_cb)
        return None, None

    for ide_code, asset_hex in encoded_assets:
        raw, b64, text = _decode_encoded_asset(asset_hex)
        _log(f"  [ides] {ide_code}: asset_hex={len(asset_hex)}字符, raw={'✓' if raw else '✗'}, b64={len(b64)}字符", log_cb)

        # 格式 1: 原始 hex 字符串
        t, _ = _try_ides(f"{ide_code}-hex", asset_hex)
        if t:
            return t, f"ides-hex-{ide_code}"

        # 格式 2: base64 编码后的字节
        t, _ = _try_ides(f"{ide_code}-b64", b64)
        if t:
            return t, f"ides-b64-{ide_code}"

        # 格式 3: 文本形式（如果是可打印字符）
        if text:
            t, _ = _try_ides(f"{ide_code}-text", text)
            if t:
                return t, f"ides-text-{ide_code}"

        # 格式 4: base64url 编码
        b64url = b64.replace("+", "-").replace("/", "_").rstrip("=") if raw else None
        if b64url:
            t, _ = _try_ides(f"{ide_code}-b64url", b64url)
            if t:
                return t, f"ides-b64url-{ide_code}"

        # 格式 5: 前8字节检测文件类型，如果是 XML，直接发 XML 文本
        if raw and len(raw) > 8:
            magic = raw[:8]
            if b"<?xml" in raw[:50] or b"<license" in raw[:50]:
                xml_text = raw.decode("utf-8", errors="replace")
                t, _ = _try_ides(f"{ide_code}-xml", xml_text)
                if t:
                    return t, f"ides-xml-{ide_code}"
            elif raw[:2] == b"PK":
                _log(f"  [ides] {ide_code}: 检测到 ZIP 格式，跳过", log_cb)
            elif raw[:3] == bytes([0x30, 0x82, 0x00]) or raw[0] == 0x30:
                _log(f"  [ides] {ide_code}: 检测到 DER/ASN.1 格式", log_cb)

    return None, "ides端点所有格式均失败"


def collect_all_ide_jwts(id_token, license_ids, log_cb=None, max_consecutive_400=4,
                         return_untrusted=False):
    """
    ★ 批量收集所有 NC free-tier JWT（每个 licenseId 独立 300K 配额）。
    对 license_ids 中每个 ID 尝试 provide-access/license/v2；
    trial 许可证返回 400，连续 max_consecutive_400 次 400 后提前退出（trial 账号优化）。
    492 = "Untrusted license"：NC 许可证刚创建，约 30-60 分钟后可用。
    返回: list of {"license_id": str, "jwt": str, "license_type": str}
    若 return_untrusted=True，返回 (results, untrusted_lids)
    """
    hdrs = {"Authorization": f"Bearer {id_token}",
            "User-Agent": "ktor-client", "Content-Type": "application/json"}
    v2_url = f"{AI}/auth/jetbrains-jwt/provide-access/license/v2"
    results = []
    untrusted_lids = []
    seen_ids = set()
    consecutive_400 = 0
    # 代理池存在时请求间隔缩短（IP 已分散），否则适当放慢避免触发限流；
    # 使用当前上下文判断，确保 LOW 专属池也能走快速间隔。
    _req_interval = 0.3 if _current_proxy_pool_size() > 0 else 0.8
    _req_idx = 0
    for lid in license_ids:
        if not lid or lid in seen_ids:
            continue
        seen_ids.add(lid)
        # 连续 400 过多：trial 许可证全部失败，提前退出节省时间
        if consecutive_400 >= max_consecutive_400:
            _log(f"  [ide-jwt] 连续 {consecutive_400} 次 400，提前退出（无 NC 许可证）", log_cb)
            break
        # 请求间隔：第一次不等，之后按间隔等待
        if _req_idx > 0:
            time.sleep(_req_interval)
        _req_idx += 1
        try:
            r = _cf_post(v2_url, json={"licenseId": lid}, headers=hdrs, timeout=15)
            if r.status_code == 200:
                consecutive_400 = 0
                data = r.json()
                tok = data.get("token", "")
                if tok:
                    try:
                        _parts = tok.split(".")
                        _pl = json.loads(base64.urlsafe_b64decode(_parts[1] + "=="))
                        ltype = _pl.get("license_type", "")
                        real_lid = _pl.get("license", lid)
                    except Exception:
                        ltype = ""
                        real_lid = lid
                    if "free-tier" in ltype:
                        results.append({"license_id": real_lid, "jwt": tok, "license_type": ltype})
                        _log(f"  [ide-jwt] ✓ {real_lid} → {ltype}", log_cb)
                    else:
                        _log(f"  [ide-jwt] 跳过 {lid}（type={ltype}）", log_cb)
            elif r.status_code == 400:
                consecutive_400 += 1
                _log(f"  [ide-jwt] {lid} HTTP 400（trial，连续{consecutive_400}次）", log_cb)
            elif r.status_code == 429:
                # 429 = 此 licenseId 今日已限流；说明它是有效的 NC licenseId（否则早就返回400）
                # 将其记入 untrusted_lids，让后台重试任务下次周期再尝试
                consecutive_400 = 0
                untrusted_lids.append(lid)
                try:
                    _body_429 = r.text[:300]
                except Exception:
                    _body_429 = "(无法读取响应体)"
                _log(f"  [ide-jwt] {lid} HTTP 429 响应体: {_body_429}", log_cb)
                _log(f"  [ide-jwt] {lid} 429限流（可能是有效 NC，已记入 pending）", log_cb)
                # 429 后额外冷却，减少后续请求继续触发限流的概率
                time.sleep(2.0)
            elif r.status_code == 492:
                # 492 = Untrusted license：NC 许可证刚创建，约 30-60 分钟后可用
                consecutive_400 = 0
                untrusted_lids.append(lid)
                try:
                    _body_492 = r.text[:250]
                except Exception:
                    _body_492 = "(无法读取响应体)"
                _log(f"  [ide-jwt] {lid} HTTP 492 响应体: {_body_492}", log_cb)
                _log(f"  [ide-jwt] {lid} HTTP 492（Untrusted，约30-60分钟后可用）", log_cb)
            else:
                consecutive_400 = 0
                try:
                    _body_other = r.text[:300]
                except Exception:
                    _body_other = ""
                _log(f"  [ide-jwt] {lid} HTTP {r.status_code} 响应体: {_body_other}", log_cb)
        except Exception as e:
            _log(f"  [ide-jwt] {lid} 异常: {e}", log_cb)
    if return_untrusted:
        return results, untrusted_lids
    return results


def create_nc_licenses(s, user_id, log_cb=None):
    """
    ★ 2026-04-25 实测：obtainFreeLicense.action + checkedOptions=agreementAccepted
    可为任意新账号创建 NC 许可证（jetbrains-ai.individual.free-tier，300K 配额/月）。
    新建许可证约需 30-60 分钟被 Grazie 信任（492 Untrusted → 200 OK）。

    返回: list of new NC licenseIds (str)
    """
    _log("  [nc-create] 开始创建 NC 许可证（RM/RR/CL/WS/DB/RD + checkedOptions=agreementAccepted）...", log_cb)
    try:
        before_lids = set(extract_license_ids(s))   # 用正确的解析器，避免漏掉以数字开头的 licenseId
    except Exception as e:
        _log(f"  [nc-create] 读取现有 licenseId 失败: {e}", log_cb)
        before_lids = set()

    # 对全部 6 个 IDE 各调一次 obtainFreeLicense.action（均支持 NC 路径）
    # 每个成功的 IDE 产生一个独立的 NC licenseId（300K/月），最多 6 个 × 300K = 1.8M/月
    nc_products = [
        ("RM", "2026.1.1 Build RM-261.23567.142", "20260325"),
        ("RR", "2026.1.1 Build RR-261.23567.140", "20260325"),
        ("CL", "2026.1.1 Build CL-261.23567.135", "20260423"),
        ("WS", "2026.1.1 Build WS-261.23567.141", "20260325"),
        ("DB", "2026.1.2 Build DB-261.23567.23",  "20260325"),
        ("RD", "2026.1.0.1 Build RD-261.22158.394", "20260325"),
    ]
    ctx = _get_current_proxy_pool_context()
    workers = _activation_parallel_workers(len(nc_products), no_proxy_workers=2)
    _log(f"  [nc-create] 并发创建 {len(nc_products)} 个 NC 许可证（并发={workers}）...", log_cb)

    def _create_one(item):
        _apply_proxy_pool_context(ctx)
        pc, build, bdate = item
        params = {
            "productFamilyId": pc, "hostName": ENCRYPTED_HOSTNAME,
            "salt": str(int(time.time() * 1000)),
            "ideProductCode": pc, "buildDate": bdate, "clientVersion": "21",
            "secure": "false", "userName": ENCRYPTED_USERNAME,
            "buildNumber": build, "userId": user_id, "version": "2026100",
            "machineId": MACHINE_ID, "productCode": pc,
            "checkedOptions": "agreementAccepted", "machineUUID": MACHINE_UUID,
        }
        try:
            # 走 CF 代理池：6 个 IDE 各调一次 obtainFreeLicense.action（批量激活时压力大）
            r = _cf_get(f"{JB}/lservice/rpc/obtainFreeLicense.action", params=params,
                        headers={"User-Agent": f"{pc}/261.23567.141"}, timeout=15)
            rc = re.search(r"<responseCode>(.*?)</responseCode>", r.text)
            has_asset = "<EncodedAsset>" in r.text
            msg = re.search(r"<message>(.*?)</message>", r.text)
            return pc, rc.group(1) if rc else "?", has_asset, msg.group(1) if msg else "", None
        except Exception as e:
            return pc, "?", False, "", e
        finally:
            _clear_proxy_pool_context()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_create_one, item) for item in nc_products]
        for fut in concurrent.futures.as_completed(futures):
            pc, rc, has_asset, msg, err = fut.result()
            if err is not None:
                _log(f"  [nc-create:{pc}] 异常: {err}", log_cb)
            else:
                _log(f"  [nc-create:{pc}] rc={rc} "
                     f"asset={'✓' if has_asset else '✗'} "
                     f"msg={msg}", log_cb)

    # 调 obtainLicense.action (AIP, 无 licenseId) — 让服务器绑定 AIP 权益
    params_aip = {
        "productFamilyId": "AIP", "hostName": ENCRYPTED_HOSTNAME,
        "salt": str(int(time.time() * 1000)),
        "ideProductCode": "WS", "buildDate": "20260325", "clientVersion": "21",
        "secure": "false", "userName": ENCRYPTED_USERNAME,
        "buildNumber": "2026.1.1 Build WS-261.23567.141", "userId": user_id,
        "version": "2026100", "machineId": MACHINE_ID, "productCode": "AIP",
        "expiredLicenseDays": "0", "machineUUID": MACHINE_UUID,
    }
    try:
        # 走 CF 代理池：批量场景下与 obtainFreeLicense 同源同 IP 限流
        r_aip = _cf_get(f"{JB}/lservice/rpc/obtainLicense.action", params=params_aip,
                        headers={"User-Agent": "WebStorm/261.23567.141"}, timeout=15)
        rc_aip = re.search(r"<responseCode>(.*?)</responseCode>", r_aip.text)
        _log(f"  [nc-create:AIP] obtainLicense rc={rc_aip.group(1) if rc_aip else '?'}", log_cb)
    except Exception as e:
        _log(f"  [nc-create:AIP] 异常: {e}", log_cb)

    # 读取新 licenseId：以前固定等待 3s；现在改成短轮询，成功同步即提前返回。
    last_err = None
    for attempt, wait_s in enumerate((1.0, 1.0, 2.0), start=1):
        time.sleep(wait_s)
        try:
            after_lids = set(extract_license_ids(s))    # 用正确的解析器
            new_lids = list(after_lids - before_lids)
            all_nc = list(after_lids)
            if new_lids or all_nc or attempt == 3:
                _log(f"  [nc-create] 新增 licenseId: {new_lids}，账号全量: {all_nc}", log_cb)
                return new_lids, all_nc
        except Exception as e:
            last_err = e
            _log(f"  [nc-create] 读取新 licenseId 失败(第{attempt}次): {e}", log_cb)
    if last_err:
        _log(f"  [nc-create] 读取新 licenseId 最终失败: {last_err}", log_cb)
    return [], []


def get_jwt_from_grazie_lite(id_token, log_cb=None):
    """
    ★ 已验证可用的激活链路（2026-04-24 实测）：
    1. POST /auth/jetbrains-jwt/license/obtain/grazie-lite（Bearer id_token，无 body）
       → 返回 {"license": {"licenseId": "...", "type": "grazie.individual.lite", ...}}
    2. POST /auth/jetbrains-jwt/provide-access/license/v2（Bearer id_token，{"licenseId": ...}）
       → 返回 {"state": "NONE", "token": "<GrazieJWT>"}
    适用于：任何有 Grazie Lite 权益的 JetBrains 账号（注册即有），10K 配额/月。
    返回: (token, license_id, description) 或 (None, None, error_desc)
    """
    hdrs = {"Authorization": f"Bearer {id_token}",
            "User-Agent": "ktor-client", "Content-Type": "application/json"}
    grazie_lite_url = f"{AI}/auth/jetbrains-jwt/license/obtain/grazie-lite"
    v2_url = f"{AI}/auth/jetbrains-jwt/provide-access/license/v2"

    try:
        # 步骤1: 获取 JBALicense（含 licenseId）
        # 走 CF 代理池：api.jetbrains.ai 是限流核心域，与 register_grazie/get_jwt 一致
        r = _cf_post(grazie_lite_url, headers=hdrs, timeout=15)
        _log(f"  [grazie-lite] HTTP {r.status_code}: {r.text[:300]}", log_cb)
        if r.status_code != 200:
            return None, None, f"grazie-lite HTTP {r.status_code}"
        try:
            data = r.json()
            lic_obj = data.get("license", data)
            if not isinstance(lic_obj, dict):
                return None, None, "grazie-lite: 响应格式异常"
            lic_id = lic_obj.get("licenseId")
            lic_type = lic_obj.get("type", "?")
            _log(f"  [grazie-lite] licenseId={lic_id} type={lic_type}", log_cb)
            if not lic_id:
                return None, None, "grazie-lite: 无 licenseId"

            # 步骤2: ★ 核心调用 — licenseId → provide-access/license/v2（含 429 重试）
            for attempt in range(1, 5):
                # 走 CF 代理池：高频被 429 的核心端点
                r2 = _cf_post(v2_url, json={"licenseId": lic_id}, headers=hdrs, timeout=15)
                _log(f"  [grazie-lite→v2] 尝试{attempt}: HTTP {r2.status_code}: {r2.text[:200]}", log_cb)
                if r2.status_code == 200:
                    data2 = r2.json()
                    token = data2.get("token", "")
                    if token:
                        _log(f"  [grazie-lite→v2] ★ 成功获取 Grazie JWT！licenseId={lic_id}", log_cb)
                        return token, lic_id, f"grazie-lite→v2 ({lic_type})"
                    break
                elif r2.status_code == 429:
                    wait_s = attempt * 8
                    _log(f"  [grazie-lite→v2] 429 限流，等待 {wait_s}s...", log_cb)
                    time.sleep(wait_s)
                else:
                    break

        except Exception as e:
            _log(f"  [grazie-lite] 解析错误: {e}", log_cb)
    except Exception as e:
        _log(f"  [grazie-lite] 请求异常: {e}", log_cb)
    return None, None, None


def get_jwt_multiformat(id_token, license_ids, encoded_assets, log_cb=None):
    """
    多格式 JWT 获取：依次尝试各种 Grazie 认证格式，找出哪种格式能成功。

    尝试顺序：
    -1. /auth/jetbrains-jwt/license/obtain/grazie-lite（最新！无需 body，直接用 Bearer id_token）
    0. /auth/ides/provide-access（无 Bearer token，发送 EncodedAsset）[最新发现]
    1. v2 空 licenseId（free tier）
    2. v2 标准 licenseId（来自 /licenses 页面）
    3. v2 licenseId + certificate（EncodedAsset）
    4. v2 licenseKey = EncodedAsset base64
    5. v1 端点（/provide-access/license，不带 /v2）+ licenseId
    6. v1 端点 + 空 licenseId
    7. 产品码前缀格式
    """
    hdrs = {"Authorization": f"Bearer {id_token}",
            "User-Agent": "ktor-client", "Content-Type": "application/json"}
    url_v2 = f"{AI}/auth/jetbrains-jwt/provide-access/license/v2"
    url_v1 = f"{AI}/auth/jetbrains-jwt/provide-access/license"

    def _try(label, url, body):
        try:
            # 走 CF 代理池：multiformat 会循环尝试 7+ 种格式，全打 api.jetbrains.ai，最易触发 429
            r = _cf_post(url, json=body, headers=hdrs, timeout=15)
            if r.status_code == 200:
                data = r.json()
                token = data.get("token", "")
                state = data.get("state", "")
                _log(f"  [{label}] HTTP 200 state={state} token={'✓' if token else '✗'}", log_cb)
                if token:
                    return token, state
            else:
                _log(f"  [{label}] HTTP {r.status_code}: {r.text[:80]}", log_cb)
        except Exception as e:
            _log(f"  [{label}] 异常: {e}", log_cb)
        return None, None

    # 格式 -1: /auth/jetbrains-jwt/license/obtain/grazie-lite（最新端点！）
    _log("  [-1] 尝试 obtain/grazie-lite → provide-access/license/v2...", log_cb)
    t, _lid, desc = get_jwt_from_grazie_lite(id_token, log_cb)
    if t:
        return t, desc

    # 格式 0: /auth/ides/provide-access（全新发现的端点，无需 Bearer token）
    if encoded_assets:
        _log("  [0] 尝试 /auth/ides/provide-access（EncodedAsset → license 字段）...", log_cb)
        t, desc = get_jwt_from_ides_endpoint(encoded_assets, log_cb)
        if t:
            return t, desc

    # 格式 A: free tier（空 licenseId）
    t, s = _try("A-empty", url_v2, {"licenseId": ""})
    if t:
        return t, s

    # 格式 B: 标准 licenseId
    for lid in license_ids:
        t, s = _try(f"B-lid={lid}", url_v2, {"licenseId": lid})
        if t:
            return t, s

    # 格式 C: licenseId + certificate（EncodedAsset）
    for ide, asset in encoded_assets:
        for lid in license_ids[:3]:
            t, s = _try(f"C-cert({ide})+lid={lid}", url_v2, {"licenseId": lid, "certificate": asset})
            if t:
                return t, s

    # 格式 D: licenseKey = EncodedAsset（取代 licenseId）
    for ide, asset in encoded_assets[:3]:
        t, s = _try(f"D-licenseKey({ide})", url_v2, {"licenseKey": asset})
        if t:
            return t, s
        t, s = _try(f"D-licenseId=asset({ide})", url_v2, {"licenseId": asset})
        if t:
            return t, s

    # 格式 E: v1 端点 + licenseId
    for lid in license_ids[:3]:
        t, s = _try(f"E-v1-lid={lid}", url_v1, {"licenseId": lid})
        if t:
            return t, s

    # 格式 F: v1 端点 + 空 licenseId
    t, s = _try("F-v1-empty", url_v1, {"licenseId": ""})
    if t:
        return t, s

    # 格式 G: 产品码前缀格式（如 "RR-XXXXXXXXXX"）
    product_prefixes = [x[0] for x in encoded_assets]
    for lid in license_ids[:3]:
        for prefix in product_prefixes[:3]:
            full_id = f"{prefix}-{lid}"
            t, s = _try(f"G-prefix={full_id}", url_v2, {"licenseId": full_id})
            if t:
                return t, s

    return None, "所有格式均失败"


def process_account(email: str, password: str, log_cb: Optional[Callable] = None,
                    use_low_pool: bool = False, low_discord_id: str = "") -> dict:
    """
    完整激活流程（支持无卡激活）。
    log_cb: 可选回调函数，接受 str，每步输出一行日志。
    use_low_pool: True 时此次激活全程使用 LOW_CF_PROXY_POOL（与主池完全隔离）。
    low_discord_id: 仅在 use_low_pool=True 时有意义；用于在多 Discord LOW 子池间选择对应账号的池。
    返回 dict: 成功时包含完整凭证（email, refresh_token, id_token, license_id, user_id, jwt, obtained_at, activate_mode），
               失败时包含 error 字段。
    """
    # 在当前线程上设定 CF 池上下文，process_account 内部所有 _cf_post / _get_proxy_url
    # 都会自动遵循；执行结束后清理，避免污染线程池中下一次任务。
    _set_proxy_pool_context(use_low_pool, low_discord_id)
    try:
        return _process_account_inner(email, password, log_cb)
    finally:
        _clear_proxy_pool_context()


def _process_account_inner(email: str, password: str, log_cb: Optional[Callable] = None) -> dict:
    """
    JetBrains AI Pro 一个月试用激活流程（要求账号已绑定信用卡）。
    流程：JBA 登录 → 检查 AI 状态 → OAuth PKCE → 解 user_id → obtainTrial(AIP)
        → 提取 licenseId → Grazie 注册 → provide-access 获取 JWT
    """
    result = {
        "email": email,
        "refresh_token": None,
        "id_token": None,
        "license_id": None,
        "user_id": None,
        "jwt": None,
        "obtained_at": None,
        "activate_mode": None,
        "error": None,
    }

    # Step 1: 登录
    _log("[1/8] JBA 登录...", log_cb)
    try:
        s, h = jba_login(email, password, log_cb)
    except Exception as e:
        result["error"] = f"登录异常: {e}"
        _log(f"  [FAIL] {result['error']}", log_cb)
        return result
    if not s:
        result["error"] = "登录失败，请检查邮箱和密码"
        _log(f"  [FAIL] {result['error']}", log_cb)
        return result
    _log("  ✓ 登录成功", log_cb)

    # Step 2: 检查 AI 状态
    _log("[2/8] 检查 AI 状态...", log_cb)
    try:
        show_plans, already_active = check_ai_status(s)
    except Exception as e:
        result["error"] = f"检查AI状态异常: {e}"
        _log(f"  [FAIL] {result['error']}", log_cb)
        return result
    if already_active:
        _log("  ✓ AI 已激活，跳过 obtainTrial", log_cb)
    else:
        _log(f"  showAIPlans={show_plans}，尚未激活，将申请 AI Pro 试用", log_cb)

    # Step 3: OAuth PKCE
    _log("[3/8] OAuth PKCE 获取 token...", log_cb)
    try:
        id_token, refresh_token = oauth_pkce(s, log_cb)
    except Exception as e:
        result["error"] = f"OAuth异常: {e}"
        _log(f"  [FAIL] {result['error']}", log_cb)
        return result
    if not id_token:
        result["error"] = "OAuth PKCE 失败，无法获取 id_token"
        _log(f"  [FAIL] {result['error']}", log_cb)
        return result
    result["id_token"] = id_token
    result["refresh_token"] = refresh_token
    _log("  ✓ id_token 获取成功", log_cb)

    # Step 4: 解 user_id
    _log("[4/8] 提取 Hub user_id...", log_cb)
    try:
        claims = decode_id_token(id_token)
    except Exception as e:
        result["error"] = f"解码id_token异常: {e}"
        _log(f"  [FAIL] {result['error']}", log_cb)
        return result
    user_id = claims.get("user_id", "")
    result["user_id"] = user_id
    _log(f"  ✓ user_id: {user_id}", log_cb)
    if not user_id:
        result["error"] = "id_token 中无 user_id"
        _log(f"  [FAIL] {result['error']}", log_cb)
        return result

    # Step 5: obtainTrial（AIP 一个月试用，需账号已绑卡）
    if not already_active:
        _log("[5/8] 申请 AI Pro 一个月试用...", log_cb)
        try:
            rc, reason, _asset = obtain_trial(user_id)
        except Exception as e:
            result["error"] = f"obtainTrial异常: {e}"
            _log(f"  [FAIL] {result['error']}", log_cb)
            return result
        _log(f"  responseCode: {rc}", log_cb)
        if rc == "OK":
            result["activate_mode"] = "trial"
            _log("  ✓ 试用激活成功（AI Pro 一个月）", log_cb)
        else:
            tip = ""
            if reason == "PAYMENT_PROOF_REQUIRED":
                tip = "（账号尚未绑定信用卡，无法领取试用，请先在 JetBrains 账号绑卡）"
            result["error"] = f"obtainTrial 失败: {rc} {reason}{tip}"
            _log(f"  [FAIL] {result['error']}", log_cb)
            return result
    else:
        result["activate_mode"] = "already_active"
        _log("[5/8] 跳过（AI 已激活）", log_cb)

    # Step 6: 提取 licenseId（短轮询）
    _log("[6/8] 提取 licenseId（短轮询同步）...", log_cb)
    ids = []
    for attempt, wait_s in enumerate((1, 2, 5), start=1):
        time.sleep(wait_s)
        try:
            ids = extract_license_ids(s, log_cb=log_cb)
        except Exception as e:
            _log(f"  [WARN] 提取licenseId异常(第{attempt}次): {e}", log_cb)
        if ids:
            break
        if attempt < 3:
            _log(f"  [6] 未找到 licenseId，重试中...", log_cb)
    _log(f"  最终 License IDs: {ids}", log_cb)
    if not ids:
        result["error"] = "未能提取到 licenseId（账号可能未绑卡或试用未生效）"
        _log(f"  [FAIL] {result['error']}", log_cb)
        return result

    # Step 7: Grazie 注册
    _log("[7/8] Grazie 注册...", log_cb)
    try:
        status, body = register_grazie(id_token)
        _log(f"  register: HTTP {status} | {body}", log_cb)
    except Exception as e:
        _log(f"  [WARN] register 异常: {e}（继续）", log_cb)

    # Step 8: provide-access → JWT（按 licenseId 顺序尝试，第一个成功即返回）
    _log("[8/8] 获取 JWT（provide-access）...", log_cb)
    last_err = ""
    for lid in ids:
        try:
            jwt_token, state = get_jwt(id_token, lid, log_cb=log_cb)
        except Exception as e:
            _log(f"  {lid}: 异常 {e}", log_cb)
            last_err = str(e)
            continue
        if jwt_token:
            result["license_id"] = lid
            result["jwt"] = jwt_token
            result["obtained_at"] = datetime.datetime.utcnow().isoformat() + "Z"
            _log(f"  ✓ JWT 获取成功（licenseId={lid}, state={state}）", log_cb)
            return result
        _log(f"  {lid}: jwt=None ({state})", log_cb)
        last_err = state or "no token"

    result["error"] = f"所有 licenseId 均无法获取 JWT: {last_err}"
    _log(f"  [FAIL] {result['error']}", log_cb)
    return result
