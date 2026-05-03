#!/usr/bin/env python3
# ================================================================
# JB_RULE_HEADER_V1 WARN WARN WARN 新版脚本必须 cp 旧版后 Edit，禁止 Write from scratch WARN WARN WARN
# ================================================================
"""
JetBrains AI 试用激活脚本（增强版 - 完整凭证输出）
输入：邮箱 + 密码（支持单个 / 交互式 / 批量CSV）
输出：完整凭证 JSON（email, password, refresh_token, id_token, license_id, user_id, jwt, obtained_at）

流程：
1. JBA 登录（API）
2. 检查 AI 状态（是否已激活）
3. OAuth PKCE（获取 id_token + refresh_token）
4. 从 id_token 提取 user_id（Hub ring ID）
5. 调 obtainTrial.action 激活试用（如需要）
6. 检查 /licenses 页面提取 licenseId
7. 调 register 注册 Grazie 用户
8. 调 provide-access 获取完整 JWT

用法：
  python3 jb-activate-full.py email password          # 单个
  python3 jb-activate-full.py                          # 交互式
  python3 jb-activate-full.py --batch accounts.csv     # 批量
  python3 jb-activate-full.py --batch accounts.csv --concurrency 3
"""
import sys, json, re, time, base64, hashlib, os, secrets, urllib.parse
import argparse, csv, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    import requests
except ImportError:
    print("缺少 requests 库，请先安装：pip install requests")
    input("\n按回车键退出...")
    sys.exit(1)

# ============================================================
# 固定参数（从 IDEA mitmproxy 抓包，可复用）
# ============================================================
ENCRYPTED_HOSTNAME = "837dXi0iwT8bX6hyYx/jj8C3zRdOhXGfldH6IDWxUGxhR+uNhgtqr0mXpXf/nJd5ieCAGcQXo2XtV2lzBdTEDA=="
ENCRYPTED_USERNAME = "2iPzpOCWsIFuwgcAUOrGzZJDJA2tC1zeZXPkHWhSk5rFRoqp2BtfvhVv6yMaBp9a/opRRmMKvHgHseDc2usEmg=="
MACHINE_ID = "17ff7a9c-ee0d-409f-a556-a85e43c4097a"
MACHINE_UUID = "1-15f741da-48f2-3a49-a2a0-0d45352d1eb6"

JB = "https://account.jetbrains.com"
HUB = "https://oauth.account.jetbrains.com"
AI = "https://api.jetbrains.ai"


def jba_login(email, password):
    """JBA API 登录，返回 session 对象。"""
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    s.get(f"{JB}/login", timeout=15)
    csrf = next((c.value for c in s.cookies if c.name == "_st-JBA"), None)
    h = {"X-XSRF-TOKEN": csrf, "X-Requested-With": "XMLHttpRequest",
         "Content-Type": "application/json", "Origin": JB}
    r = s.post(f"{JB}/api/auth/sessions", headers=h, timeout=15)
    sid = r.json()["id"]
    s.post(f"{JB}/api/auth/sessions/{sid}/email/login", headers=h, json={"email": email}, timeout=15)
    r2 = s.post(f"{JB}/api/auth/sessions/{sid}/password", headers=h, json={"password": password}, timeout=15)
    state = r2.json().get("state", "")
    if state != "REDIRECT_TO_RETURN_URL":
        print(f"  [FAIL] 登录失败: state={state}")
        return None, None
    csrf = next((c.value for c in s.cookies if c.name == "_st-JBA"), csrf)
    h["X-XSRF-TOKEN"] = csrf
    return s, h


def check_ai_status(s):
    """检查 AI 订阅状态。返回 (show_plans, has_card)。"""
    r = s.get(f"{JB}/api/ai/account/settings", timeout=15)
    if r.status_code != 200:
        return True, False
    ai = r.json()
    show = ai.get("personal", {}).get("showAIPlans", True)
    # 如果 showAIPlans=False，试用已激活
    return show, not show


def oauth_pkce(s):
    """OAuth PKCE 流程，返回 id_token + refresh_token。"""
    cv = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()
    cc = base64.urlsafe_b64encode(hashlib.sha256(cv.encode()).digest()).rstrip(b"=").decode()
    st = secrets.token_hex(16)
    url = (f"{JB}/oauth/login?client_id=ide&scope=openid+offline_access+r_ide_auth"
           f"&code_challenge={cc}&code_challenge_method=S256&state={st}"
           f"&redirect_uri={JB}/oauth2/ide/callback&response_type=code"
           f"&client_info=eyJwcm9kdWN0IjoiUFkiLCJidWlsZCI6IjI2MS4yMjE1OC4zNDAifQ")
    code = None
    for i in range(15):
        r = s.get(url, allow_redirects=False, timeout=15)
        loc = r.headers.get("Location", "")
        if not loc:
            # 可能已经在最终页面，检查 URL
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
        print(f"  [FAIL] OAuth 授权码获取失败（{i+1} 次重定向后）")
        return None, None
    print(f"  授权码获取成功（{i+1} 次重定向）")
    print("  正在交换 token...")
    # 用独立请求，不复用 JBA session（避免 cookie 冲突）
    r = requests.post(f"{HUB}/api/rest/oauth2/token", data={
        "grant_type": "authorization_code", "code": code,
        "code_verifier": cv, "client_id": "ide",
        "redirect_uri": f"{JB}/oauth2/ide/callback",
    }, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=15)
    tokens = r.json()
    return tokens.get("id_token", ""), tokens.get("refresh_token", "")


def decode_id_token(id_token):
    """解码 id_token，提取 user_id（Hub ring ID）。"""
    parts = id_token.split(".")
    payload = parts[1] + "==="
    claims = json.loads(base64.urlsafe_b64decode(payload))
    return claims


def obtain_trial(user_id):
    """调 obtainTrial.action 激活 AI 试用。"""
    salt = str(int(time.time() * 1000))
    r = requests.get(f"{JB}/lservice/rpc/obtainTrial.action", params={
        "productFamilyId": "AIP",
        "userId": user_id,
        "hostName": ENCRYPTED_HOSTNAME,
        "salt": salt,
        "ideProductCode": "II",
        "buildDate": "20250416",
        "clientVersion": "21",
        "secure": "false",
        "userName": ENCRYPTED_USERNAME,
        "buildNumber": "2025.1.1 Build IU-251.25410.109",
        "version": "2025100",
        "machineId": MACHINE_ID,
        "productCode": "AIP",
        "expiredLicenseDays": "0",
        "machineUUID": MACHINE_UUID,
        "checkedOption": "AGREEMENT",
    }, headers={"User-Agent": "local"}, timeout=15)
    code = re.search(r"<responseCode>(\w+)</responseCode>", r.text)
    reason = re.search(r"<trialRejectedReason>(.*?)</trialRejectedReason>", r.text)
    rc = code.group(1) if code else "UNKNOWN"
    rr = reason.group(1) if reason else ""
    return rc, rr


def extract_license_ids(s):
    """从 /licenses 页面提取 licenseId。"""
    r = s.get(f"{JB}/licenses", timeout=15)
    return re.findall(r'id="license-([A-Z0-9]{6,12})"', r.text)


def register_grazie(id_token):
    """在 Grazie 后端注册用户。"""
    r = requests.post(f"{AI}/auth/jetbrains-jwt/register",
        headers={"Authorization": f"Bearer {id_token}", "User-Agent": "ktor-client"},
        timeout=15)
    return r.status_code, r.text[:200]


def provide_access(id_token, license_id):
    """获取 JWT（返回 bool + state，保持原接口兼容）。"""
    r = requests.post(f"{AI}/auth/jetbrains-jwt/provide-access/license/v2",
        json={"licenseId": license_id},
        headers={"Authorization": f"Bearer {id_token}",
                 "User-Agent": "ktor-client", "Content-Type": "application/json"},
        timeout=15)
    if r.status_code == 200:
        data = r.json()
        return bool(data.get("token")), data.get("state", "")
    return False, f"HTTP {r.status_code}"


def get_jwt(id_token, license_id):
    """调 provide-access 返回完整 JWT string。成功返回 (jwt_str, state)，失败返回 (None, error)。"""
    r = requests.post(f"{AI}/auth/jetbrains-jwt/provide-access/license/v2",
        json={"licenseId": license_id},
        headers={"Authorization": f"Bearer {id_token}",
                 "User-Agent": "ktor-client", "Content-Type": "application/json"},
        timeout=15)
    if r.status_code == 200:
        data = r.json()
        token = data.get("token", "")
        state = data.get("state", "")
        if token:
            return token, state
        return None, f"state={state}, no token in response"
    return None, f"HTTP {r.status_code}: {r.text[:200]}"


def process_account(email, password):
    """
    处理单个账号的全流程。
    返回 dict: 成功时包含完整凭证，失败时包含 error 字段。
    """
    result = {
        "email": email,
        "password": password,
        "refresh_token": None,
        "id_token": None,
        "license_id": None,
        "user_id": None,
        "jwt": None,
        "obtained_at": None,
        "error": None,
    }

    print(f"\n{'='*60}")
    print(f"JetBrains AI 试用激活（增强版）")
    print(f"账号: {email}")
    print(f"{'='*60}")

    # Step 1: 登录
    print("\n[1] JBA 登录...")
    try:
        s, h = jba_login(email, password)
    except Exception as e:
        result["error"] = f"登录异常: {e}"
        print(f"  [FAIL] {result['error']}")
        return result
    if not s:
        result["error"] = "登录失败"
        return result
    print("  登录成功")

    # Step 2: 检查 AI 状态
    print("[2] 检查 AI 状态...")
    try:
        show_plans, already_active = check_ai_status(s)
    except Exception as e:
        result["error"] = f"检查AI状态异常: {e}"
        print(f"  [FAIL] {result['error']}")
        return result
    if already_active:
        print("  已激活！跳过 obtainTrial")
    else:
        print(f"  showAIPlans={show_plans}（未激活）")

    # Step 3: OAuth PKCE
    print("[3] OAuth PKCE...")
    try:
        id_token, refresh_token = oauth_pkce(s)
    except Exception as e:
        result["error"] = f"OAuth异常: {e}"
        print(f"  [FAIL] {result['error']}")
        return result
    if not id_token:
        result["error"] = "OAuth PKCE 失败，无 id_token"
        return result
    result["id_token"] = id_token
    result["refresh_token"] = refresh_token
    print(f"  id_token: {id_token[:60]}...")
    print(f"  refresh_token: {refresh_token[:60]}..." if refresh_token else "  refresh_token: (空)")

    # Step 4: 提取 user_id
    try:
        claims = decode_id_token(id_token)
    except Exception as e:
        result["error"] = f"解码id_token异常: {e}"
        print(f"  [FAIL] {result['error']}")
        return result
    user_id = claims.get("user_id", "")
    result["user_id"] = user_id
    print(f"[4] Hub user_id: {user_id}")
    if not user_id:
        result["error"] = "id_token 中无 user_id claim"
        print(f"  [FAIL] {result['error']}")
        return result

    # Step 5: 激活试用
    if not already_active:
        print("[5] 调 obtainTrial...")
        try:
            rc, reason = obtain_trial(user_id)
        except Exception as e:
            result["error"] = f"obtainTrial异常: {e}"
            print(f"  [FAIL] {result['error']}")
            return result
        print(f"  responseCode: {rc}")
        if rc == "OK":
            print("  试用激活成功！")
        else:
            result["error"] = f"obtainTrial 失败: {rc} {reason}"
            print(f"  [FAIL] {reason}")
            if reason == "PAYMENT_PROOF_REQUIRED":
                print("  -> 需要先绑卡")
            return result
    else:
        print("[5] 跳过（已激活）")

    # Step 6: 提取 licenseId
    print("[6] 提取 licenseId...")
    time.sleep(1)  # 等传播
    try:
        ids = extract_license_ids(s)
    except Exception as e:
        result["error"] = f"提取licenseId异常: {e}"
        print(f"  [FAIL] {result['error']}")
        return result
    print(f"  License IDs: {ids}")
    if not ids:
        result["error"] = "无 licenseId"
        print(f"  [FAIL] {result['error']}")
        return result

    # Step 7: Grazie 注册
    print("[7] Grazie register...")
    try:
        status, body = register_grazie(id_token)
    except Exception as e:
        print(f"  [WARN] register 异常: {e}（继续）")
        status, body = 0, str(e)
    print(f"  register: {status} {body}")

    # Step 8: 获取完整 JWT
    print("[8] 获取 JWT（provide-access）...")
    for lid in ids:
        try:
            jwt_token, state = get_jwt(id_token, lid)
        except Exception as e:
            print(f"  {lid}: [FAIL] 异常: {e}")
            continue
        if jwt_token:
            result["license_id"] = lid
            result["jwt"] = jwt_token
            result["obtained_at"] = datetime.datetime.utcnow().isoformat() + "Z"

            # 终端打印完整凭证
            print(f"\n{'='*60}")
            print(f"  全流程完成！完整凭证如下：")
            print(f"{'='*60}")
            print(f"  email:         {email}")
            print(f"  password:      {password}")
            print(f"  user_id:       {user_id}")
            print(f"  license_id:    {lid}")
            print(f"  state:         {state}")
            print(f"{'='*60}")
            print(f"  refresh_token:")
            print(f"    {refresh_token}")
            print(f"{'='*60}")
            print(f"  id_token:")
            print(f"    {id_token}")
            print(f"{'='*60}")
            print(f"  jwt:")
            print(f"    {jwt_token}")
            print(f"{'='*60}")
            print(f"  obtained_at:   {result['obtained_at']}")
            print(f"{'='*60}")

            # 保存单个账号凭证 JSON
            safe_email = email.replace("@", "_at_").replace(".", "_")
            cred_file = f"{safe_email}-credentials.json"
            with open(cred_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            print(f"\n  凭证已保存到: {cred_file}")
            return result
        else:
            print(f"  {lid}: jwt=None ({state})")

    result["error"] = "所有 licenseId 都无法获取 JWT"
    print(f"\n  [FAIL] {result['error']}")
    return result


def load_csv(path):
    """读取 CSV 文件，返回 [(email, password), ...]。支持有无表头。"""
    accounts = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            email_val = row[0].strip()
            password_val = row[1].strip()
            # 跳过表头
            if email_val.lower() in ("email", "e-mail", "mail", "account"):
                continue
            if not email_val or not password_val:
                continue
            accounts.append((email_val, password_val))
    return accounts


def main():
    parser = argparse.ArgumentParser(
        description="JetBrains AI 试用激活（增强版 - 完整凭证输出）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python3 jb-activate-full.py user@email.com password123
  python3 jb-activate-full.py
  python3 jb-activate-full.py --batch accounts.csv
  python3 jb-activate-full.py --batch accounts.csv --concurrency 3
        """,
    )
    parser.add_argument("email", nargs="?", default=None, help="邮箱地址")
    parser.add_argument("password", nargs="?", default=None, help="密码")
    parser.add_argument("--batch", metavar="FILE", help="批量模式：CSV 文件路径（email,password）")
    parser.add_argument("--concurrency", type=int, default=1, help="批量并发数（默认1）")
    args = parser.parse_args()

    # 批量模式
    if args.batch:
        if not os.path.isfile(args.batch):
            print(f"[FAIL] CSV 文件不存在: {args.batch}")
            return
        accounts = load_csv(args.batch)
        if not accounts:
            print(f"[FAIL] CSV 文件无有效账号: {args.batch}")
            return
        print(f"批量模式: {len(accounts)} 个账号, 并发={args.concurrency}")
        all_results = []
        if args.concurrency <= 1:
            # 串行
            for idx, (email, password) in enumerate(accounts, 1):
                print(f"\n[{idx}/{len(accounts)}] 处理: {email}")
                result = process_account(email, password)
                all_results.append(result)
        else:
            # 并发
            with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
                futures = {}
                for idx, (email, password) in enumerate(accounts, 1):
                    fut = executor.submit(process_account, email, password)
                    futures[fut] = (idx, email)
                for fut in as_completed(futures):
                    idx, email = futures[fut]
                    try:
                        result = fut.result()
                    except Exception as e:
                        result = {
                            "email": email,
                            "password": "",
                            "refresh_token": None,
                            "id_token": None,
                            "license_id": None,
                            "user_id": None,
                            "jwt": None,
                            "obtained_at": None,
                            "error": f"线程异常: {e}",
                        }
                    all_results.append(result)

        # 汇总保存
        out_file = "jb-credentials-all.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)

        # 汇总打印
        ok_count = sum(1 for r in all_results if r.get("jwt"))
        fail_count = len(all_results) - ok_count
        print(f"\n{'='*60}")
        print(f"批量完成: {ok_count} 成功 / {fail_count} 失败 / 共 {len(all_results)}")
        print(f"结果已保存到: {out_file}")
        print(f"{'='*60}")
        for r in all_results:
            status = "OK" if r.get("jwt") else f"FAIL: {r.get('error', 'unknown')}"
            print(f"  {r['email']}: {status}")
        return

    # 单个模式
    if args.email and args.password:
        email, password = args.email, args.password
    elif args.email:
        email = args.email
        password = input("Password: ")
    else:
        email = input("Email: ")
        password = input("Password: ")

    process_account(email, password)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n[异常] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            input("\n按回车键退出...")
        except (EOFError, KeyboardInterrupt):
            pass
