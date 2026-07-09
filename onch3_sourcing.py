"""온채널(onch3.co.kr) 도매 DB 소싱 모듈.

DG API IP 차단 시 폴백 소싱원으로 사용.
쿠키 기반 로그인 → /dbcenter_renewal/index.php HTML 파싱 → 상품 dict 반환.
"""
from __future__ import annotations

import asyncio
import http.cookiejar
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import httpx

ONCH3_ID = os.environ.get("ONCH3_ID", "mnm1876@naver.com")
ONCH3_PW = os.environ.get("ONCH3_PW", "gusdn@5603")

_BASE = "https://www.onch3.co.kr"
_LOGIN_URL = f"{_BASE}/login/login_web.php"
_CATALOG_URL = f"{_BASE}/dbcenter_renewal/index.php"
_DETAIL_URL = f"{_BASE}/dbcenter_renewal/detail.php"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

_CALL_INTERVAL = 1.5  # 서버 부하 최소화


def _make_opener() -> urllib.request.OpenerDirector:
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = list(_HEADERS.items())
    return opener


def _login(opener: urllib.request.OpenerDirector) -> bool:
    """온채널 PHP 사이트 로그인. 성공 시 True, 실패 시 False."""
    try:
        opener.open(_LOGIN_URL, timeout=10)  # PHPSESSID 획득
        post = urllib.parse.urlencode({
            "referer_url": "/index.php",
            "username": ONCH3_ID,
            "password": ONCH3_PW,
            "login": "",
        }).encode()
        req = urllib.request.Request(
            _LOGIN_URL, data=post,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        r = opener.open(req, timeout=10)
        html = r.read(4000).decode("utf-8", errors="replace")
        # 로그인 실패 시 "로그인" 폼 다시 등장
        if 'name="username"' in html or "비밀번호를 잘못" in html:
            print("[ONCH3] 로그인 실패 — ID/PW 확인 필요", flush=True)
            return False
        return True
    except Exception as e:
        print(f"[ONCH3] 로그인 오류: {e}", flush=True)
        return False


def _parse_products(html: str) -> list[dict]:
    """dbcenter_renewal index 페이지 HTML → 상품 dict 리스트 파싱."""
    blocks = re.findall(r'<li>\s*<dl class="product_set">(.*?)</dl>', html, re.DOTALL)
    products: list[dict] = []
    for block in blocks:
        code_m = re.search(r'name=["\']list_count\[\]["\'][^>]*value=["\']([^"\']+)["\']', block)
        title_m = re.search(r'<dt class="product_title">([^<]+)</dt>', block)
        price_m = re.search(r"<span class='price'>([\d,]+)</span>", block)
        img_m = re.search(r'data-src=["\']([^"\']+onimg\.onch3\.co\.kr[^"\']+)["\']', block)
        num_m = re.search(r'detail\.php\?num=(\d+)', block)

        if not (code_m and title_m and price_m):
            continue
        price_val = int(price_m.group(1).replace(",", ""))
        if price_val <= 0:
            continue
        products.append({
            "code": f"ONCH3_{code_m.group(1)}",
            "_onch3_code": code_m.group(1),
            "name": title_m.group(1).strip(),
            "price": price_val,
            "image": img_m.group(1) if img_m else "",
            "_onch3_detail_num": num_m.group(1) if num_m else "",
            "source": "onch3",
            "category": "",  # 검색어 기반 추론은 caller가 처리
            "stock": 99,
        })
    return products


def _get_detail_images(opener: urllib.request.OpenerDirector, detail_num: str) -> list[str]:
    """상품 상세 페이지에서 추가 이미지 URL 수집 (최대 10개)."""
    if not detail_num:
        return []
    try:
        url = f"{_DETAIL_URL}?num={detail_num}"
        r = opener.open(url, timeout=10)
        html = r.read().decode("utf-8", errors="replace")
        imgs = re.findall(
            r'(?:data-src|src)=["\']([^"\']*onimg\.onch3\.co\.kr[^"\']+)["\']', html
        )
        return list(dict.fromkeys(imgs))[:10]
    except Exception:
        return []


def fetch_onch3_products_sync(
    keywords: list[str],
    pool_size: int = 60,
    min_price: int = 2000,
    max_price: int = 150000,
    fetch_detail_images: bool = True,
) -> list[dict]:
    """온채널 키워드 검색 → 상품 dict 리스트 반환 (동기 버전).

    Args:
        keywords: 검색 키워드 리스트
        pool_size: 최대 수집 개수
        min_price: 최소 도매가 (원)
        max_price: 최대 도매가 (원)
        fetch_detail_images: True 시 상세 페이지에서 추가 이미지 수집

    Returns:
        SS 파이프라인 호환 상품 dict 리스트.
    """
    if not ONCH3_ID or not ONCH3_PW:
        print("[ONCH3] ONCH3_ID/PW 미설정 → 스킵", flush=True)
        return []

    opener = _make_opener()
    if not _login(opener):
        return []

    seen: set[str] = set()
    results: list[dict] = []

    for kw in keywords:
        if len(results) >= pool_size:
            break
        kw_enc = urllib.parse.quote(kw)
        try:
            url = f"{_CATALOG_URL}?keyword={kw_enc}&page=1"
            r = opener.open(url, timeout=15)
            html = r.read().decode("utf-8", errors="replace")
            products = _parse_products(html)

            for p in products:
                if len(results) >= pool_size:
                    break
                code = p["_onch3_code"]
                if code in seen:
                    continue
                if not (min_price <= p["price"] <= max_price):
                    continue
                seen.add(code)

                if fetch_detail_images and p["_onch3_detail_num"]:
                    time.sleep(0.5)
                    extra = _get_detail_images(opener, p["_onch3_detail_num"])
                    # 첫 번째 이미지가 메인 이미지로 더 좋을 수 있음
                    if extra and not p["image"]:
                        p["image"] = extra[0]
                    p["_dg_extra_images"] = [u for u in extra if u != p["image"]][:9]
                else:
                    p["_dg_extra_images"] = []

                results.append(p)
            print(f"[ONCH3] '{kw}' → {len(products)}개 파싱, 누적 {len(results)}개", flush=True)
            time.sleep(_CALL_INTERVAL)
        except Exception as e:
            print(f"[ONCH3] '{kw}' 검색 오류: {e}", flush=True)
            time.sleep(2)

    return results


async def fetch_onch3_products(
    keywords: list[str],
    pool_size: int = 60,
    min_price: int = 2000,
    max_price: int = 150000,
) -> list[dict]:
    """비동기 래퍼 — 동기 함수를 executor에서 실행."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        lambda: fetch_onch3_products_sync(
            keywords=keywords,
            pool_size=pool_size,
            min_price=min_price,
            max_price=max_price,
        ),
    )


# ═══════════════════════════════════════════════════════════════════════
# 자동발주 섹션 — 도매꾹 RPA와 동일 안전장치 구조
# ═══════════════════════════════════════════════════════════════════════

_API_BASE = "https://api.onch3.co.kr"
_ORDER_STATE_FILE = Path(__file__).parent / "onch3_state.json"

MAX_CONSECUTIVE_FAIL = 2    # 연속 실패 halt 임계값 (DG RPA 동일)
DAILY_ORDER_LIMIT   = 20    # 일일 발주 한도
LOW_POINT_THRESHOLD = 10_000  # 포인트 잔액 경고 기준 (원)
_KST = timezone(timedelta(hours=9))


def _today_kst() -> str:
    return datetime.now(_KST).strftime("%Y-%m-%d")


def _hour_kst() -> int:
    return datetime.now(_KST).hour


# ── 상태 관리 (DG RPA 동일 구조) ────────────────────────────────────────

def _load_order_state() -> dict:
    if _ORDER_STATE_FILE.exists():
        try:
            return json.loads(_ORDER_STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"date": "", "daily_count": 0, "consecutive_fail": 0}


def _save_order_state(state: dict) -> None:
    _ORDER_STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _record_order_success() -> None:
    state = _load_order_state()
    today = _today_kst()
    if state.get("date") != today:
        state = {"date": today, "daily_count": 0, "consecutive_fail": 0}
    state["daily_count"] += 1
    state["consecutive_fail"] = 0
    _save_order_state(state)


def _record_order_fail() -> None:
    state = _load_order_state()
    today = _today_kst()
    if state.get("date") != today:
        state = {"date": today, "daily_count": 0, "consecutive_fail": 0}
    state["consecutive_fail"] += 1
    _save_order_state(state)


def reset_onch3_consecutive_fail() -> None:
    """연속 실패 카운터 수동 초기화 (halt 해제 시 사용)."""
    state = _load_order_state()
    state["consecutive_fail"] = 0
    _save_order_state(state)


def check_order_limits() -> tuple[bool, str]:
    """발주 가능 여부 체크. Returns (ok, reason)."""
    h = _hour_kst()
    if not (9 <= h < 24):
        return False, f"운영시간 아님 ({h}시 KST — 09:00~24:00만 허용)"
    state = _load_order_state()
    today = _today_kst()
    if state.get("date") != today:
        state = {"date": today, "daily_count": 0, "consecutive_fail": 0}
        _save_order_state(state)
    if state["consecutive_fail"] >= MAX_CONSECUTIVE_FAIL:
        return False, f"연속 실패 {state['consecutive_fail']}회 — 수동 확인 필요 (halt)"
    if state["daily_count"] >= DAILY_ORDER_LIMIT:
        return False, f"일일 한도 {DAILY_ORDER_LIMIT}건 초과"
    return True, "OK"


# ── 유틸 ────────────────────────────────────────────────────────────────

def _get_jwt() -> str:
    """온채널 API JWT 발급. 실패 시 빈 문자열."""
    try:
        url = (
            f"{_API_BASE}/api/v1/getToken"
            f"?member_id={urllib.parse.quote(ONCH3_ID)}"
            f"&member_pw={urllib.parse.quote(ONCH3_PW)}"
        )
        req = urllib.request.Request(
            url, method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
        )
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())
        return data.get("Authorization", "")
    except Exception as e:
        print(f"[ONCH3] JWT 발급 실패: {e}", flush=True)
        return ""


def get_point_balance() -> Optional[int]:
    """포인트 잔액 조회. 실패 시 None."""
    jwt = _get_jwt()
    if not jwt:
        return None
    try:
        req = urllib.request.Request(
            f"{_API_BASE}/api/v1/member/point",
            headers={"Authorization": f"Bearer {jwt}", "User-Agent": "Mozilla/5.0"},
        )
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())
        return int(data.get("point", 0))
    except Exception as e:
        print(f"[ONCH3] 포인트 조회 실패: {e}", flush=True)
        return None


def _tg_alert(text: str) -> None:
    """긴급 텔레그램 알림 (포인트 부족 / halt 전용)."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not (token and chat_id):
        print(f"[ONCH3-TG] {text}", flush=True)
        return
    try:
        payload = json.dumps({"chat_id": chat_id, "text": text}).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


# ── 취소 (롤백) ─────────────────────────────────────────────────────────

def cancel_onch3_order(order_code: str) -> bool:
    """온채널 주문 취소 — 공급사 미확인 상태(cancle1) 즉시환불.

    Returns True if cancelled successfully.
    """
    opener = _make_opener()
    if not _login(opener):
        return False
    try:
        data = urllib.parse.urlencode({"code": order_code}).encode()
        req = urllib.request.Request(
            f"{_BASE}/access/order_access.php?ubr=order_cc&sec=1",
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": f"{_BASE}/seller/orders.php?state=preparing",
                "X-Requested-With": "XMLHttpRequest",
            },
            method="POST",
        )
        resp = opener.open(req, timeout=15)
        result = resp.read().decode("utf-8", errors="replace").strip()
        if result == "cancle1":
            print(f"[ONCH3] ✅ 취소+즉시환불 완료: {order_code}", flush=True)
            return True
        elif result in ("cancle2", "cancle5"):
            print(f"[ONCH3] ⚠️ 취소 요청 접수 ({result}): {order_code}", flush=True)
            return True
        else:
            print(f"[ONCH3] ❌ 취소 실패 ({result}): {order_code}", flush=True)
            return False
    except Exception as e:
        print(f"[ONCH3] 취소 예외: {e}", flush=True)
        return False


# ── 발주 (메인) ─────────────────────────────────────────────────────────

def place_onch3_order(
    prd_code: str,
    option_nm: str,
    qty: int,
    order_name: str,
    order_phone: str,
    zipcode: str,
    address: str,
    memo: str = "",
    sale_code: str = "",
    delivery_type: str = "선불",
) -> dict:
    """온채널 발주 API 호출 (안전장치 포함).

    Args:
        prd_code:     상품코드. ONCH3_CH... 또는 CH... 모두 허용.
        option_nm:    옵션명 (단일옵션이면 상품명과 동일하게)
        qty:          수량
        order_name:   수령인 이름
        order_phone:  수령인 전화번호
        zipcode:      우편번호
        address:      배송지 주소
        memo:         배송 메모 (optional)
        sale_code:    내부 주문번호 — SS/쿠팡 주문ID 권장
        delivery_type: 배송비 방식 (기본: 선불)

    Returns:
        {
          "ok": bool,
          "order_code": str,      # 온채널 발주번호 GO_...
          "error": str,
          "halt": bool,           # True면 연속실패 → 수동 개입 필요
          "daily_count": int,
          "point_after": int|None,
        }
    """
    # ── 1. 운영시간 / 연속실패 / 일일한도 체크 ───────────────────────
    can_order, reason = check_order_limits()
    if not can_order:
        halt = "halt" in reason
        print(f"[ONCH3] 발주 차단: {reason}", flush=True)
        return {"ok": False, "error": reason, "halt": halt,
                "daily_count": _load_order_state().get("daily_count", 0),
                "order_code": "", "point_after": None}

    # ── 2. 포인트 사전 체크 ──────────────────────────────────────────
    point = get_point_balance()
    if point is not None and point < LOW_POINT_THRESHOLD:
        msg = (
            f"⚠️ [온채널 포인트 부족]\n"
            f"잔액: {point:,}P (기준: {LOW_POINT_THRESHOLD:,}P)\n"
            f"충전 필요: https://www.onch3.co.kr/mypage.php"
        )
        _tg_alert(msg)
        print(f"[ONCH3] {msg}", flush=True)
        if point == 0:
            _record_order_fail()
            return {"ok": False, "error": f"포인트 잔액 0P", "halt": False,
                    "daily_count": _load_order_state().get("daily_count", 0),
                    "order_code": "", "point_after": 0}

    # ── 3. 상품코드 정규화 (ONCH3_ 접두어 제거) ──────────────────────
    raw_code = prd_code.removeprefix("ONCH3_") if prd_code.startswith("ONCH3_") else prd_code

    # ── 4. JWT 발급 ────────────────────────────────────────────────────
    jwt = _get_jwt()
    if not jwt:
        _record_order_fail()
        state = _load_order_state()
        consec = state.get("consecutive_fail", 0)
        if consec >= MAX_CONSECUTIVE_FAIL:
            _tg_alert(f"🚨 [온채널 발주 halt]\n연속 {consec}회 실패 — 수동 확인 필요\n상품: {raw_code}")
        return {"ok": False, "error": "JWT 발급 실패", "halt": consec >= MAX_CONSECUTIVE_FAIL,
                "daily_count": state.get("daily_count", 0), "order_code": "", "point_after": None}

    # ── 5. 발주 API ────────────────────────────────────────────────────
    payload = json.dumps({
        "prd_code": raw_code,
        "options": [{"option_nm": option_nm, "qty": qty}],
        "delivery_type": delivery_type,
        "order_name": order_name,
        "order_phone": order_phone,
        "zipcode": zipcode,
        "order_address": address,
        "order_memo": memo,
        "sale_code": sale_code,
        "address_id": 3,
    }, ensure_ascii=False).encode("utf-8")

    order_code = ""
    try:
        req = urllib.request.Request(
            f"{_API_BASE}/api/v1/order/regist",
            data=payload,
            headers={
                "Authorization": f"Bearer {jwt}",
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "Mozilla/5.0",
            },
            method="POST",
        )
        resp = urllib.request.urlopen(req, timeout=15)
        result = json.loads(resp.read())
    except Exception as e:
        print(f"[ONCH3] 발주 API 예외: {e}", flush=True)
        _record_order_fail()
        state = _load_order_state()
        consec = state.get("consecutive_fail", 0)
        halt = consec >= MAX_CONSECUTIVE_FAIL
        if halt:
            _tg_alert(f"🚨 [온채널 발주 halt]\n연속 {consec}회 예외 실패 — 수동 확인 필요\n상품: {raw_code}")
        return {"ok": False, "error": str(e), "halt": halt,
                "daily_count": state.get("daily_count", 0), "order_code": "", "point_after": None}

    # ── 6. 결과 분기 ───────────────────────────────────────────────────
    if result.get("isSuccess"):
        order_code = result.get("order_code", "")
        _record_order_success()
        state = _load_order_state()
        point_after = get_point_balance()

        # 발주 후 포인트 재체크
        if point_after is not None and point_after < LOW_POINT_THRESHOLD:
            _tg_alert(
                f"⚠️ [온채널 포인트 부족]\n"
                f"발주 후 잔액: {point_after:,}P\n"
                f"충전 필요: https://www.onch3.co.kr/mypage.php"
            )
        print(
            f"[ONCH3] ✅ 발주 성공: order_code={order_code} "
            f"일일={state.get('daily_count')}/{DAILY_ORDER_LIMIT} 포인트={point_after}P",
            flush=True,
        )
        return {
            "ok": True, "order_code": order_code, "error": "",
            "halt": False, "daily_count": state.get("daily_count", 0),
            "point_after": point_after,
        }
    else:
        err = result.get("message", str(result))
        print(f"[ONCH3] ❌ 발주 실패: {err}", flush=True)
        _record_order_fail()
        state = _load_order_state()
        consec = state.get("consecutive_fail", 0)
        halt = consec >= MAX_CONSECUTIVE_FAIL
        if halt:
            _tg_alert(
                f"🚨 [온채널 발주 halt]\n연속 {consec}회 실패 — 수동 확인 필요\n"
                f"오류: {err[:120]}"
            )
        return {
            "ok": False, "order_code": "", "error": err,
            "halt": halt, "daily_count": state.get("daily_count", 0),
            "point_after": None,
        }
