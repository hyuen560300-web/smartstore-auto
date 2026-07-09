"""온채널(onch3.co.kr) 도매 DB 소싱 모듈.

DG API IP 차단 시 폴백 소싱원으로 사용.
쿠키 기반 로그인 → /dbcenter_renewal/index.php HTML 파싱 → 상품 dict 반환.
"""
from __future__ import annotations

import asyncio
import http.cookiejar
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
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
