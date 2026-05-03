# -*- coding: utf-8 -*-
"""Pinterest 자동 핀 생성 — 스마트스토어 상품 등록 시 자동 포스팅."""
from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

import httpx

log = logging.getLogger("pinterest_auto")

PINTEREST_API = "https://api.pinterest.com/v5"
DATA_DIR = Path(os.environ.get("DATA_DIR", "./data"))
PINNED_FILE = DATA_DIR / "pinterest_pinned.json"


def _pinterest_token() -> str:
    return os.environ.get("PINTEREST_ACCESS_TOKEN", "")

def _pinterest_board_id() -> str:
    return os.environ.get("PINTEREST_BOARD_ID", "")

def _naver_seller_id() -> str:
    return os.environ.get("NAVER_SELLER_ID", "")


# ── 핀 이력 추적 (중복 방지) ──────────────────────────────────────────────────
def _load_pinned() -> set:
    try:
        if PINNED_FILE.exists():
            return set(json.loads(PINNED_FILE.read_text(encoding="utf-8")))
    except Exception:
        pass
    return set()

def _save_pinned(pinned: set) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    PINNED_FILE.write_text(json.dumps(list(pinned), ensure_ascii=False), encoding="utf-8")


# ── URL 생성 ──────────────────────────────────────────────────────────────────
def build_smartstore_url(channel_product_no: str) -> str:
    seller_id = _naver_seller_id()
    if seller_id and channel_product_no:
        return f"https://smartstore.naver.com/{seller_id}/products/{channel_product_no}"
    return f"https://smartstore.naver.com/{seller_id}" if seller_id else "https://smartstore.naver.com"


# ── 핀 생성 ───────────────────────────────────────────────────────────────────
def _build_description(name: str, product_url: str) -> str:
    tags = "#스마트스토어 #네이버쇼핑 #한국쇼핑 #shopping #korea #kstyle #fashion #trending"
    return f"{name}\n\n네이버 스마트스토어에서 만나보세요 →\n{product_url}\n\n{tags}"[:500]


async def create_pinterest_pin(
    name: str,
    image_url: str,
    product_url: str,
    description: str = "",
) -> dict:
    """
    Pinterest v5 API로 핀 생성.
    필요 환경변수: PINTEREST_ACCESS_TOKEN, PINTEREST_BOARD_ID
    """
    token = _pinterest_token()
    board_id = _pinterest_board_id()

    if not token or not board_id:
        log.warning("[Pinterest] PINTEREST_ACCESS_TOKEN 또는 PINTEREST_BOARD_ID 미설정 — 스킵")
        return {"status": "skipped", "reason": "missing_credentials"}

    if not image_url:
        return {"status": "skipped", "reason": "no_image"}

    desc = description or _build_description(name, product_url)
    payload = {
        "board_id": board_id,
        "title": name[:100],
        "description": desc,
        "link": product_url,
        "media_source": {
            "source_type": "image_url",
            "url": image_url,
        },
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{PINTEREST_API}/pins",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
        if resp.status_code in (200, 201):
            pin_id = resp.json().get("id", "")
            log.info("[Pinterest] 핀 생성 완료: id=%s, title=%s", pin_id, name[:40])
            return {"status": "created", "pin_id": pin_id, "title": name}
        log.warning("[Pinterest] 핀 생성 실패: %s %s", resp.status_code, resp.text[:200])
        return {"status": "error", "code": resp.status_code, "body": resp.text[:200]}
    except Exception as e:
        log.warning("[Pinterest] 핀 생성 오류: %s", e)
        return {"status": "error", "error": str(e)}


# ── 최근 스마트스토어 상품 일괄 핀 ──────────────────────────────────────────
async def pin_recent_smartstore_products(days: int = 1, max_pins: int = 10) -> dict:
    """
    Naver API에서 최근 days일 내 등록 상품 조회 → 아직 핀 안 된 상품 Pinterest 포스팅.
    pinned 이력은 PINNED_FILE에 저장해 중복 방지.
    """
    from main import naver_api

    pinned = _load_pinned()
    try:
        data = await naver_api.list_products(page=1, size=50, days=days)
    except Exception as e:
        log.warning("[Pinterest] 상품 목록 조회 실패: %s", e)
        return {"pinned": 0, "error": str(e)}

    contents = data.get("contents", [])
    pinned_count = 0
    skipped_count = 0
    errors = []

    for item in contents:
        if pinned_count >= max_pins:
            break

        channel_no = str(item.get("channelProductNo", ""))
        origin_no = str(item.get("originProductNo", ""))
        uid = channel_no or origin_no
        if not uid or uid in pinned:
            skipped_count += 1
            continue

        origin = item.get("originProduct", {})
        name = (origin.get("name") or item.get("name", "")).strip()
        if not name:
            continue

        # 대표 이미지 URL 추출
        images = origin.get("images", {})
        rep_img = images.get("representativeImage") or {}
        image_url = rep_img.get("url", "")
        if not image_url:
            # 썸네일 이미지 폴백
            thumb_list = images.get("optionalImages") or []
            if thumb_list:
                image_url = (thumb_list[0] or {}).get("url", "")
        if not image_url:
            skipped_count += 1
            continue

        product_url = build_smartstore_url(channel_no)
        result = await create_pinterest_pin(name, image_url, product_url)

        if result.get("status") == "created":
            pinned.add(uid)
            pinned_count += 1
        elif result.get("status") == "skipped":
            # 자격증명 없음 → 더 이상 시도 불필요
            break
        else:
            errors.append({"uid": uid, "error": result.get("error") or result.get("body", "")})

    _save_pinned(pinned)
    log.info("[Pinterest] 스마트스토어 일괄 핀 완료: %d개 (스킵 %d개)", pinned_count, skipped_count)
    return {"pinned": pinned_count, "skipped": skipped_count, "errors": errors}


async def get_pinterest_boards() -> list[dict]:
    """보드 목록 조회 (PINTEREST_BOARD_ID 설정 확인용)."""
    token = _pinterest_token()
    if not token:
        return []
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{PINTEREST_API}/boards",
                headers={"Authorization": f"Bearer {token}"},
                params={"page_size": 25},
            )
        if resp.status_code == 200:
            return resp.json().get("items", [])
        log.warning("[Pinterest] 보드 조회 실패: %s", resp.status_code)
        return []
    except Exception as e:
        log.warning("[Pinterest] 보드 조회 오류: %s", e)
        return []
