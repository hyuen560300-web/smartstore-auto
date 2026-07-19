"""
스마트스토어 자동화 - FastAPI 서버
총괄팀장: Claude
"""

import asyncio
import os
import json
import sys
import httpx
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, BackgroundTasks, Request, UploadFile, File
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pathlib import Path
from dotenv import load_dotenv

from pinterest_auto import (
    create_pinterest_pin, pin_recent_smartstore_products,
    get_pinterest_boards, build_smartstore_url,
)

load_dotenv()

# Windows cp949 터미널에서 em dash 등 UTF-8 문자 인코딩 오류 방지
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from main import (
    pipeline_register_products,
    pipeline_process_orders,
    pipeline_sync_inventory,
    pipeline_reply_inquiries,
    calculate_selling_price,
    EXCEL_FOLDER,
    ANTHROPIC_API_KEY,
    MARGIN_RATE,
    naver_api,
    parse_excel,
    run_qc_pipeline,
    build_dalle_prompt_smart,
    generate_dalle_image,
    generate_dalle_banner,
    generate_dalle_detail_shot,
    get_product_image,
    build_detail_html,
    generate_product_copy,
    build_product_payload,
    save_registered_code,
    load_registered_codes,
    save_registered_name,
    load_registered_names,
    _normalize_name,
    create_banner_image,
    _get_scene_context,
    pipeline_register_from_domeggook,
    fetch_domeggook_products,
    DOMEGGOOK_API_KEY,
    _DG_KEYWORDS,
    pipeline_fix_products,
    NAVER_BASE,
    update_existing_products_seo,
    generate_claude_html_detail,
    _count_html_sections,
    _validate_copy_fields,
    _tg_notify,
    pipeline_reapply_claude_html,
    _save_cost_price_async,
)
from employees import (
    employee_season_planner,
    employee_trend_scout,
    employee_naver_fashion_trend_scout,
    employee_accounting_manager,
    employee_error_auditor,
    employee_shortform_creator,
    employee_blog_manager,
    employee_review_analyst,
    employee_stock_guardian,
    employee_ad_analyst,
    employee_platform_expander,
    employee_event_manager,
)
import employees as _employees_module

app = FastAPI(title="스마트스토어 자동화 AI 직원단", version="3.0.0")

_AUDIT_CACHE: dict = {"status": "idle", "scanned": 0, "products": []}

_PURGE_CACHE: dict = {"status": "idle", "scanned": 0, "deleted": 0, "failed": 0, "log": []}

_notified_order_ids: set = set()  # 텔레그램 이미 알림한 주문 ID (재시작 시 초기화)

_PURGE_KEEP = [
    "에펠탑", "이중유리 커피", "커피 추출기", "통풍시트커버", "통풍 시트커버",
    "레인부츠", "미스트 분사기", "미스트분사기", "나노 미스트",
    "텃밭 재배키트", "텃밭재배키트", "미니텃밭 재배",
    "실외기 커버", "실외기커버", "에어컨 실외기",
    "아크릴 매니큐어 정리대", "매니큐어 정리대",
    "베란다 텃밭 부직포 화분",
]
_PURGE_OFFSEASON = {"13563365983", "13563364477", "13563153288", "13562886644"}


async def _run_purge_low_price():
    global _PURGE_CACHE
    _PURGE_CACHE = {"status": "running", "scanned": 0, "deleted": 0, "failed": 0, "log": []}
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        for p in contents:
            product_no = str(p.get("originProductNo", ""))
            origin = p.get("originProduct", {})
            name = (origin.get("name") or "").strip()
            price = int(origin.get("salePrice") or origin.get("price") or 0)
            _PURGE_CACHE["scanned"] += 1

            should_delete = False
            reason = ""
            if product_no in _PURGE_OFFSEASON:
                should_delete = True
                reason = "비시즌"
            elif price > 0 and price < 10000 and not any(k in name for k in _PURGE_KEEP):
                should_delete = True
                reason = f"가격미만 ₩{price}"

            if should_delete and product_no:
                try:
                    ok = await naver_api.delete_product(product_no)
                    if ok:
                        _PURGE_CACHE["deleted"] += 1
                        _PURGE_CACHE["log"].append({"no": product_no, "name": name, "reason": reason, "ok": True})
                    else:
                        _PURGE_CACHE["failed"] += 1
                        _PURGE_CACHE["log"].append({"no": product_no, "name": name, "reason": "삭제 실패", "ok": False})
                except Exception as e:
                    _PURGE_CACHE["failed"] += 1
                    _PURGE_CACHE["log"].append({"no": product_no, "name": name, "reason": str(e), "ok": False})
                await asyncio.sleep(0.3)

        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(0.3)

    _PURGE_CACHE["status"] = "done"
    print(f"[PURGE] 완료 — 스캔:{_PURGE_CACHE['scanned']} 삭제:{_PURGE_CACHE['deleted']} 실패:{_PURGE_CACHE['failed']}", flush=True)


@app.post("/products/purge-low-price")
async def products_purge_low_price(background_tasks: BackgroundTasks):
    """전체 상품 순회 → 1만원 미만 + 비시즌 백그라운드 삭제. 결과: /products/purge-result"""
    if _PURGE_CACHE.get("status") == "running":
        return JSONResponse({"status": "already_running", "scanned": _PURGE_CACHE.get("scanned", 0)})
    background_tasks.add_task(_run_purge_low_price)
    return JSONResponse({"status": "started"})


@app.get("/products/purge-result")
async def products_purge_result():
    """purge-low-price 진행 상황 및 결과 반환."""
    c = _PURGE_CACHE
    return JSONResponse({
        "status": c.get("status"),
        "scanned": c.get("scanned", 0),
        "deleted": c.get("deleted", 0),
        "failed": c.get("failed", 0),
        "log": c.get("log", []),
    })


# ─── 기본 ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "smartstore_auto", "version": "4.3"}


async def _run_seo_title_refresh(limit: int = 30) -> dict:
    """네이버 트렌드 키워드로 상품 제목 갱신. 스케줄러·엔드포인트 공용."""
    from employees import employee_trend_scout, employee_naver_fashion_trend_scout
    from main import NAVER_DATALAB_CLIENT_ID, NAVER_DATALAB_CLIENT_SECRET

    google_kws: list[str] = await employee_trend_scout() or []
    fashion_kws: list[str] = await employee_naver_fashion_trend_scout(
        NAVER_DATALAB_CLIENT_ID, NAVER_DATALAB_CLIENT_SECRET, top_n=10, ratio_threshold=10.0
    ) if NAVER_DATALAB_CLIENT_ID else []
    trend_kws = list(dict.fromkeys(google_kws + fashion_kws))[:15]

    if not trend_kws:
        # 폴백: 도매꾹 검색 키워드 사용
        trend_kws = list(_DG_KEYWORDS)[:15]
        print("[SEO갱신] 트렌드 수집 실패 — 기본 키워드로 대체", flush=True)
    if not trend_kws:
        return {"ok": False, "reason": "트렌드 키워드 없음"}

    products_data = await naver_api.list_products(page=1, size=100)
    items = products_data.get("contents", [])
    if not items:
        return {"ok": False, "reason": "상품 목록 없음"}

    updated = skipped = errors = 0
    log_items: list[dict] = []
    for item in items[:limit]:
        prod_id  = str(item.get("originProductNo", "") or item.get("id", ""))
        origin   = item.get("originProduct", {})
        cur_name = str(origin.get("name", "") or item.get("name", ""))
        category = str(
            (origin.get("detailAttribute") or {}).get("naverShoppingSearchInfo", {}).get("category1Name", "")
            or item.get("wholeCategoryName", "")
        )
        price    = int(origin.get("salePrice", 0) or item.get("salePrice", 0))
        status   = str(item.get("statusType", "") or origin.get("statusType", ""))

        if not prod_id or not cur_name:
            continue
        if status not in ("SALE", "", "ON_SALE", "SALE_STOPPED"):
            skipped += 1
            continue
        if sum(1 for kw in trend_kws[:5] if kw in cur_name) >= 2:
            skipped += 1
            continue

        try:
            ai = await generate_product_copy(
                {"name": cur_name, "category": category, "price": price},
                {"trends": trend_kws[:8]},
            )
            new_name = (ai.get("product_name") or "").strip()
            if not new_name or new_name == cur_name:
                skipped += 1
                continue
            # Naver API: 부분 업데이트 불가 — 기존 originProduct 전체에 name만 교체
            full_payload = dict(origin)
            full_payload["name"] = new_name
            ok, err = await naver_api.update_product(prod_id, full_payload)
            if ok:
                updated += 1
                log_items.append({"id": prod_id, "before": cur_name[:30], "after": new_name[:30]})
                print(f"  [SEO] ✅ {cur_name[:20]} → {new_name[:20]}", flush=True)
            else:
                errors += 1
                print(f"  [SEO] ❌ {cur_name[:20]}: {err[:60]}", flush=True)
        except Exception as exc:
            errors += 1
            print(f"  [SEO] 예외 ({cur_name[:20]}): {exc}", flush=True)

    print(f"[SEO갱신] 완료 — 갱신 {updated} / 스킵 {skipped} / 오류 {errors}", flush=True)
    return {"ok": True, "updated": updated, "skipped": skipped, "errors": errors,
            "trend_keywords": trend_kws[:8], "items": log_items}


@app.post("/seo-refresh")
async def seo_refresh(limit: int = 30, sync: bool = False):
    """즉시 SEO 제목 갱신. sync=true면 완료까지 기다리고 결과 반환."""
    if sync:
        result = await _run_seo_title_refresh(limit)
        return result
    import asyncio
    asyncio.create_task(_run_seo_title_refresh(limit))
    return {"ok": True, "message": f"SEO 제목 갱신 시작 (최대 {limit}개)", "status": "running"}


@app.get("/status")
async def status():
    """오케스트레이터용 — 등록 상품 수 + 오늘 주문/매출 요약."""
    codes = load_registered_codes()
    today_orders = 0
    today_revenue = 0
    new_orders = 0
    try:
        orders = await naver_api.get_new_orders()
        today_orders = len(orders)
        for o in orders:
            today_revenue += int(o.get("totalPaymentAmount") or 0)
            if o.get("productOrderStatus") in ("PAYED", "PAYMENT_WAITING"):
                new_orders += 1
    except Exception:
        pass
    return JSONResponse({
        "status": "ok",
        "service": "smartstore_auto",
        "registered_count": len(codes),
        "today_orders": today_orders,
        "today_revenue": today_revenue,
        "new_orders": new_orders,
    })


@app.get("/products/random")
async def products_random(exclude: str = "귀마개,풍선,가랜드,실리콘,장갑"):
    """등록 상품 중 랜덤 1개 반환 (Higgsfield 영상 테스트용)."""
    import random as _rand
    try:
        excl = [k.strip() for k in exclude.split(",") if k.strip()]
        data = await naver_api.list_products(page=1, size=20)
        items = data.get("contents", [])
        filtered = []
        for p in items:
            op = p.get("originProduct") or {}
            name = p.get("name") or op.get("name") or ""
            status = p.get("statusType") or op.get("statusType") or "SALE"
            if status in ("SALE", "OUTOFSTOCK", ""):
                if not any(k in name for k in excl):
                    filtered.append(p)
        if not filtered:
            return JSONResponse({"error": "조건에 맞는 상품 없음", "total": len(items)}, status_code=404)
        pick = _rand.choice(filtered)
        op = pick.get("originProduct") or {}
        name = pick.get("name") or op.get("name") or "?"
        pid  = pick.get("channelProductNo") or pick.get("originProductNo") or ""
        price = pick.get("salePrice") or op.get("salePrice") or op.get("wholeSalePrice") or "?"

        # 이미지 URL 추출 — detailImages는 dict 또는 str 혼용
        img = pick.get("representativeImageUrl") or ""
        if not img:
            di = op.get("detailImages") or []
            for d in di:
                url = d.get("url") if isinstance(d, dict) else (d if isinstance(d, str) else "")
                if url:
                    img = url
                    break
        if not img:
            img = op.get("images", {}).get("representativeImage", {}).get("url") or ""

        store_url = f"https://smartstore.naver.com/khww/products/{pid}" if pid else ""
        tag_names = [t.get("text", "") for t in (op.get("productTags") or [])[:5] if isinstance(t, dict) and t.get("text")]
        detail = (op.get("detailContent") or "")[:300]
        return JSONResponse({
            "name": name,
            "price": price,
            "product_no": str(pid),
            "image_url": img,
            "store_url": store_url,
            "tags": tag_names,
            "detail_preview": detail,
            "total_pool": len(filtered),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)[:300]}, status_code=500)


@app.get("/check-env")
def check_env():
    """API 키 설정 여부 확인"""
    return {
        "ANTHROPIC_API_KEY":  bool(os.environ.get("ANTHROPIC_API_KEY")),
        "OPENAI_API_KEY":     bool(os.environ.get("OPENAI_API_KEY")),
        "PEXELS_API_KEY":     bool(os.environ.get("PEXELS_API_KEY")),
        "GOOGLE_AI_API_KEY":  bool(os.environ.get("GOOGLE_AI_API_KEY")),
        "FLUX_API_KEY":       bool(os.environ.get("FLUX_API_KEY")),
        "NAVER_CLIENT_ID":    bool(os.environ.get("NAVER_CLIENT_ID")),
        "GOOGLE_API_KEY":     bool(os.environ.get("GOOGLE_API_KEY")),
    }


@app.get("/test-image-gen")
async def test_image_gen():
    """Gemini / Flux 직접 API 호출 진단 — 에러 본문 노출"""
    from main import GOOGLE_AI_API_KEY, FLUX_API_KEY, OPENAI_API_KEY, _get_en_name
    import httpx as _httpx, base64 as _b64
    results = {}

    # Gemini — gemini-2.0-flash-exp 로 테스트
    if GOOGLE_AI_API_KEY:
        try:
            async with _httpx.AsyncClient(timeout=30) as c:
                r = await c.post(
                    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent",
                    params={"key": GOOGLE_AI_API_KEY},
                    json={
                        "contents": [{"parts": [{"text": "Professional product photo of a massage cushion"}]}],
                        "generationConfig": {"responseModalities": ["TEXT", "IMAGE"]},
                    },
                )
            body = r.json()
            if r.status_code == 200:
                parts = body.get("candidates", [{}])[0].get("content", {}).get("parts", [])
                has_img = any("inlineData" in p for p in parts)
                results["gemini"] = f"ok(image={has_img})"
            else:
                results["gemini"] = f"http{r.status_code}:{str(body)[:300]}"
        except Exception as e:
            results["gemini"] = f"exception:{str(e)[:300]}"
    else:
        results["gemini"] = "no_key"

    # Flux — X-Key 헤더로 api.bfl.ai 테스트
    if FLUX_API_KEY:
        try:
            async with _httpx.AsyncClient(timeout=20) as c:
                r = await c.post(
                    "https://api.bfl.ai/v1/flux-pro-1.1",
                    headers={"X-Key": FLUX_API_KEY, "Content-Type": "application/json"},
                    json={"prompt": "product photo massage cushion", "width": 1024, "height": 1024},
                )
            results["flux_submit"] = f"http{r.status_code}:{str(r.json())[:300]}"
        except Exception as e:
            results["flux_submit"] = f"exception:{type(e).__name__}:{str(e)[:300]}"
    else:
        results["flux_submit"] = "no_key"

    results["dalle_key"] = "set" if OPENAI_API_KEY else "missing"
    return JSONResponse(results)


@app.get("/test-dalle")
async def test_dalle():
    """DALL-E 단독 동작 확인 — 실제 API 에러 노출"""
    from main import OPENAI_API_KEY
    import httpx as _httpx
    if not OPENAI_API_KEY:
        return JSONResponse({"status": "skip", "reason": "OPENAI_API_KEY 없음"})
    prompt = "Professional product photo of a massage cushion on white background, studio lighting."
    try:
        async with _httpx.AsyncClient(timeout=60) as c:
            r = await c.post(
                "https://api.openai.com/v1/images/generations",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "dall-e-3", "prompt": prompt, "n": 1, "size": "1024x1024", "quality": "standard"},
            )
            return JSONResponse({"status_code": r.status_code, "body": r.json()})
    except Exception as e:
        return JSONResponse({"status": "exception", "message": str(e)})


@app.get("/debug-naver")
async def debug_naver():
    """네이버 API 인증 진단"""
    import bcrypt as _bcrypt
    import base64 as _b64
    import time as _time

    client_id = os.environ.get("NAVER_CLIENT_ID", "")
    client_secret = os.environ.get("NAVER_CLIENT_SECRET", "")

    result = {
        "client_id_set": bool(client_id),
        "client_id_length": len(client_id),
        "client_id_prefix": client_id[:4] if client_id else "",
        "secret_set": bool(client_secret),
        "secret_length": len(client_secret),
        "secret_prefix": client_secret[:7] if client_secret else "",
    }

    # bcrypt 서명 시도
    try:
        timestamp = str(int(_time.time() * 1000))
        password = f"{client_id}_{timestamp}"
        hashed = _bcrypt.hashpw(password.encode("utf-8"), client_secret.encode("utf-8"))
        sig = _b64.b64encode(hashed).decode("utf-8")
        result["bcrypt_sign"] = "성공"
        result["sig_length"] = len(sig)
    except Exception as e:
        result["bcrypt_sign"] = f"실패: {e}"

    # 토큰 요청 시도 후 상세 응답
    try:
        timestamp = str(int(_time.time() * 1000))
        password = f"{client_id}_{timestamp}"
        hashed = _bcrypt.hashpw(password.encode("utf-8"), client_secret.encode("utf-8"))
        sig = _b64.b64encode(hashed).decode("utf-8")
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(
                "https://api.commerce.naver.com/external/v1/oauth2/token",
                data={
                    "client_id": client_id,
                    "timestamp": timestamp,
                    "client_secret_sign": sig,
                    "grant_type": "client_credentials",
                    "type": "SELF",
                }
            )
            result["token_status"] = r.status_code
            result["token_response"] = r.text[:300]
    except Exception as e:
        result["token_error"] = str(e)

    return JSONResponse(result)


@app.get("/find-category")
async def find_category(keyword: str = "소프트웨어"):
    """Naver Commerce 카테고리 검색 (leaf ID 확인용)"""
    token = await naver_api.get_token()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(
            "https://api.commerce.naver.com/external/v1/categories",
            headers={"Authorization": f"Bearer {token}"},
            params={"keyword": keyword},
        )
    return JSONResponse({"status": r.status_code, "body": r.json() if r.is_success else r.text[:500]})


@app.get("/origin-areas")
async def get_origin_areas(category_id: int = 50001514):
    """카테고리별 원산지 코드 목록 조회"""
    token = await naver_api.get_token()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(
            f"https://api.commerce.naver.com/external/v1/product-sale-types/{category_id}/origin-areas",
            headers={"Authorization": f"Bearer {token}"},
        )
    return JSONResponse({"status": r.status_code, "body": r.json() if r.is_success else r.text[:800]})



@app.post("/register-products-debug")
async def register_products_debug():
    """상품 등록 1개 동기 실행 — 에러 즉시 반환"""
    from main import parse_excel, generate_product_copy, calculate_selling_price, get_product_image, build_product_payload, save_registered_code, load_registered_codes
    from employees import employee_sourcing_manager, employee_ip_guardian, employee_season_planner, employee_trend_scout, employee_review_analyst

    files = sorted(Path(EXCEL_FOLDER).glob("*.xlsx"), key=lambda x: x.stat().st_mtime, reverse=True)
    if not files:
        return JSONResponse({"step": "excel", "error": "Excel 파일 없음"})

    try:
        products = parse_excel(str(files[0]))
        return_data = {"step": "parse", "total_products": len(products)}
    except Exception as e:
        return JSONResponse({"step": "parse", "error": str(e)})

    if not products:
        # 헤더 확인용
        import openpyxl
        wb = openpyxl.load_workbook(str(files[0]), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
        row0 = [str(v) for v in (rows[0] if rows else [])]
        row1 = [str(v) for v in (rows[1] if len(rows) > 1 else [])]
        row2 = [str(v) for v in (rows[2] if len(rows) > 2 else [])]
        # col_idx 직접 계산해서 진단
        from main import COLUMN_MAP, _match_col
        headers_dbg = [str(v).strip() if v else "" for v in (rows[1] if len(rows) > 1 else [])]
        col_idx_dbg = {}
        for i, h in enumerate(headers_dbg):
            m = _match_col(h)
            if m:
                col_idx_dbg[i] = {"header": h, "mapped": m}
        return JSONResponse({"step": "parse", "error": "파싱된 상품 없음", "row0": row0[:25], "row1": row1[:25], "row2": row2[:25], "col_idx": col_idx_dbg, "total_rows": len(rows)})

    p = products[0]
    return_data["sample_product"] = {"name": p.get("name"), "price": p.get("price"), "image": str(p.get("image", ""))[:50]}

    try:
        safe, kw = employee_ip_guardian(p)
        return_data["ip_check"] = "통과" if safe else f"차단: {kw}"
        if not safe:
            return JSONResponse(return_data)
    except Exception as e:
        return JSONResponse({"step": "ip_guardian", "error": str(e), **return_data})

    try:
        review = await employee_review_analyst(str(p.get("name", "")), ANTHROPIC_API_KEY)
        return_data["review"] = "완료"
    except Exception as e:
        return JSONResponse({"step": "review_analyst", "error": str(e), **return_data})

    try:
        ai = await generate_product_copy(p, {"season": "", "trends": [], "pain_points": [], "selling_points": []})
        return_data["product_copy"] = ai.get("product_name", "")[:30]
    except Exception as e:
        return JSONResponse({"step": "generate_copy", "error": str(e), **return_data})

    try:
        # DALL-E 직접 테스트
        from main import generate_dalle_image, _dalle_request, OPENAI_API_KEY
        return_data["openai_key_set"] = bool(OPENAI_API_KEY)
        # OpenAI API 직접 호출 테스트
        import httpx as _httpx
        try:
            async with _httpx.AsyncClient(timeout=60) as _c:
                _r = await _c.post(
                    "https://api.openai.com/v1/images/generations",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                    json={"model": "dall-e-3", "prompt": "white background product photo", "n": 1, "size": "1024x1024", "quality": "standard"}
                )
                return_data["dalle_status"] = _r.status_code
                return_data["dalle_url"] = str(_r.json()).replace('"', '')[:120]
        except Exception as de:
            return_data["dalle_url"] = f"에러: {str(de)[:100]}"

        img_url = await get_product_image(p)
        return_data["image"] = img_url[:60] if img_url else "이미지 없음"
        if not img_url:
            return JSONResponse({"step": "image", "error": "이미지 없음 → 등록 제외", **return_data})
    except Exception as e:
        return JSONResponse({"step": "get_image", "error": str(e), **return_data})

    try:
        from main import build_detail_html, create_banner_image
        price = calculate_selling_price(p["price"])
        payload = build_product_payload(p, ai, price)
        payload["originProduct"]["images"]["representativeImage"]["url"] = img_url

        # 상세페이지 HTML 생성 (디버그도 동일하게 적용)
        banner_url = await create_banner_image(
            img_url,
            ai.get("headline") or ai.get("banner_text") or str(p.get("name",""))[:18],
            ai.get("sub_headline") or ai.get("sub_text", "")
        )
        detail_html = build_detail_html(banner_url, img_url, ai)
        payload["originProduct"]["detailContent"] = detail_html
        return_data["detail_length"] = len(detail_html)

        result = await naver_api.register_product(payload)
        save_registered_code(str(p.get("code", "")))
        return_data["naver_result"] = str(result)[:100]
        return_data["step"] = "완료"
    except Exception as e:
        return JSONResponse({"step": "naver_register", "error": str(e), **return_data})

    return JSONResponse(return_data)


@app.get("/store-status")
async def store_status():
    """스토어 현황 — 등록 상품 수 + 시즌 + 드라이브 진행률"""
    from main import load_registered_codes
    codes = load_registered_codes()
    file_ids = _load_drive_index()
    try:
        with open(EXCEL_PROGRESS, encoding="utf-8") as f:
            progress = json.load(f)
    except Exception:
        progress = {"current_index": 0}
    idx = progress.get("current_index", 0)
    from employees import employee_season_planner
    season = employee_season_planner()
    return JSONResponse({
        "등록된_상품수": len(codes),
        "처리한_Excel파일": idx,
        "전체_Excel파일": len(file_ids),
        "남은_Excel파일": len(file_ids) - idx,
        "다가오는_시즌": [e["event"] + " D-" + str(e["days_left"]) for e in season["upcoming"][:3]],
    })


@app.get("/myip")
async def myip():
    import httpx
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get("https://api.ipify.org?format=json")
        return r.json()


# ─── POD 상품 등록 ───────────────────────────────────────────────────────────
@app.post("/register-pod")
async def register_pod_product(request: Request):
    """Printful POD 상품 스마트스토어 자동 등록 (shopify-trendify에서 호출)."""
    data = await request.json()
    name: str = str(data.get("name", "")).strip()
    image_url: str = data.get("image_url", "")
    price_krw: int = round(int(data.get("price_krw", 0)) / 10) * 10  # Naver API 10원 단위
    theme: str = data.get("theme", "")

    if not name:
        return JSONResponse({"status": "error", "error": "name 필수"}, status_code=400)
    if price_krw <= 0:
        return JSONResponse({"status": "error", "error": "price_krw > 0 필수"}, status_code=400)
    if not image_url or not image_url.startswith("http"):
        return JSONResponse({"status": "error", "error": "image_url(http) 필수"}, status_code=400)

    try:
        # 이미지 네이버 CDN 업로드
        naver_image = await naver_api.upload_image(image_url)

        # 상품명 생성 (최대 25자)
        kname = name[:25]
        description_html = (
            f"<p><strong>{kname}</strong></p>"
            f"<p>AI가 선별한 트렌딩 디자인 티셔츠입니다.</p>"
            f"<p>주제: {theme[:80]}</p>"
            f"<p>Bella+Canvas 3001 소재 | S/M/L/XL 사이즈 | Print On Demand 제작</p>"
            f"<p>주문 제작 상품으로 교환/반품이 제한될 수 있습니다.</p>"
        )

        raw = {
            "image": naver_image,
            "name": kname,
            "delivery_type": "유료",
            "delivery_fee": 3000,
            "origin": "미국",
            "stock": 999,
            "manufacturer": "Printful Inc.",
            "brand": "Trendify",
        }
        ai = {
            "product_name": kname,
            "description": description_html,
            "emotional_copy": description_html,
        }

        payload = build_product_payload(raw, ai, price_krw,
                                        tags=["POD", "티셔츠", "AI디자인", "트렌딩"])
        # T-Shirt 카테고리 강제 지정
        payload["originProduct"]["leafCategoryId"] = 50000830

        result = await naver_api.register_product(payload)
        product_id = result.get("id") or result.get("originProductNo") or result.get("smartstoreChannelProductNo", "")
        channel_no = str(result.get("smartstoreChannelProductNo") or result.get("channelProductNo") or product_id or "")
        save_registered_code(str(product_id))

        # Pinterest 핀 생성 (백그라운드)
        product_url = build_smartstore_url(channel_no)
        asyncio.create_task(create_pinterest_pin(kname, image_url, product_url))

        return JSONResponse({"status": "ok", "product_id": str(product_id), "name": kname})

    except Exception as e:
        import traceback
        return JSONResponse(
            {"status": "error", "error": str(e), "trace": traceback.format_exc()[-300:]},
            status_code=500,
        )


@app.post("/register-simple")
async def register_simple(request: Request):
    """특정 상품 단건 간편 등록 (register_product 직호출).
    AI 파이프라인(카피/HTML/태그/가격 생성) 없이 입력값 그대로 등록.
    SEO·상세페이지는 추후 /reapply-html 로 보완.
    body: {name, price_krw, image_url(cdn1), category, description?, tags?,
           origin?, manufacturer?, brand?, delivery_fee?, stock?}
    """
    data = await request.json()
    name: str = str(data.get("name", "")).strip()
    image_url: str = str(data.get("image_url", "")).strip()
    try:
        price_krw: int = round(int(data.get("price_krw", 0)) / 10) * 10  # Naver 10원 단위
    except (ValueError, TypeError):
        price_krw = 0

    if not name:
        return JSONResponse({"status": "error", "error": "name 필수"}, status_code=400)
    if price_krw <= 0:
        return JSONResponse({"status": "error", "error": "price_krw > 0 필수"}, status_code=400)
    if not image_url.startswith("http"):
        return JSONResponse({"status": "error", "error": "image_url(http) 필수"}, status_code=400)

    try:
        # 1) cdn1 이미지 → 네이버 CDN 업로드
        naver_image = await naver_api.upload_image(image_url)

        # 2) raw/ai 구성 (build_product_payload 가 get_category_id 로 카테고리 자동 해석)
        desc_html = data.get("description") or (
            f"<div style=\"font-family:sans-serif;max-width:900px;margin:0 auto;line-height:1.7\">"
            f"<h2>{name[:60]}</h2>"
            f"<p>상세 설명/상품 이미지는 순차적으로 업데이트됩니다.</p>"
            f"<p>문의는 스토어 문의하기를 이용해 주세요.</p></div>"
        )
        raw = {
            "image": naver_image,
            "name": name,
            "category": str(data.get("category", "")),
            "delivery_type": "유료",
            "delivery_fee": int(data.get("delivery_fee", 3000)),
            "origin": str(data.get("origin", "중국")),
            "stock": int(data.get("stock", 100)),
            "manufacturer": str(data.get("manufacturer", "상세페이지 참조")),
            "brand": str(data.get("brand", "")),
        }
        ai = {"product_name": name, "description": desc_html, "emotional_copy": desc_html}
        tags = data.get("tags") or []

        payload = build_product_payload(raw, ai, price_krw, tags=tags)

        # leaf_category_id 명시 지정 시 자동분류(get_category_id) 대신 그 값 사용
        _leaf = data.get("leaf_category_id")
        if _leaf:
            try:
                payload["originProduct"]["leafCategoryId"] = int(_leaf)
            except (ValueError, TypeError):
                pass

        # 가격표시제(단위가격) 대상 카테고리(타월 등) 필수 필드 — 미표시(false)로 충족
        # (Naver unitPriceYn 는 Boolean: true/false)
        payload["originProduct"].setdefault("detailAttribute", {})["unitCapacity"] = {"unitPriceYn": False}

        # 3) 등록
        result = await naver_api.register_product(payload)
        product_id = result.get("originProductNo") or result.get("id") or result.get("smartstoreChannelProductNo", "")
        channel_no = str(result.get("smartstoreChannelProductNo") or result.get("channelProductNo") or product_id or "")
        save_registered_code(str(product_id))

        leaf = payload.get("originProduct", {}).get("leafCategoryId")
        return JSONResponse({
            "status": "ok",
            "product_id": str(product_id),
            "channel_no": channel_no,
            "leaf_category_id": leaf,
            "url": build_smartstore_url(channel_no),
            "name": payload.get("originProduct", {}).get("name", name),
            "price": price_krw,
        })

    except Exception as e:
        import traceback
        return JSONResponse(
            {"status": "error", "error": str(e), "trace": traceback.format_exc()[-400:]},
            status_code=500,
        )


@app.post("/update-category")
async def update_category(request: Request):
    """등록 상품 카테고리(leafCategoryId) 변경.
    네이버는 부분 PUT을 거부(statusType 등 필수)하므로 전체 payload GET → leafCategoryId 교체 → 전체 PUT.
    body: {product_no, leaf_category_id}."""
    import httpx as _hx
    from main import NAVER_BASE
    data = await request.json()
    no = str(data.get("product_no", "")).strip()
    try:
        leaf = int(data.get("leaf_category_id", 0))
    except (ValueError, TypeError):
        leaf = 0
    if not no or not leaf:
        return JSONResponse({"status": "error", "error": "product_no, leaf_category_id 필수"}, status_code=400)
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=30) as c:
            rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers)
        if rd.status_code != 200:
            return JSONResponse({"status": "error", "error": f"상품 조회 실패 {rd.status_code}: {rd.text[:300]}"}, status_code=400)
        origin = rd.json().get("originProduct", {})
        old_leaf = origin.get("leafCategoryId")
        payload = {k: v for k, v in origin.items() if k not in _READONLY_KEYS}
        payload["leafCategoryId"] = leaf
        payload["statusType"] = "SALE"
        ok, msg = await naver_api.update_product(no, payload)
        return JSONResponse({"status": "ok" if ok else "error", "product_no": no,
                             "old_leaf": old_leaf, "new_leaf": leaf, "error": msg})
    except Exception as e:
        import traceback
        return JSONResponse({"status": "error", "error": str(e), "trace": traceback.format_exc()[-300:]}, status_code=500)


@app.post("/update-name")
async def update_product_name(request: Request):
    """상품명(name) 수정. body: {product_no, name}"""
    import httpx as _hx
    from main import NAVER_BASE
    data = await request.json()
    no = str(data.get("product_no", "")).strip()
    new_name = str(data.get("name", "")).strip()
    if not no or not new_name:
        return JSONResponse({"status": "error", "error": "product_no, name 필수"}, status_code=400)
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=30) as c:
            rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers)
        if rd.status_code != 200:
            return JSONResponse({"status": "error", "error": f"상품 조회 실패 {rd.status_code}: {rd.text[:300]}"}, status_code=400)
        origin = rd.json().get("originProduct", {})
        old_name = origin.get("name", "")
        payload = {k: v for k, v in origin.items() if k not in _READONLY_KEYS}
        payload["name"] = new_name
        payload["statusType"] = "SALE"
        ok, msg = await naver_api.update_product(no, payload)
        return JSONResponse({"status": "ok" if ok else "error", "product_no": no,
                             "old_name": old_name, "new_name": new_name, "error": msg})
    except Exception as e:
        import traceback
        return JSONResponse({"status": "error", "error": str(e), "trace": traceback.format_exc()[-300:]}, status_code=500)


@app.get("/debug-update-test")
async def debug_update_test(no: str, settest: int = 1):
    """reapply 스타일 merge-PUT 재현 — 전체 Naver 에러(invalidInputs) 반환(필드 진단용).
    settest=1(기본): detailContent 를 새 테스트 Vision HTML 로 바꿔 PUT (배치와 동일 조건으로 400 재현).
    settest=0: 기존 payload 그대로 re-PUT. GET 429 시 1회 백오프."""
    import httpx as _hx, asyncio as _aio
    from main import NAVER_BASE
    _TEST_HTML = ('<div style="font-family:\'Noto Sans KR\',sans-serif;max-width:900px;margin:0 auto">'
                  '<h1>상품 상세</h1><p>테스트 Vision 19섹션 재적용 진단용 HTML 본문입니다. Noto Sans KR.</p></div>')
    try:
        headers = await naver_api._headers()
        origin = {}
        async with _hx.AsyncClient(timeout=30) as c:
            for _att in range(2):
                rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers)
                if rd.status_code == 200:
                    origin = rd.json().get("originProduct", {})
                    break
                if "429" in rd.text or rd.status_code == 429:
                    await _aio.sleep(15); continue
                return JSONResponse({"step": "get", "http": rd.status_code, "body": rd.text[:500]}, status_code=400)
            if not origin:
                return JSONResponse({"step": "get", "error": "429 재시도 실패"}, status_code=429)
            payload = {k: v for k, v in origin.items() if k not in _READONLY_KEYS}
            orig_detail = origin.get("detailContent") or ""
            if settest:
                payload["detailContent"] = _TEST_HTML
            rp = await c.put(f"{NAVER_BASE}/v2/products/origin-products/{no}",
                             headers=headers, json={"originProduct": payload})
            restored = False
            # 비파괴: 테스트 PUT이 성공해 junk로 덮인 경우 원본 detailContent 복원
            if settest and rp.status_code == 200 and orig_detail:
                payload["detailContent"] = orig_detail
                rr = await c.put(f"{NAVER_BASE}/v2/products/origin-products/{no}",
                                 headers=headers, json={"originProduct": payload})
                restored = (rr.status_code == 200)
        return JSONResponse({
            "no": no,
            "settest": settest,
            "name": origin.get("name", "")[:40],
            "leafCategoryId": origin.get("leafCategoryId"),
            "had_noto": "Noto Sans KR" in orig_detail,
            "put_http": rp.status_code,
            "restored": restored,
            "put_body": rp.text[:1500],
        })
    except Exception as e:
        import traceback
        return JSONResponse({"status": "error", "error": str(e), "trace": traceback.format_exc()[-300:]}, status_code=500)


@app.get("/debug-gen-html")
async def debug_gen_html(no: str):
    """상품 1개에 대해 reapply와 동일하게 HTML 생성만 하고 PUT 없이 구조 분석 반환.
    NotBlank(빈값 판정) 원인 진단용 — <style>/<script>/text길이/wrapper 확인."""
    import httpx as _hx
    import re as _re
    from main import NAVER_BASE, generate_product_copy, generate_claude_html_detail, _html_style_for
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=30) as c:
            rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers)
        if rd.status_code != 200:
            return JSONResponse({"step": "get", "http": rd.status_code, "body": rd.text[:300]}, status_code=400)
        origin = rd.json().get("originProduct", {})
        name = (origin.get("name") or "").strip()
        price = int(origin.get("salePrice", 0))
        cat = ((origin.get("detailAttribute") or {}).get("naverShoppingSearchInfo", {}).get("categoryName", ""))
        imgs = []
        ri = (origin.get("images") or {}).get("representativeImage") or {}
        if isinstance(ri, dict) and ri.get("url"):
            imgs.append(ri["url"])
        pdict = {"name": name, "category": cat, "price": price}
        _style = _html_style_for(str(cat), str(name))
        ai = await generate_product_copy(pdict, {})
        html = await generate_claude_html_detail(pdict, ai, imgs)
        # sanitize 시뮬: style/script 제거 후 텍스트
        no_style = _re.sub(r"<style[\s\S]*?</style>", "", html or "")
        no_style = _re.sub(r"<script[\s\S]*?</script>", "", no_style)
        text_only = _re.sub(r"<[^>]+>", "", no_style)
        text_only = _re.sub(r"\s+", " ", text_only).strip()
        _h = html or ""
        return JSONResponse({
            "no": no, "name": name[:40],
            "category": cat, "detected_style": _style,
            "html_len": len(_h),
            "has_noto": "Noto Sans KR" in _h,
            "has_style_block": "<style" in _h,
            "has_script": "<script" in _h,
            "has_doctype_or_html": bool(_re.search(r"<!doctype|<html", _h, _re.I)),
            "starts_with": _h[:80],
            "text_len_after_strip_style": len(text_only),
            # 모바일 반응형 마커
            "has_table": bool(_re.search(r"<table|<td\b|<tr\b", _h, _re.I)),
            "has_max_width": "max-width" in _h,
            "width_100_count": len(_re.findall(r"width:\s*100%", _h, _re.I)),
            "has_flex": "flex" in _h,
            # standalone width:NNpx (max-width/min-width 제외) — 후처리 후 0이어야 정상
            "fixed_px_width_count": len(_re.findall(r"(?<![-a-zA-Z])width\s*:\s*\d+px", _h, _re.I)),
            "max_width_px_count": len(_re.findall(r"max-width\s*:\s*\d+px", _h, _re.I)),
            "fixed_px_height_count": len(_re.findall(r"(?<![-a-zA-Z])height\s*:\s*\d+px", _h, _re.I)),
            "img_height_auto": bool(_re.search(r"height:\s*auto", _h, _re.I)),
            "text_preview": text_only[:200],
        })
    except Exception as e:
        import traceback
        return JSONResponse({"status": "error", "error": str(e), "trace": traceback.format_exc()[-400:]}, status_code=500)


@app.get("/get-product-html")
async def get_product_html(no: str):
    """저장된 상품 HTML(detailContent) 조회 — 재적용 전후 품질 비교용."""
    import httpx as _hx, re as _re
    from main import NAVER_BASE
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers)
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:300]}, status_code=400)
        origin = r.json().get("originProduct", {})
        html = origin.get("detailContent") or ""
        no_style = _re.sub(r"<style[\s\S]*?</style>", "", html, flags=_re.I)
        no_style = _re.sub(r"<script[\s\S]*?</script>", "", no_style, flags=_re.I)
        text = _re.sub(r"<[^>]+>", "", no_style)
        text = _re.sub(r"\s+", " ", text).strip()
        section_kws = ["배너","히어로","후킹","수치","문제","해결","갤러리","상세","사용법","비교","후기","faq","스펙","배송","신뢰","cta","푸터"]
        section_hits = [kw for kw in section_kws if kw in html.lower()]
        img_count = len(_re.findall(r"<img\b", html, _re.I))
        h2_count = len(_re.findall(r"<h[23]\b", html, _re.I))
        section_count = len(_re.findall(r"<section\b", html, _re.I))
        return JSONResponse({
            "no": no,
            "name": (origin.get("name") or "")[:50],
            "html_len": len(html),
            "has_noto": "Noto Sans KR" in html,
            "section_tag_count": section_count,
            "h2_h3_count": h2_count,
            "img_count": img_count,
            "section_kw_hits": section_hits,
            "section_kw_count": len(section_hits),
            "text_len": len(text),
            "text_preview": text[:300],
            "html_head_200": html[:200],
            "html_tail_200": html[-200:],
        })
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()[-300:]}, status_code=500)


@app.get("/product-info")
async def get_product_info(no: str):
    """상품 기본정보(도매가/판매가/DG코드) 조회 — Naver API IP 제한 우회용."""
    import httpx as _hx, re as _re
    from main import NAVER_BASE
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers)
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:300]}, status_code=400)
        origin = r.json().get("originProduct", {})
        dg_raw = str((origin.get("detailAttribute") or {}).get("sellerCodeInfo") or {}).strip()
        # sellerCodeInfo is a dict; extract sellerManagementCode
        seller_code_info = (origin.get("detailAttribute") or {}).get("sellerCodeInfo") or {}
        mgmt_code = str(seller_code_info.get("sellerManagementCode") or "").strip()
        sale_price = int(origin.get("salePrice") or 0)

        # DG 코드 있으면 실시간 도매가 조회 (costPrice 필드는 Naver read-only라 무용)
        from main import _get_dg_wholesale
        wholesale = await _get_dg_wholesale(mgmt_code) if mgmt_code.startswith("DG_") else 0
        floor_price = int(wholesale * 1.15) if wholesale > 0 else 0
        margin = sale_price - floor_price if floor_price > 0 else None
        rep_img = ((origin.get("images") or {}).get("representativeImage") or {}).get("url", "")
        return JSONResponse({
            "no": no,
            "name": (origin.get("name") or "")[:80],
            "statusType": origin.get("statusType", ""),
            "salePrice": sale_price,
            "wholesale": wholesale,
            "wholesale_source": "dg_api" if wholesale > 0 else "none",
            "floor_price": floor_price,
            "margin": margin,
            "margin_ok": margin > 0 if margin is not None else None,
            "sellerManagementCode": mgmt_code,
            "representativeImage": rep_img,
        })
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()[-300:]}, status_code=500)


@app.get("/product-raw")
async def get_product_raw(no: str):
    """originProduct 원문 반환 — attributeInfo/leafCategoryId 등 전체 필드 진단용."""
    import httpx as _hx
    from main import NAVER_BASE
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers)
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:500]}, status_code=400)
        origin = r.json().get("originProduct", {})
        return JSONResponse({
            "leafCategoryId": origin.get("leafCategoryId"),
            "salePrice": origin.get("salePrice"),
            "name": (origin.get("name") or "")[:60],
            "statusType": origin.get("statusType"),
            "attributeInfo": origin.get("attributeInfo"),
            "detailAttribute_keys": list((origin.get("detailAttribute") or {}).keys()),
            "sellerManagementCode": ((origin.get("detailAttribute") or {}).get("sellerCodeInfo") or {}).get("sellerManagementCode", ""),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/product-raw-keys")
async def get_product_raw_keys(no: str):
    """originProduct 최상위 키 전체 + detailAttribute 키 + attributeInfo 원문 반환 (디버그용)."""
    import httpx as _hx
    from main import NAVER_BASE
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers)
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:500]}, status_code=400)
        origin = r.json().get("originProduct", {})
        da = origin.get("detailAttribute") or {}
        return JSONResponse({
            "origin_keys": list(origin.keys()),
            "detailAttribute_keys": list(da.keys()),
            "attributeInfo_top": origin.get("attributeInfo"),
            "attributeInfo_in_da": da.get("attributeInfo"),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/naver-category-attrs")
async def naver_category_attrs(category_id: str):
    """Naver 카테고리 속성 목록 조회 — 필수/선택 구분 포함."""
    import httpx as _hx
    from main import NAVER_BASE
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{NAVER_BASE}/v1/product-attributes/attributes", headers=headers, params={"categoryId": category_id})
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:500]}, status_code=400)
        return JSONResponse(r.json())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/naver-attr-values")
async def naver_attr_values(attribute_seq: int):
    """Naver 속성값 목록 조회 — attributeSeq별 선택 가능한 값 목록."""
    import httpx as _hx
    from main import NAVER_BASE
    try:
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(
                f"{NAVER_BASE}/v1/product-attributes/attribute-values",
                headers=headers,
                params={"attributeSeq": attribute_seq, "categoryId": 50000830}
            )
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:500]}, status_code=400)
        return JSONResponse(r.json())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/ip-scan")
async def ip_scan_all():
    """SALE 상품 전체를 DANGEROUS_KEYWORDS로 스캔 — IP 위반 상품 목록 반환."""
    from employees import DANGEROUS_KEYWORDS
    hits = []
    scanned = 0
    page = 1
    try:
        while True:
            data = await naver_api.list_products(page=page, size=100)
            items = data.get("contents", [])
            if not items:
                break
            for item in items:
                op = item.get("originProduct", {})
                name = op.get("name", "") or item.get("name", "")
                status = op.get("statusType", "")
                if status != "SALE":
                    continue
                pid = str(item.get("originProductNo", ""))
                name_lower = name.lower()
                for kw in DANGEROUS_KEYWORDS:
                    if kw.lower() in name_lower:
                        hits.append({"pid": pid, "name": name, "keyword": kw})
                        break
            scanned += len(items)
            if len(items) < 100:
                break
            page += 1
            await asyncio.sleep(0.5)
        return JSONResponse({"scanned": scanned, "hit_count": len(hits), "hits": hits})
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()[-500:]}, status_code=500)


# ─── Pinterest ───────────────────────────────────────────────────────────────
@app.get("/pinterest/boards")
async def pinterest_boards_list():
    """Pinterest 보드 목록 조회 (PINTEREST_BOARD_ID 설정 확인용)."""
    boards = await get_pinterest_boards()
    return JSONResponse({
        "boards": [{"id": b["id"], "name": b["name"]} for b in boards],
        "count": len(boards),
        "hint": "PINTEREST_BOARD_ID 환경변수에 사용할 id 값을 설정하세요.",
    })


@app.post("/pinterest/pin-recent")
async def pinterest_pin_recent(days: int = 1, max_pins: int = 10):
    """최근 등록된 스마트스토어 상품을 Pinterest에 핀 생성 (수동 트리거)."""
    result = await pin_recent_smartstore_products(days=days, max_pins=max_pins)
    return JSONResponse(result)


# ─── 상품 등록 ────────────────────────────────────────────────────────────────
@app.post("/register-products")
async def register_products(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        body = {}
    excel_path = body.get("excel_path")
    if not excel_path:
        files = sorted(Path(EXCEL_FOLDER).glob("*.xlsx"), key=lambda x: x.stat().st_mtime, reverse=True)
        if not files:
            return JSONResponse({"status": "error", "message": "업로드된 Excel 파일 없음"}, status_code=400)
        excel_path = str(files[0])
    limit = int(body.get("limit", 33))
    background_tasks.add_task(pipeline_register_products, excel_path, limit)
    return JSONResponse({"status": "processing", "excel": excel_path, "limit": limit})


@app.post("/fix-products")
async def fix_products(request: Request, background_tasks: BackgroundTasks):
    """등록된 상품 이미지·설명 일괄 수정 (백그라운드).
    Body(선택): {"limit": 50, "fix_images": true, "fix_descriptions": true}"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    limit            = int(body.get("limit", 50))
    fix_images       = bool(body.get("fix_images", True))
    fix_descriptions = bool(body.get("fix_descriptions", True))
    background_tasks.add_task(pipeline_fix_products, limit, fix_images, fix_descriptions)
    return JSONResponse({
        "status": "processing",
        "limit": limit,
        "fix_images": fix_images,
        "fix_descriptions": fix_descriptions,
        "note": "배치 처리 중 — 완료까지 수분 소요 (Naver API 속도제한 준수)",
    })


@app.post("/fix-products/sync")
async def fix_products_sync(request: Request):
    """상품 수정 동기 실행 (소량 테스트용). Body: {"limit": 5}"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    limit = int(body.get("limit", 5))
    result = await pipeline_fix_products(limit=limit)
    return JSONResponse(result)


@app.get("/find-duplicate-products")
async def find_duplicate_products_naver():
    """스마트스토어 등록 상품 중 sellerManagementCode 기준 중복 목록 반환 (삭제 안 함)."""
    from main import _normalize_name as _nn
    all_prods = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        for p in contents:
            product_no = str(p.get("originProductNo", ""))
            origin = p.get("originProduct", {})
            seller_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "") or f"NAVER_ID_{product_no}"
            name = (origin.get("name") or "").strip()
            all_prods.append({"product_no": product_no, "code": seller_code, "name": name})
        if len(contents) < 50:
            break
        page += 1
        import asyncio as _ai; await _ai.sleep(0.3)
    code_map: dict[str, list] = {}
    name_map: dict[str, list] = {}
    for p in all_prods:
        code_map.setdefault(p["code"], []).append(p)
        nk = _nn(p["name"])
        if nk:
            name_map.setdefault(nk, []).append(p)
    dup_codes = {k: v for k, v in code_map.items() if len(v) > 1}
    dup_names = {k: v for k, v in name_map.items() if len(v) > 1}
    return {
        "total": len(all_prods),
        "duplicate_code_groups": len(dup_codes),
        "duplicate_name_groups": len(dup_names),
        "duplicate_codes": [{"code": k, "count": len(v), "products": v} for k, v in dup_codes.items()],
        "duplicate_names": [{"name": k, "count": len(v), "products": v} for k, v in dup_names.items()],
    }


@app.get("/product-detail/{origin_no}")
async def product_detail_one(origin_no: str):
    """단일 상품 상세 — channel_no + dg_code + price + 상세HTML에서 cdn1.domeggook 이미지 추출."""
    import httpx as _hx, re as _re
    from main import NAVER_BASE
    try:
        async with _hx.AsyncClient(timeout=25) as c:
            r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                            headers=await naver_api._headers())
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:200]}, status_code=502)
        d = r.json()
        origin = d.get("originProduct", {}) or {}
        chans = d.get("channelProducts", []) or []
        channel_no = str(chans[0].get("channelProductNo", "")) if chans else ""
        dg_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "") or ""
        rep = ((origin.get("images") or {}).get("representativeImage") or {}).get("url", "")
        detail = origin.get("detailContent", "") or ""
        cdn = list(dict.fromkeys(_re.findall(r'https?://cdn[0-9]?\.domeggook\.com/[^\s"\'<>\\)]+', detail)))
        return {"origin_no": origin_no, "name": origin.get("name", ""),
                "price": origin.get("salePrice", 0), "channel_no": channel_no,
                "dg_code": dg_code, "rep_image": rep, "cdn1_imgs": cdn[:6]}
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@app.get("/products/catalog")
async def products_catalog():
    """전체 등록 상품 목록(블루오션 선정용): name, price, channel_no, dg_code, image, status."""
    import asyncio as _ai
    out = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        for p in contents:
            origin = p.get("originProduct", {}) or {}
            name = (origin.get("name") or p.get("name") or "").strip()
            price = p.get("salePrice") or origin.get("salePrice") or 0
            channel_no = str(p.get("channelProductNo") or "")
            origin_no = str(p.get("originProductNo") or "")
            dg_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "") or ""
            status = p.get("statusType") or origin.get("statusType") or ""
            img = p.get("representativeImageUrl") or ""
            out.append({"name": name, "price": price, "channel_no": channel_no,
                        "origin_no": origin_no, "dg_code": dg_code, "status": status, "image": img})
        if len(contents) < 50:
            break
        page += 1
        await _ai.sleep(0.3)
    return {"total": len(out), "products": out}


async def _run_html_coverage_scan():
    """백그라운드 HTML 커버리지 스캔 — 결과를 context_store에 저장."""
    import asyncio as _ai
    import httpx as _hx
    applied, not_applied = [], []
    price_mismatch = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50, days=1000)
        contents = resp.get("contents", [])
        if not contents:
            break
        for item in contents:
            origin = item.get("originProduct", {})
            name = (origin.get("name") or item.get("name") or "").strip()
            detail = origin.get("detailContent") or ""
            sale_price = int(origin.get("salePrice") or 0)
            origin_no = str(item.get("originProductNo") or origin.get("id") or "")
            has_html = "Noto Sans KR" in detail
            if has_html:
                applied.append(name)
                # 가격 패턴 검사: ₩X,XXX 또는 X,XXX원
                import re as _re
                price_hits = _re.findall(r"₩[\d,]+|[\d,]+원", detail)
                if price_hits:
                    price_mismatch.append({
                        "name": name[:40],
                        "origin_no": origin_no,
                        "sale_price": sale_price,
                        "prices_in_html": price_hits[:5],
                    })
            else:
                not_applied.append(name)
        if len(contents) < 50:
            break
        page += 1
        await _ai.sleep(0.3)

    total = len(applied) + len(not_applied)
    pct = round(len(applied) / total * 100, 1) if total else 0
    result = {
        "total": total,
        "applied": len(applied),
        "not_applied": len(not_applied),
        "applied_pct": pct,
        "price_in_html": len(price_mismatch),
        "price_mismatch_samples": price_mismatch[:20],
    }
    try:
        async with _hx.AsyncClient(timeout=10) as _c:
            await _c.post(
                "https://loving-serenity-production-2635.up.railway.app/context",
                json={"key": "html.coverage.result", "value": result, "category": "memory"},
            )
    except Exception:
        pass
    print(f"[HTML-COVERAGE] 완료: {len(applied)}/{total} Vision HTML, 가격포함 {len(price_mismatch)}개", flush=True)


@app.get("/html-coverage")
async def html_coverage_scan(background_tasks: BackgroundTasks):
    """전체 상품 HTML 커버리지 스캔 (백그라운드) — 결과는 /html-coverage/result 로 조회."""
    background_tasks.add_task(_run_html_coverage_scan)
    return {"status": "started", "message": "스캔 백그라운드 실행 중. /html-coverage/result 로 결과 확인."}


@app.get("/html-coverage/result")
async def html_coverage_result():
    """html-coverage 스캔 결과 조회 (context_store)."""
    import httpx as _hx
    try:
        async with _hx.AsyncClient(timeout=8) as _c:
            r = await _c.get("https://loving-serenity-production-2635.up.railway.app/context/html.coverage.result")
            return r.json().get("value") or {"status": "no_result", "message": "/html-coverage 먼저 실행 필요"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/reapply-html")
async def reapply_html_endpoint(background_tasks: BackgroundTasks, limit: int = 0, nos: str = ""):
    """Claude HTML 19섹션 재적용 (백그라운드).
    ?nos=13580354278,13580357571 로 지정 상품 우선 강제 재적용.
    ?limit=N 으로 미적용분 배치 처리. 미지정이면 전체. 개별 실패는 건너뛰고 계속."""
    no_list = [n.strip() for n in nos.split(",") if n.strip()] if nos else None
    background_tasks.add_task(pipeline_reapply_claude_html, limit, no_list)
    return {"message": "HTML 재적용 백그라운드 시작", "limit": limit or "전체",
            "nos_count": len(no_list) if no_list else 0,
            "info": "진행은 Railway 로그 [REAPPLY] 태그 확인."}


@app.post("/strip-prices-direct")
async def strip_prices_direct(background_tasks: BackgroundTasks, nos: str = ""):
    """기존 HTML에서 ₩X,XXX / X,XXX원 패턴만 제거 (Claude 재생성 없음).
    ?nos=13580354278,13580357571 — 쉼표 구분 originProductNo 목록 필수."""
    no_list = [n.strip() for n in nos.split(",") if n.strip()]
    if not no_list:
        return {"error": "nos 파라미터 필수 (쉼표 구분 originProductNo)"}
    background_tasks.add_task(_run_strip_prices, no_list)
    return {"message": "가격 제거 백그라운드 시작", "count": len(no_list)}


@app.get("/strip-prices-sync")
async def strip_prices_sync(no: str = ""):
    """단일 상품 가격 제거 — 동기 실행, 결과 즉시 반환 (디버그용)."""
    import re as _re
    if not no:
        return {"error": "no 파라미터 필수"}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}",
                        headers=await naver_api._headers())
        if r.status_code != 200:
            return {"step": "GET", "http": r.status_code, "body": r.text[:500]}
        origin = r.json().get("originProduct", {})
        html = origin.get("detailContent") or ""
        before = _re.findall(r"₩[\d,]+|[\d,]+원", html)
        html = _re.sub(r"₩[\d,]+", "", html)
        html = _re.sub(r"\d{1,3}(?:,\d{3})+원", "", html)
        after = _re.findall(r"₩[\d,]+|[\d,]+원", html)
        _SKIP = {"originProductNo", "channelProductNo", "regDate", "modDate",
                 "statusFrom", "totalSalesQuantity", "statusType", "channelProducts"}
        payload = {k: v for k, v in origin.items() if k not in _SKIP}
        payload["statusType"] = "SALE"
        payload["detailContent"] = html
        # KC인증 kindType 빈 항목 제거 (BAD_REQUEST 방지)
        da = payload.get("detailAttribute")
        if isinstance(da, dict):
            ci = da.get("productCertificationInfos") or []
            cleaned = [x for x in ci if x.get("kindType")]
            if len(cleaned) != len(ci):
                da = dict(da)
                if cleaned:
                    da["productCertificationInfos"] = cleaned
                else:
                    da.pop("productCertificationInfos", None)
                payload["detailAttribute"] = da
        upd = await c.put(f"{NAVER_BASE}/v2/products/origin-products/{no}",
                          headers=await naver_api._headers(),
                          json={"originProduct": payload})
        return {"no": no, "before_prices": before, "after_prices": after,
                "put_http": upd.status_code, "put_body": upd.text[:800]}


async def _run_strip_prices(nos: list[str]):
    import re as _re
    results = {"success": 0, "failed": 0, "skipped": 0}
    for no in nos:
        try:
            # GET with 429 retry
            origin = {}
            for attempt in range(4):
                await asyncio.sleep(2.0 * (attempt + 1))  # 2s, 4s, 6s, 8s
                async with httpx.AsyncClient(timeout=20) as c:
                    r = await c.get(
                        f"{NAVER_BASE}/v2/products/origin-products/{no}",
                        headers=await naver_api._headers()
                    )
                if r.status_code == 200:
                    origin = r.json().get("originProduct", {})
                    break
                if r.status_code == 429:
                    print(f"[STRIP] 429 재시도 {no} ({attempt+1}/4)", flush=True)
                    await asyncio.sleep(15)
                    continue
                print(f"[STRIP] ❌ 조회 실패 {no}: HTTP {r.status_code}", flush=True)
                break
            if not origin:
                results["failed"] += 1
                continue

            html = origin.get("detailContent") or ""
            if not html:
                print(f"[STRIP] ⚠️ detailContent 없음 {no} — skip", flush=True)
                results["skipped"] += 1
                continue
            # 가격 패턴 제거
            before_count = len(_re.findall(r"₩[\d,]+|[\d]{1,3}(?:,\d{3})+원", html))
            html = _re.sub(r"₩[\d,]+", "", html)
            html = _re.sub(r"\d{1,3}(?:,\d{3})+원", "", html)
            if before_count == 0:
                print(f"[STRIP] ✅ 가격 없음 {no} — skip", flush=True)
                results["skipped"] += 1
                continue
            # 전체 origin 필드 포함, readonly 제외 후 statusType 강제 SALE
            _STRIP_SKIP = {"originProductNo", "channelProductNo", "regDate", "modDate",
                           "statusFrom", "totalSalesQuantity", "statusType", "channelProducts"}
            payload = {k: v for k, v in origin.items() if k not in _STRIP_SKIP}
            payload["statusType"] = "SALE"
            payload["detailContent"] = html
            # KC인증 kindType 빈 항목 제거 (BAD_REQUEST 방지)
            _da = payload.get("detailAttribute")
            if isinstance(_da, dict):
                _ci = _da.get("productCertificationInfos") or []
                _cleaned = [x for x in _ci if x.get("kindType")]
                if len(_cleaned) != len(_ci):
                    _da = dict(_da)
                    if _cleaned:
                        _da["productCertificationInfos"] = _cleaned
                    else:
                        _da.pop("productCertificationInfos", None)
                    payload["detailAttribute"] = _da
            async with httpx.AsyncClient(timeout=30) as c:
                upd = await c.put(
                    f"{NAVER_BASE}/v2/products/origin-products/{no}",
                    headers=await naver_api._headers(),
                    json={"originProduct": payload},
                )
            if upd.status_code == 200:
                print(f"[STRIP] ✅ {no} — {before_count}개 가격 제거 완료", flush=True)
                results["success"] += 1
            else:
                print(f"[STRIP] ❌ 업데이트 실패 {no}: HTTP {upd.status_code} {upd.text[:300]}", flush=True)
                results["failed"] += 1
        except Exception as e:
            print(f"[STRIP] ❌ 오류 {no}: {e}", flush=True)
            results["failed"] += 1
        await asyncio.sleep(1.0)
    print(f"[STRIP] 완료: 성공 {results['success']} / 실패 {results['failed']} / 스킵 {results['skipped']}", flush=True)


@app.post("/find-similar-products")
async def find_similar_products_bg(background_tasks: BackgroundTasks, prefix_len: int = 12):
    """유사 상품 탐지 — 백그라운드 실행, 결과는 context_store[smartstore.similar_products_report]에 저장."""
    background_tasks.add_task(_run_find_similar, prefix_len)
    return {"message": "유사 상품 분석 시작 (백그라운드)", "result_key": "smartstore.similar_products_report"}


async def _run_find_similar(prefix_len: int = 12):
    """이름은 달라도 같은 상품일 가능성이 높은 그룹 탐지 — 내부 실행용."""
    from main import _normalize_name as _nn, _ctx_set as _mcs
    import asyncio as _ai
    all_prods = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        for p in contents:
            product_no = str(p.get("originProductNo", ""))
            origin = p.get("originProduct", {})
            seller_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "") or f"NAVER_ID_{product_no}"
            name = (origin.get("name") or "").strip()
            category = str((origin.get("productInfoProvidedNotice") or {}).get("productInfoProvidedNoticeType", "") or
                           (origin.get("detailAttribute") or {}).get("productInfoProvidedNoticeType", "") or "")
            price = int(origin.get("salePrice", 0) or 0)
            norm = _nn(name)
            all_prods.append({
                "product_no": product_no, "code": seller_code,
                "name": name, "norm": norm, "price": price
            })
        if len(contents) < 50:
            break
        page += 1
        await _ai.sleep(0.3)

    # 1) 이름 앞 prefix_len 글자 일치 그룹 (정규화 기준)
    prefix_map: dict[str, list] = {}
    for p in all_prods:
        key = p["norm"][:prefix_len]
        if len(key) >= 6:
            prefix_map.setdefault(key, []).append(p)
    prefix_groups = [
        {"prefix": k, "count": len(v),
         "products": [{"no": x["product_no"], "name": x["name"], "price": x["price"]} for x in v]}
        for k, v in prefix_map.items() if len(v) > 1
    ]

    # 2) 이름이 다른데 너무 유사한 그룹 (앞 8자 동일, 가격 50% 이내)
    suspicious: list[dict] = []
    seen_pairs: set = set()
    for i, a in enumerate(all_prods):
        for b in all_prods[i+1:]:
            if a["product_no"] == b["product_no"]:
                continue
            key = tuple(sorted([a["product_no"], b["product_no"]]))
            if key in seen_pairs:
                continue
            norm_a, norm_b = a["norm"], b["norm"]
            if len(norm_a) < 6 or len(norm_b) < 6:
                continue
            # 앞 8자 일치 + 이름은 다름
            if norm_a[:8] == norm_b[:8] and norm_a != norm_b:
                pa, pb = a["price"], b["price"]
                if pb and pa and abs(pa - pb) / max(pa, pb) < 0.5:
                    seen_pairs.add(key)
                    suspicious.append({
                        "product_a": {"no": a["product_no"], "name": a["name"], "price": pa},
                        "product_b": {"no": b["product_no"], "name": b["name"], "price": pb},
                    })

    # context_store에 결과 저장
    summary = {
        "total": len(all_prods),
        "prefix_dup_groups": len(prefix_groups),
        "suspicious_pairs": len(suspicious),
        "prefix_groups": prefix_groups[:50],
        "suspicious": suspicious[:50],
    }
    _mcs("smartstore.similar_products_report", summary)
    print(f"[유사상품] 분석 완료: {len(all_prods)}개 / 유사그룹:{len(prefix_groups)} / 의심쌍:{len(suspicious)}", flush=True)
    return summary


@app.get("/similar-products-result")
async def similar_products_result():
    """find-similar-products 백그라운드 분석 결과 조회 (context_store)."""
    from main import _ctx_get
    data = _ctx_get("smartstore.similar_products_report")
    if not data:
        return {"status": "not_ready", "message": "POST /find-similar-products 먼저 실행하세요"}
    return data


@app.post("/products/activate-sale-wait")
async def activate_sale_wait_products(background_tasks: BackgroundTasks):
    """판매대기(SALE_WAIT) 상품 전체를 판매중(SALE)으로 변경 — 백그라운드 실행.
    결과는 context_store[smartstore.activate_result]에 저장.
    """
    async def _run():
        import asyncio as _ai
        from datetime import datetime, timezone

        headers = await naver_api._headers()
        changed, failed = [], []
        scanned = 0

        async with httpx.AsyncClient(timeout=30) as c:
            page = 1
            while True:
                now = datetime.now(timezone.utc)
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=headers,
                    json={
                        "page": page,
                        "size": 100,
                        "orderType": "NO",
                        "periodType": "PROD_REG_DAY",
                        "fromDate": "2020-01-01",
                        "toDate": now.strftime("%Y-%m-%d"),
                    },
                )
                if not r.is_success:
                    print(f"[ACTIVATE] 조회 실패 p{page}: {r.status_code} {r.text[:100]}", flush=True)
                    break

                contents = r.json().get("contents", [])
                scanned += len(contents)
                print(f"[ACTIVATE] 페이지 {page} — {len(contents)}개 스캔 중 (누적 {scanned})", flush=True)

                for item in contents:
                    prod_no = str(item.get("originProductNo", ""))
                    if not prod_no:
                        continue

                    # 검색 결과 statusType 우선, 없으면 상세 조회
                    status = item.get("statusType", "")
                    name = item.get("name", "")[:40]
                    if not status:
                        dr = await c.get(
                            f"{NAVER_BASE}/v2/products/origin-products/{prod_no}",
                            headers=headers, timeout=15,
                        )
                        if dr.is_success:
                            origin = dr.json().get("originProduct", {})
                            status = origin.get("statusType", "")
                            name = origin.get("name", name)[:40]

                    if status != "SALE_WAIT":
                        continue

                    print(f"[ACTIVATE] 판매대기 발견: {prod_no} {name}", flush=True)
                    upd = await c.put(
                        f"{NAVER_BASE}/v2/products/origin-products/{prod_no}",
                        headers=headers,
                        json={"originProduct": {"statusType": "SALE"}},
                        timeout=15,
                    )
                    if upd.status_code == 200:
                        changed.append({"id": prod_no, "name": name})
                        print(f"[ACTIVATE] ✅ {prod_no} {name}", flush=True)
                    else:
                        failed.append({"id": prod_no, "name": name, "error": upd.text[:100]})
                        print(f"[ACTIVATE] ❌ {prod_no} {name} → {upd.status_code}", flush=True)
                    await _ai.sleep(0.5)

                if len(contents) < 100:
                    break
                page += 1
                await _ai.sleep(0.3)

        result = {
            "scanned": scanned,
            "sale_wait_found": len(changed) + len(failed),
            "changed": len(changed),
            "failed": len(failed),
            "changed_list": changed,
            "failed_list": failed,
            "done": True,
        }
        from main import _ctx_set as _mcs
        _mcs("smartstore.activate_result", result)
        print(f"[ACTIVATE] 완료 — 스캔 {scanned}개 / 판매대기 {len(changed)+len(failed)}개 / 전환 {len(changed)}개", flush=True)

    background_tasks.add_task(_run)
    return JSONResponse({"status": "started", "message": "백그라운드 실행 중 — /products/activate-sale-wait/result 로 결과 확인"})


@app.get("/products/activate-sale-wait/result")
async def activate_sale_wait_result():
    """activate-sale-wait 백그라운드 작업 결과 조회."""
    from main import _ctx_get as _mcg
    data = _mcg("smartstore.activate_result")
    if not data:
        return JSONResponse({"status": "not_started_or_running", "message": "아직 실행 중이거나 시작 안 됨"})
    return JSONResponse(data)


@app.post("/products/deduplicate")
async def deduplicate_naver(background_tasks: BackgroundTasks):
    """스마트스토어 중복 상품 삭제 — 같은 code/name 중 최신 1개만 유지 (백그라운드)."""
    async def _run():
        from main import _normalize_name as _nn
        import asyncio as _ai
        all_prods = []
        page = 1
        while True:
            resp = await naver_api.list_products(page=page, size=50)
            contents = resp.get("contents", [])
            if not contents:
                break
            for p in contents:
                product_no = str(p.get("originProductNo", ""))
                origin = p.get("originProduct", {})
                seller_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "") or f"NAVER_ID_{product_no}"
                name = (origin.get("name") or "").strip()
                all_prods.append({"product_no": product_no, "code": seller_code, "name": name})
            if len(contents) < 50:
                break
            page += 1
            await _ai.sleep(0.3)
        # code 기준 중복 — 작은 ID 먼저 → 오래된 것 삭제
        deleted = 0
        kept = 0
        seen_codes: set[str] = set()
        seen_names: set[str] = set()
        for p in sorted(all_prods, key=lambda x: int(x["product_no"] or 0), reverse=True):
            code = p["code"]
            nk = _nn(p["name"])
            is_dup = code in seen_codes or (nk and nk in seen_names)
            if is_dup:
                ok = await naver_api.delete_product(p["product_no"])
                if ok:
                    deleted += 1
                    print(f"[DEDUP] 삭제: [{p['product_no']}] {p['name'][:40]} (code:{code})", flush=True)
                await _ai.sleep(0.5)
            else:
                seen_codes.add(code)
                if nk:
                    seen_names.add(nk)
                kept += 1
        print(f"[DEDUP 완료] 삭제={deleted} / 유지={kept}", flush=True)
        return {"deleted": deleted, "kept": kept}
    background_tasks.add_task(_run)
    return {"message": "중복 상품 정리 시작 (백그라운드)", "check": "/find-duplicate-products"}


@app.post("/products/deduplicate/sync")
async def deduplicate_naver_sync():
    """스마트스토어 중복 상품 삭제 — 완료까지 대기 (동기)."""
    from main import _normalize_name as _nn
    import asyncio as _ai
    all_prods = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        for p in contents:
            product_no = str(p.get("originProductNo", ""))
            origin = p.get("originProduct", {})
            seller_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "") or f"NAVER_ID_{product_no}"
            name = (origin.get("name") or "").strip()
            all_prods.append({"product_no": product_no, "code": seller_code, "name": name})
        if len(contents) < 50:
            break
        page += 1
        await _ai.sleep(0.3)
    deleted = 0
    kept = 0
    seen_codes: set[str] = set()
    seen_names: set[str] = set()
    for p in sorted(all_prods, key=lambda x: int(x["product_no"] or 0), reverse=True):
        code = p["code"]
        nk = _nn(p["name"])
        is_dup = code in seen_codes or (nk and nk in seen_names)
        if is_dup:
            ok = await naver_api.delete_product(p["product_no"])
            if ok:
                deleted += 1
                print(f"[DEDUP] 삭제: [{p['product_no']}] {p['name'][:40]}", flush=True)
            await _ai.sleep(0.5)
        else:
            seen_codes.add(code)
            if nk:
                seen_names.add(nk)
            kept += 1
    print(f"[DEDUP 완료] 삭제={deleted} / 유지={kept}", flush=True)
    return {"deleted": deleted, "kept": kept}


@app.post("/register-domeggook-sync")
async def register_domeggook_sync(request: Request):
    """도매꾹 등록 동기 실행 — 결과/에러 즉시 반환 (진단용). Body: {"limit": 1}"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    limit = int(body.get("limit", 1))
    keywords = body.get("keywords") or _DG_KEYWORDS
    min_price = int(body.get("min_price", 3000))
    max_price = int(body.get("max_price", 150000))
    start_page = int(body.get("start_page", 0))
    result = await pipeline_register_from_domeggook(limit, keywords, min_price, max_price, start_page)
    return JSONResponse(result if isinstance(result, dict) else {"result": str(result)})


@app.post("/register-domeggook")
async def register_from_domeggook(request: Request, background_tasks: BackgroundTasks):
    """도매꾹 API 소싱 → 스마트스토어 상품 등록 (백그라운드 실행).
    Body(선택): {"limit": 10, "keywords": ["생활용품","뷰티"], "min_price": 3000, "max_price": 150000}
    DOMEGGOOK_API_KEY 환경변수 필수."""
    if not _sync_done:
        return JSONResponse(
            {"status": "error", "message": "registered_codes 동기화 미완료 — 중복 방지를 위해 잠시 후 다시 시도하세요."},
            status_code=503,
        )
    if not DOMEGGOOK_API_KEY:
        return JSONResponse(
            {"status": "error", "message": "DOMEGGOOK_API_KEY 환경변수가 설정되지 않았습니다."},
            status_code=400,
        )
    try:
        body = await request.json()
    except Exception:
        body = {}
    limit      = int(body.get("limit", 10))
    keywords   = body.get("keywords") or _DG_KEYWORDS
    min_price  = int(body.get("min_price", 3000))
    max_price  = int(body.get("max_price", 150000))
    start_page = int(body.get("start_page", 0))

    background_tasks.add_task(
        pipeline_register_from_domeggook, limit, keywords, min_price, max_price, start_page
    )
    return JSONResponse({
        "status":     "processing",
        "source":     "domeggook",
        "limit":      limit,
        "keywords":   keywords[:5],
        "min_price":  min_price,
        "start_page": start_page if start_page > 0 else "auto",
    })


@app.get("/domeggook-preview")
async def domeggook_preview(limit: int = 10, keyword: str = ""):
    """도매꾹 API 상품 미리보기 — 등록 없이 수집 결과만 확인."""
    if not DOMEGGOOK_API_KEY:
        return JSONResponse({"status": "error", "message": "DOMEGGOOK_API_KEY 없음"}, status_code=400)
    kws = [keyword] if keyword else _DG_KEYWORDS
    products = await fetch_domeggook_products(kws, pool_size=limit * 2)
    return JSONResponse({
        "count": len(products[:limit]),
        "products": [
            {"code": p["code"], "name": p["name"], "price": p["price"],
             "image": p["image"], "category": p["category"]}
            for p in products[:limit]
        ],
    })


@app.get("/domeggook-search")
async def domeggook_search_endpoint(kw: str = "", sz: int = 10):
    """도매꾹 키워드 검색 — 원가 조회용."""
    import httpx as _hx
    if not DOMEGGOOK_API_KEY:
        return JSONResponse({"error": "DOMEGGOOK_API_KEY 없음"}, status_code=400)
    if not kw:
        return JSONResponse({"error": "kw 파라미터 필요"}, status_code=400)
    try:
        async with _hx.AsyncClient(timeout=15) as c:
            r = await c.get("https://domeggook.com/ssl/api/", params={
                "ver": "4.1", "mode": "getItemList",
                "aid": DOMEGGOOK_API_KEY, "market": "dome",
                "kw": kw, "om": "json", "sz": sz, "pg": "1", "so": "rd",
            })
        items = r.json().get("domeggook", {}).get("list", {}).get("item", [])
        if isinstance(items, dict):
            items = [items]
        result = [{"no": it.get("no"), "title": it.get("title","")[:60],
                   "price": it.get("price"), "delivery_price": it.get("delivery_price")}
                  for it in items]
        return JSONResponse({"count": len(result), "items": result})
    except Exception as e:
        return JSONResponse({"error": str(e)})


@app.get("/domeggook-debug")
async def domeggook_debug():
    """도매꾹 API 직접 호출 + fetch_domeggook_products 함수 테스트 (진단용)"""
    import httpx as _httpx
    key = DOMEGGOOK_API_KEY
    if not key:
        return JSONResponse({"error": "DOMEGGOOK_API_KEY 없음"}, status_code=400)
    url = "https://domeggook.com/ssl/api/"

    # ① 직접 API 호출
    direct_result = {}
    try:
        async with _httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url, params={
                "ver": "4.1", "mode": "getItemList",
                "aid": key, "market": "dome",
                "kw": "생활용품", "om": "json",
                "mnp": "3000", "mxp": "150000",
                "sz": "3", "pg": "1", "so": "rd",
            })
        data = r.json()
        items = data.get("domeggook", {}).get("list", {}).get("item", [])
        direct_result = {"status": r.status_code, "items_count": len(items),
                         "first_title": items[0].get("title","") if items else ""}
    except Exception as e:
        direct_result = {"error": str(e)}

    # ② fetch_domeggook_products 함수 호출
    func_result = {}
    try:
        prods = await fetch_domeggook_products(["생활용품"], pool_size=3, min_price=3000, max_price=150000)
        func_result = {"count": len(prods), "first_name": prods[0].get("name","") if prods else ""}
    except Exception as e:
        func_result = {"error": str(e)}

    return JSONResponse({
        "key_prefix": key[:6] + "***",
        "direct_api": direct_result,
        "fetch_func": func_result,
    })


@app.get("/dg-item/{item_no}")
async def dg_item_detail(item_no: str):
    """도매꾹 단일 상품 이미지 URL 조회 (item_no 직접 입력)."""
    import httpx as _httpx
    key = DOMEGGOOK_API_KEY
    if not key:
        return JSONResponse({"error": "DOMEGGOOK_API_KEY 없음"}, status_code=400)
    try:
        async with _httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://domeggook.com/ssl/api/", params={
                "ver": "4.5", "mode": "getItemView", "aid": key,
                "no": item_no, "om": "json",
            })
        data = r.json().get("domeggook", {})
        thumb = data.get("thumb", {})
        def _dg_s(v):
            return str(v.get("#text","") or v.get("text","") or "") if isinstance(v, dict) else str(v or "")
        def _to_full(url):
            s = _dg_s(url)
            if not s: return ""
            return s if s.startswith("http") else f"https://cdn1.domeggook.com/{s}"
        img_url = _to_full(thumb.get("original")) or _to_full(thumb.get("large", ""))
        basis = data.get("basis", {})
        price_obj = data.get("price", {}) or {}
        def _price_int(v):
            s = _dg_s(v)
            return int("".join(c for c in s if c.isdigit()) or "0")
        dome_price = _price_int(price_obj.get("dome") or price_obj.get("domePrice") or 0)
        return JSONResponse({
            "item_no": item_no,
            "name": _dg_s(basis.get("title", "")),
            "img": img_url,
            "wholesale_price": dome_price,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/update-products-seo")
async def update_products_seo_smartstore(request: Request, background_tasks: BackgroundTasks):
    """스마트스토어 기존 상품 GEO/SEO/HS코드 일괄 업데이트 (백그라운드)."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    limit = int(body.get("limit", 100))
    skip_has_geo = bool(body.get("skip_has_geo", True))

    async def _run():
        await update_existing_products_seo(limit=limit, skip_has_geo=skip_has_geo)

    background_tasks.add_task(_run)
    return JSONResponse({"status": "started", "message": f"스마트스토어 상품 최대 {limit}개 SEO 업데이트 시작"})


@app.post("/register-single")
async def register_single_product(request: Request):
    """
    n8n Loop 전용 — 상품 1개 즉시 등록
    Body: {"title": "상품명", "category": "카테고리명", "price": 15000, "image": "https://..."}
    n8n 변수 예시: $node["Input"].json["title"]
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"status": "error", "message": "JSON 파싱 실패"}, status_code=400)

    product_name = str(body.get("title", "")).strip()
    if not product_name:
        return JSONResponse({"status": "error", "message": "title 필드 필수"}, status_code=400)

    category = str(body.get("category", ""))
    raw_price = body.get("price", 0)
    image_url = str(body.get("image", ""))

    p = {
        "name": product_name,
        "category": category,
        "price": raw_price,
        "image": image_url,
        "code": body.get("code", ""),
        "category_id": body.get("category_id", ""),   # 유효 리프 카테고리ID 직접 지정 (없으면 키워드맵 폴백)
    }

    registered_codes = load_registered_codes()
    registered_names = load_registered_names()
    name_norm = _normalize_name(str(p.get("name", "")))
    if p["code"] and p["code"] in registered_codes:
        return JSONResponse({"status": "duplicate", "message": "이미 등록된 상품 (코드 중복)"})
    if name_norm and name_norm in registered_names:
        return JSONResponse({"status": "duplicate", "message": "이미 등록된 상품 (상품명 중복)"})

    try:
        from employees import (
            employee_ip_guardian, employee_review_analyst,
            employee_price_optimizer, employee_tag_generator,
        )
        safe, danger_kw = employee_ip_guardian(p)
        if not safe:
            return JSONResponse({"status": "ip_blocked", "keyword": danger_kw})

        review = await employee_review_analyst(product_name, ANTHROPIC_API_KEY)
        ai = await generate_product_copy(p, {
            "season": "", "trends": [], "pain_points": review.get("pain_points", []),
            "selling_points": review.get("selling_points", []),
        })

        # Tool 3: SEO 태그 생성
        seo_tags = await employee_tag_generator(
            product_name, category, review.get("selling_points", []), ANTHROPIC_API_KEY)
        ai["tags"] = seo_tags

        # Tool 2: 경쟁사 가격 수집 → 최적 가격 산정
        from main import search_naver_shopping
        competitor_prices = await search_naver_shopping(product_name)
        price_result = await employee_price_optimizer(
            product_name, category, raw_price, ANTHROPIC_API_KEY,
            competitor_prices=competitor_prices)
        price = price_result["suggested_price"]
        print(f"[가격최적화] {price:,}원 — {price_result.get('reason','')}", flush=True)
        if price_result.get("skip"):
            return JSONResponse({
                "status": "skip",
                "reason": price_result.get("reason", "경쟁가 하회"),
                "competitor_min": price_result.get("competitor_min"),
            })

        naver_img_url = await get_product_image(p)
        if not naver_img_url:
            return JSONResponse({"status": "error", "message": "이미지 소스 없음"}, status_code=500)

        headline_txt = ai.get("headline") or product_name[:18]
        if image_url:
            # 원본 이미지 있으면 DALL-E 배너 스킵
            banner_url = await create_banner_image(
                naver_img_url, headline_txt, ai.get("sub_headline", ""))
        else:
            dalle_banner_raw = await generate_dalle_banner(product_name, headline_txt, category)
            if dalle_banner_raw:
                banner_url = await naver_api.upload_image(dalle_banner_raw, is_banner=True)
            else:
                banner_url = await create_banner_image(
                    naver_img_url, headline_txt, ai.get("sub_headline", ""))

        detail_img_url = ""
        if not image_url:
            # 원본 이미지 없을 때만 DALL-E 상세컷 생성
            dalle_detail_raw = await generate_dalle_detail_shot(
                product_name, ai.get("spec_hint", ""), category)
            if dalle_detail_raw:
                try:
                    detail_img_url = await naver_api.upload_image(dalle_detail_raw)
                except Exception:
                    pass

        detail_html = build_detail_html(banner_url, naver_img_url, ai, detail_img_url)

        _, reject_kws = _get_scene_context(product_name)
        qc_result = await run_qc_pipeline(
            naver_img_url, product_name, detail_html, ANTHROPIC_API_KEY, reject_kws)

        if not qc_result["passed"]:
            if qc_result["stage"] == 2:
                retry_raw = await generate_dalle_image(
                    f"{product_name} {qc_result.get('retry_prompt','')}".strip(), category)
                if retry_raw:
                    retry_img = await naver_api.upload_image(retry_raw)
                    qc2 = await run_qc_pipeline(
                        retry_img, product_name, detail_html, ANTHROPIC_API_KEY, reject_kws)
                    if qc2["passed"]:
                        naver_img_url = retry_img
                    else:
                        return JSONResponse({"status": "qc_fail", "stage": qc2["stage"], "reason": qc2["reason"]})
                else:
                    return JSONResponse({"status": "qc_fail", "stage": 2, "reason": "DALL-E 재생성 실패"})
            else:
                return JSONResponse({"status": "qc_fail", "stage": qc_result["stage"], "reason": qc_result["reason"]})

        payload = build_product_payload(p, ai, price, tags=ai.get("tags"))
        # 상품명은 무조건 입력된 title 사용 (AI 생성 이름 덮어쓰기)
        from main import clean_product_name
        safe_name = clean_product_name(product_name) or product_name[:25]
        payload["originProduct"]["name"] = safe_name
        payload["originProduct"]["images"]["representativeImage"]["url"] = naver_img_url
        if detail_html:
            payload["originProduct"]["detailContent"] = detail_html

        # 가격표시제 대상 카테고리 필수 필드 (unitPriceYn) — 미표시(false)로 충족
        payload["originProduct"].setdefault("detailAttribute", {})["unitCapacity"] = {"unitPriceYn": False}

        result = await naver_api.register_product(payload)
        save_registered_code(p["code"])
        save_registered_name(safe_name)
        _res = result or {}
        origin_no = str(_res.get("originProductNo", "") or "")
        chans = _res.get("channelProducts", []) or []
        channel_no = str(_res.get("smartstoreChannelProductNo", "")
                         or (chans[0].get("channelProductNo", "") if chans else "") or "")
        store_url = (f"https://smartstore.naver.com/khww/products/{channel_no}"
                     if channel_no else (f"https://smartstore.naver.com/khww/products/{origin_no}" if origin_no else ""))
        # ── costPrice 저장 (도매가 → Naver costPrice GET+PUT) ──
        if origin_no:
            asyncio.create_task(_save_cost_price_async(origin_no, int(p.get("price", 0) or 0)))
        # ── Vision HTML 강제적용 (등록 직후, 실패해도 등록은 성공 유지) ──
        if origin_no:
            try:
                import asyncio as _aio
                from main import generate_claude_html_detail as _gen_html, _to_naver_fragment as _frag
                _vimgs = [u for u in [naver_img_url] if u]
                try:
                    import httpx as _hx2
                    from main import _dg_item_detail as _dgd_fn, _dg_to_product as _dgp_fn
                    _dgno = "".join(_c for _c in str(p.get("code", "")) if _c.isdigit())
                    if _dgno and len(_dgno) >= 6:
                        _dgd = await _dgd_fn(_dgno)
                        _dgp = _dgp_fn({"no": _dgno, "title": product_name, "price": price or 1}, _dgd) if _dgd else None
                        _exraw = (_dgp or {}).get("_dg_extra_images") or []
                        async with _hx2.AsyncClient(timeout=15, follow_redirects=True) as _ec:
                            for _eu in _exraw[:14]:
                                try:
                                    _er = await _ec.get(_eu)
                                    if _er.status_code != 200 or len(_er.content) < 30 * 1024:
                                        continue
                                    _enav = await naver_api.upload_detail_image(_er.content)
                                    if _enav:
                                        _vimgs.append(_enav)
                                except Exception:
                                    pass
                        print(f"[register-single] 도매꾹 추가이미지 {len(_vimgs)-1}장 수집", flush=True)
                except Exception as _dge:
                    print(f"[register-single] 도매꾹 추가이미지 실패(무시): {str(_dge)[:120]}", flush=True)
                _vh = await _gen_html(p, ai, _vimgs)
                if _vh and "Noto Sans KR" in _vh and len(_vh) >= 1000:
                    _vh = _frag(_vh)
                    if _vh and len(_vh) >= 500:
                        _upd = {k: v for k, v in payload["originProduct"].items()
                                if k not in ("originProductNo", "channelProductNo", "regDate", "modDate", "statusFrom", "totalSalesQuantity")}
                        _upd["detailContent"] = _vh
                        _vok, _verr = False, ""
                        for _va in range(3):
                            try:
                                _vok, _verr = await naver_api.update_product(origin_no, _upd)
                            except Exception as _ve:
                                _vok, _verr = False, str(_ve)[:150]
                            if _vok:
                                break
                            await _aio.sleep(20 * (_va + 1) if ("429" in str(_verr) or "RATE" in str(_verr).upper()) else 3)
                        print(f"[register-single] {'OK Vision HTML 강제적용 완료' if _vok else 'WARN Vision HTML 적용실패(등록은 성공)'}: {safe_name[:30]} {'' if _vok else str(_verr)[:200]}", flush=True)
                    else:
                        print(f"[register-single] WARN Vision HTML fragment 빈값 skip(등록은 성공): {safe_name[:30]}", flush=True)
                else:
                    print(f"[register-single] WARN Vision HTML 생성실패/짧음(등록은 성공): {safe_name[:30]}", flush=True)
            except Exception as _ve:
                print(f"[register-single] WARN Vision HTML 단계 예외(등록은 성공): {str(_ve)[:200]}", flush=True)
        return JSONResponse({"status": "success", "product_name": safe_name, "price": price,
                             "origin_no": origin_no, "channel_no": channel_no, "store_url": store_url})

    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    save_path = Path(EXCEL_FOLDER) / file.filename
    content = await file.read()
    save_path.write_bytes(content)
    return JSONResponse({"status": "uploaded", "path": str(save_path), "size": len(content)})


@app.post("/download-excel")
async def download_excel_from_url(request: Request):
    """URL에서 Excel 파일 다운로드 (Google Drive 등)"""
    import httpx
    try:
        body = await request.json()
        url = body.get("url", "")
        filename = body.get("filename", "ownerclan_latest.xlsx")
    except Exception:
        return JSONResponse({"status": "error", "message": "url 필요"}, status_code=400)

    if not url:
        return JSONResponse({"status": "error", "message": "url 필요"}, status_code=400)

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
        r = await c.get(url)
        r.raise_for_status()

    save_path = Path(EXCEL_FOLDER) / filename
    save_path.write_bytes(r.content)
    return JSONResponse({"status": "downloaded", "path": str(save_path), "size": len(r.content)})


DRIVE_FOLDER_ID   = "1jTkPYwxOqdGEcCQCUgm5A2YlNDGRqprF"
FALLBACK_FILE_ID  = "1h6KTzD5-rcqCODII0GHsLPdIxaYTSlVz"
GOOGLE_API_KEY    = os.environ.get("GOOGLE_API_KEY", "")
_TMP_UPLOADS = Path("/tmp/uploads")
_TMP_UPLOADS.mkdir(parents=True, exist_ok=True)
DRIVE_INDEX_FILE  = str(_TMP_UPLOADS / "drive_index.json")
EXCEL_PROGRESS    = str(_TMP_UPLOADS / "excel_progress.json")


def _load_drive_index() -> list:
    try:
        with open(DRIVE_INDEX_FILE, encoding="utf-8") as f:
            return json.load(f).get("file_ids", [])
    except Exception:
        return []


def _save_drive_index(file_ids: list):
    with open(DRIVE_INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump({"file_ids": file_ids, "scanned_at": str(datetime.now(timezone.utc))}, f)


async def _scan_drive_folder() -> list:
    """Google Drive API로 폴더 내 파일 ID 목록 조회"""
    if not GOOGLE_API_KEY:
        print("[DRIVE] GOOGLE_API_KEY 없음", flush=True)
        return []
    try:
        file_ids = []
        page_token = None
        async with httpx.AsyncClient(timeout=20) as c:
            while True:
                params = {
                    "q": f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
                    "fields": "nextPageToken,files(id,name)",
                    "pageSize": 200,
                    "key": GOOGLE_API_KEY,
                }
                if page_token:
                    params["pageToken"] = page_token
                r = await c.get("https://www.googleapis.com/drive/v3/files", params=params)
                r.raise_for_status()
                data = r.json()
                for f in data.get("files", []):
                    file_ids.append(f["id"])
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
        print(f"[DRIVE] API 스캔 완료: {len(file_ids)}개 파일 발견", flush=True)
        return file_ids
    except Exception as e:
        print(f"[DRIVE] API 스캔 실패: {e}", flush=True)
        return []


@app.post("/build-drive-index")
async def build_drive_index():
    """Google Drive 폴더 스캔 → 파일 ID 인덱스 구축"""
    api_key_set = bool(GOOGLE_API_KEY)
    debug_info = {"api_key_set": api_key_set, "folder_id": DRIVE_FOLDER_ID}

    # Drive API 직접 호출해서 에러 내용 확인
    error_detail = None
    file_ids = []
    if GOOGLE_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(
                    "https://www.googleapis.com/drive/v3/files",
                    params={
                        "q": f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
                        "fields": "nextPageToken,files(id,name)",
                        "pageSize": 200,
                        "key": GOOGLE_API_KEY,
                    }
                )
                debug_info["api_status"] = r.status_code
                data = r.json()
                if r.status_code == 200:
                    file_ids = [f["id"] for f in data.get("files", [])]
                    # 페이지 추가 처리
                    page_token = data.get("nextPageToken")
                    while page_token:
                        r2 = await c.get(
                            "https://www.googleapis.com/drive/v3/files",
                            params={
                                "q": f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
                                "fields": "nextPageToken,files(id,name)",
                                "pageSize": 200,
                                "key": GOOGLE_API_KEY,
                                "pageToken": page_token,
                            }
                        )
                        d2 = r2.json()
                        file_ids += [f["id"] for f in d2.get("files", [])]
                        page_token = d2.get("nextPageToken")
                else:
                    error_detail = data.get("error", {}).get("message", str(data))
        except Exception as e:
            error_detail = str(e)
    else:
        error_detail = "GOOGLE_API_KEY 환경변수 없음"

    if file_ids:
        _save_drive_index(file_ids)
        return JSONResponse({"status": "ok", "message": f"{len(file_ids)}개 파일 ID 저장 완료", "count": len(file_ids), "debug": debug_info})
    else:
        _save_drive_index([FALLBACK_FILE_ID])
        return JSONResponse({"status": "fallback", "message": "스캔 실패", "error": error_detail, "debug": debug_info})


@app.post("/add-drive-file-ids")
async def add_drive_file_ids(request: Request):
    """파일 ID 목록 수동 등록 (Drive 스캔이 안 될 때 직접 입력)
    Body: {"file_ids": ["id1", "id2", ...]}
    """
    try:
        body = await request.json()
        new_ids = body.get("file_ids", [])
    except Exception:
        return JSONResponse({"status": "error", "message": "file_ids 배열 필요"}, status_code=400)

    existing = _load_drive_index()
    combined = list(dict.fromkeys(existing + new_ids))  # 중복 제거, 순서 유지
    _save_drive_index(combined)
    return JSONResponse({"status": "ok", "total": len(combined), "added": len(combined) - len(existing)})


@app.get("/drive-index-status")
async def drive_index_status():
    """현재 Drive 인덱스 상태 확인"""
    file_ids = _load_drive_index()
    try:
        with open(EXCEL_PROGRESS, encoding="utf-8") as f:
            progress = json.load(f)
    except Exception:
        progress = {"current_index": 0}
    idx = progress.get("current_index", 0)
    return JSONResponse({
        "indexed_files": len(file_ids),
        "current_index": idx,
        "next_file_id": file_ids[idx % len(file_ids)] if file_ids else FALLBACK_FILE_ID,
        "cycles_completed": idx // len(file_ids) if file_ids else 0,
    })


@app.post("/next-excel")
async def next_excel_from_drive():
    """Google Drive에서 다음 순서 Excel 다운로드 (인덱스 순환)"""
    # 1. 파일 ID 목록 로드 — 없으면 Drive 스캔 시도
    file_ids = _load_drive_index()
    if not file_ids:
        print("[DRIVE] 인덱스 없음 — Drive 스캔 시도", flush=True)
        file_ids = await _scan_drive_folder()
        if file_ids:
            _save_drive_index(file_ids)
        else:
            file_ids = [FALLBACK_FILE_ID]
            print("[DRIVE] 스캔 실패 — 폴백 파일 ID 사용", flush=True)

    # 2. 진행 상황 로드
    try:
        with open(EXCEL_PROGRESS, encoding="utf-8") as f:
            progress = json.load(f)
    except Exception:
        progress = {"current_index": 0}

    idx = progress.get("current_index", 0) % len(file_ids)
    file_id = file_ids[idx]

    # 3. 다운로드 (3회 재시도)
    download_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"
    print(f"[DRIVE] 다운로드 중: {file_id} (인덱스 {idx+1}/{len(file_ids)})", flush=True)

    import asyncio as _asyncio
    content: bytes | None = None
    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
                r = await c.get(download_url)
                r.raise_for_status()
                content = r.content
            break
        except Exception as exc:
            if attempt < 3:
                print(f"[DRIVE] 다운로드 실패({attempt}/3): {exc} — 5s 재시도", flush=True)
                await _asyncio.sleep(5)
            else:
                raise
    save_path = Path(EXCEL_FOLDER) / "ownerclan_latest.xlsx"
    save_path.write_bytes(content)

    # 4. 진행 상황 저장
    next_idx = (idx + 1) % len(file_ids)
    progress["current_index"] = next_idx
    with open(EXCEL_PROGRESS, "w", encoding="utf-8") as f:
        json.dump(progress, f)

    return JSONResponse({
        "status": "downloaded",
        "file_id": file_id,
        "index": idx + 1,
        "total": len(file_ids),
        "next_index": next_idx + 1,
        "size_bytes": len(r.content),
    })


@app.post("/process-orders")
async def process_orders(background_tasks: BackgroundTasks):
    background_tasks.add_task(pipeline_process_orders)
    return JSONResponse({"status": "processing"})


@app.post("/sync-inventory")
async def sync_inventory(background_tasks: BackgroundTasks):
    background_tasks.add_task(pipeline_sync_inventory)
    return JSONResponse({"status": "processing"})


@app.post("/reply-inquiries")
async def reply_inquiries(background_tasks: BackgroundTasks):
    background_tasks.add_task(pipeline_reply_inquiries)
    return JSONResponse({"status": "processing"})


@app.get("/cs/inquiries")
async def cs_inquiries(status: str = "pending", limit: int = 10):
    """CS 문의 조회 — cs_automator 연동용 (Naver 문의 API 미연동 시 빈 목록 반환)."""
    return JSONResponse({"inquiries": [], "total": 0})


@app.get("/cs/reviews")
async def cs_reviews(limit: int = 20, days: int = 1):
    """최근 리뷰 조회 — cs_automator 연동용 (Naver 리뷰 API 미연동 시 빈 목록 반환)."""
    return JSONResponse({"reviews": [], "total": 0})


@app.post("/price-audit")
async def price_audit_now(limit: int = 100):
    """경쟁사 가격 감사 즉시 실행 (백그라운드). 일일 자동 실행과 동일한 로직."""
    from main import _run_price_competition_update
    asyncio.create_task(_run_price_competition_update(limit=limit))
    return JSONResponse({"status": "started", "limit": limit, "message": f"가격 감사 {limit}개 백그라운드 실행 중"})


@app.post("/set-cost-price")
async def set_cost_price(product_no: str, cost_price: int):
    """단일 상품 costPrice 수동 저장. PUT 응답 body에서 costPrice를 직접 확인."""
    import httpx as _hx, asyncio as _aio
    from main import NAVER_BASE
    headers = await naver_api._headers()
    async with _hx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{product_no}", headers=headers)
    if r.status_code != 200:
        return JSONResponse({"ok": False, "error": f"Naver GET 실패: HTTP {r.status_code} {r.text[:200]}"})

    data = r.json()
    origin = dict(data.get("originProduct", {}))
    old_cost = origin.get("costPrice", 0)
    origin["costPrice"] = cost_price

    # PUT 직접 실행 — 응답 body에서 저장 결과 확인
    async with _hx.AsyncClient(timeout=30) as cp:
        rp = await cp.put(
            f"{NAVER_BASE}/v2/products/origin-products/{product_no}",
            headers=await naver_api._headers(),
            json={"originProduct": origin},
        )

    put_status = rp.status_code
    if put_status != 200:
        return JSONResponse({"ok": False, "error": f"Naver PUT 실패: HTTP {put_status} {rp.text[:300]}"})

    # PUT 응답 body에서 costPrice 직접 읽기
    put_body = rp.json() if rp.text else {}
    put_cost = put_body.get("originProduct", {}).get("costPrice", -1)

    # 5초 후 재조회 (429 방지)
    await _aio.sleep(5)
    async with _hx.AsyncClient(timeout=20) as c2:
        r2 = await c2.get(f"{NAVER_BASE}/v2/products/origin-products/{product_no}",
                          headers=await naver_api._headers())
    recheck_cost = r2.json().get("originProduct", {}).get("costPrice", -1) if r2.status_code == 200 else -1
    recheck_http = r2.status_code

    return JSONResponse({
        "ok": True,
        "product_no": product_no,
        "old_cost_price": old_cost,
        "requested_cost_price": cost_price,
        "put_http": put_status,
        "put_body_cost_price": put_cost,       # PUT 응답 body 기준
        "recheck_http": recheck_http,
        "recheck_cost_price": recheck_cost,    # 5초 후 재조회 기준
        "saved": recheck_cost == cost_price,
        "note": "put_body_cost_price=-1이면 Naver가 costPrice 응답 미포함, recheck 기준으로 판단"
    })


@app.post("/set-dg-code")
async def set_dg_code(product_no: str, dg_code: str):
    """상품의 sellerManagementCode(DG 코드)를 Naver API로 직접 업데이트."""
    import httpx as _hx
    from main import NAVER_BASE
    headers = await naver_api._headers()
    async with _hx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{product_no}", headers=headers)
    if r.status_code != 200:
        return JSONResponse({"ok": False, "error": f"Naver 조회 실패: HTTP {r.status_code}"})
    data = r.json()
    origin = dict(data.get("originProduct", {}))
    seller_code_info = dict(origin.get("sellerCodeInfo") or {})
    old_code = seller_code_info.get("sellerManagementCode", "")
    seller_code_info["sellerManagementCode"] = dg_code
    origin["sellerCodeInfo"] = seller_code_info
    ok, err = await naver_api.update_product(product_no, origin)
    if ok:
        return JSONResponse({"ok": True, "product_no": product_no,
                             "old_dg_code": old_code, "new_dg_code": dg_code})
    return JSONResponse({"ok": False, "error": err})


@app.get("/dg-search")
async def dg_search_keyword(keyword: str, limit: int = 10):
    """도매꾹 getItemList API 키워드 검색 — DG 상품번호/이름/도매가 반환."""
    import httpx as _hx
    from main import DOMEGGOOK_API_URL
    if not DOMEGGOOK_API_KEY:
        return JSONResponse({"ok": False, "error": "DOMEGGOOK_API_KEY 미설정"})
    try:
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(DOMEGGOOK_API_URL, params={
                "ver": "4.1", "mode": "getItemList", "aid": DOMEGGOOK_API_KEY,
                "market": "dome", "kw": keyword, "om": "json", "sz": str(limit),
            })
        raw = r.json().get("domeggook", {})
        items = raw.get("list", {}).get("item", []) or []
        if not isinstance(items, list):
            items = [items]
        return {"ok": True, "keyword": keyword, "count": len(items),
                "items": [{"item_no": it.get("no"), "name": it.get("name", ""),
                           "wholesale": int(it.get("price", 0) or 0)} for it in items]}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:300]})


_backfill_running = False  # 중복 실행 방지 플래그


async def _cs_save(key: str, value: dict):
    """context_store에 저장 (실패 시 로그)"""
    import httpx as _hx2
    try:
        async with _hx2.AsyncClient(timeout=10) as c2:
            await c2.post(
                "https://loving-serenity-production-2635.up.railway.app/context",
                json={"key": key, "value": json.dumps(value), "category": "audit"},
            )
    except Exception as _e2:
        print(f"[BACKFILL] context_store 저장 실패 key={key}: {_e2}", flush=True)


async def _run_backfill_cost_prices():
    """백그라운드: 전체 상품 costPrice 소급 저장 (DG 웹스크래핑). 결과 context_store에 저장."""
    global _backfill_running
    if _backfill_running:
        print("[BACKFILL] 이미 실행 중 — 중복 실행 방지", flush=True)
        return
    _backfill_running = True

    import httpx as _hx, re as _re, random as _rnd
    print("[BACKFILL] costPrice 소급 저장 시작 (웹스크래핑 방식)", flush=True)

    # 헤더는 루프 내에서 50개마다 갱신 (Naver 토큰 1시간 만료 대비)
    hdrs = await naver_api._headers()
    _hdr_refresh_counter = 0
    dg_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }
    _SKIP = {"originProductNo", "channelProductNo", "regDate", "modDate",
             "statusFrom", "totalSalesQuantity", "channelProducts"}

    # 1. 전체 상품 origin_no 수집 (SALE + SUSPENSION + OUTOFSTOCK)
    all_origin_nos: list[str] = []
    page = 1
    while True:
        try:
            async with _hx.AsyncClient(timeout=20) as c:
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                    json={"productStatusTypes": ["SALE", "SUSPENSION", "OUTOFSTOCK"],
                          "page": page, "size": 100, "orderType": "NO",
                          "periodType": "PROD_REG_DAY",
                          "fromDate": "2020-01-01", "toDate": "2030-12-31"},
                )
            contents = r.json().get("contents", [])
            if not contents:
                break
            all_origin_nos.extend([str(p.get("originProductNo", "")) for p in contents])
            if len(contents) < 100:
                break
            page += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"[BACKFILL] 목록 조회 실패 page{page}: {e}", flush=True)
            break

    results = {"done": False, "ok": 0, "skip_no_code": 0, "skip_dg_fail": 0,
               "skip_already_set": 0, "fail": 0, "total": len(all_origin_nos), "processed": 0}
    print(f"[BACKFILL] 대상 {len(all_origin_nos)}개", flush=True)
    await _cs_save("ss.backfill_cost_prices.result", results)  # 시작 상태 저장

    # 2. 각 상품 처리
    for idx, origin_no in enumerate(all_origin_nos):
        # 50개마다 토큰 갱신 (1시간 만료 대비)
        _hdr_refresh_counter += 1
        if _hdr_refresh_counter % 50 == 0:
            hdrs = await naver_api._headers()
        try:
            async with _hx.AsyncClient(timeout=15) as c:
                rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{origin_no}", headers=hdrs)
            if rd.status_code != 200:
                results["fail"] += 1
                continue
            od = rd.json().get("originProduct", {})

            # costPrice 이미 있으면 스킵
            if int(od.get("costPrice") or 0) > 0:
                results["skip_already_set"] += 1
                continue

            # DG 코드 파싱 ("DG_12345" 또는 "12345" 두 형식)
            dg_raw = str(od.get("detailAttribute", {}).get("sellerCodeInfo", {})
                           .get("sellerManagementCode", "") or "").strip()
            if dg_raw.upper().startswith("DG_"):
                item_no = dg_raw[3:].strip()
            elif dg_raw.isdigit():
                item_no = dg_raw
            else:
                results["skip_no_code"] += 1
                continue

            # DG 웹스크래핑으로 도매가 추출 (API 대신 — Railway IP 차단 우회)
            await asyncio.sleep(_rnd.uniform(3.0, 5.0))
            try:
                _item_no_cap = item_no  # 클로저 캡처 고정
                async def _dg_get(_no=_item_no_cap):
                    async with _hx.AsyncClient(timeout=12, follow_redirects=True) as _c:
                        return await _c.get(
                            f"https://domeggook.com/main/item/itemView.php?no={_no}",
                            headers=dg_headers,
                        )
                r_dg = await asyncio.wait_for(_dg_get(), timeout=20)
                text = r_dg.text
                m = (_re.search(r'["\']?baseAmtDome["\']?\s*[:=]\s*["\']?(\d+)', text)
                     or _re.search(r'optionPrice["\']?\s*[:=]\s*["\']?(\d+)', text)
                     or _re.search(r'"price"\s*:\s*(\d+)', text)
                     or _re.search(r'판매가[^0-9]*(\d{3,7})', text))
                wholesale = int(m.group(1)) if m else 0
            except asyncio.TimeoutError:
                print(f"[BACKFILL] DG 20초 타임아웃({item_no}) — 스킵", flush=True)
                results["skip_dg_fail"] += 1
                continue
            except Exception as e:
                print(f"[BACKFILL] DG 스크래핑 실패({item_no}): {e}", flush=True)
                results["skip_dg_fail"] += 1
                continue

            if wholesale <= 0:
                results["skip_dg_fail"] += 1
                continue

            # read-modify-write로 costPrice 저장
            payload = {k: v for k, v in od.items() if k not in _SKIP}
            payload["costPrice"] = wholesale
            async with _hx.AsyncClient(timeout=20) as c:
                rp = await c.put(
                    f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                    headers=hdrs, json={"originProduct": payload},
                )
            if rp.status_code == 200:
                results["ok"] += 1
                print(f"[BACKFILL] ✅ {od.get('name','')[:25]} costPrice={wholesale:,}", flush=True)
            else:
                results["fail"] += 1
                print(f"[BACKFILL] ❌ {origin_no}: {rp.text[:80]}", flush=True)
        except Exception as e:
            results["fail"] += 1
            print(f"[BACKFILL] 오류 {origin_no}: {str(e)[:60]}", flush=True)

        results["processed"] = idx + 1
        # 50개마다 중간 결과 저장
        if (idx + 1) % 50 == 0:
            print(f"[BACKFILL] 진행 {idx+1}/{len(all_origin_nos)} — ok={results['ok']} "
                  f"skip_set={results['skip_already_set']} skip_dg={results['skip_dg_fail']}", flush=True)
            await _cs_save("ss.backfill_cost_prices.result", results)

    results["done"] = True
    await _cs_save("ss.backfill_cost_prices.result", results)
    _backfill_running = False
    print(f"[BACKFILL] 완료 — {results}", flush=True)


@app.post("/backfill-cost-prices")
async def backfill_cost_prices():
    """SALE 상품 전체 costPrice 소급 저장 (DG API 활용, 백그라운드). 결과: /backfill-cost-prices-result"""
    if _backfill_running:
        return JSONResponse({"status": "already_running", "message": "이미 실행 중. /backfill-cost-prices-result 에서 진행상황 확인"})
    asyncio.create_task(_run_backfill_cost_prices())
    return JSONResponse({"status": "started", "message": "결과는 /backfill-cost-prices-result 에서 조회"})


@app.get("/backfill-cost-prices-result")
async def backfill_cost_prices_result():
    import httpx as _hx
    async with _hx.AsyncClient(timeout=10) as c:
        r = await c.get("https://loving-serenity-production-2635.up.railway.app/context/ss.backfill_cost_prices.result")
    if r.status_code != 200:
        return JSONResponse({"done": False, "message": "아직 결과 없음"})
    raw = r.json()
    val = raw.get("value", "{}")
    return JSONResponse(json.loads(val) if isinstance(val, str) else val)


_SALE_SCAN_CACHE: dict = {}

async def _run_sale_products_scan():
    global _SALE_SCAN_CACHE
    _SALE_SCAN_CACHE = {"status": "running", "phase": 1, "found": 0}
    import httpx as _hx
    from datetime import datetime as _dt, timezone as _tz

    headers = await naver_api._headers()
    now = _dt.now(_tz.utc)
    sale_nos: list = []

    # Phase 1: SALE 상품 ID만 수집 (toDate=2099-12-31 — 오늘날짜 버그 우회)
    async with _hx.AsyncClient(timeout=30) as c:
        page = 1
        while True:
            r = await c.post(
                f"{NAVER_BASE}/v1/products/search", headers=headers,
                json={
                    "productStatusTypes": ["SALE"],
                    "page": page, "size": 50,
                    "orderType": "NO",
                    "periodType": "PROD_REG_DAY",
                    "fromDate": "2020-01-01",
                    "toDate": "2099-12-31",
                }
            )
            if r.status_code != 200:
                print(f"[SALE_SCAN] 검색 실패 {r.status_code}: {r.text[:200]}", flush=True)
                break
            data = r.json()
            contents = data.get("contents", [])
            total_els = data.get("totalElements", 0)
            if page == 1:
                print(f"[SALE_SCAN] totalElements={total_els}", flush=True)
            if not contents:
                break
            for item in contents:
                no = item.get("originProductNo")
                if no:
                    sale_nos.append(no)
            if len(contents) < 50:
                break
            page += 1
            await asyncio.sleep(0.2)

    _SALE_SCAN_CACHE.update({"phase": 2, "found": len(sale_nos)})
    print(f"[SALE_SCAN] Phase1 완료 — SALE 상품 {len(sale_nos)}개", flush=True)

    # Phase 2: SALE 상품만 detail 조회 (costPrice 포함)
    results: list[dict] = []
    async with _hx.AsyncClient(timeout=30) as c:
        async def _fetch(no):
            try:
                dr = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{no}", headers=headers, timeout=15)
                if dr.status_code != 200:
                    return None
                origin = dr.json().get("originProduct", {}) or {}
                img = ((origin.get("images") or {}).get("representativeImage") or {}).get("url", "")
                dg = ((origin.get("detailAttribute") or {}).get("sellerCodeInfo") or {}).get("sellerManagementCode", "") or ""
                return {
                    "origin_no": str(no),
                    "name": (origin.get("name") or "")[:60],
                    "sale_price": int(origin.get("salePrice") or 0),
                    "cost_price": int(origin.get("costPrice") or 0),
                    "status": origin.get("statusType", "SALE"),
                    "dg_code": dg,
                    "image": img,
                }
            except Exception:
                return None

        for i in range(0, len(sale_nos), 5):
            chunk = sale_nos[i:i+5]
            chunk_res = await asyncio.gather(*[_fetch(no) for no in chunk])
            results.extend([r for r in chunk_res if r])
            await asyncio.sleep(0.3)

    await _cs_save("ss.sale_products_with_cost", results)
    _SALE_SCAN_CACHE = {"status": "done", "count": len(results)}
    print(f"[SALE_SCAN] 완료 — {len(results)}개 저장", flush=True)


@app.post("/sale-products-scan")
async def sale_products_scan(background_tasks: BackgroundTasks):
    """SALE 상품 전체 + costPrice 스캔 (백그라운드). 결과: /sale-products-scan-result"""
    if _SALE_SCAN_CACHE.get("status") == "running":
        return JSONResponse({"status": "already_running", **_SALE_SCAN_CACHE})
    background_tasks.add_task(_run_sale_products_scan)
    return JSONResponse({"status": "started", "result_url": "/sale-products-scan-result"})


@app.get("/sale-products-scan-result")
async def sale_products_scan_result():
    """sale-products-scan 결과 반환. done 전엔 status 반환."""
    if _SALE_SCAN_CACHE.get("status") != "done":
        return JSONResponse(_SALE_SCAN_CACHE or {"status": "not_started"})
    import httpx as _hx
    async with _hx.AsyncClient(timeout=10) as c:
        r = await c.get("https://loving-serenity-production-2635.up.railway.app/context/ss.sale_products_with_cost")
    if r.status_code != 200:
        return JSONResponse({"status": "no_data"})
    raw = r.json()
    val = raw.get("value", "[]")
    return JSONResponse({"status": "done", "products": json.loads(val) if isinstance(val, str) else val})


# ─── 전체 마진 스캔 (DG API 실시간 도매가 기반, resume 지원) ─────────────────

_MARGIN_SCAN_STATE: dict = {"status": "idle"}
_CS_MARGIN_KEY = "ss.margin_scan_checkpoint"
_CS_MARGIN_RESULT = "ss.margin_scan_result"
_CS_BASE = "https://loving-serenity-production-2635.up.railway.app"


async def _margin_cs_get(key: str):
    import httpx as _hx
    try:
        async with _hx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{_CS_BASE}/context/{key}")
        if r.status_code == 200:
            val = r.json().get("value")
            return json.loads(val) if isinstance(val, str) else val
    except Exception:
        pass
    return None


async def _margin_cs_save(key: str, value: dict):
    import httpx as _hx
    try:
        async with _hx.AsyncClient(timeout=10) as c:
            await c.post(f"{_CS_BASE}/context",
                         json={"key": key, "value": json.dumps(value), "category": "audit"})
    except Exception as _e:
        print(f"[MARGIN_SCAN] context_store 저장 실패 {key}: {_e}", flush=True)


async def _run_margin_scan_bg(
    resume_from: int = 0,
    resume_nos: list | None = None,
    resume_counts: dict | None = None,
    resume_negative: list | None = None,
):
    """전체 SALE 상품 마진 스캔. DG API 실시간 도매가로 역마진 감지 → 즉시 판매중지."""
    import httpx as _hx, asyncio as _aio, time as _time
    from main import NAVER_BASE, _get_dg_wholesale
    global _MARGIN_SCAN_STATE

    _MARGIN_SCAN_STATE = {
        "status": "running", "total": 0, "scanned": 0,
        "margin_safe": 0, "negative_margin": 0, "no_dg_code": 0,
        "errors": 0, "suspended": 0, "done": False,
    }
    if resume_counts:
        _MARGIN_SCAN_STATE.update({k: resume_counts.get(k, 0) for k in
            ("scanned", "margin_safe", "negative_margin", "no_dg_code", "errors", "suspended")})

    hdrs = await naver_api._headers()

    # Phase 1: SALE 상품 origin_no 전체 수집
    if resume_nos:
        all_nos = list(resume_nos)
        print(f"[MARGIN_SCAN] 재개: {resume_from}/{len(all_nos)}번째부터", flush=True)
    else:
        all_nos = []
        page = 1
        while True:
            try:
                async with _hx.AsyncClient(timeout=20) as c:
                    r = await c.post(
                        f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                        json={
                            "productStatusTypes": ["SALE"],
                            "page": page, "size": 100,
                            "orderType": "NO",
                            "periodType": "PROD_REG_DAY",
                            "fromDate": "2020-01-01",
                            "toDate": "2030-12-31",
                        }
                    )
                data = r.json()
                contents = data.get("contents", [])
                if page == 1:
                    print(f"[MARGIN_SCAN] Phase1: totalElements={data.get('totalElements', 0)}", flush=True)
                if not contents:
                    break
                all_nos.extend(str(p.get("originProductNo", "") or "") for p in contents)
                if len(contents) < 100:
                    break
                page += 1
                await _aio.sleep(0.5)
            except Exception as e:
                print(f"[MARGIN_SCAN] Phase1 오류 page={page}: {e}", flush=True)
                break
        print(f"[MARGIN_SCAN] Phase1 완료: {len(all_nos)}개 수집", flush=True)

    _MARGIN_SCAN_STATE["total"] = len(all_nos)
    negative_items: list = list(resume_negative or [])

    # Phase 2: 상품별 도매가 조회 + 마진 판정
    for scan_idx, origin_no in enumerate(all_nos):
        if scan_idx < resume_from:
            continue
        if not origin_no:
            _MARGIN_SCAN_STATE["errors"] += 1
            continue

        # Naver 상세 조회
        try:
            async with _hx.AsyncClient(timeout=15) as c:
                rd = await c.get(
                    f"{NAVER_BASE}/v2/products/origin-products/{origin_no}", headers=hdrs,
                )
            if rd.status_code == 429:
                print(f"[MARGIN_SCAN] Naver 429 — 5분 대기", flush=True)
                await _aio.sleep(300)
                async with _hx.AsyncClient(timeout=15) as c:
                    rd = await c.get(
                        f"{NAVER_BASE}/v2/products/origin-products/{origin_no}", headers=hdrs,
                    )
            if rd.status_code != 200:
                _MARGIN_SCAN_STATE["errors"] += 1
                await _aio.sleep(1)
                continue
            origin = rd.json().get("originProduct", {}) or {}
        except Exception as e:
            print(f"[MARGIN_SCAN] Naver 조회 실패 {origin_no}: {e}", flush=True)
            _MARGIN_SCAN_STATE["errors"] += 1
            continue

        sale_price = int(origin.get("salePrice") or 0)
        prod_name = (origin.get("name") or "")[:50]
        dg_code = str(((origin.get("detailAttribute") or {}).get("sellerCodeInfo") or {})
                       .get("sellerManagementCode") or "").strip()

        if not dg_code.startswith("DG_"):
            _MARGIN_SCAN_STATE["no_dg_code"] += 1
            _MARGIN_SCAN_STATE["scanned"] += 1
            await _aio.sleep(0.3)
        else:
            wholesale = await _get_dg_wholesale(dg_code)
            await _aio.sleep(3)  # DG API 차단 방지

            if wholesale <= 0:
                _MARGIN_SCAN_STATE["no_dg_code"] += 1
                _MARGIN_SCAN_STATE["scanned"] += 1
            else:
                floor_price = int(wholesale * 1.15)
                margin = sale_price - floor_price
                if margin > 0:
                    _MARGIN_SCAN_STATE["margin_safe"] += 1
                    _MARGIN_SCAN_STATE["scanned"] += 1
                    print(f"[MARGIN_SCAN] ✓ {prod_name} 판매{sale_price:,} 도매{wholesale:,} 마진+{margin:,}", flush=True)
                else:
                    _MARGIN_SCAN_STATE["negative_margin"] += 1
                    _MARGIN_SCAN_STATE["scanned"] += 1
                    neg_item = {
                        "origin_no": origin_no, "name": prod_name,
                        "sale_price": sale_price, "wholesale": wholesale,
                        "floor_price": floor_price, "margin": margin,
                    }
                    negative_items.append(neg_item)
                    print(f"[MARGIN_SCAN] ⚠ 역마진 {prod_name} 판매{sale_price:,} 도매{wholesale:,} "
                          f"마진{margin:,} → 판매중지", flush=True)
                    try:
                        ok = await naver_api.set_product_status(origin_no, "SUSPENSION")
                        if ok:
                            _MARGIN_SCAN_STATE["suspended"] += 1
                            print(f"[MARGIN_SCAN] 판매중지 완료 [{origin_no}]", flush=True)
                        else:
                            print(f"[MARGIN_SCAN] 판매중지 실패(non-200) [{origin_no}]", flush=True)
                    except Exception as e:
                        print(f"[MARGIN_SCAN] 판매중지 오류 {origin_no}: {e}", flush=True)

        # 20개마다 체크포인트 저장
        if (scan_idx + 1) % 20 == 0:
            await _margin_cs_save(_CS_MARGIN_KEY, {
                "last_idx": scan_idx, "all_nos": all_nos,
                "counts": {k: _MARGIN_SCAN_STATE[k] for k in
                           ("scanned","margin_safe","negative_margin","no_dg_code","errors","suspended")},
                "negative_items": negative_items,
            })
            s = _MARGIN_SCAN_STATE
            print(f"[MARGIN_SCAN] 체크포인트 {scan_idx+1}/{len(all_nos)} "
                  f"안전:{s['margin_safe']} 역마진:{s['negative_margin']} "
                  f"DG없음:{s['no_dg_code']}", flush=True)

    # 완료 — 최종 결과 저장
    dg_priced = _MARGIN_SCAN_STATE["margin_safe"] + _MARGIN_SCAN_STATE["negative_margin"]
    margin_safe_pct = round(_MARGIN_SCAN_STATE["margin_safe"] / dg_priced * 100, 1) if dg_priced > 0 else 0
    result = {
        "total": _MARGIN_SCAN_STATE["total"],
        "scanned": _MARGIN_SCAN_STATE["scanned"],
        "margin_safe": _MARGIN_SCAN_STATE["margin_safe"],
        "negative_margin": _MARGIN_SCAN_STATE["negative_margin"],
        "no_dg_code": _MARGIN_SCAN_STATE["no_dg_code"],
        "errors": _MARGIN_SCAN_STATE["errors"],
        "suspended": _MARGIN_SCAN_STATE["suspended"],
        "dg_with_price_count": dg_priced,
        "margin_safe_pct": margin_safe_pct,
        "negative_items": negative_items,
    }
    await _margin_cs_save(_CS_MARGIN_RESULT, result)
    await _margin_cs_save(_CS_MARGIN_KEY, {"done": True})
    _MARGIN_SCAN_STATE.update({"status": "done", "done": True, **result})
    print(f"[MARGIN_SCAN] 완료 — 전체:{result['total']} 안전:{result['margin_safe']} "
          f"역마진:{result['negative_margin']}(판매중지:{result['suspended']}) "
          f"DG없음:{result['no_dg_code']} DG보유 마진안전율:{margin_safe_pct}%", flush=True)


@app.post("/margin-scan")
async def margin_scan_start(background_tasks: BackgroundTasks, force: bool = False):
    """전체 SALE 상품 마진 스캔 시작 (백그라운드). 미완료 체크포인트 있으면 자동 재개.
    ?force=true 시 체크포인트 무시하고 처음부터."""
    if _MARGIN_SCAN_STATE.get("status") == "running" and not force:
        s = _MARGIN_SCAN_STATE
        return JSONResponse({"status": "already_running",
                             "progress": f"{s.get('scanned',0)}/{s.get('total',0)}",
                             "margin_safe": s.get("margin_safe", 0),
                             "negative_margin": s.get("negative_margin", 0),
                             "no_dg_code": s.get("no_dg_code", 0),
                             "suspended": s.get("suspended", 0)})
    ckpt = await _margin_cs_get(_CS_MARGIN_KEY)
    if ckpt and not ckpt.get("done") and not force:
        last_idx = ckpt.get("last_idx", 0)
        background_tasks.add_task(
            _run_margin_scan_bg,
            resume_from=last_idx + 1,
            resume_nos=ckpt.get("all_nos"),
            resume_counts=ckpt.get("counts"),
            resume_negative=ckpt.get("negative_items"),
        )
        return JSONResponse({"status": "resumed", "resume_from": last_idx + 1,
                             "total_nos": len(ckpt.get("all_nos") or [])})
    background_tasks.add_task(_run_margin_scan_bg)
    return JSONResponse({"status": "started"})


@app.get("/margin-scan-status")
async def margin_scan_status():
    """마진 스캔 진행 상황 (실시간)."""
    s = _MARGIN_SCAN_STATE
    status = s.get("status", "idle")
    if status == "running":
        total = s.get("total", 0)
        scanned = s.get("scanned", 0)
        pct = round(scanned / total * 100, 1) if total > 0 else 0
        return JSONResponse({
            "status": "running",
            "progress": f"{scanned}/{total} ({pct}%)",
            "margin_safe": s.get("margin_safe", 0),
            "negative_margin": s.get("negative_margin", 0),
            "no_dg_code": s.get("no_dg_code", 0),
            "suspended": s.get("suspended", 0),
            "errors": s.get("errors", 0),
        })
    if status == "done":
        return JSONResponse({"status": "done", **{k: s.get(k) for k in
            ("total","scanned","margin_safe","negative_margin","no_dg_code",
             "suspended","errors","margin_safe_pct","dg_with_price_count")}})
    return JSONResponse({"status": status})


@app.get("/margin-scan-result")
async def margin_scan_result_ep():
    """마진 스캔 최종 결과 (context_store). 역마진 상품 목록 포함."""
    data = await _margin_cs_get(_CS_MARGIN_RESULT)
    if not data:
        return JSONResponse({"status": "no_result"}, status_code=404)
    return JSONResponse({"status": "done", **data})


@app.get("/price-ratio-scan")
async def price_ratio_scan(ratio_min: float = 2.0, ratio_max: float = 2.3, check_market: bool = False):
    """가격 비율 스캔 (읽기 전용). 가격/원가 비율이 ratio_min~ratio_max 인 상품 목록 반환.
    check_market=true 시 해당 상품 네이버쇼핑 최저가 추가 조회."""
    from main import search_naver_shopping
    flagged = []
    all_products: list[dict] = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(0.3)

    skipped_no_cost = 0
    skipped_not_sale = 0
    for prod in all_products:
        origin = prod.get("originProduct", {})
        if origin.get("statusType") != "SALE":
            skipped_not_sale += 1
            continue
        sale_price = int(origin.get("salePrice") or 0)
        cost_price = int(origin.get("costPrice") or 0)
        if sale_price <= 0 or cost_price <= 0:
            skipped_no_cost += 1
            continue
        ratio = round(sale_price / cost_price, 3)
        if ratio_min <= ratio <= ratio_max:
            entry = {
                "product_no": str(prod.get("originProductNo", "")),
                "name": origin.get("name", "")[:60],
                "sale_price": sale_price,
                "cost_price": cost_price,
                "ratio": ratio,
            }
            if check_market:
                items = await search_naver_shopping(origin.get("name", "")[:20], display=10)
                prices = [it["price"] for it in (items or []) if it.get("price", 0) > 0]
                if prices:
                    market_min = min(prices)
                    entry["market_min"] = market_min
                    entry["vs_market"] = f"+{round((sale_price/market_min-1)*100,1)}%" if sale_price > market_min else f"{round((sale_price/market_min-1)*100,1)}%"
                else:
                    entry["market_min"] = None
                    entry["vs_market"] = "검색없음"
                await asyncio.sleep(0.5)
            flagged.append(entry)

    flagged.sort(key=lambda x: x["ratio"], reverse=True)
    return JSONResponse({
        "total_scanned": len(all_products),
        "skipped_not_sale": skipped_not_sale,
        "skipped_no_cost": skipped_no_cost,
        "flagged_count": len(flagged),
        "ratio_range": f"{ratio_min}~{ratio_max}",
        "flagged": flagged,
    })


async def _run_overpriced_scan(threshold: float = 1.10):
    """백그라운드: SALE 상품 전체 네이버쇼핑 시세 비교 → context_store 저장."""
    from main import search_naver_shopping
    import httpx as _hx
    print("[OVERPRICED] 스캔 시작", flush=True)
    all_products: list[dict] = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(0.3)

    sale_products = [p for p in all_products if p.get("originProduct", {}).get("statusType") == "SALE"]
    overpriced = []
    competitive = 0
    no_result = 0
    for prod in sale_products:
        origin = prod.get("originProduct", {})
        sale_price = int(origin.get("salePrice") or 0)
        cost_price = int(origin.get("costPrice") or 0)
        if sale_price <= 0:
            continue
        name = origin.get("name", "")
        items = await search_naver_shopping(name[:20], display=10)
        prices = [it["price"] for it in (items or []) if it.get("price", 0) > 0]
        if not prices:
            no_result += 1
            await asyncio.sleep(0.5)
            continue
        market_min = min(prices)
        ratio_vs_market = round(sale_price / market_min, 3)
        if ratio_vs_market > threshold:
            overpriced.append({
                "product_no": str(prod.get("originProductNo", "")),
                "name": name[:60],
                "sale_price": sale_price,
                "cost_price": cost_price,
                "market_min": market_min,
                "ratio_vs_market": ratio_vs_market,
                "overprice_pct": f"+{round((ratio_vs_market-1)*100,1)}%",
            })
        else:
            competitive += 1
        await asyncio.sleep(0.5)

    overpriced.sort(key=lambda x: x["ratio_vs_market"], reverse=True)
    result = {
        "sale_total": len(sale_products),
        "overpriced_count": len(overpriced),
        "competitive_count": competitive,
        "no_market_data": no_result,
        "threshold": f"최저가 × {threshold}",
        "overpriced": overpriced,
        "done": True,
    }
    try:
        async with _hx.AsyncClient(timeout=10) as c:
            await c.post(
                "https://loving-serenity-production-2635.up.railway.app/context",
                json={"key": "ss.overpriced_scan.result", "value": json.dumps(result, ensure_ascii=False), "category": "audit"},
            )
    except Exception as e:
        print(f"[OVERPRICED] context_store 저장 실패: {e}", flush=True)
    print(f"[OVERPRICED] 완료 — 고가:{len(overpriced)} 경쟁력:{competitive} 시세없음:{no_result}", flush=True)


@app.get("/overpriced-scan")
async def overpriced_scan(threshold: float = 1.10):
    """SALE 상품 전체를 네이버쇼핑 최저가와 비교 (백그라운드). 결과는 /overpriced-scan-result 로 조회."""
    asyncio.create_task(_run_overpriced_scan(threshold=threshold))
    return JSONResponse({"status": "started", "threshold": threshold, "message": "결과는 /overpriced-scan-result 에서 조회"})


@app.get("/overpriced-scan-result")
async def overpriced_scan_result():
    """overpriced_scan 결과 조회 (context_store)."""
    import httpx as _hx
    async with _hx.AsyncClient(timeout=10) as c:
        r = await c.get("https://loving-serenity-production-2635.up.railway.app/context/ss.overpriced_scan.result")
    if r.status_code != 200:
        return JSONResponse({"done": False, "message": "아직 결과 없음 (스캔 진행 중 또는 미실행)"})
    raw = r.json()
    val = raw.get("value", "{}")
    return JSONResponse(json.loads(val) if isinstance(val, str) else val)


@app.post("/batch-price-fix")
async def batch_price_fix(items: list[dict]):
    """배치 가격 수정 (1개씩 순차). items=[{"product_no":"...", "new_price":N}, ...]
    new_price는 시장최저가×1.05 기준으로 호출자가 계산해 넘김."""
    results = []
    for item in items:
        product_no = str(item.get("product_no", ""))
        new_price   = int(item.get("new_price", 0))
        if not product_no or new_price <= 0:
            results.append({"product_no": product_no, "ok": False, "error": "invalid params"})
            continue
        # ⛔ 절대규칙: 도매가(wholesale_price) 없으면 가격 변경 불가
        wholesale = int(item.get("wholesale_price", 0))
        if wholesale <= 0:
            results.append({"product_no": product_no, "ok": False,
                            "error": "⛔ 절대규칙 위반: wholesale_price 미제공 — 도매가 없이 가격 변경 불가"})
            print(f"[BATCH-PRICE] ⛔ SKIP {product_no} — wholesale_price 미제공", flush=True)
            continue
        floor = int(round(wholesale * 1.15 / 10) * 10)
        if new_price < floor:
            results.append({"product_no": product_no, "ok": False,
                            "error": f"⛔ floor 미달: 새판매가₩{new_price:,} < floor₩{floor:,}(도매가₩{wholesale:,}×1.15)"})
            print(f"[BATCH-PRICE] ⛔ floor 미달 {product_no}: {new_price:,} < {floor:,}", flush=True)
            continue
        # Naver API 직접 조회 (빠름)
        import httpx as _hx
        from main import NAVER_BASE
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{product_no}", headers=headers)
        if r.status_code != 200:
            results.append({"product_no": product_no, "ok": False, "error": f"Naver 조회 실패 HTTP {r.status_code}"})
            continue
        target_origin = dict(r.json().get("originProduct", {}))

        if not target_origin:
            results.append({"product_no": product_no, "ok": False, "error": "상품 미발견"})
            continue

        old_price = target_origin.get("salePrice", 0)
        target_origin["salePrice"] = new_price
        ok, err = await naver_api.update_product(product_no, target_origin)
        results.append({
            "product_no": product_no,
            "name": target_origin.get("name", "")[:40],
            "old_price": old_price,
            "new_price": new_price,
            "ok": ok,
            "error": err if not ok else "",
        })
        print(f"[BATCH-PRICE] {'✅' if ok else '❌'} {target_origin.get('name','')[:30]} {old_price:,}→{new_price:,}", flush=True)
        await asyncio.sleep(1.5)

    ok_count = sum(1 for r in results if r["ok"])
    return JSONResponse({"ok_count": ok_count, "fail_count": len(results) - ok_count, "results": results})


@app.get("/verify-margin-result")
async def verify_margin_result():
    """verify-margin 결과 조회 (context_store)."""
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get("https://loving-serenity-production-2635.up.railway.app/context/ss.verify_margin.result")
        if r.status_code == 200:
            val = r.json().get("value")
            return JSONResponse(json.loads(val) if isinstance(val, str) else (val or {"status": "no_result"}))
    return JSONResponse({"status": "error"})


async def _run_verify_margin():
    """22개 가격 수정 상품의 도매가 대비 안전성 검증 (백그라운드).
    Naver에서 sellerManagementCode 조회 → DG API 도매가 → floor(×1.15) vs 현재 판매가."""
    import httpx as _hx
    from main import NAVER_BASE, DOMEGGOOK_API_KEY, DOMEGGOOK_API_URL

    FLOOR = 1.15
    OLD_MARKUP = 2.2
    DG_URL = DOMEGGOOK_API_URL or "https://domeggook.com/ssl/api/"
    DG_KEY = DOMEGGOOK_API_KEY

    # ranks 54~65 (확정 데이터)
    items_54_65 = [
        (54, 13574289110, "USB충전식 초소형 선풍기 클립형", 23900, 10290),
        (55, 13564329492, "진주목걸이 DIY 직접 캐기",      21900,  9490),
        (56, 13563141895, "360도 회전스프링클러 정원용",    13900,  6090),
        (57, 13563140296, "3구 회전 스프링클러 텃밭용",      6900,  3030),
        (58, 13571387436, "에어컨 실외기 커버 53cm",        19900,  9340),
        (59, 13564087260, "비즈 DIY 만들기 세트 24색",      12900,  6200),
        (60, 13562419805, "2단 알루미늄 노트북 거치대",      17900,  8820),
        (61, 13572349804, "사각체크 여행용 세면도구 7종",    19900,  9960),
        (62, 13562417539, "공룡스티커 50장",                 4490,  2260),
        (63, 13566962206, "원형 대형 텃밭 화분",             19800,  9980),
        (64, 13574515999, "USB충전식 초경량 선풍기",         10400,  5340),
        (65, 13562453526, "메쉬 벨트백",                     9900,  5120),
    ]
    all_items = [{"rank": r, "pno": pno, "name": name,
                  "old_price": op, "new_price": np}
                 for r, pno, name, op, np in items_54_65]

    # ranks 66~75 context_store에서 보충
    try:
        async with _hx.AsyncClient(timeout=15) as c:
            r = await c.get("https://loving-serenity-production-2635.up.railway.app/context/ss.overpriced_scan.result")
            if r.status_code == 200:
                val = r.json().get("value")
                if isinstance(val, str):
                    val = json.loads(val)
                for it in (val.get("items") or val.get("overpriced") or []):
                    rk = it.get("rank", 0)
                    if 66 <= rk <= 75:
                        pno = it.get("product_no") or it.get("productNo") or it.get("origin_product_no")
                        old_p = it.get("current_price") or it.get("salePrice") or it.get("old_price")
                        mmin = it.get("market_min") or it.get("naver_min")
                        new_p = (int(round(mmin * 1.05 / 10) * 10) if mmin
                                 else it.get("new_price") or it.get("target_price"))
                        all_items.append({"rank": rk, "pno": int(pno) if pno else 0,
                                          "name": it.get("name", ""),
                                          "old_price": old_p, "new_price": new_p})
    except Exception as e:
        print(f"[verify-margin] context_store 조회 실패: {e}", flush=True)

    all_items.sort(key=lambda x: x["rank"])
    headers = await naver_api._headers()
    results = []

    for it in all_items:
        pno = it["pno"]
        new_p = it["new_price"]
        old_p = it["old_price"]
        if not pno or not new_p:
            results.append({**it, "status": "skip", "reason": "데이터 없음"})
            continue

        # Naver → sellerManagementCode + 현재 판매가 확인
        dg_code = ""
        current_sale = new_p
        try:
            async with _hx.AsyncClient(timeout=15) as c:
                r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{pno}", headers=headers)
            if r.status_code == 200:
                origin = r.json().get("originProduct", {})
                dg_code = origin.get("sellerManagementCode", "")
                current_sale = origin.get("salePrice", new_p)
        except Exception as e:
            print(f"[verify-margin] Naver {pno}: {e}", flush=True)

        # DG API 도매가
        wholesale = 0
        source = ""
        if DG_KEY and dg_code:
            try:
                async with _hx.AsyncClient(timeout=10) as c:
                    r = await c.get(DG_URL, params={
                        "aid": DG_KEY, "cmd": "getItemView",
                        "oid": dg_code, "outType": "json"})
                if r.status_code == 200:
                    item_data = r.json().get("item") or {}
                    for field in ["price", "minPrice", "salePrice", "consumerPrice"]:
                        val = item_data.get(field)
                        if val:
                            wholesale = int(str(val).replace(",", ""))
                            source = f"DG({dg_code})"
                            break
            except Exception as e:
                print(f"[verify-margin] DG {dg_code}: {e}", flush=True)

        if wholesale == 0 and old_p:
            wholesale = int(old_p / OLD_MARKUP)
            source = f"추정(구가{old_p:,}÷{OLD_MARKUP})"

        floor = int(wholesale * FLOOR)
        gap = current_sale - floor
        safe = current_sale >= floor

        results.append({
            "rank": it["rank"],
            "name": it["name"],
            "pno": pno,
            "dg_code": dg_code,
            "wholesale": wholesale,
            "floor": floor,
            "current_sale": current_sale,
            "old_price": old_p,
            "gap": gap,
            "safe": safe,
            "source": source,
        })
        print(f"[verify-margin] rank{it['rank']:>3} {'✅' if safe else '❌'} "
              f"도매:{wholesale:,} floor:{floor:,} 현재:{current_sale:,} gap:{gap:+,} [{source}]", flush=True)
        await asyncio.sleep(0.3)

    danger = [r for r in results if not r.get("safe") and r.get("status") != "skip"]
    safe_list = [r for r in results if r.get("safe")]
    result = {
        "total": len(results),
        "safe": len(safe_list),
        "danger": len(danger),
        "items": results,
        "danger_items": danger,
        "done": True,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            await c.post(
                "https://loving-serenity-production-2635.up.railway.app/context",
                json={"key": "ss.verify_margin.result",
                      "value": json.dumps(result, ensure_ascii=False),
                      "category": "audit"},
            )
        print("[verify-margin] 결과 context_store 저장 완료", flush=True)
    except Exception as e:
        print(f"[verify-margin] context_store 저장 실패: {e}", flush=True)


@app.get("/verify-margin")
async def verify_margin_trigger():
    """22개 가격 수정 상품 안전성 검증 시작 (백그라운드). 결과는 /verify-margin-result 에서 조회."""
    asyncio.create_task(_run_verify_margin())
    return JSONResponse({"status": "started", "result_url": "/verify-margin-result"})


@app.post("/daily-price-check")
async def daily_price_check_ss(force: bool = False):
    """일일 가격비교 즉시 실행 (동기 대기, 결과 반환). force=true 시 오늘 중복 실행 허용."""
    from main import _run_daily_price_check_ss
    result = await _run_daily_price_check_ss(limit=7, force=force)
    return JSONResponse(result)


@app.get("/price-check")
def price_check(wholesale_price: int):
    selling = calculate_selling_price(wholesale_price)
    margin = float(os.environ.get("MARGIN_RATE", "0.15"))
    return {"wholesale_price": wholesale_price, "selling_price": selling,
            "margin_rate": f"{margin * 100:.0f}%", "profit": selling - wholesale_price}


@app.get("/naver-dynamic-price")
async def naver_dynamic_price(keyword: str, wholesale_price: int):
    """네이버쇼핑 경쟁가 기반 AI 동적가격 계산 (도매꾹 소싱용)
    target = min_competitor × 1.05, floor = wholesale × 1.15
    final  = max(target, floor), round to 10원
    """
    from main import search_naver_shopping
    items = await search_naver_shopping(keyword, display=20)
    if not items:
        floor = int(wholesale_price * 1.15)
        final = int(round(floor / 10) * 10)
        return {"keyword": keyword, "wholesale_price": wholesale_price,
                "competitor_min": None, "target": None,
                "floor": floor, "final_price": final,
                "note": "네이버쇼핑 검색 결과 없음 → floor 적용"}
    prices = [it["price"] for it in items if it.get("price", 0) > 0]
    min_price = min(prices)
    target = int(min_price * 1.05)
    floor  = int(wholesale_price * 1.15)
    final  = int(round(max(target, floor) / 10) * 10)
    return {
        "keyword": keyword, "wholesale_price": wholesale_price,
        "competitor_min": min_price, "competitor_count": len(prices),
        "target": target, "floor": floor,
        "final_price": final,
        "applied": "target" if target >= floor else "floor",
        "top5": sorted(prices)[:5],
    }


# ─── AI 직원단 엔드포인트 ──────────────────────────────────────────────────────

@app.get("/season-plan")
async def season_plan():
    """📅 시즌 기획자 — 다가오는 이벤트 & 소싱 키워드"""
    return JSONResponse(employee_season_planner())


@app.get("/trend-scout")
async def trend_scout():
    """📈 트렌드 스카우터 — 한국 실시간 트렌딩 키워드"""
    keywords = await employee_trend_scout()
    return JSONResponse({"trending": keywords, "count": len(keywords)})


@app.get("/daily-report")
async def daily_report():
    """📊 일일 종합 리포트 — 회계+시즌+트렌드"""
    orders = await naver_api.get_new_orders()
    accounting = await employee_accounting_manager(orders, MARGIN_RATE)
    season = employee_season_planner()
    trends = await employee_trend_scout()
    return JSONResponse({
        "accounting": accounting,
        "upcoming_events": season["upcoming"][:3],
        "trending_keywords": trends[:10],
    })


@app.post("/stock-alert")
async def stock_alert():
    """⚠️ 품절 방지 알림이 — 재고 부족 상품 체크"""
    files = sorted(Path(EXCEL_FOLDER).glob("*.xlsx"), key=lambda x: x.stat().st_mtime, reverse=True)
    if not files:
        return JSONResponse({"status": "no_excel"})
    products = parse_excel(str(files[0]))
    low_stock = employee_stock_guardian(products)
    return JSONResponse({"low_stock_count": len(low_stock), "items": low_stock[:20]})


@app.post("/error-audit")
async def error_audit(request: Request):
    """🔍 시스템 에러 감사원 — 에러 분석 & 해결책"""
    try:
        body = await request.json()
        errors = body.get("errors", [])
    except Exception:
        errors = []
    report = await employee_error_auditor(errors, ANTHROPIC_API_KEY)
    return JSONResponse({"report": report})


@app.post("/create-shortform")
async def create_shortform(request: Request):
    """🎬 숏폼 영상 제작자 — 상품 홍보 영상 제작 요청"""
    try:
        body = await request.json()
        product_name = body.get("product_name", "")
    except Exception:
        product_name = ""
    if not product_name:
        return JSONResponse({"status": "error", "message": "product_name 필요"}, status_code=400)
    result = await employee_shortform_creator(product_name)
    return JSONResponse(result)


@app.post("/write-blog")
async def write_blog(request: Request):
    """📝 블로그 포스팅 매니저 — 네이버 블로그 홍보글 생성"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    post = await employee_blog_manager(body, ANTHROPIC_API_KEY)
    return JSONResponse({"post": post})


@app.get("/review-analysis")
async def review_analysis(product_name: str):
    """⭐ 리뷰 분석가 — Pain Point & 셀링포인트 분석"""
    result = await employee_review_analyst(product_name, ANTHROPIC_API_KEY)
    return JSONResponse(result)


@app.post("/ad-analysis")
async def ad_analysis(request: Request):
    """💰 광고 효율 분석가 — ROAS 계산 & 입찰가 조정"""
    try:
        body = await request.json()
        ad_cost = int(body.get("ad_cost", 0))
    except Exception:
        ad_cost = 0
    orders = await naver_api.get_new_orders()
    result = await employee_ad_analyst(orders, ad_cost, ANTHROPIC_API_KEY)
    return JSONResponse(result)


@app.get("/event-manager")
async def event_manager():
    """🎉 이벤트 매니저 — 프로모션/알림 문구 자동 생성"""
    result = await employee_event_manager(ANTHROPIC_API_KEY)
    return JSONResponse(result)


@app.post("/expand-platform")
async def expand_platform(request: Request):
    """🌐 플랫폼 확장 전문가 — 타 플랫폼 상품정보 변환"""
    try:
        body = await request.json()
        product = body.get("product", {})
        platform = body.get("platform", "쿠팡")
    except Exception:
        return JSONResponse({"status": "error"}, status_code=400)
    result = await employee_platform_expander(product, platform, ANTHROPIC_API_KEY)
    return JSONResponse(result)


# ─── 상품 관리 ────────────────────────────────────────────────────────────────

@app.get("/list-products")
async def list_products(page: int = 1, size: int = 50):
    """📦 등록된 상품 목록 조회 — 최신 도매꾹 신규 상품만 반환 (오너클랜 구 상품 제외)"""
    try:
        new_product_days = int(os.environ.get("NEW_PRODUCT_DAYS", "90"))
        cutoff = datetime.now(timezone.utc) - timedelta(days=new_product_days)

        result = await naver_api.list_products(page, size, days=new_product_days)
        products = result.get("contents", [])

        def _sort_key(p: dict) -> tuple:
            # (regDate 파싱값, originProductNo) 내림차순 — regDate 없으면 현재시각(신규 상품)
            raw = p.get("originProduct", {}).get("regDate", "")
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00")) if raw else datetime.now(timezone.utc)
            except Exception:
                dt = datetime.now(timezone.utc)
            return (dt, int(p.get("originProductNo") or 0))

        def _reg_dt(p: dict) -> datetime:
            return _sort_key(p)[0]

        # cutoff 이후 등록된 신규 상품만 선택 후 최신순 정렬 (ID 보조키)
        # 아직 도매꾹 상품이 없을 경우 전체 상품 최신순(ID 기준)으로 폴백
        filtered = sorted(
            [p for p in products if _reg_dt(p) >= cutoff],
            key=_sort_key,
            reverse=True,
        )
        if not filtered:
            result_all = await naver_api.list_products(page, size, days=365)
            filtered = sorted(result_all.get("contents", []), key=_sort_key, reverse=True)

        return JSONResponse({
            "total": len(filtered),
            "page": page,
            "count": len(filtered),
            "products": [
                {
                    "id": p.get("originProductNo"),
                    "channel_no": (
                        (p.get("channelProducts") or [{}])[0].get("channelProductNo")
                        or p.get("channelProductNo")
                    ),
                    "name": p.get("originProduct", {}).get("name", ""),
                    "price": p.get("originProduct", {}).get("salePrice", 0),
                    "status": p.get("originProduct", {}).get("statusType", ""),
                    "stock": p.get("originProduct", {}).get("stockQuantity", 0),
                    "image": (
                        p.get("originProduct", {})
                        .get("images", {})
                        .get("representativeImage", {})
                        .get("url", "")
                    ),
                    "category": (
                        p.get("originProduct", {})
                        .get("detailAttribute", {})
                        .get("naverShoppingSearchInfo", {})
                        .get("categoryName", "")
                    ),
                    "regDate": p.get("originProduct", {}).get("regDate", ""),
                }
                for p in filtered
            ]
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/auto-cleanup")
async def auto_cleanup(request: Request):
    """저성과 상품 자동 판매중지 (수동 트리거 또는 월요일 자정 자동 실행)
    Body(선택): {"min_age_days": 30, "max_views": 100}
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    min_age_days = int(body.get("min_age_days", 30))
    max_views    = int(body.get("max_views", 100))
    from main import pipeline_auto_cleanup
    result = await pipeline_auto_cleanup(min_age_days=min_age_days, max_views=max_views)
    return JSONResponse(result)


async def _run_force_reduce(min_age_days: int, batch_limit: int):
    from main import naver_api, _retry
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=min_age_days)
    active_statuses = {"SALE", "SALE_WAIT", "OUT_OF_STOCK", ""}
    all_products, page = [], 1
    while True:
        try:
            resp = await _retry(lambda p=page: naver_api.list_products(page=p, size=100), retries=3, delay=5.0, label=f"force-reduce p{page}")
        except Exception:
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        if len(contents) < 100:
            break
        page += 1
    print(f"[FORCE-REDUCE] 전체: {len(all_products)}개", flush=True)
    deactivated, skipped, status_counts = 0, 0, {}
    for prod in all_products:
        origin = prod.get("originProduct", {})
        st = origin.get("statusType", "")
        status_counts[st] = status_counts.get(st, 0) + 1
        if st not in active_statuses:
            continue
        try:
            reg_date = datetime.fromisoformat(origin.get("regDate", "").replace("Z", "+00:00"))
        except Exception:
            skipped += 1
            continue
        if reg_date > cutoff:
            skipped += 1
            continue
        if deactivated >= batch_limit:
            break
        product_no = str(prod.get("originProductNo", ""))
        ok = await naver_api.set_product_status(product_no, "SUSPENSION")
        if ok:
            deactivated += 1
            print(f"[FORCE-REDUCE] ✅ [{product_no}] {origin.get('name','')[:20]} ({st}, {(now-reg_date).days}일 경과)", flush=True)
        else:
            skipped += 1
    print(f"[FORCE-REDUCE] 완료 — 중지:{deactivated} 스킵:{skipped} 상태분포:{status_counts}", flush=True)


@app.post("/force-reduce")
async def force_reduce(request: Request, background_tasks: BackgroundTasks):
    """한도 초과 시 강제 정리 — 나이 기준으로 SALE/SALE_WAIT/OUT_OF_STOCK 상품 일괄 판매중지 (백그라운드).
    Body: {"min_age_days": 30, "limit": 300}"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    min_age_days = int(body.get("min_age_days", 30))
    batch_limit  = int(body.get("limit", 300))
    background_tasks.add_task(_run_force_reduce, min_age_days, batch_limit)
    return JSONResponse({"status": "started", "min_age_days": min_age_days, "limit": batch_limit})


@app.get("/naver-product-count")
async def naver_product_count():
    """Naver 검색 API totalElements로 상태별 상품 수 즉시 조회."""
    from main import naver_api, NAVER_BASE
    from datetime import datetime, timezone
    import httpx
    headers = await naver_api._headers()
    now = datetime.now(timezone.utc)
    counts = {}
    async with httpx.AsyncClient(timeout=15) as c:
        for st, label in [("SALE","판매중"), ("SUSPENSION","판매중지"), ("WAIT","판매대기"), ("OUTOFSTOCK","품절")]:
            try:
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=headers,
                    json={"productStatusTypes": [st], "page": 1, "size": 1,
                          "orderType": "NO", "periodType": "PROD_REG_DAY",
                          "fromDate": "2020-01-01", "toDate": now.strftime("%Y-%m-%d")},
                )
                data = r.json()
                counts[label] = data.get("totalElements", data.get("total", -1))
            except Exception as e:
                counts[label] = f"오류: {e}"
    active_keys = ["판매중", "판매대기", "품절"]
    active_total = sum(counts[k] for k in active_keys if isinstance(counts.get(k), int))
    return JSONResponse({**counts, "활성합계_한도대상": active_total, "한도": 1000, "여유": 1000 - active_total})


async def _run_delete_blurry():
    """대표이미지 흐릿/소형 상품 백그라운드 삭제."""
    from main import naver_api, _retry, _check_image_quality
    import asyncio, httpx

    all_products, page = [], 1
    while True:
        try:
            resp = await _retry(lambda p=page: naver_api.list_products(page=p, size=50, days=3650), retries=3, delay=5.0, label=f"blurry-scan p{page}")
        except Exception as e:
            print(f"[BLURRY-DELETE] 목록 조회 실패 p{page}: {e}", flush=True)
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        print(f"[BLURRY-DELETE] 수집: {len(all_products)}개", flush=True)
        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(0.5)

    print(f"[BLURRY-DELETE] 전체 {len(all_products)}개 이미지 품질 검사 시작", flush=True)
    deleted = skipped = errors = 0

    for prod in all_products:
        try:
            origin    = prod.get("originProduct", {})
            images    = origin.get("images", {})
            rep_img   = images.get("representativeImage", {})
            img_url   = rep_img.get("url", "")
            product_no = str(prod.get("originProductNo", ""))
            name       = origin.get("name", "")[:30]

            if not img_url or not product_no:
                skipped += 1
                continue

            ok, reason, w, h = await _check_image_quality(img_url)
            if ok:
                skipped += 1
                continue

            # blurry / too_small / text_heavy → 삭제
            result = await naver_api.delete_product(product_no)
            if result:
                deleted += 1
                print(f"[BLURRY-DELETE] ✅ 삭제 [{product_no}] {name} | {reason} ({w}×{h})", flush=True)
            else:
                errors += 1
                print(f"[BLURRY-DELETE] ❌ 삭제 실패 [{product_no}] {name}", flush=True)

            await asyncio.sleep(0.3)
        except Exception as e:
            errors += 1
            print(f"[BLURRY-DELETE] 오류: {e}", flush=True)

    print(f"[BLURRY-DELETE] 완료 — 삭제:{deleted} 정상스킵:{skipped} 오류:{errors}", flush=True)


_blurry_status: dict = {"running": False, "deleted": 0, "skipped": 0, "errors": 0, "done": False}


async def _run_delete_all():
    """전체 상품 삭제 백그라운드 작업."""
    from main import naver_api, _retry
    import asyncio

    all_nos, page = [], 1
    print(f"[DELETE-ALL] 상품 목록 수집 시작", flush=True)
    while True:
        try:
            resp = await _retry(lambda p=page: naver_api.list_products(page=p, size=50, days=3650), retries=3, delay=5.0, label=f"delete-all p{page}")
        except Exception as e:
            print(f"[DELETE-ALL] 목록 조회 실패 p{page}: {e}", flush=True)
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        nos = [str(c.get("originProductNo", "")) for c in contents if c.get("originProductNo")]
        all_nos.extend(nos)
        print(f"[DELETE-ALL] 수집: {len(all_nos)}개", flush=True)
        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(0.5)

    print(f"[DELETE-ALL] 전체 {len(all_nos)}개 삭제 시작", flush=True)
    deleted = errors = 0

    for no in all_nos:
        try:
            result = await naver_api.delete_product(no)
            if result:
                deleted += 1
                if deleted % 50 == 0:
                    print(f"[DELETE-ALL] 진행 {deleted}/{len(all_nos)}", flush=True)
            else:
                errors += 1
                print(f"[DELETE-ALL] ❌ 삭제 실패 [{no}]", flush=True)
            await asyncio.sleep(0.2)
        except Exception as e:
            errors += 1
            print(f"[DELETE-ALL] 오류 [{no}]: {e}", flush=True)

    print(f"[DELETE-ALL] ✅ 완료 — 삭제:{deleted} 실패:{errors}", flush=True)


@app.post("/delete-all-products")
async def delete_all_products(background_tasks: BackgroundTasks):
    """전체 상품 삭제 (백그라운드). 진행: Railway 로그 [DELETE-ALL] 태그."""
    background_tasks.add_task(_run_delete_all)
    return JSONResponse({"status": "started", "message": "전체 상품 삭제 시작. Railway 로그에서 [DELETE-ALL] 태그 확인."})


@app.get("/products/keyword-scan")
async def products_keyword_scan(keywords: str = "방한,내복,발열,핫팩,난방,겨울,두꺼운,보온,히터,전기장판,온수매트,목도리,장갑,패딩,방풍,방설,스키,방한복,귀마개,워머"):
    """키워드 포함 상품 목록 조회 (삭제 없음). keywords=쉼표구분 쿼리파라미터."""
    kw_list = [k.strip() for k in keywords.split(",") if k.strip()]
    all_prods = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        for p in contents:
            product_no = str(p.get("originProductNo", ""))
            origin = p.get("originProduct", {})
            name = (origin.get("name") or "").strip()
            if product_no and name:
                all_prods.append({"product_no": product_no, "name": name})
        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(0.3)

    matched = [p for p in all_prods if any(kw in p["name"] for kw in kw_list)]
    return JSONResponse({
        "total_scanned": len(all_prods),
        "matched_count": len(matched),
        "keywords": kw_list,
        "products": matched,
    })


@app.post("/products/delete-by-nos")
async def products_delete_by_nos(request: Request):
    """지정된 product_nos 목록 순차 삭제. body: {"product_nos": ["123","456",...]}"""
    body = await request.json()
    product_nos = body.get("product_nos", [])
    if not product_nos:
        return JSONResponse({"error": "product_nos 필드가 비어있습니다."}, status_code=400)

    deleted, failed = [], []
    for no in product_nos:
        try:
            result = await naver_api.delete_product(str(no))
            if result:
                deleted.append(no)
            else:
                failed.append({"no": no, "reason": "delete_product returned falsy"})
        except Exception as e:
            failed.append({"no": no, "reason": str(e)})
        await asyncio.sleep(0.3)

    return JSONResponse({
        "deleted_count": len(deleted),
        "failed_count": len(failed),
        "deleted": deleted,
        "failed": failed,
    })


async def _run_audit():
    global _AUDIT_CACHE
    _AUDIT_CACHE = {"status": "running", "scanned": 0, "products": []}
    all_prods = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        for p in contents:
            product_no = str(p.get("originProductNo", ""))
            origin = p.get("originProduct", {})
            name = (origin.get("name") or "").strip()
            price = int(origin.get("salePrice") or origin.get("price") or 0)
            if product_no and name:
                all_prods.append({"product_no": product_no, "name": name, "price": price})
        _AUDIT_CACHE["scanned"] = len(all_prods)
        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(0.3)
    _AUDIT_CACHE["status"] = "done"
    _AUDIT_CACHE["products"] = all_prods
    print(f"[AUDIT] 완료 — 전체 {len(all_prods)}개 수집", flush=True)


@app.post("/products/audit-start")
async def products_audit_start(background_tasks: BackgroundTasks):
    """전체 상품 이름+가격 백그라운드 스캔 시작. /products/audit-result 로 결과 조회."""
    if _AUDIT_CACHE.get("status") == "running":
        return JSONResponse({"status": "already_running", "scanned": _AUDIT_CACHE.get("scanned", 0)})
    background_tasks.add_task(_run_audit)
    return JSONResponse({"status": "started"})


@app.get("/products/audit-result")
async def products_audit_result():
    """audit-start 로 시작한 스캔 결과 반환. status=done 되면 products 배열 포함."""
    return JSONResponse({
        "status": _AUDIT_CACHE.get("status"),
        "scanned": _AUDIT_CACHE.get("scanned", 0),
        "count": len(_AUDIT_CACHE.get("products", [])),
        "products": _AUDIT_CACHE.get("products", []),
    })


@app.post("/delete-blurry-products")
async def delete_blurry_products(background_tasks: BackgroundTasks):
    """흐릿/소형 대표이미지 상품 전체 삭제 (백그라운드).
    진행 상황은 Railway 로그에서 [BLURRY-DELETE] 태그로 확인."""
    background_tasks.add_task(_run_delete_blurry)
    return JSONResponse({"status": "started", "message": "흐릿 이미지 상품 삭제 시작. Railway 로그에서 [BLURRY-DELETE] 태그 확인."})


@app.get("/auto-cleanup-log")
async def auto_cleanup_log(lines: int = 10):
    """최근 자동 판매중지 실행 로그 조회"""
    from main import CLEANUP_LOG_FILE
    try:
        with open(CLEANUP_LOG_FILE, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        recent = [json.loads(l) for l in all_lines[-lines:] if l.strip()]
        return JSONResponse({"count": len(recent), "logs": recent})
    except FileNotFoundError:
        return JSONResponse({"count": 0, "logs": [], "message": "아직 실행 기록 없음"})


@app.post("/deactivate-product")
async def deactivate_product(request: Request):
    """🚫 상품 판매중지 (전체 origin GET→statusType 수정→PUT)"""
    import httpx as _hx
    from main import NAVER_BASE
    body = await request.json()
    product_id = str(body.get("product_id", ""))
    if not product_id:
        return JSONResponse({"status": "error", "message": "product_id 필요"}, status_code=400)
    try:
        hdrs = await naver_api._headers()
        # 1) 전체 origin 조회
        async with _hx.AsyncClient(timeout=20) as c:
            rg = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{product_id}", headers=hdrs)
        if rg.status_code != 200:
            return JSONResponse({"status": "fail", "step": "get", "http": rg.status_code,
                                 "body": rg.text[:300]}, status_code=400)
        origin = rg.json().get("originProduct", {})
        # 2) statusType 수정 후 PUT
        origin["statusType"] = "SUSPENSION"
        async with _hx.AsyncClient(timeout=20) as c:
            rp = await c.put(f"{NAVER_BASE}/v2/products/origin-products/{product_id}",
                             headers=hdrs, json={"originProduct": origin})
        if rp.status_code == 200:
            return JSONResponse({"status": "ok", "product_id": product_id})
        return JSONResponse({"status": "fail", "step": "put", "http": rp.status_code,
                             "body": rp.text[:500]})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/activate-product")
async def activate_product(request: Request):
    """✅ 상품 판매재개"""
    body = await request.json()
    product_id = str(body.get("product_id", ""))
    if not product_id:
        return JSONResponse({"status": "error", "message": "product_id 필요"}, status_code=400)
    ok = await naver_api.set_product_status(product_id, "SALE")
    return JSONResponse({"status": "ok" if ok else "fail", "product_id": product_id})


@app.post("/set-status-safe")
async def set_status_safe(product_no: str, status: str):
    """GET 전체 원본→statusType 수정→PUT 방식 안전한 상태 변경 (SALE/SUSPENSION/CLOSE).
    set_product_status()의 partial-body 실패 문제 우회."""
    import httpx as _hx
    from main import NAVER_BASE
    if status not in ("SALE", "SUSPENSION", "CLOSE"):
        return JSONResponse({"ok": False, "error": f"지원하지 않는 상태: {status}"})
    headers = await naver_api._headers()
    async with _hx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{product_no}", headers=headers)
    if r.status_code != 200:
        return JSONResponse({"ok": False, "error": f"Naver 조회 실패: HTTP {r.status_code} {r.text[:200]}"})
    data = r.json()
    origin = dict(data.get("originProduct", {}))
    old_status = origin.get("statusType", "")
    origin["statusType"] = status
    ok, err = await naver_api.update_product(product_no, origin)
    if ok:
        return JSONResponse({"ok": True, "product_no": product_no,
                             "old_status": old_status, "new_status": status})
    return JSONResponse({"ok": False, "error": err})


@app.delete("/delete-product/{product_id}")
async def delete_product(product_id: str):
    """🗑️ 상품 삭제"""
    ok = await naver_api.delete_product(product_id)
    return JSONResponse({"status": "ok" if ok else "fail", "product_id": product_id})


@app.post("/update-price")
async def update_price(request: Request):
    """💰 상품 가격 수정 (read-modify-write — Naver는 부분 PUT 거부)"""
    import httpx as _hx
    body = await request.json()
    product_id = str(body.get("product_id", ""))
    price = int(body.get("price", 0))
    if not product_id or price <= 0:
        return JSONResponse({"status": "error", "message": "product_id, price 필요"}, status_code=400)
    headers = await naver_api._headers()
    url = f"https://api.commerce.naver.com/external/v2/products/origin-products/{product_id}"
    async with _hx.AsyncClient(timeout=20) as c:
        r = await c.get(url, headers=headers)
        if r.status_code != 200:
            return JSONResponse({"status": "error", "message": f"GET {r.status_code}: {r.text[:200]}"}, status_code=502)
        origin = r.json().get("originProduct", {})
        _READONLY = {"originProductNo", "channelProductNo", "regDate", "modDate", "statusFrom", "totalSalesQuantity", "channelProducts"}
        payload = {k: v for k, v in origin.items() if k not in _READONLY}
        payload["salePrice"] = price
        r2 = await c.put(url, headers=headers, json={"originProduct": payload})
    ok = r2.status_code == 200
    return JSONResponse({"status": "ok" if ok else "fail", "product_id": product_id, "new_price": price,
                         "error": r2.text[:300] if not ok else ""})


@app.post("/set-product-status")
async def set_product_status_endpoint(request: Request):
    """상품 판매상태 변경 (SALE/SUSPENSION/CLOSE). read-modify-write 방식."""
    import httpx as _hx
    body = await request.json()
    product_no = str(body.get("product_no", ""))
    new_status  = str(body.get("status", "SUSPENSION"))
    if not product_no:
        return JSONResponse({"status": "error", "message": "product_no 필요"}, status_code=400)
    headers = await naver_api._headers()
    url = f"https://api.commerce.naver.com/external/v2/products/origin-products/{product_no}"
    async with _hx.AsyncClient(timeout=20) as c:
        r = await c.get(url, headers=headers)
        if r.status_code != 200:
            return JSONResponse({"status": "error", "message": f"GET {r.status_code}: {r.text[:200]}"}, status_code=502)
        origin = r.json().get("originProduct", {})
        _READONLY = {"originProductNo", "channelProductNo", "regDate", "modDate", "statusFrom", "totalSalesQuantity", "channelProducts"}
        payload = {k: v for k, v in origin.items() if k not in _READONLY}
        payload["statusType"] = new_status
        r2 = await c.put(url, headers=headers, json={"originProduct": payload})
    ok = r2.status_code == 200
    return JSONResponse({"status": "ok" if ok else "fail", "product_no": product_no,
                         "new_status": new_status, "error": r2.text[:300] if not ok else ""})


@app.post("/update-stock")
async def update_stock_endpoint(request: Request):
    """📦 재고 수정"""
    body = await request.json()
    product_id = str(body.get("product_id", ""))
    stock = int(body.get("stock", 0))
    if not product_id:
        return JSONResponse({"status": "error", "message": "product_id 필요"}, status_code=400)
    ok = await naver_api.update_stock(product_id, stock)
    return JSONResponse({"status": "ok" if ok else "fail", "product_id": product_id, "stock": stock})


# ─── 주문 관리 ────────────────────────────────────────────────────────────────

@app.get("/orders")
async def get_orders(days: int = 7):
    """📋 최근 N일 주문 조회 (기본 7일)"""
    try:
        orders = await naver_api.get_all_orders(days)
        return JSONResponse({
            "total": len(orders),
            "days": days,
            "orders": [
                {
                    "order_id": o.get("productOrderId"),
                    "product_name": o.get("productName", ""),
                    "quantity": o.get("quantity", 0),
                    "amount": o.get("totalPaymentAmount", 0),
                    "unit_price": round(int(o.get("totalPaymentAmount", 0) or 0) / max(int(o.get("quantity", 1) or 1), 1)),
                    "status": o.get("productOrderStatus", ""),
                    "buyer": o.get("buyerName", ""),
                    "ordered_at": o.get("paymentDate", ""),
                    "seller_code": o.get("sellerProductCode", ""),
                }
                for o in orders
            ]
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/order-summary")
async def order_summary(days: int = 1):
    """📊 오늘 주문 수 + 총 매출 (CEO 대시보드용)"""
    try:
        orders = await naver_api.get_all_orders(days)
        return JSONResponse({
            "days": days,
            "total_orders": len(orders),
            "total_revenue": sum(int(o.get("totalPaymentAmount", 0) or 0) for o in orders),
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


class DispatchInvoiceRequest(BaseModel):
    productOrderId: str
    trackingNumber: str
    deliveryCompanyCode: str = "CJGLS"


@app.post("/dispatch-invoice")
async def dispatch_invoice(req: DispatchInvoiceRequest):
    """📦 송장번호 입력 → 스마트스토어 배송처리 자동 등록
    deliveryCompanyCode: CJGLS(CJ대한통운) HANJIN(한진) LOTTE(롯데) POST(우체국) HYUNDAI(현대)
    """
    try:
        ok, err = await naver_api.dispatch_orders([{
            "productOrderId": req.productOrderId,
            "deliveryCompanyCode": req.deliveryCompanyCode,
            "trackingNumber": req.trackingNumber,
        }])
        if ok:
            _tg_ss(
                f"📦 <b>[스마트스토어] 배송처리 완료</b>\n"
                f"주문번호: <code>{req.productOrderId}</code>\n"
                f"택배사: {req.deliveryCompanyCode}\n"
                f"송장번호: {req.trackingNumber}"
            )
            return JSONResponse({"status": "ok", "message": "배송처리 완료"})
        return JSONResponse({"status": "error", "message": err}, status_code=400)
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/register-digital")
async def register_digital_product(request: Request):
    """디지털 상품 직접 등록 (AI 파이프라인 없이 payload 그대로 전달)"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"status": "error", "message": "JSON 파싱 실패"}, status_code=400)

    leaf_id     = int(body.get("leafCategoryId", 50000727))
    name        = str(body.get("name", "")).strip()
    price       = int(body.get("price", 0))
    detail      = str(body.get("detailContent", ""))
    image_url   = str(body.get("image", "")).strip()
    stock       = int(body.get("stock", 9999))
    origin_code = str(body.get("originAreaCode", "0009380"))  # 0009380 = 국산(서울특별시 은평구)

    if not name or not price:
        return JSONResponse({"status": "error", "message": "name, price 필수"}, status_code=400)

    # 이미지 처리: 외부 URL 업로드 or DALL-E 생성
    if image_url and image_url.startswith("http"):
        try:
            image_url = await naver_api.upload_image(image_url)
        except Exception as img_e:
            return JSONResponse({"status": "error", "message": f"이미지 업로드 실패: {img_e}"}, status_code=500)
    else:
        # DALL-E로 제품 이미지 자동 생성
        try:
            dalle_prompt = (
                f"Professional product thumbnail for Korean e-commerce: '{name}'. "
                "Dark blue gradient background (#0a0a20), centered white text overlay, "
                "robot/AI icons, clean minimal tech style, square 1:1 ratio, high quality commercial photo."
            )
            raw_img = await generate_dalle_image(dalle_prompt)
            if raw_img:
                image_url = await naver_api.upload_image(raw_img)
            else:
                return JSONResponse({"status": "error", "message": "이미지 생성 실패 — image 파라미터에 공개 이미지 URL 입력"}, status_code=400)
        except Exception as img_e:
            return JSONResponse({"status": "error", "message": f"이미지 자동생성 실패: {img_e}"}, status_code=500)

    payload = {
        "originProduct": {
            "statusType": "SALE",
            "saleType": "NEW",
            "leafCategoryId": leaf_id,
            "name": name,
            "detailContent": detail or name,
            "images": {
                "representativeImage": {"url": image_url},
                "optionalImages": [],
            },
            "salePrice": price,
            "stockQuantity": stock,
            "deliveryInfo": {
                "deliveryType": "DELIVERY",
                "deliveryAttributeType": "NORMAL",
                "deliveryCompany": "CJGLS",
                "deliveryFee": {
                    "deliveryFeeType": "FREE",
                    "deliveryFeePayType": "PREPAID",
                    "baseFee": 0,
                    "freeConditionalAmount": 0,
                },
                "claimDeliveryInfo": {
                    "returnDeliveryFee": 0,
                    "exchangeDeliveryFee": 0,
                    "deliveryCompany": "CJGLS",
                    "returnDeliveryCompany": "CJGLS",
                },
            },
            "detailAttribute": {
                "afterServiceInfo": {
                    "afterServiceTelephoneNumber": "010-9299-9666",
                    "afterServiceGuideContent": "카카오 mnm1876 또는 mnm1876@naver.com으로 문의해 주세요.",
                },
                "originAreaInfo": {
                    "originAreaCode": origin_code,
                    "content": "국내산 (대한민국)",
                    "plural": False,
                    "importer": "해당없음",
                },
                "minorPurchasable": True,
                "productInfoProvidedNotice": {
                    "productInfoProvidedNoticeType": "ETC",
                    "etc": {
                        "itemName": name[:50],
                        "modelName": "AI Suite v1",
                        "manufacturer": "AI Suite (mnm1876)",
                        "customerServicePhoneNumber": "010-9299-9666",
                        "returnCostReason": "디지털 콘텐츠 특성상 수령 후 환불 불가",
                        "noRefundReason": "디지털 콘텐츠 특성상 수령 후 환불 불가",
                        "qualityAssuranceStandard": "설치 완료 후 7일 이내 미작동 시 환불",
                        "compensationProcedure": "010-9299-9666 또는 mnm1876@naver.com",
                        "troubleShootingContents": "카카오 mnm1876 / 010-9299-9666",
                    },
                },
            },
        },
        "smartstoreChannelProduct": {
            "naverShoppingRegistration": True,
            "channelProductDisplayStatusType": "ON",
        },
    }

    try:
        result = await naver_api.register_product(payload)
        product_no = result.get("originProductNo") or result.get("id")
        return JSONResponse({"status": "ok", "originProductNo": product_no, "raw": result})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/update-digital/{product_no}")
async def update_digital_product(product_no: int, request: Request):
    """디지털 상품 이미지·상세내용·원산지 업데이트"""
    body = await request.json()
    image_url    = str(body.get("image", "")).strip()
    detail       = str(body.get("detailContent", ""))
    name         = str(body.get("name", ""))
    price        = int(body.get("price", 0))
    stock        = int(body.get("stock", 9999))
    origin_code  = str(body.get("originAreaCode", "0009380"))  # 0009380 = 국산(한국)
    status_type  = str(body.get("statusType", "SALE"))  # SALE | SUSPENSION | CLOSE
    leaf_cat_id  = int(body.get("leafCategoryId", 50001514))

    if image_url.startswith("http"):
        try:
            image_url = await naver_api.upload_image(image_url)
        except Exception as e:
            return JSONResponse({"status": "error", "message": f"이미지 업로드 실패: {e}"}, status_code=500)

    # update_product()가 내부에서 {"originProduct": payload}로 감싸므로 inner dict만 전달
    origin_payload = {
        "statusType": status_type,
        "saleType": "NEW",
        "leafCategoryId": leaf_cat_id,
        "name": name,
        "detailContent": detail,
        "images": {
            "representativeImage": {"url": image_url},
            "optionalImages": [],
        },
        "salePrice": price,
        "stockQuantity": stock,
        "deliveryInfo": {
            "deliveryType": "DELIVERY",
            "deliveryAttributeType": "NORMAL",
            "deliveryCompany": "CJGLS",
            "deliveryFee": {"deliveryFeeType": "FREE", "deliveryFeePayType": "PREPAID", "baseFee": 0, "freeConditionalAmount": 0},
            "claimDeliveryInfo": {"returnDeliveryFee": 0, "exchangeDeliveryFee": 0, "deliveryCompany": "CJGLS", "returnDeliveryCompany": "CJGLS"},
        },
        "detailAttribute": {
            "afterServiceInfo": {"afterServiceTelephoneNumber": "010-9299-9666", "afterServiceGuideContent": "카카오 mnm1876 또는 mnm1876@naver.com으로 문의해 주세요."},
            "originAreaInfo": {"originAreaCode": origin_code, "content": "국내산 (대한민국)", "plural": False, "importer": "해당없음"},
            "minorPurchasable": True,
            "productInfoProvidedNotice": {
                "productInfoProvidedNoticeType": "ETC",
                "etc": {
                    "itemName": name[:50],
                    "modelName": "AI Suite v1",
                    "manufacturer": "AI Suite (mnm1876)",
                    "customerServicePhoneNumber": "010-9299-9666",
                    "returnCostReason": "디지털 콘텐츠 특성상 수령 후 환불 불가",
                    "noRefundReason": "디지털 콘텐츠 특성상 수령 후 환불 불가",
                    "qualityAssuranceStandard": "설치 완료 후 7일 이내 미작동 시 환불",
                    "compensationProcedure": "010-9299-9666 또는 mnm1876@naver.com",
                    "troubleShootingContents": "카카오 mnm1876 / 010-9299-9666",
                },
            },
        },
    }

    try:
        ok, err = await naver_api.update_product(product_no, origin_payload)
        if ok:
            return JSONResponse({"status": "ok", "product_no": product_no})
        return JSONResponse({"status": "error", "message": err}, status_code=500)
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/origin-areas")
async def get_origin_areas():
    """네이버 원산지 코드 목록 조회"""
    import httpx as _hx
    headers = await naver_api._headers()
    # 여러 후보 endpoint 시도
    candidates = [
        "https://api.commerce.naver.com/external/v2/products/meta/origin-areas",
        "https://api.commerce.naver.com/external/v1/product-service/meta/origin-areas",
        "https://api.commerce.naver.com/external/v1/products/meta/origin-areas",
    ]
    results = {}
    async with _hx.AsyncClient(timeout=10) as c:
        for url in candidates:
            try:
                r = await c.get(url, headers=headers)
                results[url] = {"status": r.status_code, "body": r.text[:500]}
            except Exception as e:
                results[url] = {"error": str(e)}
    return JSONResponse(results)


@app.get("/get-product/{product_no}")
async def get_product_detail(product_no: int):
    """상품 원본 데이터 조회 (originAreaInfo 확인용)"""
    try:
        import httpx as _hx
        headers = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(
                f"https://api.commerce.naver.com/external/v2/products/origin-products/{product_no}",
                headers=headers,
            )
        if r.status_code == 200:
            data = r.json()
            origin = data.get("originProduct", {})
            detail_attr = origin.get("detailAttribute", {})
            return JSONResponse({
                "status": "ok",
                "name": origin.get("name"),
                "statusType": origin.get("statusType"),
                "salePrice": origin.get("salePrice"),
                "wholeSalePrice": origin.get("wholeSalePrice"),
                "stockQuantity": origin.get("stockQuantity"),
                "originAreaInfo": detail_attr.get("originAreaInfo"),
                "leafCategoryId": origin.get("leafCategoryId"),
            })
        return JSONResponse({"status": "error", "code": r.status_code, "body": r.text[:500]}, status_code=500)
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/cancel-order")
async def cancel_order(request: Request):
    """🚫 주문 취소 처리 (판매자 귀책 사유)"""
    import httpx as _hx
    body = await request.json()
    product_order_id = str(body.get("product_order_id", ""))
    reason = str(body.get("reason", "상품 가격 표시 오류로 인한 판매자 귀책 취소"))
    if not product_order_id:
        return JSONResponse({"status": "error", "message": "product_order_id 필요"}, status_code=400)

    headers = await naver_api._headers()
    NAVER_BASE = "https://api.commerce.naver.com/external"

    # 1단계: 현재 주문 상태 조회
    try:
        async with _hx.AsyncClient(timeout=15) as c:
            rs = await c.post(
                f"{NAVER_BASE}/v1/pay-order/seller/product-orders/query",
                headers=headers,
                json={"productOrderIds": [product_order_id]}
            )
        if rs.status_code != 200:
            return JSONResponse({"status": "error", "step": "status_check",
                                 "code": rs.status_code, "body": rs.text[:400]})
        items = rs.json().get("data", [])
        po = (items[0].get("productOrder", {}) if items else {})
        order_status = po.get("productOrderStatus", "UNKNOWN")
    except Exception as e:
        return JSONResponse({"status": "error", "step": "status_check", "message": str(e)})

    # 2단계: 취소 API 호출
    cancel_body = {
        "productOrderIds": [product_order_id],
        "cancelReasonType": "SELLER_CAUSE",
        "cancelReason": reason
    }
    results = {}
    async with _hx.AsyncClient(timeout=20) as c:
        for path in [
            f"{NAVER_BASE}/v1/pay-order/seller/product-orders/cancel",
            f"{NAVER_BASE}/v1/pay-order/seller/product-orders/{product_order_id}/cancel",
            f"{NAVER_BASE}/v1/pay-order/seller/product-orders/request-cancel",
        ]:
            try:
                rc = await c.post(path, headers=headers, json=cancel_body)
                key = path.split("/pay-order/seller/")[1]
                results[key] = {"status": rc.status_code, "body": rc.text[:400]}
                if rc.status_code in (200, 201):
                    break
            except Exception as e:
                results[path] = {"error": str(e)}

    return JSONResponse({
        "status": "ok",
        "product_order_id": product_order_id,
        "order_status_before": order_status,
        "cancel_result": results
    })


@app.post("/confirm-orders")
async def confirm_orders(request: Request):
    """✅ 주문 발주확인 처리"""
    body = await request.json()
    order_ids = body.get("order_ids", [])
    if not order_ids:
        # 발주확인 필요한 주문 자동 조회 후 전체 처리
        orders = await naver_api.get_new_orders()
        order_ids = [o.get("productOrderId") for o in orders
                     if o.get("productOrderStatus") == "PAYED"]
    if not order_ids:
        return JSONResponse({"status": "ok", "message": "발주확인 대기 주문 없음", "confirmed": 0})
    ok = await naver_api.confirm_orders(order_ids)
    return JSONResponse({"status": "ok" if ok else "fail", "confirmed": len(order_ids)})


# ─── 서버 시작 시 Drive 인덱스 자동 복구 ─────────────────────────────────────

DRIVE_FILE_IDS_PERMANENT = ["1F5BYQ4DqnMSZW-oeuz4EtZGGIfyZ0y-X","1gLAbw9lGhR3BVZCNAa_P7RMwUZ88U50k","1jV6AxMEyJsg7XOg6C5sosZ5dHORAumJh","1xZ47ndTtOmG261DdijOgL8C2SiAbuyL6","16AGyMIIJXKlZP_ihd7tXerSz6pCkSs9H","1_I1dUiFX7D0rnof4rC7obk2yYWblZpnR","136S01eIBe00xCWo2dAXuxB7uX10xKlje","1R44kThE6GRDDcr1896ngE_0XB5PTynWO","17qy8J8EH_vlY8P8XP17XlhCzvWQpbx4V","1m8kZTgreECPiLcIQ_SkJ7-55Iz0oNFcd","1JK04RyMbNegk3MOoAWS68hBHeq0dHP6B","1rWS4PmF6azG-FeUnQnyT6ck7FSbIDyMQ","1BKhyhLCqwHTkKcaKw6woUnICHONoB5jP","1UBbWPc4cLa0HTW0h-OaheVW4JWTjjN0a","1kTdJsxZk8SpiD13T-XG-jAY-EI7VX3VN","1ruA9DR188aU0xECYhX1Zt-BXJ9r_rYts","1G0x8w7J-v5gNWHGq1gxsOfo1nmm81ohF","1Rx5Qto0IjKsLZsHM1Za-R2r0LMJ21Q2A","1N2vIv8EZptFFNRTBYYj2OZhjN04PGf80","1iWQeMw-l4fMyGMjmpjNu3TfkQTiR0x8I","1OSxxuS035-sE7VNIRlc-puJ5kq2_6Gmt","1WacefkrQLELQKiKPJeEA2-QVO9UxIfIV","1ROgmOKDXcaQ-g-vMr7AziTxsmMSllAOm","1UReOo21RGL5W6cj7Nkn4Q8eL3dbQVQ5E","1hy-CYqLxulXuHDcSEzAJ4XllBNpLhYZm","1pMYqfejj_FQlh5tolEji5jIr6rK1tW2a","1u7WoYhBl4p7uK-1eKNAngQ4roHh2xBv4","1OjI642UaLTIouubjRyby7iCxFcYvL33F","1Y0_-7GYDH5X5Ec4b_3wrQrjOsTJ8MGKs","1lzKcE6tMIanUhhwWckzEgrdZa07vVsPM","14OpPEfS9cHbW5QX4nVY4tzzLIFAVYo2T","17BoqT3OiJQ9X7NFSvJXoN8dMDOzOwUho","1HMWYeI68b9QwUqfxLyPjugbeYm6UOCEA","1J1g3g0E0bCyeAlOYH-1DbuNEE5yYsSiW","1DhHzFgz9Ugii4jv9QqjYa7Gbjolx_rKo","1VNxBrRO8bCr45BJBSNdk-eljnM4V9o01","1rmcK-W5BLrI6f4sGdeebdF65qoO35rDE","1-0n6qo9QuTEFau0H_XaKvCkEsTWCCkuu","1AKMgOcB-Rwm4vAbOdsYvnk-TJ7BXTDKd","1RUBacH7H3IdYGNKlkXKLKVkYp8QS_O0U","1ghsRkLDZrujEw9FzX1Wt25vssyGl1K3s","1ncek80ov0wP2UvgMrvnKPGXDZV2qqye8","1Ax1PxhZxGJ7GTvSaDFvg_WhAHCOSv2QK","1I4M5l5ZXYM0ENRT5r_DDow7KuLHTQtK4","1MLuON0bayJvPxAkMzQA-kkML7-sRdpF2","1NSOrchmXlrxyVMXoP43wZET7iDq8QVm2","1qO-2UByqCafS3OlVWgTxMbEizUHH45Kx","1siOiZVLn9HltdoThgoB1zThMgmej8V2T","1wHixMn1oKCaZ4SiJuU1p41e-LwIsPzG_","1Eg2b_gakaB_hyZ6Xq1cDyggVHmXwz4CF","1PcouzQhb9_vaDYQI0C8sIVfmXCoDr7Mo","1ph5PWisdk4qKfzOh3xHXYaZJTxw9DHVw","1xW8PUy--_4nYuNcC6KJDKzjELBYQOVBK","1BQsWH4K1R_HZsRViJs0Ur8ZogDMgJwgX","1QlyBfWW1lOyZCR8XS2qsH2aYM8Gs7uwp","1WBR2ENBnxYJOsaZQ7Kf2XNwDKSaDBznp","1XVWlYTgKu7MF-KNX7Rg_DtgUhPLa2_Va","1yR9IHTYqmPXCS3LBUEOsn5MzO4ANTepo","11T4mj6_EkKmcObkRvbuUuNmeol5cA_ii","157oPxSB2d60bZyXWll5ydfvxOaiHEmZi","1AX0QZb9ywqpdKnIpbeOF8HnXvH1UXQhH","1oNWkUdTrFyZIOFJVzU_82XokzTogSKIr","1oeHDaC-PfVjsQowLkuz1unv8WGZxQ5db","16PUdz772aVdiOgvopZA51KRRiAH53PGA","1If2XNJhzim-VwtOeRE3WaskZ1sFAYFGm","1JAws2ZHI0Mun47Qr0mYXJKibv9X8tmgm","1Z25uo9fazYjqTlKvWQ5FIl0OZQTBHt1_","1sT6-RZ0iJIT8oAZq_Flz9xXCT6w4fwvU","1EJ_nUj0KFB4i74ox5O4QQnZ-hdnNwtkM","1Qt4tJnJdKq2u0x1ltyQKxTckPjqWuY1-","1Vj2czJGi34Z_e-rmmaaZzRrLaHqQbNO4","1XBgsRrVNzyE5QKuHTVGigzaQmWKhxliK","1lHYHebmM_CcPFU0NUyl9vIzRkyWX1Y8l","1wJ4SGiak6aaWjWUurLMEDAY9Lxfiy-gm","1MKf2Mt-iKaXjKDV8y88M1BiMFjkQfZKL","1N2bMF6zicrqlAtEFVUawxZw_kAVgZPLU","1bOHq1d4vMbBxAO6lPgrXgvzg2wUA6tT4","1fnTMIh-0Iaa52Gj1FdurGr79KiH8s1Ji","1yOXhzXgmtypzb17D2b0GJl2yn7udJS25","1_bTHeN4wRvVoE8bUqcxCgNh40BvLeQhq","1aZ8LhL1YvA2-4KCmD4k9Tf5jmiKcDO-n","1rtQ-V2dWRP_swvUaiMgmmqJ20x6WNcS1","1zn4pHgAtv_7PL0sSAJyp-21VilJAd8TY","14m9VKg_Mf0fXUzuuzbXJb73FuN8sVXK_","15tSQNhEShtvhw1m3R_MZabXUtZamVfZs","1A5iQUlri9GNQxtHsJF705iGDySxvqijB","1T3CO3cz5SltsAxlaQLaptHtoGGwiPmYi","1auVWrv8tbwvH2msTwFlUpvN4zpX2Hfxl","1qQXd7-f2RgbQKW0R736AgnH80miMQZmD","1B-ZmqSmchfBVXVUNdj4Tso2IZnNU3kB4","1Cgc-TZu39T-9uodBJTty9bWYx0WLcSmG","1GyBNNnp4Rpv6zgkQhzj6nih_QMUzhM0h","1NbIdzQNUNwhWy2De_7DAOqKLv1Nc_Bqp","1Q2bBdF43r4sMZowAYH892SDE76lS45Zw","1YnmCsMRGmbLbfJnuChQKcytmue5RJSAZ","16dWlOpx6PZ6eV-uJJPteLSileR44hDFU","1RyVncFlc0405E4MGa80u7fMG40YoOWt_","1_sWOhaL-CMIL0npAbtRcY3mDgiBzxdFJ","1wsJsYMhIWdJDzDYiKnq310OFX41iV4Hs","1BqKYPd0ToNlb3ANBX70DY2iyo2sEP_e0","1EVzmB1B_ytHXA9636yY-iFlLHnXyXM9s","1HCDX9dg4WWcWsjaiv1dnqzXz9VR2zdO1","1VrcyTXLfu63a3bTc6RzVH7DScvrilOjz","1yiMUE-3dXiZalDhe1s7_e3i96XYCttmu","1TegWQGFp4_gnLQR5owFab9p1_8Im96MX","1ToXKEyn0pobQ8pmxjCenRqPjHbjh1ci1","1YF9oHUXb93fPhIvO5E59ccGClmGsBdYa","1bWYJqUfRQdgiH4mUDiu85DdUJiSQB-x_","1IrUNjWmdaRVtAeaOPTqGbjXdb35dWycj","1i_P6kT1CNaL7F20Vmh3qXZvL_yMnVLWH","1lbvL7uu3ssgfYvB1fdSOCnmEA892vEVU","1luu7f1xChN-CoUKuRjQs_v55wsqgcrIP","1Gkz53GQtRajwGxXlxZGs3BmjZxcfz98S","1aUTOgLQUcg_hEQQVuwzfnP1wk9gwOFXy","1r5siU-1IdGnSN9yBrW_7lm0pKTa25T81","1z1ZPX-56iY3rbzrGMyBJrtNLwCrxSk82","1G2Ornl4RbSYNPK5pX2uvKzk_VINAaKdp","1Hso2RmL08J78F4jCIPpltwi1B4zx_MpS","1LRgY3HlCLMfc8m4g1w7IHK92g0GxKHPJ","1evSX1iLnVc-9FFyMm1sqw34-uO8mZtl5","1RhXaznEnw8LGhrhtMcjEeGs2ht7X98wK","1qjfzFm2tvqqEQpKvHqjwQIvjc9B-09Pk","1vTEOAgQKuC5oYWVFvR4tssERahbUV4W_","13h6QLiWULI9zfVBJyp6Wwr4YxFrZeLME","13kw2jcXWoAXKBmh1kysA5yuDSuweeStv","1UOo_sLBYXGwog9iJhTp5m6WDLfofvXxW","1ArJKR_Ha3VKXBBQ8r-8QcUCC8-aPhdGq","1J9-P6AMGjoqIB3BNR8qjp92mOeZyH1rg","1MUhAQsn45c98o3r08WADbGIoAAHtEkkL","1OP74P7NPvbkVU7D57z9BEhHDXlRUUkit","12Tgjh0MrXyfOfQiNTCgRg6EWA-jL4bVj","1Jq2O_1z13GjgG6Qmn2lTgZgbGcXXGekK","1_j3jPGp25-PqECF4Jlrtpgv-TpdlrBBx","1ua9c6gBJsl9wZX_10skCvwh9r68lDrNO","106q6bRDNZjSnBjAI-Hlp6L42IFTDpAPT","12V1bcyTYgr7TOUWbaOF0SSPTdeTBCvng","1AUYuELkcuuumtZtefr641kYOq4kzTo0y","1XfN2YiaYuj4z17EITCtYpNmNumE7eu86","1KnoLKpqCUtdaHFjhpNWtbBIV1xCAdG2x","1vuJvBSpIRPol6PRduh5NKLnJZfWYDR54","1EJLZ0bMqwwULosztfyGWLkFwPKNK0364","1vUZipeLzpWQftlk9oTJ1RvjGgKeF7cl6","11ZOejeQL-so2J4ku6OzsSTjdhcIa96W5","1YwL0MOnYRNo7C1zqPfvDKca2z7DgxniG","17Lt_3vrK0Up1lWu9Zw_ghgQa_gBeTUCy","1EtI3v_2UUzwB5Eca9v6FNtxIR1-xTEFC","1DEFlm039yWVM_wvcKEjaH87PWY7sDx-T","1ymSnyqJPLsult7Y2UGi3OfWAkbn8cTCB","1u0QoRrypl0eTu-4mQoXLtbXRAjsboNIO","1zGMrtjXLOskZPKpbeFw4NGmUEqXyMuHe","12J4RUEXRgmNhrOkf3tljxo3XFvD21LOb","1a83RMRVW0KU6RJHcK4eszGPOk0572C4y","1hSJ4XOi446f2JAnFk1i9E8aB61c86m62","1QkwuYvBpPmpk__-4kHgB9bhObmRS_R3h","1cbloe-yFwkXnPzh5tXys3zHqlTY4VjkN","1ybUp8l1X4I_t6BRBx8__N6xxYCZeplws","1zwKum5N68_0mnORYW0-tD1smZw2Ei4yu","1Iho3wyoJ8KMVP0XGkUE3X1LMtKje8Y6N","1K-11X4Ha-EDoJ071ZV1ezTY-36z-K0NX","1mGUotPNdjOMT9U2S9wKTT0xH9EyZ-22I","1gU3dBS9TJJ8nz3BQbkBh8mlapM6lrjl8","1wriCB5ACsGrI4Qk3OaV3mK4p-bjTc7sZ","104m1Nn5Yn_K6YTbVm4iYS85Of-k_S4K-","1JzeA97DE1BSbm5Ya2pK2CvaYiFW0Et7C","11TGeqWDWpgmE9D3oAQt0rKCVMeaeJd6K","1_mrSh71x1RKIpI80Y56sbYMWt6mhvXfK","1OCe_H9pPv8cwCMfewnuVq-g21QvSzd5d","18U7KAZXzsXPSMk_PQ-_frCQ5vHVqyuw_","1R7Wzcy4s7U_gRtsIndobsog0ASyRXhnR","13MJ0kvm2Plihs__1iwONchp-1oBCUKMH","1V81aQA2ZhHAI_mYzu5yTGVjS4w8EOAgH","1CPuRA5ycFkJxF6iy63p75fJ6vS8znGTa","1Rx2dsq81CsvhMzPcqXaryCUWoCd3jAps","1o-sqx7Byql-2QpE0TVdgDFYUq675X5fg","19WcOdoyExwmRsMUZqDbkot0BvzlF6Hby","18U5ktqW1Qv8BKXKPOlQzHKVq0m4rELNw","1FEg57Qhb01nVuzh1GClay9szBbbWbkeE","1Hz4SDPCSPFHfLxiymXm9Rpkvenxy_lRt","1karcwPejIkfauRY7rChcoE3zj0qzgBnv","1GLEoDqU4sh94vWPjhcyZKzfEvOEEc867","1uNIgYG60Uqqy2NFkKWco1GkpdCp6znFN","11Bj_GZgeKpjeZzDkkqpfvDcUzVV-nixg","1AaMU6tb-Xupt8zsW7opdIBraMolbXP1i","1dz9uQ-BTdTCsBTMoNheO_UkFw3S6YKZs","1EFAJ_sBFkpCPydIJnPIOW2gTMJX0wLCp","1ZxkN6N9fA7EWY0yIUqkYW7F8S9c2ot6g","1putzYsp62ggXXS_tJ0qSdKzb5iJ-1d8T","1BUDm-E15IEaopYG69rAbJ3m_s7EPsjZV","1IABoz-WhYeqx9a4qpCYf5ejJFwIMVK_S","1Z_fY7Vff71ADcHEQpF0xN0eBA-RSo_p0","1nWngRjmzC08Sce_e5LJKhOKkKZEXY_fB","1Q80t3QqknIKUvSVXKuWTeTHvfqUJtazl","1nSI73rtaQp6dpE8jaytm_BeH5DwYQ7Hc","1CQUCNX7ZhqU2UjMdDUtfDPZdRKT902tk","1T39vHXQiLJof5qOH34gJLwb6KmboD35q","1a5xPrxSb5Wse1kiRrYQeTs8kHDFsAqLM","1voLwq4dB5peIs9Y_25GWg6_RPknwOF82","1EW0feDsEu3kM_3Gn3_I4TPS05uDWuXvh","1odfn60_x74Q4QWFWCvEmDY-liTurzoyT","1L2Mu4FLmL5jc_KHjH5PDk3nGps9_Npj7"]

@app.get("/quality-report")
async def quality_report():
    """등록 상품 품질 자동 검사 — 상품명·가격·상태 분석"""
    from datetime import datetime, timezone

    try:
        # 전체 페이지 스캔 — 50개 초과 시에도 정확한 집계
        import httpx as _hx
        products = []
        page = 1
        while True:
            data = await naver_api.list_products(page=page, size=50)
            raw_list = data.get("contents", [])
            if not raw_list:
                break
            # 빈 origin 상품은 직접 재조회: 404면 인덱스 지연(이미삭제) → 제외
            for p in raw_list:
                op = p.get("originProduct", {})
                if not op:
                    pid = str(p.get("originProductNo", ""))
                    try:
                        token = await naver_api.get_token()
                        async with _hx.AsyncClient(timeout=10) as _c:
                            _r = await _c.get(
                                f"{NAVER_BASE}/v2/products/origin-products/{pid}",
                                headers={"Authorization": f"Bearer {token}"},
                            )
                            if _r.status_code == 200:
                                op = _r.json().get("originProduct", {})
                            elif _r.status_code == 404:
                                continue  # 이미 삭제됨, 인덱스 지연 — 제외
                    except Exception:
                        pass
                products.append({
                    "id":     p.get("originProductNo"),
                    "name":   op.get("name", ""),
                    "price":  int(op.get("salePrice", 0)),
                    "status": op.get("statusType", ""),
                })
            if len(raw_list) < 50:
                break
            page += 1
    except Exception as e:
        return JSONResponse({"error": f"상품 조회 실패: {str(e)}"}, status_code=500)

    total = len(products)
    if total == 0:
        return JSONResponse({"message": "등록된 상품 없음", "score": 0,
                             "naver_total": data.get("totalElements", 0)})

    name_ok = name_short = name_long = 0
    price_dist = {"1만미만": 0, "1-3만": 0, "3-5만": 0, "5만이상": 0}
    status_dist: dict = {}
    problems = []
    price_list = []

    for prod in products:
        name   = str(prod.get("name", ""))
        price  = int(prod.get("price", 0))
        status = str(prod.get("status", ""))

        status_dist[status] = status_dist.get(status, 0) + 1
        price_list.append(price)

        name_len = len(name)
        if name_len < 5:
            name_short += 1
            problems.append({"상품명": name[:30], "문제": f"이름 너무 짧음({name_len}자)"})
        elif name_len > 25:
            name_long += 1
            problems.append({"상품명": name[:30], "문제": f"이름 너무 김({name_len}자)"})
        else:
            name_ok += 1

        if price < 10000:   price_dist["1만미만"] += 1
        elif price < 30000: price_dist["1-3만"]   += 1
        elif price < 50000: price_dist["3-5만"]   += 1
        else:               price_dist["5만이상"]  += 1

    avg_price = int(sum(price_list) / max(len(price_list), 1))
    t = max(total, 1)
    score = 100
    score -= round((name_short / t) * 25)
    score -= round((name_long  / t) * 10)
    score -= round((price_dist["1만미만"] / t) * 15)
    score = max(0, min(100, score))

    return JSONResponse({
        "스캔_시각":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "총_상품수":   total,
        "전체_등록수": data.get("total", total),
        "종합_점수":   score,
        "점수_기준":   "상품명 짧음(<5자) -25점 / 상품명 긺(>25자) -10점 / 저가(<1만) -15점",
        "판매_상태":   status_dist,
        "상품명_품질": {
            "정상(5-25자)":    name_ok,
            "너무_짧음(<5자)": name_short,
            "너무_김(>25자)":  name_long,
        },
        "가격_분포":   price_dist,
        "평균_판매가": f"{avg_price:,}원",
        "문제_상품":   problems[:20],
    })


@app.get("/full-report")
async def full_report():
    """📊 전체 현황 한 번에 조회"""
    import asyncio
    from main import load_registered_codes, MARGIN_RATE
    from employees import employee_season_planner, employee_trend_scout, employee_accounting_manager, employee_stock_guardian

    # 병렬 조회
    orders, inquiries, trends, products_raw = await asyncio.gather(
        naver_api.get_all_orders(7),
        naver_api.get_inquiries(),
        employee_trend_scout(),
        naver_api.list_products(1, 50),
    )

    codes = load_registered_codes()
    season = employee_season_planner()
    accounting = await employee_accounting_manager(orders, MARGIN_RATE)

    # 저재고 상품
    from main import parse_excel
    from pathlib import Path as _Path
    excel_files = sorted(_Path(EXCEL_FOLDER).glob("*.xlsx"), key=lambda x: x.stat().st_mtime, reverse=True)
    low_stock = []
    if excel_files:
        prods = parse_excel(str(excel_files[0]))
        low_stock = employee_stock_guardian(prods, threshold=5)

    # 주문 상태별 분류
    status_map = {}
    for o in orders:
        s = o.get("productOrderStatus", "기타")
        status_map[s] = status_map.get(s, 0) + 1

    return JSONResponse({
        "🏪 스토어 현황": {
            "등록 상품 수": len(codes),
            "처리한 Excel 파일": (json.load(open(EXCEL_PROGRESS, encoding="utf-8")) if os.path.exists(EXCEL_PROGRESS) else {}).get("current_index", 0),
            "전체 Excel 파일": len(_load_drive_index()),
        },
        "💰 매출 (7일)": accounting,
        "📦 주문 현황": {
            "총 주문 수": len(orders),
            "상태별": status_map,
        },
        "❓ 미답변 문의": len(inquiries),
        "⚠️ 재고 부족": low_stock[:5] if low_stock else "없음",
        "📅 다가오는 시즌": [f"{e['event']} D-{e['days_left']} ({e['urgency']})" for e in season["upcoming"][:3]],
        "📈 실시간 트렌드": trends[:5],
    })


@app.get("/sync-registered-codes")
async def sync_registered_codes():
    """네이버 등록 상품의 sellerManagementCode + 상품명 추출 → registered_codes/names.json 동기화
    Railway 재배포 후 로컬 파일 초기화 시 수동 복구용. 결과가 0개면 기존 파일 유지(오버라이트 방지)."""
    from main import REGISTERED_CODES_FILE, REGISTERED_NAMES_FILE, _normalize_name
    import asyncio as _asyncio
    codes: set[str] = set()
    names: set[str] = set()
    page = 1
    while True:
        try:
            resp = await naver_api.list_products(page=page, size=50)
        except Exception as e:
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
        contents = resp.get("contents", [])
        if not contents:
            break
        for prod in contents:
            product_no = str(prod.get("originProductNo", ""))
            origin = prod.get("originProduct", {})
            seller_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "")
            prod_name = (origin.get("name") or "").strip()
            if seller_code:
                codes.add(seller_code)
            elif product_no:
                codes.add(f"NAVER_ID_{product_no}")
            if prod_name:
                names.add(_normalize_name(prod_name))
        if len(contents) < 50:
            break
        page += 1
        await _asyncio.sleep(0.5)

    if len(codes) == 0 and len(names) == 0:
        return JSONResponse({
            "status": "skipped",
            "message": "API가 0개 반환 — 기존 파일 유지 (중복 방지)",
            "synced_codes": 0,
            "synced_names": 0,
        })

    if codes:
        with open(REGISTERED_CODES_FILE, "w", encoding="utf-8") as f:
            json.dump(list(codes), f)
    if names:
        with open(REGISTERED_NAMES_FILE, "w", encoding="utf-8") as f:
            json.dump(list(names), f)

    return JSONResponse({
        "status": "ok",
        "synced_codes": len(codes),
        "synced_names": len(names),
        "sample_codes": list(codes)[:10],
        "sample_names": list(names)[:10],
    })


@app.post("/cleanup-empty-products")
async def cleanup_empty_products():
    """네이버 등록 상품 중 이름·가격이 없는 빈 상품 일괄 삭제 (등록 도중 실패한 껍데기 제거)
    순차 상세조회 + 재시도: origin={} 시 2초 후 재조회. 재시도 후에도 비어 있으면 좀비로 삭제."""
    import asyncio as _asyncio
    import httpx as _httpx
    deleted: list[str] = []
    errors:  list[str] = []

    async def _get_origin(product_id: str):
        """상품 상세 직접 조회. 404=이미삭제→None, 기타 실패→{}."""
        try:
            token = await naver_api.get_token()
            async with _httpx.AsyncClient(timeout=15) as _c:
                r = await _c.get(
                    f"{NAVER_BASE}/v2/products/origin-products/{product_id}",
                    headers={"Authorization": f"Bearer {token}"},
                )
                if r.status_code == 200:
                    return r.json().get("originProduct", {})
                if r.status_code == 404:
                    return None  # 이미 삭제된 상품 — 인덱스 지연
        except Exception:
            pass
        return {}

    page = 1
    while True:
        # 검색 API로 ID 목록만 먼저 수집
        try:
            resp = await naver_api.list_products(page=page, size=50)
        except Exception as e:
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
        contents = resp.get("contents", [])
        if not contents:
            break

        for prod in contents:
            product_id = str(prod.get("originProductNo", ""))
            if not product_id:
                continue
            await _asyncio.sleep(0.5)  # 속도제한 방지

            origin = prod.get("originProduct", {})
            # 병렬 조회 실패(origin={}) → 직접 재조회
            if not origin:
                origin = await _get_origin(product_id)
                if origin is None:
                    deleted.append(product_id)  # 404: 이미 삭제됨, 인덱스 지연
                    continue
                if not origin:
                    # 2초 후 최종 재시도
                    await _asyncio.sleep(2.0)
                    origin = await _get_origin(product_id)
                    if origin is None:
                        deleted.append(product_id)  # 404: 이미 삭제됨
                        continue
                    if not origin:
                        # 재시도 후에도 빈 응답 → rate limit 가능성, 삭제 금지
                        print(f"[CLEANUP] 재조회 후에도 빈 응답 → 스킵 (ID: {product_id})", flush=True)
                        continue

            name  = origin.get("name", "").strip() if origin else ""
            price = int(origin.get("salePrice", 0)) if origin else 0

            if not name and price == 0:
                ok = await naver_api.delete_product(product_id)
                if ok:
                    deleted.append(product_id)
                else:
                    errors.append(product_id)

        if len(contents) < 50:
            break
        page += 1
        await _asyncio.sleep(1.0)

    return JSONResponse({
        "status":      "ok",
        "deleted":     len(deleted),
        "errors":      len(errors),
        "deleted_ids": deleted,
        "error_ids":   errors,
    })


# 비동기 빈 상품 정리 상태 추적
_empty_cleanup_state: dict = {"running": False, "deleted": 0, "errors": 0, "done": False, "started_at": None}

@app.post("/cleanup-empty-products/async")
async def cleanup_empty_products_async():
    """빈 상품 정리를 백그라운드로 실행 (타임아웃 없이). 결과는 /cleanup-empty-products/status 로 확인."""
    if _empty_cleanup_state["running"]:
        return JSONResponse({"status": "already_running", **_empty_cleanup_state})
    asyncio.create_task(_run_cleanup_empty_background())
    return JSONResponse({"status": "accepted", "message": "빈 상품 정리 백그라운드 시작. /cleanup-empty-products/status 로 확인."})

@app.get("/cleanup-empty-products/status")
async def cleanup_empty_products_status():
    return JSONResponse(_empty_cleanup_state)

async def _run_cleanup_empty_background():
    import httpx as _httpx
    import re as _re
    from main import NAVER_BASE as _NAVER_BASE
    global _empty_cleanup_state
    _KST = timezone(timedelta(hours=9))
    _empty_cleanup_state = {"running": True, "deleted": 0, "errors": 0, "done": False, "started_at": datetime.now(_KST).isoformat()}
    deleted: list[str] = []
    errors: list[str] = []

    def _is_bad_product(name: str, price: int) -> bool:
        """삭제 대상: 빈 이름, 너무 짧은 이름, garbled 이름, 0원 상품"""
        if not name or len(name) < 3:
            return True
        if price <= 0:
            return True
        # 의미없는 코드성 이름 (긴 대문자 영어만 있는 경우)
        if _re.search(r'^[A-Z0-9\-_]{5,}$', name):
            return True
        # 상품명이 영어 대문자 4글자 이상 연속 (한국어 상품에 부적절)
        if _re.search(r'[A-Z]{5,}', name) and not _re.search(r'[가-힣]', name):
            return True
        return False

    async def _get_origin(product_id: str):
        try:
            token = await naver_api.get_token()
            async with _httpx.AsyncClient(timeout=15) as _c:
                r = await _c.get(
                    f"{_NAVER_BASE}/v2/products/origin-products/{product_id}",
                    headers={"Authorization": f"Bearer {token}"},
                )
                if r.status_code == 200:
                    return r.json().get("originProduct", {})
                if r.status_code == 404:
                    return None
        except Exception:
            pass
        return {}

    page = 1
    while True:
        try:
            resp = await naver_api.list_products(page=page, size=50)
        except Exception as e:
            _empty_cleanup_state.update({"running": False, "done": True, "error": str(e), "deleted": len(deleted), "errors": len(errors)})
            return
        contents = resp.get("contents", [])
        if not contents:
            break
        for prod in contents:
            product_id = str(prod.get("originProductNo", ""))
            if not product_id:
                continue
            await asyncio.sleep(0.3)
            origin = prod.get("originProduct", {})
            if not origin:
                origin = await _get_origin(product_id)
                if origin is None:
                    # 404: origin 자체가 없으면 채널 상품도 삭제
                    ok = await naver_api.delete_product(product_id)
                    if ok:
                        deleted.append(product_id)
                    else:
                        errors.append(product_id)
                    _empty_cleanup_state["deleted"] = len(deleted)
                    _empty_cleanup_state["errors"] = len(errors)
                    continue
                if not origin:
                    await asyncio.sleep(1.0)
                    origin = await _get_origin(product_id)
                    if origin is None or not origin:
                        # 2회 시도 후도 빈 origin → 유령 상품 삭제
                        print(f"[CLEANUP-EMPTY] 빈 origin 삭제: {product_id}", flush=True)
                        ok = await naver_api.delete_product(product_id)
                        if ok:
                            deleted.append(product_id)
                        else:
                            errors.append(product_id)
                        _empty_cleanup_state["deleted"] = len(deleted)
                        _empty_cleanup_state["errors"] = len(errors)
                        continue
            name = origin.get("name", "").strip() if origin else ""
            price = int(origin.get("salePrice", 0)) if origin else 0
            if _is_bad_product(name, price):
                print(f"[CLEANUP-EMPTY] 삭제 대상: {product_id} name={name!r} price={price}", flush=True)
                ok = await naver_api.delete_product(product_id)
                if ok:
                    deleted.append(product_id)
                else:
                    errors.append(product_id)
                _empty_cleanup_state["deleted"] = len(deleted)
                _empty_cleanup_state["errors"] = len(errors)
        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(0.5)

    _empty_cleanup_state.update({"running": False, "done": True, "deleted": len(deleted), "errors": len(errors)})
    print(f"[CLEANUP-EMPTY] 완료 — 삭제:{len(deleted)}, 실패:{len(errors)}", flush=True)


@app.get("/scan-all-products")
async def scan_all_products(days: int = 365, check_garbled: bool = True):
    """전체 상품 스캔 — 모든 페이지 순회하며 이상 상품(garbled 이름, 0원) 탐색.
    Returns: total_scanned, garbled_names, zero_price, items"""
    import httpx as _httpx
    import re as _re
    try:
        now = datetime.now(timezone.utc)
        token = await naver_api.get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        all_items = []
        garbled = []
        zero_price = []
        page = 1

        def _is_garbled(name: str) -> bool:
            if not name or len(name) < 3:
                return True
            # Detects nonsensical Korean-English mixes or overly random character patterns
            if _re.search(r'[A-Z]{4,}', name):  # Long uppercase English strings in Korean name
                return True
            if _re.search(r'[가-힣][A-Za-z]{4,}[가-힣]', name):  # English word buried mid-Korean
                return True
            return False

        async with _httpx.AsyncClient(timeout=30) as c:
            while True:
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=headers,
                    json={
                        "productStatusTypes": ["SALE", "SUSPENSION"],
                        "page": page,
                        "size": 100,
                        "orderType": "NO",
                        "periodType": "PROD_REG_DAY",
                        "fromDate": (now - timedelta(days=days)).strftime("%Y-%m-%d"),
                        "toDate": now.strftime("%Y-%m-%d"),
                    }
                )
                if r.status_code != 200:
                    break
                data = r.json()
                contents = data.get("contents", [])
                if not contents:
                    break

                for prod in contents:
                    pno = str(prod.get("originProductNo", ""))
                    origin = prod.get("originProduct", {})
                    if not origin and pno:
                        try:
                            dr = await c.get(
                                f"{NAVER_BASE}/v2/products/origin-products/{pno}",
                                headers=headers, timeout=10
                            )
                            if dr.status_code == 200:
                                origin = dr.json().get("originProduct", {})
                        except Exception:
                            pass
                    name = origin.get("name", "")
                    price = origin.get("salePrice", 0)
                    item = {"id": pno, "name": name, "price": price, "status": origin.get("statusType", "")}
                    all_items.append(item)
                    if check_garbled and _is_garbled(name):
                        garbled.append(item)
                    if price == 0 and name:
                        zero_price.append(item)

                if len(contents) < 100:
                    break
                page += 1
                await asyncio.sleep(1.0)

        return JSONResponse({
            "status": "ok",
            "total_scanned": len(all_items),
            "garbled_count": len(garbled),
            "zero_price_count": len(zero_price),
            "garbled": garbled,
            "zero_price": zero_price,
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/sample-products")
async def sample_products(page: int = 1, size: int = 20):
    """상품 샘플 조회 — 이름·가격·상태 요약 (진단용)"""
    try:
        resp = await naver_api.list_products(page=page, size=size)
        contents = resp.get("contents", [])
        items = []
        for p in contents:
            origin = p.get("originProduct", {})
            name = (origin.get("name") or "").strip()
            price = origin.get("salePrice", 0) or 0
            status = origin.get("statusType", "")
            items.append({
                "id": str(p.get("originProductNo", "")),
                "name": name[:50],
                "price": price,
                "status": status,
                "name_len": len(name),
            })
        bad = [x for x in items if not x["name"] or len(x["name"]) < 3 or x["price"] <= 0]
        return JSONResponse({"total_on_page": len(items), "bad_count": len(bad), "items": items, "bad": bad})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/debug-product/{product_no}")
async def debug_product(product_no: str):
    """특정 상품의 originProduct 원본 JSON 반환 — sellerCodeInfo 위치 확인용"""
    try:
        token = await naver_api.get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(
                f"{NAVER_BASE}/v2/products/origin-products/{product_no}",
                headers=headers,
            )
            if r.status_code == 200:
                data = r.json().get("originProduct", {})
                return JSONResponse({
                    "seller_code_top": data.get("sellerCodeInfo"),
                    "seller_code_detail": (data.get("detailAttribute") or {}).get("sellerCodeInfo"),
                    "has_detail_attribute": bool(data.get("detailAttribute")),
                    "detail_attribute_keys": list((data.get("detailAttribute") or {}).keys()),
                    "top_keys": list(data.keys()),
                })
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:300]}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/scan-suspended-products")
async def scan_suspended_products():
    """SUSPENSION(판매중지) + PROHIBITION(판매금지) 상품 목록 조회.
    Returns: total_suspension, items (id, name, price, status)"""
    import httpx as _httpx
    try:
        now = datetime.now(timezone.utc)
        token = await naver_api.get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        items = []
        page = 1
        async with _httpx.AsyncClient(timeout=30) as c:
            while True:
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=headers,
                    json={
                        "productStatusTypes": ["SUSPENSION", "PROHIBITION"],
                        "page": page,
                        "size": 50,
                        "orderType": "NO",
                        "periodType": "PROD_REG_DAY",
                        "fromDate": (now - timedelta(days=365)).strftime("%Y-%m-%d"),
                        "toDate": now.strftime("%Y-%m-%d"),
                    }
                )
                if r.status_code != 200:
                    break
                data = r.json()
                contents = data.get("contents", [])
                if not contents:
                    break
                for prod in contents:
                    pno = str(prod.get("originProductNo", ""))
                    origin = prod.get("originProduct", {})
                    if not origin and pno:
                        try:
                            dr = await c.get(
                                f"{NAVER_BASE}/v2/products/origin-products/{pno}",
                                headers=headers, timeout=10
                            )
                            if dr.status_code == 200:
                                origin = dr.json().get("originProduct", {})
                        except Exception:
                            pass
                    items.append({
                        "id": pno,
                        "name": origin.get("name", ""),
                        "price": origin.get("salePrice", 0),
                        "status": origin.get("statusType", prod.get("statusType", "SUSPENSION")),
                    })
                if len(contents) < 50:
                    break
                page += 1
                await asyncio.sleep(0.5)
        return JSONResponse({"status": "ok", "total_suspension": len(items), "items": items})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/restore-suspended-products")
async def restore_suspended_products():
    """모든 SUSPENSION 상품을 SALE 상태로 복원 (판매 재개)"""
    import httpx as _httpx
    try:
        now = datetime.now(timezone.utc)
        token = await naver_api.get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        restored = []
        failed = []
        page = 1
        async with _httpx.AsyncClient(timeout=30) as c:
            while True:
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=headers,
                    json={
                        "productStatusTypes": ["SUSPENSION"],
                        "page": page,
                        "size": 50,
                        "orderType": "NO",
                        "periodType": "PROD_REG_DAY",
                        "fromDate": (now - timedelta(days=365)).strftime("%Y-%m-%d"),
                        "toDate": now.strftime("%Y-%m-%d"),
                    }
                )
                if r.status_code != 200:
                    break
                data = r.json()
                contents = data.get("contents", [])
                if not contents:
                    break
                for prod in contents:
                    pno = str(prod.get("originProductNo", ""))
                    if not pno:
                        continue
                    ok = await naver_api.set_product_status(pno, "SALE")
                    if ok:
                        restored.append(pno)
                    else:
                        failed.append(pno)
                    await asyncio.sleep(0.3)
                if len(contents) < 50:
                    break
                page += 1
                await asyncio.sleep(1.0)
        return JSONResponse({"status": "ok", "restored": len(restored), "failed": len(failed),
                             "restored_ids": restored, "failed_ids": failed})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ────────────────────────────────────────────────────────────────────────────
# 도매꾹 maker/brand/origin → 스마트스토어 + 쇼피파이 일괄 업데이트
# ────────────────────────────────────────────────────────────────────────────
async def _dg_get_info_ss(dg_no: str) -> dict:
    """도매꾹 getItem API로 maker/brand/origin 조회. 없으면 기본값."""
    DEFAULT = {"maker": "해외브랜드", "brand": "해외브랜드", "origin": "해외"}
    if not DOMEGGOOK_API_KEY or not dg_no:
        return DEFAULT
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://domeggook.com/ssl/api/",
                params={"ver": "4.0", "mode": "getItem", "aid": DOMEGGOOK_API_KEY,
                        "no": dg_no, "otype": "json"},
            )
            if r.status_code == 200:
                d = r.json().get("domeggook", {})
                maker  = str(d.get("maker")  or d.get("manuf")  or "").strip()
                brand  = str(d.get("brand")  or "").strip()
                origin = str(d.get("origin") or d.get("origin_country") or "").strip()
                if maker or brand or origin:
                    return {
                        "maker":  maker  or DEFAULT["maker"],
                        "brand":  brand  or maker or DEFAULT["brand"],
                        "origin": origin or DEFAULT["origin"],
                    }
    except Exception as e:
        print(f"[DG-INFO] getItem 실패({dg_no}): {e}", flush=True)
    return DEFAULT


def _tg_ss(msg: str):
    """동기 Telegram 헬퍼 (asyncio.create_task 래핑)"""
    async def _send():
        import httpx as _httpx
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        if not bot_token or not chat_id:
            return
        try:
            async with _httpx.AsyncClient(timeout=10) as c:
                await c.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
                )
        except Exception:
            pass
    asyncio.create_task(_send())


async def _dg_auto_order_ss(
    dg_no: str, name: str, phone: str, zipcode: str,
    address1: str, address2: str, memo: str,
    order_id: str, product_name: str,
):
    """Bridge /dg-order 호출 (SS용) — 결과 직접 보고."""
    import httpx as _hx
    bridge_url = os.environ.get("DG_BRIDGE_URL", "https://abiding-iron-saddlebag.ngrok-free.dev") + "/dg-order"
    payload = {
        "dg_no": dg_no, "name": name, "phone": phone,
        "zipcode": zipcode, "address1": address1,
        "address2": address2, "memo": memo,
    }
    print(f"[DG-RPA-SS] 발주 시작 no={dg_no} order={order_id}", flush=True)
    try:
        async with _hx.AsyncClient(timeout=300) as c:
            r = await c.post(bridge_url, json=payload)
            result = r.json()
    except Exception as e:
        print(f"[DG-RPA-SS] Bridge 연결 오류: {e}", flush=True)
        _tg_ss(
            f"❌ <b>[SS 도매꾹 RPA 연결 실패]</b>\n"
            f"주문: {order_id} / {product_name[:30]}\n"
            f"Bridge 오류: {e}\n수동: https://domeggook.com/{dg_no}"
        )
        return
    ok = result.get("ok", False)
    dg_order_id = result.get("order_id", "-")
    halt = result.get("halt", False)
    daily = result.get("daily_count", "?")
    print(f"[DG-RPA-SS] 결과 no={dg_no} ok={ok} dg_order={dg_order_id} halt={halt}", flush=True)
    if ok:
        _tg_ss(
            f"✅ <b>[SS 도매꾹 자동발주 완료]</b>\n"
            f"주문: {order_id}\n상품: {product_name[:35]}\n"
            f"도매꾹주문번호: <code>{dg_order_id}</code>\n오늘 발주: {daily}건"
        )
    else:
        err = result.get("error", "알 수 없음")
        _tg_ss(
            f"❌ <b>[SS 도매꾹 자동발주 실패]</b>\n"
            f"주문: {order_id}\n상품: {product_name[:35]}\n오류: {err}\n"
            f"{'⛔ RPA 중단 (연속실패)' if halt else '수동 발주 필요'}\n"
            f"링크: https://domeggook.com/{dg_no}"
        )


async def _onch3_auto_order_ss(
    prd_code: str, option_nm: str, qty: int,
    name: str, phone: str, zipcode: str,
    address1: str, address2: str, memo: str,
    order_id: str, product_name: str,
):
    """온채널 REST API 발주 (SS용) — onch3_sourcing 직접 호출."""
    import onch3_sourcing as _onch3
    print(f"[ONCH3-SS] 발주 시작 code={prd_code} order={order_id}", flush=True)
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: _onch3.place_onch3_order(
                prd_code=prd_code,
                option_nm=option_nm,
                qty=qty,
                order_name=name,
                order_phone=phone,
                zipcode=zipcode,
                address=f"{address1} {address2}".strip(),
                memo=memo,
                sale_code=order_id,
            ),
        )
    except Exception as e:
        print(f"[ONCH3-SS] 발주 예외: {e}", flush=True)
        _tg_ss(
            f"❌ <b>[SS 온채널 발주 예외]</b>\n"
            f"주문: {order_id} / {product_name[:30]}\n오류: {e}"
        )
        return

    ok = result.get("ok", False)
    order_code = result.get("order_code", "-")
    halt = result.get("halt", False)
    daily = result.get("daily_count", "?")
    print(f"[ONCH3-SS] 결과 code={prd_code} ok={ok} onch3_order={order_code} halt={halt}", flush=True)
    if ok:
        _tg_ss(
            f"✅ <b>[SS 온채널 자동발주 완료]</b>\n"
            f"주문: {order_id}\n상품: {product_name[:35]}\n"
            f"온채널주문번호: <code>{order_code}</code>\n오늘 발주: {daily}건"
        )
    else:
        err = result.get("error", "알 수 없음")
        _tg_ss(
            f"❌ <b>[SS 온채널 자동발주 실패]</b>\n"
            f"주문: {order_id}\n상품: {product_name[:35]}\n오류: {err}\n"
            f"{'⛔ 온채널 발주 중단 (연속실패)' if halt else '수동 발주 필요'}"
        )


@app.post("/onch3-order")
async def onch3_order_endpoint(request: Request):
    """온채널 발주 HTTP 엔드포인트 (쿠팡 서비스에서 HTTP 호출).

    Body: {prd_code, option_nm, qty, name, phone, zipcode, address, memo, sale_code}
    Returns: {ok, order_code, error, halt, daily_count, point_after}
    """
    import onch3_sourcing as _onch3
    body = await request.json()
    prd_code    = body.get("prd_code", "")
    option_nm   = body.get("option_nm", prd_code)
    qty         = int(body.get("qty", 1) or 1)
    order_name  = body.get("name", "")
    order_phone = body.get("phone", "")
    zipcode     = body.get("zipcode", "")
    address     = body.get("address", "")
    memo        = body.get("memo", "")
    sale_code   = body.get("sale_code", "")

    if not all([prd_code, order_name, order_phone, zipcode, address]):
        return JSONResponse({"ok": False, "error": "필수 파라미터 누락"}, status_code=400)

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: _onch3.place_onch3_order(
            prd_code=prd_code, option_nm=option_nm, qty=qty,
            order_name=order_name, order_phone=order_phone,
            zipcode=zipcode, address=address, memo=memo, sale_code=sale_code,
        ),
    )
    return JSONResponse(result)


_update_info_ss_running = False

@app.post("/update-product-info")
async def update_product_info_ss():
    """도매꾹 API에서 maker/brand/origin 조회 →
    스마트스토어 manufacturer/brand/origin + 쇼피파이 vendor 일괄 업데이트.
    sellerManagementCode = DG_XXXXX 형식 상품 대상."""
    global _update_info_ss_running
    if _update_info_ss_running:
        return JSONResponse({"status": "running", "message": "이미 실행 중"})
    _update_info_ss_running = True
    asyncio.create_task(_run_update_product_info_ss())
    return JSONResponse({"status": "accepted", "message": "백그라운드 실행 시작. 완료 시 텔레그램 알림."}, status_code=202)


async def _run_update_product_info_ss():
    global _update_info_ss_running
    SHOPIFY_SERVER = "https://shopify-trendify-production.up.railway.app"
    ss_updated, ss_failed, ss_skipped = 0, 0, 0
    shopify_vendor_map: dict[str, str] = {}  # DG번호 → vendor명
    dg_cache: dict[str, dict] = {}

    try:
        print("[UPDATE-INFO-SS] 제조사/브랜드/원산지 업데이트 시작", flush=True)
        # 1. 전체 Naver 상품 페이지네이션
        page = 1
        while True:
            try:
                resp = await naver_api.list_products(page=page, size=50)
            except Exception as e:
                print(f"[UPDATE-INFO-SS] list_products 실패 p{page}: {e}", flush=True)
                break
            contents = resp.get("contents", [])
            print(f"[UPDATE-INFO-SS] p{page} contents={len(contents)}", flush=True)
            if not contents:
                break

            for prod in contents:
                pno    = str(prod.get("originProductNo", ""))
                origin = prod.get("originProduct", {})
                if not pno:
                    continue

                # sellerManagementCode 추출 (detailAttribute.sellerCodeInfo 안에 있음)
                detail_attr = origin.get("detailAttribute") or {}
                code = (detail_attr.get("sellerCodeInfo") or {}).get("sellerManagementCode", "")
                # 폴백: 최상위 sellerCodeInfo (구 버전 상품)
                if not code:
                    code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "")
                print(f"[UPDATE-INFO-SS] pno={pno} code={code!r} origin_empty={not origin}", flush=True)
                if not code or not code.upper().startswith("DG_"):
                    ss_skipped += 1
                    continue
                dg_no = code[3:]  # "DG_12345" → "12345"

                # 도매꾹 조회
                if dg_no not in dg_cache:
                    dg_cache[dg_no] = await _dg_get_info_ss(dg_no)
                    await asyncio.sleep(0.2)
                info = dg_cache[dg_no]
                shopify_vendor_map[dg_no] = info["brand"] or info["maker"]

                # Naver 상품 업데이트 — PUT은 전체 originProduct 필요 → read-modify-write
                _READONLY = {"originProductNo", "channelProductNo", "regDate",
                             "modDate", "statusFrom", "totalSalesQuantity"}
                full_payload = {k: v for k, v in origin.items() if k not in _READONLY}
                # detailAttribute 딥 머지 (기존 필드 유지 + 제조사/브랜드/원산지만 교체)
                da = dict(full_payload.get("detailAttribute") or {})
                da["naverShoppingSearchInfo"] = {
                    **(da.get("naverShoppingSearchInfo") or {}),
                    "manufacturerName": info["maker"][:50],
                    "brandName":        info["brand"][:50],
                }
                da["originAreaInfo"] = {
                    **(da.get("originAreaInfo") or {}),
                    "originAreaCode": "0200037",
                    "content":        info["origin"][:50],
                    "plural":         False,
                    "importer":       "해당없음",
                }
                pnoti = dict(da.get("productInfoProvidedNotice") or {})
                pnoti_etc = dict(pnoti.get("etc") or {})
                pnoti_etc["manufacturer"] = info["maker"][:50]
                pnoti["productInfoProvidedNoticeType"] = "ETC"
                pnoti["etc"] = pnoti_etc
                da["productInfoProvidedNotice"] = pnoti
                full_payload["detailAttribute"] = da
                ok, err = await naver_api.update_product(pno, full_payload)
                if ok:
                    ss_updated += 1
                else:
                    print(f"[UPDATE-INFO-SS] ❌ Naver #{pno}: {err}", flush=True)
                    ss_failed += 1
                await asyncio.sleep(0.3)

            if len(contents) < 50:
                break
            page += 1
            await asyncio.sleep(1.0)

        # 2. Shopify vendor 업데이트 (쇼피파이 서버 호출)
        sh_updated, sh_failed = 0, 0
        if shopify_vendor_map:
            try:
                async with httpx.AsyncClient(timeout=60) as c:
                    r = await c.post(
                        f"{SHOPIFY_SERVER}/update-vendor-by-sku",
                        json={"updates": shopify_vendor_map, "default_vendor": "Overseas Brand"},
                    )
                    if r.status_code == 200:
                        sh_res = r.json()
                        sh_updated = sh_res.get("updated", 0)
                        sh_failed  = sh_res.get("failed", 0)
                    else:
                        print(f"[UPDATE-INFO-SS] Shopify 서버 오류: {r.status_code}", flush=True)
            except Exception as e:
                print(f"[UPDATE-INFO-SS] Shopify 호출 실패: {e}", flush=True)

    except Exception as e:
        print(f"[UPDATE-INFO-SS] 치명적 오류: {e}", flush=True)
    finally:
        _update_info_ss_running = False

    print(f"[UPDATE-INFO-SS] 완료 SS={ss_updated}/{ss_failed}, SH={sh_updated}/{sh_failed}", flush=True)


_sync_done = False  # 동기화 완료 플래그 — 스케줄러가 완료 전 실행되는 것을 막음

async def _sync_registered_codes():
    """registered_codes.json + registered_names.json 동기화.
    - 0개 반환 시 기존 파일 유지 (API 오류 방지)
    - 완료 후 _sync_done=True 설정"""
    global _sync_done
    try:
        from main import REGISTERED_CODES_FILE, REGISTERED_NAMES_FILE, _normalize_name
        import asyncio as _asyncio
        # 서버 시작 직후 API 과부하 방지 — 10초 대기 후 동기화 시작
        await _asyncio.sleep(10)
        codes: set[str] = set()
        names: set[str] = set()
        page = 1
        while True:
            resp = await naver_api.list_products(page=page, size=50)
            contents = resp.get("contents", [])
            if not contents:
                break
            for prod in contents:
                product_no = str(prod.get("originProductNo", ""))
                origin = prod.get("originProduct", {})
                seller_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "")
                prod_name = (origin.get("name") or "").strip()
                if seller_code:
                    codes.add(seller_code)
                elif product_no:
                    codes.add(f"NAVER_ID_{product_no}")
                if prod_name:
                    names.add(_normalize_name(prod_name))
            if len(contents) < 50:
                break
            page += 1
            await _asyncio.sleep(0.5)
        # 0개 반환 시 기존 파일 유지 (API 오류·인증 실패 방어)
        if codes:
            with open(REGISTERED_CODES_FILE, "w", encoding="utf-8") as f:
                json.dump(list(codes), f)
        if names:
            with open(REGISTERED_NAMES_FILE, "w", encoding="utf-8") as f:
                json.dump(list(names), f)
        # context_store도 동기화 — Railway 재시작 후 /tmp 초기화 복원용
        from main import _ctx_set as _mcs
        if codes:
            _mcs("smartstore.registered_codes", list(codes))
        if names:
            _mcs("smartstore.registered_names", list(names))
        print(f"[STARTUP] 동기화 완료: codes={len(codes)}개 / names={len(names)}개", flush=True)
    except Exception as e:
        print(f"[STARTUP] 동기화 실패 (기존 파일 유지): {e}", flush=True)
    finally:
        _sync_done = True


@app.on_event("startup")
async def startup_event():
    """서버 시작 시 Drive 인덱스 자동 복구 + registered_codes 동기화 + 스케줄러 시작"""
    import asyncio as _asyncio
    if not _load_drive_index():
        _save_drive_index(DRIVE_FILE_IDS_PERMANENT)
        print(f"[STARTUP] Drive 인덱스 자동 복구 완료: {len(DRIVE_FILE_IDS_PERMANENT)}개", flush=True)

    # registered_codes.json 동기화를 백그라운드로 실행 (healthcheck 응답 차단 방지)
    _asyncio.create_task(_sync_registered_codes())

    # DG 재고 스캔 미완료 자동 재개 (배포 재시작으로 끊긴 경우)
    async def _resume_dg_scan_if_needed():
        import json as _jj3
        await _asyncio.sleep(15)  # 서버 완전 초기화 + 스케줄러 기동 대기
        try:
            async with httpx.AsyncClient(timeout=8) as _c3:
                _r3 = await _c3.get(
                    "https://loving-serenity-production-2635.up.railway.app/context/ss.dg_stock_scan.progress"
                )
            if _r3.status_code != 200:
                return
            _val3 = _r3.json().get("value", {})
            if isinstance(_val3, str):
                try:
                    prog = _jj3.loads(_val3)
                except Exception:
                    import ast as _ast3
                    prog = _ast3.literal_eval(_val3)
            else:
                prog = _val3
            if prog.get("done"):
                return  # 이미 완료된 스캔
            _all_nos = prog.get("all_nos", [])
            _last_idx = int(prog.get("last_index", -1))
            _total = int(prog.get("total", 0))
            if not _all_nos or _last_idx + 1 >= _total:
                return  # 재개할 항목 없음
            if _dg_stock_state.get("status") == "running":
                return  # 이미 실행 중
            _resume_from = _last_idx + 1
            _dry_run = bool(prog.get("dry_run", False))
            _counts = {
                "checked": prog.get("checked", 0),
                "no_stock_found": prog.get("no_stock_found", 0),
                "suspended": prog.get("suspended", 0),
            }
            print(f"[STARTUP] DG 재고 스캔 미완료 감지 → {_resume_from}/{_total}번째부터 자동 재개", flush=True)
            _asyncio.create_task(_scan_dg_stock_bg(
                dry_run=_dry_run, resume_from=_resume_from,
                resume_nos=_all_nos, resume_counts=_counts,
            ))
        except Exception as _e3:
            print(f"[STARTUP] DG 스캔 재개 체크 실패: {_e3}", flush=True)
    _asyncio.create_task(_resume_dg_scan_if_needed())

    # 오늘 알림된 주문 ID 복원 (재배포 후 중복 알림 방지)
    global _notified_order_ids
    try:
        _today_kst = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")
        async with httpx.AsyncClient(timeout=5) as _c:
            _r = await _c.get(
                f"https://loving-serenity-production-2635.up.railway.app/context/ss.order_notified_ids.{_today_kst}"
            )
        if _r.status_code == 200:
            import json as _jj
            _ids = _r.json().get("value", [])
            if isinstance(_ids, str):
                _ids = _jj.loads(_ids)
            _notified_order_ids.update(_ids or [])
            print(f"[STARTUP] 주문 ID {len(_notified_order_ids)}건 복원 완료", flush=True)
    except Exception as _e:
        print(f"[STARTUP] 주문 ID 복원 스킵: {_e}", flush=True)

    # ── APScheduler: n8n 워크플로우 3개 대체 ─────────────────────────────────
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from employees import (
        employee_stock_guardian, employee_trend_scout,
        employee_accounting_manager, employee_season_planner,
        employee_error_auditor, employee_shortform_creator,
        employee_ad_analyst, employee_review_analyst,
        employee_event_manager, employee_blog_manager,
        employee_platform_expander,
    )
    from main import (
        pipeline_process_orders, pipeline_sync_inventory,
        pipeline_reply_inquiries, pipeline_auto_cleanup,
        _run_advanced_performance_cleanup,
        _run_price_competition_update,
        _run_category_diversity_check,
        _run_review_wishlist_monitor,
        _send_weekly_performance_summary,
        ANTHROPIC_API_KEY, MARGIN_RATE,
    )

    async def job_process_orders():
        print("[SCHED] 주문 처리", flush=True)
        result = await pipeline_process_orders()
        count = result.get("count", 0)
        if count > 0:
            _tg_ss(f"🛒 <b>[스마트스토어] 신규 주문 {count}건</b>")

    async def job_order_checker():
        """30분마다 신규 주문 체크 → 발주확인 자동처리 + 도매꾹 발주링크 + 텔레그램 알림 (중복 방지)"""
        global _notified_order_ids
        try:
            orders = await naver_api.get_all_orders(1)
            new_orders = [o for o in orders
                          if o.get("productOrderId") not in _notified_order_ids]
            new_ids = [o["productOrderId"] for o in new_orders if o.get("productOrderId")]

            # 발주확인 자동처리 (결제완료 → 발주확인)
            if new_ids:
                try:
                    ok = await naver_api.confirm_orders(new_ids)
                    print(f"[ORDER_CHECK] 발주확인 자동처리 {len(new_ids)}건 — {'성공' if ok else '실패'}", flush=True)
                except Exception as _ce:
                    print(f"[ORDER_CHECK] 발주확인 오류(무시): {_ce}", flush=True)

            for o in new_orders:
                oid   = o.get("productOrderId", "?")
                pname = o.get("productName", "상품명 없음")
                qty   = int(o.get("quantity", 1) or 1)
                amt   = int(o.get("totalPaymentAmount", 0) or 0)
                buyer = o.get("buyerName", "?")
                sc    = o.get("sellerProductCode", "")

                # 도매꾹 상품 — RPA 자동발주 (09~24시) 또는 수동링크
                dg_line = ""
                if sc.startswith("DG_"):
                    dg_no = sc[3:]
                    _kst_now = datetime.now(timezone(timedelta(hours=9)))
                    _is_op = 9 <= _kst_now.hour < 24
                    rpa_name  = o.get("shippingName", "").strip()
                    rpa_phone = o.get("shippingTel", "").strip()
                    rpa_zip   = o.get("shippingZipCode", "").strip()
                    rpa_addr1 = o.get("shippingAddr1", "").strip()
                    rpa_addr2 = o.get("shippingAddr2", "").strip()
                    rpa_memo  = (o.get("deliveryMessage") or "부재시 문 앞에 놓아주세요").strip()
                    if all([rpa_name, rpa_phone, rpa_zip, rpa_addr1]) and _is_op:
                        asyncio.create_task(_dg_auto_order_ss(
                            dg_no, rpa_name, rpa_phone, rpa_zip,
                            rpa_addr1, rpa_addr2, rpa_memo, oid, pname,
                        ))
                        dg_line = "\n🤖 도매꾹 RPA 자동발주 실행 중..."
                    else:
                        reason = "운영시간 외" if not _is_op else "배송지 부족"
                        dg_line = f"\n🔗 도매꾹 수동발주({reason}): https://domeggook.com/{dg_no}"
                elif sc.startswith("ONCH3_"):
                    _kst_now = datetime.now(timezone(timedelta(hours=9)))
                    _is_op = 9 <= _kst_now.hour < 24
                    rpa_name  = o.get("shippingName", "").strip()
                    rpa_phone = o.get("shippingTel", "").strip()
                    rpa_zip   = o.get("shippingZipCode", "").strip()
                    rpa_addr1 = o.get("shippingAddr1", "").strip()
                    rpa_addr2 = o.get("shippingAddr2", "").strip()
                    rpa_memo  = (o.get("deliveryMessage") or "부재시 문 앞에 놓아주세요").strip()
                    if all([rpa_name, rpa_phone, rpa_zip, rpa_addr1]) and _is_op:
                        asyncio.create_task(_onch3_auto_order_ss(
                            sc, pname, qty,
                            rpa_name, rpa_phone, rpa_zip,
                            rpa_addr1, rpa_addr2, rpa_memo, oid, pname,
                        ))
                        dg_line = "\n🤖 온채널 자동발주 실행 중..."
                    else:
                        reason = "운영시간 외" if not _is_op else "배송지 부족"
                        dg_line = f"\n🔗 온채널 수동발주({reason}): https://www.onch3.co.kr/seller/orders.php"

                msg = (
                    f"🛒 <b>[스마트스토어 신규주문]</b>\n"
                    f"주문번호: <code>{oid}</code>\n"
                    f"상품명: {pname}\n"
                    f"수량: {qty}개\n"
                    f"결제금액: ₩{amt:,}\n"
                    f"주문자: {buyer}\n"
                    f"✅ 발주확인 자동처리됨{dg_line}\n"
                    f"📦 송장입력: POST /dispatch-invoice"
                )
                _tg_ss(msg)
                _notified_order_ids.add(oid)
            if new_orders:
                print(f"[ORDER_CHECK] 신규 주문 {len(new_orders)}건 알림 완료", flush=True)
                try:
                    _today_kst = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")
                    async with httpx.AsyncClient(timeout=5) as _c:
                        await _c.post(
                            "https://loving-serenity-production-2635.up.railway.app/context",
                            json={
                                "key": f"ss.order_notified_ids.{_today_kst}",
                                "value": list(_notified_order_ids),
                                "category": "system",
                            },
                        )
                except Exception:
                    pass
            else:
                print("[ORDER_CHECK] 신규 주문 없음", flush=True)
        except Exception as e:
            print(f"[ORDER_CHECK] 오류: {e}", flush=True)

    async def job_pending_orders_checker():
        """1시간마다 미배송 주문 체크 → 7일 이상 발송 지연 시 텔레그램 경보"""
        try:
            orders = await naver_api.get_all_orders(10)
            now_utc = datetime.now(timezone.utc)
            delayed = []
            for o in orders:
                if o.get("productOrderStatus") in ("PAYED", "PAYMENT_WAIT"):
                    paid_str = o.get("paymentDate", "")
                    if paid_str:
                        try:
                            paid_dt = datetime.fromisoformat(paid_str.replace("Z", "+00:00"))
                            if (now_utc - paid_dt).days >= 7:
                                delayed.append(o)
                        except Exception:
                            pass
            if delayed:
                lines = "\n".join(
                    f"  • {o.get('productName','?')[:25]} ({o.get('paymentDate','')[:10]})"
                    for o in delayed[:5]
                )
                _tg_ss(
                    f"⚠️ <b>[스마트스토어] 발송 지연 주문 {len(delayed)}건</b>\n"
                    f"7일 이상 미배송:\n{lines}\n"
                    f"📦 도매꾹 발주 및 송장 입력 필요"
                )
                print(f"[PENDING_CHECK] 발송 지연 {len(delayed)}건 경보", flush=True)
        except Exception as e:
            print(f"[PENDING_CHECK] 오류: {e}", flush=True)

    async def job_error_audit():
        print("[SCHED] 에러 감사원", flush=True)
        await employee_error_auditor([], ANTHROPIC_API_KEY)

    async def job_reply_inquiries():
        print("[SCHED] 고객 문의 답변", flush=True)
        await pipeline_reply_inquiries()

    async def job_stock_alert():
        print("[SCHED] 품절 방지 알림이", flush=True)
        files = sorted(Path(EXCEL_FOLDER).glob("*.xlsx"), key=lambda x: x.stat().st_mtime, reverse=True)
        if not files:
            print("[SCHED] 품절 알림이: Excel 파일 없음", flush=True)
            return
        products = parse_excel(str(files[0]))
        low_stock = employee_stock_guardian(products)
        print(f"[SCHED] 재고 부족 상품 {len(low_stock)}개", flush=True)

    async def job_sync_inventory():
        print("[SCHED] 재고 동기화", flush=True)
        await pipeline_sync_inventory()

    async def job_trend_scout():
        print("[SCHED] 트렌드 스카우터", flush=True)
        await employee_trend_scout()

    async def job_fashion_trend_update():
        """주간 — 네이버 쇼핑인사이트 패션의류 핫 키워드(ratio≥15.0) 캐시 갱신."""
        print("[SCHED] 네이버 패션 트렌드 주간 업데이트", flush=True)
        kws = await employee_naver_fashion_trend_scout(
            os.getenv("NAVER_DATALAB_CLIENT_ID", ""),
            os.getenv("NAVER_DATALAB_CLIENT_SECRET", ""),
            ratio_threshold=15.0,
        )
        if kws:
            _employees_module._naver_fashion_trend_cache = kws
            print(f"[SCHED] 패션 트렌드 캐시 갱신 — {len(kws)}개: {kws[:5]}", flush=True)
        else:
            print("[SCHED] 패션 트렌드 — 핫 키워드 없음(ratio<15.0 또는 키 미설정)", flush=True)

    async def job_daily_report():
        print("[SCHED] 일일 리포트", flush=True)
        orders = await naver_api.get_all_orders(1)
        await employee_accounting_manager(orders, MARGIN_RATE)

    async def job_season_plan():
        print("[SCHED] 시즌 기획자", flush=True)
        employee_season_planner()

    async def job_shortform():
        print("[SCHED] 숏폼 제작자", flush=True)
        await employee_shortform_creator(naver_api, ANTHROPIC_API_KEY)

    async def job_ad_analysis():
        print("[SCHED] 광고 분석가", flush=True)
        await employee_ad_analyst(naver_api, ANTHROPIC_API_KEY)

    async def job_event_manager():
        print("[SCHED] 이벤트 매니저", flush=True)
        await employee_event_manager(ANTHROPIC_API_KEY)

    async def job_blog_manager():
        print("[SCHED] 블로그 포스팅 매니저", flush=True)
        await employee_blog_manager(ANTHROPIC_API_KEY)

    async def job_review_analysis():
        print("[SCHED] 리뷰 분석가", flush=True)
        await employee_review_analyst("인기 상품", ANTHROPIC_API_KEY)

    async def job_expand_platform():
        print("[SCHED] 플랫폼 확장 전문가", flush=True)
        await employee_platform_expander(ANTHROPIC_API_KEY)

    async def job_auto_cleanup():
        """매주 월요일 00:00 — 저성과 상품 자동 판매중지"""
        print("[SCHED] 저성과 상품 자동 정리 시작", flush=True)
        try:
            result = await pipeline_auto_cleanup(min_age_days=30, max_views=100)
            print(f"[SCHED] 정리 완료 — 중지:{result['deactivated']}개", flush=True)
        except Exception as e:
            print(f"[SCHED] 자동 정리 오류: {e}", flush=True)

    async def job_register_products():
        """매일 09:00 / 13:00 / 20:00 — 도매꾹 API 소싱 후 상품 등록 (1000개 한도)"""
        if os.getenv("SOURCING_PAUSED", "false").lower() == "true":
            print("[SCHED] SOURCING_PAUSED=true — 신규 소싱 중단 중", flush=True)
            return
        if os.getenv("AUTO_REGISTER_ENABLED", "true").lower() != "true":
            print("[SCHED] 상품 자동 등록 비활성화 (AUTO_REGISTER_ENABLED=false)", flush=True)
            return
        if not _sync_done:
            print("[SCHED] 상품 등록 건너뜀 — registered_codes 동기화 미완료 (중복 방지)", flush=True)
            return
        _codes_now = load_registered_codes()
        _names_now = load_registered_names()
        if len(_codes_now) == 0 and len(_names_now) == 0:
            print("[SCHED] 상품 등록 건너뜀 — 동기화 후에도 codes·names 모두 0개 (비정상 상태, 중복 방지)", flush=True)
            return
        # ── 1000개 한도 체크 ──
        _cur = await naver_api.count_sale_products()
        if _cur >= 1000:
            msg = f"[스마트스토어] 상품 등록 한도 도달 ({_cur}/1000) — 자동 등록 중단"
            print(msg, flush=True)
            await _tg_notify(msg)
            return
        _daily_limit = int(os.getenv("DAILY_SOURCING_LIMIT", "11"))
        _slots = min(_daily_limit, 1000 - _cur)
        print(f"[SCHED] 상품 자동 등록 시작 (현재:{_cur}/1000, 이번:{_slots}개, 일일한도:{_daily_limit})", flush=True)
        try:
            await pipeline_register_from_domeggook(limit=_slots)
        except Exception as e:
            print(f"[SCHED] 상품 등록 오류: {e}", flush=True)

    async def job_pinterest_pin():
        """매일 09:00 / 13:00 / 21:00 — 당일 등록 상품 Pinterest 자동 핀 생성."""
        print("[SCHED] Pinterest 자동 핀 시작", flush=True)
        try:
            result = await pin_recent_smartstore_products(days=1, max_pins=10)
            print(f"[SCHED] Pinterest 핀 완료 — {result.get('pinned', 0)}개", flush=True)
        except Exception as e:
            print(f"[SCHED] Pinterest 핀 오류: {e}", flush=True)

    scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

    # 1시간
    scheduler.add_job(job_process_orders,       "interval", hours=1,    id="process_orders")
    scheduler.add_job(job_order_checker,        "interval", minutes=30, id="order_checker")
    scheduler.add_job(job_error_audit,          "interval", hours=24,   id="error_audit")
    scheduler.add_job(job_pending_orders_checker, "interval", hours=1,  id="pending_orders_checker")
    # 2시간
    scheduler.add_job(job_reply_inquiries, "interval", hours=6,  id="reply_inquiries")
    scheduler.add_job(job_stock_alert,     "interval", hours=6,  id="stock_alert")
    # 6시간
    scheduler.add_job(job_sync_inventory,  "interval", hours=6,  id="sync_inventory")
    scheduler.add_job(job_trend_scout,     "interval", hours=24, id="trend_scout")
    # 매일
    scheduler.add_job(job_daily_report,    "interval", hours=24, id="daily_report")
    scheduler.add_job(job_season_plan,     "interval", hours=24, id="season_plan")
    scheduler.add_job(job_shortform,       "interval", hours=24, id="shortform")
    scheduler.add_job(job_ad_analysis,     "interval", hours=24, id="ad_analysis")
    # 매주
    scheduler.add_job(job_event_manager,        "interval", weeks=1, id="event_manager")
    scheduler.add_job(job_blog_manager,         "interval", weeks=1, id="blog_manager")
    scheduler.add_job(job_review_analysis,      "interval", weeks=1, id="review_analysis")
    scheduler.add_job(job_expand_platform,      "interval", weeks=1, id="expand_platform")
    # 주간 네이버 패션 트렌드 업데이트 (월간→주간 단축)
    scheduler.add_job(job_fashion_trend_update, "interval", weeks=1, id="fashion_trend_update")
    # 09:00 / 13:00 / 20:00 상품 등록
    scheduler.add_job(job_register_products, "cron", hour="9,13,20", minute=0, id="register_products_8")
    # 09:00 / 13:00 / 21:00 Pinterest 자동 핀 (상품 등록 1시간 후)
    scheduler.add_job(job_pinterest_pin, "cron", hour="9,13,21", minute=0, id="pinterest_pin")
    # 매주 월요일 00:00 저성과 상품 정리
    scheduler.add_job(job_auto_cleanup, "cron", day_of_week="mon", hour=0, minute=0, id="auto_cleanup")

    async def job_seo_title_refresh():
        """3일마다 — 네이버 트렌드 키워드로 저성과 상품 제목 갱신."""
        print("[SEO갱신] 스케줄 실행", flush=True)
        try:
            await _run_seo_title_refresh(limit=30)
        except Exception as e:
            print(f"[SEO갱신] 실패: {e}", flush=True)

    # 3일마다 SEO 제목 자동 갱신
    scheduler.add_job(job_seo_title_refresh, "interval", days=3, id="seo_title_refresh")

    # ── 소싱 고도화 v2 스케줄 ────────────────────────────────────────────────
    async def job_advanced_cleanup():
        print("[SCHED] 고도화 성과 정리", flush=True)
        try:
            await _run_advanced_performance_cleanup()
        except Exception as e:
            print(f"[SCHED] 고도화 성과 정리 오류: {e}", flush=True)

    async def job_price_competition():
        print("[SCHED] 가격 경쟁 자동 조정", flush=True)
        try:
            await _run_price_competition_update(limit=30)
        except Exception as e:
            print(f"[SCHED] 가격 조정 오류: {e}", flush=True)

    async def job_category_diversity():
        print("[SCHED] 카테고리 다각화 분석", flush=True)
        try:
            await _run_category_diversity_check()
        except Exception as e:
            print(f"[SCHED] 카테고리 분석 오류: {e}", flush=True)

    async def job_review_monitor():
        print("[SCHED] 리뷰·위시리스트 모니터링", flush=True)
        try:
            await _run_review_wishlist_monitor(limit=20)
        except Exception as e:
            print(f"[SCHED] 리뷰 모니터링 오류: {e}", flush=True)

    async def job_weekly_summary():
        print("[SCHED] 주간 성과 요약 발송", flush=True)
        try:
            await _send_weekly_performance_summary()
        except Exception as e:
            print(f"[SCHED] 주간 요약 오류: {e}", flush=True)

    # 매주 수요일 02:00 — 고도화 성과 정리 (기존 월요일 00:00과 다른 날)
    scheduler.add_job(job_advanced_cleanup,  "cron", day_of_week="wed", hour=2,  minute=0, id="advanced_cleanup")
    # 매일 03:00 — 가격 경쟁 자동 조정
    scheduler.add_job(job_price_competition, "cron", hour=3, minute=0, id="price_competition")
    # 매주 목요일 01:00 — 카테고리 다각화 분석
    scheduler.add_job(job_category_diversity, "cron", day_of_week="thu", hour=1, minute=0, id="category_diversity")
    # 매일 02:30 — 리뷰·위시리스트 모니터링
    scheduler.add_job(job_review_monitor,    "cron", hour=2, minute=30, id="review_monitor")
    # 매주 월요일 09:00 — 주간 성과 요약
    scheduler.add_job(job_weekly_summary,    "cron", day_of_week="mon", hour=9, minute=0, id="weekly_summary")

    # 매일 09:00 — 일일 가격 비교 (7개)
    from main import _run_daily_price_check_ss
    async def _job_daily_price_check_ss():
        try:
            await _run_daily_price_check_ss(limit=7)
        except Exception as e:
            print(f"[SCHED-SS] 일일가격비교 오류: {e}", flush=True)
    scheduler.add_job(_job_daily_price_check_ss, "cron", hour=9, minute=0, id="daily_price_check_ss", replace_existing=True)

    # 매일 03:00 — DG 재고 양방향 동기화 (Phase1: SALE→중지 / Phase2: SUSPENSION→재개)
    async def job_dg_stock_scan():
        if _dg_stock_state.get("status") == "running":
            print("[SCHED] DG 재고 스캔 이미 실행 중 — 스킵", flush=True)
            return
        asyncio.create_task(_scan_dg_stock_bg(dry_run=False))
    scheduler.add_job(job_dg_stock_scan, "cron", hour=3, minute=0, id="dg_stock_scan")

    # ── 01:30 KST 과적가 재검증 + 판매중지/가격조정 ─────────────────────────
    async def _job_overpriced_scan_and_fix():
        """매일 01:30 KST — SALE 상품 경쟁가 재검증 → 판매중지(>3x) / 가격조정(1.5~3x)."""
        from main import search_naver_shopping
        import math as _math
        import httpx as _hx
        print("[OVERPRICED-FIX] 01:30 스캔+처리 시작", flush=True)

        all_products: list[dict] = []
        page = 1
        while True:
            resp = await naver_api.list_products(page=page, size=50)
            contents = resp.get("contents", [])
            if not contents:
                break
            all_products.extend(contents)
            if len(contents) < 50:
                break
            page += 1
            await asyncio.sleep(0.3)

        sale_products = [p for p in all_products if p.get("originProduct", {}).get("statusType") == "SALE"]
        print(f"[OVERPRICED-FIX] SALE 상품 {len(sale_products)}개 검사", flush=True)

        suspended = 0; adjusted = 0; skipped = 0; errors: list[str] = []
        _READONLY = {"originProductNo", "channelProductNo", "regDate", "modDate",
                     "statusFrom", "totalSalesQuantity", "channelProducts"}

        for prod in sale_products:
            name = ""
            try:
                origin     = prod.get("originProduct", {})
                product_no = str(prod.get("originProductNo", ""))
                sale_price = int(origin.get("salePrice") or 0)
                name       = origin.get("name", "")
                if sale_price <= 0 or not product_no:
                    continue

                # DG 코드 → 실시간 도매가 (costPrice 필드 대체)
                from main import _get_dg_wholesale
                dg_code = str(((origin.get("detailAttribute") or {}).get("sellerCodeInfo") or {})
                               .get("sellerManagementCode") or "").strip()
                cost_price = await _get_dg_wholesale(dg_code) if dg_code else 0

                items  = await search_naver_shopping(name[:20], display=10)
                prices = [it["price"] for it in (items or []) if it.get("price", 0) > 0]
                if cost_price > 0:
                    prices = [p for p in prices if p >= cost_price * 0.5]
                if not prices:
                    await asyncio.sleep(0.5)
                    continue

                market_min = min(prices)
                ratio      = sale_price / market_min if market_min > 0 else 999.0
                floor_p    = _math.ceil(cost_price * 1.15 / 10) * 10 if cost_price > 0 else 0

                if ratio <= 1.5:
                    skipped += 1
                    await asyncio.sleep(0.3)
                    continue

                # 경쟁불가(>3x)이고 floor도 최저가 1.5x 초과 → 판매중지
                if ratio > 3.0 and (floor_p == 0 or (floor_p / market_min) > 1.5):
                    ok = await naver_api.set_product_status(product_no, "SUSPENSION")
                    if ok:
                        suspended += 1
                        print(f"[OVERPRICED-FIX] ⛔ 판매중지 {name[:20]} {sale_price:,}원 ratio={ratio:.1f}x", flush=True)
                    else:
                        errors.append(f"{name[:15]}: 판매중지 실패")
                else:
                    # 가격 조정 (1.5~3x 또는 >3x이지만 floor로 커버 가능)
                    if ratio > 3.0:
                        new_price = max(floor_p, _math.ceil(market_min * 1.2 / 10) * 10)
                    else:
                        target    = _math.ceil(market_min * 1.2 / 10) * 10
                        new_price = max(target, floor_p) if floor_p > 0 else target

                    if new_price <= 0 or new_price >= sale_price:
                        skipped += 1
                        await asyncio.sleep(0.3)
                        continue

                    headers = await naver_api._headers()
                    url = f"https://api.commerce.naver.com/external/v2/products/origin-products/{product_no}"
                    async with _hx.AsyncClient(timeout=20) as c:
                        r = await c.get(url, headers=headers)
                        if r.status_code != 200:
                            errors.append(f"{name[:15]}: GET{r.status_code}")
                            await asyncio.sleep(0.5)
                            continue
                        payload = {k: v for k, v in r.json().get("originProduct", {}).items()
                                   if k not in _READONLY}
                        payload["salePrice"] = new_price
                        r2 = await c.put(url, headers=headers, json={"originProduct": payload})
                    if r2.status_code == 200:
                        adjusted += 1
                        print(f"[OVERPRICED-FIX] ✅ {name[:20]} {sale_price:,}→{new_price:,}원 (ratio={ratio:.1f}x)", flush=True)
                    else:
                        errors.append(f"{name[:15]}: PUT{r2.status_code}")
            except Exception as ex:
                errors.append(f"{name[:15] or '?'}: {ex}")
            await asyncio.sleep(0.5)

        print(f"[OVERPRICED-FIX] 완료: 판매중지={suspended} 가격조정={adjusted} 유지={skipped} 오류={len(errors)}", flush=True)
        if errors:
            print(f"[OVERPRICED-FIX] 오류목록: {errors[:5]}", flush=True)

    scheduler.add_job(_job_overpriced_scan_and_fix, "cron", hour=1, minute=30,
                      id="overpriced_scan_fix", replace_existing=True,
                      misfire_grace_time=3600, coalesce=True)
    print("[SERVER] 스케줄러 등록: 과적가 재검증+처리 01:30 KST", flush=True)

    try:
        scheduler.start()
        print("[STARTUP] APScheduler 시작 완료 — n8n 워크플로우 3개 대체", flush=True)
    except Exception as _sched_err:
        print(f"[STARTUP] APScheduler 시작 실패 (서버는 계속 실행됨): {_sched_err}", flush=True)


async def _next_excel_internal() -> str | None:
    """스케줄러용 내부 next-excel — /next-excel 엔드포인트와 동일 로직"""
    file_ids = _load_drive_index()
    if not file_ids:
        file_ids = await _scan_drive_folder()
        if file_ids:
            _save_drive_index(file_ids)
        else:
            file_ids = [FALLBACK_FILE_ID]
    try:
        with open(EXCEL_PROGRESS, encoding="utf-8") as f:
            progress = json.load(f)
    except Exception:
        progress = {"current_index": 0}
    idx = progress.get("current_index", 0) % len(file_ids)
    file_id = file_ids[idx]
    url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"
    import asyncio as _asyncio
    content: bytes | None = None
    for attempt in range(1, 4):
        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
                r = await c.get(url)
                r.raise_for_status()
                content = r.content
            break
        except Exception as exc:
            if attempt < 3:
                print(f"[SCHED/DRIVE] 다운로드 실패({attempt}/3): {exc} — 5s 재시도", flush=True)
                await _asyncio.sleep(5)
            else:
                raise
    save_path = Path(EXCEL_FOLDER) / "ownerclan_latest.xlsx"
    save_path.write_bytes(content)
    progress["current_index"] = (idx + 1) % len(file_ids)
    with open(EXCEL_PROGRESS, "w", encoding="utf-8") as f:
        json.dump(progress, f)
    print(f"[SCHED] Excel 다운로드 완료: {file_id} ({len(r.content)//1024}KB)", flush=True)
    return str(save_path)


# ─── 스마트스토어 내부 AI 직원 활동 로그 (오케스트레이터 보고용) ──────────────────
import time as _time_module
from datetime import datetime as _dt

_TEAM_ACTIVITY: dict[str, dict] = {
    "소싱팀장":           {"calls": 0, "last_run": None, "last_result": ""},
    "IP감시관":           {"calls": 0, "last_run": None, "blocked": 0},
    "시즌기획자":         {"calls": 0, "last_run": None, "upcoming_events": []},
    "트렌드스카우터":     {"calls": 0, "last_run": None, "keywords": []},
    "리뷰분석가":         {"calls": 0, "last_run": None, "last_product": ""},
    "품질검수관":         {"calls": 0, "last_run": None, "pass": 0, "fail": 0},
    "가격최적화분석가":   {"calls": 0, "last_run": None, "avg_margin": 0.0},
    "하이브리드배너생성자": {"calls": 0, "last_run": None, "success": 0, "fail": 0},
}


def _log_employee(name: str, **kwargs) -> None:
    """내부 직원 활동 기록 — pipeline_register_from_domeggook 등에서 호출."""
    if name not in _TEAM_ACTIVITY:
        _TEAM_ACTIVITY[name] = {"calls": 0, "last_run": None}
    _TEAM_ACTIVITY[name]["calls"] = _TEAM_ACTIVITY[name].get("calls", 0) + 1
    _TEAM_ACTIVITY[name]["last_run"] = _dt.now().strftime("%Y-%m-%d %H:%M")
    for k, v in kwargs.items():
        if k in ("blocked", "pass", "fail", "success"):
            _TEAM_ACTIVITY[name][k] = _TEAM_ACTIVITY[name].get(k, 0) + int(v)
        else:
            _TEAM_ACTIVITY[name][k] = v
    # 중앙 DB 기록 (실패해도 무시)
    try:
        from db import log_action as _db_log_action
        action_type = str(kwargs.get("action_type") or "employee_activity")
        _db_log_action(name, action_type, "smartstore", {k: str(v)[:100] for k, v in kwargs.items()})
    except Exception:
        pass


@app.get("/employee-report")
async def employee_report():
    """오케스트레이터 회의에 보고용 — 내부 AI 직원 활동 현황 + 최근 파이프라인 통계."""
    codes = load_registered_codes()
    names = load_registered_names()
    report = {
        "generated_at": _dt.now().strftime("%Y-%m-%d %H:%M KST"),
        "pipeline_stats": {
            "registered_total": len(codes),
            "registered_names": len(names),
        },
        "team_activity": {
            k: {kk: vv for kk, vv in v.items()}
            for k, v in _TEAM_ACTIVITY.items()
        },
    }
    return JSONResponse(report)


# ─── 오케스트레이터 명령 수신 ──────────────────────────────────────────────────
_COMMAND_MAP_SS = {
    "register_domeggook": "도매꾹 상품 등록",
    "register_products":  "엑셀 상품 등록",
    "process_orders":     "주문 처리",
    "sync_inventory":     "재고 동기화",
    "reply_inquiries":    "문의 자동 회신",
    "season_plan":        "시즌 트렌드 기획",
    "trend_scout":        "트렌드 수집",
    "ad_analysis":        "광고 분석",
}

@app.post("/exchange-rate/update")
async def update_exchange_rate(request: Request):
    """환율팀장(오케스트레이터)에서 호출 — KRW_USD_RATE 갱신."""
    import main as _main
    body = await request.json()
    rate = float(body.get("rate", 0))
    if rate <= 0:
        from fastapi.responses import JSONResponse
        return JSONResponse({"status": "error", "message": "rate must be > 0"}, status_code=400)

    old_rate = float(os.environ.get("KRW_USD_RATE", "1350"))
    os.environ["KRW_USD_RATE"] = str(rate)
    print(f"[EXCHANGE] 환율 갱신: {old_rate:.1f} → {rate:.1f} KRW/USD", flush=True)
    return {"status": "ok", "old_rate": old_rate, "new_rate": rate}


@app.post("/command")
async def command_endpoint(request: Request, background_tasks: BackgroundTasks):
    """오케스트레이터에서 명령 수신 → 백그라운드 실행.
    Body: {"command": "register_domeggook", "params": {"limit": 5}, "source": "orchestrator"}"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    cmd = body.get("command", "")
    params = body.get("params", {}) or {}

    if cmd not in _COMMAND_MAP_SS:
        return JSONResponse(
            {"status": "error", "message": f"알 수 없는 명령: {cmd}",
             "available": list(_COMMAND_MAP_SS.keys())},
            status_code=400,
        )

    if cmd == "register_domeggook":
        if not DOMEGGOOK_API_KEY:
            return JSONResponse({"status": "error", "message": "DOMEGGOOK_API_KEY 미설정"}, status_code=400)
        limit      = int(params.get("limit", 5))
        keywords   = params.get("keywords") or _DG_KEYWORDS
        min_price  = int(params.get("min_price", 3000))
        max_price  = int(params.get("max_price", 150000))
        start_page = int(params.get("start_page", 0))
        background_tasks.add_task(pipeline_register_from_domeggook, limit, keywords, min_price, max_price, start_page)
        detail = f"limit={limit}, start_page={start_page if start_page > 0 else 'auto'}"

    elif cmd == "register_products":
        files = sorted(Path(EXCEL_FOLDER).glob("*.xlsx"), key=lambda x: x.stat().st_mtime, reverse=True)
        if not files:
            return JSONResponse({"status": "error", "message": "업로드된 Excel 파일 없음"}, status_code=400)
        excel_path = str(files[0])
        limit = int(params.get("limit", 50))
        background_tasks.add_task(pipeline_register_products, excel_path, limit)
        detail = f"limit={limit}"

    elif cmd == "process_orders":
        background_tasks.add_task(pipeline_process_orders)
        detail = ""

    elif cmd == "sync_inventory":
        background_tasks.add_task(pipeline_sync_inventory)
        detail = ""

    elif cmd == "reply_inquiries":
        background_tasks.add_task(pipeline_reply_inquiries)
        detail = ""

    elif cmd == "season_plan":
        background_tasks.add_task(employee_season_planner)
        detail = ""

    elif cmd == "trend_scout":
        background_tasks.add_task(employee_trend_scout)
        detail = ""

    elif cmd == "ad_analysis":
        background_tasks.add_task(employee_ad_analyst)
        detail = ""

    print(f"[CMD] {_COMMAND_MAP_SS[cmd]} 실행 시작 ({detail})", flush=True)
    return JSONResponse({"status": "accepted", "command": cmd, "label": _COMMAND_MAP_SS[cmd], "params": params})


_HTML_MARKERS = ("hero", "Noto Sans KR")
_READONLY_KEYS = {"originProductNo", "channelProductNo", "regDate", "modDate",
                  "statusFrom", "totalSalesQuantity"}
_FALLBACK_TAGS = ["좋은상품", "추천상품", "베스트상품", "인기상품", "가성비",
                  "프리미엄", "고품질", "특가", "한정수량", "당일배송"]


async def _run_fix_html_all(limit: int = 200) -> None:
    """① 전체 스캔 → ② Claude HTML 재적용 → ③ 중복 제거 → ④ 텔레그램"""
    results = {"scanned": 0, "html_fixed": 0, "html_fail": 0, "dup_deleted": 0, "errors": []}

    # ── ① 전체 상품 수집 (50개씩 페이지) ────────────────────────────────────
    all_items: list[dict] = []
    page = 1
    while True:
        try:
            resp = await naver_api.list_products(page=page, size=50, days=365)
            chunk = resp.get("contents", [])
            if not chunk:
                break
            all_items.extend(chunk)
            total = resp.get("totalElements", len(chunk))
            print(f"[FIXHTML] 페이지 {page}: {len(chunk)}개 (누적 {len(all_items)}/{total})", flush=True)
            if len(all_items) >= total or len(chunk) < 50:
                break
            page += 1
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[FIXHTML] 페이지 {page} 조회 실패: {e}", flush=True)
            break

    results["scanned"] = len(all_items)
    print(f"[FIXHTML] 총 {len(all_items)}개 스캔 완료", flush=True)

    # ── ③ 중복 감지 및 제거 (sellerManagementCode 기준) ─────────────────────
    code_map: dict[str, list[dict]] = {}
    for item in all_items:
        origin = item.get("originProduct", {})
        prod_no = str(item.get("originProductNo", ""))
        seller_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "")
        if seller_code:
            code_map.setdefault(seller_code, []).append({"no": prod_no, "item": item})

    for code, entries in code_map.items():
        if len(entries) <= 1:
            continue
        entries.sort(key=lambda x: int(x["no"]) if x["no"].isdigit() else 0)
        for to_del in entries[:-1]:
            try:
                ok = await naver_api.delete_product(to_del["no"])
                if ok:
                    results["dup_deleted"] += 1
                    print(f"[FIXHTML] 중복 삭제 ✅ {to_del['no']} ({code})", flush=True)
                else:
                    print(f"[FIXHTML] 중복 삭제 실패 {to_del['no']}", flush=True)
            except Exception as e:
                print(f"[FIXHTML] 중복 삭제 오류 {to_del['no']}: {e}", flush=True)
            await asyncio.sleep(0.5)

    # ── ② HTML 미적용 상품 추출 → Claude HTML 재생성 ─────────────────────────
    fix_targets = [
        item for item in all_items
        if not any(m in (item.get("originProduct", {}).get("detailContent", "") or "")
                   for m in _HTML_MARKERS)
    ]
    print(f"[FIXHTML] HTML 미적용: {len(fix_targets)}개 → 재적용 시작", flush=True)

    for idx, item in enumerate(fix_targets[:limit], 1):
        origin = item.get("originProduct", {})
        prod_no = str(item.get("originProductNo", ""))
        name = origin.get("name", "")
        cat = str(origin.get("leafCategoryId") or origin.get("wholeCategoryId") or "")
        price = origin.get("salePrice", 0)
        rep_img = (origin.get("images") or {}).get("representativeImage") or {}
        img_url = rep_img.get("url", "") if isinstance(rep_img, dict) else ""

        print(f"[FIXHTML] ({idx}/{len(fix_targets)}) {name[:35]}", flush=True)
        try:
            p_dict = {"name": name, "category": cat, "price": price, "code": prod_no}
            context = {"season": "", "trends": [], "pain_points": [], "selling_points": []}

            # STEP A: 카피 + 태그
            ai = await generate_product_copy(p_dict, context)
            ai["tags"] = await employee_tag_generator(name, cat, [])
            _, _, ai = _validate_copy_fields(ai)
            tags = list(ai.get("tags") or [])
            while len(tags) < 10:
                tags.append(_FALLBACK_TAGS[len(tags) % len(_FALLBACK_TAGS)])
            ai["tags"] = tags[:10]

            # STEP B: Claude HTML (최대 2회)
            img_urls = [img_url] if img_url else []
            html = await generate_claude_html_detail(p_dict, ai, img_urls)
            ok_html = bool(html) and len(html) >= 5000 and _count_html_sections(html) >= 12
            if not ok_html:
                html2 = await generate_claude_html_detail(p_dict, ai, img_urls)
                if html2 and len(html2) >= 5000 and _count_html_sections(html2) >= 12:
                    html = html2
                    ok_html = True

            if not html:
                results["html_fail"] += 1
                results["errors"].append(f"{name[:25]}: HTML 생성 실패")
                continue

            # STEP C: Naver API 업데이트 (전체 origin merge)
            full_payload = {k: v for k, v in origin.items() if k not in _READONLY_KEYS}
            full_payload["detailContent"] = html
            ok, err = await naver_api.update_product(prod_no, full_payload)
            if ok:
                results["html_fixed"] += 1
                print(f"[FIXHTML] ✅ {name[:35]}", flush=True)
            else:
                results["html_fail"] += 1
                results["errors"].append(f"{name[:25]}: {err[:80]}")
                print(f"[FIXHTML] ❌ {name[:35]}: {err[:80]}", flush=True)

        except Exception as e:
            results["html_fail"] += 1
            results["errors"].append(f"{name[:25]}: {str(e)[:80]}")
            print(f"[FIXHTML] 오류 {name[:35]}: {e}", flush=True)

        await asyncio.sleep(1.5)

    # ── ④ 텔레그램 ──────────────────────────────────────────────────────────
    try:
        status_icon = "✅" if results["html_fail"] == 0 else "⚠️"
        msg = (
            f"{status_icon} 스마트스토어 HTML 재적용 완료\n\n"
            f"📦 전체 스캔: {results['scanned']}개\n"
            f"🔧 HTML 재적용: {results['html_fixed']}개\n"
            f"❌ 실패: {results['html_fail']}개\n"
            f"🗑️ 중복 제거: {results['dup_deleted']}개"
        )
        if results["errors"]:
            msg += "\n\n주요 오류:\n" + "\n".join(f"• {e}" for e in results["errors"][:5])
        await _tg_notify(msg)
    except Exception as e:
        print(f"[FIXHTML] 텔레그램 실패: {e}", flush=True)

    print(f"[FIXHTML] 전체 완료 → {results}", flush=True)


@app.post("/fix-html-all")
async def fix_html_all(background_tasks: BackgroundTasks, limit: int = 200):
    """① 전체 스캔 ② Claude HTML 미적용 상품 재생성 ③ 중복 제거 ④ 텔레그램"""
    background_tasks.add_task(_run_fix_html_all, limit)
    return JSONResponse({
        "status": "accepted",
        "message": f"HTML 재적용 + 중복 제거 시작 (최대 {limit}개)",
    })


async def _run_suspend_excess(keep: int = 99) -> None:
    """판매중 상품 중 최신 keep개만 남기고 나머지 판매중지.
    originProductNo 내림차순(최신순) 정렬 → keep+1번째부터 SUSPENSION."""
    print(f"[SUSPEND-EXCESS] 판매중지 시작 — 최신 {keep}개 유지", flush=True)

    # 1. 전체 SALE 상품 수집
    all_sale: list[dict] = []
    page = 1
    while True:
        try:
            resp = await naver_api.list_products(page=page, size=100, days=3650)
        except Exception as e:
            print(f"[SUSPEND-EXCESS] 목록 조회 오류(p{page}): {e}", flush=True)
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        for prod in contents:
            if prod.get("originProduct", {}).get("statusType") == "SALE":
                all_sale.append(prod)
        if len(contents) < 100:
            break
        page += 1
        await asyncio.sleep(0.5)

    total_sale = len(all_sale)
    excess = total_sale - keep
    print(f"[SUSPEND-EXCESS] SALE 상품 {total_sale}개, 초과 {max(0,excess)}개 판매중지 예정", flush=True)

    if excess <= 0:
        await _tg_notify(f"✅ 판매중지 불필요 — 현재 SALE {total_sale}개 (한도 {keep}개 이하)")
        return

    # 2. originProductNo 내림차순(최신순) 정렬 → 오래된 것이 뒤로
    all_sale.sort(key=lambda x: int(x.get("originProductNo", 0)), reverse=True)
    to_suspend = all_sale[keep:]

    # 대상 목록 텔레그램 발송
    names_preview = "\n".join(
        f"  {i+1}. {p.get('originProduct',{}).get('name','')[:25]} (#{p.get('originProductNo','')})"
        for i, p in enumerate(to_suspend[:20])
    )
    if len(to_suspend) > 20:
        names_preview += f"\n  ... 외 {len(to_suspend)-20}개"
    await _tg_notify(f"[스마트스토어] 판매중지 대상 {len(to_suspend)}개:\n{names_preview}")

    # 3. 1개씩 판매중지
    suspended = 0
    failed = 0
    for i, prod in enumerate(to_suspend):
        pid  = str(prod.get("originProductNo", ""))
        name = prod.get("originProduct", {}).get("name", "")[:30]
        ok   = await naver_api.set_product_status(pid, "SUSPENSION")
        if ok:
            suspended += 1
            print(f"[SUSPEND-EXCESS] ✅ ({i+1}/{len(to_suspend)}) {name}", flush=True)
        else:
            failed += 1
            print(f"[SUSPEND-EXCESS] ❌ ({i+1}/{len(to_suspend)}) {name}", flush=True)
        # 10개마다 중간 보고
        if (i + 1) % 10 == 0:
            await _tg_notify(f"[판매중지] 진행 중 {i+1}/{len(to_suspend)} — 완료:{suspended} 실패:{failed}")
        await asyncio.sleep(0.4)

    # 4. 최종 요약
    final_sale = total_sale - suspended
    msg = (
        f"✅ 판매중지 완료\n"
        f"처리: {len(to_suspend)}개 → 완료:{suspended} 실패:{failed}\n"
        f"현재 SALE 상품: {final_sale}개 (목표 {keep}개)"
    )
    await _tg_notify(msg)
    print(f"[SUSPEND-EXCESS] 완료 — suspended:{suspended} failed:{failed}", flush=True)


@app.post("/suspend-excess")
async def suspend_excess_products(background_tasks: BackgroundTasks, keep: int = 99):
    """판매중 상품 keep개 초과분 판매중지 (최신 keep개 유지, 기본 99개)."""
    background_tasks.add_task(_run_suspend_excess, keep)
    return JSONResponse({
        "status": "processing",
        "message": f"판매중지 작업 시작 — 최신 {keep}개 유지, 초과분 SUSPENSION 처리 중",
    })


@app.post("/batch-suspend")
async def batch_suspend(nos: list[str]):
    """특정 originProductNo 목록을 SUSPENSION 처리 (1개씩 순차, 동기 응답)."""
    ok_list, fail_list = [], []
    for pid in nos:
        ok = await naver_api.set_product_status(pid, "SUSPENSION")
        if ok:
            ok_list.append(pid)
            print(f"[BATCH-SUSPEND] ✅ {pid}", flush=True)
        else:
            fail_list.append(pid)
            print(f"[BATCH-SUSPEND] ❌ {pid}", flush=True)
        await asyncio.sleep(0.4)
    return JSONResponse({"ok": ok_list, "fail": fail_list, "done": True})


async def _run_restore_all_sale() -> None:
    """판매중지(SUSPENSION) 상품 전체를 판매중(SALE)으로 복구."""
    print("[RESTORE-SALE] 전체 복구 시작", flush=True)

    all_suspended: list[dict] = []
    page = 1
    while True:
        try:
            resp = await naver_api.list_products(page=page, size=100, days=3650)
        except Exception as e:
            print(f"[RESTORE-SALE] 목록 조회 오류(p{page}): {e}", flush=True)
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        for prod in contents:
            if prod.get("originProduct", {}).get("statusType") == "SUSPENSION":
                all_suspended.append(prod)
        if len(contents) < 100:
            break
        page += 1
        await asyncio.sleep(0.5)

    total = len(all_suspended)
    print(f"[RESTORE-SALE] SUSPENSION 상품 {total}개 복구 시작", flush=True)

    if total == 0:
        await _tg_notify("✅ 복구 불필요 — 판매중지 상품 없음")
        return

    await _tg_notify(f"[스마트스토어] 판매중지→판매중 복구 시작: {total}개")

    restored = 0
    failed = 0
    for i, prod in enumerate(all_suspended):
        pid    = str(prod.get("originProductNo", ""))
        origin = prod.get("originProduct", {})
        name   = origin.get("name", "")[:30]

        # 전체 페이로드로 SALE 복구 (statusType만 변경 시 Naver API가 거부할 수 있음)
        payload = {k: v for k, v in origin.items() if k not in _READONLY_KEYS}
        payload["statusType"] = "SALE"
        ok, err = await naver_api.update_product(pid, payload)
        if ok:
            restored += 1
            print(f"[RESTORE-SALE] ✅ ({i+1}/{total}) {name}", flush=True)
        else:
            # 전체 페이로드 실패 시 단순 status 변경 시도
            ok2 = await naver_api.set_product_status(pid, "SALE")
            if ok2:
                restored += 1
                print(f"[RESTORE-SALE] ✅(fallback) ({i+1}/{total}) {name}", flush=True)
            else:
                failed += 1
                print(f"[RESTORE-SALE] ❌ ({i+1}/{total}) {name} | {err[:60]}", flush=True)
        if (i + 1) % 10 == 0:
            await _tg_notify(f"[복구] 진행 중 {i+1}/{total} — 완료:{restored} 실패:{failed}")
        await asyncio.sleep(0.4)

    msg = (
        f"✅ 판매중 복구 완료\n"
        f"처리: {total}개 → 복구:{restored} 실패:{failed}"
    )
    await _tg_notify(msg)
    print(f"[RESTORE-SALE] 완료 — restored:{restored} failed:{failed}", flush=True)


@app.post("/restore-all-sale")
async def restore_all_sale(background_tasks: BackgroundTasks):
    """판매중지 상품 전체를 판매중으로 복구."""
    background_tasks.add_task(_run_restore_all_sale)
    return JSONResponse({
        "status": "processing",
        "message": "판매중지 → 판매중 복구 시작 — 텔레그램으로 진행 상황 전송",
    })


@app.get("/product-count")
async def product_count():
    """네이버 API 기준 실제 SALE / SUSPENSION 수 조회.
    toDate=오늘 + 단일 상태 필터 버그로 인해 SALE/SUSPENSION 각각 직접 조회."""
    import httpx as _hx
    from main import NAVER_BASE
    _params = {
        "page": 1, "size": 1,
        "orderType": "NO",
        "periodType": "PROD_REG_DAY",
        "fromDate": "2020-01-01",
        "toDate": "2099-12-31",
    }
    headers = await naver_api._headers()
    async with _hx.AsyncClient(timeout=15) as c:
        r_sale = await c.post(f"{NAVER_BASE}/v1/products/search", headers=headers,
                              json={**_params, "productStatusTypes": ["SALE"]})
        r_sus  = await c.post(f"{NAVER_BASE}/v1/products/search", headers=headers,
                              json={**_params, "productStatusTypes": ["SUSPENSION"]})
    sale  = int(r_sale.json().get("totalElements", 0)) if r_sale.status_code == 200 else -1
    sus   = int(r_sus.json().get("totalElements",  0)) if r_sus.status_code == 200 else -1
    return JSONResponse({"sale": sale, "suspension": sus, "total": max(0, sale) + max(0, sus)})


@app.get("/debug-restore-one")
async def debug_restore_one():
    """SUSPENSION 상품 1개를 SALE로 복구 시도 — API 오류 응답 전체 반환 (디버그용)."""
    import httpx as _hx
    from main import NAVER_BASE

    headers = await naver_api._headers()
    async with _hx.AsyncClient(timeout=30) as c:
        # 1) 원시 검색 API — SUSPENSION 상품 ID 직접 조회
        r_search = await c.post(
            f"{NAVER_BASE}/v1/products/search",
            headers=headers,
            json={
                "productStatusTypes": ["SUSPENSION"],
                "page": 1, "size": 3,
                "orderType": "NO",
                "periodType": "PROD_REG_DAY",
                "fromDate": "2020-01-01",
                "toDate": "2030-12-31",
            },
        )
        search_data = r_search.json() if r_search.status_code == 200 else {}
        contents = search_data.get("contents", [])
        total = search_data.get("totalElements", 0)

        if not contents:
            return JSONResponse({
                "search_status": r_search.status_code,
                "search_body": r_search.text[:800],
                "total_suspension": total,
                "error": "검색 결과 없음",
            })

        pid = str(contents[0].get("originProductNo", ""))

        # 2) 상세 조회
        r_detail = await c.get(
            f"{NAVER_BASE}/v2/products/origin-products/{pid}",
            headers=headers,
        )
        origin = {}
        if r_detail.status_code == 200:
            origin = r_detail.json().get("originProduct", {})

        # 3) 방법 A: 전체 페이로드 + statusType=SALE
        payload_a = {k: v for k, v in origin.items() if k not in _READONLY_KEYS}
        payload_a["statusType"] = "SALE"
        r_a = await c.put(
            f"{NAVER_BASE}/v2/products/origin-products/{pid}",
            headers=headers,
            json={"originProduct": payload_a},
        )

        # 4) 방법 B: 최소 페이로드
        r_b = await c.put(
            f"{NAVER_BASE}/v2/products/origin-products/{pid}",
            headers=headers,
            json={"originProduct": {"statusType": "SALE"}},
        )

    return JSONResponse({
        "total_suspension": total,
        "product_id": pid,
        "detail_status": r_detail.status_code,
        "origin_statusType": origin.get("statusType"),
        "origin_keys": list(origin.keys()),
        "payload_a_keys": list(payload_a.keys()),
        "method_a": {"http": r_a.status_code, "body": r_a.text[:600]},
        "method_b": {"http": r_b.status_code, "body": r_b.text[:600]},
    })


@app.get("/debug-status-scan")
async def debug_status_scan():
    """모든 상태 유형별 상품 수 조회 — 실제 상태 파악용."""
    import httpx as _hx
    from main import NAVER_BASE

    all_statuses = ["SALE", "SUSPENSION", "OUTOFSTOCK", "CLOSE", "WAIT"]
    headers = await naver_api._headers()
    results = {}

    async with _hx.AsyncClient(timeout=30) as c:
        for st in all_statuses:
            r = await c.post(
                f"{NAVER_BASE}/v1/products/search",
                headers=headers,
                json={
                    "productStatusTypes": [st],
                    "page": 1, "size": 1,
                    "orderType": "NO",
                    "periodType": "PROD_REG_DAY",
                    "fromDate": "2020-01-01",
                    "toDate": "2030-12-31",
                },
            )
            results[st] = r.json().get("totalElements", f"err:{r.status_code}")

        # ALL 상태 조합
        r_all = await c.post(
            f"{NAVER_BASE}/v1/products/search",
            headers=headers,
            json={
                "productStatusTypes": ["SALE", "SUSPENSION", "OUTOFSTOCK", "CLOSE", "WAIT"],
                "page": 1, "size": 5,
                "orderType": "NO",
                "periodType": "PROD_REG_DAY",
                "fromDate": "2020-01-01",
                "toDate": "2030-12-31",
            },
        )
        all_data = r_all.json()
        sample_products = []
        for p in all_data.get("contents", []):
            prod_no = p.get("originProductNo")
            if prod_no:
                rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{prod_no}", headers=headers)
                if rd.status_code == 200:
                    orig = rd.json().get("originProduct", {})
                    sample_products.append({
                        "id": prod_no,
                        "name": orig.get("name", "")[:20],
                        "status": orig.get("statusType"),
                    })

    return JSONResponse({
        "by_status": results,
        "all_total": all_data.get("totalElements", 0),
        "sample_products": sample_products,
    })


# ─── DG 재고 스캔 + 판매중지 ──────────────────────────────────────────────────
_dg_stock_state: dict = {}


async def _scan_dg_stock_bg(dry_run: bool = False, resume_from: int = 0,
                             resume_nos: list | None = None,
                             resume_counts: dict | None = None):
    import httpx as _hx, asyncio as _aio, random as _rnd, time as _time, re as _re
    from main import NAVER_BASE, _ctx_set as _mcs
    global _dg_stock_state
    _dg_stock_state = {
        "status": "running", "dry_run": dry_run,
        "total": 0, "checked": 0, "no_dg_code": 0,
        "no_stock_found": 0, "suspended": 0,
        "no_cost_price": [], "errors": [], "done": False,
        "suspended_items": [], "start_ts": _time.time(),
        # Phase 2: 재입고 감지
        "susp_total": 0, "restocked": 0, "restock_skipped": 0, "restock_items": [],
    }
    # resume 시 이전 카운터 복원
    if resume_counts:
        _dg_stock_state.update({
            "checked":       resume_counts.get("checked", 0),
            "no_stock_found": resume_counts.get("no_stock_found", 0),
            "suspended":     resume_counts.get("suspended", 0),
        })

    hdrs = await naver_api._headers()
    dg_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }

    # 1. 전체 SALE 상품 목록 수집 (resume 시 기존 목록 재사용)
    if resume_nos:
        all_nos = list(resume_nos)
        print(f"[DG_SCAN] 재개 모드: {resume_from}번째부터 / 전체 {len(all_nos)}개", flush=True)
    else:
        _raw_products = []
        page = 1
        while True:
            try:
                async with _hx.AsyncClient(timeout=20) as c:
                    r = await c.post(
                        f"{NAVER_BASE}/v1/products/search",
                        headers=hdrs,
                        json={
                            "productStatusTypes": ["SALE"],
                            "page": page, "size": 100,
                            "orderType": "NO",
                            "periodType": "PROD_REG_DAY",
                            "fromDate": "2020-01-01",
                            "toDate": "2030-12-31",
                        }
                    )
                data = r.json()
                contents = data.get("contents", [])
                if not contents:
                    break
                _raw_products.extend(contents)
                if len(contents) < 100:
                    break
                page += 1
                await _aio.sleep(1)
            except Exception as e:
                _dg_stock_state["errors"].append(f"list page{page}: {str(e)[:80]}")
                break
        all_nos = [str(p.get("originProductNo", "") or "") for p in _raw_products]

    _dg_stock_state["total"] = len(all_nos)

    # 2. 각 origin_no 순회 → DG 재고 확인 → 판매중지 (resume_from 이전 인덱스 스킵)
    for _scan_idx, origin_no in enumerate(all_nos):
        if _scan_idx < resume_from:
            continue
        prod_name = ""

        # origin-product 상세 (DG코드 + costPrice + 이름)
        dg_code = ""
        cost_price = None
        try:
            async with _hx.AsyncClient(timeout=15) as c:
                rd = await c.get(
                    f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                    headers=hdrs,
                )
            if rd.status_code == 200:
                od = rd.json().get("originProduct", {})
                prod_name = str(od.get("name", ""))[:40]
                cost_price = od.get("costPrice")
                seller_info = od.get("detailAttribute", {}).get("sellerCodeInfo", {})
                dg_code = str(seller_info.get("sellerManagementCode", "") or "").strip()
        except Exception as e:
            _dg_stock_state["errors"].append(f"{origin_no} detail: {str(e)[:60]}")

        # costPrice 미입력 목록
        if not cost_price:
            _dg_stock_state["no_cost_price"].append({
                "origin_no": origin_no, "name": prod_name, "dg_code": dg_code,
            })

        # DG 코드 파싱 ("DG_12345" 또는 "12345" 두 형식 모두 허용)
        if dg_code.upper().startswith("DG_"):
            item_no = dg_code[3:].strip()
        elif dg_code.isdigit():
            item_no = dg_code
        else:
            _dg_stock_state["no_dg_code"] += 1
            _dg_stock_state["checked"] += 1
            continue

        # DG 페이지 재고 확인
        try:
            async with _hx.AsyncClient(timeout=12, follow_redirects=True) as c:
                r_dg = await c.get(
                    f"https://domeggook.com/main/item/itemView.php?no={item_no}",
                    headers=dg_headers,
                )
            no_stock = (
                "재고가 없는 상품" in r_dg.text
                or r_dg.status_code == 404
            )
            if no_stock:
                _dg_stock_state["no_stock_found"] += 1
                if not dry_run:
                    # read-modify-write 방식으로 판매중지 (단순 PUT은 실패 가능)
                    try:
                        async with _hx.AsyncClient(timeout=20) as c2:
                            rg = await c2.get(
                                f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                                headers=hdrs,
                            )
                            if rg.status_code == 200:
                                _SKIP = {"originProductNo","channelProductNo","regDate","modDate",
                                         "statusFrom","totalSalesQuantity","channelProducts"}
                                payload = {k: v for k, v in rg.json().get("originProduct", {}).items()
                                           if k not in _SKIP}
                                payload["statusType"] = "SUSPENSION"
                                rp = await c2.put(
                                    f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                                    headers=hdrs, json={"originProduct": payload},
                                )
                                if rp.status_code == 200:
                                    _dg_stock_state["suspended"] += 1
                                    _dg_stock_state["suspended_items"].append({
                                        "origin_no": origin_no, "name": prod_name, "dg_code": dg_code,
                                    })
                    except Exception as e2:
                        _dg_stock_state["errors"].append(f"{origin_no} suspend: {str(e2)[:60]}")
        except Exception as e:
            _dg_stock_state["errors"].append(f"{dg_code} DG: {str(e)[:60]}")

        _dg_stock_state["checked"] += 1

        # 50개마다 중간 저장 (resume 재개를 위해 last_index + all_nos 포함)
        if _dg_stock_state["checked"] % 50 == 0:
            try:
                _mcs("ss.dg_stock_scan.progress", {
                    "checked": _dg_stock_state["checked"],
                    "total": _dg_stock_state["total"],
                    "suspended": _dg_stock_state["suspended"],
                    "no_stock_found": _dg_stock_state["no_stock_found"],
                    "last_index": _scan_idx,
                    "all_nos": all_nos,
                    "dry_run": dry_run,
                    "done": False,
                })
            except Exception:
                pass

        # DG 차단 방지 딜레이 (3~5초)
        await _aio.sleep(_rnd.uniform(3.0, 5.0))

    # ── Phase 2: SUSPENSION → SALE 재입고 감지 ──────────────────────────────
    _dg_stock_state["status"] = "restock_scan"
    print("[RESTOCK] Phase2 시작 — SUSPENSION 상품 재입고 확인", flush=True)

    # 토큰 갱신 (Phase1이 길었을 수 있음)
    hdrs = await naver_api._headers()

    susp_products: list = []
    page = 1
    while True:
        try:
            async with _hx.AsyncClient(timeout=20) as c:
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=hdrs,
                    json={
                        "productStatusTypes": ["SUSPENSION"],
                        "page": page, "size": 100,
                        "orderType": "NO",
                        "periodType": "PROD_REG_DAY",
                        "fromDate": "2020-01-01",
                        "toDate": "2030-12-31",
                    }
                )
            contents = r.json().get("contents", [])
            if not contents:
                break
            susp_products.extend(contents)
            if len(contents) < 100:
                break
            page += 1
            await _aio.sleep(1)
        except Exception as e:
            _dg_stock_state["errors"].append(f"susp list p{page}: {str(e)[:80]}")
            break

    _dg_stock_state["susp_total"] = len(susp_products)
    print(f"[RESTOCK] SUSPENSION 상품 {len(susp_products)}개 수집", flush=True)

    _susp_checked = 0
    for prod in susp_products:
        origin_no = str(prod.get("originProductNo", "") or "")
        prod_name = ""
        dg_code = ""

        # 50개마다 토큰 갱신
        if _susp_checked > 0 and _susp_checked % 50 == 0:
            hdrs = await naver_api._headers()

        # origin-product 상세 조회 (DG코드)
        try:
            async with _hx.AsyncClient(timeout=15) as c:
                rd = await c.get(
                    f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                    headers=hdrs,
                )
            if rd.status_code == 200:
                od = rd.json().get("originProduct", {})
                prod_name = str(od.get("name", ""))[:40]
                seller_info = od.get("detailAttribute", {}).get("sellerCodeInfo", {})
                dg_code = str(seller_info.get("sellerManagementCode", "") or "").strip()
        except Exception as e:
            _dg_stock_state["errors"].append(f"{origin_no} susp_detail: {str(e)[:60]}")

        # DG 코드 파싱
        if dg_code.upper().startswith("DG_"):
            item_no = dg_code[3:].strip()
        elif dg_code.isdigit():
            item_no = dg_code
        else:
            _dg_stock_state["restock_skipped"] += 1
            _susp_checked += 1
            continue

        # DG 재고 확인
        try:
            async with _hx.AsyncClient(timeout=12, follow_redirects=True) as c:
                r_dg = await c.get(
                    f"https://domeggook.com/main/item/itemView.php?no={item_no}",
                    headers=dg_headers,
                )
            no_stock = ("재고가 없는 상품" in r_dg.text or r_dg.status_code == 404)
            if not no_stock:
                # 재입고 감지 — 새 도매가 파싱
                text = r_dg.text
                m = (_re.search(r'["\']?baseAmtDome["\']?\s*[:=]\s*["\']?(\d+)', text)
                     or _re.search(r'optionPrice["\']?\s*[:=]\s*["\']?(\d+)', text))
                new_wholesale = int(m.group(1)) if m else 0

                if new_wholesale <= 0:
                    _dg_stock_state["restock_skipped"] += 1
                    _susp_checked += 1
                    await _aio.sleep(_rnd.uniform(3.0, 5.0))
                    continue

                # 새 판매가: 기본 ×2.2, floor=도매가×1.15, 10원 단위
                new_sale_price = round(max(new_wholesale * 2.2, new_wholesale * 1.15) / 10) * 10

                from main import (
                    get_category_id as _get_cat_id,
                    _dg_to_product as _dg_to_prod_fn,
                    _dg_item_detail as _dg_detail_fn,
                    DEFAULT_CATEGORY_ID as _DEFAULT_CAT,
                )
                _SKIP2 = {"originProductNo","channelProductNo","regDate","modDate",
                          "statusFrom","totalSalesQuantity","channelProducts"}

                if not dry_run:
                    try:
                        async with _hx.AsyncClient(timeout=15) as c2:
                            rg = await c2.get(
                                f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                                headers=hdrs,
                            )
                        if rg.status_code == 200:
                            _rg_op = rg.json().get("originProduct", {})
                            cur_cat = _rg_op.get("leafCategoryId", _DEFAULT_CAT)
                            _reregistered = False

                            # ── 카테고리 재판정: 잘못된 기본값(50002480)이면 삭제+재등록 ──
                            if cur_cat == _DEFAULT_CAT:
                                new_cat = _get_cat_id({"name": prod_name})
                                if new_cat != _DEFAULT_CAT:
                                    print(f"[RESTOCK] 카테고리 재판정 {prod_name[:20]}: {cur_cat}→{new_cat}, 삭제+재등록", flush=True)
                                    try:
                                        _dg_det = await _dg_detail_fn(item_no)
                                        if _dg_det:
                                            _item_stub = {
                                                "no": item_no,
                                                "title": _dg_det.get("basis", {}).get("title") or prod_name,
                                                "thumb": (_dg_det.get("thumb") or {}).get("original", ""),
                                                "price": (_dg_det.get("price") or {}).get("dome", 0),
                                                "deli": 0,
                                            }
                                            _new_payload = _dg_to_prod_fn(_item_stub, _dg_det)
                                            if _new_payload:
                                                _new_payload["salePrice"] = new_sale_price
                                                _new_payload["costPrice"] = new_wholesale
                                                _new_payload["statusType"] = "SALE"
                                                async with _hx.AsyncClient(timeout=15) as c_del:
                                                    await c_del.delete(
                                                        f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                                                        headers=hdrs,
                                                    )
                                                _new_no = await naver_api.register_product(_new_payload)
                                                if _new_no:
                                                    _reregistered = True
                                                    _dg_stock_state["restocked"] += 1
                                                    _dg_stock_state["restock_items"].append({
                                                        "origin_no": _new_no, "name": prod_name,
                                                        "dg_code": dg_code,
                                                        "new_wholesale": new_wholesale,
                                                        "new_sale_price": new_sale_price,
                                                        "category_fixed": f"{cur_cat}→{new_cat}",
                                                    })
                                                    print(f"[RESTOCK] ✅ {prod_name[:20]} 카테고리교정+재등록: cat={new_cat}", flush=True)
                                    except Exception as e_rr:
                                        _dg_stock_state["errors"].append(f"{origin_no} re-register: {str(e_rr)[:80]}")

                            # ── 재등록 미실행이면 일반 판매재개 PUT ──
                            if not _reregistered:
                                payload2 = {k: v for k, v in _rg_op.items() if k not in _SKIP2}
                                payload2["statusType"] = "SALE"
                                payload2["salePrice"] = new_sale_price
                                payload2["costPrice"] = new_wholesale
                                payload2.setdefault("detailAttribute", {})["unitCapacity"] = {"unitPriceYn": False}
                                async with _hx.AsyncClient(timeout=20) as c2:
                                    rp = await c2.put(
                                        f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                                        headers=hdrs, json={"originProduct": payload2},
                                    )
                                if rp.status_code == 200:
                                    _dg_stock_state["restocked"] += 1
                                    _dg_stock_state["restock_items"].append({
                                        "origin_no": origin_no, "name": prod_name,
                                        "dg_code": dg_code,
                                        "new_wholesale": new_wholesale,
                                        "new_sale_price": new_sale_price,
                                    })
                                    print(f"[RESTOCK] ✅ {prod_name} 재개: 도매가={new_wholesale:,} 판매가={new_sale_price:,}", flush=True)
                                else:
                                    _dg_stock_state["errors"].append(
                                        f"{origin_no} restock PUT {rp.status_code}: {rp.text[:80]}")
                    except Exception as e2:
                        _dg_stock_state["errors"].append(f"{origin_no} restock: {str(e2)[:60]}")
                else:
                    # dry_run: 상품명 기반 카테고리 재판정 결과만 기록 (API 호출 없음)
                    _dry_new_cat = _get_cat_id({"name": prod_name})
                    _cat_note = f"재등록예정({_DEFAULT_CAT}→{_dry_new_cat})" if _dry_new_cat != _DEFAULT_CAT else "카테고리정상→재개만"
                    _dg_stock_state["restock_items"].append({
                        "origin_no": origin_no, "name": prod_name, "dg_code": dg_code,
                        "new_wholesale": new_wholesale, "new_sale_price": new_sale_price,
                        "dry_run": True, "category_note": _cat_note,
                    })
                    print(f"[RESTOCK][DRY] {prod_name} 재입고 감지: 도매가={new_wholesale:,} 판매가={new_sale_price:,} cat={_cat_note}", flush=True)
        except Exception as e:
            _dg_stock_state["errors"].append(f"{dg_code} DG restock: {str(e)[:60]}")

        _susp_checked += 1
        await _aio.sleep(_rnd.uniform(3.0, 5.0))

    print(f"[RESTOCK] Phase2 완료 — 재개:{_dg_stock_state['restocked']}개 / 스킵:{_dg_stock_state['restock_skipped']}개", flush=True)
    # ── Phase 2 끝 ──────────────────────────────────────────────────────────

    _dg_stock_state["done"] = True
    _dg_stock_state["status"] = "done"
    _dg_stock_state["elapsed_min"] = round((_time.time() - _dg_stock_state["start_ts"]) / 60, 1)

    # 최종 결과 저장 (progress에도 done=True → 재시작 시 resume 스킵)
    try:
        _mcs("ss.dg_stock_scan.progress", {"done": True, "total": _dg_stock_state["total"],
                                            "checked": _dg_stock_state["checked"]})
    except Exception:
        pass
    try:
        _mcs("ss.dg_stock_scan.result", {
            "done": True,
            "total": _dg_stock_state["total"],
            "suspended": _dg_stock_state["suspended"],
            "no_stock_found": _dg_stock_state["no_stock_found"],
            "no_cost_count": len(_dg_stock_state["no_cost_price"]),
            "elapsed_min": _dg_stock_state["elapsed_min"],
            "suspended_items": _dg_stock_state["suspended_items"],
            "susp_total": _dg_stock_state["susp_total"],
            "restocked": _dg_stock_state["restocked"],
            "restock_items": _dg_stock_state["restock_items"],
        })
    except Exception:
        pass


@app.post("/scan-dg-stock")
async def scan_dg_stock_start(request: Request, background_tasks: BackgroundTasks):
    """🔍 DG 재고 없는 상품 판매중지 (백그라운드, 3~5초 간격). dry_run=true면 중지 없이 목록만."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    dry_run = bool(body.get("dry_run", False))
    if _dg_stock_state.get("status") == "running":
        return JSONResponse({
            "status": "already_running",
            "checked": _dg_stock_state.get("checked", 0),
            "total": _dg_stock_state.get("total", 0),
        })
    background_tasks.add_task(_scan_dg_stock_bg, dry_run)
    return JSONResponse({"status": "started", "dry_run": dry_run, "result_url": "/scan-dg-stock-result"})


@app.get("/scan-dg-stock-result")
async def scan_dg_stock_result():
    """📊 DG 재고 스캔 진행상황 조회"""
    s = _dg_stock_state
    if not s:
        return JSONResponse({"status": "not_started"})
    return JSONResponse({
        "status": s.get("status", "running"),
        "done": s.get("done", False),
        "dry_run": s.get("dry_run", False),
        "total": s.get("total", 0),
        "checked": s.get("checked", 0),
        "no_dg_code": s.get("no_dg_code", 0),
        "no_stock_found": s.get("no_stock_found", 0),
        "suspended": s.get("suspended", 0),
        "no_cost_price_count": len(s.get("no_cost_price", [])),
        "no_cost_price_sample": s.get("no_cost_price", [])[:10],
        "suspended_items_recent": s.get("suspended_items", [])[-20:],
        "susp_total": s.get("susp_total", 0),
        "restocked": s.get("restocked", 0),
        "restock_skipped": s.get("restock_skipped", 0),
        "restock_items": s.get("restock_items", []),
        "errors": s.get("errors", [])[-10:],
        "elapsed_min": s.get("elapsed_min"),
        "progress_pct": round(s.get("checked", 0) / max(s.get("total", 1), 1) * 100, 1),
    })


@app.get("/product-status-counts")
async def product_status_counts():
    """📊 Naver API totalElements 기준 상태별 정확한 상품 수 집계"""
    base = {
        "page": 1, "size": 1,
        "orderType": "NO",
        "periodType": "PROD_REG_DAY",
        "fromDate": "2020-01-01",
        "toDate": "2099-12-31",
    }
    import httpx as _hx
    hdrs = await naver_api._headers()
    async with _hx.AsyncClient(timeout=20) as c:
        r_sale = await c.post(f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                              json={**base, "productStatusTypes": ["SALE"]})
        r_susp = await c.post(f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                              json={**base, "productStatusTypes": ["SUSPENSION"]})
        r_oos  = await c.post(f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                              json={**base, "productStatusTypes": ["OUTOFSTOCK"]})
        r_wait = await c.post(f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                              json={**base, "productStatusTypes": ["WAIT"]})
        r_close= await c.post(f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                              json={**base, "productStatusTypes": ["CLOSE"]})
    sale  = int(r_sale.json().get("totalElements", 0)) if r_sale.is_success else -1
    susp  = int(r_susp.json().get("totalElements", 0)) if r_susp.is_success else -1
    oos   = int(r_oos.json().get("totalElements",  0)) if r_oos.is_success  else -1
    wait  = int(r_wait.json().get("totalElements", 0)) if r_wait.is_success else -1
    close = int(r_close.json().get("totalElements",0)) if r_close.is_success else -1
    total = sum(v for v in [sale, susp, oos, wait, close] if v >= 0)
    return JSONResponse({
        "SALE": sale,
        "SUSPENSION": susp,
        "OUTOFSTOCK": oos,
        "WAIT": wait,
        "CLOSE": close,
        "total": total,
    })


# ─── 속성(attributeInfo) 검증·채움 ────────────────────────────────────────────

@app.get("/naver-raw/{origin_no}")
async def naver_raw_product(origin_no: str):
    """Naver API 원문 — attributeInfo 포함 전체 반환 (검증용)."""
    import httpx as _hx
    from main import NAVER_BASE
    try:
        hdrs = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{origin_no}", headers=hdrs)
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:300]}, status_code=400)
        op = r.json().get("originProduct", {})
        return {
            "origin_no": origin_no,
            "name": op.get("name", ""),
            "leafCategoryId": op.get("leafCategoryId"),
            "statusType": op.get("statusType", ""),
            "attributeInfo": op.get("attributeInfo"),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


@app.get("/naver-category-raw/{category_id}")
async def naver_category_raw(category_id: int):
    """Naver 속성 API 원문 전체 반환 (디버깅용)."""
    import httpx as _hx
    from main import NAVER_BASE
    hdrs = await naver_api._headers()
    async with _hx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{NAVER_BASE}/v1/product-attributes/attributes",
                        headers=hdrs, params={"categoryId": category_id})
    return JSONResponse({"status": r.status_code, "body": r.json()})


@app.get("/category-debug")
async def category_debug(limit: int = 5):
    """SALE 상품의 DG basis.section vs Naver 등록 leafCategoryId 비교 (카테고리 매핑 진단)."""
    import httpx as _hx
    from main import NAVER_BASE, _dg_item_detail, get_category_id, DEFAULT_CATEGORY_ID
    hdrs = await naver_api._headers()
    async with _hx.AsyncClient(timeout=30) as c:
        r = await c.post(
            f"{NAVER_BASE}/v1/products/search",
            headers=hdrs,
            json={"productStatusTypes": ["SALE"], "page": 1, "size": min(limit, 20),
                  "orderType": "NO", "periodType": "PROD_REG_DAY",
                  "fromDate": "2020-01-01", "toDate": "2030-12-31"},
        )
    items = r.json().get("contents", [])
    import asyncio as _asyncio
    results = []
    async with _hx.AsyncClient(timeout=30) as c:
        for item in items[:limit]:
            origin_no = str(item.get("originProductNo", ""))
            await _asyncio.sleep(0.4)
            rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{origin_no}", headers=hdrs)
            if rd.status_code != 200:
                results.append({"origin_no": origin_no, "error": rd.status_code})
                continue
            op = rd.json().get("originProduct", {})
            name = op.get("name", "")
            leaf_cat = op.get("leafCategoryId", 0)
            seller_code = (op.get("sellerCodeInfo") or {}).get("sellerManagementCode", "")
            dg_section, computed_cat, detail_ok = "", DEFAULT_CATEGORY_ID, False
            if seller_code.startswith("DG_"):
                detail = await _dg_item_detail(seller_code[3:])
                detail_ok = bool(detail)
                basis = detail.get("basis", {})
                dg_section = str(basis.get("section") or basis.get("keywords") or "")
                cat_str = dg_section.split(">")[0].strip()
                computed_cat = get_category_id({"category": cat_str, "name": name})
            results.append({
                "origin_no": origin_no, "name": name[:40],
                "registered_leafCategoryId": leaf_cat,
                "seller_code": seller_code,
                "dg_detail_ok": detail_ok,
                "dg_section": dg_section,
                "category_first_part": dg_section.split(">")[0].strip() if dg_section else "",
                "computed_get_category_id": computed_cat,
                "would_change": leaf_cat != computed_cat,
            })
    return JSONResponse(results)


@app.get("/category-attributes/{category_id}")
async def get_category_attributes_endpoint(category_id: int):
    """카테고리 필수 속성 목록 조회 (Naver API)."""
    import httpx as _hx
    from main import NAVER_BASE
    try:
        hdrs = await naver_api._headers()
        async with _hx.AsyncClient(timeout=20) as c:
            r = await c.get(
                f"{NAVER_BASE}/v1/product-attributes/attributes",
                headers=hdrs,
                params={"categoryId": category_id},
            )
        if r.status_code != 200:
            return JSONResponse({"error": f"HTTP {r.status_code}", "body": r.text[:400]}, status_code=400)
        data = r.json()
        # Naver API 응답 두 가지 형태 처리: list 또는 {productAttributeGroups:[...]}
        raw_list: list = []
        if isinstance(data, list):
            raw_list = data
        elif isinstance(data, dict):
            for group in (data.get("productAttributeGroups") or []):
                raw_list.extend(group.get("attributes") or [])
        attrs = []
        for attr in raw_list:
            attrs.append({
                "seq": attr.get("attributeSeq"),
                "name": attr.get("name"),
                "required": attr.get("required", False),
                "values": [
                    {"seq": v.get("attributeValueSeq"), "name": v.get("name")}
                    for v in (attr.get("attributeValues") or [])[:30]
                ],
            })
        return {"categoryId": category_id, "raw_format": type(data).__name__, "count": len(attrs), "attributes": attrs}
    except Exception as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=500)


def _pick_attr_value_seq(attr: dict, product_name: str) -> int | None:
    """속성 dict + 상품명 → 최적 attributeValueSeq. 없으면 None."""
    attr_name = attr.get("attributeName") or attr.get("name", "")
    values = attr.get("attributeValues") or []
    if not values:
        return None

    def _vn(v: dict) -> str:
        return v.get("minAttributeValue") or v.get("attributeValueName") or v.get("name", "")

    nl = product_name.lower()

    if any(k in attr_name for k in ("색상", "컬러")):
        COLOR_KW = [
            ("블랙",  ["블랙","검정","black","dark"]),
            ("화이트", ["화이트","흰","white"]),
            ("그레이", ["그레이","회색","gray","grey","실버"]),
            ("네이비", ["네이비","남색","navy"]),
            ("베이지", ["베이지","크림","아이보리","beige"]),
            ("카키",   ["카키","올리브","khaki"]),
            ("브라운", ["브라운","갈색","brown","카멜"]),
            ("핑크",   ["핑크","분홍","pink"]),
            ("블루",   ["블루","파란","blue"]),
            ("레드",   ["레드","빨간","red","버건디"]),
            ("오렌지", ["오렌지","주황","orange"]),
            ("그린",   ["그린","녹색","green","민트"]),
            ("퍼플",   ["퍼플","보라","purple","라벤더"]),
            ("옐로우", ["옐로우","노란","yellow"]),
            ("멀티",   ["멀티","컬러풀","다색","무지개"]),
        ]
        for color_key, keywords in COLOR_KW:
            for kw in keywords:
                if kw in nl:
                    for v in values:
                        if color_key in _vn(v):
                            return v["attributeValueSeq"]
        for v in values:
            if _vn(v) in ("기타", "혼합색상", "해당없음", "멀티컬러", "기타색상"):
                return v["attributeValueSeq"]

    elif any(k in attr_name for k in ("소재", "재질", "원단")):
        MATERIAL_KW = [
            ("면",       ["면", "코튼", "cotton"]),
            ("폴리에스터", ["폴리", "나일론", "화섬"]),
            ("스테인리스", ["스테인리스", "스틸", "steel"]),
            ("실리콘",   ["실리콘", "silicon"]),
            ("플라스틱", ["플라스틱", "abs", "pp", "pvc"]),
            ("가죽",     ["가죽", "양피", "leather"]),
            ("대나무",   ["대나무", "bamboo"]),
            ("알루미늄", ["알루미늄", "aluminum"]),
            ("스판",     ["스판", "스판덱스", "신축"]),
            ("린넨",     ["린넨", "linen"]),
        ]
        for mat_key, keywords in MATERIAL_KW:
            for kw in keywords:
                if kw in nl:
                    for v in values:
                        if mat_key in _vn(v):
                            return v["attributeValueSeq"]

    elif any(k in attr_name for k in ("사이즈", "크기", "치수")):
        for v in values:
            if _vn(v) in ("FREE", "기타", "해당없음", "ONE SIZE", "F", "프리", "FREE SIZE"):
                return v["attributeValueSeq"]

    elif any(k in attr_name for k in ("제조국", "원산지", "생산지")):
        for v in values:
            if _vn(v) in ("중국", "중국산", "China"):
                return v["attributeValueSeq"]

    elif any(k in attr_name for k in ("성별", "대상")):
        for v in values:
            if _vn(v) in ("공용", "유니섹스", "남녀공용"):
                return v["attributeValueSeq"]

    # 공통 폴백: 기타/해당없음
    for v in values:
        if _vn(v) in ("기타", "해당없음", "없음", "해당 없음"):
            return v["attributeValueSeq"]

    return values[0].get("attributeValueSeq")  # 최후 수단: 첫 번째 값


_fix_cat_state: dict = {"running": False, "done": 0, "total": 0, "ok": 0, "skip": 0, "errors": 0, "log": []}


@app.get("/fix-categories-result")
async def fix_categories_result():
    return _fix_cat_state


@app.post("/fix-categories")
async def fix_categories_endpoint(
    background_tasks: BackgroundTasks,
    limit: int = 0,
    dry_run: bool = True,
):
    """상품명 기반으로 leafCategoryId를 올바른 카테고리로 교정.
    dry_run=true: 변경 대상만 보고, 실제 PUT 없음.
    limit=0: 전체 SALE 상품."""
    if _fix_cat_state["running"]:
        return JSONResponse({"status": "already_running"})
    _fix_cat_state.update({"running": True, "done": 0, "total": 0, "ok": 0, "skip": 0, "errors": 0, "log": []})
    background_tasks.add_task(_fix_categories_job, limit, dry_run)
    return {"status": "started", "dry_run": dry_run, "limit": limit}


async def _fix_categories_job(limit: int, dry_run: bool):
    import httpx as _hx
    import asyncio as _ai
    from main import NAVER_BASE, get_category_id, DEFAULT_CATEGORY_ID

    SKIP_KEYS = {"originProductNo", "channelProductNo", "regDate", "modDate",
                 "statusFrom", "totalSalesQuantity", "channelProducts"}
    try:
        hdrs = await naver_api._headers()
        all_products = []
        page = 1
        async with _hx.AsyncClient(timeout=25) as c:
            while True:
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                    json={"productStatusTypes": ["SALE"], "page": page, "size": 50,
                          "orderType": "NO", "periodType": "PROD_REG_DAY",
                          "fromDate": "2020-01-01", "toDate": "2030-12-31"},
                )
                if r.status_code != 200:
                    _fix_cat_state["log"].append(f"[ERR] search {r.status_code}: {r.text[:200]}")
                    break
                items = r.json().get("contents", [])
                if not items:
                    break
                all_products.extend(items)
                if len(items) < 50:
                    break
                page += 1

        if limit > 0:
            all_products = all_products[:limit]
        _fix_cat_state["total"] = len(all_products)
        _fix_cat_state["log"].append(f"SALE 상품 {len(all_products)}개 대상 (dry_run={dry_run})")

        async with _hx.AsyncClient(timeout=25) as c:
            for item in all_products:
                origin_no = str(item.get("originProductNo", ""))
                await _ai.sleep(1.2)
                rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{origin_no}", headers=hdrs)
                if rd.status_code == 429:
                    await _ai.sleep(3.0)
                    rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{origin_no}", headers=hdrs)
                if rd.status_code != 200:
                    _fix_cat_state["errors"] += 1
                    _fix_cat_state["log"].append(f"[ERR] GET {origin_no}: {rd.status_code}")
                    _fix_cat_state["done"] += 1
                    continue

                op = rd.json().get("originProduct", {})
                name = op.get("name", "")
                cur_cat = op.get("leafCategoryId", 0)

                # 현재 이미 올바른 카테고리면 건너뜀
                if str(cur_cat) != str(DEFAULT_CATEGORY_ID):
                    _fix_cat_state["log"].append(f"[SKIP-OK] {origin_no} '{name[:25]}' cat={cur_cat}")
                    _fix_cat_state["skip"] += 1
                    _fix_cat_state["done"] += 1
                    continue

                new_cat = get_category_id({"name": name})
                if new_cat == DEFAULT_CATEGORY_ID:
                    _fix_cat_state["log"].append(f"[SKIP-NOMATCH] {origin_no} '{name[:25]}'")
                    _fix_cat_state["skip"] += 1
                    _fix_cat_state["done"] += 1
                    continue

                _fix_cat_state["log"].append(f"[{'DRY' if dry_run else 'PUT'}] {origin_no} '{name[:25]}' {cur_cat}→{new_cat}")
                if not dry_run:
                    payload = {k: v for k, v in op.items() if k not in SKIP_KEYS}
                    payload["leafCategoryId"] = new_cat
                    await _ai.sleep(0.5)
                    rp = await c.put(
                        f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                        headers=hdrs, json={"originProduct": payload},
                    )
                    if rp.status_code == 200:
                        _fix_cat_state["ok"] += 1
                    else:
                        _fix_cat_state["errors"] += 1
                        _fix_cat_state["log"].append(f"  └ PUT실패 {rp.status_code}: {rp.text[:150]}")
                else:
                    _fix_cat_state["ok"] += 1
                _fix_cat_state["done"] += 1

    except Exception as e:
        _fix_cat_state["log"].append(f"[FATAL] {e}")
    finally:
        _fix_cat_state["running"] = False


_fill_attr_state: dict = {"running": False, "done": 0, "total": 0, "ok": 0, "skip": 0, "errors": 0, "log": []}


@app.get("/fill-attributes-result")
async def fill_attributes_result():
    return _fill_attr_state


@app.post("/fill-attributes")
async def fill_attributes(
    background_tasks: BackgroundTasks,
    limit: int = 0,
    dry_run: bool = False,
):
    """기존 SALE 상품 전체 attributeInfo 채움 (백그라운드)."""
    global _fill_attr_state
    if _fill_attr_state.get("running"):
        return JSONResponse({"error": "이미 실행 중"}, status_code=409)
    _fill_attr_state = {"running": True, "done": 0, "total": 0, "ok": 0, "skip": 0, "errors": 0, "log": [], "dry_run": dry_run}
    background_tasks.add_task(_fill_attributes_job, limit=limit, dry_run=dry_run)
    return {"status": "started", "dry_run": dry_run}


async def _fill_attributes_job(limit: int = 0, dry_run: bool = False):
    global _fill_attr_state
    import httpx as _hx
    from main import NAVER_BASE
    import asyncio as _ai

    SKIP_KEYS = {"originProductNo", "channelProductNo", "regDate", "modDate",
                 "statusFrom", "totalSalesQuantity", "channelProducts"}

    try:
        hdrs = await naver_api._headers()

        # 1. SALE 상품 전체 수집 (POST /v1/products/search)
        all_products = []
        page = 1
        async with _hx.AsyncClient(timeout=25) as c:
            while True:
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                    json={"productStatusTypes": ["SALE"], "page": page, "size": 50,
                          "orderType": "NO", "periodType": "PROD_REG_DAY",
                          "fromDate": "2020-01-01", "toDate": "2030-12-31"},
                )
                if r.status_code != 200:
                    break
                items = r.json().get("contents", [])
                if not items:
                    break
                all_products.extend(items)
                if len(items) < 50:
                    break
                page += 1

        if limit > 0:
            all_products = all_products[:limit]

        _fill_attr_state["total"] = len(all_products)
        _fill_attr_state["log"].append(f"SALE 상품 {len(all_products)}개 대상")

        # 2. 카테고리 속성 캐시
        cat_cache: dict[int, list] = {}

        async def _get_cat_attrs(cat_id: int) -> list:
            if cat_id in cat_cache:
                return cat_cache[cat_id]
            async with _hx.AsyncClient(timeout=20) as c2:
                r2 = await c2.get(
                    f"{NAVER_BASE}/v1/product-attributes/attributes",
                    headers=hdrs,
                    params={"categoryId": cat_id},
                )
            if r2.status_code != 200:
                cat_cache[cat_id] = []
                return []
            d2 = r2.json()
            attrs = []
            if isinstance(d2, list):
                attrs = d2
            elif isinstance(d2, dict):
                for group in (d2.get("productAttributeGroups") or []):
                    attrs.extend(group.get("attributes") or [])
            # 각 속성의 값 목록을 별도 API로 조회 (attributeValues는 속성 목록에 포함 안 됨)
            for attr in attrs:
                attr_seq = attr.get("attributeSeq")
                if not attr_seq:
                    continue
                try:
                    async with _hx.AsyncClient(timeout=10) as cv:
                        rv = await cv.get(
                            f"{NAVER_BASE}/v1/product-attributes/attribute-values",
                            headers=hdrs,
                            params={"attributeSeq": attr_seq, "categoryId": cat_id},
                        )
                    attr["attributeValues"] = rv.json() if rv.status_code == 200 and isinstance(rv.json(), list) else []
                except Exception:
                    attr["attributeValues"] = []
            cat_cache[cat_id] = attrs
            return attrs

        # 3. 상품별 처리
        for item in all_products:
            origin_no = str(item.get("originProductNo", ""))
            try:
                async with _hx.AsyncClient(timeout=20) as c:
                    rd = await c.get(f"{NAVER_BASE}/v2/products/origin-products/{origin_no}", headers=hdrs)
                if rd.status_code != 200:
                    _fill_attr_state["errors"] += 1
                    _fill_attr_state["log"].append(f"[ERR] {origin_no}: GET {rd.status_code}")
                    _fill_attr_state["done"] += 1
                    continue

                full = rd.json()
                op = full.get("originProduct", {})
                product_name = op.get("name", "")
                cat_id = op.get("leafCategoryId")
                existing = op.get("attributeInfo")

                if existing and existing.get("values"):
                    _fill_attr_state["log"].append(f"[SKIP-기존] {origin_no} {product_name[:20]}")
                    _fill_attr_state["skip"] += 1
                    _fill_attr_state["done"] += 1
                    continue

                if not cat_id:
                    _fill_attr_state["log"].append(f"[SKIP-카테고리없음] {origin_no}")
                    _fill_attr_state["skip"] += 1
                    _fill_attr_state["done"] += 1
                    continue

                cat_attrs = await _get_cat_attrs(cat_id)
                if not cat_attrs:
                    _fill_attr_state["log"].append(f"[SKIP-속성없음] {origin_no} cat={cat_id}")
                    _fill_attr_state["skip"] += 1
                    _fill_attr_state["done"] += 1
                    continue

                attr_values = []
                for attr in cat_attrs:
                    seq = _pick_attr_value_seq(attr, product_name)
                    if seq is not None:
                        attr_values.append({
                            "attributeSeq": attr["attributeSeq"],
                            "attributeValueSeq": seq,
                        })

                if not attr_values:
                    _fill_attr_state["log"].append(f"[SKIP-값없음] {origin_no} {product_name[:20]}")
                    _fill_attr_state["skip"] += 1
                    _fill_attr_state["done"] += 1
                    continue

                if dry_run:
                    _fill_attr_state["log"].append(
                        f"[DRY] {origin_no} {product_name[:20]} cat={cat_id} → {len(attr_values)}개"
                    )
                    _fill_attr_state["ok"] += 1
                    _fill_attr_state["done"] += 1
                    continue

                # PUT — 읽기전용 키 제거 후 attributeInfo 추가
                payload = {k: v for k, v in op.items() if k not in SKIP_KEYS}
                payload["attributeInfo"] = {"values": attr_values}

                async with _hx.AsyncClient(timeout=25) as c:
                    rp = await c.put(
                        f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                        headers=hdrs,
                        json={"originProduct": payload},
                    )

                if rp.status_code in (200, 201):
                    _fill_attr_state["ok"] += 1
                    _fill_attr_state["log"].append(
                        f"[OK] {origin_no} {product_name[:20]} → {len(attr_values)}개 속성"
                    )
                else:
                    _fill_attr_state["errors"] += 1
                    _fill_attr_state["log"].append(
                        f"[ERR] {origin_no}: PUT {rp.status_code} {rp.text[:500]}"
                    )

                _fill_attr_state["done"] += 1
                await _ai.sleep(0.4)  # rate limit

            except Exception as e:
                _fill_attr_state["errors"] += 1
                _fill_attr_state["log"].append(f"[ERR] {origin_no}: {str(e)[:100]}")
                _fill_attr_state["done"] += 1

        _fill_attr_state["running"] = False
        _fill_attr_state["log"].append(
            f"완료: OK={_fill_attr_state['ok']} SKIP={_fill_attr_state['skip']} ERR={_fill_attr_state['errors']}"
        )

    except Exception as e:
        _fill_attr_state["running"] = False
        _fill_attr_state["log"].append(f"[FATAL] {str(e)}")


# ── SALE 상품 배치 삭제+재등록 (카테고리 교정) ─────────────────────────────
_rereg_state: dict = {"running": False}


@app.get("/rereg-result")
async def rereg_result():
    return _rereg_state


@app.post("/rereg-recover-dg")
async def rereg_recover_dg(dg_item_no: str, prod_name: str = ""):
    """소멸된 상품을 DG item_no로 복구 등록 (재등록 실패 후 긴급 복구용)."""
    import httpx as _hx
    from main import (
        _dg_item_detail as _dg_det,
        build_product_payload,
        calculate_selling_price,
    )
    try:
        detail = await _dg_det(dg_item_no)
        if not detail:
            return {"ok": False, "reason": "DG 상세 없음(품절추정)"}
        basis = detail.get("basis") or {}
        price_info = detail.get("price") or {}
        thumb = detail.get("thumb") or {}
        qty = detail.get("qty") or {}
        wholesale = int(price_info.get("dome", 0) or 0)
        if wholesale <= 0:
            return {"ok": False, "reason": "도매가 0"}
        title = prod_name or basis.get("title", "") or f"DG_{dg_item_no}"
        image = thumb.get("original", "") or thumb.get("large", "")
        stock = int(qty.get("inventory", 100) or 100)
        deli = int(price_info.get("deli", 0) or 0)
        selling_price = round(max(wholesale * 2.2, wholesale * 1.15) / 10) * 10
        # DG 이미지 → Naver 업로드 (DG URL은 Naver API에서 직접 사용 불가)
        naver_img_url = image
        if image:
            try:
                naver_img_url = await naver_api.upload_image(image)
            except Exception as _ie:
                return {"ok": False, "reason": f"이미지 업로드 실패: {str(_ie)[:200]}"}

        raw = {
            "code": f"DG_{dg_item_no}",
            "name": title,
            "price": wholesale,
            "image": naver_img_url,
            "stock": stock,
            "delivery_type": "무료배송" if not deli else "",
            "delivery_fee": deli or 3000,
        }
        payload = build_product_payload(raw, {"product_name": title}, selling_price)
        result = await naver_api.register_product(payload)
        if result and isinstance(result, dict):
            new_no = str(result.get("originProductNo", ""))
            return {"ok": True, "new_no": new_no, "wholesale": wholesale,
                    "selling_price": selling_price, "name": title}
        return {"ok": False, "reason": "register_product 반환값 없음"}
    except Exception as e:
        return {"ok": False, "reason": str(e)[:300]}


@app.post("/rereg-batch")
async def rereg_batch(
    background_tasks: BackgroundTasks,
    offset: int = 0,
    size: int = 5,
    dry_run: bool = True,
    stop_on_error: bool = True,
):
    """SALE 상품 배치 삭제+재등록 (카테고리 교정).
    dry_run=true(기본)로 먼저 미리보기 확인 후 dry_run=false로 실행.
    stop_on_error=true(기본): 에러 1건 발생 시 즉시 중단."""
    global _rereg_state
    if _rereg_state.get("running"):
        return JSONResponse({"error": "이미 실행 중"}, status_code=409)
    _rereg_state = {
        "running": True, "dry_run": dry_run, "offset": offset, "size": size,
        "total_sale": 0, "processed": 0, "ok": 0, "skip": 0, "err": 0,
        "items": [], "errors": [],
    }
    background_tasks.add_task(_rereg_batch_job, offset=offset, size=size,
                              dry_run=dry_run, stop_on_error=stop_on_error)
    return {"status": "started", "dry_run": dry_run, "offset": offset,
            "size": size, "stop_on_error": stop_on_error}


async def _rereg_batch_job(offset: int, size: int, dry_run: bool, stop_on_error: bool = True):
    global _rereg_state
    import httpx as _hx, asyncio as _aio
    from main import (
        NAVER_BASE,
        get_category_id as _get_cat,
        _dg_to_product as _dg_prod,
        _dg_item_detail as _dg_det,
        DEFAULT_CATEGORY_ID as _DEF_CAT,
    )

    _SKIP_KEYS = {"originProductNo", "channelProductNo", "regDate", "modDate",
                  "statusFrom", "totalSalesQuantity", "channelProducts"}

    try:
        hdrs = await naver_api._headers()

        # 1. 전체 SALE 상품 수집
        all_products = []
        page = 1
        while True:
            try:
                async with _hx.AsyncClient(timeout=20) as c:
                    r = await c.post(
                        f"{NAVER_BASE}/v1/products/search", headers=hdrs,
                        json={"productStatusTypes": ["SALE"], "page": page, "size": 100,
                              "orderType": "NO", "periodType": "PROD_REG_DAY",
                              "fromDate": "2020-01-01", "toDate": "2030-12-31"},
                    )
                items_page = r.json().get("contents", [])
                if not items_page:
                    break
                all_products.extend(items_page)
                if len(items_page) < 100:
                    break
                page += 1
                await _aio.sleep(0.8)
            except Exception as e_list:
                _rereg_state["errors"].append(f"list p{page}: {str(e_list)[:60]}")
                break

        _rereg_state["total_sale"] = len(all_products)
        batch = all_products[offset: offset + size]
        _rereg_state["batch_range"] = f"{offset}~{offset + len(batch) - 1} / 전체{len(all_products)}"

        # 2. 배치 처리
        for item in batch:
            origin_no = str(item.get("originProductNo", "") or "")
            item_log: dict = {"origin_no": origin_no}

            try:
                hdrs = await naver_api._headers()

                # GET 상품 상세
                async with _hx.AsyncClient(timeout=15) as c:
                    rg = await c.get(
                        f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                        headers=hdrs,
                    )
                if rg.status_code == 429:
                    # 429 → 최대 3회 재시도 (12s/20s/30s)
                    for _wait in (12, 20, 30):
                        await _aio.sleep(_wait)
                        hdrs = await naver_api._headers()
                        async with _hx.AsyncClient(timeout=15) as c:
                            rg = await c.get(
                                f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                                headers=hdrs,
                            )
                        if rg.status_code != 429:
                            break
                if rg.status_code != 200:
                    item_log.update({"status": f"ERR-GET{rg.status_code}"})
                    _rereg_state["err"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    if stop_on_error:
                        _rereg_state["stopped_reason"] = f"ERR-GET{rg.status_code} at {origin_no}"
                        break
                    await _aio.sleep(2.5)
                    continue

                op = rg.json().get("originProduct", {})
                prod_name = str(op.get("name", ""))[:50]
                cur_cat = op.get("leafCategoryId", _DEF_CAT)
                old_html = op.get("detailContent")  # 기존 HTML 보존
                old_cost = op.get("costPrice") or 0
                item_log["name"] = prod_name
                item_log["old_cat"] = cur_cat

                # 이미 올바른 카테고리면 SKIP (DEFAULT_CAT 50002480이 아닌 경우)
                if str(cur_cat) != str(_DEF_CAT):
                    item_log["status"] = "SKIP-이미올바른카테고리"
                    _rereg_state["skip"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    await _aio.sleep(1.0)
                    continue

                # DG 코드 추출
                seller_info = (op.get("detailAttribute") or {}).get("sellerCodeInfo") or {}
                dg_code = str(seller_info.get("sellerManagementCode", "") or "").strip()

                if not dg_code:
                    item_log["status"] = "SKIP-DG코드없음"
                    _rereg_state["skip"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    await _aio.sleep(2.5)
                    continue

                if dg_code.upper().startswith("DG_"):
                    item_no = dg_code[3:].strip()
                elif dg_code.isdigit():
                    item_no = dg_code
                else:
                    item_log["status"] = "SKIP-DG코드형식오류"
                    _rereg_state["skip"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    await _aio.sleep(2.5)
                    continue

                # DG 최신 상세 조회
                dg_detail = await _dg_det(item_no)
                if not dg_detail:
                    item_log["status"] = "SKIP-DG상세없음(품절추정)"
                    _rereg_state["skip"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    await _aio.sleep(2.5)
                    continue

                # 새 도매가 파싱 (DG 현재가 우선, 없으면 기존 costPrice)
                dg_price_raw = (dg_detail.get("price") or {}).get("dome", 0)
                new_wholesale = int(dg_price_raw) if dg_price_raw else old_cost

                if new_wholesale <= 0:
                    item_log["status"] = "SKIP-도매가0"
                    _rereg_state["skip"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    await _aio.sleep(2.5)
                    continue

                # 판매가: floor=×1.15, target=×2.2, 10원 단위
                new_sale_price = round(max(new_wholesale * 2.2, new_wholesale * 1.15) / 10) * 10

                # 카테고리 재판정: Naver 상품명 + DG 섹션 참조
                dg_section = (dg_detail.get("basis") or {}).get("section", "")
                new_cat = _get_cat({"name": prod_name, "category": dg_section})
                item_log["dg_code"] = dg_code  # 복구 추적용
                item_log["new_cat"] = new_cat
                # 카테고리 매핑 실패(DEFAULT_CAT 반환) → 재등록 불필요, 기존 상품 유지
                if new_cat == _DEF_CAT:
                    item_log["status"] = "SKIP-카테고리매핑실패"
                    _rereg_state["skip"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    await _aio.sleep(1)
                    continue
                item_log["dg_section"] = dg_section[:30] if dg_section else ""
                item_log["new_wholesale"] = new_wholesale
                item_log["new_sale_price"] = new_sale_price

                # 재등록 payload: 기존 Naver op 필드 그대로 재활용 → Naver API 포맷 유지
                # ⚠️ _dg_to_product 내부 dict를 직접 register_product에 넘기면 Naver 400 발생
                #    (register_product는 json=payload 그대로 POST — Naver 포맷 필요)
                new_payload = {
                    "originProduct": {k: v for k, v in op.items() if k not in _SKIP_KEYS},
                    "smartstoreChannelProduct": {
                        "naverShoppingRegistration": True,
                        "channelProductDisplayStatusType": "ON",
                    },
                }
                new_payload["originProduct"]["leafCategoryId"] = new_cat
                new_payload["originProduct"]["statusType"] = "SALE"
                new_payload["originProduct"]["salePrice"] = new_sale_price
                new_payload["originProduct"]["costPrice"] = new_wholesale

                # KC 인증 필드 정리: kindType=null인 항목 제거
                # (기존 op에 잘못된 productCertificationInfos가 있으면 카테고리 변경 시 Naver 400)
                _da = new_payload["originProduct"].get("detailAttribute") or {}
                _certs = _da.get("productCertificationInfos") or []
                _valid = [c for c in _certs if (c.get("kindType") or c.get("certificationKindType"))]
                if _valid:
                    _da["productCertificationInfos"] = _valid
                else:
                    _da.pop("productCertificationInfos", None)
                new_payload["originProduct"]["detailAttribute"] = _da

                if dry_run:
                    item_log["status"] = "DRY-재등록예정"
                    _rereg_state["ok"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    await _aio.sleep(2.5)
                    continue

                # ── 안전 순서: 신규 SUSPENSION 등록 → 성공 확인 → DELETE 기존 → SALE 활성화 ──
                # (DELETE 먼저 하면 REGISTER 실패 시 상품 소멸 위험)

                # 1) 신규 상품 SUSPENSION 상태로 먼저 등록 (기존 상품 유지 상태)
                new_payload["originProduct"]["statusType"] = "SUSPENSION"
                try:
                    new_no_raw = await naver_api.register_product(new_payload)
                except Exception as _reg_err:
                    err_msg = str(_reg_err)
                    # KC 인증 필수 카테고리 → SKIP (기존 상품 유지)
                    if "productCertificationInfos" in err_msg or "certificationInfos" in err_msg:
                        item_log["status"] = "SKIP-KC인증필수카테고리"
                        _rereg_state["skip"] += 1
                    else:
                        item_log["status"] = f"ERR-등록실패(기존유지): {err_msg[:150]}"
                        _rereg_state["errors"].append(f"{origin_no}: {err_msg[:300]}")
                        _rereg_state["err"] += 1
                        if stop_on_error:
                            _rereg_state["items"].append(item_log)
                            _rereg_state["processed"] += 1
                            _rereg_state["stopped_reason"] = f"등록실패 at {origin_no}: {err_msg[:100]}"
                            break
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    await _aio.sleep(2.5)
                    continue

                # 등록 성공 → new_no 추출
                new_no_int = (new_no_raw.get("originProductNo") if isinstance(new_no_raw, dict)
                              else new_no_raw)
                new_no_str = str(new_no_int) if new_no_int else ""
                if not new_no_str:
                    item_log["status"] = "ERR-등록실패no없음(기존유지)"
                    _rereg_state["err"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    if stop_on_error:
                        _rereg_state["stopped_reason"] = f"new_no없음 at {origin_no}"
                        break
                    await _aio.sleep(2.5)
                    continue

                await _aio.sleep(0.8)

                # 2) 기존 상품 DELETE
                async with _hx.AsyncClient(timeout=15) as c:
                    r_del = await c.delete(
                        f"{NAVER_BASE}/v2/products/origin-products/{origin_no}",
                        headers=hdrs,
                    )
                if r_del.status_code not in (200, 204):
                    # 삭제 실패 → 신규 상품도 삭제해서 중복 방지
                    try:
                        async with _hx.AsyncClient(timeout=10) as c2:
                            await c2.delete(
                                f"{NAVER_BASE}/v2/products/origin-products/{new_no_str}",
                                headers=hdrs,
                            )
                    except Exception:
                        pass
                    item_log["status"] = f"ERR-삭제실패({r_del.status_code})(기존유지)"
                    _rereg_state["errors"].append(
                        f"{origin_no} DELETE {r_del.status_code}: {r_del.text[:80]}")
                    _rereg_state["err"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    if stop_on_error:
                        _rereg_state["stopped_reason"] = f"삭제실패({r_del.status_code}) at {origin_no}"
                        break
                    await _aio.sleep(2.5)
                    continue

                await _aio.sleep(1.0)

                # 3) 신규 상품 SALE 활성화
                act_payload = {k: v for k, v in new_payload["originProduct"].items()
                               if k not in _SKIP_KEYS}
                act_payload["statusType"] = "SALE"
                async with _hx.AsyncClient(timeout=15) as c:
                    await c.put(
                        f"{NAVER_BASE}/v2/products/origin-products/{new_no_str}",
                        headers=hdrs, json={"originProduct": act_payload},
                    )
                await _aio.sleep(1.0)

                new_no = new_no_raw
                if new_no:
                    # 4) 등록 후 카테고리 실측 검증
                    await _aio.sleep(1.5)
                    try:
                        async with _hx.AsyncClient(timeout=15) as c:
                            rv = await c.get(
                                f"{NAVER_BASE}/v2/products/origin-products/{new_no}",
                                headers=hdrs,
                            )
                        verified_cat = rv.json().get("originProduct", {}).get("leafCategoryId", "?")
                        item_log["verified_cat"] = verified_cat
                        item_log["cat_ok"] = (verified_cat == new_cat)
                    except Exception:
                        item_log["verified_cat"] = "검증실패"
                        item_log["cat_ok"] = False

                    item_log["new_no"] = new_no
                    item_log["status"] = "OK"
                    _rereg_state["ok"] += 1
                else:
                    item_log["status"] = "ERR-등록실패(상품삭제됨)"
                    _rereg_state["errors"].append(f"{origin_no} register_product 실패 — 상품 소멸")
                    _rereg_state["err"] += 1
                    _rereg_state["items"].append(item_log)
                    _rereg_state["processed"] += 1
                    if stop_on_error:
                        _rereg_state["stopped_reason"] = f"등록실패(상품소멸) at {origin_no}"
                        break
                    await _aio.sleep(3.0)
                    continue

                _rereg_state["items"].append(item_log)
                _rereg_state["processed"] += 1
                await _aio.sleep(3.0)

            except Exception as e:
                item_log["status"] = f"ERR-예외: {str(e)[:200]}"
                _rereg_state["errors"].append(f"{origin_no}: {str(e)[:500]}")
                _rereg_state["err"] += 1
                _rereg_state["items"].append(item_log)
                _rereg_state["processed"] += 1
                if stop_on_error:
                    _rereg_state["stopped_reason"] = f"예외 at {origin_no}: {str(e)[:200]}"
                    break
                await _aio.sleep(2.5)

    except Exception as e:
        _rereg_state["errors"].append(f"[FATAL] {str(e)}")
    finally:
        _rereg_state["running"] = False


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
