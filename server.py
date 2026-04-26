"""
스마트스토어 자동화 - FastAPI 서버
총괄팀장: Claude
"""

import os
import json
import httpx
from datetime import datetime, timezone
from fastapi import FastAPI, BackgroundTasks, Request, UploadFile, File
from fastapi.responses import JSONResponse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

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
    create_banner_image,
    _get_scene_context,
)
from employees import (
    employee_season_planner,
    employee_trend_scout,
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

app = FastAPI(title="스마트스토어 자동화 AI 직원단", version="3.0.0")


# ─── 기본 ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "smartstore_auto", "version": "3.0"}


@app.get("/check-env")
def check_env():
    """API 키 설정 여부 확인"""
    return {
        "ANTHROPIC_API_KEY": bool(os.environ.get("ANTHROPIC_API_KEY")),
        "OPENAI_API_KEY": bool(os.environ.get("OPENAI_API_KEY")),
        "PEXELS_API_KEY": bool(os.environ.get("PEXELS_API_KEY")),
        "PIXABAY_API_KEY": bool(os.environ.get("PIXABAY_API_KEY")),
        "NAVER_CLIENT_ID": bool(os.environ.get("NAVER_CLIENT_ID")),
        "GOOGLE_API_KEY": bool(os.environ.get("GOOGLE_API_KEY")),
    }


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
        with open(EXCEL_PROGRESS) as f:
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
    limit = int(body.get("limit", 50))
    background_tasks.add_task(pipeline_register_products, excel_path, limit)
    return JSONResponse({"status": "processing", "excel": excel_path, "limit": limit})


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
    }

    registered_codes = load_registered_codes()
    if p["code"] and p["code"] in registered_codes:
        return JSONResponse({"status": "duplicate", "message": "이미 등록된 상품"})

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

        result = await naver_api.register_product(payload)
        save_registered_code(p["code"])
        return JSONResponse({"status": "success", "product_name": safe_name, "price": price})

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
DRIVE_INDEX_FILE  = "./uploads/drive_index.json"
EXCEL_PROGRESS    = "./uploads/excel_progress.json"


def _load_drive_index() -> list:
    try:
        with open(DRIVE_INDEX_FILE) as f:
            return json.load(f).get("file_ids", [])
    except Exception:
        return []


def _save_drive_index(file_ids: list):
    with open(DRIVE_INDEX_FILE, "w") as f:
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
        with open(EXCEL_PROGRESS) as f:
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
        with open(EXCEL_PROGRESS) as f:
            progress = json.load(f)
    except Exception:
        progress = {"current_index": 0}

    idx = progress.get("current_index", 0) % len(file_ids)
    file_id = file_ids[idx]

    # 3. 다운로드
    download_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"
    print(f"[DRIVE] 다운로드 중: {file_id} (인덱스 {idx+1}/{len(file_ids)})", flush=True)

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
        r = await c.get(download_url)
        r.raise_for_status()

    save_path = Path(EXCEL_FOLDER) / "ownerclan_latest.xlsx"
    save_path.write_bytes(r.content)

    # 4. 진행 상황 저장
    next_idx = (idx + 1) % len(file_ids)
    progress["current_index"] = next_idx
    with open(EXCEL_PROGRESS, "w") as f:
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


@app.get("/price-check")
def price_check(wholesale_price: int):
    selling = calculate_selling_price(wholesale_price)
    margin = float(os.environ.get("MARGIN_RATE", "0.15"))
    return {"wholesale_price": wholesale_price, "selling_price": selling,
            "margin_rate": f"{margin * 100:.0f}%", "profit": selling - wholesale_price}


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
    """📦 등록된 상품 목록 조회"""
    try:
        result = await naver_api.list_products(page, size)
        products = result.get("contents", [])
        return JSONResponse({
            "total": result.get("totalElements", 0),
            "page": page,
            "count": len(products),
            "products": [
                {
                    "id": p.get("originProductNo"),
                    "name": p.get("originProduct", {}).get("name", ""),
                    "price": p.get("originProduct", {}).get("salePrice", 0),
                    "status": p.get("originProduct", {}).get("statusType", ""),
                    "stock": p.get("originProduct", {}).get("stockQuantity", 0),
                }
                for p in products
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
    """🚫 상품 판매중지"""
    body = await request.json()
    product_id = str(body.get("product_id", ""))
    if not product_id:
        return JSONResponse({"status": "error", "message": "product_id 필요"}, status_code=400)
    ok = await naver_api.set_product_status(product_id, "SUSPENSION")
    return JSONResponse({"status": "ok" if ok else "fail", "product_id": product_id})


@app.post("/activate-product")
async def activate_product(request: Request):
    """✅ 상품 판매재개"""
    body = await request.json()
    product_id = str(body.get("product_id", ""))
    if not product_id:
        return JSONResponse({"status": "error", "message": "product_id 필요"}, status_code=400)
    ok = await naver_api.set_product_status(product_id, "SALE")
    return JSONResponse({"status": "ok" if ok else "fail", "product_id": product_id})


@app.delete("/delete-product/{product_id}")
async def delete_product(product_id: str):
    """🗑️ 상품 삭제"""
    ok = await naver_api.delete_product(product_id)
    return JSONResponse({"status": "ok" if ok else "fail", "product_id": product_id})


@app.post("/update-price")
async def update_price(request: Request):
    """💰 상품 가격 수정"""
    body = await request.json()
    product_id = str(body.get("product_id", ""))
    price = int(body.get("price", 0))
    if not product_id or price <= 0:
        return JSONResponse({"status": "error", "message": "product_id, price 필요"}, status_code=400)
    ok = await naver_api.update_price(product_id, price)
    return JSONResponse({"status": "ok" if ok else "fail", "product_id": product_id, "new_price": price})


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
                    "status": o.get("productOrderStatus", ""),
                    "buyer": o.get("buyerName", ""),
                    "ordered_at": o.get("paymentDate", ""),
                }
                for o in orders
            ]
        })
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


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
            "처리한 Excel 파일": json.load(open(EXCEL_PROGRESS)).get("current_index", 0) if os.path.exists(EXCEL_PROGRESS) else 0,
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


@app.on_event("startup")
async def startup_event():
    """서버 시작 시 Drive 인덱스 자동 복구 + 스케줄러 시작"""
    if not _load_drive_index():
        _save_drive_index(DRIVE_FILE_IDS_PERMANENT)
        print(f"[STARTUP] Drive 인덱스 자동 복구 완료: {len(DRIVE_FILE_IDS_PERMANENT)}개", flush=True)

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
        ANTHROPIC_API_KEY, MARGIN_RATE,
    )

    async def job_process_orders():
        print("[SCHED] 주문 처리", flush=True)
        await pipeline_process_orders()

    async def job_error_audit():
        print("[SCHED] 에러 감사원", flush=True)
        await employee_error_auditor(ANTHROPIC_API_KEY)

    async def job_reply_inquiries():
        print("[SCHED] 고객 문의 답변", flush=True)
        await pipeline_reply_inquiries()

    async def job_stock_alert():
        print("[SCHED] 품절 방지 알림이", flush=True)
        await employee_stock_guardian(naver_api, ANTHROPIC_API_KEY)

    async def job_sync_inventory():
        print("[SCHED] 재고 동기화", flush=True)
        await pipeline_sync_inventory()

    async def job_trend_scout():
        print("[SCHED] 트렌드 스카우터", flush=True)
        await employee_trend_scout()

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
        """매일 08:00 / 12:00 / 20:00 — next-excel 다운로드 후 상품 17개 등록"""
        print("[SCHED] 상품 자동 등록 시작", flush=True)
        try:
            excel_path = await _next_excel_internal()
            if excel_path:
                await pipeline_register_products(excel_path, limit=17)
        except Exception as e:
            print(f"[SCHED] 상품 등록 오류: {e}", flush=True)

    scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

    # 1시간
    scheduler.add_job(job_process_orders,  "interval", hours=1, id="process_orders")
    scheduler.add_job(job_error_audit,     "interval", hours=1, id="error_audit")
    # 2시간
    scheduler.add_job(job_reply_inquiries, "interval", hours=2, id="reply_inquiries")
    scheduler.add_job(job_stock_alert,     "interval", hours=2, id="stock_alert")
    # 6시간
    scheduler.add_job(job_sync_inventory,  "interval", hours=6, id="sync_inventory")
    scheduler.add_job(job_trend_scout,     "interval", hours=6, id="trend_scout")
    # 매일
    scheduler.add_job(job_daily_report,    "interval", hours=24, id="daily_report")
    scheduler.add_job(job_season_plan,     "interval", hours=24, id="season_plan")
    scheduler.add_job(job_shortform,       "interval", hours=24, id="shortform")
    scheduler.add_job(job_ad_analysis,     "interval", hours=24, id="ad_analysis")
    # 매주
    scheduler.add_job(job_event_manager,   "interval", weeks=1, id="event_manager")
    scheduler.add_job(job_blog_manager,    "interval", weeks=1, id="blog_manager")
    scheduler.add_job(job_review_analysis, "interval", weeks=1, id="review_analysis")
    scheduler.add_job(job_expand_platform, "interval", weeks=1, id="expand_platform")
    # 08:00 / 12:00 / 20:00 상품 등록
    scheduler.add_job(job_register_products, "cron", hour="8,12,20", minute=0, id="register_products_8")
    # 매주 월요일 00:00 저성과 상품 정리
    scheduler.add_job(job_auto_cleanup, "cron", day_of_week="mon", hour=0, minute=0, id="auto_cleanup")

    scheduler.start()
    print("[STARTUP] APScheduler 시작 완료 — n8n 워크플로우 3개 대체", flush=True)


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
        with open(EXCEL_PROGRESS) as f:
            progress = json.load(f)
    except Exception:
        progress = {"current_index": 0}
    idx = progress.get("current_index", 0) % len(file_ids)
    file_id = file_ids[idx]
    url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
        r = await c.get(url)
        r.raise_for_status()
    save_path = Path(EXCEL_FOLDER) / "ownerclan_latest.xlsx"
    save_path.write_bytes(r.content)
    progress["current_index"] = (idx + 1) % len(file_ids)
    with open(EXCEL_PROGRESS, "w") as f:
        json.dump(progress, f)
    print(f"[SCHED] Excel 다운로드 완료: {file_id} ({len(r.content)//1024}KB)", flush=True)
    return str(save_path)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
