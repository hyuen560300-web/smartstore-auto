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
    """Google Drive 폴더 스캔 → 파일 ID 인덱스 구축 (최초 1회 실행)"""
    file_ids = await _scan_drive_folder()
    if not file_ids:
        file_ids = [FALLBACK_FILE_ID]
        msg = "스캔 실패 — 폴백 파일 ID 1개 저장"
    else:
        msg = f"{len(file_ids)}개 파일 ID 저장 완료"
    _save_drive_index(file_ids)
    return JSONResponse({"status": "ok", "message": msg, "count": len(file_ids)})


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


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
