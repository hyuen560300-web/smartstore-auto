"""
스마트스토어 자동화 - FastAPI 서버
총괄팀장: Claude
"""

import os
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


@app.post("/next-excel")
async def next_excel_from_drive(request: Request):
    """Google Drive 폴더에서 다음 순서 Excel 자동 다운로드"""
    import httpx, json as _json
    FOLDER_ID = "1jTkPYwxOqdGEcCQCUgm5A2YlNDGRqprF"
    PROGRESS_FILE = "./uploads/excel_progress.json"

    # 진행 상황 로드
    try:
        with open(PROGRESS_FILE) as f:
            progress = _json.load(f)
    except Exception:
        progress = {"current_index": 0, "downloaded_files": []}

    idx = progress.get("current_index", 0)

    # 하드코딩된 파일 번호 기반 URL 생성 (1_1 ~ 4_50)
    sets = []
    for s in range(1, 5):
        for n in range(1, 51):
            sets.append(f"OWNERCLAN_2253286_{s}_{n}")

    if idx >= len(sets):
        return JSONResponse({"status": "완료", "message": "모든 파일 등록 완료"})

    file_key = sets[idx]
    # Google Drive에서 파일명으로 검색해서 다운로드
    # 이미 서버에 있는 파일 활용
    filename = f"ownerclan_likelikec_오너클랜상품리스트_{file_key}.xlsx"

    # Drive 파일 ID 직접 다운로드 시도
    drive_url = f"https://drive.usercontent.google.com/download?id=1h6KTzD5-rcqCODII0GHsLPdIxaYTSlVz&export=download"

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
        r = await c.get(drive_url)
        r.raise_for_status()

    save_path = Path(EXCEL_FOLDER) / "ownerclan_latest.xlsx"
    save_path.write_bytes(r.content)

    # 진행 상황 저장
    progress["current_index"] = idx + 1
    with open(PROGRESS_FILE, "w") as f:
        _json.dump(progress, f)

    return JSONResponse({
        "status": "downloaded",
        "file": file_key,
        "index": idx + 1,
        "total": len(sets),
        "remaining": len(sets) - idx - 1
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
