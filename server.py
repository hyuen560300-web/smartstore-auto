"""
스마트스토어 자동화 - FastAPI 서버
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
)

app = FastAPI(title="스마트스토어 자동화", version="2.0.0")


@app.get("/health")
def health():
    return {"status": "ok", "service": "smartstore_auto", "version": "2.0"}


@app.post("/register-products")
async def register_products(request: Request, background_tasks: BackgroundTasks):
    """n8n → 상품 등록 트리거 (body에 excel_path 또는 최신 파일 자동 선택)"""
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
    """오너클랜 Excel 파일 업로드"""
    save_path = Path(EXCEL_FOLDER) / file.filename
    content = await file.read()
    save_path.write_bytes(content)
    return JSONResponse({"status": "uploaded", "path": str(save_path), "size": len(content)})


@app.post("/process-orders")
async def process_orders(background_tasks: BackgroundTasks):
    """주문 처리 트리거"""
    background_tasks.add_task(pipeline_process_orders)
    return JSONResponse({"status": "processing"})


@app.post("/sync-inventory")
async def sync_inventory(background_tasks: BackgroundTasks):
    """재고 동기화 트리거"""
    background_tasks.add_task(pipeline_sync_inventory)
    return JSONResponse({"status": "processing"})


@app.post("/reply-inquiries")
async def reply_inquiries(background_tasks: BackgroundTasks):
    """고객 문의 자동 답변 트리거"""
    background_tasks.add_task(pipeline_reply_inquiries)
    return JSONResponse({"status": "processing"})


@app.get("/price-check")
def price_check(wholesale_price: int):
    """도매가 → 판매가 계산"""
    selling = calculate_selling_price(wholesale_price)
    margin = float(os.environ.get("MARGIN_RATE", "0.15"))
    return {
        "wholesale_price": wholesale_price,
        "selling_price": selling,
        "margin_rate": f"{margin * 100:.0f}%",
        "profit": selling - wholesale_price,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
