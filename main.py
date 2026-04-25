"""
스마트스토어 자동화 v2
소싱: 오너클랜 Excel DB
판매: 네이버 스마트스토어 커머스 API
"""

import asyncio
import base64
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import bcrypt
import httpx
import openpyxl
from dotenv import load_dotenv

load_dotenv()

# ─── 환경변수 ────────────────────────────────────────────────────────────────
NAVER_CLIENT_ID     = os.environ.get("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")
NAVER_SELLER_ID     = os.environ.get("NAVER_SELLER_ID", "")
ANTHROPIC_API_KEY   = os.environ.get("ANTHROPIC_API_KEY", "")
PEXELS_API_KEY      = os.environ.get("PEXELS_API_KEY", "")
MARGIN_RATE         = float(os.environ.get("MARGIN_RATE", "0.15"))
EXCEL_FOLDER        = os.environ.get("EXCEL_FOLDER", "./uploads")
AS_PHONE            = os.environ.get("AS_PHONE", "010-0000-0000")

NAVER_BASE = "https://api.commerce.naver.com/external"

Path(EXCEL_FOLDER).mkdir(exist_ok=True)


# ─── 네이버 커머스 API ────────────────────────────────────────────────────────
class NaverCommerceAPI:
    def __init__(self):
        self.client_id = NAVER_CLIENT_ID
        self.client_secret = NAVER_CLIENT_SECRET
        self.access_token = None
        self.token_expires_at = 0.0

    def _sign(self) -> tuple[str, str]:
        timestamp = str(int(time.time() * 1000))
        password = f"{self.client_id}_{timestamp}"
        hashed = bcrypt.hashpw(password.encode("utf-8"), self.client_secret.encode("utf-8"))
        sig = base64.b64encode(hashed).decode("utf-8")
        return timestamp, sig

    async def get_token(self) -> str:
        if self.access_token and time.time() < self.token_expires_at - 60:
            return self.access_token
        timestamp, sig = self._sign()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                f"{NAVER_BASE}/v1/oauth2/token",
                data={
                    "client_id": self.client_id,
                    "timestamp": timestamp,
                    "client_secret_sign": sig,
                    "grant_type": "client_credentials",
                    "type": "SELF",
                }
            )
            r.raise_for_status()
            data = r.json()
            self.access_token = data["access_token"]
            self.token_expires_at = time.time() + data.get("expires_in", 3600)
        return self.access_token

    async def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {await self.get_token()}",
            "Content-Type": "application/json",
        }

    async def upload_image(self, image_url: str) -> str:
        img_resp = await httpx.AsyncClient(timeout=15, follow_redirects=True).get(image_url)
        img_resp.raise_for_status()
        token = await self.get_token()
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{NAVER_BASE}/v1/product-images/upload",
                headers={"Authorization": f"Bearer {token}"},
                files={"imageFiles": ("image.jpg", img_resp.content, "image/jpeg")}
            )
            r.raise_for_status()
            return r.json()["images"][0]["url"]

    async def register_product(self, payload: dict) -> dict:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{NAVER_BASE}/v2/products",
                headers=await self._headers(),
                json=payload
            )
            r.raise_for_status()
            return r.json()

    async def get_new_orders(self) -> list:
        now = datetime.now(timezone.utc)
        from_dt = now.strftime("%Y-%m-%dT00:00:00.000Z")
        to_dt = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(
                f"{NAVER_BASE}/v1/pay-order/seller/product-orders",
                headers=await self._headers(),
                params={"from": from_dt, "to": to_dt, "pageSize": 100}
            )
            r.raise_for_status()
            return r.json().get("data", {}).get("contents", [])

    async def get_channel_no(self) -> int:
        if hasattr(self, '_channel_no'):
            return self._channel_no
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f"{NAVER_BASE}/v1/seller/channels",
                headers=await self._headers()
            )
            r.raise_for_status()
            channels = r.json()
            self._channel_no = channels[0]["channelNo"] if channels else 1100092437
        return self._channel_no

    async def get_inquiries(self) -> list:
        ch = await self.get_channel_no()
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(
                f"{NAVER_BASE}/v1/channels/{ch}/question/questions",
                headers=await self._headers(),
                params={"answered": "false", "page": 1, "pageSize": 20}
            )
            r.raise_for_status()
            return r.json().get("questions", [])

    async def reply_inquiry(self, question_id: str, answer: str) -> bool:
        ch = await self.get_channel_no()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                f"{NAVER_BASE}/v1/channels/{ch}/question/questions/{question_id}/answers",
                headers=await self._headers(),
                json={"content": answer}
            )
            return r.status_code in (200, 201)

    async def update_stock(self, product_id: str, stock: int) -> bool:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.put(
                f"{NAVER_BASE}/v2/products/{product_id}/quantities",
                headers=await self._headers(),
                json={"stockQuantity": stock}
            )
            return r.status_code == 200


# ─── 오너클랜 Excel 파서 ──────────────────────────────────────────────────────
COLUMN_MAP = {
    "상품코드": "code", "공급사상품코드": "code",
    "상품명": "name",
    "오너클랜판매가": "price", "판매가": "price", "마켓판매가": "market_price",
    "카테고리": "category",
    "대카테고리": "cat_large", "중카테고리": "cat_medium", "소카테고리": "cat_small",
    "대표이미지": "image", "이미지URL": "image", "이미지": "image",
    "재고수량": "stock",
    "배송방법": "delivery_type", "배송비": "delivery_fee",
    "원산지": "origin", "브랜드": "brand", "제조사": "manufacturer",
    "상품설명": "desc",
}

def parse_excel(filepath: str) -> list[dict]:
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []

    # 헤더 행 자동 감지
    header_idx = 0
    for i, row in enumerate(rows[:5]):
        if row and sum(1 for v in row if str(v).strip() in COLUMN_MAP) >= 2:
            header_idx = i
            break

    headers = [str(v).strip() if v else "" for v in rows[header_idx]]
    col_idx = {i: COLUMN_MAP[h] for i, h in enumerate(headers) if h in COLUMN_MAP}

    products = []
    for row in rows[header_idx + 1:]:
        if not row or not any(row):
            continue
        item = {}
        for i, val in enumerate(row):
            if i in col_idx:
                item[col_idx[i]] = val
        if not item.get("name"):
            continue
        raw_price = item.get("market_price") or item.get("price") or 0
        try:
            item["price"] = int(float(str(raw_price).replace(",", "")))
        except (ValueError, TypeError):
            item["price"] = 0
        if item["price"] > 0:
            products.append(item)

    return products


# ─── 텍스트 정제 ─────────────────────────────────────────────────────────────
import re

def clean_product_name(name: str) -> str:
    name = re.sub(r'[^\w\s가-힣ㄱ-ㅎㅏ-ㅣa-zA-Z0-9\-/&()%+]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:50]


# ─── 가격 계산 ────────────────────────────────────────────────────────────────
def calculate_selling_price(wholesale_price: int) -> int:
    price = wholesale_price * (1 + MARGIN_RATE)
    return round(price / 10) * 10


# ─── Claude AI 상품 설명 생성 ─────────────────────────────────────────────────
async def generate_product_copy(product: dict) -> dict:
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=[{
            "type": "text",
            "text": "당신은 네이버 스마트스토어 상품 등록 전문가입니다. 반드시 JSON만 출력하세요.",
            "cache_control": {"type": "ephemeral"}
        }],
        messages=[{
            "role": "user",
            "content": f"""아래 상품 정보를 스마트스토어 최적화 형식으로 변환해주세요.

상품 정보:
{json.dumps(product, ensure_ascii=False)}

출력 형식 (JSON만):
{{
  "product_name": "검색 최적화 상품명 (50자 이내)",
  "description": "구매 욕구 자극 HTML 설명 (500자 이내, <p> 태그 사용)",
  "tags": ["태그1", "태그2", "태그3", "태그4", "태그5"]
}}"""
        }]
    )
    text = resp.content[0].text.strip()
    if "```" in text:
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:].strip()
    return json.loads(text)


# ─── 스마트스토어 상품 payload 빌더 ──────────────────────────────────────────
# 카테고리 기본값 (오너클랜 카테고리 → 네이버 카테고리 ID 매핑)
# 필요 시 https://api.commerce.naver.com/external/v1/categories/roots 조회 후 추가
CATEGORY_ID_MAP = {
    "남성의류": 50000830,
    "여성의류": 50000803,
    "티셔츠": 50000830,
    "바지": 50000831,
    "아우터": 50021640,
    "주방": 50002717,
    "생활용품": 50002717,
    "가전": 50005373,
    "뷰티": 50000140,
    "식품": 50000236,
    "스포츠": 50000430,
    "완구": 50000564,
    "도서": 50000727,
}
DEFAULT_CATEGORY_ID = 50000830


def get_category_id(product: dict) -> int:
    for key in ("cat_large", "cat_medium", "category"):
        val = str(product.get(key, ""))
        for k, v in CATEGORY_ID_MAP.items():
            if k in val:
                return v
    return DEFAULT_CATEGORY_ID


def build_product_payload(raw: dict, ai: dict, selling_price: int) -> dict:
    is_free = str(raw.get("delivery_type", "")).strip() in ("무료배송", "무료")
    try:
        delivery_fee = int(float(str(raw.get("delivery_fee", 3000)).replace(",", "")))
    except (ValueError, TypeError):
        delivery_fee = 3000

    return {
        "originProduct": {
            "statusType": "SALE",
            "saleType": "NEW",
            "leafCategoryId": get_category_id(raw),
            "name": clean_product_name(ai["product_name"]),
            "detailContent": ai["description"],
            "images": {
                "representativeImage": {"url": str(raw.get("image", ""))},
                "optionalImages": [],
            },
            "salePrice": selling_price,
            "stockQuantity": int(raw.get("stock") or 100),
            "deliveryInfo": {
                "deliveryType": "DELIVERY",
                "deliveryAttributeType": "NORMAL",
                "deliveryCompany": "CJGLS",
                "deliveryFee": {
                    "deliveryFeeType": "FREE" if is_free else "CHARGE",
                    "baseFee": 0 if is_free else delivery_fee,
                    "freeConditionalAmount": 0,
                },
                "claimDeliveryInfo": {
                    "returnDeliveryFee": 3000,
                    "exchangeDeliveryFee": 6000,
                    "deliveryCompany": "CJGLS",
                    "returnDeliveryCompany": "CJGLS",
                },
            },
            "detailAttribute": {
                "minorPurchasable": True,
                "naverShoppingSearchInfo": {
                    "modelInfo": str(raw.get("name", "")),
                    "manufacturerName": str(raw.get("manufacturer", "")),
                    "brandName": str(raw.get("brand", "")),
                },
                "afterServiceInfo": {
                    "afterServiceTelephoneNumber": AS_PHONE,
                    "afterServiceGuideContent": "상품 관련 문의는 스토어 문의하기를 이용해 주세요.",
                },
                "originAreaInfo": {
                    "originAreaCode": "0200037",
                    "content": str(raw.get("origin", "")),
                    "plural": False,
                    "importer": "해당없음",
                },
                "productInfoProvidedNotice": {
                    "productInfoProvidedNoticeType": "ETC",
                    "etc": {
                        "itemName": str(raw.get("name", "상세페이지 참조")),
                        "modelName": str(raw.get("name", "상세페이지 참조")),
                        "manufacturer": str(raw.get("manufacturer", "상세페이지 참조")) or "상세페이지 참조",
                        "customerServicePhoneNumber": AS_PHONE,
                        "returnCostReason": "상세페이지 참조",
                        "noRefundReason": "상세페이지 참조",
                        "qualityAssuranceStandard": "상세페이지 참조",
                        "compensationProcedure": AS_PHONE,
                        "troubleShootingContents": "상세페이지 참조",
                    },
                },
                "sellerCodeInfo": {
                    "sellerManagementCode": str(raw.get("code", ""))
                },
            },
        },
        "smartstoreChannelProduct": {
            "naverShoppingRegistration": True,
            "channelProductDisplayStatusType": "ON",
        },
    }


# ─── 이미지 처리 ─────────────────────────────────────────────────────────────
PEXELS_KEYWORD_MAP = {
    "티셔츠": "t-shirt", "바지": "pants", "아우터": "jacket outer",
    "원피스": "dress", "스커트": "skirt", "후드": "hoodie",
    "가방": "bag", "지갑": "wallet", "신발": "shoes sneakers",
    "주방": "kitchen cookware", "생활용품": "household items",
    "가전": "electronics appliance", "뷰티": "beauty cosmetics",
    "화장품": "cosmetics makeup", "식품": "food",
    "스포츠": "sports fitness", "완구": "toy", "도서": "book",
    "침구": "bedding", "가구": "furniture", "조명": "lighting lamp",
    "시계": "watch", "안경": "glasses eyewear", "모자": "hat cap",
    "양말": "socks", "속옷": "underwear", "수영복": "swimwear",
}

async def search_pexels_image(product_name: str) -> str | None:
    if not PEXELS_API_KEY:
        return None
    keyword = "product"
    for ko, en in PEXELS_KEYWORD_MAP.items():
        if ko in product_name:
            keyword = en
            break
    else:
        english = re.sub(r'[가-힣ㄱ-ㅎㅏ-ㅣ\W]+', ' ', product_name).strip()
        if english:
            keyword = english
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://api.pexels.com/v1/search",
                headers={"Authorization": PEXELS_API_KEY},
                params={"query": keyword, "per_page": 3, "orientation": "square"}
            )
            if r.status_code == 200:
                photos = r.json().get("photos", [])
                if photos:
                    return photos[0]["src"]["large"]
    except Exception:
        pass
    return None


async def get_product_image(p: dict) -> str | None:
    """오너클랜 이미지 시도 → 실패 시 Pexels 검색 → 없으면 None(등록 제외)"""
    image_url = str(p.get("image", "")).strip()

    # 1. 오너클랜 이미지 시도
    if image_url.startswith("http"):
        try:
            return await naver_api.upload_image(image_url)
        except Exception:
            print(f"[IMAGE] 오너클랜 이미지 실패, Pexels 검색 중...", flush=True)

    # 2. Pexels 폴백
    pexels_url = await search_pexels_image(str(p.get("name", "")))
    if pexels_url:
        try:
            return await naver_api.upload_image(pexels_url)
        except Exception:
            pass

    return None


# ─── 파이프라인 ───────────────────────────────────────────────────────────────
naver_api = NaverCommerceAPI()


async def pipeline_register_products(excel_path: str, limit: int = 50) -> dict:
    """파이프라인 1: 오너클랜 Excel → 소싱팀장 선별 → IP감시 → AI설명 → 스마트스토어 등록"""
    from employees import employee_sourcing_manager, employee_ip_guardian
    print(f"[REGISTER] 시작: {excel_path}", flush=True)
    products = parse_excel(excel_path)
    print(f"[REGISTER] 파싱 완료: {len(products)}개", flush=True)

    # 소싱팀장: 잘 팔릴 상품 선별
    products = await employee_sourcing_manager(products, limit, ANTHROPIC_API_KEY)
    print(f"[소싱팀장] 선별 완료: {len(products)}개", flush=True)

    results = {"success": 0, "fail": 0, "skip": 0, "ip_blocked": 0, "errors": []}
    for p in products[:limit]:
        try:
            # IP 감시관: 상표권 위험 체크
            safe, danger_kw = employee_ip_guardian(p)
            if not safe:
                print(f"[IP감시관] 차단: {p.get('name','')} — {danger_kw}", flush=True)
                results["ip_blocked"] += 1
                continue

            ai = await generate_product_copy(p)
            price = calculate_selling_price(p["price"])
            naver_img_url = await get_product_image(p)
            if not naver_img_url:
                print(f"[REGISTER] SKIP 이미지없음: {p.get('name', '')}", flush=True)
                results["skip"] += 1
                continue
            payload = build_product_payload(p, ai, price)
            payload["originProduct"]["images"]["representativeImage"]["url"] = naver_img_url
            await naver_api.register_product(payload)
            results["success"] += 1
            print(f"[REGISTER] OK {ai['product_name']} ({price:,}원)", flush=True)
            await asyncio.sleep(0.5)
        except Exception as e:
            results["fail"] += 1
            results["errors"].append(str(e))
            print(f"[REGISTER] FAIL {e}", flush=True)

    print(f"[REGISTER] 완료 — 성공:{results['success']} 실패:{results['fail']} 스킵:{results['skip']}", flush=True)
    return results


async def pipeline_process_orders() -> dict:
    """파이프라인 2: 신규 주문 조회"""
    print("[ORDER] 주문 조회 시작", flush=True)
    orders = await naver_api.get_new_orders()
    print(f"[ORDER] 신규 주문: {len(orders)}건", flush=True)
    return {"count": len(orders), "orders": orders}


async def pipeline_sync_inventory() -> dict:
    """파이프라인 3: 재고 동기화 (최신 Excel 기준)"""
    print("[STOCK] 재고 동기화 시작", flush=True)
    excel_files = sorted(Path(EXCEL_FOLDER).glob("*.xlsx"), key=lambda x: x.stat().st_mtime, reverse=True)
    if not excel_files:
        return {"status": "no_excel", "message": "업로드된 Excel 파일 없음"}
    products = parse_excel(str(excel_files[0]))
    print(f"[STOCK] {len(products)}개 상품 재고 확인", flush=True)
    return {"status": "synced", "count": len(products)}


async def pipeline_reply_inquiries() -> dict:
    """파이프라인 4: 미답변 고객 문의 자동 답변"""
    print("[INQUIRY] 문의 답변 시작", flush=True)
    try:
        inquiries = await naver_api.get_inquiries()
    except Exception as e:
        print(f"[INQUIRY] 문의 조회 실패: {e}", flush=True)
        return {"replied": 0, "total": 0, "error": str(e)}
    print(f"[INQUIRY] 미답변 문의: {len(inquiries)}건", flush=True)

    claude = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    replied = 0
    for q in inquiries:
        try:
            r = await claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=512,
                messages=[{
                    "role": "user",
                    "content": (
                        "당신은 친절한 스마트스토어 고객센터 직원입니다. "
                        "아래 고객 문의에 정중하고 간결하게 답변해주세요.\n\n"
                        f"문의 내용: {q.get('content', '')}"
                    )
                }]
            )
            answer = r.content[0].text
            success = await naver_api.reply_inquiry(str(q.get("questionId")), answer)
            if success:
                replied += 1
                print(f"[INQUIRY] ✅ 답변 완료: {q.get('questionId')}", flush=True)
        except Exception as e:
            print(f"[INQUIRY] ❌ {e}", flush=True)

    return {"replied": replied, "total": len(inquiries)}
