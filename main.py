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
OPENAI_API_KEY      = os.environ.get("OPENAI_API_KEY", "")
MARGIN_RATE         = float(os.environ.get("MARGIN_RATE", "0.15"))
EXCEL_FOLDER        = os.environ.get("EXCEL_FOLDER", "./uploads")
AS_PHONE            = os.environ.get("AS_PHONE", "010-0000-0000")

NAVER_BASE = "https://api.commerce.naver.com/external"

Path(EXCEL_FOLDER).mkdir(exist_ok=True)
REGISTERED_CODES_FILE = "./uploads/registered_codes.json"

def load_registered_codes() -> set:
    try:
        with open(REGISTERED_CODES_FILE, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_registered_code(code: str):
    codes = load_registered_codes()
    codes.add(str(code))
    with open(REGISTERED_CODES_FILE, "w") as f:
        json.dump(list(codes), f)


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
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            img_resp = await c.get(image_url)
            img_resp.raise_for_status()

        # 1000×1000 이상으로 업스케일 후 고화질 저장
        try:
            from PIL import Image
            import io as _io
            img = Image.open(_io.BytesIO(img_resp.content)).convert("RGB")
            w, h = img.size
            target = 1000
            if w < target or h < target:
                scale = target / min(w, h)
                new_w, new_h = int(w * scale), int(h * scale)
                img = img.resize((new_w, new_h), Image.LANCZOS)
            buf = _io.BytesIO()
            img.save(buf, format="JPEG", quality=95, subsampling=0,
                     optimize=True, progressive=True)
            image_bytes = buf.getvalue()
        except Exception:
            image_bytes = img_resp.content

        token = await self.get_token()
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{NAVER_BASE}/v1/product-images/upload",
                headers={"Authorization": f"Bearer {token}"},
                files={"imageFiles": ("image.jpg", image_bytes, "image/jpeg")}
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
            if not r.is_success:
                raise Exception(f"Naver API {r.status_code}: {r.text[:500]}")
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

    async def list_products(self, page: int = 1, size: int = 50) -> dict:
        """등록된 상품 목록 조회"""
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{NAVER_BASE}/v1/products/search",
                headers=await self._headers(),
                json={
                    "productStatusTypes": ["SALE", "SUSPENSION"],
                    "page": page,
                    "size": size,
                    "orderType": "NO",
                    "periodType": "PROD_REG_DAY",
                    "fromDate": (now - timedelta(days=365)).strftime("%Y-%m-%d"),
                    "toDate": now.strftime("%Y-%m-%d"),
                }
            )
            r.raise_for_status()
            return r.json()

    async def set_product_status(self, product_id: str, status: str) -> bool:
        """상품 상태 변경: SALE(판매중) / SUSPENSION(판매중지) / CLOSE(판매종료)"""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.put(
                f"{NAVER_BASE}/v2/products/{product_id}",
                headers=await self._headers(),
                json={"originProduct": {"statusType": status}}
            )
            return r.status_code == 200

    async def delete_product(self, product_id: str) -> bool:
        """상품 삭제"""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.delete(
                f"{NAVER_BASE}/v2/products/{product_id}",
                headers=await self._headers()
            )
            return r.status_code in (200, 204)

    async def update_price(self, product_id: str, price: int) -> bool:
        """상품 가격 수정"""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.put(
                f"{NAVER_BASE}/v2/products/{product_id}",
                headers=await self._headers(),
                json={"originProduct": {"salePrice": price}}
            )
            return r.status_code == 200

    async def confirm_orders(self, product_order_ids: list) -> bool:
        """주문 발주확인 처리"""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                f"{NAVER_BASE}/v1/pay-order/seller/product-orders/confirm",
                headers=await self._headers(),
                json={"productOrderIds": product_order_ids}
            )
            return r.status_code == 200

    async def get_all_orders(self, days: int = 7) -> list:
        """최근 N일 주문 전체 조회"""
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        from_dt = (now - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00.000Z")
        to_dt = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(
                f"{NAVER_BASE}/v1/pay-order/seller/product-orders",
                headers=await self._headers(),
                params={"from": from_dt, "to": to_dt, "pageSize": 300}
            )
            r.raise_for_status()
            return r.json().get("data", {}).get("contents", [])


# ─── 오너클랜 Excel 파서 ──────────────────────────────────────────────────────
COLUMN_MAP = {
    "상품코드": "code", "공급사상품코드": "code", "업체상품코드": "code",
    "판매자관리코드": "code",
    "상품명": "name", "상품명(필수)": "name",
    "원본상품명": "name", "마켓상품명": "name",
    "오너클랜판매가": "price", "판매가": "price", "공급가": "price", "도매가": "price",
    "마켓판매가": "market_price", "마켓실제판매가": "market_price", "소비자가": "market_price",
    "카테고리코드": "category_id",
    "카테고리명": "category", "카테고리": "category",
    "대카테고리": "cat_large", "중카테고리": "cat_medium", "소카테고리": "cat_small",
    "대표이미지": "image", "이미지URL": "image", "이미지": "image",
    "이미지1": "image", "대표이미지URL": "image", "상품이미지": "image",
    "재고수량": "stock", "재고": "stock",
    "배송방법": "delivery_type", "배송유형": "delivery_type", "배송비": "delivery_fee",
    "원산지": "origin", "브랜드": "brand", "제조사": "manufacturer",
    "상품설명": "desc",
}

def _match_col(header):
    """COLUMN_MAP 완전일치 → 포함관계 순으로 매핑"""
    h = str(header).strip() if header else ""
    if not h:
        return None
    if h in COLUMN_MAP:
        return COLUMN_MAP[h]
    for k, v in COLUMN_MAP.items():
        if k in h:
            return v
    return None

def _to_int(v):
    try:
        return int(float(str(v).replace(",", "")))
    except (ValueError, TypeError):
        return 0

def parse_excel(filepath):
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []

    # 헤더 행 자동 감지 — row1부터 탐색 (오너클랜 row0=그룹헤더라 건너뜀)
    header_idx = min(1, len(rows) - 1)
    for i, row in enumerate(rows[1:6], start=1):
        if row and sum(1 for v in row if _match_col(v)) >= 2:
            header_idx = i
            break

    headers = [str(v).strip() if v else "" for v in rows[header_idx]]
    col_idx = {}
    for i, h in enumerate(headers):
        m = _match_col(h)
        if m:
            col_idx[i] = m

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
        price = _to_int(item.get("price")) or _to_int(item.get("market_price"))
        item["price"] = price
        if price > 0:
            products.append(item)

    return products


# ─── 텍스트 정제 ─────────────────────────────────────────────────────────────
import re

_NAVER_BANNED = {
    "최고","제일","넘버원","1등","공짜","무료","특가","대박","혜자","최저가",
    "최대","독점","정품보장","즉시발송","당일발송","무조건","완전","초강력",
    "역대급","미쳤다","개이득","레전드","신상","핫딜"
}
_AD_PATTERN = re.compile(
    r'즉시발송|당일출발|최저가|무료배송|특가|광고|이벤트|[★☆◆◇■□●○▶▷]'
)

def clean_product_name(name: str) -> str:
    # ① 광고성 패턴 제거
    name = _AD_PATTERN.sub(' ', name)
    # ② 특수문자 블록 제거 (괄호류, 슬래시)
    name = re.sub(r'[\[\](){}<>/\\|*~`^]', ' ', name)
    # ③ 모델·관리번호 제거 (영문+숫자 혼합)
    name = re.sub(r'\b[A-Za-z]{1,5}[-_]?\d{3,}[A-Za-z0-9-]*\b', '', name)
    name = re.sub(r'\b\d{5,}\b', '', name)
    # ④ 나머지 특수문자 제거
    name = re.sub(r'[^\w\s가-힣ㄱ-ㅎㅏ-ㅣa-zA-Z0-9\- ]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    # ⑤ 금지어 제거
    words = [w for w in name.split() if w not in _NAVER_BANNED]
    # ⑥ 중복 단어 제거 (순서 유지) — "캠핑의자 의자" → "캠핑의자"
    seen, deduped = set(), []
    for w in words:
        key = w.rstrip('s').lower()  # 영문 복수형 동일 처리
        if key not in seen:
            seen.add(key); deduped.append(w)
    return ' '.join(deduped)[:25]


# ─── 가격 계산 ────────────────────────────────────────────────────────────────
def calculate_selling_price(wholesale_price: int) -> int:
    price = wholesale_price * (1 + MARGIN_RATE)
    return round(price / 10) * 10


# ─── Claude AI 상품 설명 생성 (전 직원 협업 버전) ────────────────────────────
async def generate_product_copy(product: dict, context: dict = None) -> dict:
    """시즌+트렌드+리뷰 Pain Point 반영한 상품 설명 생성"""
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    ctx = context or {}
    season_info = ctx.get("season", "")
    trend_keywords = ctx.get("trends", [])
    pain_points = ctx.get("pain_points", [])
    selling_points = ctx.get("selling_points", [])

    extra_context = ""
    if season_info:
        extra_context += f"\n현재 시즌 이벤트: {season_info}"
    if trend_keywords:
        extra_context += f"\n실시간 트렌딩 키워드: {', '.join(trend_keywords[:5])}"
    if pain_points:
        extra_context += f"\n고객 Pain Point (반드시 해결책 언급): {', '.join(pain_points)}"
    if selling_points:
        extra_context += f"\n핵심 셀링포인트: {', '.join(selling_points)}"

    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        system=[{
            "type": "text",
            "text": (
                "당신은 10년차 네이버 스마트스토어 마케팅 전문가입니다. "
                "상품명 규칙: ① [브랜드명]+[핵심키워드]+[속성/규격] 25자 내외 "
                "② 특수문자([],(),/,*), 관리번호, 동일단어 중복 절대 금지 "
                "③ 최고·제일·1등·공짜·무료·특가 등 네이버 금지어 사용 금지. "
                "반드시 JSON만 출력하세요."
            ),
            "cache_control": {"type": "ephemeral"}
        }],
        messages=[{
            "role": "user",
            "content": f"""아래 상품을 스마트스토어 SEO 최적화 형식으로 변환하세요.
{extra_context}

상품 정보:
{json.dumps(product, ensure_ascii=False)}

출력 형식 (JSON만):
{{
  "product_name": "브랜드+핵심키워드+속성/규격, 25자 내외, 모델번호/특수문자/중복단어/금지어 없이 자연스러운 한국어",
  "headline": "Pillow 배너용 핵심 편익 (예: '한 번으로 3배 오래!'), 18자 이내, 숫자 필수",
  "sub_headline": "배너 서브 문구 (예: '교체 빈도 줄이고 비용 절약'), 28자 이내",
  "emotional_copy": "감성 설득 문구 2~3문장. 구매자의 일상과 연결되는 따뜻하고 공감가는 문장으로 작성. 100자 내외.",
  "recommend_list": ["이런 분 추천 1 (20자 이내)", "이런 분 추천 2", "이런 분 추천 3", "이런 분 추천 4", "이런 분 추천 5"],
  "reason_1": "사야 하는 이유 1 — Pain Point 해결 중심, 구체적 수치 포함 (40자 이내)",
  "reason_2": "사야 하는 이유 2 — 차별점/기술력 강조 (40자 이내)",
  "reason_3": "사야 하는 이유 3 — 시즌/트렌드/절약 연결 (40자 이내)",
  "spec_rows": [["항목","값"],["항목","값"],["항목","값"],["항목","값"],["항목","값"]],
  "tags": ["태그1","태그2","태그3","태그4","태그5"]
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
    # 오너클랜 Excel의 카테고리코드 직접 사용
    cat_id = product.get("category_id")
    if cat_id:
        try:
            return int(str(cat_id).strip())
        except (ValueError, TypeError):
            pass
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
            "detailContent": ai.get("description", ai.get("emotional_copy", "")),
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
                    "deliveryFeeType": "FREE" if is_free else "PAID",
                    "deliveryFeePayType": "PREPAID",
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
                **({"sellerCodeInfo": {"sellerManagementCode": str(raw.get("code", ""))}} if raw.get("code") else {}),
            },
        },
        "smartstoreChannelProduct": {
            "naverShoppingRegistration": True,
            "channelProductDisplayStatusType": "ON",
        },
    }


# ─── 이미지 디렉터: 텍스트 배너 생성 ────────────────────────────────────────
async def create_banner_image(image_url: str, main_text: str, sub_text: str = "") -> str | None:
    """1000×500 헤드라인 배너 — #1a1a1a 상단바 + 고화질 저장"""
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io

        W, H = 1000, 500
        FONT_SIZE_MAIN = int(W * 0.08)   # 가로 폭의 8%
        FONT_SIZE_SUB  = int(W * 0.042)

        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.get(image_url)
            r.raise_for_status()

        # 원본 이미지를 LANCZOS로 중앙 크롭-리사이즈
        orig = Image.open(io.BytesIO(r.content)).convert("RGB")
        orig_ratio = orig.width / orig.height
        target_ratio = W / H
        if orig_ratio > target_ratio:
            new_h = orig.height
            new_w = int(new_h * target_ratio)
            left = (orig.width - new_w) // 2
            orig = orig.crop((left, 0, left + new_w, new_h))
        else:
            new_w = orig.width
            new_h = int(new_w / target_ratio)
            top = (orig.height - new_h) // 2
            orig = orig.crop((0, top, new_w, top + new_h))
        img = orig.resize((W, H), Image.LANCZOS)

        # 상단 텍스트 배너 영역: 배경색 #1a1a1a, 높이 H*0.36
        bar_h = int(H * 0.36)
        bar = Image.new("RGB", (W, bar_h), (26, 26, 26))  # #1a1a1a

        # 하단 이미지 부분 (나머지 높이)
        img_part = img.crop((0, 0, W, H - bar_h))
        # 최종 합성: 상단 #1a1a1a 바 + 하단 상품 이미지
        final = Image.new("RGB", (W, H))
        final.paste(bar, (0, 0))
        final.paste(img_part, (0, bar_h))

        draw = ImageDraw.Draw(final)

        # 폰트 (Bold)
        font_paths = [
            "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ]
        main_font = sub_font = ImageFont.load_default()
        for fp in font_paths:
            try:
                main_font = ImageFont.truetype(fp, FONT_SIZE_MAIN)
                sub_font  = ImageFont.truetype(fp, FONT_SIZE_SUB)
                break
            except Exception:
                pass

        # 텍스트: 상단 바 중앙
        cy_main = int(bar_h * 0.42)
        cy_sub  = int(bar_h * 0.78)
        draw.text((W // 2, cy_main), main_text, font=main_font, fill="#ffffff", anchor="mm")
        if sub_text:
            draw.text((W // 2, cy_sub), sub_text[:30], font=sub_font, fill="#cccccc", anchor="mm")

        buf = io.BytesIO()
        # quality=95, optimize=True, progressive=True
        final.save(buf, format="JPEG", quality=95, subsampling=0,
                   optimize=True, progressive=True)
        buf.seek(0)
        token = await naver_api.get_token()
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{NAVER_BASE}/v1/product-images/upload",
                headers={"Authorization": f"Bearer {token}"},
                files={"imageFiles": ("banner.jpg", buf.read(), "image/jpeg")}
            )
            r.raise_for_status()
            return r.json()["images"][0]["url"]
    except Exception as e:
        print(f"[이미지디렉터] 배너 생성 실패: {e}", flush=True)
        return None


# ─── 스마트스토어 상세페이지 HTML 빌더 ──────────────────────────────────────
_IMG = 'style="max-width:100%;height:auto;display:block;margin:20px auto;"'
_WRAP = ('style="max-width:860px;margin:0 auto;padding:0 16px 24px;'
         'font-family:\'나눔고딕\',Apple SD Gothic Neo,sans-serif;color:#333;"')

def build_detail_html(banner_url: str, product_img_url: str, ai: dict) -> str:
    """
    6-섹션 설득형 상세페이지
    ① Pillow 헤드라인 배너
    ② Intro <h2> — 상품 최대 장점 한 줄
    ③ 이런 분께 추천 체크리스트 (3개 이상)
    ④ 상품 이미지 + 이미지 사이 감성 문구
    ⑤ 이유 3가지 컬러 카드
    ⑥ 스펙 <table>
    """
    # ① Pillow 헤드라인 배너
    sec1 = f'<img src="{banner_url}" {_IMG}>' if banner_url else ""

    # ② Intro — h2 태그, 상품 최대 장점
    headline = ai.get("headline", ai.get("banner_text", ""))
    emotional = ai.get("emotional_copy", "")
    sec2 = (
        f'<div {_WRAP}>'
        + (f'<h2 style="font-size:22px;color:#1a1a1a;margin:28px 0 10px;line-height:1.5;">'
           f'{headline}</h2>' if headline else "")
        + (f'<p style="font-size:15px;line-height:1.9;color:#555;margin:0 0 8px;">'
           f'{emotional}</p>' if emotional else "")
        + '</div>'
    )

    # ③ 이런 분께 강력 추천 — 체크리스트 (불렛포인트 3~5개)
    recs = [r for r in ai.get("recommend_list", []) if r][:5]
    if recs:
        items_html = "".join(
            f'<li style="padding:11px 0 11px 38px;position:relative;'
            f'border-bottom:1px solid #dde8ff;font-size:15px;color:#333;line-height:1.6;">'
            f'<span style="position:absolute;left:0;top:10px;color:#1a73e8;font-size:20px;'
            f'font-weight:bold;">✔</span>{r}</li>'
            for r in recs
        )
        sec3 = (
            f'<div {_WRAP}>'
            f'<div style="background:#eef4ff;border-radius:14px;padding:26px 22px;">'
            f'<h3 style="font-size:17px;font-weight:bold;color:#1a73e8;margin:0 0 18px;'
            f'text-align:center;">💙 이런 분들께 강력 추천합니다</h3>'
            f'<ul style="list-style:none;padding:0;margin:0;">{items_html}</ul>'
            f'</div></div>'
        )
    else:
        sec3 = ""

    # ④ 상품 이미지 + 이미지 사이 감성 문구 (reason_1 사이에 삽입)
    r1 = ai.get("reason_1", "")
    sec4 = (
        f'<img src="{product_img_url}" {_IMG}>'
        + (
            f'<div {_WRAP}>'
            f'<p style="font-size:15px;line-height:1.9;color:#555;'
            f'border-left:3px solid #1a73e8;padding:12px 16px;'
            f'background:#f8fbff;margin:0;">{r1}</p>'
            f'</div>'
            if r1 else ""
        )
    ) if product_img_url else ""

    # ⑤ 이유 3가지 컬러 카드 (reason_2, reason_3)
    reason_cards = []
    for i, bg in enumerate(["#fff3e0", "#fce4ec"], start=2):
        txt = ai.get(f"reason_{i}", "")
        if txt:
            reason_cards.append(
                f'<div style="background:{bg};border-radius:10px;padding:18px 20px;margin:10px 0;">'
                f'<b style="font-size:20px;color:#888;">0{i}&nbsp;</b>'
                f'<span style="font-size:15px;line-height:1.8;">{txt}</span></div>'
            )
    if reason_cards:
        sec5 = (
            f'<div {_WRAP}>'
            f'<h3 style="font-size:17px;border-left:4px solid #1a73e8;'
            f'padding-left:12px;margin:28px 0 14px;">✅ 이 상품을 선택해야 하는 이유</h3>'
            + "".join(reason_cards) + '</div>'
        )
    else:
        sec5 = ""

    # ⑥ 스펙 <table>
    spec_rows = ai.get("spec_rows", [])
    if spec_rows:
        rows_html = "".join(
            f'<tr>'
            f'<td style="background:#f5f7fa;padding:11px 14px;font-weight:bold;'
            f'width:38%;border:1px solid #ddd;font-size:14px;">{r[0]}</td>'
            f'<td style="padding:11px 14px;border:1px solid #ddd;font-size:14px;">'
            f'{r[1] if len(r) > 1 else ""}</td></tr>'
            for r in spec_rows
        )
        sec6 = (
            f'<div {_WRAP}>'
            f'<h3 style="font-size:17px;border-left:4px solid #1a73e8;'
            f'padding-left:12px;margin:28px 0 14px;">📋 상품 스펙</h3>'
            f'<table style="width:100%;border-collapse:collapse;">{rows_html}</table>'
            f'</div>'
        )
    else:
        sec6 = ""

    return sec1 + sec2 + sec3 + sec4 + sec5 + sec6


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


async def generate_dalle_image(product_name: str) -> str | None:
    """DALL-E 3로 상품 이미지 생성 → URL 반환"""
    if not OPENAI_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(
                "https://api.openai.com/v1/images/generations",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "dall-e-3",
                    "prompt": f"Professional product photo for Korean e-commerce, clean white background, high quality studio lighting, no text, no watermark. Product: {product_name}",
                    "n": 1,
                    "size": "1024x1024",
                    "quality": "standard",
                }
            )
            r.raise_for_status()
            return r.json()["data"][0]["url"]
    except Exception as e:
        print(f"[DALLE] 이미지 생성 실패: {e}", flush=True)
        return None


async def _is_text_heavy_image(image_url: str) -> bool:
    """배송/반품 안내 등 텍스트 과다 이미지 감지 — 세로가 가로의 2.5배 초과 시 제외"""
    try:
        from PIL import Image
        import io
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(image_url)
            r.raise_for_status()
        img = Image.open(io.BytesIO(r.content))
        w, h = img.size
        return h > w * 2.5
    except Exception:
        return False


async def get_product_image(p: dict) -> str | None:
    """오너클랜 이미지 (텍스트 과다 제외) → Pexels 폴백 → DALL-E 폴백 → None"""
    image_url = str(p.get("image", "")).strip()

    # 1. 오너클랜 이미지 시도 (텍스트 과다 이미지 제외)
    if image_url.startswith("http"):
        if await _is_text_heavy_image(image_url):
            print(f"[IMAGE] 텍스트 과다 이미지 제외 → Pexels 폴백", flush=True)
        else:
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

    # 3. DALL-E 폴백
    print(f"[IMAGE] Pexels 실패, DALL-E 생성 중...", flush=True)
    dalle_url = await generate_dalle_image(str(p.get("name", "")))
    if dalle_url:
        try:
            return await naver_api.upload_image(dalle_url)
        except Exception:
            pass

    return None


# ─── 파이프라인 ───────────────────────────────────────────────────────────────
naver_api = NaverCommerceAPI()


async def pipeline_register_products(excel_path: str, limit: int = 50) -> dict:
    """파이프라인 1: 전 직원 협업 — 소싱→IP→시즌→트렌드→리뷰→설명→이미지→등록"""
    from employees import (
        employee_sourcing_manager, employee_ip_guardian,
        employee_season_planner, employee_trend_scout, employee_review_analyst
    )
    print(f"[총괄] 상품 등록 시작: {excel_path}", flush=True)
    products = parse_excel(excel_path)
    print(f"[총괄] 파싱 완료: {len(products)}개", flush=True)

    # ① 소싱팀장: 잘 팔릴 상품 선별
    products = await employee_sourcing_manager(products, limit, ANTHROPIC_API_KEY)
    print(f"[소싱팀장] 선별: {len(products)}개", flush=True)

    # ② 시즌 기획자: 현재 시즌 파악
    season_data = employee_season_planner()
    season_info = season_data["upcoming"][0]["event"] if season_data["upcoming"] else ""
    if season_info:
        print(f"[시즌기획자] 현재 시즌: {season_info}", flush=True)

    # ③ 트렌드 스카우터: 트렌딩 키워드 수집
    trend_keywords = await employee_trend_scout()
    print(f"[트렌드스카우터] 키워드 {len(trend_keywords)}개 수집", flush=True)

    registered_codes = load_registered_codes()
    print(f"[총괄] 기등록 상품: {len(registered_codes)}개 제외", flush=True)

    results = {"success": 0, "fail": 0, "skip": 0, "duplicate": 0, "ip_blocked": 0, "errors": []}
    for p in products[:limit]:
        try:
            # 중복 체크
            code = str(p.get("code", ""))
            if code and code in registered_codes:
                results["duplicate"] += 1
                continue

            # ④ IP 감시관: 상표권 위험 체크
            safe, danger_kw = employee_ip_guardian(p)
            if not safe:
                print(f"[IP감시관] 차단: {p.get('name','')} — {danger_kw}", flush=True)
                results["ip_blocked"] += 1
                continue

            # ⑤ 리뷰 분석가: Pain Point 분석
            review = await employee_review_analyst(str(p.get("name", "")), ANTHROPIC_API_KEY)

            # ⑥ 상품 설명 작가: 전 직원 데이터 통합해서 설명 생성
            context = {
                "season": season_info,
                "trends": trend_keywords[:5],
                "pain_points": review.get("pain_points", []),
                "selling_points": review.get("selling_points", []),
            }
            ai = await generate_product_copy(p, context)
            price = calculate_selling_price(p["price"])

            # ⑦ 이미지 디렉터: 메인 이미지
            naver_img_url = await get_product_image(p)
            if not naver_img_url:
                print(f"[이미지디렉터] SKIP 이미지없음: {p.get('name', '')}", flush=True)
                results["skip"] += 1
                continue

            # ⑧ 이미지 디렉터: 상세페이지 텍스트 배너 생성
            banner_url = await create_banner_image(
                naver_img_url,
                ai.get("headline") or ai.get("banner_text") or p.get("name", "")[:18],
                ai.get("sub_headline") or ai.get("sub_text", "")
            )

            payload = build_product_payload(p, ai, price)
            payload["originProduct"]["images"]["representativeImage"]["url"] = naver_img_url
            detail_html = build_detail_html(banner_url, naver_img_url, ai)
            if detail_html:
                payload["originProduct"]["detailContent"] = detail_html

            await naver_api.register_product(payload)
            save_registered_code(code)
            results["success"] += 1
            print(f"[총괄] ✅ {ai['product_name']} ({price:,}원)", flush=True)
            await asyncio.sleep(0.5)

        except Exception as e:
            results["fail"] += 1
            results["errors"].append(str(e))
            print(f"[총괄] ❌ {e}", flush=True)

    print(f"[총괄] 완료 — 성공:{results['success']} 실패:{results['fail']} 스킵:{results['skip']} IP차단:{results['ip_blocked']}", flush=True)
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
