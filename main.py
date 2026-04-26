"""
스마트스토어 자동화 v2
소싱: 오너클랜 Excel DB
판매: 네이버 스마트스토어 커머스 API
"""

import asyncio
import base64
import io as _io
import json
import os
import re as _re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import anthropic
import bcrypt
import httpx
import openpyxl
from dotenv import load_dotenv


# ─── 이미지 고화질 처리 유틸 ──────────────────────────────────────────────────
_THUMB_PATTERN = _re.compile(
    r'(_\d{2,4}x\d{2,4}|_thumb|_small|_medium|_low|_300|_400|_500'
    r'|\?.*?(width|w|size)=\d+.*?)',
    _re.IGNORECASE
)

def _extract_hq_url(url: str) -> str:
    """썸네일 접미사 제거 → 원본 고해상도 URL 반환"""
    if not url:
        return url
    # 쿼리스트링 크기 제한 제거
    clean = _re.sub(r'[?&](width|w|size|h|height)=\d+', '', url)
    # 경로 접미사 제거 (_300, _thumb 등)
    clean = _re.sub(
        r'(_\d{2,4}x\d{2,4}|_thumb|_small|_medium|_low|_300|_400|_500)',
        '', clean, flags=_re.IGNORECASE
    )
    return clean.strip('?&')


def _process_image_hq(raw_bytes: bytes, target: int = 1000, banner: bool = False) -> bytes:
    """
    상품컷: 1000×1000 흰 캔버스 중앙 배치 (정사각형 패딩)
    배너:  가로 860px 유지, 비율 그대로 리사이징 (찌그러짐 방지)
    필터: 축소=LANCZOS, 업스케일=BICUBIC
    저장: quality=95, subsampling=0, optimize, progressive
    """
    try:
        from PIL import Image
        img = Image.open(_io.BytesIO(raw_bytes)).convert("RGB")
        w, h = img.size

        if banner:
            # 배너: 가로 860px에 맞춰 비율 유지 리사이징
            target_w = 860
            scale = target_w / w
            new_w, new_h = target_w, max(1, int(h * scale))
            filt = Image.LANCZOS if scale < 1.0 else Image.BICUBIC
            img = img.resize((new_w, new_h), filt)
            canvas = img  # 배너는 패딩 없이 그대로
        else:
            # 상품컷: 1000×1000 흰 캔버스 중앙 배치
            scale = min(target / w, target / h)
            filt = Image.LANCZOS if scale < 1.0 else Image.BICUBIC
            new_w, new_h = max(1, int(w * scale)), max(1, int(h * scale))
            img = img.resize((new_w, new_h), filt)
            canvas = Image.new("RGB", (target, target), (255, 255, 255))
            canvas.paste(img, ((target - new_w) // 2, (target - new_h) // 2))

        buf = _io.BytesIO()
        canvas.save(buf, format="JPEG", quality=95, subsampling=0,
                    optimize=True, progressive=True)
        return buf.getvalue()
    except Exception as e:
        print(f"[IMAGE] 처리 실패, 원본 사용: {e}", flush=True)
        return raw_bytes

load_dotenv()

# ─── 환경변수 ────────────────────────────────────────────────────────────────
NAVER_CLIENT_ID     = os.environ.get("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")
NAVER_SELLER_ID     = os.environ.get("NAVER_SELLER_ID", "")
import re as _re_keys
def _clean_key(k: str) -> str:
    return _re_keys.sub(r'[^\x21-\x7E]', '', k or "")

ANTHROPIC_API_KEY   = _clean_key(os.environ.get("ANTHROPIC_API_KEY", ""))
PEXELS_API_KEY      = _clean_key(os.environ.get("PEXELS_API_KEY", ""))
OPENAI_API_KEY      = _clean_key(os.environ.get("OPENAI_API_KEY", ""))
GOOGLE_AI_API_KEY   = _clean_key(os.environ.get("GOOGLE_AI_API_KEY", ""))
FLUX_API_KEY        = _clean_key(os.environ.get("FLUX_API_KEY", ""))
MARGIN_RATE         = float(os.environ.get("MARGIN_RATE", "0.15"))
EXCEL_FOLDER        = os.environ.get("EXCEL_FOLDER", "./uploads")
AS_PHONE            = os.environ.get("AS_PHONE", "010-0000-0000")

NAVER_BASE = "https://api.commerce.naver.com/external"

Path(EXCEL_FOLDER).mkdir(exist_ok=True)
REGISTERED_CODES_FILE = "./uploads/registered_codes.json"
CLEANUP_LOG_FILE      = "./uploads/auto_cleanup.jsonl"

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

    async def upload_image(self, image_url: str, is_banner: bool = False) -> str:
        # ① 원본 고해상도 URL 추출 (썸네일 접미사 제거)
        clean_url = _extract_hq_url(image_url)
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            img_resp = await c.get(clean_url)
            if img_resp.status_code != 200 and clean_url != image_url:
                img_resp = await c.get(image_url)
            img_resp.raise_for_status()

        # ② 이미지 처리 — 배너는 860px 가로 유지, 상품컷은 1000×1000 정사각형
        image_bytes = _process_image_hq(img_resp.content, target=1000, banner=is_banner)

        token = await self.get_token()
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{NAVER_BASE}/v1/product-images/upload",
                headers={"Authorization": f"Bearer {token}"},
                files={"imageFiles": ("image.jpg", image_bytes, "image/jpeg")}
            )
            r.raise_for_status()
            return r.json()["images"][0]["url"]

    async def upload_raw_image(self, raw_bytes: bytes, is_banner: bool = False) -> str:
        """raw bytes를 직접 네이버에 업로드 (Gemini 등 URL 없는 소스용)"""
        image_bytes = _process_image_hq(raw_bytes, target=1000, banner=is_banner)
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
        """등록된 상품 목록 조회 (상품명/가격/상태 포함).
        search API는 ID만 반환하므로 개별 상세 조회를 asyncio.gather로 병렬 처리."""
        now = datetime.now(timezone.utc)
        headers = await self._headers()
        async with httpx.AsyncClient(timeout=30) as c:
            # 1단계: 상품 ID 목록 조회
            r = await c.post(
                f"{NAVER_BASE}/v1/products/search",
                headers=headers,
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
            data = r.json()
            contents = data.get("contents", [])

            # 2단계: 각 상품 상세 병렬 조회
            async def _fetch_detail(item: dict) -> dict:
                product_no = item.get("originProductNo")
                if not product_no:
                    return item
                try:
                    dr = await c.get(
                        f"{NAVER_BASE}/v2/products/origin-products/{product_no}",
                        headers=headers,
                        timeout=15,
                    )
                    if dr.status_code == 200:
                        item["originProduct"] = dr.json().get("originProduct", {})
                except Exception:
                    pass
                return item

            enriched = await asyncio.gather(*[_fetch_detail(item) for item in contents])
            data["contents"] = list(enriched)
            return data

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

    async def get_product_insight(self, channel_product_no: str, days: int = 30) -> dict | None:
        """채널상품 조회수·판매수 조회 (30일 기준)"""
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        from_date = (now - timedelta(days=days)).strftime("%Y-%m-%d")
        to_date = now.strftime("%Y-%m-%d")
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(
                    f"{NAVER_BASE}/v1/channel-products/{channel_product_no}/insights",
                    headers=await self._headers(),
                    params={"searchDateFrom": from_date, "searchDateTo": to_date},
                )
                if r.status_code == 200:
                    return r.json()
        except Exception as e:
            print(f"[INSIGHT] {channel_product_no} 조회 실패: {e}", flush=True)
        return None

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
    # ② 네이버 허용 문자만 남김: 한글·영문·숫자·공백·하이픈
    name = re.sub(r'[^가-힣ㄱ-ㆎa-zA-Z0-9\s\-]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    # ③ 모델·관리번호 제거 (영문+숫자 혼합 5자리 이상)
    name = re.sub(r'\b[A-Za-z]{1,4}\d{3,}[A-Za-z0-9\-]*\b', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    # ④ 금지어 제거
    words = [w for w in name.split() if w not in _NAVER_BANNED]
    # ⑤ 중복 단어 제거
    seen, deduped = set(), []
    for w in words:
        key = w.rstrip('s').lower()
        if key not in seen:
            seen.add(key); deduped.append(w)
    result = ' '.join(deduped)[:25].strip()
    return result if result else name[:25]


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
  "spec_hint": "디테일컷 DALL-E 프롬프트용 힌트 — 소재·기능·특징 영어 10단어 이내 (예: soft microfiber texture and adjustable buckle)",
  "compare_points": ["타사 대비 차별점 1 (30자)", "차별점 2", "차별점 3"],
  "tags": ["태그1","태그2","태그3","태그4","태그5"]
}}"""
        }]
    )
    text = resp.content[0].text.strip()
    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end > start:
        text = text[start:end]
    _BAD_NAME_PATTERNS = ("불완전", "오류", "에러", "유효하지", "입력된", "BAD_REQUEST", "error", "invalid")
    try:
        result = json.loads(text)
        pname = result.get("product_name", "")
        if not pname or any(p in pname for p in _BAD_NAME_PATTERNS):
            result["product_name"] = product.get("name", "상품")[:25]
        return result
    except Exception:
        name = product.get("name", "상품")[:25]
        return {
            "product_name": name,
            "headline": f"{name[:14]} 특가",
            "sub_headline": "지금 바로 확인하세요",
            "emotional_copy": f"{name}을 합리적인 가격에 만나보세요.",
            "recommend_list": ["실용적인 제품을 원하시는 분", "가성비를 중시하는 분",
                               "품질을 중시하는 분", "선물용으로 찾으시는 분", "빠른 배송이 필요하신 분"],
            "reason_1": "검증된 품질로 만족도 높음",
            "reason_2": "합리적인 가격대 형성",
            "reason_3": "빠른 배송 및 안전 포장",
            "spec_rows": [["상품명", name], ["배송", "택배"], ["원산지", "국내/해외"],
                          ["반품", "수령 후 7일 이내"], ["교환", "상품 불량 시 가능"]],
            "spec_hint": "product detail texture and quality",
            "compare_points": ["검증된 품질", "합리적 가격", "빠른 배송"],
            "tags": [name[:5], "추천", "특가", "당일발송", "품질보장"],
            "description": f"{name} 상품입니다.",
        }


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
    "가전": 50000830,   # 임시: 유효한 leaf ID로 교체 필요
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


def build_product_payload(raw: dict, ai: dict, selling_price: int, tags: list = None) -> dict:
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
            "name": (clean_product_name(ai.get("product_name") or ai.get("name") or str(raw.get("name", "")))
                     or str(raw.get("name", "상품"))[:25]),
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
                        "itemName": clean_product_name(ai.get("product_name") or str(raw.get("name", ""))) or "상세페이지 참조",
                        "modelName": clean_product_name(ai.get("product_name") or str(raw.get("name", ""))) or "상세페이지 참조",
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
                **({"searchTagInfo": {"searchTagList": tags[:10]}} if tags else {}),
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
        io = _io

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

def build_detail_html(
    banner_url: str,
    product_img_url: str,
    ai: dict,
    detail_img_url: str = "",
) -> str:
    """
    5-타입 스마트스토어 표준 상세페이지
    ① 메인 인트로 (배너) — 훅
    ② 실제 사용컷 (라이프스타일) + 감성 문구
    ③ 이런 분께 추천 체크리스트
    ④ 디테일/기능컷 + 스펙 테이블
    ⑤ 비교/인증 + 배송/공지 고정
    """
    # ① 메인 인트로 배너
    sec1 = (
        f'<img src="{banner_url}" {_IMG}>'
        f'<div {_WRAP}>'
        + (f'<h2 style="font-size:22px;color:#1a1a1a;margin:20px 0 8px;line-height:1.5;">'
           f'{ai.get("headline","")}</h2>' if ai.get("headline") else "")
        + (f'<p style="font-size:15px;line-height:1.9;color:#555;margin:0;">'
           f'{ai.get("emotional_copy","")}</p>' if ai.get("emotional_copy") else "")
        + '</div>'
    ) if banner_url else ""

    # ② 실제 사용컷 (라이프스타일) + 감성 문구 3가지
    r1, r2, r3 = ai.get("reason_1",""), ai.get("reason_2",""), ai.get("reason_3","")
    reason_blocks = "".join(
        f'<div style="background:{bg};border-radius:10px;padding:16px 18px;margin:8px 0;">'
        f'<b style="font-size:18px;color:#888;">0{i} </b>'
        f'<span style="font-size:15px;line-height:1.8;">{txt}</span></div>'
        for i, (txt, bg) in enumerate([
            (r1,"#e8f5e9"),(r2,"#fff3e0"),(r3,"#fce4ec")
        ], start=1) if txt
    )
    sec2 = (
        f'<img src="{product_img_url}" {_IMG}>'
        f'<div {_WRAP}>'
        f'<h3 style="font-size:17px;border-left:4px solid #1a73e8;padding-left:12px;margin:20px 0 14px;">'
        f'✅ 이 상품을 선택해야 하는 이유</h3>'
        + reason_blocks + '</div>'
    ) if product_img_url else ""

    # ③ 이런 분께 추천 체크리스트
    recs = [r for r in ai.get("recommend_list", []) if r][:5]
    if recs:
        items_html = "".join(
            f'<li style="padding:10px 0 10px 36px;position:relative;'
            f'border-bottom:1px solid #dde8ff;font-size:15px;line-height:1.6;">'
            f'<span style="position:absolute;left:0;top:9px;color:#1a73e8;font-size:20px;font-weight:bold;">✔</span>'
            f'{r}</li>'
            for r in recs
        )
        sec3 = (
            f'<div {_WRAP}>'
            f'<div style="background:#eef4ff;border-radius:14px;padding:24px 20px;">'
            f'<h3 style="font-size:17px;color:#1a73e8;margin:0 0 16px;text-align:center;">'
            f'💙 이런 분들께 강력 추천합니다</h3>'
            f'<ul style="list-style:none;padding:0;margin:0;">{items_html}</ul>'
            f'</div></div>'
        )
    else:
        sec3 = ""

    # ④ 디테일/기능 설명컷 + 스펙 테이블
    spec_rows = ai.get("spec_rows", [])
    spec_html = ""
    if spec_rows:
        rows_html = "".join(
            f'<tr><td style="background:#f5f7fa;padding:10px 14px;font-weight:bold;width:38%;'
            f'border:1px solid #ddd;font-size:14px;">{r[0]}</td>'
            f'<td style="padding:10px 14px;border:1px solid #ddd;font-size:14px;">'
            f'{r[1] if len(r)>1 else ""}</td></tr>'
            for r in spec_rows
        )
        spec_html = (
            f'<h3 style="font-size:17px;border-left:4px solid #1a73e8;padding-left:12px;margin:24px 0 12px;">'
            f'📋 상품 스펙</h3>'
            f'<table style="width:100%;border-collapse:collapse;">{rows_html}</table>'
        )
    sec4 = (
        (f'<img src="{detail_img_url}" {_IMG}>' if detail_img_url else "")
        + (f'<div {_WRAP}>{spec_html}</div>' if spec_html else "")
    )

    # ⑤ 비교/인증 + 배송/공지 고정
    compare = [c for c in ai.get("compare_points", []) if c]
    compare_html = ""
    if compare:
        items = "".join(f'<li style="padding:8px 0;font-size:14px;color:#444;">⭐ {c}</li>' for c in compare)
        compare_html = (
            f'<div style="background:#fffde7;border-radius:10px;padding:20px;margin:16px 0;">'
            f'<h4 style="font-size:15px;color:#f57f17;margin:0 0 10px;">🏆 타사 대비 차별점</h4>'
            f'<ul style="list-style:none;padding:0;margin:0;">{items}</ul></div>'
        )
    delivery_html = (
        '<div style="background:#f5f5f5;border-top:2px solid #ddd;padding:20px 16px;margin-top:24px;'
        'font-size:13px;color:#666;line-height:1.8;">'
        '<b style="color:#333;">📦 배송 안내</b><br>'
        '· 주문 후 1~3일 이내 출고 (주말·공휴일 제외)<br>'
        '· 도서산간 지역 추가 배송비 발생 가능<br><br>'
        '<b style="color:#333;">🔄 교환/반품 안내</b><br>'
        '· 수령 후 7일 이내 교환/반품 가능<br>'
        '· 단순 변심 반품 시 왕복 배송비 고객 부담<br>'
        '· 불량/오배송 시 무료 교환/반품</div>'
    )
    sec5 = f'<div {_WRAP}>{compare_html}</div>' + delivery_html

    html = sec1 + sec2 + sec3 + sec4 + sec5

    # ⑦ 모든 섹션이 비어있으면 최소 폴백 HTML 강제 생성
    if not html.strip():
        name = ai.get("product_name", "상품")
        html = (
            f'<div {_WRAP}>'
            f'<h2 style="font-size:20px;color:#1a1a1a;margin:24px 0 12px;">{name}</h2>'
            f'<p style="font-size:15px;line-height:1.9;color:#555;margin:0 0 20px;">'
            f'고객님의 일상을 더욱 편리하고 풍요롭게 만들어 드리는 상품입니다.</p>'
            f'<ul style="font-size:15px;line-height:2;color:#444;padding-left:20px;">'
            f'<li>우수한 품질로 오랫동안 사용 가능합니다</li>'
            f'<li>빠른 배송으로 신속하게 받아보실 수 있습니다</li>'
            f'<li>상품에 대한 문의는 스토어 문의하기를 이용해 주세요</li>'
            f'</ul></div>'
        )
    return html


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


# ─── 네이버 쇼핑 검색 — 경쟁사 가격/키워드 수집 ──────────────────────────────
async def search_naver_shopping(query: str, display: int = 10) -> list:
    """네이버 쇼핑 검색 API로 경쟁사 상위 상품 가격·키워드 수집"""
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://openapi.naver.com/v1/search/shop.json",
                headers={
                    "X-Naver-Client-Id": NAVER_CLIENT_ID,
                    "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
                },
                params={"query": query, "display": display, "sort": "sim"},
            )
            r.raise_for_status()
        items = r.json().get("items", [])
        results = []
        for item in items:
            price = int(item.get("lprice", 0))
            if price > 0:
                results.append({
                    "title": re.sub(r"<[^>]+>", "", item.get("title", "")),
                    "price": price,
                    "mall": item.get("mallName", ""),
                })
        print(f"[쇼핑검색] '{query[:15]}' → {len(results)}개 경쟁 상품 수집", flush=True)
        return results
    except Exception as e:
        print(f"[쇼핑검색] 실패: {e}", flush=True)
        return []


# ─── DALL-E 3 공통 스타일 접미사 ─────────────────────────────────────────────
# ─── 브랜드 글로벌 비주얼 가이드라인 (모든 이미지 공통 적용) ─────────────────
_DALLE_SUFFIX = (
    "GLOBAL VISUAL STYLE: photorealistic 8K, natural soft lighting, "
    "clean white-themed minimal aesthetic, warm and premium atmosphere, "
    "no text, no watermark, no people, "
    "professional Korean smart store product photography."
)

# ─── 3단계 통합 QC 파이프라인 ────────────────────────────────────────────────
import html as _html_parser

async def run_qc_pipeline(
    image_url: str,
    product_name: str,
    detail_html: str,
    anthropic_key: str,
    reject_keywords: list = None,
) -> dict:
    """
    1단계: 기술 검수 — 해상도·용량
    2단계: 비전 검수 — 상품명↔이미지 일치
    3단계: 콘텐츠 검수 — HTML 핵심 정보 3줄 이상

    반환: {passed, stage, reason, score}
    """
    # ── 1단계: 기술 검수 ──────────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(image_url)
            r.raise_for_status()
        img_bytes = r.content
        from PIL import Image as _PILImg
        img = _PILImg.open(_io.BytesIO(img_bytes))
        w, h = img.size
        size_kb = len(img_bytes) / 1024

        if min(w, h) < 800:
            return {"passed": False, "stage": 1,
                    "reason": f"해상도 미달 ({w}×{h}, 최소 800px 필요)",
                    "score": 0}
        if size_kb < 5:
            return {"passed": False, "stage": 1,
                    "reason": f"파일 용량 너무 작음 ({size_kb:.1f}KB)",
                    "score": 0}
        if size_kb > 10240:
            return {"passed": False, "stage": 1,
                    "reason": f"파일 용량 초과 ({size_kb:.0f}KB, 최대 10MB)",
                    "score": 0}
        print(f"[QC-1] ✅ 기술검수 통과 ({w}×{h}, {size_kb:.0f}KB)", flush=True)
    except Exception as e:
        print(f"[QC-1] ⚠️ 기술검수 스킵: {e}", flush=True)

    # ── 2단계: 비전 검수 ──────────────────────────────────────────────────────
    from employees import employee_image_inspector
    qc = await employee_image_inspector(
        image_url, product_name, anthropic_key,
        reject_keywords=reject_keywords
    )
    score = qc.get("score", 100)
    if score < 50:
        print(f"[QC-2] ⚠️ 비전검수 경고 {score}점 — 진행 허용", flush=True)
    else:
        print(f"[QC-2] ✅ 비전검수 통과 ({score}점)", flush=True)

    # ── 3단계: 콘텐츠 검수 ───────────────────────────────────────────────────
    clean_text = _re.sub(r'<[^>]+>', ' ', detail_html)
    clean_text = _re.sub(r'\s+', ' ', clean_text).strip()
    lines = [l.strip() for l in clean_text.split('·') + clean_text.split('•')
             if len(l.strip()) > 10]
    if len(clean_text) < 100:
        return {"passed": False, "stage": 3,
                "reason": f"상세페이지 내용 부족 ({len(clean_text)}자, 최소 100자)",
                "score": score}
    print(f"[QC-3] ✅ 콘텐츠검수 통과 ({len(clean_text)}자)", flush=True)

    return {"passed": True, "stage": 3, "reason": "전체 통과", "score": score}


async def build_dalle_prompt_smart(
    product_name: str,
    category: str = "",
    shot_type: str = "lifestyle",
    spec_hint: str = "",
) -> str:
    """
    지능형 프롬프트 생성기
    - _SCENE_MAP으로 1차 분류
    - 미분류 시 Claude가 씬 자동 생성
    - shot_type: lifestyle | detail | banner
    """
    scene, _ = _get_scene_context(product_name)

    # DALL-E용: AI 키워드 번역 (Haiku) → 정적 맵 폴백
    try:
        from employees import employee_keyword_translator
        en_name = await employee_keyword_translator(product_name, category, ANTHROPIC_API_KEY)
    except Exception:
        en_name = _get_en_name(product_name, category)

    # 미분류(기본 씬) + 카테고리 있으면 Claude로 씬 보강
    is_default = scene == _DEFAULT_SCENE[0]
    if is_default and ANTHROPIC_API_KEY and category:
        try:
            client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
            resp = await client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=100,
                messages=[{"role": "user", "content":
                    f"상품명: '{product_name}', 카테고리: '{category}'\n"
                    f"이 상품을 실제로 사용하는 구체적인 배경 장면을 영어 20단어 이내로 묘사해. "
                    f"장소/분위기만 설명하고 상품명은 포함하지 마."}]
            )
            scene = resp.content[0].text.strip()
            print(f"[PROMPT] Claude 씬 생성: {scene[:40]}", flush=True)
        except Exception:
            pass

    if shot_type == "detail":
        highlight = spec_hint if spec_hint else "material texture and fine craftsmanship"
        return (
            f"Extreme close-up macro product photography of '{en_name}'. "
            f"Highlight: {highlight}. "
            f"Pure white background, overhead flat lay, ultra-sharp studio lighting. "
            f"{_DALLE_SUFFIX}"
        )
    elif shot_type == "banner":
        return (
            f"Wide Korean e-commerce banner image. "
            f"RIGHT 40%: '{en_name}' {scene}. "
            f"LEFT 60%: large plain off-white empty negative space for text overlay. "
            f"Clean professional composition. No text, no watermark. {_DALLE_SUFFIX}"
        )
    else:  # lifestyle
        return (
            f"Photorealistic 8K lifestyle product photography. "
            f"'{en_name}' {scene}. "
            f"The product is the clear main subject, beautifully lit. "
            f"No people, no text, no watermark. {_DALLE_SUFFIX}"
        )


async def _dalle_request(prompt: str, size: str = "1024x1024", quality: str = "hd") -> str | None:
    """DALL-E 3 공통 호출"""
    if not OPENAI_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=90) as c:
            r = await c.post(
                "https://api.openai.com/v1/images/generations",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": "dall-e-3", "prompt": prompt, "n": 1,
                      "size": size, "quality": quality}
            )
            r.raise_for_status()
            return r.json()["data"][0]["url"]
    except Exception as e:
        print(f"[DALLE] 실패: {e}", flush=True)
        return None


# ─── 카테고리별 DALL-E 영어 제품명 맵 ───────────────────────────────────────
_CATEGORY_EN_MAP = {
    "생활/건강":    "health and lifestyle product",
    "패션잡화":     "fashion accessory",
    "스포츠/레저":  "sports and leisure item",
    "가구/인테리어": "home interior furniture item",
    "패션의류":     "fashion clothing apparel",
    "디지털/가전":  "electronic digital device",
    "화장품/미용":  "beauty cosmetic skincare product",
    "식품":        "food and health food item",
    "출산/육아":    "baby and parenting item",
    "자동차용품":   "car interior automotive accessory",
    "주방용품":     "kitchen cooking utensil",
    "반려동물":     "pet accessory",
    "운동/스포츠":  "fitness sports equipment",
    "청소용품":     "cleaning household tool",
}

# 상품명 키워드 → 영어 토큰 (삽입 순서 = 조합 순서)
# 긴 키워드를 먼저 배치해 부분 매칭 오류 방지
_PRODUCT_KEYWORD_MAP = {
    # ── 자동차/차량 ───────────────────────────────────────────────────────────
    "차량용":        ["automotive", "car"],
    "자동차":        ["automotive", "car"],
    "카시트":        ["car", "seat"],
    "통풍시트":      ["ventilated", "car", "seat", "cushion"],
    "시트커버":      ["car", "seat", "cover"],
    "카매트":        ["car", "floor", "mat"],
    "핸들커버":      ["steering", "wheel", "cover"],
    "차량용품":      ["automotive", "car", "accessory"],

    # ── 디지털/가전: 생활 가전 ────────────────────────────────────────────────
    "에어프라이어":  ["air", "fryer", "kitchen", "appliance"],
    "에어프라이":    ["air", "fryer", "kitchen", "appliance"],
    "전자레인지":    ["microwave", "oven", "kitchen", "appliance"],
    "냉장고":        ["refrigerator", "home", "appliance"],
    "세탁기":        ["washing", "machine", "home", "appliance"],
    "식기세척기":    ["dishwasher", "kitchen", "appliance"],
    "공기청정기":    ["air", "purifier", "home", "appliance"],
    "제습기":        ["dehumidifier", "home", "appliance"],
    "가습기":        ["humidifier", "home", "appliance"],
    "선풍기":        ["electric", "fan", "appliance"],
    "서큘레이터":    ["air", "circulator", "fan", "appliance"],
    "에어컨":        ["air", "conditioner", "appliance"],
    "청소기":        ["vacuum", "cleaner", "appliance"],
    "스팀청소기":    ["steam", "cleaner", "appliance"],
    "로봇청소기":    ["robot", "vacuum", "cleaner"],
    "정수기":        ["water", "purifier", "dispenser"],
    "전기포트":      ["electric", "kettle", "appliance"],
    "전기밥솥":      ["electric", "rice", "cooker"],
    "인덕션":        ["induction", "cooktop", "kitchen"],
    "토스터":        ["toaster", "kitchen", "appliance"],
    "블렌더":        ["blender", "mixer", "kitchen"],
    "커피머신":      ["coffee", "machine", "maker"],

    # ── 디지털: IT/모바일 ─────────────────────────────────────────────────────
    "노트북":        ["laptop", "computer", "device"],
    "태블릿":        ["tablet", "computer", "device"],
    "스마트폰":      ["smartphone", "mobile", "device"],
    "핸드폰":        ["smartphone", "mobile", "device"],
    "스마트워치":    ["smartwatch", "wearable", "device"],
    "이어폰":        ["earphone", "audio", "device"],
    "헤드폰":        ["headphone", "audio", "device"],
    "헤드셋":        ["headset", "audio", "device"],
    "블루투스이어폰":["bluetooth", "earphone", "wireless"],
    "무선이어폰":    ["wireless", "earphone", "audio"],
    "스피커":        ["bluetooth", "speaker", "audio", "device"],
    "충전기":        ["USB", "charger", "cable", "device"],
    "보조배터리":    ["portable", "battery", "power", "bank"],
    "케이블":        ["charging", "cable", "USB", "connector"],
    "허브":          ["USB", "hub", "adapter", "device"],
    "마우스":        ["mouse", "computer", "accessory"],
    "키보드":        ["keyboard", "computer", "accessory"],
    "모니터":        ["monitor", "display", "screen"],
    "웹캠":          ["webcam", "camera", "device"],
    "카메라":        ["digital", "camera", "device"],
    "액션캠":        ["action", "camera", "sports"],
    "드론":          ["drone", "aerial", "camera"],
    "프린터":        ["printer", "office", "device"],
    "블루투스":      ["bluetooth", "wireless", "device"],

    # ── 안마/마사지 ───────────────────────────────────────────────────────────
    "안마기":        ["neck", "shoulder", "massager", "device"],
    "안마의자":      ["massage", "chair", "recliner"],
    "마사지기":      ["electric", "massager", "device", "machine"],
    "마사지건":      ["massage", "gun", "percussion", "device"],
    "마사지":        ["massage", "device"],
    "찜질기":        ["heating", "pad", "therapy", "device"],
    "온열매트":      ["heated", "mat", "electric", "blanket"],

    # ── 수면/침구 ────────────────────────────────────────────────────────────
    "베개":          ["pillow", "sleep"],
    "메모리폼":      ["memory", "foam", "pillow"],
    "이불":          ["blanket", "comforter", "bedding"],
    "매트리스":      ["mattress", "bed", "sleep"],
    "수면안대":      ["sleep", "eye", "mask"],

    # ── 음료/보온 ────────────────────────────────────────────────────────────
    "텀블러":        ["tumbler", "insulated", "bottle"],
    "보온병":        ["insulated", "thermos", "bottle"],
    "물병":          ["water", "bottle"],
    "머그컵":        ["mug", "cup", "drinkware"],

    # ── 주방용품 ────────────────────────────────────────────────────────────
    "냄비":          ["cooking", "pot", "kitchen"],
    "프라이팬":      ["frying", "pan", "cookware"],
    "도마":          ["cutting", "board", "kitchen"],
    "칼":            ["kitchen", "knife", "cooking"],
    "밀폐용기":      ["food", "storage", "container"],
    "냄비받침":      ["pot", "holder", "kitchen"],
    "수납함":        ["storage", "organizer", "box"],
    "쓰레기통":      ["trash", "bin", "waste", "basket"],

    # ── 패션의류 ────────────────────────────────────────────────────────────
    "티셔츠":        ["t-shirt", "clothing", "apparel"],
    "후드티":        ["hoodie", "sweatshirt", "clothing"],
    "맨투맨":        ["sweatshirt", "clothing", "apparel"],
    "니트":          ["knit", "sweater", "clothing"],
    "코트":          ["coat", "outerwear", "clothing"],
    "패딩":          ["padded", "jacket", "winter", "clothing"],
    "청바지":        ["jeans", "denim", "pants"],
    "슬랙스":        ["slacks", "dress", "pants"],
    "원피스":        ["dress", "clothing", "apparel"],
    "블라우스":      ["blouse", "top", "clothing"],
    "레깅스":        ["leggings", "activewear"],
    "수영복":        ["swimsuit", "swimwear"],

    # ── 패션잡화/신발 ────────────────────────────────────────────────────────
    "운동화":        ["sneakers", "shoes", "footwear"],
    "구두":          ["dress", "shoes", "leather", "footwear"],
    "슬리퍼":        ["slippers", "sandals", "footwear"],
    "부츠":          ["boots", "footwear"],
    "백팩":          ["backpack", "bag", "accessory"],
    "가방":          ["bag", "handbag", "accessory"],
    "숄더백":        ["shoulder", "bag", "accessory"],
    "크로스백":      ["crossbody", "bag", "accessory"],
    "지갑":          ["wallet", "leather", "accessory"],
    "카드지갑":      ["card", "wallet", "accessory"],
    "모자":          ["hat", "cap", "accessory"],
    "선글라스":      ["sunglasses", "eyewear", "accessory"],
    "벨트":          ["belt", "leather", "accessory"],
    "스카프":        ["scarf", "shawl", "accessory"],

    # ── 화장품/미용 ──────────────────────────────────────────────────────────
    "선크림":        ["sunscreen", "SPF", "skincare"],
    "선블록":        ["sunscreen", "SPF", "skincare"],
    "마스크팩":      ["face", "mask", "sheet", "skincare"],
    "세럼":          ["serum", "essence", "skincare"],
    "크림":          ["moisturizer", "cream", "skincare"],
    "토너":          ["toner", "skincare", "lotion"],
    "폼클렌저":      ["foam", "cleanser", "skincare"],
    "샴푸":          ["shampoo", "hair", "care"],
    "린스":          ["conditioner", "hair", "care"],
    "트리트먼트":    ["hair", "treatment", "mask"],
    "드라이기":      ["hair", "dryer", "styling"],
    "고데기":        ["hair", "straightener", "curler"],
    "립스틱":        ["lipstick", "lip", "makeup"],
    "파운데이션":    ["foundation", "makeup", "cosmetic"],
    "아이섀도":      ["eyeshadow", "eye", "makeup"],
    "향수":          ["perfume", "fragrance"],
    "바디워시":      ["body", "wash", "shower", "gel"],
    "핸드크림":      ["hand", "cream", "lotion"],

    # ── 건강/의료 ────────────────────────────────────────────────────────────
    "혈압계":        ["blood", "pressure", "monitor"],
    "체온계":        ["thermometer", "health", "device"],
    "체중계":        ["weight", "scale", "body"],
    "보조기":        ["support", "brace", "orthopedic"],
    "무릎보호대":    ["knee", "support", "brace"],
    "허리보호대":    ["back", "lumbar", "support", "brace"],

    # ── 출산/육아 ────────────────────────────────────────────────────────────
    "유아용품":      ["baby", "infant", "product"],
    "아기":          ["baby", "infant"],
    "기저귀":        ["diaper", "baby"],
    "젖병":          ["baby", "bottle", "feeding"],
    "유모차":        ["baby", "stroller", "pram"],
    "아기띠":        ["baby", "carrier", "wrap"],
    "장난감":        ["toy", "children", "play"],
    "블록":          ["building", "blocks", "toy"],
    "인형":          ["doll", "stuffed", "toy"],

    # ── 반려동물 ────────────────────────────────────────────────────────────
    "강아지":        ["dog", "pet", "canine"],
    "고양이":        ["cat", "pet", "feline"],
    "펫":            ["pet", "animal", "accessory"],
    "반려동물":      ["pet", "animal", "care"],
    "사료":          ["pet", "food", "nutrition"],
    "리드줄":        ["dog", "leash", "collar"],
    "캣타워":        ["cat", "tree", "tower"],

    # ── 스포츠/레저 ──────────────────────────────────────────────────────────
    "덤벨":          ["dumbbell", "weight", "fitness"],
    "아령":          ["dumbbell", "weight", "fitness"],
    "바벨":          ["barbell", "weight", "gym"],
    "요가":          ["yoga", "mat", "fitness"],
    "필라테스":      ["pilates", "fitness", "mat"],
    "헬스":          ["fitness", "gym", "equipment"],
    "러닝화":        ["running", "shoes", "athletic"],
    "자전거":        ["bicycle", "bike", "cycling"],
    "수영":          ["swimming", "water", "sports"],
    "등산":          ["hiking", "outdoor", "mountain"],
    "텐트":          ["camping", "tent", "outdoor"],
    "캠핑":          ["camping", "outdoor", "gear"],
    "낚시":          ["fishing", "rod", "outdoor"],
    "골프":          ["golf", "club", "sports"],

    # ── 가구/인테리어 ────────────────────────────────────────────────────────
    "소파":          ["sofa", "couch", "furniture"],
    "의자":          ["chair", "furniture", "seating"],
    "책상":          ["desk", "table", "furniture"],
    "침대":          ["bed", "frame", "furniture"],
    "수납장":        ["cabinet", "storage", "furniture"],
    "선반":          ["shelf", "rack", "storage"],
    "커튼":          ["curtain", "window", "decor"],
    "러그":          ["rug", "carpet", "floor", "decor"],
    "조명":          ["lamp", "light", "lighting"],
    "화분":          ["flower", "pot", "plant"],

    # ── 식품 ────────────────────────────────────────────────────────────────
    "홍삼":          ["red", "ginseng", "health", "supplement"],
    "비타민":        ["vitamin", "supplement", "health"],
    "단백질":        ["protein", "supplement", "nutrition"],
    "견과류":        ["nuts", "dried", "fruit", "snack"],
    "커피":          ["coffee", "drink", "beverage"],
    "차":            ["tea", "herbal", "beverage"],

    # ── 시트/쿠션 (자동차 외) ────────────────────────────────────────────────
    "쿠션":          ["cushion", "pad", "comfort"],
    "방석":          ["seat", "cushion", "pad"],
}


def _get_en_name(product_name: str, category: str) -> str:
    """상품명 키워드 조합 우선, 없으면 카테고리 폴백"""
    tokens: list[str] = []
    seen: set[str] = set()
    for kor, parts in _PRODUCT_KEYWORD_MAP.items():
        if kor in product_name:
            for p in parts:
                if p not in seen:
                    tokens.append(p)
                    seen.add(p)
    if tokens:
        return " ".join(tokens)
    return _CATEGORY_EN_MAP.get(category, "lifestyle product")


# ─── 키워드 기반 씬 컨텍스트 맵 ──────────────────────────────────────────────
_SCENE_MAP = [
    # (한국어 키워드 리스트, 씬 설명, 거부 키워드)
    (["차량용","자동차","카시트","시트커버","통풍시트","차량","카매트"],
     "installed neatly on a luxurious sedan leather car seat inside a premium vehicle interior, "
     "soft sunlight streaming through car window, cozy and stylish atmosphere",
     ["chair wheels","office chair","desk chair","casters","5-leg base"]),

    (["주방","조리","요리","냄비","프라이팬","도마","칼","그릇","수저"],
     "placed on a clean marble kitchen counter with soft natural morning light, "
     "minimal Scandinavian kitchen background",
     ["office","bedroom","car"]),

    (["침실","베개","이불","매트리스","침대","수면"],
     "displayed on a neatly made white linen bed in a bright minimalist bedroom, "
     "soft morning light through sheer curtains",
     ["kitchen","office","car"]),

    (["욕실","목욕","샤워","비누","샴푸","바디"],
     "arranged on a clean white bathroom shelf with soft warm lighting, "
     "fresh and clean spa-like atmosphere",
     ["kitchen","bedroom","car"]),

    (["캠핑","아웃도어","등산","텐트","트레킹"],
     "set up in a beautiful outdoor camping scene with lush greenery and golden hour light",
     ["office","bedroom","kitchen"]),

    (["유아","아기","베이비","신생아","어린이"],
     "placed in a bright cheerful nursery room with soft pastel tones and gentle natural light",
     ["office","car","kitchen"]),

    (["반려동물","강아지","고양이","펫","애견"],
     "shown with an adorable pet in a cozy warm home setting with soft natural light",
     ["office","car"]),

    (["운동","헬스","스포츠","요가","필라테스","피트니스"],
     "displayed in a clean modern gym or yoga studio with bright motivating atmosphere",
     ["office","bedroom","kitchen"]),

    (["청소","세탁","빨래","걸레","청소기"],
     "shown in a spotlessly clean bright home interior, demonstrating ease of cleaning",
     ["office","car"]),

    (["사무","문구","노트","펜","책상","오피스"],
     "neatly arranged on a clean organized desk in a bright minimalist home office",
     ["bedroom","kitchen","car"]),
]

_DEFAULT_SCENE = (
    "placed on a warm white surface with soft morning light, "
    "minimal Nordic home interior background, cozy lifestyle setting",
    []
)


def _get_scene_context(product_name: str) -> tuple[str, list[str]]:
    """상품명 키워드 분석 → (씬 설명, 거부 키워드 리스트)"""
    for keywords, scene, reject in _SCENE_MAP:
        if any(kw in product_name for kw in keywords):
            return scene, reject
    return _DEFAULT_SCENE


async def generate_dalle_image(product_name: str, category: str = "") -> str | None:
    """② 라이프스타일 상품컷 — 지능형 씬 자동 주입"""
    prompt = await build_dalle_prompt_smart(product_name, category, shot_type="lifestyle")
    print(f"[DALLE] 상품컷: {product_name[:20]}", flush=True)
    return await _dalle_request(prompt, size="1024x1024", quality="hd")


async def generate_dalle_banner(product_name: str, headline: str = "", category: str = "") -> str | None:
    """① 메인 배너 — 지능형 씬 자동 주입 + 좌측 여백"""
    prompt = await build_dalle_prompt_smart(product_name, category, shot_type="banner")
    print(f"[DALLE] 배너: {product_name[:20]}", flush=True)
    return await _dalle_request(prompt, size="1792x1024", quality="hd")


async def generate_dalle_detail_shot(product_name: str, spec_hint: str = "", category: str = "") -> str | None:
    """④ 디테일/기능 설명컷 — 클로즈업, spec_hint 반영"""
    prompt = await build_dalle_prompt_smart(product_name, category, shot_type="detail", spec_hint=spec_hint)
    print(f"[DALLE] 디테일컷: {product_name[:20]}", flush=True)
    return await _dalle_request(prompt, size="1024x1024", quality="hd")


async def generate_gemini_image(product_name: str, category: str = "") -> bytes | None:
    """Gemini 2.0 Flash 이미지 생성 — raw bytes 반환"""
    if not GOOGLE_AI_API_KEY:
        return None
    en_name = _get_en_name(product_name, category)
    scene, _ = _get_scene_context(product_name)
    prompt = (
        f"Photorealistic product photo of '{en_name}'. "
        f"{scene}. "
        f"Natural lighting, real photo style, DSLR camera quality. "
        f"NOT AI generated looking, NOT cartoon, NOT synthetic. "
        f"Shot like a real photographer: sharp focus, natural shadows, "
        f"authentic textures, clean minimal background. "
        f"No text, no watermarks, no people."
    )
    try:
        import base64 as _b64
        # gemini-2.0-flash-exp: 이미지 생성 응답 시도
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent",
                params={"key": GOOGLE_AI_API_KEY},
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"responseModalities": ["TEXT", "IMAGE"]},
                },
            )
        if r.status_code == 200:
            for part in r.json().get("candidates", [{}])[0].get("content", {}).get("parts", []):
                if "inlineData" in part:
                    print(f"[GEMINI] ✅ {product_name[:20]}", flush=True)
                    return _b64.b64decode(part["inlineData"]["data"])
        print(f"[GEMINI] HTTP{r.status_code} — 이미지 생성 미지원, 스킵", flush=True)
    except Exception as e:
        print(f"[GEMINI] 실패: {e}", flush=True)
    return None


async def generate_flux_image(product_name: str, category: str = "") -> str | None:
    """Flux Pro 1.1 이미지 생성 — BFL API 비동기 폴링, 이미지 URL 반환"""
    if not FLUX_API_KEY:
        return None
    en_name = _get_en_name(product_name, category)
    scene, _ = _get_scene_context(product_name)
    prompt = (
        f"Photorealistic professional product photography of {en_name}. "
        f"{scene}. "
        f"Shot with DSLR camera, natural lighting, real photo style. "
        f"NOT AI generated, NOT cartoon, NOT synthetic. "
        f"Authentic textures, sharp focus, clean minimal background. "
        f"No text, no watermarks, no people."
    )
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(
                "https://api.bfl.ai/v1/flux-pro-1.1",
                headers={"X-Key": FLUX_API_KEY, "Content-Type": "application/json"},
                json={"prompt": prompt, "width": 1024, "height": 1024},
            )
            r.raise_for_status()
            resp_json = r.json()
            task_id = resp_json.get("id")
            # 응답에 polling_url이 있으면 그것을 사용 (리전 자동 처리)
            polling_url = resp_json.get("polling_url") or f"https://api.bfl.ai/v1/get_result?id={task_id}"
        if not task_id:
            return None
        # 결과 폴링 (최대 60초, 2초 간격)
        for _ in range(30):
            await asyncio.sleep(2)
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(
                    polling_url,
                    headers={"X-Key": FLUX_API_KEY},
                )
                data = r.json()
            status = data.get("status")
            if status == "Ready":
                img_url = data.get("result", {}).get("sample")
                if img_url:
                    print(f"[FLUX] ✅ {product_name[:20]}", flush=True)
                    return img_url
                return None
            if status not in ("Pending", "Processing"):
                print(f"[FLUX] 실패 상태: {status}", flush=True)
                return None
        print(f"[FLUX] 타임아웃 (60s)", flush=True)
    except Exception as e:
        print(f"[FLUX] 실패: {e}", flush=True)
    return None


async def generate_flux_bg_edit(
    image_url: str, product_name: str, category: str = ""
) -> str | None:
    """Flux Kontext — 원본 제품 형태 유지, 배경만 라이프스타일로 교체"""
    if not FLUX_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.get(_extract_hq_url(image_url))
            r.raise_for_status()
        import base64 as _b64
        img_b64 = _b64.b64encode(r.content).decode()
    except Exception as e:
        print(f"[FLUX_BG] 원본 다운로드 실패: {e}", flush=True)
        return None

    scene, _ = _get_scene_context(product_name)
    prompt = (
        f"Keep the product object exactly as-is — same shape, color, texture, and all details. "
        f"Replace ONLY the background with a lifestyle scene: {scene}. "
        f"The product stays centered and unchanged. "
        f"Natural lighting, photorealistic, high quality. No text, no watermarks."
    )
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(
                "https://api.bfl.ai/v1/flux-kontext-pro",
                headers={"X-Key": FLUX_API_KEY, "Content-Type": "application/json"},
                json={"prompt": prompt, "input_image": img_b64,
                      "width": 1024, "height": 1024},
            )
            r.raise_for_status()
            resp_json = r.json()
            task_id = resp_json.get("id")
            polling_url = resp_json.get("polling_url") or f"https://api.bfl.ai/v1/get_result?id={task_id}"
        if not task_id:
            return None
        for _ in range(30):
            await asyncio.sleep(2)
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(polling_url, headers={"X-Key": FLUX_API_KEY})
                data = r.json()
            status = data.get("status")
            if status == "Ready":
                img_url = data.get("result", {}).get("sample")
                if img_url:
                    print(f"[FLUX_BG] ✅ {product_name[:20]}", flush=True)
                    return img_url
                return None
            if status not in ("Pending", "Processing"):
                print(f"[FLUX_BG] 실패 상태: {status}", flush=True)
                return None
        print(f"[FLUX_BG] 타임아웃", flush=True)
    except Exception as e:
        print(f"[FLUX_BG] 실패: {e}", flush=True)
    return None


# 디지털/가전 카테고리 키워드
_DIGITAL_CATEGORIES = ("디지털", "가전", "전자", "컴퓨터", "모바일", "IT")


async def _is_digital_category(category: str) -> bool:
    return any(kw in category for kw in _DIGITAL_CATEGORIES)


async def _generate_ai_image_with_qc(
    product_name: str, category: str, reject_kws: list,
    use_flux_first: bool = False,
) -> str | None:
    """이미지 생성 후 Vision QC — URL 반환
    use_flux_first=True (Pexels QC < 60): Flux → Gemini
    use_flux_first=False (Pexels QC >= 60): Gemini only
    """
    from employees import employee_image_inspector
    sources = (
        [("Flux", generate_flux_image), ("Gemini", generate_gemini_image)]
        if use_flux_first
        else [("Gemini", generate_gemini_image)]
    )
    for label, gen_fn in sources:
        print(f"[IMAGE] {label} 생성 중: {product_name[:20]}", flush=True)
        raw = await gen_fn(product_name, category)
        if not raw:
            print(f"[IMAGE] {label} 생성 실패", flush=True)
            continue
        try:
            url = (
                await naver_api.upload_raw_image(raw)  # Gemini → bytes
                if isinstance(raw, bytes)
                else await naver_api.upload_image(raw)  # Flux → URL 문자열
            )
            qc = await employee_image_inspector(
                url, product_name, ANTHROPIC_API_KEY, reject_keywords=reject_kws
            )
            print(f"[IMAGE] {label} QC: {qc.get('score',0)}점 — {qc.get('recommendation','')}", flush=True)
            if qc.get("passed", False):
                print(f"[IMAGE] {label} ✅", flush=True)
                return url
            print(f"[IMAGE] {label} QC 미통과 → 다음 소스", flush=True)
        except Exception as e:
            print(f"[IMAGE] {label} 업로드/QC 실패: {e}", flush=True)
    return None


async def _is_text_heavy_image(image_url: str) -> bool:
    """배송/반품 안내 등 텍스트 과다 이미지 감지 — 세로가 가로의 2.5배 초과 시 제외"""
    try:
        from PIL import Image
        io = _io
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(image_url)
            r.raise_for_status()
        img = Image.open(io.BytesIO(r.content))
        w, h = img.size
        return h > w * 2.5
    except Exception:
        return False


async def _check_image_quality(image_url: str) -> tuple[bool, str, int, int]:
    """
    이미지 품질 체크 — (사용가능여부, 사유, 가로, 세로)
    사유: "ok" | "too_small" | "text_heavy" | "error"
    """
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(_extract_hq_url(image_url))
            r.raise_for_status()
        from PIL import Image
        img = Image.open(_io.BytesIO(r.content))
        w, h = img.size
        if h > w * 2.5:
            return False, "text_heavy", w, h
        if min(w, h) < 300:     # 300px 미만만 거부 (너무 작은 것만)
            return False, "too_small", w, h
        return True, "ok", w, h
    except Exception as e:
        return True, "error", 0, 0  # 체크 실패 시 원본 그대로 사용


async def get_product_image(p: dict) -> str | None:
    """
    이미지 우선순위:
    1. 오너클랜 원본 + Flux Kontext 배경 교체 (실패 시 원본 그대로)
    2. Pexels 실사진 (QC 통과)
    3. AI 생성 + Vision QC (Pexels < 60: Flux → Gemini / 이상: Gemini)
    4. DALL-E 3 최종 폴백
    """
    image_url = str(p.get("image", "")).strip()
    product_name = str(p.get("name", ""))
    category = str(p.get("category", ""))

    # 1. 오너클랜 원본 — Flux Kontext로 배경 교체 → 실패 시 원본 그대로 사용
    if image_url.startswith("http"):
        ok, reason, w, h = await _check_image_quality(image_url)
        if ok:
            print(f"[IMAGE] Flux 배경 교체 시도: {product_name[:20]}", flush=True)
            edited_url = await generate_flux_bg_edit(image_url, product_name, category)
            if edited_url:
                try:
                    result = await naver_api.upload_image(edited_url)
                    print(f"[IMAGE] Flux 배경교체 ✅", flush=True)
                    return result
                except Exception as e:
                    print(f"[IMAGE] Flux 배경교체 업로드 실패: {e}", flush=True)
            # 배경 교체 실패 → 원본 폴백
            try:
                result = await naver_api.upload_image(image_url)
                print(f"[IMAGE] 오너클랜 원본 ✅ {w}×{h}", flush=True)
                return result
            except Exception as e:
                print(f"[IMAGE] 오너클랜 업로드 실패: {e}", flush=True)
        else:
            print(f"[IMAGE] 오너클랜 품질 불량({reason} {w}×{h})", flush=True)

    # 2. Pexels 실사진 + 연관성 QC
    pexels_score = 0
    print(f"[IMAGE] Pexels 검색 중: {product_name[:20]}", flush=True)
    pexels_url = await search_pexels_image(product_name)
    if pexels_url:
        try:
            from employees import employee_pexels_qc
            qc = await employee_pexels_qc(pexels_url, product_name, ANTHROPIC_API_KEY)
            pexels_score = qc.get("score", 0)
            print(f"[IMAGE] Pexels QC: {pexels_score}점 — {qc.get('reason','')}", flush=True)
            if qc.get("relevant", True):
                result = await naver_api.upload_image(pexels_url)
                print(f"[IMAGE] Pexels ✅", flush=True)
                return result
            next_src = "Flux" if pexels_score < 60 else "Gemini"
            print(f"[IMAGE] Pexels {pexels_score}점 미통과 → {next_src}", flush=True)
        except Exception as e:
            print(f"[IMAGE] Pexels 업로드 실패: {e}", flush=True)
    else:
        print(f"[IMAGE] Pexels 검색 실패 → Flux", flush=True)

    # 3. AI 생성 — Pexels QC 점수 기반 선택
    #    score < 60 (미검색 포함) → Flux 우선 → Gemini 폴백
    #    score >= 60              → Gemini 바로
    _, reject_kws = _get_scene_context(product_name)
    use_flux = pexels_score < 60
    ai_result = await _generate_ai_image_with_qc(product_name, category, reject_kws, use_flux_first=use_flux)
    if ai_result:
        return ai_result

    # 4. DALL-E 3 폴백
    print(f"[IMAGE] DALL-E 생성 중: {product_name[:20]}", flush=True)
    dalle_url = await generate_dalle_image(product_name)
    if dalle_url:
        try:
            result = await naver_api.upload_image(dalle_url)
            print(f"[IMAGE] DALL-E ✅", flush=True)
            return result
        except Exception as e:
            print(f"[IMAGE] DALL-E 업로드 실패: {e}", flush=True)

    # 5. AI 최종 시도 — QC 없이 (DALL-E 빌링 한도 등 완전 실패 시 보험)
    print(f"[IMAGE] AI 최종 시도(QC 스킵): {product_name[:20]}", flush=True)
    last_fns = (
        [generate_flux_image, generate_gemini_image] if use_flux
        else [generate_gemini_image, generate_flux_image]
    )
    for gen_fn in last_fns:
        raw = await gen_fn(product_name, category)
        if raw:
            try:
                url = (
                    await naver_api.upload_raw_image(raw)
                    if isinstance(raw, bytes)
                    else await naver_api.upload_image(raw)
                )
                print(f"[IMAGE] AI 최종 ✅ (QC 없음)", flush=True)
                return url
            except Exception as e:
                print(f"[IMAGE] AI 최종 업로드 실패: {e}", flush=True)

    print(f"[IMAGE] ❌ 모든 소스 실패: {product_name[:20]}", flush=True)
    return None


# ─── 파이프라인 ───────────────────────────────────────────────────────────────
naver_api = NaverCommerceAPI()


async def pipeline_register_products(excel_path: str, limit: int = 50) -> dict:
    """파이프라인 1: 전 직원 협업 — 소싱→IP→시즌→트렌드→리뷰→설명→이미지→등록"""
    from employees import (
        employee_sourcing_manager, employee_ip_guardian,
        employee_season_planner, employee_trend_scout, employee_review_analyst,
        employee_price_optimizer, employee_tag_generator,
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

            # Tool 3: SEO 태그 생성
            seo_tags = await employee_tag_generator(
                str(p.get("name", "")), str(p.get("category", "")),
                review.get("selling_points", []), ANTHROPIC_API_KEY)
            ai["tags"] = seo_tags
            print(f"[태그생성] {seo_tags[:3]}...", flush=True)

            # Tool 2: 경쟁사 가격 수집 → 최적 가격 산정
            competitor_prices = await search_naver_shopping(str(p.get("name", "")))
            price_result = await employee_price_optimizer(
                str(p.get("name", "")), str(p.get("category", "")),
                int(p.get("price", 0)), ANTHROPIC_API_KEY,
                competitor_prices=competitor_prices)
            price = price_result["suggested_price"]
            print(f"[가격최적화] {price:,}원 — {price_result.get('reason','')}", flush=True)

            # ⑦ 이미지 디렉터: 메인 이미지
            _cat = str(p.get("category", ""))
            naver_img_url = await get_product_image(p)
            if not naver_img_url:
                print(f"[이미지디렉터] SKIP 이미지없음: {p.get('name', '')}", flush=True)
                results["skip"] += 1
                continue

            # ⑧ 배너 생성 (DALL-E → Pillow 폴백)
            headline_txt = ai.get("headline") or ai.get("banner_text") or p.get("name", "")[:18]
            dalle_banner_raw = await generate_dalle_banner(str(p.get("name", "")), headline_txt, _cat)
            if dalle_banner_raw:
                banner_url = await naver_api.upload_image(dalle_banner_raw, is_banner=True)
            else:
                banner_url = await create_banner_image(
                    naver_img_url, headline_txt,
                    ai.get("sub_headline") or ai.get("sub_text", "")
                )

            # ⑨ 디테일컷 생성
            detail_img_url = ""
            dalle_detail_raw = await generate_dalle_detail_shot(
                str(p.get("name","")), ai.get("spec_hint",""), _cat)
            if dalle_detail_raw:
                try:
                    detail_img_url = await naver_api.upload_image(dalle_detail_raw)
                except Exception:
                    pass

            # ⑩ HTML 상세페이지 빌드
            detail_html = build_detail_html(banner_url, naver_img_url, ai, detail_img_url)

            # ⑪ 3단계 통합 QC 파이프라인 (기술+비전+콘텐츠)
            _, reject_kws = _get_scene_context(str(p.get("name", "")))
            qc_result = await run_qc_pipeline(
                naver_img_url, str(p.get("name","")),
                detail_html, ANTHROPIC_API_KEY, reject_kws
            )
            print(f"[QC] 단계:{qc_result['stage']} 통과:{qc_result['passed']} — {qc_result['reason']}", flush=True)

            if not qc_result["passed"]:
                if qc_result["stage"] == 2:
                    # 비전검수 실패 → DALL-E 재시도 1회
                    retry_prompt = qc_result.get("retry_prompt","")
                    retry_raw = await generate_dalle_image(f"{p.get('name','')} {retry_prompt}".strip(), _cat)
                    if retry_raw:
                        retry_img = await naver_api.upload_image(retry_raw)
                        qc2 = await run_qc_pipeline(
                            retry_img, str(p.get("name","")),
                            detail_html, ANTHROPIC_API_KEY, reject_kws
                        )
                        if qc2["passed"]:
                            naver_img_url = retry_img
                            print(f"[QC] 재시도 ✅", flush=True)
                        else:
                            reject_msg = f"[반려] {p.get('name','')} | QC단계:{qc2['stage']} — {qc2['reason']}"
                            print(reject_msg, flush=True)
                            results["fail"] += 1; results["errors"].append(reject_msg)
                            continue
                    else:
                        reject_msg = f"[반려] {p.get('name','')} — DALL-E 재생성 실패"
                        print(reject_msg, flush=True)
                        results["fail"] += 1; results["errors"].append(reject_msg)
                        continue
                else:
                    # 1단계(기술)/3단계(콘텐츠) 실패 → 스킵
                    reject_msg = f"[반려] {p.get('name','')} | QC단계:{qc_result['stage']} — {qc_result['reason']}"
                    print(reject_msg, flush=True)
                    results["fail"] += 1; results["errors"].append(reject_msg)
                    continue

            payload = build_product_payload(p, ai, price, tags=ai.get("tags"))
            payload["originProduct"]["images"]["representativeImage"]["url"] = naver_img_url
            if detail_html:
                payload["originProduct"]["detailContent"] = detail_html

            await naver_api.register_product(payload)
            save_registered_code(code)
            results["success"] += 1
            print(f"[총괄] ✅ {ai.get('product_name', p.get('name',''))} ({price:,}원)", flush=True)
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


async def pipeline_auto_cleanup(
    min_age_days: int = 30,
    max_views: int = 100,
) -> dict:
    """파이프라인 5: 저성과 상품 자동 판매중지
    조건 (3가지 모두 충족 시 판매중지):
      - 등록 후 min_age_days일 이상 경과
      - 조회수(클릭수) max_views 미만
      - 판매 0건
    """
    print("[CLEANUP] 저성과 상품 검사 시작", flush=True)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=min_age_days)

    results = {
        "timestamp": now.isoformat(),
        "checked": 0,
        "deactivated": 0,
        "skipped_new": 0,
        "skipped_no_data": 0,
        "errors": [],
        "deactivated_list": [],
    }

    # 전체 상품 수집 (페이지 순회)
    all_products = []
    page = 1
    while True:
        try:
            resp = await naver_api.list_products(page=page, size=100)
        except Exception as e:
            results["errors"].append(f"상품 목록 조회 실패(p{page}): {e}")
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        if len(contents) < 100:
            break
        page += 1

    print(f"[CLEANUP] 전체 상품: {len(all_products)}개", flush=True)

    for prod in all_products:
        try:
            origin = prod.get("originProduct", {})
            status = origin.get("statusType", "")
            if status != "SALE":
                continue

            product_no    = str(prod.get("originProductNo", ""))
            channel_no    = str(prod.get("channelProductNo", ""))
            name          = origin.get("name", "")[:20]
            reg_date_str  = origin.get("regDate", "")

            # 등록일 파싱
            try:
                reg_date = datetime.fromisoformat(reg_date_str.replace("Z", "+00:00"))
            except Exception:
                results["skipped_no_data"] += 1
                continue

            # 아직 30일 미경과 → 스킵
            if reg_date > cutoff:
                results["skipped_new"] += 1
                continue

            results["checked"] += 1

            # 조회수·판매 데이터 조회
            insight = await naver_api.get_product_insight(channel_no, days=min_age_days)
            if not insight:
                print(f"[CLEANUP] 인사이트 없음 스킵: {name}", flush=True)
                results["skipped_no_data"] += 1
                continue

            click_count = insight.get("clickCount")
            order_count = insight.get("orderCount")

            # 데이터 없으면 보수적으로 스킵 (잘못된 삭제 방지)
            if click_count is None or order_count is None:
                results["skipped_no_data"] += 1
                continue

            # 저성과 조건 3가지 동시 충족 확인
            if int(click_count) < max_views and int(order_count) == 0:
                ok = await naver_api.set_product_status(product_no, "SUSPENSION")
                if ok:
                    results["deactivated"] += 1
                    entry = {
                        "product_no": product_no,
                        "name": name,
                        "click_count": click_count,
                        "order_count": order_count,
                        "reg_date": reg_date_str[:10],
                        "days_old": (now - reg_date).days,
                    }
                    results["deactivated_list"].append(entry)
                    print(f"[CLEANUP] 판매중지 ✅ {name} | 조회:{click_count} 판매:{order_count} ({(now-reg_date).days}일 경과)", flush=True)
                else:
                    results["errors"].append(f"판매중지 실패: {name} ({product_no})")
        except Exception as e:
            results["errors"].append(str(e))

    # 결과 로그 JSONL 저장
    try:
        Path(CLEANUP_LOG_FILE).parent.mkdir(exist_ok=True)
        with open(CLEANUP_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(results, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[CLEANUP] 로그 저장 실패: {e}", flush=True)

    print(
        f"[CLEANUP] 완료 — 검사:{results['checked']} "
        f"중지:{results['deactivated']} "
        f"스킵(신규):{results['skipped_new']} "
        f"스킵(데이터없음):{results['skipped_no_data']}",
        flush=True,
    )
    return results
