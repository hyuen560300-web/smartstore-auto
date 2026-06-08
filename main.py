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
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Windows cp949 터미널에서 em dash 등 UTF-8 문자 인코딩 오류 방지
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from typing import Optional, List

import anthropic
import bcrypt
import httpx
import openpyxl
from dotenv import load_dotenv


# ─── 네트워크 재시도 유틸 ────────────────────────────────────────────────────
async def _retry(coro_fn, retries: int = 3, delay: float = 5.0, label: str = "") -> any:
    """비동기 코루틴을 최대 retries회 재시도 (실패 시 delay초 대기)."""
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return await coro_fn()
        except Exception as exc:
            last_err = exc
            if attempt < retries:
                print(f"[RETRY] {label} 실패({attempt}/{retries}): {exc} — {delay}s 후 재시도", flush=True)
                await asyncio.sleep(delay)
    raise last_err


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
    # 경로 접미사 제거 (_300, _thumb, 도매꾹 _img_NNN 등)
    clean = _re.sub(
        r'(_\d{2,4}x\d{2,4}|_thumb|_small|_medium|_low|_300|_400|_500|_img_\d+)',
        '', clean, flags=_re.IGNORECASE
    )
    # _stt_NNN.png: cdn1.domeggook.com은 suffix 없으면 404 → 유지, 그 외만 제거
    if 'cdn1.domeggook.com' not in clean:
        clean = _re.sub(r'_stt_\d+\.png', '', clean)
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


def _process_image_detail(raw_bytes: bytes, max_width: int = 860) -> bytes:
    """상세 설명 이미지: 원본 비율 유지, 최대 860px 폭 리사이즈. 정사각형 패딩 없음.
    텍스트·표가 포함된 공급사 상세 이미지를 왜곡 없이 유지."""
    try:
        from PIL import Image
        img = Image.open(_io.BytesIO(raw_bytes)).convert("RGB")
        w, h = img.size
        if w > max_width:
            scale = max_width / w
            new_w = max_width
            new_h = max(1, int(h * scale))
            img = img.resize((new_w, new_h), Image.LANCZOS)
        buf = _io.BytesIO()
        img.save(buf, format="JPEG", quality=95, subsampling=0,
                 optimize=True, progressive=True)
        return buf.getvalue()
    except Exception as e:
        print(f"[IMAGE] 상세이미지 처리 실패, 원본 사용: {e}", flush=True)
        return raw_bytes


load_dotenv()

# ─── 환경변수 ────────────────────────────────────────────────────────────────
NAVER_CLIENT_ID     = os.environ.get("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")
NAVER_SELLER_ID     = os.environ.get("NAVER_SELLER_ID", "")
# 네이버 Open API (쇼핑검색) — Commerce API 키와 별도
NAVER_SEARCH_CLIENT_ID     = os.environ.get("NAVER_SEARCH_CLIENT_ID") or NAVER_CLIENT_ID
NAVER_SEARCH_CLIENT_SECRET = os.environ.get("NAVER_SEARCH_CLIENT_SECRET") or NAVER_CLIENT_SECRET
if not NAVER_CLIENT_ID:
    raise ValueError("환경변수 미설정: NAVER_CLIENT_ID")
if not NAVER_CLIENT_SECRET:
    raise ValueError("환경변수 미설정: NAVER_CLIENT_SECRET")
if not NAVER_SELLER_ID:
    raise ValueError("환경변수 미설정: NAVER_SELLER_ID")
import re as _re_keys
def _clean_key(k: str) -> str:
    return _re_keys.sub(r'[^\x21-\x7E]', '', k or "")

ANTHROPIC_API_KEY   = _clean_key(os.environ.get("ANTHROPIC_API_KEY", ""))
PEXELS_API_KEY      = _clean_key(os.environ.get("PEXELS_API_KEY", ""))
OPENAI_API_KEY      = _clean_key(os.environ.get("OPENAI_API_KEY", ""))
GOOGLE_AI_API_KEY   = _clean_key(os.environ.get("GOOGLE_AI_API_KEY", ""))
FLUX_API_KEY        = _clean_key(os.environ.get("FLUX_API_KEY", ""))
REPLICATE_API_KEY   = _clean_key(os.environ.get("REPLICATE_API_KEY", ""))
MARGIN_RATE         = float(os.environ.get("MARGIN_RATE", "0.15"))
EXCEL_FOLDER        = os.environ.get("EXCEL_FOLDER", "/tmp/uploads")
AS_PHONE            = os.environ.get("AS_PHONE", "010-0000-0000")
DOMEGGOOK_API_KEY          = _clean_key(os.environ.get("DOMEGGOOK_API_KEY", ""))
NAVER_DATALAB_CLIENT_ID    = os.environ.get("NAVER_DATALAB_CLIENT_ID", "")
NAVER_DATALAB_CLIENT_SECRET = os.environ.get("NAVER_DATALAB_CLIENT_SECRET", "")

DOMEGGOOK_API_URL  = "https://domeggook.com/ssl/api/"
DOMEGGOOK_IMG_BASE = "https://img.domeggook.com/"
# 기본 검색 키워드 — DOMEGGOOK_KEYWORDS 환경변수로 덮어쓰기 가능
_DG_KEYWORDS_ALL: list[str] = [
    kw.strip() for kw in os.environ.get(
        "DOMEGGOOK_KEYWORDS",
        (
            # 생활/주방
            "생활용품,주방용품,청소용품,수납정리,침구,욕실용품,인테리어소품,커튼,러그,조명,"
            # 뷰티/건강
            "화장품,스킨케어,헤어케어,네일,향수,다이어트,건강식품,영양제,마스크팩,선크림,"
            # 패션
            "패션잡화,여성의류,남성의류,가방,지갑,벨트,모자,양말,속옷,신발,"
            # 스포츠/아웃도어
            "스포츠용품,캠핑,등산,골프,자전거,요가매트,홈트레이닝,낚시,수영,런닝,"
            # 유아/반려
            "유아용품,출산용품,아동의류,장난감,반려동물,강아지용품,고양이용품,애완용품,"
            # 디지털/전자
            "디지털기기,스마트폰악세사리,이어폰,충전기,케이블,블루투스,노트북,태블릿,"
            # 자동차/공구
            "자동차용품,차량방향제,공구,DIY,원예,텃밭,씨앗,비료,"
            # 여행/취미
            "여행용품,캐리어,여권지갑,독서,문구,사무용품,취미,만들기,"
            # 식품/음료
            "간식,건과류,음료,커피,차,조미료,반찬,건강음료,"
            # 계절/특수
            "여름용품,수영복,래쉬가드,겨울용품,방한용품,핫팩,선풍기,에어컨커버"
        )
    ).split(",") if kw.strip()
]

def _get_rotating_keywords(n: int = 15) -> list[str]:
    """날짜 시드 기반으로 매일 다른 키워드 n개 선택."""
    import random as _r, datetime as _dt
    seed = int(_dt.date.today().strftime("%Y%m%d"))
    rng = _r.Random(seed)
    return rng.sample(_DG_KEYWORDS_ALL, min(n, len(_DG_KEYWORDS_ALL)))

_DG_KEYWORDS = _get_rotating_keywords(15)

NAVER_BASE = "https://api.commerce.naver.com/external"

Path(EXCEL_FOLDER).mkdir(parents=True, exist_ok=True)
REGISTERED_CODES_FILE = os.path.join(EXCEL_FOLDER, "registered_codes.json")
REGISTERED_NAMES_FILE = os.path.join(EXCEL_FOLDER, "registered_names.json")
CLEANUP_LOG_FILE      = os.path.join(EXCEL_FOLDER, "auto_cleanup.jsonl")

_CONTEXT_STORE_URL = os.environ.get(
    "CONTEXT_STORE_URL", "https://loving-serenity-production-2635.up.railway.app"
)


def _ctx_get(key: str):
    """context_store에서 값 조회. 실패 시 None 반환."""
    try:
        import requests as _req
        r = _req.get(f"{_CONTEXT_STORE_URL}/context/{key}", timeout=5)
        if r.status_code == 200:
            return json.loads(r.json().get("value", "null"))
    except Exception:
        pass
    return None


def _ctx_set(key: str, value) -> None:
    """context_store에 값 저장. 실패 시 무시."""
    try:
        import requests as _req
        _req.post(
            f"{_CONTEXT_STORE_URL}/context",
            json={"key": key, "value": json.dumps(value), "category": "cache"},
            timeout=5,
        )
    except Exception:
        pass


def load_registered_codes() -> set:
    # 1순위: /tmp 파일
    try:
        with open(REGISTERED_CODES_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        pass
    # 2순위: PostgreSQL context_store 복원
    data = _ctx_get("smartstore.registered_codes")
    if isinstance(data, list):
        try:
            with open(REGISTERED_CODES_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception:
            pass
        return set(data)
    return set()


def save_registered_code(code: str):
    codes = load_registered_codes()
    codes.add(str(code))
    codes_list = list(codes)
    try:
        with open(REGISTERED_CODES_FILE, "w", encoding="utf-8") as f:
            json.dump(codes_list, f)
    except Exception:
        pass
    _ctx_set("smartstore.registered_codes", codes_list)


def _normalize_name(name: str) -> str:
    """상품명 정규화: 공백·특수문자 제거 후 소문자화 (중복 체크용)."""
    import re
    return re.sub(r"[\s\W]+", "", name).lower()


def load_registered_names() -> set:
    # 1순위: /tmp 파일
    try:
        with open(REGISTERED_NAMES_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        pass
    # 2순위: PostgreSQL context_store 복원
    data = _ctx_get("smartstore.registered_names")
    if isinstance(data, list):
        try:
            with open(REGISTERED_NAMES_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception:
            pass
        return set(data)
    return set()


def save_registered_name(name: str):
    norm = _normalize_name(name)
    if not norm:
        return
    names = load_registered_names()
    names.add(norm)
    names_list = list(names)
    try:
        with open(REGISTERED_NAMES_FILE, "w", encoding="utf-8") as f:
            json.dump(names_list, f)
    except Exception:
        pass
    _ctx_set("smartstore.registered_names", names_list)


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

    async def upload_detail_image(self, raw_bytes: bytes) -> str:
        """상세 설명 이미지 업로드: 원본 비율 유지, 860px 폭, 정사각형 패딩 없음."""
        image_bytes = _process_image_detail(raw_bytes, max_width=860)
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

    async def list_products(self, page: int = 1, size: int = 50, days: int = 365) -> dict:
        """등록된 상품 목록 조회 (상품명/가격/상태 포함).
        search API는 ID만 반환하므로 개별 상세 조회를 asyncio.gather로 병렬 처리."""
        now = datetime.now(timezone.utc)
        headers = await self._headers()
        async with httpx.AsyncClient(timeout=30) as c:
            # 1단계: 상품 ID 목록 조회 (3회 재시도)
            async def _search():
                r = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=headers,
                    json={
                        "productStatusTypes": ["SALE", "SUSPENSION"],
                        "page": page,
                        "size": size,
                        "orderType": "NO",
                        "periodType": "PROD_REG_DAY",
                        "fromDate": (now - timedelta(days=days)).strftime("%Y-%m-%d"),
                        "toDate": now.strftime("%Y-%m-%d"),
                    }
                )
                r.raise_for_status()
                return r.json()

            data = await _retry(_search, retries=3, delay=5.0, label=f"list_products(p{page})")
            contents = data.get("contents", [])

            # 2단계: 각 상품 상세 병렬 조회 (개별 실패 시 3회 재시도)
            async def _fetch_detail(item: dict) -> dict:
                product_no = item.get("originProductNo")
                if not product_no:
                    return item
                async def _get():
                    dr = await c.get(
                        f"{NAVER_BASE}/v2/products/origin-products/{product_no}",
                        headers=headers,
                        timeout=15,
                    )
                    if dr.status_code == 200:
                        item["originProduct"] = dr.json().get("originProduct", {})
                    return item
                try:
                    return await _retry(_get, retries=3, delay=5.0, label=f"fetch_detail({product_no})")
                except Exception:
                    return item

            # 5개씩 청크 병렬 처리 — 50개 동시 호출 시 대부분 타임아웃 발생 방지
            enriched: list[dict] = []
            for i in range(0, len(contents), 5):
                chunk = contents[i:i + 5]
                chunk_results = await asyncio.gather(*[_fetch_detail(c) for c in chunk])
                enriched.extend(chunk_results)
                if i + 5 < len(contents):
                    await asyncio.sleep(0.4)
            data["contents"] = enriched
            return data

    async def update_product(self, product_id: str, payload: dict) -> tuple[bool, str]:
        """상품 정보 수정. 반환: (성공여부, 에러메시지)"""
        try:
            async with httpx.AsyncClient(timeout=60) as c:
                r = await c.put(
                    f"{NAVER_BASE}/v2/products/origin-products/{product_id}",
                    headers=await self._headers(),
                    json={"originProduct": payload},
                )
            if r.status_code == 200:
                return True, ""
            msg = f"HTTP {r.status_code}: {r.text[:400]}"
            print(f"[UPDATE] ❌ ({product_id}): {msg}", flush=True)
            return False, msg
        except Exception as e:
            msg = str(e)[:200]
            print(f"[UPDATE] 상품 수정 실패({product_id}): {msg}", flush=True)
            return False, msg

    async def set_product_status(self, product_id: str, status: str) -> bool:
        """상품 상태 변경: SALE(판매중) / SUSPENSION(판매중지) / CLOSE(판매종료)"""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.put(
                f"{NAVER_BASE}/v2/products/origin-products/{product_id}",
                headers=await self._headers(),
                json={"originProduct": {"statusType": status}}
            )
            return r.status_code == 200

    async def count_sale_products(self) -> int:
        """현재 판매중(SALE) 상품 수 조회.
        Naver 검색 API는 toDate=오늘 + 단일 SALE 필터 조합에서 0을 반환하는 버그가 있음.
        toDate를 먼 미래로 설정해 우회."""
        try:
            base_params = {
                "page": 1, "size": 1,
                "orderType": "NO",
                "periodType": "PROD_REG_DAY",
                "fromDate": "2020-01-01",
                "toDate": "2099-12-31",
            }
            async with httpx.AsyncClient(timeout=15) as c:
                r_all = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=await self._headers(),
                    json={**base_params, "productStatusTypes": ["SALE", "SUSPENSION"]},
                )
                r_sus = await c.post(
                    f"{NAVER_BASE}/v1/products/search",
                    headers=await self._headers(),
                    json={**base_params, "productStatusTypes": ["SUSPENSION"]},
                )
            total = int(r_all.json().get("totalElements", 0)) if r_all.ok else 0
            suspension = int(r_sus.json().get("totalElements", 0)) if r_sus.ok else 0
            return max(0, total - suspension)
        except Exception:
            return 0

    async def delete_product(self, product_id: str) -> bool:
        """상품 삭제"""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.delete(
                f"{NAVER_BASE}/v2/products/origin-products/{product_id}",
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
            hdrs = await self._headers()
            async with httpx.AsyncClient(timeout=15) as c:
                async def _get():
                    r = await c.get(
                        f"{NAVER_BASE}/v1/channel-products/{channel_product_no}/insights",
                        headers=hdrs,
                        params={"searchDateFrom": from_date, "searchDateTo": to_date},
                    )
                    r.raise_for_status()
                    return r.json()
                return await _retry(_get, retries=3, delay=5.0, label=f"insight({channel_product_no})")
        except Exception as e:
            print(f"[INSIGHT] {channel_product_no} 조회 실패: {e}", flush=True)
        return None

    async def update_price(self, product_id: str, price: int) -> bool:
        """상품 가격 수정"""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.put(
                f"{NAVER_BASE}/v2/products/origin-products/{product_id}",
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


# ─── 도매꾹 API 소싱 ─────────────────────────────────────────────────────────

def _dg_str(val) -> str:
    """XML→JSON 응답에서 문자열 값 추출. dict이면 #text 또는 text 키 사용."""
    if isinstance(val, dict):
        return str(val.get("#text", "") or val.get("text", "") or val.get("@text", "") or "")
    return str(val or "")


def _dg_img_url(thumb: str) -> str:
    """도매꾹 thumb 값 → 이미지 풀 URL 변환 (해시 또는 이미 URL인 경우 모두 처리)"""
    if not thumb:
        return ""
    if str(thumb).startswith("http"):
        return str(thumb)
    return f"{DOMEGGOOK_IMG_BASE}{thumb}"


def _dg_stt_to_original(url: str) -> str:
    """도매꾹 이미지 URL → 최대 화질 URL 변환. CDN별 처리:
    - img.domeggook.com  : _stt_NNN.png → _img_760 (suffix 없으면 404)
    - cdn1.domeggook.com : _stt_NNN.png 유지 (suffix 제거하면 해시만 남아 404)
    - 기타              : _stt_NNN.png, _img_NNN 제거 → 원본"""
    if not url:
        return url
    import re as _re2
    url = str(url)
    if "img.domeggook.com" in url:
        return _re2.sub(r'_stt_\d+\.png', '_img_760', url)
    if "cdn1.domeggook.com" in url:
        # suffix 제거 시 확장자 없는 해시 URL → 404. 330px 썸네일 그대로 사용
        return url
    # 그 외 CDN: suffix 제거
    url = _re2.sub(r'_stt_\d+\.png', '', url)
    url = _re2.sub(r'_img_\d+', '', url)
    return url


async def _dg_item_detail(item_no: str) -> dict:
    """도매꾹 상품 상세 조회 ver=4.5 (getItemView).
    반환: data["domeggook"] 내부 dict (basis/price/qty/thumb 포함). 실패 시 {}."""
    if not DOMEGGOOK_API_KEY:
        return {}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(DOMEGGOOK_API_URL, params={
                "ver": "4.5", "mode": "getItemView",
                "aid": DOMEGGOOK_API_KEY, "no": item_no, "om": "json",
            })
            r.raise_for_status()
            return r.json().get("domeggook", {})
    except Exception as e:
        print(f"[DOMEGGOOK] 상세조회 실패({item_no}): {e}", flush=True)
        return {}


def _dg_to_product(item: dict, detail: dict) -> dict | None:
    """도매꾹 getItemList item + getItemView detail → 내부 product dict.
    실제 응답 구조 기반:
      item: {no, title, thumb(풀URL), price, deli}
      detail: {basis:{title,section,keywords,img_use}, price:{dome}, qty:{inventory}, thumb:{original,large,list}}
    """
    no   = str(item.get("no", "")).strip()
    name = str(item.get("title", "") or "").strip()
    if not no or not name:
        return None

    price = _to_int(str(item.get("price", 0)))
    if price <= 0:
        return None

    # 이미지 우선순위: 상세 original > 상세 large > 목록 thumb
    # _dg_str() 사용: XML→JSON 변환 시 dict으로 올 수 있음
    thumb_obj = detail.get("thumb", {}) if detail else {}
    image = (
        _dg_stt_to_original(_dg_str(thumb_obj.get("original"))) or
        _dg_stt_to_original(_dg_str(thumb_obj.get("large"))) or
        _dg_stt_to_original(_dg_str(item.get("thumb")))
    )

    # 이미지 장수: thumb.list 또는 개별 필드 카운트
    thumb_list = thumb_obj.get("list", [])
    if isinstance(thumb_list, dict):
        # XML→JSON 패턴: thumb.list.item (단일 dict 또는 배열)
        _items = thumb_list.get("item", [])
        if isinstance(_items, dict):
            thumb_list = [_items]
        elif isinstance(_items, list):
            thumb_list = _items
        else:
            thumb_list = []
    if isinstance(thumb_list, list) and thumb_list:
        # 각 item에서 실제 URL 추출 (dict인 경우 _dg_str 사용)
        valid_imgs = [_dg_str(i) if isinstance(i, dict) else str(i or "") for i in thumb_list]
        img_count = sum(1 for u in valid_imgs if u.strip())
    else:
        # list 없으면 개별 필드(original/large/medium/small) 개수로 근사
        img_count = sum(1 for k in ("original", "large", "medium", "small") if _dg_str(thumb_obj.get(k)))
        img_count = max(img_count, 1 if image else 0)

    # 카테고리: 상세 basis.section (예: "생활/주방 > 수납/정리")
    basis    = detail.get("basis", {}) if detail else {}
    category = str(basis.get("section") or basis.get("keywords") or "").split(">")[0].strip()

    # 이미지사용허용 플래그: basis.img_use (Y/1 = 허용)
    raw_img_use = str(basis.get("img_use", "Y") or "Y").strip().upper()
    img_use_ok  = raw_img_use in ("Y", "1", "YES", "TRUE")

    # 재고: qty.inventory (기본 100 캡)
    qty   = detail.get("qty", {}) if detail else {}
    stock = min(_to_int(str(qty.get("inventory", 100))) or 100, 100)

    # 상세 설명 HTML (공급사 상세페이지 이미지 포함)
    content = str(detail.get("content", "") or "") if detail else ""

    # desc.contents.item에서 추가 이미지 파싱
    _desc_html = str((detail.get("desc", {}) or {}).get("contents", {}).get("item", "") or "") if detail else ""
    extra_images = extract_domeggook_images(_desc_html, main_url=image, max_count=5)

    return {
        "code":             f"DG_{no}",
        "name":             name,
        "price":            price,
        "image":            image,
        "category":         category,
        "stock":            stock,
        "source":           "domeggook",
        "_dg_no":           no,
        "_dg_content":      content,
        "_dg_img_count":    img_count,
        "_dg_img_use":      img_use_ok,
        "_dg_extra_images": extra_images,
    }


import re as _re_img


_SUPPLIER_FILTER_KWS = ["소싱", "무역", "견적", "운송", "공급사", "셀렉터", "DISTRIBUTION", "배송 거리"]

def extract_domeggook_images(desc_html: str, main_url: str = "", max_count: int = 5) -> list[str]:
    """도매꾹 desc.contents.item HTML에서 추가 이미지 URL 파싱.
    공급사 홍보 문구(소싱/무역/견적 등) 주변 이미지 제외. 메인 이미지 중복 제외."""
    if not desc_html:
        return []
    seen = {main_url} if main_url else set()
    result = []
    for m in _re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']', desc_html, _re.I):
        url = m.group(1).strip()
        if not url.startswith("http") or url in seen:
            continue
        pos = m.start()
        ctx_text = _re.sub(r'<[^>]+>', '', desc_html[max(0, pos - 500) : pos + 500])
        if any(kw in ctx_text for kw in _SUPPLIER_FILTER_KWS):
            continue
        seen.add(url)
        result.append(url)
        if len(result) >= max_count:
            break
    return result


# ─── 소싱 품질 필터 ──────────────────────────────────────────────────────────

_FAKE_BRAND_NAMES = {
    "나이키", "nike", "아디다스", "adidas", "구찌", "gucci",
    "루이비통", "louis vuitton", "에르메스", "hermes", "샤넬", "chanel",
    "롤렉스", "rolex", "버버리", "burberry", "발렌시아가", "balenciaga",
    "프라다", "prada", "몽클레어", "moncler", "캐나다구스", "canada goose",
    "무스너클", "오메가", "까르띠에", "cartier", "페라리", "포르쉐",
    "애플정품", "삼성정품", "아이폰정품",
}
_FORBIDDEN_KEYWORDS = {
    "성인용", "19금", "섹스", "콘돔", "처방전", "의약품", "마약",
    "도박", "베팅", "복권당첨", "불법", "총기", "폭발물",
}

def _is_fake_product(p: dict) -> bool:
    """가짜·위험 상품 판별. True 반환 시 소싱 제외."""
    name  = (p.get("name") or "").lower()
    price = int(p.get("price") or 0)
    # 금지 키워드
    for kw in _FORBIDDEN_KEYWORDS:
        if kw.lower() in name:
            return True
    # 위조 브랜드 + 비정상 저가 (정품 불가 가격대)
    for brand in _FAKE_BRAND_NAMES:
        if brand.lower() in name and price < 50_000:
            return True
    # 비정상 가격
    if price < 1_000:
        return True
    return False


async def _tg_sourcing_skip(name: str, reason: str):
    """소싱 스킵 텔레그램 알림 — 건수 요약으로 대체, 개별 알림 비활성화."""
    pass


async def _check_image_sharpness(url: str) -> tuple[float, int]:
    """이미지 URL → (Laplacian variance, file_size_bytes).
    실패·조회불가 시 (-1, 0) 반환."""
    try:
        from PIL import Image, ImageFilter
        import numpy as np
        import io as _io
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200 or not r.content:
            return -1, 0
        file_size = len(r.content)
        img = Image.open(_io.BytesIO(r.content)).convert("L")
        if img.width < 500 or img.height < 500:
            return 0.0, file_size
        arr = np.array(img.filter(ImageFilter.FIND_EDGES), dtype=float)
        return float(np.var(arr)), file_size
    except Exception:
        return -1, 0


async def _dg_apply_quality_filter(products: list[dict]) -> list[dict]:
    """도매꾹 소싱 품질 필터 (6단계).
    ① 가짜/위험 상품  ② 이미지사용 미허용  ③ 이미지 장수 < 3
    ④ 이름 유사도 ≥ 70% (중복)  ⑤ 해상도 < 500px / 파일크기 < 50KB  ⑥ 흐릿한 이미지
    스킵 시 텔레그램 [소싱스킵] 알림."""
    from difflib import SequenceMatcher
    BLUR_THRESHOLD  = 200   # Laplacian variance (FIND_EDGES+np.var 기준)
    MIN_FILE_KB     = 50    # 50KB
    MIN_IMG_COUNT   = 1     # 최소 이미지 장수 (thumb 구조 다양성으로 1로 조정)
    SIM_THRESHOLD   = 0.70  # 이름 유사도 상한

    before = len(products)
    registered_names = load_registered_names()  # 정규화된 이름 set

    async def _notify_skip(p: dict, reason: str):
        print(f"[품질필터] ❌ {reason}: {p.get('name','')[:40]}", flush=True)

    # ① 가짜/위험 상품
    valid, fake_removed = [], 0
    for p in products:
        if _is_fake_product(p):
            fake_removed += 1
            await _notify_skip(p, "가짜/위험 상품")
        else:
            valid.append(p)
    if fake_removed:
        print(f"[품질필터] 가짜 제거 {fake_removed}개", flush=True)

    # ② 이미지사용 미허용
    passed2, img_use_removed = [], 0
    for p in valid:
        if not p.get("_dg_img_use", True):
            img_use_removed += 1
            await _notify_skip(p, "이미지사용 미허용")
        else:
            passed2.append(p)

    # ③ 이미지 장수 < 3
    passed3, count_removed = [], 0
    for p in passed2:
        cnt = p.get("_dg_img_count", 3)
        if cnt < MIN_IMG_COUNT:
            count_removed += 1
            await _notify_skip(p, f"이미지 {cnt}장 (최소 {MIN_IMG_COUNT}장)")
        else:
            passed3.append(p)

    # ④ 이름 유사도 중복 체크 (difflib)
    passed4, sim_removed = [], 0
    for p in passed3:
        norm = _normalize_name(p.get("name", ""))
        similar = any(
            SequenceMatcher(None, norm, rn).ratio() >= SIM_THRESHOLD
            for rn in registered_names
            if abs(len(norm) - len(rn)) <= max(len(norm), 1) // 2  # 길이 차 큰 건 건너뜀
        )
        if similar:
            sim_removed += 1
            await _notify_skip(p, "유사 상품명 중복")
        else:
            passed4.append(p)

    # ⑤⑥ 이미지 다운로드 → 해상도·파일크기·선명도 병렬 체크 (5개씩)
    async def _score(p: dict) -> tuple[dict, float, int]:
        variance, fsize = await _check_image_sharpness(p.get("image", ""))
        return p, variance, fsize

    scored: list[tuple[dict, float, int]] = []
    for i in range(0, len(passed4), 5):
        chunk_res = await asyncio.gather(*[_score(p) for p in passed4[i:i+5]])
        scored.extend(chunk_res)
        if i + 5 < len(passed4):
            await asyncio.sleep(0.2)

    passed, img_removed = [], 0
    for p, variance, fsize in scored:
        name_s = p.get("name", "")[:40]
        if variance < 0:
            img_removed += 1
            await _notify_skip(p, "이미지 없음/오류")
        elif fsize < MIN_FILE_KB * 1024:
            img_removed += 1
            await _notify_skip(p, f"파일크기 {fsize//1024}KB < {MIN_FILE_KB}KB")
        elif variance == 0.0:
            img_removed += 1
            await _notify_skip(p, "해상도 500px 미만")
        elif variance < BLUR_THRESHOLD:
            img_removed += 1
            await _notify_skip(p, f"흐릿한 이미지(score={variance:.0f})")
        else:
            passed.append(p)

    removed_total = fake_removed + img_use_removed + count_removed + sim_removed + img_removed
    print(
        f"[품질필터] 최종 통과 {len(passed)}개 / 원본 {before}개 "
        f"(가짜-{fake_removed} / 이미지허용-{img_use_removed} / 장수-{count_removed} "
        f"/ 유사중복-{sim_removed} / 이미지불량-{img_removed})",
        flush=True,
    )
    return passed


async def _dg_content_to_naver_html(content_html: str) -> str:
    """도매꾹 상세 설명 HTML → 이미지 URL을 Naver 업로드 URL로 교체한 HTML.
    이미지가 없거나 전부 실패하면 빈 문자열 반환 (AI 폴백 사용)."""
    if not content_html:
        return ""
    img_urls = _re_img.findall(r'<img[^>]+src=["\']([^"\']+)["\']', content_html, _re_img.IGNORECASE)
    if not img_urls:
        return ""
    result = content_html
    uploaded = 0
    for url in img_urls:
        try:
            clean_url = _dg_stt_to_original(_extract_hq_url(url))
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
                r = await c.get(clean_url)
                if r.status_code != 200 and clean_url != url:
                    r = await c.get(url)
                r.raise_for_status()
            naver_url = await naver_api.upload_detail_image(r.content)
            result = result.replace(url, naver_url)
            uploaded += 1
            print(f"[상세이미지] ✅ {url[:70]}", flush=True)
        except Exception as e:
            print(f"[상세이미지] 실패({url[:50]}): {e}", flush=True)
    if uploaded == 0:
        return ""
    return result


async def fetch_domeggook_products(
    keywords: Optional[List[str]] = None,
    pool_size: int = 90,
    min_price: int = 3000,
    max_price: int = 150000,
    start_page: int = 0,
) -> list[dict]:
    """도매꾹 키워드 검색 → 상세 병렬 조회 → product dict 리스트 반환.
    pool_size: sourcing manager에게 넘길 후보 수 (limit의 3배 권장).
    start_page: 0이면 등록 코드 수 기반으로 페이지 자동 산출 (중복 회피).

    정확한 파라미터명 (오류 발생 주의):
      sz=페이지당수, mnp/mxp=가격범위, who=S(판매자부담=무료배송), org=kr(국산), market=dome
    응답 구조: data["domeggook"]["list"]["item"]
    """
    if not DOMEGGOOK_API_KEY:
        print("[DOMEGGOOK] DOMEGGOOK_API_KEY 없음", flush=True)
        return []

    if start_page <= 0:
        registered_count = len(load_registered_codes())
        # 페이지당 30개 기준, 등록 수에 맞춰 다음 페이지 산출 (최대 10페이지)
        start_page = min(10, max(1, (registered_count // 30) + 1))
    print(f"[DOMEGGOOK] 검색 시작 페이지: {start_page}", flush=True)

    kws = keywords or _DG_KEYWORDS
    seen: set[str] = set()
    raw_items: list[dict] = []

    for kw in kws:
        if len(raw_items) >= pool_size:
            break
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.get(DOMEGGOOK_API_URL, params={
                    "ver": "4.1", "mode": "getItemList",
                    "aid": DOMEGGOOK_API_KEY,
                    "market": "dome",
                    "kw": kw, "om": "json",
                    "mnp": str(min_price), "mxp": str(max_price),
                    "sz": "30",    # 페이지당 30개
                    "pg": str(start_page),
                    "so": "rd",    # 추천도순
                })
                r.raise_for_status()
                data = r.json()
            # 실제 응답: data["domeggook"]["list"]["item"]
            items = data.get("domeggook", {}).get("list", {}).get("item", [])
            if not isinstance(items, list):
                items = []
            added = 0
            for it in items:
                no = str(it.get("no", ""))
                if no and no not in seen:
                    seen.add(no)
                    raw_items.append(it)
                    added += 1
            total = data.get("domeggook", {}).get("header", {}).get("numberOfItems", "?")
            print(f"[DOMEGGOOK] '{kw}' → +{added}개 (전체 {total}개, 누적 {len(raw_items)})", flush=True)
            await asyncio.sleep(0.35)   # API 제한 180req/min 준수
        except Exception as e:
            print(f"[DOMEGGOOK] '{kw}' 검색 오류: {e}", flush=True)

    if not raw_items:
        return []

    # 상세 병렬 조회 (asyncio.gather 한 번에 너무 많으면 API 제한 → 10개씩 청크)
    candidates = raw_items[:pool_size]
    print(f"[DOMEGGOOK] 상세 조회 시작: {len(candidates)}개", flush=True)
    details: list = []
    chunk_size = 10
    for i in range(0, len(candidates), chunk_size):
        chunk = candidates[i:i + chunk_size]
        chunk_results = await asyncio.gather(
            *[_dg_item_detail(str(it["no"])) for it in chunk],
            return_exceptions=True,
        )
        details.extend(chunk_results)
        if i + chunk_size < len(candidates):
            await asyncio.sleep(0.5)

    products = []
    for it, det in zip(candidates, details):
        det_dict = det if isinstance(det, dict) else {}
        p = _dg_to_product(it, det_dict)
        if p:
            products.append(p)

    # 소싱 단계 필수 데이터 검증: name + price + image 없으면 제외
    products = [p for p in products if p.get("name","").strip() and p.get("price",0) > 0 and str(p.get("image","")).startswith("http")]
    print(f"[DOMEGGOOK] 변환 완료: {len(products)}개 (name+price+image 검증 통과)", flush=True)

    # 품질 필터: 가짜 상품 + 흐릿한/없는 이미지 제거
    products = await _dg_apply_quality_filter(products)
    # 블루오션 점수로 정렬 (높은 점수 = 검색량 많고 경쟁 적음)
    products = await _rank_products_blue_ocean(products)
    return products


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
        if price > 0 and str(item.get("image", "")).startswith("http"):
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
    fashion_trends = ctx.get("fashion_trends", [])
    pain_points = ctx.get("pain_points", [])
    selling_points = ctx.get("selling_points", [])

    extra_context = ""
    if season_info:
        extra_context += f"\n현재 시즌 이벤트: {season_info}"
    if trend_keywords:
        extra_context += f"\n실시간 트렌딩 키워드: {', '.join(trend_keywords[:5])}"
    if fashion_trends:
        extra_context += f"\n네이버 패션 핫 키워드(ratio≥15.0): {', '.join(fashion_trends[:5])}"
    if pain_points:
        extra_context += f"\n고객 Pain Point (반드시 해결책 언급): {', '.join(pain_points)}"
    if selling_points:
        extra_context += f"\n핵심 셀링포인트: {', '.join(selling_points)}"
    naver_keywords = ctx.get("naver_keywords", [])
    if naver_keywords:
        extra_context += f"\n네이버 쇼핑 검색량 높은 키워드(상품명에 우선 반영): {', '.join(naver_keywords[:5])}"

    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=3000,
        system=[{
            "type": "text",
            "text": (
                "당신은 네이버·카페24·Shopify 멀티플랫폼 이커머스 마케팅 전문가입니다. "
                "상품명 규칙: ① naver_keywords가 제공된 경우 해당 키워드를 상품명 앞부분에 배치 "
                "② [핵심키워드]+[속성/규격] 25자 내외 "
                "③ 특수문자([],(),/,*), 관리번호, 동일단어 중복 절대 금지 "
                "④ 최고·제일·1등·공짜·무료·특가 등 네이버 금지어 사용 금지. "
                "SEO: 네이버/구글 검색 노출용 키워드 자연스럽게 포함. "
                "GEO(생성형AI 검색 최적화): ChatGPT·Perplexity·Google AI Overview에서 "
                "이 상품이 추천되도록 '질문-답변' 구조의 자연어 설명 작성. "
                "HS코드: 실제 관세청 기준 6자리 HS코드 추정 (틀리면 0000.00). "
                "반드시 JSON만 출력하세요."
            ),
            "cache_control": {"type": "ephemeral"}
        }],
        messages=[{
            "role": "user",
            "content": f"""아래 상품을 멀티플랫폼(스마트스토어·카페24·Shopify) SEO/GEO 최적화 형식으로 변환하세요.
{extra_context}

상품 정보:
{json.dumps(product, ensure_ascii=False)}

출력 형식 (JSON만):
{{
  "product_name": "핵심키워드+속성/규격, 25자 내외, 특수문자/중복단어/금지어 없이 자연스러운 한국어",
  "headline": "배너용 핵심 편익 (예: '한 번으로 3배 오래!'), 18자 이내, 숫자 필수",
  "sub_headline": "배너 서브 문구 28자 이내",
  "emotional_copy": "감성 설득 문구 2~3문장. 구매자 일상과 연결, 100자 내외.",
  "recommend_list": ["이런 분 추천 1 (20자 이내)", "이런 분 추천 2", "이런 분 추천 3", "이런 분 추천 4", "이런 분 추천 5"],
  "reason_1": "사야 하는 이유 1 — Pain Point 해결, 구체적 수치 포함 (40자 이내)",
  "reason_2": "사야 하는 이유 2 — 차별점/기술력 강조 (40자 이내)",
  "reason_3": "사야 하는 이유 3 — 시즌/트렌드/절약 연결 (40자 이내)",
  "spec_rows": [["항목","값"],["항목","값"],["항목","값"],["항목","값"],["항목","값"]],
  "spec_hint": "DALL-E 프롬프트용 힌트 — 소재·기능·특징 영어 10단어 이내",
  "compare_points": ["타사 대비 차별점 1 (30자)", "차별점 2", "차별점 3"],
  "tags": ["태그1","태그2","태그3","태그4","태그5"],
  "seo_description": "네이버·구글 검색 노출용 200자 내외 상품 설명. 핵심 키워드 3~5개 자연스럽게 포함. 상품 특징·용도·소재·사이즈 중심.",
  "geo_faq": [
    {{"q": "이 상품은 어떤 분께 추천하나요?", "a": "30자 내외 자연어 답변"}},
    {{"q": "소재/사이즈/용도 관련 자주 묻는 질문", "a": "30자 내외 답변"}},
    {{"q": "배송·반품 관련 질문", "a": "30자 내외 답변"}}
  ],
  "hs_code": "관세청 기준 6자리 HS코드 (예: 392490, 610910). 불확실하면 000000",
  "hs_code_desc": "HS코드 품목 설명 영문 (예: Household articles of plastics)",
  "customs_product_name": "통관 시 품목명 영문 (예: Plastic storage container)",
  "customs_material": "주요 소재 영문 (예: Polypropylene, Cotton, Stainless steel)",
  "customs_origin": "원산지 추정 (China / Korea / Unknown 중 1개)"
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
            "seo_description": f"{name} 상품입니다. 합리적인 가격과 빠른 배송으로 만족스러운 쇼핑을 경험하세요.",
            "geo_faq": [
                {"q": "이 상품은 어떤 분께 추천하나요?", "a": f"{name}이 필요하신 분께 추천합니다."},
                {"q": "배송은 얼마나 걸리나요?", "a": "주문 후 1~3일 이내 배송됩니다."},
                {"q": "반품이 가능한가요?", "a": "수령 후 7일 이내 반품 가능합니다."},
            ],
            "hs_code": "000000",
            "hs_code_desc": "General merchandise",
            "customs_product_name": name,
            "customs_material": "Unknown",
            "customs_origin": "Unknown",
        }


# ─── 스마트스토어 상품 payload 빌더 ──────────────────────────────────────────
# 카테고리 기본값 (오너클랜 카테고리 → 네이버 카테고리 ID 매핑)
# 필요 시 https://api.commerce.naver.com/external/v1/categories/roots 조회 후 추가
CATEGORY_ID_MAP = {
    # 기본 생활
    "남성의류": 50000830, "여성의류": 50000803,
    "티셔츠": 50000830,  "바지": 50000831,    "아우터": 50021640,
    "주방": 50002717,    "생활용품": 50002717, "주방용품": 50002717,
    "청소": 50002717,    "인테리어": 50002717, "수납정리": 50002717,
    "침구": 50002717,    "욕실용품": 50002717, "원예": 50002717,
    "공구": 50002717,    "가전": 50000830,
    # 뷰티
    "뷰티": 50000140,    "화장품": 50000140,  "헤어케어": 50000140,
    "네일": 50000140,    "향수": 50000140,    "다이어트": 50000140,
    # 식품/건강
    "식품": 50000236,    "건강": 50000236,
    # 스포츠/아웃도어
    "스포츠": 50000430,  "캠핑": 50000430,    "등산": 50000430,
    "골프": 50000430,    "자전거": 50000430,  "요가": 50000430,
    "홈트레이닝": 50000430, "여행용품": 50000430, "자동차용품": 50000430,
    # 유아/완구/문구
    "유아용품": 50000564, "완구": 50000564,   "문구": 50000564,
    "도서": 50000727,
    # 반려동물
    "반려동물": 50000430,
    # 디지털
    "디지털": 50000830,
    # 패션잡화
    "패션잡화": 50000803,
    # 의류 상세
    "블라우스": 50000803, "니트": 50000830,   "후드티": 50000830,
    "맨투맨": 50000830,   "셔츠": 50000830,   "탑": 50000803,    "조끼": 50000830,
    "청바지": 50000831,   "슬랙스": 50000831,  "반바지": 50000831,
    "스커트": 50000803,   "레깅스": 50000831,  "트레이닝바지": 50000831,
    "자켓": 50021640,     "코트": 50021640,    "패딩": 50021640,
    "바람막이": 50021640, "가디건": 50021640,  "점퍼": 50021640,  "트렌치코트": 50021640,
    "원피스": 50000803,   "투피스": 50000803,  "정장": 50000830,  "수트": 50000830,
    "20대패션": 50000803, "30대패션": 50000803,
}
DEFAULT_CATEGORY_ID = 50000830


def get_category_id(product: dict, hot_trends: Optional[List[str]] = None) -> int:
    # 1. 오너클랜 Excel의 카테고리코드 직접 사용
    cat_id = product.get("category_id")
    if cat_id:
        try:
            return int(str(cat_id).strip())
        except (ValueError, TypeError):
            pass
    # 2. 카테고리 필드 기반 매핑
    for key in ("cat_large", "cat_medium", "category"):
        val = str(product.get(key, ""))
        for k, v in CATEGORY_ID_MAP.items():
            if k in val:
                return v
    # 3. 네이버 패션 트렌드 핫 키워드(ratio≥15.0)로 상품명 매칭
    if hot_trends:
        prod_name = str(product.get("name", ""))
        for kw in hot_trends:
            mapped = CATEGORY_ID_MAP.get(kw)
            if mapped and kw in prod_name:
                return mapped
    return DEFAULT_CATEGORY_ID


def build_product_payload(
    raw: dict,
    ai: dict,
    selling_price: int,
    tags: list = None,
    hot_trends: Optional[List[str]] = None,
) -> dict:
    is_free = str(raw.get("delivery_type", "")).strip() in ("무료배송", "무료")
    try:
        delivery_fee = int(float(str(raw.get("delivery_fee", 3000)).replace(",", "")))
    except (ValueError, TypeError):
        delivery_fee = 3000

    return {
        "originProduct": {
            "statusType": "SALE",
            "saleType": "NEW",
            "leafCategoryId": get_category_id(raw, hot_trends),
            "name": (clean_product_name(ai.get("product_name") or ai.get("name") or str(raw.get("name", "")))),
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
    """1000×1000 헤드라인 배너 — #1a1a1a 상단바 + contain 방식 (crop 없음) + 고화질 저장"""
    try:
        from PIL import Image, ImageDraw, ImageFont
        io = _io

        W, H = 1000, 1000
        FONT_SIZE_MAIN = int(W * 0.075)  # 가로 폭의 7.5%
        FONT_SIZE_SUB  = int(W * 0.040)

        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.get(image_url)
            r.raise_for_status()

        # ─ contain 방식: 비율 유지 + 패딩 (crop 절대 금지) ─────────────────
        orig = Image.open(io.BytesIO(r.content)).convert("RGB")

        # 상단 #1a1a1a 텍스트 바 (H의 32%)
        bar_h  = int(H * 0.32)
        img_h  = H - bar_h             # 이미지 영역 높이 (나머지 68%)

        # 이미지 영역(W×img_h) 안에 비율 유지로 배치 — 절대 crop 안 함
        scale  = min(W / orig.width, img_h / orig.height)
        new_w  = max(1, int(orig.width  * scale))
        new_h  = max(1, int(orig.height * scale))
        resized = orig.resize((new_w, new_h), Image.LANCZOS)

        # 캔버스: 흰 배경 1000×1000
        final = Image.new("RGB", (W, H), (255, 255, 255))

        # 상단 바 붙이기
        bar = Image.new("RGB", (W, bar_h), (26, 26, 26))
        final.paste(bar, (0, 0))

        # 이미지를 이미지 영역 중앙에 배치
        img_x = (W    - new_w) // 2
        img_y = bar_h + (img_h - new_h) // 2
        final.paste(resized, (img_x, img_y))

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
# 이미지 CSS: 작은 원본 이미지를 강제로 늘리지 않도록 width:auto 우선
_IMG = ('style="max-width:100%;width:auto;height:auto;display:block;'
        'margin:20px auto;min-width:200px;"')
_IMG_FIXED = ('style="width:860px;max-width:100%;height:auto;display:block;'
              'margin:20px auto;"')  # 실제 860px 이상 이미지용
_WRAP = ('style="max-width:860px;margin:0 auto;padding:0 16px 24px;'
         'font-family:\'나눔고딕\',Apple SD Gothic Neo,sans-serif;color:#333;"')


def _build_seo_text_section(ai: dict, product_name: str) -> str:
    """SEO/GEO 최적화 텍스트 섹션 — 네이버 검색 노출 + 지역 타깃 포함."""
    tags = ai.get("tags", [])[:8]
    name = ai.get("product_name", product_name or "상품")
    emotional = ai.get("emotional_copy", "")
    kw_str = " · ".join(tags) if tags else name

    geo_keywords = [
        "전국 빠른 배송", "서울 당일출고", "무료배송 가능",
        "스마트스토어 공식판매", "정품 보장",
    ]
    geo_html = " &nbsp;|&nbsp; ".join(geo_keywords)

    seo_block = (
        f'<div style="background:#f8f9ff;border-radius:10px;padding:16px 20px;'
        f'margin:16px 0;border-left:3px solid #1a73e8;">'
        f'<p style="font-size:13px;color:#888;margin:0 0 6px;">🔍 검색 키워드</p>'
        f'<p style="font-size:14px;color:#1a73e8;font-weight:600;margin:0;">'
        f'{kw_str}</p>'
        f'<p style="font-size:12px;color:#aaa;margin:8px 0 0;">{geo_html}</p>'
        f'</div>'
    )

    intro_html = (
        f'<div {_WRAP}>'
        f'<h1 style="font-size:20px;color:#1a1a1a;margin:24px 0 10px;line-height:1.5;">'
        f'{name}</h1>'
        + (f'<p style="font-size:15px;line-height:1.9;color:#555;margin:0 0 16px;">'
           f'{emotional}</p>' if emotional else "")
        + seo_block
        + f'</div>'
    )
    return intro_html


def build_detail_html(
    banner_url: str,
    product_img_url: str,
    ai: dict,
    detail_img_url: str = "",
    product_name: str = "",
) -> str:
    """
    6-타입 스마트스토어 표준 상세페이지 (SEO/GEO 강화)
    ① SEO/GEO 텍스트 인트로 — 검색 노출 최적화
    ② 메인 배너 이미지
    ③ 실제 사용컷 + 이 상품을 선택해야 하는 이유
    ④ 이런 분께 추천 체크리스트
    ⑤ 디테일/기능컷 + 스펙 테이블
    ⑥ 비교/인증 + 배송/공지 고정
    """
    # ① SEO/GEO 텍스트 인트로 (네이버 텍스트 인덱싱용 — 맨 앞)
    sec0 = _build_seo_text_section(ai, product_name)

    # ② 메인 인트로 배너 (이미지 강제 860px — 고화질 배너는 항상 full width)
    sec1 = (
        f'<img src="{banner_url}" {_IMG_FIXED}>'
        f'<div {_WRAP}>'
        + (f'<h2 style="font-size:22px;color:#1a1a1a;margin:20px 0 8px;line-height:1.5;">'
           f'{ai.get("headline","")}</h2>' if ai.get("headline") else "")
        + '</div>'
    ) if banner_url else ""

    # ③ 실제 사용컷 (라이프스타일) + 이유 3가지
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
        f'<img src="{product_img_url}" {_IMG_FIXED}>'
        f'<div {_WRAP}>'
        f'<h3 style="font-size:17px;border-left:4px solid #1a73e8;padding-left:12px;margin:20px 0 14px;">'
        f'✅ 이 상품을 선택해야 하는 이유</h3>'
        + reason_blocks + '</div>'
    ) if product_img_url else ""

    # ④ 이런 분께 추천 체크리스트
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

    # ⑤ 디테일/기능 설명컷 + 스펙 테이블
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
    # 디테일 이미지: 원본 비율 유지 (작은 이미지 강제 확대 금지)
    sec4 = (
        (f'<img src="{detail_img_url}" {_IMG}>' if detail_img_url else "")
        + (f'<div {_WRAP}>{spec_html}</div>' if spec_html else "")
    )

    # ⑥ 비교/인증 + 배송/공지 고정
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
        '· 배송비 3,000원 / 도서산간 추가비 발생 가능<br><br>'
        '<b style="color:#333;">🔄 교환/반품 안내</b><br>'
        '· 수령 후 7일 이내 교환/반품 가능<br>'
        '· 단순 변심 반품 시 왕복 배송비 고객 부담<br>'
        '· 불량/오배송 시 무료 교환/반품</div>'
    )
    sec5 = f'<div {_WRAP}>{compare_html}</div>' + delivery_html

    # ⑦ GEO FAQ — ChatGPT·Perplexity·Google AI Overview 노출용 Q&A
    geo_faq = ai.get("geo_faq") or []
    if geo_faq:
        qa_html = "".join(
            f'<div style="border-bottom:1px solid #e8eaf6;padding:14px 0;">'
            f'<p style="font-size:14px;font-weight:700;color:#1a73e8;margin:0 0 6px;">Q. {qa.get("q","")}</p>'
            f'<p style="font-size:14px;color:#444;line-height:1.7;margin:0;">A. {qa.get("a","")}</p>'
            f'</div>'
            for qa in geo_faq[:3] if qa.get("q") and qa.get("a")
        )
        sec6 = (
            f'<div {_WRAP}>'
            f'<div style="background:#f3f4ff;border-radius:12px;padding:20px 18px;margin:16px 0;">'
            f'<h4 style="font-size:15px;color:#3949ab;margin:0 0 12px;">🤖 자주 묻는 질문 (AI 검색 최적화)</h4>'
            f'{qa_html}</div></div>'
        )
    else:
        sec6 = ""

    html = sec0 + sec1 + sec2 + sec3 + sec4 + sec6 + sec5

    # 모든 섹션이 비어있으면 최소 폴백 HTML 강제 생성
    if not html.strip():
        name = ai.get("product_name", product_name or "상품")
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


# ─── Claude HTML 상세페이지 생성 ──────────────────────────────────────────────
async def generate_claude_html_detail(product: dict, ai: dict, image_urls: list) -> str:
    """Claude Haiku로 19섹션 완전한 HTML 상세페이지 생성 (최소 5000자, 3회 재시도).
    실패 시 빈 문자열 반환 → 호출부에서 build_detail_html 폴백."""
    if not ANTHROPIC_API_KEY:
        return ""
    product_name = ai.get("product_name") or str(product.get("name", ""))
    category     = str(product.get("category", ""))
    price        = int(product.get("price", 0))
    main_img     = image_urls[0] if image_urls else ""
    extra_imgs   = [u for u in image_urls[1:6] if u]
    hero_title   = (ai.get("headline") or product_name)[:18]
    sub_title    = ai.get("sub_headline", "")
    emotional    = ai.get("emotional_copy", "")
    selling_pts  = ai.get("selling_points") or ai.get("recommend_list") or []
    reasons      = [ai.get(f"reason_{i}", "") for i in range(1, 4)]
    tags         = ai.get("tags") or []
    gallery_html = "\n".join(
        f'<img src="{u}" style="width:100%;max-width:860px;height:auto;display:block;margin:8px auto;">'
        for u in extra_imgs
    ) or f'<img src="{main_img}" style="width:100%;max-width:860px;height:auto;display:block;margin:0 auto;">'

    features_str = ", ".join(str(s) for s in selling_pts[:5]) or product_name

    prompt = (
        "당신은 한국 프리미엄 이커머스 상세페이지 전문 디자이너입니다.\n"
        "아래 상품 정보로 스마트스토어 상세페이지 HTML을 만들어주세요.\n"
        "HTML만 출력 (마크다운·백틱·주석 없이). 인라인 CSS만 사용. 최소 5000자.\n\n"
        "[디자인 규칙 - 반드시 준수]\n"
        "- 색상: #1a1a1a(다크) / #c8a97a(골드) / #ffffff(흰색) / #f8f5f0(베이지) 4가지만 사용\n"
        "- 그라데이션 절대 금지\n"
        "- 알록달록한 컬러 카드 금지\n"
        "- 폰트: Noto Sans KR (Google Fonts)\n"
        "- 카드/섹션 배경: 흰색 or 연베이지 + 얇은 테두리(#e8e0d0)\n"
        "- 포인트 컬러는 골드(#c8a97a)만 사용\n"
        "- 전체 느낌: 쿠팡/무신사 수준의 프리미엄하고 세련된 스타일\n"
        "- 불필요한 이모지 최소화\n"
        "- 여백 충분히 사용\n\n"
        "[필수 섹션 19개]\n"
        "1. 상단배너 (골드 배경, 빠른배송/정품보장 문구)\n"
        "2. 히어로 (다크 배경, 골드 포인트 텍스트)\n"
        "3. 5초 후킹 (베이지 배경, 임팩트 문구)\n"
        f"4. 메인이미지 <img src=\"{main_img}\" style=\"width:100%;max-width:860px;height:auto;display:block;margin:0 auto;\">\n"
        "5. 핵심수치 4개 (골드 배경 그리드)\n"
        "6. 문제제기 4개 (흰 카드, 골드 왼쪽 테두리)\n"
        "7. 해결책 3개 (다크 배경, 흰 텍스트)\n"
        f"8. 이미지갤러리 (2열 그리드)\n{gallery_html}\n"
        "9. 상세설명1 (좌이미지+우텍스트)\n"
        "10. 상세설명2 (좌다크텍스트+우이미지)\n"
        "11. 사용법 4단계 (다크 배경, STEP 번호)\n"
        "12. 비교표 (골드 헤더)\n"
        "13. 후기 3개 (흰 카드, 별점)\n"
        "14. FAQ 4개 (베이지 배경)\n"
        "15. 스펙표 (베이지 배경 라벨)\n"
        "16. 배송/교환 안내 (2열 카드)\n"
        "17. 신뢰배지 3개 (다크 배경, 빠른배송/정품보장/AS보장 — 무료배송 문구 절대 금지)\n"
        "18. CTA (골드 배경)\n"
        "19. 스토어찜 유도 + 푸터 (다크 배경)\n\n"
        "[상품 정보]\n"
        f"- 상품명: {product_name}\n"
        f"- 가격: ₩{price:,}\n"
        f"- 카테고리: {category}\n"
        f"- 이미지URL: {main_img}\n"
        f"- 특징: {features_str}\n\n"
        "위 규칙을 엄격히 지켜서 완성도 높은 HTML을 생성하세요.\n"
        "width: 860px, Noto Sans KR 반드시 포함."
    )

    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    for attempt in range(3):
        try:
            resp = await client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=8000,
                messages=[{"role": "user", "content": prompt}],
            )
            html = resp.content[0].text.strip()
            html = re.sub(r'^```(?:html)?\n?', '', html)
            html = re.sub(r'\n?```$', '', html)
            if len(html) < 5000:
                print(f"[CLAUDE-HTML] 시도{attempt+1}: {len(html)}자 미달, 재시도", flush=True)
                await asyncio.sleep(1)
                continue
            print(f"[CLAUDE-HTML] ✅ {len(html):,}자 생성 (시도{attempt+1})", flush=True)
            return html
        except Exception as e:
            print(f"[CLAUDE-HTML] 시도{attempt+1} 실패: {e}", flush=True)
            await asyncio.sleep(2)
    print("[CLAUDE-HTML] 3회 실패 → build_detail_html 폴백", flush=True)
    return ""


async def _save_to_obsidian(product_name: str, category: str, detail_html: str,
                            ai: dict, tags: list, channels: dict) -> None:
    """등록 완료 상품 → context_store 저장 + Obsidian Local REST API 시도."""
    try:
        from datetime import datetime as _dt
        date_str  = _dt.now().strftime("%Y-%m-%d")
        now_str   = _dt.now().strftime("%Y-%m-%d %H:%M")
        safe_name = re.sub(r'[\\/:*?"<>|]', '_', product_name[:30])
        path      = f"Products/{category or 'General'}/{date_str}_{safe_name}.md"
        tag_str   = ", ".join(str(t) for t in tags[:10])
        channel_lines = "\n".join(f"- {k}: {v}" for k, v in (channels or {}).items() if v)
        md = (
            f"---\ndate: {date_str}\ncategory: {category}\ntags: [{tag_str}]\n---\n\n"
            f"# {product_name}\n\n"
            f"## 카피\n"
            f"- **히어로**: {ai.get('headline', ai.get('title', ''))}\n"
            f"- **부제목**: {ai.get('sub_headline', ai.get('seo_title', ''))}\n"
            f"- **감성**: {str(ai.get('emotional_copy', ''))[:100]}\n\n"
            f"## 등록 채널 ({now_str})\n{channel_lines}\n\n"
            f"## 태그\n{tag_str}\n\n"
            f"## HTML 상세페이지 (앞 3000자)\n```html\n{detail_html[:3000]}\n```\n"
        )
        _cs_url = os.environ.get("CONTEXT_STORE_URL", "https://loving-serenity-production-2635.up.railway.app")
        async with httpx.AsyncClient(timeout=10) as _c:
            await _c.post(f"{_cs_url}/context", json={
                "key": f"product.{date_str}.{safe_name}",
                "value": md, "category": "product_register",
            })
        _obs_url = os.environ.get("OBSIDIAN_API_URL", "http://127.0.0.1:27123")
        _obs_key = os.environ.get("OBSIDIAN_API_KEY", "fc0baa3f6a6363c3174155ae5a3367bda267fcef7ccfe7e05534c3465600c261")
        try:
            async with httpx.AsyncClient(timeout=10) as _oc:
                r = await _oc.put(
                    f"{_obs_url}/vault/{path}",
                    headers={
                        "Authorization": f"Bearer {_obs_key}",
                        "Content-Type": "text/markdown",
                        "ngrok-skip-browser-warning": "1",
                    },
                    content=md.encode("utf-8"),
                )
                if r.status_code in (200, 204):
                    print(f"[OBSIDIAN] ✅ {path}", flush=True)
                else:
                    print(f"[OBSIDIAN] ⚠️ {r.status_code} {path}", flush=True)
        except Exception as _oe:
            print(f"[OBSIDIAN] 연결 실패: {_oe}", flush=True)
    except Exception as e:
        print(f"[OBSIDIAN] 오류: {e}", flush=True)


async def _update_obsidian_note(results: dict, limit: int = 0) -> None:
    """등록 배치 완료 후 Projects/스마트스토어.md 통계 업데이트."""
    try:
        from datetime import datetime as _dt
        now_str  = _dt.now().strftime("%Y-%m-%d %H:%M")
        date_str = _dt.now().strftime("%Y-%m-%d")
        note_path = "Projects/%EC%8A%A4%EB%A7%88%ED%8A%B8%EC%8A%A4%ED%86%A0%EC%96%B4.md"
        _obs_url = os.environ.get("OBSIDIAN_API_URL", "http://127.0.0.1:27123")
        _obs_key = os.environ.get("OBSIDIAN_API_KEY", "fc0baa3f6a6363c3174155ae5a3367bda267fcef7ccfe7e05534c3465600c261")
        hdrs = {"Authorization": f"Bearer {_obs_key}", "ngrok-skip-browser-warning": "1"}
        async with httpx.AsyncClient(timeout=10) as _oc:
            _r = await _oc.get(f"{_obs_url}/vault/{note_path}", headers=hdrs)
            if _r.status_code != 200:
                return
            content = _r.text
        content = re.sub(r'updated: \S+', f'updated: {date_str}', content)
        content = re.sub(r'last_check: .+', f'last_check: {now_str}', content)
        skip_total = results.get('skip', 0) + results.get('duplicate', 0)
        stats_block = (
            f"## 최근 등록 이력\n"
            f"- **{now_str}**: 배치 {limit}개 → "
            f"✅{results.get('success',0)} "
            f"❌{results.get('fail',0)} "
            f"⊘{skip_total} "
            f"🚫{results.get('ip_blocked',0)}\n"
        )
        if "## 최근 등록 이력" in content:
            content = re.sub(r'## 최근 등록 이력\n.*?(?=\n## |\Z)', stats_block, content, flags=re.DOTALL)
        else:
            content += f"\n\n{stats_block}"
        async with httpx.AsyncClient(timeout=10) as _oc:
            _rw = await _oc.put(
                f"{_obs_url}/vault/{note_path}",
                headers={**hdrs, "Content-Type": "text/markdown"},
                content=content.encode("utf-8"),
            )
            print(f"[OBSIDIAN] 서비스노트 {'✅' if _rw.status_code in (200,204) else '⚠️ '+str(_rw.status_code)}", flush=True)
    except Exception as e:
        print(f"[OBSIDIAN] _update_obsidian_note 오류: {e}", flush=True)


async def _enqueue_retry(channel: str, product: dict, error: str) -> None:
    """등록 실패 상품 → context_store 재시도 큐 저장 (3회 초과 시 텔레그램 알림)."""
    try:
        import time as _time
        cnt = int(product.get("_retry_count", 0)) + 1
        _cs_url = os.environ.get("CONTEXT_STORE_URL", "https://loving-serenity-production-2635.up.railway.app")
        key  = f"retry_queue.{channel}.{int(_time.time())}"
        safe = {k: v for k, v in product.items()
                if isinstance(v, (str, int, float, bool, list, type(None))) and k != "_dg_content"}
        data = {**safe, "_retry_count": cnt, "_retry_error": error[:100]}
        async with httpx.AsyncClient(timeout=8) as _c:
            await _c.post(f"{_cs_url}/context", json={
                "key": key, "value": json.dumps(data, ensure_ascii=False), "category": "retry_queue"
            })
        if cnt >= 3:
            asyncio.create_task(_tg_notify(
                f"[재시도실패] {channel}: {str(product.get('name', ''))[:25]}\n"
                f"사유: {error[:60]}\n3회 모두 실패 — 수동 확인 필요"))
        else:
            print(f"[RETRY-Q] {channel}/{str(product.get('name', ''))[:20]} 큐추가({cnt}회)", flush=True)
    except Exception as e:
        print(f"[RETRY-Q] 큐 추가 실패: {e}", flush=True)


def _validate_copy_fields(ai: dict) -> tuple[bool, str, dict]:
    corrected = dict(ai)
    name = corrected.get("product_name", "")
    if len(name) > 50:
        corrected["product_name"] = name[:47] + "..."
    headline = corrected.get("headline", "")
    if len(headline) > 20:
        corrected["headline"] = headline[:18] + "…"
    tags = corrected.get("tags") or []
    return len(tags) >= 10, f"태그 {len(tags)}개", corrected


_HTML_SECTION_KEYS = [
    "배너", "히어로", "후킹", "수치", "문제", "해결",
    "갤러리", "상세", "사용법", "비교", "후기", "faq",
    "스펙", "배송", "신뢰", "cta", "푸터",
]


def _count_html_sections(html: str) -> int:
    h = html.lower()
    return sum(1 for kw in _HTML_SECTION_KEYS if kw in h)


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
    if not NAVER_SEARCH_CLIENT_ID or not NAVER_SEARCH_CLIENT_SECRET:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://openapi.naver.com/v1/search/shop.json",
                headers={
                    "X-Naver-Client-Id": NAVER_SEARCH_CLIENT_ID,
                    "X-Naver-Client-Secret": NAVER_SEARCH_CLIENT_SECRET,
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


def _extract_naver_keywords(search_results: list) -> list[str]:
    """네이버 쇼핑 상위 결과 타이틀에서 고빈도 키워드 추출."""
    from collections import Counter
    _STOP = {"상품", "제품", "판매", "추천", "무료", "배송", "할인", "특가", "정품",
             "브랜드", "직접", "공식", "국내", "해외", "당일", "빠른", "이상", "이하",
             "구매", "인기", "최신", "신상", "세트", "묶음", "증정", "포함"}
    words = []
    for item in search_results:
        title = re.sub(r"<[^>]+>", "", item.get("title", ""))
        words.extend(re.findall(r"[가-힣]{2,}", title))
    counter = Counter(w for w in words if w not in _STOP)
    return [w for w, _ in counter.most_common(8)]


# ─── DALL-E 하루 생성 개수 제한 ──────────────────────────────────────────────
class _DailyDALLELimit:
    def __init__(self, max_per_day: int):
        self._max = max_per_day
        self._date = None
        self._count = 0

    def allowed(self) -> bool:
        from datetime import date as _d
        today = _d.today()
        if self._date != today:
            self._date, self._count = today, 0
        if self._count >= self._max:
            return False
        self._count += 1
        return True

    @property
    def used(self) -> int:
        return self._count

_dalle_day_limit = _DailyDALLELimit(10)  # 스마트스토어: 하루 최대 10개


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
    if not _dalle_day_limit.allowed():
        print(f"[DALLE] 하루 한도 초과 ({_dalle_day_limit._max}개/일) — 스킵", flush=True)
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


async def generate_gemini_image(product_name: str, category: str = "") -> Optional[bytes]:
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


async def upscale_image(image_url: str, scale: int = 4) -> Optional[bytes]:
    """오너클랜 원본 이미지 업스케일
    1차: Replicate Real-ESRGAN (API 키 있을 때, 실제 AI 업스케일)
    2차: Pillow LANCZOS (폴백, 고품질 리샘플링)
    """
    # 원본 다운로드
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.get(_extract_hq_url(image_url))
            r.raise_for_status()
        raw_bytes = r.content
    except Exception as e:
        print(f"[UPSCALE] 다운로드 실패: {e}", flush=True)
        return None

    # ── 1차: Replicate Real-ESRGAN ────────────────────────────────────────────
    if REPLICATE_API_KEY:
        try:
            import base64 as _b64
            img_data_url = "data:image/jpeg;base64," + _b64.b64encode(raw_bytes).decode()
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.post(
                    "https://api.replicate.com/v1/predictions",
                    headers={"Authorization": f"Token {REPLICATE_API_KEY}",
                             "Content-Type": "application/json"},
                    json={
                        "version": "42fed1c4974146d4d2414e2be2c5277c7fcf05fcc3a73abf41610695738c1d7b",
                        "input": {"image": img_data_url, "scale": scale, "face_enhance": False},
                    },
                )
                r.raise_for_status()
                prediction_id = r.json().get("id")
            # 폴링 최대 90초
            for _ in range(45):
                await asyncio.sleep(2)
                async with httpx.AsyncClient(timeout=15) as c:
                    r = await c.get(
                        f"https://api.replicate.com/v1/predictions/{prediction_id}",
                        headers={"Authorization": f"Token {REPLICATE_API_KEY}"},
                    )
                    data = r.json()
                status = data.get("status")
                if status == "succeeded":
                    output_url = data.get("output")
                    if output_url:
                        async with httpx.AsyncClient(timeout=30) as c:
                            res = await c.get(output_url)
                        print(f"[UPSCALE] Real-ESRGAN ✅ {scale}×", flush=True)
                        return res.content
                    break
                if status in ("failed", "canceled"):
                    print(f"[UPSCALE] Replicate 실패: {data.get('error','')}", flush=True)
                    break
        except Exception as e:
            print(f"[UPSCALE] Replicate 오류: {e}", flush=True)

    # ── 2차: Pillow LANCZOS 폴백 ─────────────────────────────────────────────
    try:
        from PIL import Image
        import io as _io_mod
        img = Image.open(_io_mod.BytesIO(raw_bytes))
        w, h = img.size
        # 4배 업스케일, 최대 4000px 캡
        ratio = min(scale, 4000 / max(w, h, 1))
        new_w, new_h = max(int(w * ratio), w), max(int(h * ratio), h)
        upscaled = img.resize((new_w, new_h), Image.LANCZOS)
        if upscaled.mode != "RGB":
            upscaled = upscaled.convert("RGB")
        buf = _io_mod.BytesIO()
        upscaled.save(buf, format="JPEG", quality=95, optimize=True)
        print(f"[UPSCALE] LANCZOS ✅ {w}×{h} → {new_w}×{new_h}", flush=True)
        return buf.getvalue()
    except Exception as e:
        print(f"[UPSCALE] Pillow 오류: {e}", flush=True)

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
    사유: "ok" | "too_small" | "text_heavy" | "blurry" | "error"
    """
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(_extract_hq_url(image_url))
            r.raise_for_status()
        from PIL import Image, ImageFilter, ImageStat
        img = Image.open(_io.BytesIO(r.content))
        w, h = img.size
        if h > w * 2.5:
            return False, "text_heavy", w, h
        if min(w, h) < 300:
            return False, "too_small", w, h
        # Laplacian 근사 blur 체크 (PIL만 사용)
        edges = img.convert("L").filter(ImageFilter.FIND_EDGES)
        sharpness = ImageStat.Stat(edges).stddev[0]
        if sharpness < 8.0:
            return False, "blurry", w, h
        return True, "ok", w, h
    except Exception:
        return True, "error", 0, 0  # 체크 실패 시 원본 그대로 사용


async def get_product_image(p: dict) -> str | None:
    """
    이미지 우선순위:
    1. 오너클랜 원본 → 업스케일(Real-ESRGAN/LANCZOS) → QC 95점+
    2. Pexels 실사진 QC 75점+
    3. Gemini 생성 QC 75점+
    4. Flux 2 Pro QC 95점+ (원본 없을 때만)
    5. DALL-E (최후 수단, 미달이라도 사용)
    업스케일 QC 미달 시: 원본 폴백 없이 2순위부터 진행
    모든 소스 실패 시: 원본 이미지 마지막 보험
    """
    image_url    = str(p.get("image", "")).strip()
    product_name = str(p.get("name", ""))
    category     = str(p.get("category", ""))
    _, reject_kws = _get_scene_context(product_name)
    from employees import employee_pexels_qc, employee_image_inspector

    # ── 1. 오너클랜 원본 → 업스케일 → 직원19 하이브리드 배너 → QC 95점+ ──────────
    if image_url.startswith("http"):
        ok, reason, w, h = await _check_image_quality(image_url)
        if reason == "blurry":
            print(f"[IMAGE] ⛔ 흐린 이미지 — AI 대체 없이 상품 스킵: {product_name[:20]}", flush=True)
            return None
        if ok:
            print(f"[IMAGE] 업스케일 시도: {product_name[:20]} ({w}×{h})", flush=True)
            upscaled = await upscale_image(image_url)
            if upscaled:
                # ── 1a. 직원19: rembg 배경제거 + AI배경 합성 ─────────────────────
                from employees import employee_hybrid_banner
                print(f"[IMAGE] 직원19 하이브리드 배너 시도", flush=True)
                try:
                    hybrid_bytes = await employee_hybrid_banner(
                        upscaled, product_name, category,
                        gemini_key=GOOGLE_AI_API_KEY,
                        openai_key=OPENAI_API_KEY,
                        flux_key=FLUX_API_KEY,
                    )
                    if hybrid_bytes:
                        hybrid_result = await naver_api.upload_raw_image(hybrid_bytes)
                        qc = await employee_image_inspector(
                            hybrid_result, product_name, ANTHROPIC_API_KEY,
                            reject_keywords=reject_kws)
                        score = qc.get("score", 0)
                        print(f"[IMAGE] 하이브리드 QC: {score}점", flush=True)
                        if score >= 95:
                            print(f"[IMAGE] 하이브리드 배너 ✅", flush=True)
                            return hybrid_result
                        print(f"[IMAGE] 하이브리드 {score}점 미달 → 일반 업스케일", flush=True)
                except Exception as e:
                    print(f"[IMAGE] 직원19 실패: {e}", flush=True)

                # ── 1b. 일반 업스케일 QC ──────────────────────────────────────────
                try:
                    up_result = await naver_api.upload_raw_image(upscaled)
                    qc = await employee_image_inspector(
                        up_result, product_name, ANTHROPIC_API_KEY, reject_keywords=reject_kws)
                    score = qc.get("score", 0)
                    print(f"[IMAGE] 업스케일 QC: {score}점", flush=True)
                    if score >= 95:
                        print(f"[IMAGE] 업스케일 ✅", flush=True)
                        return up_result
                    print(f"[IMAGE] 업스케일 {score}점 미달 → Pexels", flush=True)
                except Exception as e:
                    print(f"[IMAGE] 업스케일 업로드 실패: {e}", flush=True)
        else:
            print(f"[IMAGE] 오너클랜 품질 불량({reason} {w}×{h})", flush=True)

    # ── 2. Pexels QC 75점+ ───────────────────────────────────────────────────
    print(f"[IMAGE] Pexels 검색: {product_name[:20]}", flush=True)
    pexels_url = await search_pexels_image(product_name)
    if pexels_url:
        try:
            qc = await employee_pexels_qc(pexels_url, product_name, ANTHROPIC_API_KEY)
            score = qc.get("score", 0)
            print(f"[IMAGE] Pexels QC {score}점 — {qc.get('reason','')}", flush=True)
            if score >= 75:
                result = await naver_api.upload_image(pexels_url)
                print(f"[IMAGE] Pexels ✅", flush=True)
                return result
            print(f"[IMAGE] Pexels {score}점 미달 → Gemini", flush=True)
        except Exception as e:
            print(f"[IMAGE] Pexels 실패: {e}", flush=True)

    # ── 3. Gemini QC 75점+ ───────────────────────────────────────────────────
    print(f"[IMAGE] Gemini 생성: {product_name[:20]}", flush=True)
    gemini_raw = await generate_gemini_image(product_name, category)
    if gemini_raw:
        try:
            gemini_result = await naver_api.upload_raw_image(gemini_raw)
            qc = await employee_image_inspector(
                gemini_result, product_name, ANTHROPIC_API_KEY, reject_keywords=reject_kws)
            score = qc.get("score", 0)
            print(f"[IMAGE] Gemini QC {score}점", flush=True)
            if score >= 75:
                print(f"[IMAGE] Gemini ✅", flush=True)
                return gemini_result
            print(f"[IMAGE] Gemini {score}점 미달 → Flux", flush=True)
        except Exception as e:
            print(f"[IMAGE] Gemini 실패: {e}", flush=True)

    # ── 4. Flux QC 95점+ (원본 없을 때만) ────────────────────────────────────
    if not image_url.startswith("http"):
        print(f"[IMAGE] Flux 생성(원본 없음): {product_name[:20]}", flush=True)
        flux_url = await generate_flux_image(product_name, category)
        if flux_url:
            try:
                flux_result = await naver_api.upload_image(flux_url)
                qc = await employee_image_inspector(
                    flux_result, product_name, ANTHROPIC_API_KEY, reject_keywords=reject_kws)
                score = qc.get("score", 0)
                print(f"[IMAGE] Flux QC {score}점", flush=True)
                if score >= 95:
                    print(f"[IMAGE] Flux ✅", flush=True)
                    return flux_result
                print(f"[IMAGE] Flux {score}점 미달 → DALL-E", flush=True)
            except Exception as e:
                print(f"[IMAGE] Flux 실패: {e}", flush=True)

    # ── 5. DALL-E (최후 수단) ─────────────────────────────────────────────────
    print(f"[IMAGE] DALL-E 생성: {product_name[:20]}", flush=True)
    dalle_url = await generate_dalle_image(product_name)
    if dalle_url:
        try:
            dalle_result = await naver_api.upload_image(dalle_url)
            qc = await employee_image_inspector(
                dalle_result, product_name, ANTHROPIC_API_KEY, reject_keywords=reject_kws)
            score = qc.get("score", 0)
            print(f"[IMAGE] DALL-E QC {score}점", flush=True)
            if score >= 95:
                print(f"[IMAGE] DALL-E ✅", flush=True)
            else:
                print(f"[IMAGE] DALL-E {score}점 미달이나 최후 수단 사용", flush=True)
            return dalle_result
        except Exception as e:
            print(f"[IMAGE] DALL-E 실패: {e}", flush=True)

    # ── 보험: 원본 이미지라도 사용 ───────────────────────────────────────────
    if image_url.startswith("http"):
        try:
            result = await naver_api.upload_image(image_url)
            print(f"[IMAGE] 원본 보험 사용 ✅", flush=True)
            return result
        except Exception as e:
            print(f"[IMAGE] 원본 보험 실패: {e}", flush=True)

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
    _before_sourcing = len(products)
    products = await employee_sourcing_manager(products, limit, ANTHROPIC_API_KEY)
    print(f"[소싱팀장] 선별: {len(products)}개", flush=True)
    try:
        from server import _log_employee
        _log_employee("소싱팀장", last_result=f"{_before_sourcing}개 → {len(products)}개 선별")
    except Exception:
        pass

    # ② 시즌 기획자: 현재 시즌 파악
    season_data = employee_season_planner()
    season_info = season_data["upcoming"][0]["event"] if season_data["upcoming"] else ""
    if season_info:
        print(f"[시즌기획자] 현재 시즌: {season_info}", flush=True)
    try:
        from server import _log_employee
        _log_employee("시즌기획자", upcoming_events=[e["event"] for e in season_data["upcoming"][:3]])
    except Exception:
        pass

    # ③ 트렌드 스카우터: 구글 트렌딩 키워드 수집 (실패 시 빈 리스트로 계속)
    try:
        trend_keywords = await employee_trend_scout()
    except Exception as _te:
        print(f"[트렌드스카우터] 실패(무시): {_te}", flush=True)
        trend_keywords = []
    print(f"[트렌드스카우터] 키워드 {len(trend_keywords)}개 수집", flush=True)
    try:
        from server import _log_employee
        _log_employee("트렌드스카우터", keywords=trend_keywords[:10])
    except Exception:
        pass

    # ③a 네이버 패션 트렌드 (ratio≥15.0) — 카테고리 매핑 및 AI 카피 강화
    try:
        from employees import employee_naver_fashion_trend_scout
        fashion_trends = await employee_naver_fashion_trend_scout(
            NAVER_DATALAB_CLIENT_ID,
            NAVER_DATALAB_CLIENT_SECRET,
            ratio_threshold=15.0,
        )
    except Exception as _fe:
        print(f"[패션트렌드] 실패(무시): {_fe}", flush=True)
        fashion_trends = []
    print(f"[패션트렌드] 핫 키워드 {len(fashion_trends)}개 (ratio≥15.0)", flush=True)

    registered_codes = load_registered_codes()
    registered_names = load_registered_names()
    print(f"[총괄] 기등록 상품: {len(registered_codes)}개(코드) / {len(registered_names)}개(이름) 제외", flush=True)

    results = {"success": 0, "fail": 0, "skip": 0, "duplicate": 0, "ip_blocked": 0, "errors": []}
    for p in products[:limit]:
        try:
            # 중복 체크: 코드 OR 정규화된 상품명
            code = str(p.get("code", ""))
            name_norm = _normalize_name(str(p.get("name", "")))
            if (code and code in registered_codes) or (name_norm and name_norm in registered_names):
                results["duplicate"] += 1
                continue

            # ④ IP 감시관: 상표권 위험 체크
            safe, danger_kw = employee_ip_guardian(p)
            if not safe:
                print(f"[IP감시관] 차단: {p.get('name','')} — {danger_kw}", flush=True)
                results["ip_blocked"] += 1
                try:
                    from server import _log_employee
                    _log_employee("IP감시관", blocked=1)
                except Exception:
                    pass
                continue

            # ⑤ 리뷰 분석가: Pain Point 분석
            review = await employee_review_analyst(str(p.get("name", "")), ANTHROPIC_API_KEY)

            # 네이버 쇼핑 키워드 수집 (상품명 최적화 + 가격 최적화 겸용)
            competitor_prices = await search_naver_shopping(str(p.get("name", "")))
            naver_keywords = _extract_naver_keywords(competitor_prices)
            if naver_keywords:
                print(f"[키워드최적화] {p.get('name','')[:15]} → {naver_keywords[:3]}", flush=True)

            # ⑥ 상품 설명 작가: 전 직원 데이터 통합해서 설명 생성
            context = {
                "season": season_info,
                "trends": trend_keywords[:5],
                "fashion_trends": fashion_trends[:5],
                "pain_points": review.get("pain_points", []),
                "selling_points": review.get("selling_points", []),
                "naver_keywords": naver_keywords,
            }
            ai = await generate_product_copy(p, context)

            # Tool 3: SEO 태그 생성
            seo_tags = await employee_tag_generator(
                str(p.get("name", "")), str(p.get("category", "")),
                review.get("selling_points", []), ANTHROPIC_API_KEY)
            ai["tags"] = seo_tags
            print(f"[태그생성] {seo_tags[:3]}...", flush=True)

            # 경쟁사 가격 최적화 (위에서 수집한 competitor_prices 재사용)
            price_result = await employee_price_optimizer(
                str(p.get("name", "")), str(p.get("category", "")),
                int(p.get("price", 0)), ANTHROPIC_API_KEY,
                competitor_prices=competitor_prices)
            price = price_result["suggested_price"]
            print(f"[가격최적화] {price:,}원 — {price_result.get('reason','')}", flush=True)

            # ⑦ 등록 전 필수 데이터 검증 (name + price 없으면 절대 등록 금지)
            final_name = (ai.get("product_name") or "").strip() or str(p.get("name", "")).strip()
            if not final_name or price <= 0:
                print(f"[검증] SKIP — 상품명/가격 없음: name={final_name!r} price={price}", flush=True)
                results["skip"] += 1
                continue

            # ⑧ 이미지 디렉터: 메인 이미지
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
            detail_html = build_detail_html(banner_url, naver_img_url, ai, detail_img_url,
                                            product_name=str(p.get("name", "")))

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

            payload = build_product_payload(p, ai, price, tags=ai.get("tags"), hot_trends=fashion_trends)
            payload["originProduct"]["images"]["representativeImage"]["url"] = naver_img_url
            if detail_html:
                payload["originProduct"]["detailContent"] = detail_html

            await naver_api.register_product(payload)
            save_registered_code(code)
            save_registered_name(ai.get("product_name") or p.get("name", ""))
            results["success"] += 1
            print(f"[총괄] ✅ {ai.get('product_name', p.get('name',''))} ({price:,}원)", flush=True)
            await asyncio.sleep(0.5)

        except Exception as e:
            results["fail"] += 1
            results["errors"].append(str(e))
            print(f"[총괄] ❌ {e}", flush=True)

    print(f"[총괄] 완료 — 성공:{results['success']} 실패:{results['fail']} 스킵:{results['skip']} IP차단:{results['ip_blocked']}", flush=True)
    return results


async def pipeline_register_from_domeggook(
    limit: int = 10,
    keywords: Optional[List[str]] = None,
    min_price: int = 3000,
    max_price: int = 150000,
    start_page: int = 0,
) -> dict:
    """도매꾹 API 소싱 → 전 직원 협업 등록 파이프라인.
    소싱부분만 Excel→도매꾹 API로 교체; 이후 로직은 pipeline_register_products와 동일."""
    from employees import (
        employee_sourcing_manager, employee_ip_guardian,
        employee_season_planner, employee_trend_scout, employee_review_analyst,
        employee_price_optimizer, employee_tag_generator,
    )
    print(f"[도매꾹파이프라인] 시작 — limit={limit}", flush=True)

    # ── 1000개 한도 체크 ──
    _cur_sale = await naver_api.count_sale_products()
    if _cur_sale >= 1000:
        msg = f"[도매꾹파이프라인] 등록 한도 도달({_cur_sale}/1000) — 등록 건너뜀"
        print(msg, flush=True)
        return {"status": "limit_reached", "current": _cur_sale, "limit": 1000, "message": msg}
    limit = min(limit, 1000 - _cur_sale)
    print(f"[도매꾹파이프라인] 현재 {_cur_sale}개, 이번 최대 {limit}개 등록", flush=True)

    # ① 도매꾹 API 상품 수집 (pool = limit * 3 으로 sourcing manager 선별 여유)
    products = await fetch_domeggook_products(
        keywords, pool_size=limit * 3,
        min_price=min_price, max_price=max_price, start_page=start_page
    )
    if not products:
        return {"status": "error", "message": "도매꾹 상품 없음 — API 키/키워드 확인"}

    # ② 소싱팀장 선별
    products = await employee_sourcing_manager(products, limit, ANTHROPIC_API_KEY)
    print(f"[소싱팀장] {len(products)}개 선별", flush=True)

    season_data  = employee_season_planner()
    season_info  = season_data["upcoming"][0]["event"] if season_data["upcoming"] else ""
    try:
        trend_keywords = await employee_trend_scout()
    except Exception as _te:
        print(f"[트렌드스카우터] 실패(무시): {_te}", flush=True)
        trend_keywords = []

    try:
        from employees import employee_naver_fashion_trend_scout
        fashion_trends = await employee_naver_fashion_trend_scout(
            NAVER_DATALAB_CLIENT_ID, NAVER_DATALAB_CLIENT_SECRET, ratio_threshold=15.0
        )
    except Exception as _fe:
        print(f"[패션트렌드] 실패(무시): {_fe}", flush=True)
        fashion_trends = []
    print(f"[패션트렌드] 핫 키워드 {len(fashion_trends)}개 (ratio≥15.0)", flush=True)

    registered_codes = load_registered_codes()
    registered_names = load_registered_names()
    print(f"[도매꾹파이프라인] 기등록: {len(registered_codes)}개(코드) / {len(registered_names)}개(이름) 제외", flush=True)
    _proc_total = len(products[:limit])
    _proc_n = 0
    results = {"success": 0, "fail": 0, "skip": 0, "duplicate": 0,
               "ip_blocked": 0, "errors": [], "source": "domeggook"}

    for p in products[:limit]:
        try:
            _proc_n += 1
            code = str(p.get("code", ""))
            name_norm = _normalize_name(str(p.get("name", "")))
            print(f"\n[STEP1] ({_proc_n}/{_proc_total}) 소싱검증: {str(p.get('name',''))[:30]}", flush=True)
            # ─── STEP 1: 소싱 검증 ───
            if (code and code in registered_codes) or (name_norm and name_norm in registered_names):
                results["duplicate"] += 1
                continue

            safe, danger_kw = employee_ip_guardian(p)
            if not safe:
                print(f"[IP감시관] 차단: {p.get('name','')} — {danger_kw}", flush=True)
                results["ip_blocked"] += 1
                continue

            review = await employee_review_analyst(str(p.get("name", "")), ANTHROPIC_API_KEY)
            context = {
                "season": season_info,
                "trends": trend_keywords[:5],
                "fashion_trends": fashion_trends[:5],
                "pain_points": review.get("pain_points", []),
                "selling_points": review.get("selling_points", []),
            }
            # ─── STEP 2: 카피/SEO 검증 (3회 재시도) ───
            print(f"[STEP2] ({_proc_n}/{_proc_total}) 카피/SEO 검증", flush=True)
            _copy_ok = False
            ai = {}
            for _copy_attempt in range(3):
                ai = await generate_product_copy(p, context)
                ai["tags"] = await employee_tag_generator(
                    str(p.get("name", "")), str(p.get("category", "")),
                    review.get("selling_points", []), ANTHROPIC_API_KEY)
                _copy_ok, _copy_msg, ai = _validate_copy_fields(ai)
                if _copy_ok:
                    break
                print(f"[STEP2] 재시도 {_copy_attempt+1}/3 — {_copy_msg}", flush=True)
            if not _copy_ok:
                _tags = ai.get("tags") or []
                _base_tags = ["좋은상품", "추천상품", "베스트상품", "인기상품",
                              "가성비", "프리미엄", "고품질", "특가", "한정수량", "당일배송"]
                while len(_tags) < 10:
                    _tags.append(_base_tags[len(_tags) % len(_base_tags)])
                ai["tags"] = _tags[:10]
                print(f"[STEP2] 태그 기본값 보완 → {len(ai['tags'])}개", flush=True)

            competitor_prices = await search_naver_shopping(str(p.get("name", "")))
            price_result = await employee_price_optimizer(
                str(p.get("name", "")), str(p.get("category", "")),
                int(p.get("price", 0)), ANTHROPIC_API_KEY,
                competitor_prices=competitor_prices)
            price = price_result["suggested_price"]

            _cat         = str(p.get("category", ""))
            naver_img_url = await get_product_image(p)
            if not naver_img_url:
                print(f"[이미지] SKIP: {p.get('name','')}", flush=True)
                results["skip"] += 1
                continue

            # 추가 이미지 네이버 CDN 업로드 (500px+, 50KB+ 필터)
            _extra_naver_urls: list[str] = []
            _extra_raw = p.get("_dg_extra_images") or []
            if _extra_raw:
                async with httpx.AsyncClient(timeout=15, follow_redirects=True) as _ec:
                    for _eu in _extra_raw[:9]:
                        try:
                            _er = await _ec.get(_eu)
                            if _er.status_code != 200 or len(_er.content) < 50 * 1024:
                                continue
                            from PIL import Image as _PIM
                            _im = _PIM.open(_io.BytesIO(_er.content))
                            if min(_im.size) < 500:
                                continue
                            _eu_naver = await naver_api.upload_detail_image(_er.content)
                            _extra_naver_urls.append(_eu_naver)
                        except Exception:
                            pass
                p["_dg_extra_naver_urls"] = _extra_naver_urls
                asyncio.create_task(_tg_notify(
                    f"[이미지파싱] {p.get('name','')[:30]} - {len(_extra_naver_urls)}장 추출"))

            headline_txt = ai.get("headline") or ai.get("banner_text") or p.get("name", "")[:18]
            dalle_banner_raw = await generate_dalle_banner(str(p.get("name", "")), headline_txt, _cat)
            if dalle_banner_raw:
                banner_url = await naver_api.upload_image(dalle_banner_raw, is_banner=True)
            else:
                banner_url = await create_banner_image(
                    naver_img_url, headline_txt,
                    ai.get("sub_headline") or ai.get("sub_text", ""))

            # 도매꾹 상세 설명 이미지 → Naver URL로 교체 (공급사 실제 스펙 이미지)
            dg_content_html = await _dg_content_to_naver_html(str(p.get("_dg_content", "")))

            # ─── STEP 3: HTML 생성 및 검증 ───
            print(f"[STEP3] ({_proc_n}/{_proc_total}) HTML 생성 및 검증", flush=True)
            _all_imgs = [naver_img_url] + (p.get("_dg_extra_naver_urls") or [])
            claude_html = await generate_claude_html_detail(p, ai, [u for u in _all_imgs if u])
            _html_ok = bool(claude_html) and len(claude_html) >= 5000 and _count_html_sections(claude_html) >= 12
            if not _html_ok and claude_html:
                _sec_cnt = _count_html_sections(claude_html)
                print(f"[STEP3] HTML 재생성 시도 (길이:{len(claude_html)}, 섹션:{_sec_cnt}/17)", flush=True)
                _claude_html2 = await generate_claude_html_detail(p, ai, [u for u in _all_imgs if u])
                if _claude_html2 and len(_claude_html2) >= 5000 and _count_html_sections(_claude_html2) >= 12:
                    claude_html = _claude_html2
                    _html_ok = True
            if _html_ok:
                detail_html = claude_html
                if dg_content_html:
                    detail_html += f'\n<div style="margin-top:24px;">{dg_content_html}</div>'
                    print(f"[상세페이지] 도매꾹 상세 이미지 추가 ✅", flush=True)
            else:
                detail_img_url = ""
                if not dg_content_html:
                    dalle_detail_raw = await generate_dalle_detail_shot(
                        str(p.get("name", "")), ai.get("spec_hint", ""), _cat)
                    if dalle_detail_raw:
                        try:
                            detail_img_url = await naver_api.upload_image(dalle_detail_raw)
                        except Exception:
                            pass
                detail_html = build_detail_html(banner_url, naver_img_url, ai, detail_img_url,
                                                product_name=str(p.get("name", "")))
                if dg_content_html:
                    detail_html += f'\n<div style="margin-top:24px;">{dg_content_html}</div>'
                    print(f"[상세페이지] 도매꾹 상세 이미지 추가 ✅ (폴백모드)", flush=True)
                print(f"[STEP3] HTML 폴백 사용 (길이:{len(detail_html)})", flush=True)

            _, reject_kws = _get_scene_context(str(p.get("name", "")))
            qc_result = await run_qc_pipeline(
                naver_img_url, str(p.get("name", "")),
                detail_html, ANTHROPIC_API_KEY, reject_kws)
            print(f"[QC] 단계:{qc_result['stage']} 통과:{qc_result['passed']}", flush=True)

            if not qc_result["passed"]:
                if qc_result["stage"] == 2:
                    retry_raw = await generate_dalle_image(
                        f"{p.get('name','')} {qc_result.get('retry_prompt','')}".strip(), _cat)
                    if retry_raw:
                        retry_img = await naver_api.upload_image(retry_raw)
                        qc2 = await run_qc_pipeline(
                            retry_img, str(p.get("name", "")),
                            detail_html, ANTHROPIC_API_KEY, reject_kws)
                        if qc2["passed"]:
                            naver_img_url = retry_img
                        else:
                            results["fail"] += 1
                            results["errors"].append(f"{p.get('name','?')[:20]}: QC재시도실패")
                            continue
                    else:
                        results["fail"] += 1
                        results["errors"].append(f"{p.get('name','?')[:20]}: DALLE재생성실패")
                        continue
                else:
                    results["fail"] += 1
                    results["errors"].append(f"{p.get('name','?')[:20]}: QC{qc_result['stage']}단계실패")
                    continue

            payload = build_product_payload(p, ai, price, tags=ai.get("tags"), hot_trends=fashion_trends)
            payload["originProduct"]["images"]["representativeImage"]["url"] = naver_img_url
            if _extra_naver_urls:
                payload["originProduct"]["images"]["optionalImages"] = [{"url": u} for u in _extra_naver_urls[:9]]
            if detail_html:
                payload["originProduct"]["detailContent"] = detail_html

            # ─── STEP 4: 등록 + Obsidian 저장 + 진행률 알림 ───
            print(f"[STEP4] ({_proc_n}/{_proc_total}) 등록 시작", flush=True)
            reg_result = await naver_api.register_product(payload)
            if code:
                save_registered_code(code)
            final_name = ai.get("product_name") or p.get("name", "")
            save_registered_name(final_name)
            save_registered_name(p.get("name", ""))  # 도매꾹 원본명도 저장 — STEP1 name dedup 키와 일치 (memory: domeggook-dedup-broken)
            results["success"] += 1
            _pid = (reg_result.get("originProductNo")
                    or reg_result.get("channelProducts", [{}])[0].get("channelProductNo", "")
                    if isinstance(reg_result, dict) else "")
            _product_url = (f"https://smartstore.naver.com/thehwmall/products/{_pid}"
                            if _pid else "https://smartstore.naver.com/thehwmall")
            print(f"[도매꾹파이프라인] ✅ {final_name} ({price:,}원) → {_product_url}", flush=True)
            asyncio.create_task(_tg_notify(f"[{_proc_n}/{_proc_total}] {final_name} ✅\n{_product_url}"))
            asyncio.create_task(_save_to_obsidian(
                final_name, _cat, detail_html, ai, ai.get("tags") or [],
                {"smartstore": _product_url}))
            await asyncio.sleep(0.5)

        except Exception as e:
            results["fail"] += 1
            err_msg = str(e)[:80]
            results["errors"].append(f"{str(p.get('name','?'))[:20]}: {err_msg}")
            print(f"[도매꾹파이프라인] ❌ {e}", flush=True)
            asyncio.create_task(_enqueue_retry("smartstore", p, err_msg))

    print(f"[도매꾹파이프라인] 완료 — 성공:{results['success']} 실패:{results['fail']} "
          f"스킵:{results['skip']} IP차단:{results['ip_blocked']}", flush=True)
    asyncio.create_task(_tg_notify(
        f"[스마트스토어 등록완료]\n"
        f"✅ 성공: {results['success']}개\n"
        f"❌ 실패: {results['fail']}개\n"
        f"⊘ 스킵: {results['skip']}개\n"
        f"🚫 IP차단: {results['ip_blocked']}개"))
    asyncio.create_task(_update_obsidian_note(results, limit))
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


async def pipeline_fix_products(
    limit: int = 50,
    fix_images: bool = True,
    fix_descriptions: bool = True,
    batch_size: int = 5,
) -> dict:
    """등록된 상품 이미지·설명 일괄 수정 파이프라인.
    - 이미지: 도매꾹 상품(_img_760 재업로드) 또는 Pexels 검색 대체
    - 설명: Claude 전문 판매자 스타일로 재생성
    batch_size: 한 번에 처리할 상품 수 (Naver API 제한 준수)
    """
    from employees import (
        employee_pexels_qc, employee_image_inspector, employee_season_planner,
    )

    results = {
        "total": 0, "image_fixed": 0, "description_fixed": 0,
        "updated": 0, "skipped": 0, "errors": [], "products": [],
    }
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

    # 1. 전체 상품 목록 수집 (페이지 순회)
    all_products: list[dict] = []
    page = 1
    while True:
        try:
            resp = await _retry(
                lambda p=page: naver_api.list_products(page=p, size=20),
                retries=3, delay=5.0, label=f"fix list(p{page})"
            )
        except Exception as e:
            print(f"[FIX] 목록 조회 실패(p{page}): {e}", flush=True)
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        print(f"[FIX] p{page} 로드 — 누적 {len(all_products)}개", flush=True)
        if len(contents) < 20:
            break
        page += 1
        await asyncio.sleep(1.0)

    results["total"] = len(all_products)
    print(f"[FIX] 전체 {len(all_products)}개 로드 완료", flush=True)

    processed = 0
    for prod in all_products:
        if processed >= limit:
            break

        origin      = prod.get("originProduct", {})
        product_id  = str(prod.get("originProductNo", ""))
        name        = origin.get("name", "").strip()
        status      = origin.get("statusType", "")
        seller_code = (origin.get("sellerCodeInfo") or {}).get("sellerManagementCode", "")
        category    = (origin.get("detailAttribute") or {}).get("naverShoppingSearchInfo", {}).get("categoryName", "")

        if not product_id or not name:
            results["skipped"] += 1
            continue
        if status not in ("SALE", "SUSPENSION"):
            results["skipped"] += 1
            continue

        processed += 1
        prod_log = {"id": product_id, "name": name[:30]}
        update_payload: dict = {}
        print(f"[FIX] [{processed}/{limit}] {name[:30]}", flush=True)

        # ── 이미지 수정 ─────────────────────────────────────────────────────
        if fix_images:
            new_img_url: str | None = None

            # 현재 대표 이미지 URL 확인
            raw_img_node = (origin.get("images") or {}).get("representativeImage") or {}
            existing_img = raw_img_node.get("url", "") if isinstance(raw_img_node, dict) else ""
            has_760 = "_img_760" in existing_img
            no_image = not existing_img

            # 교체 필요한 경우만 처리: _img_760 감지 or 이미지 없음
            if has_760 or no_image:
                print(f"  [IMG] {'_img_760 저화질 감지' if has_760 else '이미지 없음'} → 교체 시도", flush=True)

                # 1순위: 도매꾹 상품 → API로 원본 이미지
                if seller_code.startswith("DG_"):
                    dg_no = seller_code[3:]
                    try:
                        detail = await _dg_item_detail(dg_no)
                        thumb_obj = detail.get("thumb", {})
                        orig_url = thumb_obj.get("original") or thumb_obj.get("large") or ""
                        if orig_url:
                            new_img_url = await naver_api.upload_image(orig_url)
                            print(f"  [IMG] 도매꾹 원본 업로드 ✅", flush=True)
                    except Exception as e:
                        print(f"  [IMG] 도매꾹 재업로드 실패: {e}", flush=True)

                # 2순위: _img_760 URL 직접 치환 (CDN URL에서 _img_760 제거 → 원본)
                if not new_img_url and has_760:
                    hires_url = existing_img.replace("_img_760", "")
                    try:
                        new_img_url = await naver_api.upload_image(hires_url)
                        print(f"  [IMG] _img_760 제거 → 고화질 ✅", flush=True)
                    except Exception as e:
                        print(f"  [IMG] URL 치환 실패: {e}", flush=True)

                # 3순위: Pexels (_img_760 감지 시 QC 60, 이미지 없음 시 QC 70)
                if not new_img_url:
                    pexels_url = await search_pexels_image(name)
                    if pexels_url:
                        try:
                            qc = await employee_pexels_qc(pexels_url, name, ANTHROPIC_API_KEY)
                            threshold = 60 if has_760 else 70
                            score = qc.get("score", 0)
                            if score >= threshold:
                                new_img_url = await naver_api.upload_image(pexels_url)
                                print(f"  [IMG] Pexels 대체 (score={score}) ✅", flush=True)
                            else:
                                print(f"  [IMG] Pexels QC 미달 (score={score}<{threshold})", flush=True)
                        except Exception as e:
                            print(f"  [IMG] Pexels 실패: {e}", flush=True)
            else:
                prod_log["image"] = "ok"
                print(f"  [IMG] 정상 이미지 — 스킵", flush=True)

            if new_img_url:
                update_payload["images"] = {
                    "representativeImage": {"url": new_img_url}
                }
                results["image_fixed"] += 1
                prod_log["image"] = "fixed"
            elif has_760 or no_image:
                prod_log["image"] = "unfixed"

        # ── 설명 재생성 ──────────────────────────────────────────────────────
        if fix_descriptions:
            try:
                p_dict = {"name": name, "category": category, "price": origin.get("salePrice", 0), "code": seller_code}
                season_data = employee_season_planner()
                season_info = season_data["upcoming"][0]["event"] if season_data["upcoming"] else ""
                context = {"season": season_info, "trends": [], "pain_points": [], "selling_points": []}
                ai = await generate_product_copy(p_dict, context)

                # 기존 이미지 URL: origin → 없으면 빈 문자열로 대체
                raw_img_node  = (origin.get("images") or {}).get("representativeImage") or {}
                existing_img  = raw_img_node.get("url", "") if isinstance(raw_img_node, dict) else ""
                banner_src    = (update_payload.get("images", {}).get("representativeImage", {}).get("url", "")
                                 or existing_img)

                # origin 구조 디버그 로그
                print(f"  [DESC] origin keys={list(origin.keys())[:6]}", flush=True)
                print(f"  [DESC] banner_src={'있음' if banner_src else '없음'}", flush=True)

                detail_html   = build_detail_html(banner_src, banner_src, ai, "",
                                                  product_name=name)
                print(f"  [DESC] html len={len(detail_html)}", flush=True)

                if detail_html:
                    update_payload["detailContent"] = detail_html
                    results["description_fixed"] += 1
                    prod_log["description"] = "fixed"
                else:
                    # build_detail_html이 빈 문자열 → 섹션 모두 비어있음 (banner_src 없음)
                    # fallback: 최소 텍스트 설명이라도 업데이트
                    minimal_html = (
                        f"<div style='padding:16px;font-family:sans-serif;'>"
                        f"<h2>{ai.get('headline','')}</h2>"
                        f"<p>{ai.get('emotional_copy','')}</p></div>"
                    )
                    if minimal_html.strip() != "<div style='padding:16px;font-family:sans-serif;'><h2></h2><p></p></div>":
                        update_payload["detailContent"] = minimal_html
                        results["description_fixed"] += 1
                        prod_log["description"] = "minimal"
                    else:
                        prod_log["description"] = "no_content"
            except Exception as e:
                err_msg = f"{name[:20]} 설명오류: {str(e)[:120]}"
                print(f"  [DESC] {err_msg}", flush=True)
                results["errors"].append(err_msg)

        # ── Naver API 업데이트 (전체 payload merge 방식) ────────────────────
        if update_payload:
            try:
                # Naver PUT은 partial update 불가 → origin 전체 + 변경분 merge
                _READONLY = {"originProductNo", "channelProductNo", "regDate",
                             "modDate", "statusFrom", "totalSalesQuantity"}
                full_payload = {k: v for k, v in origin.items() if k not in _READONLY}
                full_payload.update(update_payload)   # 이미지/설명 덮어씌우기

                ok, err_msg = await naver_api.update_product(product_id, full_payload)
                prod_log["updated"] = ok
                if ok:
                    results["updated"] += 1
                    print(f"  [UPDATE] ✅ {product_id}", flush=True)
                else:
                    print(f"  [UPDATE] ❌ {product_id}: {err_msg}", flush=True)
                    results["errors"].append(f"{name[:20]}: {err_msg}")
            except Exception as e:
                results["errors"].append(f"{name[:20]}: {str(e)[:60]}")

        results["products"].append(prod_log)
        await asyncio.sleep(1.5)   # Naver API 속도 제한 준수

    print(f"[FIX] 완료 — 이미지:{results['image_fixed']} 설명:{results['description_fixed']} 스킵:{results['skipped']}", flush=True)
    return results


async def pipeline_reapply_claude_html() -> dict:
    """Noto Sans KR 미적용 상품 전체에 Claude HTML 19섹션 재적용.
    1개씩 순차 처리, 매 처리 후 텔레그램, 실패 시 즉시 중단."""
    results = {"success": 0, "failed": 0, "skipped": 0, "total": 0, "stopped_at": ""}

    # 1. 전체 상품 수집
    all_products: list[dict] = []
    page = 1
    while True:
        try:
            resp = await _retry(
                lambda p=page: naver_api.list_products(page=p, size=50, days=1000),
                retries=3, delay=5.0, label=f"reapply list(p{page})"
            )
        except Exception as e:
            print(f"[REAPPLY] 목록 조회 실패(p{page}): {e}", flush=True)
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        print(f"[REAPPLY] p{page} 로드 — 누적 {len(all_products)}개", flush=True)
        if len(contents) < 50:
            break
        page += 1
        await asyncio.sleep(1.0)

    # 2. 미적용 상품 필터 (이름 있고 Noto Sans KR 없는 것)
    not_applied: list[dict] = []
    for item in all_products:
        origin = item.get("originProduct", {})
        detail = origin.get("detailContent") or ""
        name = (origin.get("name") or "").strip()
        if not name or "Noto Sans KR" in detail:
            continue
        not_applied.append(item)

    results["total"] = len(not_applied)
    print(f"[REAPPLY] 미적용 {len(not_applied)}개 확인 — 재적용 시작", flush=True)
    await _tg_notify(
        f"[HTML 재적용 시작]\n\n미적용 상품: {len(not_applied)}개\n"
        "Claude HTML 19섹션 순차 적용 시작합니다."
    )

    _READONLY = {"originProductNo", "channelProductNo", "regDate",
                 "modDate", "statusFrom", "totalSalesQuantity"}

    # 3. 1개씩 순차 처리
    for idx, item in enumerate(not_applied, 1):
        origin = item.get("originProduct", {})
        product_id = str(item.get("originProductNo", ""))
        name = (origin.get("name") or "").strip()
        price = int(origin.get("salePrice", 0))
        category = ((origin.get("detailAttribute") or {})
                    .get("naverShoppingSearchInfo", {})
                    .get("categoryName", ""))
        channel_products = origin.get("channelProducts", [])
        channel_no = (channel_products[0].get("channelProductNo", "")
                      if channel_products else "")
        product_url = (f"https://smartstore.naver.com/thehwmall/products/{channel_no}"
                       if channel_no else "https://smartstore.naver.com/thehwmall")

        # 이미지 URL 수집
        raw_img = (origin.get("images") or {}).get("representativeImage") or {}
        main_img = raw_img.get("url", "") if isinstance(raw_img, dict) else ""
        optional_imgs = (origin.get("images") or {}).get("optionalImages") or []
        extra_imgs = [
            (oi.get("url", "") if isinstance(oi, dict) else "")
            for oi in optional_imgs
        ]
        image_urls = [u for u in [main_img] + extra_imgs if u]

        product_dict = {"name": name, "category": category, "price": price}

        print(f"[REAPPLY] [{idx}/{len(not_applied)}] {name[:40]}", flush=True)

        # AI copy 생성
        try:
            ai_copy = await generate_product_copy(product_dict, {})
        except Exception as e:
            msg = f"[재적용 실패] [{idx}/{len(not_applied)}] {name[:30]}\n오류: AI 카피 생성 실패\n{str(e)[:120]}\n\n작업 중단."
            print(f"[REAPPLY] {msg}", flush=True)
            await _tg_notify(msg)
            results["failed"] += 1
            results["stopped_at"] = f"{idx}/{len(not_applied)} — {name[:30]}"
            break

        # Claude HTML 19섹션 생성
        html = await generate_claude_html_detail(product_dict, ai_copy, image_urls)
        if not html or "Noto Sans KR" not in html:
            msg = (f"[재적용 실패] [{idx}/{len(not_applied)}] {name[:30]}\n"
                   f"오류: HTML 생성 실패 (Noto Sans KR 미포함)\n\n작업 중단.")
            print(f"[REAPPLY] {msg}", flush=True)
            await _tg_notify(msg)
            results["failed"] += 1
            results["stopped_at"] = f"{idx}/{len(not_applied)} — {name[:30]}"
            break

        # Naver API 업데이트 (full payload merge)
        full_payload = {k: v for k, v in origin.items() if k not in _READONLY}
        full_payload["detailContent"] = html

        try:
            ok, err_msg = await naver_api.update_product(product_id, full_payload)
        except Exception as e:
            ok, err_msg = False, str(e)[:120]

        if not ok:
            msg = (f"[재적용 실패] [{idx}/{len(not_applied)}] {name[:30]}\n"
                   f"오류: Naver API 업데이트 실패\n{err_msg}\n\n작업 중단.")
            print(f"[REAPPLY] {msg}", flush=True)
            await _tg_notify(msg)
            results["failed"] += 1
            results["stopped_at"] = f"{idx}/{len(not_applied)} — {name[:30]}"
            break

        results["success"] += 1
        print(f"[REAPPLY] [{idx}/{len(not_applied)}] ✅ {name[:40]}", flush=True)
        await _tg_notify(f"[재적용 {idx}/{len(not_applied)}] {name[:40]} ✅")
        await asyncio.sleep(2.5)

    # 4. 최종 요약
    if not results["stopped_at"]:
        summary = (
            f"[HTML 재적용 완료]\n\n"
            f"✅ 성공: {results['success']}개\n"
            f"❌ 실패: {results['failed']}개\n"
            f"📊 전체 목표: {results['total']}개"
        )
    else:
        summary = (
            f"[HTML 재적용 중단]\n\n"
            f"✅ 성공: {results['success']}개\n"
            f"❌ 실패: {results['failed']}개\n"
            f"🛑 중단 위치: {results['stopped_at']}"
        )
    print(f"[REAPPLY] {summary}", flush=True)
    await _tg_notify(summary)
    return results


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

    # 전체 상품 수집 (페이지 순회) — 네트워크 오류는 재시도, 실제 빈 페이지만 종료
    all_products = []
    page = 1
    consecutive_empty = 0
    while True:
        try:
            resp = await _retry(
                lambda p=page: naver_api.list_products(page=p, size=100),
                retries=3, delay=5.0, label=f"cleanup list_products(p{page})"
            )
        except Exception as e:
            results["errors"].append(f"상품 목록 조회 실패(p{page}): {e}")
            print(f"[CLEANUP] p{page} 3회 재시도 실패 — 수집 종료", flush=True)
            break
        contents = resp.get("contents", [])
        if not contents:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            continue
        consecutive_empty = 0
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


# ──────────────────────────────────────────────────────────────
# 기존 상품 SEO/GEO/HS코드 일괄 업데이트
# ──────────────────────────────────────────────────────────────
_GEO_MARKER = "geo-summary"

async def update_existing_products_seo(limit: int = 100, skip_has_geo: bool = True) -> dict:
    """스마트스토어 기존 상품에 GEO FAQ·HS코드·통관정보 일괄 업데이트."""
    results = {"updated": 0, "skipped": 0, "failed": 0, "total": 0}
    print(f"[SEO-UPDATE] 스마트스토어 최대 {limit}개 업데이트 시작", flush=True)

    # 상품 수집 (list_products는 size 50이므로 페이지 순회)
    all_items: list[dict] = []
    page = 1
    while len(all_items) < limit:
        resp = await naver_api.list_products(page=page, size=50, days=3650)
        contents = resp.get("contents", [])
        if not contents:
            break
        all_items.extend(contents)
        if len(contents) < 50:
            break
        page += 1

    all_items = all_items[:limit]
    results["total"] = len(all_items)
    print(f"[SEO-UPDATE] 수집 완료 {len(all_items)}개", flush=True)

    for item in all_items:
        product_no = str(item.get("originProductNo", ""))
        origin = item.get("originProduct", {})
        if not origin or not product_no:
            results["skipped"] += 1
            continue

        detail_content = origin.get("detailContent", "") or ""
        if skip_has_geo and _GEO_MARKER in detail_content:
            results["skipped"] += 1
            continue

        try:
            stub = {
                "name": origin.get("name", ""),
                "price": origin.get("salePrice", 0),
                "category": "",
            }
            copy = await generate_product_copy(stub)
            if not copy:
                results["failed"] += 1
                continue

            # GEO FAQ HTML 블록
            geo_faq = copy.get("geo_faq", [])
            if geo_faq:
                faq_html = '<div class="geo-summary" style="background:#f9f9f9;padding:12px;margin-bottom:16px;border-radius:6px">'
                for qa in geo_faq[:3]:
                    q = qa.get("q", "")
                    a = qa.get("a", "")
                    if q and a:
                        faq_html += f"<p><strong>{q}</strong><br>{a}</p>"
                faq_html += "</div>"
                new_detail = faq_html + detail_content
            else:
                new_detail = detail_content

            # 수정할 originProduct (전체 교체)
            updated_origin = dict(origin)
            updated_origin["detailContent"] = new_detail

            # 검색 태그 업데이트
            tags = copy.get("tags", [])
            if tags:
                tag_list = [{"tagName": t} for t in tags[:10] if t]
                detail_attr = dict(updated_origin.get("detailAttribute", {}))
                search_tag_info = dict(detail_attr.get("searchTagInfo", {}))
                search_tag_info["searchTagList"] = tag_list
                detail_attr["searchTagInfo"] = search_tag_info
                updated_origin["detailAttribute"] = detail_attr

            ok, err = await naver_api.update_product(product_no, updated_origin)
            if ok:
                results["updated"] += 1
                print(f"  ✓ [{product_no}] {stub['name'][:30]}", flush=True)
            else:
                results["failed"] += 1
                print(f"  ✗ [{product_no}] {err[:80]}", flush=True)
            await asyncio.sleep(0.6)
        except Exception as e:
            print(f"  ✗ [{product_no}] {e}", flush=True)
            results["failed"] += 1

    print(f"[SEO-UPDATE] 스마트스토어 완료 — {results}", flush=True)
    return results


# ══════════════════════════════════════════════════════════════════════════════
# 소싱 고도화 v2 — ①블루오션 ②SEO ③성과정리 ④가격경쟁 ⑤리뷰모니터 ⑥카테고리
# ══════════════════════════════════════════════════════════════════════════════

async def _tg_notify(msg: str) -> None:
    """텔레그램 범용 알림 (실패 무시)."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not bot_token or not chat_id:
        return
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            await c.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": chat_id, "text": msg[:4000]},
            )
    except Exception:
        pass


# ─── ① 블루오션 필터 ─────────────────────────────────────────────────────────

async def _get_datalab_trend_score(keyword: str) -> float:
    """네이버 데이터랩 검색 트렌드 점수 (0~100). 키 없거나 실패 시 50 반환."""
    cid  = NAVER_DATALAB_CLIENT_ID
    csec = NAVER_DATALAB_CLIENT_SECRET
    if not cid or not csec:
        return 50.0
    from datetime import date as _date, timedelta as _td
    today = _date.today()
    body = {
        "startDate": (today - _td(days=30)).strftime("%Y-%m-%d"),
        "endDate":   today.strftime("%Y-%m-%d"),
        "timeUnit":  "week",
        "keywordGroups": [{"groupName": keyword[:20], "keywords": [keyword[:20]]}],
        "device": "", "ages": [], "gender": "",
    }
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(
                "https://openapi.naver.com/v1/datalab/search",
                headers={"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec,
                         "Content-Type": "application/json"},
                json=body,
            )
        if r.status_code == 200:
            data = r.json().get("results", [])
            if data and data[0].get("data"):
                return float(data[0]["data"][-1].get("ratio", 50))
    except Exception:
        pass
    return 50.0


async def _get_blue_ocean_score(keyword: str) -> float:
    """블루오션 점수 = 검색 트렌드 / (경쟁상품수 + 1) × 10"""
    trend, shopping = await asyncio.gather(
        _get_datalab_trend_score(keyword),
        search_naver_shopping(keyword[:15], display=20),
        return_exceptions=True,
    )
    trend_val = float(trend) if isinstance(trend, (int, float)) else 50.0
    comp_cnt  = len(shopping) if isinstance(shopping, list) else 10
    return round(trend_val / (comp_cnt + 1) * 10, 2)


async def _rank_products_blue_ocean(products: list[dict]) -> list[dict]:
    """블루오션 점수 산출 후 내림차순 정렬 반환. 실패 시 원본 순서 유지."""
    if not products:
        return products
    try:
        names   = [p.get("name", "")[:15] for p in products]
        unique  = list(dict.fromkeys(names))
        scores  = await asyncio.gather(*[_get_blue_ocean_score(n) for n in unique], return_exceptions=True)
        smap    = {n: (float(s) if isinstance(s, (int, float)) else 50.0) for n, s in zip(unique, scores)}
        for p in products:
            p["_blue_ocean_score"] = smap.get(p.get("name", "")[:15], 50.0)
        ranked = sorted(products, key=lambda x: x.get("_blue_ocean_score", 0), reverse=True)
        top3   = [(p.get("name", "")[:12], p.get("_blue_ocean_score", 0)) for p in ranked[:3]]
        print(f"[블루오션] TOP3: {top3}", flush=True)
        return ranked
    except Exception as e:
        print(f"[블루오션] 스코어링 실패 — 원본 순서: {e}", flush=True)
        return products


# ─── ③ 성과 기반 자동 교체 ──────────────────────────────────────────────────

async def _source_replacement_product() -> None:
    """저성과 삭제 후 1개 신규 소싱 대체. 실패 무시."""
    try:
        products = await fetch_domeggook_products(pool_size=5, min_price=3000, max_price=50000)
        if not products:
            return
        p     = products[0]
        copy  = await generate_product_copy(p, {})
        price = calculate_selling_price(int(p.get("price", 10000)))
        pload = build_product_payload(copy, price, p.get("image", ""), "", p)
        res   = await naver_api.register_product(pload)
        if res:
            save_registered_code(str(p.get("code", "")))
            print("[PERF] 소싱대체 완료 ✅", flush=True)
    except Exception as e:
        print(f"[PERF] 소싱대체 실패(무시): {e}", flush=True)


async def _run_advanced_performance_cleanup() -> dict:
    """7일 0클릭 → SUSPENSION, 14일 <10클릭+0주문 → DELETE + 소싱대체 트리거."""
    print("[PERF-CLEANUP] 고도화 성과 정리 시작", flush=True)
    now     = datetime.now(timezone.utc)
    results = {"suspended": 0, "deleted": 0, "replaced": 0, "checked": 0, "errors": []}

    all_products: list[dict] = []
    page = 1
    while True:
        try:
            resp = await _retry(
                lambda p=page: naver_api.list_products(page=p, size=100),
                retries=3, delay=5.0, label=f"perf list(p{page})"
            )
        except Exception as e:
            results["errors"].append(f"목록조회: {str(e)[:60]}")
            break
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        if len(contents) < 100:
            break
        page += 1

    for prod in all_products:
        try:
            origin = prod.get("originProduct", {})
            if origin.get("statusType") != "SALE":
                continue
            product_no = str(prod.get("originProductNo", ""))
            channel_no = str(prod.get("channelProductNo", ""))
            name       = origin.get("name", "")[:20]
            reg_str    = origin.get("regDate", "")
            if not reg_str or not product_no:
                continue
            reg_date = datetime.fromisoformat(reg_str.replace("Z", "+00:00"))
            days_old = (now - reg_date).days
            if days_old < 7:
                continue
            results["checked"] += 1

            insight = await naver_api.get_product_insight(channel_no, days=14)
            clicks  = int((insight or {}).get("clickCount") or 0)
            orders  = int((insight or {}).get("orderCount") or 0)

            if days_old >= 7 and clicks == 0 and orders == 0:
                ok = await naver_api.set_product_status(product_no, "SUSPENSION")
                if ok:
                    results["suspended"] += 1
                    print(f"[PERF] 중지 {name} | {days_old}일/{clicks}클릭", flush=True)
            elif days_old >= 14 and clicks < 10 and orders == 0:
                ok = await naver_api.delete_product(product_no)
                if ok:
                    results["deleted"] += 1
                    print(f"[PERF] 삭제 {name} | {days_old}일/{clicks}클릭", flush=True)
                    asyncio.create_task(_source_replacement_product())
                    results["replaced"] += 1
        except Exception as e:
            results["errors"].append(str(e)[:60])
        await asyncio.sleep(0.4)

    if results["suspended"] + results["deleted"] > 0:
        msg = (f"[스마트스토어 성과정리]\n검사:{results['checked']} "
               f"중지:{results['suspended']} 삭제:{results['deleted']} 대체:{results['replaced']}")
        asyncio.create_task(_tg_notify(msg))
    print(f"[PERF-CLEANUP] 완료 — {results}", flush=True)
    return results


# ─── ④ 가격 경쟁 자동 조정 ──────────────────────────────────────────────────

async def _run_price_competition_update(limit: int = 30) -> dict:
    """경쟁 최저가 × 1.10 초과 시 최저가 × 1.05 로 자동 인하 (원가 × 1.15 바닥)."""
    print("[PRICE] 가격 경쟁 자동 조정 시작", flush=True)
    results = {"checked": 0, "adjusted": 0, "skipped": 0, "errors": []}

    all_products: list[dict] = []
    page = 1
    while len(all_products) < limit:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        if len(contents) < 50:
            break
        page += 1
    all_products = all_products[:limit]

    for prod in all_products:
        try:
            origin    = prod.get("originProduct", {})
            if origin.get("statusType") != "SALE":
                continue
            product_no = str(prod.get("originProductNo", ""))
            name       = origin.get("name", "")
            our_price  = int(origin.get("salePrice") or 0)
            cost_price = int(origin.get("costPrice") or 0)
            if our_price <= 0:
                continue
            results["checked"] += 1

            competitors = await search_naver_shopping(name[:20], display=10)
            prices = [c["price"] for c in (competitors or []) if c.get("price", 0) > 0]
            if not prices:
                results["skipped"] += 1
                continue
            min_price = min(prices)

            if our_price <= min_price * 1.10:
                results["skipped"] += 1
                continue

            target    = round(min_price * 1.05 / 10) * 10
            floor     = round(cost_price * 1.15 / 10) * 10 if cost_price > 0 else 0
            new_price = max(target, floor)
            if new_price <= 0 or new_price >= our_price:
                results["skipped"] += 1
                continue

            full = dict(origin)
            full["salePrice"] = new_price
            ok, err = await naver_api.update_product(product_no, full)
            if ok:
                results["adjusted"] += 1
                print(f"[PRICE] ✅ {name[:20]} {our_price:,}→{new_price:,}원 (경쟁최저:{min_price:,})", flush=True)
            else:
                results["errors"].append(f"{name[:15]}: {err[:40]}")
        except Exception as e:
            results["errors"].append(str(e)[:50])
        await asyncio.sleep(1.2)

    print(f"[PRICE] 완료 — {results}", flush=True)
    return results


# ─── ⑤ 리뷰·위시리스트 모니터링 ────────────────────────────────────────────

_REVIEW_NEG_KW = ["불량", "파손", "오배송", "사기", "환불", "취소", "실망", "별로", "최악", "쓰레기"]


async def _run_review_wishlist_monitor(limit: int = 20) -> dict:
    """위시리스트 급증(전일 대비 1.5배+5개) 감지, 부정 키워드 경쟁사 리뷰 모니터링."""
    print("[REVIEW] 리뷰·위시리스트 모니터링 시작", flush=True)
    results = {"checked": 0, "wishlist_surges": [], "neg_alerts": [], "errors": []}
    prev_key  = "smartstore.wishlist.prev"
    prev_data: dict = _ctx_get(prev_key) or {}

    all_products: list[dict] = []
    page = 1
    while len(all_products) < limit:
        resp = await naver_api.list_products(page=page, size=50)
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        if len(contents) < 50:
            break
        page += 1
    all_products = all_products[:limit]

    new_data: dict = {}
    for prod in all_products:
        try:
            origin    = prod.get("originProduct", {})
            if origin.get("statusType") != "SALE":
                continue
            product_no = str(prod.get("originProductNo", ""))
            channel_no = str(prod.get("channelProductNo", ""))
            name       = origin.get("name", "")[:20]
            results["checked"] += 1

            insight  = await naver_api.get_product_insight(channel_no, days=7)
            wishlist = int((insight or {}).get("wishlistCount") or (insight or {}).get("keepCount") or 0)
            new_data[product_no] = wishlist

            prev_w = prev_data.get(product_no, 0)
            if prev_w > 0 and wishlist >= prev_w * 1.5 and wishlist - prev_w >= 5:
                results["wishlist_surges"].append(f"📈 {name}: {prev_w}→{wishlist}")

            shopping = await search_naver_shopping(name[:15], display=5)
            for item in (shopping or []):
                title = item.get("title", "").lower()
                negs  = [k for k in _REVIEW_NEG_KW if k in title]
                if negs:
                    results["neg_alerts"].append(f"⚠️ {name}: {negs}")
                    break
        except Exception as e:
            results["errors"].append(str(e)[:50])
        await asyncio.sleep(0.5)

    _ctx_set(prev_key, new_data)

    alerts = results["wishlist_surges"] + results["neg_alerts"]
    if alerts:
        asyncio.create_task(_tg_notify("[리뷰·위시리스트]\n" + "\n".join(alerts[:10])))
    print(f"[REVIEW] 완료 — 급증:{len(results['wishlist_surges'])} 부정:{len(results['neg_alerts'])}", flush=True)
    return results


# ─── ⑥ 카테고리 다각화 분석 ─────────────────────────────────────────────────

async def _run_category_diversity_check() -> dict:
    """카테고리 분포 분석 — 30% 초과 또는 10개 미만 시 텔레그램 경고."""
    from collections import Counter as _Counter
    print("[CATEGORY] 카테고리 다각화 분석 시작", flush=True)
    results = {"total": 0, "categories": {}, "warnings": []}

    all_products: list[dict] = []
    page = 1
    while True:
        resp = await naver_api.list_products(page=page, size=100, days=3650)
        contents = resp.get("contents", [])
        if not contents:
            break
        all_products.extend(contents)
        if len(contents) < 100:
            break
        page += 1

    total = len(all_products)
    results["total"] = total
    if total == 0:
        return results

    cats = []
    for prod in all_products:
        origin = prod.get("originProduct", {})
        cat = (
            (origin.get("detailAttribute") or {})
            .get("naverShoppingSearchInfo", {})
            .get("category1Name", "미분류")
        ) or "미분류"
        cats.append(cat)

    counter  = _Counter(cats)
    results["categories"] = dict(counter.most_common(20))
    warnings = []
    for cat, cnt in counter.most_common():
        if cnt / total * 100 > 30:
            warnings.append(f"⚠️ '{cat}' {cnt/total*100:.1f}% ({cnt}개) — 30% 초과")
    if len(counter) < 10:
        warnings.append(f"⚠️ 카테고리 {len(counter)}개 — 목표: 10개 이상")
    results["warnings"] = warnings

    if warnings:
        top3 = ", ".join(f"{c}:{n}" for c, n in counter.most_common(3))
        msg  = f"[카테고리 분석] 총{total}개/{len(counter)}카테고리\nTop3: {top3}\n" + "\n".join(warnings)
        asyncio.create_task(_tg_notify(msg))
    print(f"[CATEGORY] {len(counter)}카테고리, 경고:{len(warnings)}", flush=True)
    return results


# ─── 주간 성과 요약 (매주 월요일 09:00) ────────────────────────────────────

async def _send_weekly_performance_summary() -> None:
    """스마트스토어 주간 성과 요약 텔레그램 발송."""
    print("[WEEKLY] 주간 성과 요약 시작", flush=True)
    try:
        resp     = await naver_api.list_products(page=1, size=50, days=3650)
        total    = resp.get("totalElements", len(resp.get("contents", [])))
        contents = resp.get("contents", [])[:10]

        top_items: list[dict] = []
        for prod in contents:
            origin = prod.get("originProduct", {})
            if origin.get("statusType") != "SALE":
                continue
            ch_no = str(prod.get("channelProductNo", ""))
            if not ch_no:
                continue
            ins = await naver_api.get_product_insight(ch_no, days=7)
            if ins:
                top_items.append({
                    "name":   origin.get("name", "")[:20],
                    "clicks": int(ins.get("clickCount") or 0),
                    "orders": int(ins.get("orderCount") or 0),
                })
        top_items.sort(key=lambda x: x["clicks"], reverse=True)

        lines = [
            "📊 스마트스토어 주간 리포트",
            f"📦 총 상품: {total}개",
            "",
            "🏆 주간 TOP 클릭:",
        ]
        for i, it in enumerate(top_items[:3], 1):
            lines.append(f"  {i}. {it['name']} — 클릭:{it['clicks']} 주문:{it['orders']}")
        if not top_items:
            lines.append("  (데이터 없음)")
        asyncio.create_task(_tg_notify("\n".join(lines)))
        print("[WEEKLY] 발송 완료", flush=True)
    except Exception as e:
        print(f"[WEEKLY] 실패: {e}", flush=True)
