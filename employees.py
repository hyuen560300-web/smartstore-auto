"""
스마트스토어 AI 직원단
총괄팀장: Claude
"""
import asyncio
import json
import re
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import anthropic
import httpx


# ─── 직원 1: 소싱팀장 ────────────────────────────────────────────────────────
async def employee_sourcing_manager(products: list, limit: int, anthropic_key: str) -> list:
    """Excel 상품 중 잘 팔릴 상품 선별"""
    filtered = [p for p in products if (
        str(p.get("image", "")).startswith("http") and
        p.get("price", 0) > 0
    )]
    if len(filtered) <= limit:
        return filtered

    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    sample = filtered[:min(100, len(filtered))]
    product_list = [
        {"idx": i, "name": p.get("name", ""), "price": p.get("price", 0), "category": p.get("cat_large", "")}
        for i, p in enumerate(sample)
    ]
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        system=[{"type": "text", "text": "당신은 이커머스 상품 소싱 전문가입니다. 반드시 JSON 배열만 출력하세요.", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"""
스마트스토어에서 잘 팔릴 상품 {limit}개의 idx를 골라주세요.
기준: 트렌디함, 합리적 가격대, 검색 수요 높은 카테고리
JSON 배열만 출력: [0, 3, 7, ...]

{json.dumps(product_list, ensure_ascii=False)}
"""}]
    )
    try:
        indices = json.loads(resp.content[0].text.strip())
        return [sample[i] for i in indices if i < len(sample)][:limit]
    except Exception:
        return filtered[:limit]


# ─── 직원 2: IP 감시관 ───────────────────────────────────────────────────────
DANGEROUS_KEYWORDS = [
    "나이키", "아디다스", "구찌", "루이비통", "샤넬", "프라다", "발렌시아가",
    "애플", "삼성갤럭시", "소니", "디즈니", "마블", "BTS", "블랙핑크",
    "스타벅스", "맥도날드", "무인양품", "유니클로",
    "nike", "adidas", "gucci", "louis vuitton", "chanel", "apple", "disney", "sony"
]

def employee_ip_guardian(product: dict) -> tuple:
    """상표권 위험 감지. 반환: (안전여부, 위험키워드)"""
    name = str(product.get("name", "")).lower()
    for kw in DANGEROUS_KEYWORDS:
        if kw.lower() in name:
            return False, kw
    return True, ""


# ─── 직원 3: 시즌 기획자 ─────────────────────────────────────────────────────
SEASON_CALENDAR = [
    (1, 1,  "신정",        ["새해선물", "달력", "플래너"]),
    (1, 25, "설날",        ["명절선물", "한과", "전통차"]),
    (2, 14, "발렌타인",    ["초콜릿", "커플선물"]),
    (3, 14, "화이트데이",  ["사탕", "커플선물"]),
    (5, 5,  "어린이날",    ["장난감", "어린이선물", "완구"]),
    (5, 8,  "어버이날",    ["카네이션", "부모님선물", "건강식품"]),
    (6, 20, "여름",        ["수영복", "선크림", "냉감의류", "모자"]),
    (7, 15, "장마",        ["우산", "방수용품", "제습제"]),
    (9, 15, "추석",        ["명절선물세트", "한과", "홍삼"]),
    (10,31, "핼러윈",      ["코스튬", "파티용품"]),
    (11,11, "빼빼로데이",  ["빼빼로", "선물세트"]),
    (12, 1, "크리스마스",  ["크리스마스선물", "트리", "파티용품"]),
]

def employee_season_planner() -> dict:
    """현재 날짜 기준 60일 내 시즌 이벤트 분석"""
    today = date.today()
    upcoming = []
    for month, day, event, keywords in SEASON_CALENDAR:
        try:
            event_date = date(today.year, month, day)
            if event_date < today:
                event_date = date(today.year + 1, month, day)
            days_left = (event_date - today).days
            if 0 <= days_left <= 60:
                upcoming.append({
                    "event": event,
                    "date": str(event_date),
                    "days_left": days_left,
                    "keywords": keywords,
                    "urgency": "🚨긴급" if days_left <= 14 else "📅준비중"
                })
        except ValueError:
            pass
    upcoming.sort(key=lambda x: x["days_left"])
    return {"today": str(today), "upcoming": upcoming}


# ─── 직원 4: 트렌드 스카우터 ────────────────────────────────────────────────
async def employee_trend_scout() -> list:
    """Google Trends 한국 실시간 트렌딩 키워드 수집"""
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                "https://trends.google.com/trends/trendingsearches/daily/rss",
                params={"geo": "KR"},
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            )
            if r.status_code == 200:
                root = ET.fromstring(r.text)
                keywords = []
                for item in root.findall(".//item")[:20]:
                    title = item.find("title")
                    if title is not None and title.text:
                        keywords.append(title.text)
                return keywords
    except Exception as e:
        print(f"[TREND] 수집 실패: {e}", flush=True)
    return []


# ─── 직원 4b: 네이버 쇼핑인사이트 패션 트렌드 스카우터 ──────────────────────
# 네이버 DataLab 쇼핑인사이트 — 패션의류 카테고리 ID
_FASHION_CATEGORY_ID = "50000000"

# 매 배치 최대 5개 키워드 (API 제한)
FASHION_KEYWORD_MASTER: list[str] = [
    # 상의
    "티셔츠", "블라우스", "니트", "후드티", "맨투맨", "셔츠", "탑", "조끼",
    # 하의
    "청바지", "슬랙스", "반바지", "스커트", "레깅스", "트레이닝바지",
    # 아우터
    "자켓", "코트", "패딩", "바람막이", "가디건", "점퍼", "트렌치코트",
    # 원피스/세트
    "원피스", "투피스", "정장", "수트",
    # 핏/스타일
    "오버핏", "슬림핏", "루즈핏", "크롭", "롱",
    # 소재
    "린넨", "데님", "쉬폰", "면티", "폴리에스터",
    # 트렌드
    "Y2K", "레트로", "미니멀", "캐주얼", "포멀",
    # 시즌
    "봄신상", "여름신상", "가을신상", "겨울신상",
    # 성별/연령
    "여성의류", "남성의류", "20대패션", "30대패션",
]

# 패션 카테고리 감지 키워드 (상품 category 문자열과 매칭)
FASHION_CATEGORY_KEYWORDS = [
    "패션", "의류", "옷", "패딩", "자켓", "코트", "티셔츠", "청바지",
    "원피스", "블라우스", "니트", "후드", "스커트", "슬랙스",
]


# 주간 패션 트렌드 캐시 (job_fashion_trend_update → pipeline에서 재활용)
_naver_fashion_trend_cache: list[str] = []


def get_cached_fashion_trends() -> list[str]:
    """가장 최근 주간 업데이트된 네이버 패션 트렌드 핫 키워드 반환."""
    return list(_naver_fashion_trend_cache)


async def employee_naver_fashion_trend_scout(
    datalab_client_id: str,
    datalab_client_secret: str,
    top_n: int = 20,
    ratio_threshold: float = 15.0,
) -> list[str]:
    """네이버 쇼핑인사이트 API로 패션의류 트렌딩 키워드 수집.

    API: POST https://openapi.naver.com/v1/datalab/shopping/category/keywords
    필요 권한: 네이버 개발자센터 > 쇼핑인사이트 (NAVER_DATALAB_CLIENT_ID/SECRET)
    ratio_threshold: 이 값 이상인 키워드만 반환 (기본 15.0 — 핫 트렌드 기준)
    결과 없을 때: 빈 리스트 반환 (파이프라인 중단 없음)
    """
    if not datalab_client_id or not datalab_client_secret:
        print("[패션트렌드] DataLab 키 미설정 — 스킵", flush=True)
        return []

    end_date = date.today()
    # 월간(30일) → 주간(7일): 가장 최신 트렌드만 반영
    start_date = end_date - timedelta(days=7)
    headers = {
        "X-Naver-Client-Id": datalab_client_id,
        "X-Naver-Client-Secret": datalab_client_secret,
        "Content-Type": "application/json",
    }

    keyword_scores: dict[str, float] = {}
    batch_size = 5
    keywords = FASHION_KEYWORD_MASTER

    async with httpx.AsyncClient(timeout=20) as c:
        for i in range(0, len(keywords), batch_size):
            batch = keywords[i : i + batch_size]
            body = {
                "startDate": str(start_date),
                "endDate": str(end_date),
                "timeUnit": "week",
                "category": _FASHION_CATEGORY_ID,
                "keyword": [{"name": kw, "param": [kw]} for kw in batch],
                "device": "",
                "ages": [],
                "gender": "",
            }
            try:
                r = await c.post(
                    "https://openapi.naver.com/v1/datalab/shopping/category/keywords",
                    headers=headers,
                    json=body,
                )
                if r.status_code == 200:
                    for result in r.json().get("results", []):
                        kw_name = result.get("title", "")
                        ratios = [
                            d["ratio"]
                            for d in result.get("data", [])
                            if d.get("ratio") is not None
                        ]
                        if ratios:
                            # 최근 2주 평균으로 현재 트렌드 점수 산출
                            recent = ratios[-2:] if len(ratios) >= 2 else ratios
                            keyword_scores[kw_name] = sum(recent) / len(recent)
                elif r.status_code == 401:
                    print("[패션트렌드] 인증 실패 — DataLab 키 확인 필요", flush=True)
                    return []
                else:
                    print(f"[패션트렌드] API 오류 {r.status_code}", flush=True)
            except Exception as e:
                print(f"[패션트렌드] 배치 {i // batch_size + 1} 실패: {e}", flush=True)

            await asyncio.sleep(0.3)  # 네이버 API rate limit 회피

    # ratio_threshold 이상 키워드만 핫 트렌드로 선별
    hot_scores = {kw: s for kw, s in keyword_scores.items() if s >= ratio_threshold}
    sorted_kws = sorted(hot_scores.items(), key=lambda x: x[1], reverse=True)
    result = [kw for kw, _ in sorted_kws[:top_n]]
    print(
        f"[패션트렌드] 수집 완료 — 전체:{len(keyword_scores)} 핫(ratio≥{ratio_threshold}):{len(result)}: {result[:5]}",
        flush=True,
    )
    return result


def is_fashion_product(category: str) -> bool:
    """카테고리명 기준 패션의류 상품 여부 판별"""
    cat = str(category).lower()
    return any(kw in cat for kw in FASHION_CATEGORY_KEYWORDS)


# ─── 직원 5: 리뷰 분석가 ────────────────────────────────────────────────────
async def employee_review_analyst(product_name: str, anthropic_key: str) -> dict:
    """상품 Pain Point & 셀링포인트 분석"""
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        system=[{"type": "text", "text": "이커머스 상품 분석 전문가. 반드시 JSON만 출력.", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"""
'{product_name}' 상품의 고객 Pain Point와 셀링포인트를 분석해주세요.

JSON만 출력:
{{
  "pain_points": ["불만1", "불만2", "불만3"],
  "selling_points": ["장점1", "장점2", "장점3"],
  "key_message": "핵심 마케팅 메시지"
}}
"""}]
    )
    try:
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:].strip()
        return json.loads(text)
    except Exception:
        return {"pain_points": [], "selling_points": [], "key_message": ""}


# ─── 직원 6: 회계/정산 매니저 ───────────────────────────────────────────────
async def employee_accounting_manager(orders: list, margin_rate: float) -> dict:
    """주문 데이터 기반 일일 수익 계산"""
    total_revenue = 0
    for order in orders:
        try:
            total_revenue += int(order.get("totalPaymentAmount", 0))
        except (ValueError, TypeError):
            pass
    naver_fee = int(total_revenue * 0.033)
    gross_profit = int(total_revenue * margin_rate)
    net_profit = gross_profit - naver_fee
    return {
        "date": str(date.today()),
        "total_orders": len(orders),
        "total_revenue": f"{total_revenue:,}원",
        "gross_profit": f"{gross_profit:,}원",
        "naver_fee": f"{naver_fee:,}원",
        "net_profit": f"{net_profit:,}원",
        "margin": f"{margin_rate*100:.0f}%"
    }


# ─── 직원 7: 품절 방지 알림이 ───────────────────────────────────────────────
def employee_stock_guardian(products: list, threshold: int = 5) -> list:
    """재고 임계치 이하 상품 목록 반환"""
    low_stock = []
    for p in products:
        try:
            stock = int(p.get("stock", 999))
            if 0 < stock <= threshold:
                low_stock.append({"name": p.get("name", ""), "stock": stock, "code": p.get("code", "")})
        except (ValueError, TypeError):
            pass
    return low_stock


# ─── 직원 8: 시스템 에러 감사원 ─────────────────────────────────────────────
async def employee_error_auditor(errors: list, anthropic_key: str) -> str:
    """에러 목록 분석 후 원인 & 해결책 보고"""
    if not errors:
        return "✅ 시스템 정상 가동 중"
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        messages=[{"role": "user", "content": f"다음 시스템 에러를 분석해서 원인과 해결책을 한국어로 간단히 보고해주세요:\n{errors[:5]}"}]
    )
    return resp.content[0].text.strip()


# ─── 직원 9: 이벤트 매니저 ──────────────────────────────────────────────────
async def employee_event_manager(anthropic_key: str) -> dict:
    """스토어 프로모션 문구 & 알림 메시지 자동 생성"""
    season = employee_season_planner()
    upcoming = season["upcoming"][:2] if season["upcoming"] else []

    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    context = f"다가오는 이벤트: {[e['event'] for e in upcoming]}" if upcoming else "일반 시즌"

    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        system=[{"type": "text", "text": "스마트스토어 이벤트 마케팅 전문가. JSON만 출력.", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"""
{context} 기준으로 스마트스토어 프로모션 문구를 생성해주세요.

JSON 출력:
{{
  "store_notice": "스토어 공지사항 문구 (50자)",
  "first_buy_message": "첫 구매 할인 안내 문구 (30자)",
  "alarm_message": "알림받기 유도 문구 (30자)",
  "hashtags": ["이벤트태그1", "이벤트태그2", "이벤트태그3"]
}}
"""}]
    )
    try:
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:].strip()
        return json.loads(text)
    except Exception:
        return {}


# ─── 직원 10: 숏폼 영상 제작자 ──────────────────────────────────────────────
async def employee_shortform_creator(product_name: str) -> dict:
    """숏폼 공장에 상품 홍보 영상 제작 요청"""
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                "https://shortform-factory-production.up.railway.app/webhook",
                json={"topic": f"{product_name} 상품 홍보 숏폼"}
            )
            return {"status": "요청완료", "product": product_name}
    except Exception as e:
        return {"status": "실패", "error": str(e)}


# ─── 직원 11: 블로그 포스팅 매니저 ──────────────────────────────────────────
async def employee_blog_manager(product: dict, anthropic_key: str) -> str:
    """상품 정보 기반 네이버 블로그용 홍보 포스팅 글 생성"""
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=[{"type": "text", "text": "네이버 블로그 마케팅 전문가. 자연스러운 정보성 글 작성.", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"""
다음 상품으로 네이버 블로그 홍보 포스팅을 작성해주세요.
형식: 제목 + 본문 800자 내외, 자연스러운 정보성 글, 상품 장점 녹여내기

상품명: {product.get('name', '')}
가격: {product.get('price', 0):,}원
카테고리: {product.get('cat_large', '')}
"""}]
    )
    return resp.content[0].text.strip()


# ─── 직원 12: 광고 효율 분석가 ──────────────────────────────────────────────
async def employee_ad_analyst(orders: list, ad_cost: int, anthropic_key: str) -> dict:
    """ROAS 계산 및 광고 효율 분석"""
    total_revenue = sum(int(o.get("totalPaymentAmount", 0)) for o in orders)
    roas = (total_revenue / ad_cost * 100) if ad_cost > 0 else 0

    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{"role": "user", "content": f"ROAS {roas:.0f}%, 광고비 {ad_cost:,}원, 매출 {total_revenue:,}원. 광고 효율 평가와 입찰가 조정 방향을 한 줄로 알려주세요."}]
    )
    return {
        "roas": f"{roas:.0f}%",
        "ad_cost": f"{ad_cost:,}원",
        "revenue": f"{total_revenue:,}원",
        "recommendation": resp.content[0].text.strip()
    }


# ─── 직원 13: 플랫폼 확장 전문가 ────────────────────────────────────────────
async def employee_platform_expander(product: dict, target_platform: str, anthropic_key: str) -> dict:
    """타 플랫폼(쿠팡/11번가/지마켓) 맞춤 상품정보 변환"""
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        system=[{"type": "text", "text": "이커머스 플랫폼 전문가. JSON만 출력.", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"""
다음 상품 정보를 {target_platform} 플랫폼 형식으로 최적화해주세요.

상품: {json.dumps(product, ensure_ascii=False)}

JSON 출력:
{{
  "title": "최적화된 상품명",
  "description": "플랫폼 최적화 설명",
  "tags": ["태그1", "태그2", "태그3"]
}}
"""}]
    )
    try:
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception:
        return {}


# ─── 직원 14: DALL-E 키워드 번역가 ─────────────────────────────────────────
async def employee_keyword_translator(product_name: str, category: str, anthropic_key: str) -> str:
    """상품명 → DALL-E 최적화 영어 키워드 (Haiku)"""
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=60,
        system=[{"type": "text", "text": "Convert Korean product names to concise English keywords for DALL-E. Output only 3-6 English words, no explanation, no brand names.", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"Product: '{product_name}', category: '{category}'"}]
    )
    result = resp.content[0].text.strip()
    if any('가' <= c <= '힣' for c in result):
        return category or "lifestyle product"
    return result[:80]


# ─── 직원 15: 가격 최적화 분석가 ────────────────────────────────────────────
async def employee_price_optimizer(
    product_name: str,
    category: str,
    cost_price: int,
    anthropic_key: str,
    competitor_prices: list = None,
) -> dict:
    """경쟁사 실시간 가격 데이터 기반 최적 판매가 제안 (Sonnet)"""
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)

    # 경쟁사 가격 데이터 요약
    comp_context = ""
    if competitor_prices:
        prices = [c["price"] for c in competitor_prices if c.get("price", 0) > 0]
        if prices:
            avg = int(sum(prices) / len(prices))
            lo, hi = min(prices), max(prices)
            top3 = ", ".join(
                f"{c['title'][:12]} {c['price']:,}원"
                for c in competitor_prices[:3]
            )
            comp_context = (
                f"\n\n[실시간 네이버 쇼핑 경쟁사 데이터]\n"
                f"수집 상품 수: {len(prices)}개\n"
                f"가격 범위: {lo:,}원 ~ {hi:,}원\n"
                f"평균가: {avg:,}원\n"
                f"상위 3개: {top3}"
            )

    resp = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=200,
        system=[{"type": "text", "text": "네이버 스마트스토어 가격 전략 전문가. 반드시 JSON만 출력.", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"""
상품명: '{product_name}', 카테고리: '{category}', 원가: {cost_price:,}원{comp_context}

최적 판매가 제안 (마진율 최소 15%, 심리적 가격대 적용, 경쟁사 평균가 기준 포지셔닝).
JSON만 출력:
{{"suggested_price": 28000, "margin_rate": 0.25, "reason": "근거 한 줄"}}
"""}]
    )
    try:
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:].strip()
        result = json.loads(text)
        price = int(result.get("suggested_price", 0))
        if price < cost_price * 1.1 or price > cost_price * 5:
            price = round(cost_price * 1.15 / 10) * 10
        result["suggested_price"] = price
        return result
    except Exception:
        return {"suggested_price": round(cost_price * 1.15 / 10) * 10, "reason": "기본 마진 적용"}


# ─── 직원 16: 네이버 SEO 태그 생성자 ────────────────────────────────────────
async def employee_tag_generator(product_name: str, category: str, selling_points: list, anthropic_key: str) -> list:
    """네이버 검색 최적화 태그 자동 생성 (Haiku)"""
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    sp_text = ", ".join(selling_points[:3]) if selling_points else ""
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        system=[{"type": "text", "text": "네이버 쇼핑 검색 최적화 전문가. 반드시 JSON 배열만 출력.", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"""
상품명: '{product_name}', 카테고리: '{category}', 셀링포인트: {sp_text}

구매자가 실제 검색할 키워드 중심으로 태그 10개 생성. 각 태그 최대 15자.
JSON 배열만: ["태그1", "태그2", ...]
"""}]
    )
    try:
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:].strip()
        tags = json.loads(text)
        if isinstance(tags, list):
            return [str(t)[:15] for t in tags[:10] if t]
    except Exception:
        pass
    return [product_name[:8], category[:8], "추천", "인기", "가성비"]


# ─── 직원 17: Pexels 이미지 연관성 QC ────────────────────────────────────────
async def employee_pexels_qc(image_url: str, product_name: str, anthropic_key: str) -> dict:
    """Pexels 이미지 ↔ 상품 연관성 + 실사진 여부 검증 (Sonnet Vision)"""
    if not anthropic_key or not image_url:
        return {"relevant": True, "score": 80, "reason": "검수 생략"}
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.get(image_url)
            r.raise_for_status()
        import base64 as _b64
        img_b64 = _b64.standard_b64encode(r.content).decode()
    except Exception as e:
        return {"relevant": True, "score": 70, "reason": f"다운로드 실패: {e}"}

    client = anthropic.AsyncAnthropic(api_key=anthropic_key)
    resp = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=200,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}},
                {"type": "text", "text": f"""이 이미지가 '{product_name}' 스마트스토어 대표 이미지로 적합한지 판단해줘.

🚫 즉시 0점 조건:
- AI가 생성한 것처럼 보이는 이미지 (cartoon, synthetic, over-smooth, unnatural)
- 상품과 전혀 무관한 사진

점수 기준 (실제 사진만 해당):
95+: 상품과 직접 관련된 고품질 실사진
75-94: 같은 카테고리 유사 실사진
75 미만: 연관성 부족 또는 품질 미달

JSON만: {{"score": 85, "relevant": true, "reason": "한 줄 근거"}}"""}
            ]
        }]
    )
    try:
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:].strip()
        result = json.loads(text)
        result["relevant"] = result.get("score", 0) >= 75
        return result
    except Exception:
        return {"relevant": True, "score": 75, "reason": "파싱 실패"}


# ─── 직원 18: 품질검수관 (Image Inspector) ────────────────────────────────────
async def employee_image_inspector(
    image_url: str,
    product_name: str,
    anthropic_key: str,
    is_banner: bool = False,
    reject_keywords: list = None,
) -> dict:
    """
    Claude Vision으로 이미지 품질 채점 (0~100점)
    반환: {score, passed, issues, recommendation, retry_prompt}
    """
    if not anthropic_key or not image_url:
        return {"score": 100, "passed": True, "issues": [], "recommendation": "검수 생략"}

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.get(image_url)
            r.raise_for_status()
        import base64 as _b64
        img_b64 = _b64.standard_b64encode(r.content).decode()
        media_type = "image/jpeg"
    except Exception as e:
        return {"score": 100, "passed": True, "issues": [f"다운로드 실패: {e}"], "recommendation": "원본 사용"}

    img_type = "배너 이미지" if is_banner else "대표 상품 이미지"
    # 씬별 거부 기준 문구 생성
    if reject_keywords:
        reject_clause = "이미지 안에 " + " 또는 ".join(reject_keywords) + "이(가) 보이면 즉시 score=0 REJECT"
    else:
        reject_clause = "상품과 전혀 무관한 물체가 메인으로 보이면 score=0 REJECT"
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)

    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=600,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {"type": "base64", "media_type": media_type, "data": img_b64}
                },
                {
                    "type": "text",
                    "text": f"""너는 네이버 스마트스토어 전문 품질검수관이야.
'{product_name}'의 {img_type}를 아래 기준으로 채점해줘.

채점 기준 (각 20점):
1. 픽셀 깨짐/계단 현상 없음
2. 상품 형태 자연스럽고 배경 깔끔함
3. 네이버 스마트스토어 메인에 쓰기에 고급스러움
4. 밝기/채도/선명도 적절
5. 실제 사진처럼 보임 (AI 생성 티 없음, natural photo style)

🚫 즉시 0점 REJECT 조건 (하나라도 해당 시 score=0):
- AI가 그린 것처럼 보임 (cartoon, over-smooth, synthetic, unnatural texture)
- 상품과 전혀 무관한 이미지
{reject_clause}

반드시 JSON만 출력:
{{
  "score": 85,
  "issues": ["문제점1", "문제점2"],
  "recommendation": "개선 방향 한 줄",
  "retry_prompt": "재생성 시 추가할 프롬프트 힌트 (문제 없으면 빈 문자열)"
}}"""
                }
            ]
        }]
    )

    try:
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:].strip()
        result = json.loads(text)
        result["passed"] = result.get("score", 0) >= 95
        return result
    except Exception:
        return {"score": 75, "passed": False, "issues": ["파싱 실패"], "recommendation": "재시도 권장", "retry_prompt": ""}


# ─── 직원 19: 하이브리드 배너 생성자 ────────────────────────────────────────────
async def _hybrid_bg_prompt(product_name: str, category: str, gemini_key: str) -> str:
    """Gemini 텍스트 API로 상품에 어울리는 감성 배경 프롬프트 생성. 실패 시 기본값."""
    default = "Soft elegant studio background with warm gradient, premium minimal lifestyle, natural lighting bokeh"
    if not gemini_key:
        return default
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent",
                params={"key": gemini_key},
                json={"contents": [{"parts": [{"text": (
                    f"상품명: {product_name}\n카테고리: {category}\n\n"
                    "이 상품 메인 이미지 AI 배경 생성 영어 프롬프트를 40단어 이내로 써줘.\n"
                    "규칙: 배경 장면만(상품 언급 금지), 고급스럽고 감성적, 영어 프롬프트 텍스트만 출력"
                )}]}]},
            )
        if r.status_code == 200:
            txt = r.json()["candidates"][0]["content"]["parts"][0].get("text", "").strip()
            txt = txt.split("\n")[0].strip('"').strip("'").strip()
            if len(txt) > 10:
                print(f"[HYBRID] 배경 프롬프트: {txt[:70]}", flush=True)
                return txt
    except Exception as e:
        print(f"[HYBRID] Gemini 프롬프트 실패: {e}", flush=True)
    return default


async def _hybrid_generate_bg(prompt: str, flux_key: str, openai_key: str) -> bytes | None:
    """AI 배경 이미지 생성 — Flux Pro 1.1 → DALL-E 3 폴백, PNG/JPEG bytes 반환."""
    import asyncio

    full_prompt = prompt + ", no people, no text, no products, pure background only"

    # Flux Pro 1.1
    if flux_key:
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.post(
                    "https://api.bfl.ai/v1/flux-pro-1.1",
                    headers={"X-Key": flux_key, "Content-Type": "application/json"},
                    json={"prompt": full_prompt, "width": 1024, "height": 1024},
                )
                r.raise_for_status()
                data = r.json()
                task_id = data.get("id")
                polling_url = data.get("polling_url") or f"https://api.bfl.ai/v1/get_result?id={task_id}"
            for _ in range(30):
                await asyncio.sleep(2)
                async with httpx.AsyncClient(timeout=15) as c:
                    pr = await c.get(polling_url, headers={"X-Key": flux_key})
                    pdata = pr.json()
                if pdata.get("status") == "Ready":
                    img_url = (pdata.get("result") or {}).get("sample")
                    if img_url:
                        async with httpx.AsyncClient(timeout=30) as c:
                            ir = await c.get(img_url)
                            ir.raise_for_status()
                        print(f"[HYBRID] Flux 배경 완료", flush=True)
                        return ir.content
        except Exception as e:
            print(f"[HYBRID] Flux 배경 실패: {e}", flush=True)

    # DALL-E 3 폴백
    if openai_key:
        try:
            async with httpx.AsyncClient(timeout=60) as c:
                r = await c.post(
                    "https://api.openai.com/v1/images/generations",
                    headers={"Authorization": f"Bearer {openai_key}"},
                    json={"model": "dall-e-3", "prompt": full_prompt,
                          "n": 1, "size": "1024x1024", "quality": "hd"},
                )
                r.raise_for_status()
                img_url = r.json()["data"][0]["url"]
            async with httpx.AsyncClient(timeout=30) as c:
                ir = await c.get(img_url)
                ir.raise_for_status()
            print(f"[HYBRID] DALL-E 배경 완료", flush=True)
            return ir.content
        except Exception as e:
            print(f"[HYBRID] DALL-E 배경 실패: {e}", flush=True)
    return None


async def employee_hybrid_banner(
    image_bytes: bytes,
    product_name: str,
    category: str,
    gemini_key: str,
    openai_key: str,
    flux_key: str = "",
) -> bytes | None:
    """
    직원 19: 하이브리드 배너 생성자
    1. rembg로 상품 배경 제거 (RGBA)
    2. Gemini에게 감성 배경 프롬프트 요청
    3. Flux Pro 1.1 → DALL-E 3으로 AI 배경 생성
    4. 상품(투명 배경) + AI 배경 합성 → JPEG bytes 반환
    """
    import asyncio
    import io as _io
    from PIL import Image

    # Step 1: rembg 배경 제거 (동기 → to_thread로 호출)
    try:
        from rembg import remove as _rembg_remove
        bg_removed = await asyncio.to_thread(_rembg_remove, image_bytes)
        product_img = Image.open(_io.BytesIO(bg_removed)).convert("RGBA")
        print(f"[HYBRID] rembg 배경 제거 완료: {product_img.size}", flush=True)
    except Exception as e:
        print(f"[HYBRID] rembg 실패: {e}", flush=True)
        return None

    # Step 2: Gemini 배경 프롬프트
    bg_prompt = await _hybrid_bg_prompt(product_name, category, gemini_key)

    # Step 3: AI 배경 이미지 생성
    bg_bytes = await _hybrid_generate_bg(bg_prompt, flux_key, openai_key)
    if not bg_bytes:
        print(f"[HYBRID] AI 배경 생성 실패 — 합성 불가", flush=True)
        return None

    # Step 4: 상품 + AI 배경 합성
    try:
        TARGET = (1000, 1000)
        bg_img = Image.open(_io.BytesIO(bg_bytes)).convert("RGBA").resize(TARGET, Image.LANCZOS)

        # 상품을 배경 75% 크기로 축소, 하단 중앙 배치
        pw, ph = product_img.size
        max_dim = int(TARGET[0] * 0.75)
        scale = min(max_dim / pw, max_dim / ph, 1.0)
        nw, nh = int(pw * scale), int(ph * scale)
        prod = product_img.resize((nw, nh), Image.LANCZOS)

        x = (TARGET[0] - nw) // 2
        y = max(0, TARGET[1] - nh - int(TARGET[1] * 0.05))  # 하단 5% 여백

        composite = bg_img.copy()
        composite.paste(prod, (x, y), prod)

        out = _io.BytesIO()
        composite.convert("RGB").save(out, format="JPEG", quality=95, optimize=True)
        print(f"[HYBRID] 합성 완료 — 상품 {nw}x{nh} @ ({x},{y})", flush=True)
        return out.getvalue()
    except Exception as e:
        print(f"[HYBRID] 합성 실패: {e}", flush=True)
        return None
