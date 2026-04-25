"""
스마트스토어 AI 직원단
총괄팀장: Claude
"""
import json
import re
import xml.etree.ElementTree as ET
from datetime import date

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


# ─── 직원 9: 숏폼 영상 제작자 ───────────────────────────────────────────────
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


# ─── 직원 10: 블로그 포스팅 매니저 ──────────────────────────────────────────
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


# ─── 직원 11: 광고 효율 분석가 ──────────────────────────────────────────────
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


# ─── 직원 12: 플랫폼 확장 전문가 ────────────────────────────────────────────
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
