import urllib.request, urllib.parse, json, time, os

DOMEGGOOK_API_KEY = os.environ.get("DOMEGGOOK_API_KEY", "1715da83a69031b92b5d897573f5a465")
DOMEGGOOK_API_URL = "https://domeggook.com/ssl/api/"
SMARTSTORE_ID     = "mnm1876"

CANDIDATES = [
    {"no": "13574291739", "name": "충전식 3in1 선풍기 탁상형 핸디 벽걸이",    "price": 27900, "kw": "3in1 선풍기 탁상 핸디"},
    {"no": "13566963747", "name": "돌돌이 썬캡 베이지 여름 스포츠모자",          "price": 29900, "kw": "돌돌이 썬캡 여름 모자"},
    {"no": "13574182980", "name": "차량선풍기 뒷좌석 헤드레스트 저소음 공기순환", "price": 17900, "kw": "차량 선풍기 뒷좌석 헤드레스트"},
]

def dg_search_image(kw):
    """getItemList로 검색 후 첫 번째 상품 thumb URL 반환."""
    params = urllib.parse.urlencode({
        "ver": "4.1", "mode": "getItemList",
        "aid": DOMEGGOOK_API_KEY,
        "market": "dome",
        "kw": kw, "om": "json",
        "sz": "5", "pg": "1", "so": "rd",
    })
    url = f"{DOMEGGOOK_API_URL}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read().decode("utf-8"))
    items = data.get("domeggook", {}).get("list", {}).get("item", [])
    if not isinstance(items, list):
        items = []
    if not items:
        return "", ""
    first = items[0]
    item_no = str(first.get("no", ""))
    thumb   = str(first.get("thumb", ""))
    return item_no, thumb

def dg_detail_image(item_no):
    """getItemView로 상세 original 이미지 URL 반환."""
    params = urllib.parse.urlencode({
        "ver": "4.5", "mode": "getItemView",
        "aid": DOMEGGOOK_API_KEY,
        "no": item_no, "om": "json",
    })
    url = f"{DOMEGGOOK_API_URL}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read().decode("utf-8"))
    detail = data.get("domeggook", {})
    thumb  = detail.get("thumb", {})
    return (thumb.get("original") or thumb.get("large") or thumb.get("list") or "")

print("=== 블루오션 상품 3개 선정 — 도매꾹 이미지 조회 ===\n")
results = []
for c in CANDIDATES:
    try:
        item_no, thumb_url = dg_search_image(c["kw"])
        # thumb이 없으면 getItemView로 재시도
        if item_no and "cdn" not in thumb_url:
            detail_img = dg_detail_image(item_no)
            img_url = detail_img or thumb_url
        else:
            img_url = thumb_url
    except Exception as e:
        img_url = f"ERROR: {e}"
        item_no = ""

    ss_url = f"https://smartstore.naver.com/{SMARTSTORE_ID}/products/{c['no']}"
    results.append({**c, "image": img_url, "ss_url": ss_url, "dg_no": item_no})
    print(f"✅ {c['name']}")
    print(f"   도매꾹번호: {item_no}")
    print(f"   이미지URL : {img_url}")
    print(f"   스마트스토어: {ss_url}")
    print()
    time.sleep(0.5)

# Claude.ai 전달 포맷
print("\n" + "="*65)
print("【 Claude.ai 전달 내용 — Higgsfield 영상 제작용 블루오션 3선 】")
print("="*65)

FEATURES = {
    "13574291739": ["탁상·핸디·벽걸이 3가지 모드 전환", "C타입 충전식 무선 사용", "360도 회전 바람 방향 조절"],
    "13566963747": ["돌돌이 접이식 — 가방에 쏙 보관", "챙 넓어 자외선 완벽 차단", "땀 흡수 속건 스포츠 소재"],
    "13574182980": ["헤드레스트 장착 뒷좌석 전용 설계", "저소음 3단계 풍속 조절", "USB 차량 시가잭 간편 연결"],
}

for i, r in enumerate(results, 1):
    feats = FEATURES.get(r["no"], ["특징1", "특징2", "특징3"])
    print(f"""
{'─'*60}
📦 상품 {i}: {r['name']}
💰 가격: {r['price']:,}원
🖼  도매꾹 원본 이미지 URL:
    {r['image']}
🛒 스마트스토어 URL:
    {r['ss_url']}
✨ 핵심 특징:
    1. {feats[0]}
    2. {feats[1]}
    3. {feats[2]}""")

print(f"\n{'─'*60}")
print("📌 Higgsfield 영상 제작 요청:")
print("   위 3개 상품 이미지로 각각 15초 숏폼 영상 제작 부탁드립니다.")
print("   스타일: 제품 클로즈업 → 사용 장면 → CTA (스마트스토어 링크)")
