import urllib.request, json
from collections import defaultdict

with urllib.request.urlopen("https://smartstore-auto-production.up.railway.app/products/audit-result", timeout=30) as r:
    data = json.loads(r.read().decode("utf-8"))

products = data["products"]
KEEP = ["에펠탑","이중유리","통풍시트커버","통풍 시트","레인부츠","미스트 분사기","미스트분사기","나노 미스트",
        "텃밭 재배키트","텃밭재배키트","미니텃밭","부직포 화분","에어컨 실외기","실외기커버","아크릴 매니큐어",
        "매니큐어 정리대","커피 추출기","이중유리 커피"]
def is_protected(name): return any(k in name for k in KEEP)

# 카테고리 키워드 → 그룹명
CAT = {
    "강아지": "반려동물", "고양이": "반려동물", "펫": "반려동물",
    "네일": "네일/뷰티", "큐티클": "네일/뷰티", "젤네일": "네일/뷰티", "매니큐어": "네일/뷰티", "발톱": "네일/뷰티",
    "에어컨": "에어컨커버", "실외기": "에어컨커버",
    "텃밭": "텃밭/원예", "화분": "텃밭/원예", "모종": "텃밭/원예", "재배": "텃밭/원예", "씨앗": "텃밭/원예",
    "DIY": "DIY/공예", "뜨개": "DIY/공예", "위빙": "DIY/공예", "직조": "DIY/공예",
    "악세사리": "악세사리/DIY", "귀걸이": "악세사리/DIY", "팔찌": "악세사리/DIY",
    "수납": "수납/정리", "수납함": "수납/정리", "정리대": "수납/정리", "바구니": "수납/정리",
}
def get_cat(name):
    for kw, cat in CAT.items():
        if kw in name: return cat
    return None

# 1만원 미만 상품 카테고리별 그룹화
cat_map = defaultdict(list)
low_price_no_cat = []
for p in products:
    if p["price"] > 0 and p["price"] < 10000 and not is_protected(p["name"]):
        c = get_cat(p["name"])
        if c:
            cat_map[c].append(p)
        else:
            low_price_no_cat.append(p)

print("=" * 65)
print("[기준1] 가격 1만원 미만 — 카테고리별")
print("=" * 65)
total_lp = 0
for cat, items in sorted(cat_map.items(), key=lambda x: -len(x[1])):
    print(f"\n  ▶ {cat} — {len(items)}개")
    for p in items:
        print(f"    [{p['product_no']}] {p['name'][:45]}  ₩{p['price']:,}")
    total_lp += len(items)
if low_price_no_cat:
    print(f"\n  ▶ 기타 — {len(low_price_no_cat)}개")
    for p in low_price_no_cat[:20]:
        print(f"    [{p['product_no']}] {p['name'][:45]}  ₩{p['price']:,}")
    total_lp += len(low_price_no_cat)
print(f"\n  소계: {total_lp}개")

# 비시즌
WINTER_KW = ["방한","내복","발열","핫팩","난방","겨울용","기모","워머","보온병","히터","전기장판","온수매트","목도리","방풍","방한복","방설","스키복","양털기모","양털"]
offseason = [p for p in products if not is_protected(p["name"]) and any(k in p["name"] for k in WINTER_KW)]
print()
print("=" * 65)
print(f"[기준2] 비시즌 (겨울 잔존) — {len(offseason)}개")
print("=" * 65)
for p in offseason:
    matched = [k for k in WINTER_KW if k in p["name"]]
    print(f"  [{p['product_no']}] {p['name'][:45]}  (키워드: {matched[0]})")

# 같은 카테고리 5개 이상 중복
print()
print("=" * 65)
print("[기준3] 카테고리별 5개 이상 중복 그룹")
print("=" * 65)
for cat, items in sorted(cat_map.items(), key=lambda x: -len(x[1])):
    if len(items) >= 5:
        print(f"\n  ▶ [{cat}] {len(items)}개 — 초과분 삭제 후보")
        # 가격 오름차순으로 하위 상품이 후보
        for p in sorted(items, key=lambda x: x["price"])[:5]:
            print(f"    [{p['product_no']}] {p['name'][:45]}  ₩{p['price']:,}")

print()
print(f"\n※ 230개만 수집된 이유: 네이버 API 개별 상세조회 실패 상품 제외")
print(f"※ 이미지 품질 불량은 API 접근 불가 → 시각 검수 필요")
print(f"※ 영상 제작 상품 {len(KEEP)}종 보호 (삭제 후보 제외)")
