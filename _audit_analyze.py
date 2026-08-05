import urllib.request, json

# 결과 가져오기
with urllib.request.urlopen("https://smartstore-auto-production.up.railway.app/products/audit-result", timeout=30) as r:
    data = json.loads(r.read().decode("utf-8"))

products = data["products"]
print(f"수집 총계: {len(products)}개\n")

# 절대 삭제 금지 목록 (영상 제작 상품)
KEEP = [
    "에펠탑", "이중유리", "통풍시트커버", "통풍 시트", "시트커버",
    "레인부츠", "미스트 분사기", "미스트분사기", "나노 미스트",
    "텃밭 재배키트", "텃밭재배키트", "미니텃밭", "부직포 화분",
    "에어컨 실외기", "실외기커버", "아크릴 매니큐어", "매니큐어 정리대",
    "커피 추출기", "이중유리 커피",
]
def is_protected(name):
    return any(k in name for k in KEEP)

# ── 기준 1: 가격 1만원 미만 ──────────────────────────────────────────
low_price = [p for p in products if p["price"] > 0 and p["price"] < 10000 and not is_protected(p["name"])]

# ── 기준 2: 비시즌 (6월 여름 기준) ──────────────────────────────────
WINTER_KW  = ["방한","내복","발열","핫팩","난방","겨울용","기모","두꺼운털","보온병","히터","전기장판","온수매트","목도리","방풍","방한복","핫팩","워머","방설","스키복"]
SPRING_KW  = ["꽃샘추위","봄코트","봄나들이","봄맞이","봄 자켓"]
AUTUMN_KW  = ["가을자켓","가을 자켓","트렌치코트","가을코트","가을 코트"]

def has_kw(name, kws):
    return any(k in name for k in kws)

offseason = []
for p in products:
    if is_protected(p["name"]): continue
    if has_kw(p["name"], WINTER_KW):
        offseason.append({**p, "reason": "겨울 잔존"})
    elif has_kw(p["name"], SPRING_KW):
        offseason.append({**p, "reason": "봄 전용"})
    elif has_kw(p["name"], AUTUMN_KW):
        offseason.append({**p, "reason": "가을 전용"})

# ── 기준 3: 이름 앞 3글자 기준 중복 유사 (카테고리 추정) ───────────────
from collections import defaultdict
prefix_map = defaultdict(list)
for p in products:
    if is_protected(p["name"]): continue
    prefix = p["name"][:4]
    prefix_map[prefix].append(p)

dup_groups = {k: v for k, v in prefix_map.items() if len(v) >= 5}

# ── 출력 ────────────────────────────────────────────────────────────
print("=" * 60)
print(f"[기준1] 가격 1만원 미만 — {len(low_price)}개")
print("=" * 60)
for p in low_price[:50]:
    print(f"  [{p['product_no']}] {p['name'][:40]}  ₩{p['price']:,}")

print()
print("=" * 60)
print(f"[기준2] 비시즌 상품 — {len(offseason)}개")
print("=" * 60)
for p in offseason:
    print(f"  [{p['product_no']}] {p['name'][:40]}  ({p['reason']})")

print()
print("=" * 60)
print(f"[기준3] 유사 중복 그룹 (5개 이상) — {len(dup_groups)}그룹")
print("=" * 60)
for prefix, items in list(dup_groups.items())[:10]:
    print(f"  '{prefix}' 군 {len(items)}개:")
    for p in items[:6]:
        print(f"    [{p['product_no']}] {p['name'][:45]}")
