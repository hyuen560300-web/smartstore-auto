import urllib.request, json, sys

# 제외 목록 (이미 영상 제작한 상품)
EXCLUDE = [
    "에펠탑","이중유리 커피","산리오","귀마개 샤워캡","풍선 가랜드",
    "실리콘 주방장갑","시스맥스","통풍시트커버","통풍 시트커버",
    "레인부츠","미스트 분사기","미스트분사기","나노 미스트",
    "텃밭 재배키트","텃밭재배키트","미니텃밭","실외기 커버","실외기커버",
    "에어컨 실외기","매니큐어 정리대","베란다 텃밭 부직포 화분",
]

# 브랜드/캐릭터 키워드
BRAND_KW = [
    "삼성","LG","나이키","아디다스","산리오","디즈니","포켓몬","아기상어",
    "해리포터","카카오","라인프렌즈","BTS","스타벅스","시스맥스",
    "스탠들러","파카","로고","브랜드",
]

# 여름 비적합 키워드
WINTER_KW = ["방한","내복","발열","핫팩","난방","두꺼운","보온","기모","털","방한복","털실","겨울"]

# 여름 적합 키워드 (우선순위 높음)
SUMMER_KW = [
    "냉감","쿨링","캠핑","물놀이","주방","뷰티","선풍기","보냉","여름","에어컨","자외선",
    "아이스","UV","수영","해변","바다","모자","선크림","방수","쿨","청량",
    "아이스팩","보냉백","미니팬","쿨링타월","썬글라스","아웃도어","야외","휴대용",
    "비치","워터","바베큐","BBQ","텀블러","물통","가방","선글라스",
]

with urllib.request.urlopen(
    "https://smartstore-auto-production.up.railway.app/products/audit-result",
    timeout=30
) as r:
    data = json.loads(r.read().decode("utf-8"))

products = data["products"]
print(f"전체 스캔: {len(products)}개\n", flush=True)

def is_excluded(name):
    return any(k in name for k in EXCLUDE)

def has_brand(name):
    return any(k in name for k in BRAND_KW)

def is_winter(name):
    return any(k in name for k in WINTER_KW)

def summer_score(name):
    return sum(1 for k in SUMMER_KW if k in name)

# 필터링
candidates = []
for p in products:
    name  = p.get("name", "")
    price = p.get("price", 0)
    if price < 10000 or price > 50000:
        continue
    if is_excluded(name):
        continue
    if has_brand(name):
        continue
    if is_winter(name):
        continue
    score = summer_score(name)
    candidates.append({**p, "_score": score})

# 여름 점수 높은 순 정렬
candidates.sort(key=lambda x: x["_score"], reverse=True)

print(f"조건 충족 후보: {len(candidates)}개\n", flush=True)
print("=== 상위 20개 후보 ===", flush=True)
for i, p in enumerate(candidates[:20]):
    print(f"[{i+1:2}] {p['price']:,}원 | score={p['_score']} | {p['name'][:55]}", flush=True)
    print(f"      no={p['product_no']}", flush=True)
