import urllib.request, json, time

# 영상 제작 상품 절대 보호
KEEP = [
    "에펠탑", "이중유리", "통풍시트커버", "통풍 시트커버",
    "레인부츠", "미스트 분사기", "미스트분사기", "나노 미스트",
    "텃밭 재배키트", "텃밭재배키트", "미니텃밭 재배",
    "에어컨 실외기 커버", "실외기 커버", "실외기커버",
    "아크릴 매니큐어 정리대", "매니큐어 정리대",
    "이중유리 커피", "커피 추출기",
    "베란다 텃밭 부직포 화분",
]
def is_protected(name):
    return any(k in name for k in KEEP)

# audit-result 에서 전체 상품 가져오기
with urllib.request.urlopen(
    "https://smartstore-auto-production.up.railway.app/products/audit-result", timeout=30
) as r:
    data = json.loads(r.read().decode("utf-8"))

products = data["products"]
print(f"audit 수집: {len(products)}개\n")

# 1. 가격 1만원 미만 (보호 상품 제외)
low_price = [
    p for p in products
    if 0 < p["price"] < 10000 and not is_protected(p["name"])
]

# 2. 비시즌 (명시 지정)
OFFSEASON_NOS = {"13564988215", "13563367338"}

# 3. 최종 삭제 목록
delete_map = {}  # product_no -> name
for p in low_price:
    delete_map[p["product_no"]] = p["name"]
for p in products:
    if p["product_no"] in OFFSEASON_NOS:
        delete_map[p["product_no"]] = p["name"]
# 비시즌 2개는 audit에 없을 수도 있으므로 직접 추가
if "13564988215" not in delete_map:
    delete_map["13564988215"] = "USB충전식 물티슈워머 온도조절 휴대용"
if "13563367338" not in delete_map:
    delete_map["13563367338"] = "촌캉스 온가족 양털기모 조끼 마법덧신 세트"

nos = list(delete_map.keys())
print(f"삭제 대상: {len(nos)}개")
print(f"  (가격 1만원 미만 {len(low_price)}개 + 비시즌 추가분)")
print()

# 삭제 실행 — /products/delete-by-nos (서버 내부 0.3초 순차 처리)
body = json.dumps({"product_nos": nos}).encode("utf-8")
req = urllib.request.Request(
    "https://smartstore-auto-production.up.railway.app/products/delete-by-nos",
    data=body,
    headers={"Content-Type": "application/json"},
    method="POST",
)
print("삭제 요청 전송 중...")
with urllib.request.urlopen(req, timeout=300) as r:
    result = json.loads(r.read().decode("utf-8"))

print(f"\n✅ 삭제 완료: {result['deleted_count']}개")
print(f"❌ 실패: {result['failed_count']}개")

if result["deleted"]:
    print("\n[삭제 성공]")
    for no in result["deleted"]:
        print(f"  ✅ [{no}] {delete_map.get(no, no)}")

if result["failed"]:
    print("\n[실패 목록]")
    for f in result["failed"]:
        print(f"  ❌ [{f['no']}] {delete_map.get(f['no'], f['no'])} — {f['reason']}")
