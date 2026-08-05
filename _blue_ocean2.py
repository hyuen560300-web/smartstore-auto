import urllib.request, json

# 선정 후보 product_no 목록 (브랜드명 제외 후 상위)
TARGETS = [
    "13574284965",  # USB충전식 미니 탁상선풍기 휴대용 21,900
    "13574291739",  # 충전식 3in1 선풍기 탁상형 핸디 벽걸이 27,900
    "13574278364",  # C타입 충전식 무선 탁상용 선풍기 7엽 23,900
    "13574182980",  # 차량선풍기 뒷좌석 헤드레스트 17,900
    "13566963747",  # 돌돌이 썬캡 베이지 여름 스포츠모자 29,900
    "13562817361",  # 챙넓은 와이드 모자 돌돌이 여름 햇빛 18,900
    "13562813266",  # 초광각 썬캡 자외선차단 넓은챙 모자 12,900
    "13567260124",  # 대형견 쿨패딩 프린팅 강아지옷 38,760
    "13570067396",  # 창문형에어컨커버 윈도우핏 UV방수 13,900
]

with urllib.request.urlopen(
    "https://smartstore-auto-production.up.railway.app/products/audit-result",
    timeout=30
) as r:
    data = json.loads(r.read().decode("utf-8"))

products = {str(p["product_no"]): p for p in data["products"]}

print("=== 후보 상품 상세 ===\n")
for no in TARGETS:
    p = products.get(no)
    if not p:
        print(f"[{no}] 데이터 없음\n")
        continue
    name  = p.get("name", "")
    price = p.get("price", 0)
    img   = p.get("image_url", "") or p.get("images", [""])[0] if p.get("images") else ""
    # audit 결과에서 사용 가능한 모든 필드 확인
    keys = list(p.keys())
    print(f"상품명: {name}")
    print(f"가격  : {price:,}원")
    print(f"no    : {no}")
    print(f"필드  : {keys}")
    # 이미지 관련 필드
    for k in keys:
        if "image" in k.lower() or "img" in k.lower() or "url" in k.lower():
            print(f"  {k}: {str(p[k])[:100]}")
    print()
