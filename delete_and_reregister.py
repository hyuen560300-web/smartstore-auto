"""
AI 이커머스 상품 전체 삭제 후 1개 신규 등록
- 스마트스토어: 13453762030, 13453767805, 13453763714, 13453911344 삭제
- Shopify: 8506931773634 삭제
- 도매꾹 자동 등록에는 영향 없음
"""
import urllib.request, urllib.error, json, sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SS_SERVER   = "https://smartstore-auto-production.up.railway.app"
SHO_SERVER  = "https://shopify-trendify-production.up.railway.app"

SS_DELETE_IDS  = [13453762030, 13453767805, 13453763714, 13453911344]
SHO_DELETE_ID  = 8506931773634

MAIN_IMAGE  = "https://res.cloudinary.com/dev11ovzx/image/upload/v1778480772/vfbdqebxirrtam7cpp7g.png"
with open(r"C:\Users\USER\Desktop\CEO-AI\docs\detail_simple.html", encoding="utf-8") as f:
    DETAIL_HTML = f.read()


def call(method, url, payload=None, timeout=30):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload else None
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"} if data else {},
                                  method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:300]
        return {"error": f"HTTP {e.code}", "body": body}
    except Exception as e:
        return {"error": str(e)}


# ── 1. 스마트스토어 4개 삭제 ─────────────────────────────────────────────────
print("=" * 60)
print("1. 스마트스토어 AI 상품 4개 삭제")
print("=" * 60)
for pid in SS_DELETE_IDS:
    res = call("DELETE", f"{SS_SERVER}/delete-product/{pid}")
    ok  = res.get("status") == "ok"
    print(f"  {pid}: {'✓ 삭제 완료' if ok else f'✗ {res}'}")

# ── 2. Shopify 삭제 ───────────────────────────────────────────────────────────
print()
print("=" * 60)
print("2. Shopify AI 상품 삭제")
print("=" * 60)
res = call("DELETE", f"{SHO_SERVER}/delete-product/{SHO_DELETE_ID}")
ok  = res.get("status") == "ok" or res.get("deleted") == True
print(f"  {SHO_DELETE_ID}: {'✓ 삭제 완료' if ok else f'? {res}'}")

# ── 3. 스마트스토어 신규 등록 (1개) ──────────────────────────────────────────
print()
print("=" * 60)
print("3. 스마트스토어 신규 등록 (1개)")
print("=" * 60)
ss_payload = {
    "name":           "AI 이커머스 풀 자동화 스위트 소싱봇 숏폼봇 SEO봇 보고봇 n8n 워크플로우",
    "price":          129000,
    "stock":          9999,
    "image":          MAIN_IMAGE,
    "detailContent":  DETAIL_HTML,
    "originAreaCode": "0009380",
    "statusType":     "SALE",
}
res = call("POST", f"{SS_SERVER}/register-digital", ss_payload, timeout=120)
if res.get("product_no") or res.get("status") == "ok":
    print(f"  ✓ 등록 완료: {res}")
else:
    print(f"  ✗ 실패: {res}")

# ── 4. Shopify 신규 등록 (1개) ────────────────────────────────────────────────
print()
print("=" * 60)
print("4. Shopify 신규 등록 (1개)")
print("=" * 60)
sho_payload = {
    "status":    "active",
    "title":     "AI 이커머스 풀 자동화 스위트 (소싱봇·숏폼봇·SEO봇·보고봇 n8n)",
    "body_html": DETAIL_HTML,
    "images":    [{"src": MAIN_IMAGE}],
    "variants":  [{"price": "95.99", "inventory_management": None}],
}
res = call("POST", f"{SHO_SERVER}/register-digital", sho_payload, timeout=60)
if res.get("id") or res.get("status") == "ok":
    print(f"  ✓ 등록 완료: {res}")
else:
    print(f"  ✗ 실패: {res}")

print()
print("=" * 60)
print("완료!")
