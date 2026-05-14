"""
스마트스토어 판매 중단 + Shopify 초안으로 전환 + 내용 업데이트
"""
import asyncio, os, json, sys, time
import urllib.request, urllib.error
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
load_dotenv(r"C:\Users\USER\Desktop\shopify_auto\.env")

SS_SERVER    = "https://smartstore-auto-production.up.railway.app"
SHOPIFY_URL  = "https://shopify-trendify-production.up.railway.app"
SS_PRODUCT   = 13453911344
SHO_PRODUCT  = 8506931773634

MAIN_IMAGE   = "https://res.cloudinary.com/dev11ovzx/image/upload/v1778480772/vfbdqebxirrtam7cpp7g.png"

with open(r"C:\Users\USER\Desktop\CEO-AI\docs\detail_simple.html", encoding="utf-8") as f:
    DETAIL_HTML = f.read()


def post_json(url: str, payload: dict, timeout: int = 60) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:500]
        return {"error": f"HTTP {e.code}", "body": body}
    except Exception as e:
        return {"error": str(e)}


# ── 1. 스마트스토어 판매 중단 + 내용 업데이트 ────────────────────────────────
print("=" * 60)
print("1. 스마트스토어 상품 판매 중단 + 내용 업데이트")
print(f"   상품번호: {SS_PRODUCT}")
print("=" * 60)

ss_payload = {
    "name":           "AI 이커머스 풀 자동화 스위트 소싱봇 숏폼봇 SEO봇 보고봇 n8n 워크플로우",
    "price":          129000,
    "stock":          9999,
    "image":          MAIN_IMAGE,
    "detailContent":  DETAIL_HTML,
    "originAreaCode": "0009380",   # 국산(서울특별시 은평구)
    "statusType":     "SUSPENSION",
}

ss_result = post_json(f"{SS_SERVER}/update-digital/{SS_PRODUCT}", ss_payload, timeout=120)
print(f"   결과: {json.dumps(ss_result, ensure_ascii=False)[:300]}")

if ss_result.get("status") == "ok":
    print("   ✓ 스마트스토어 판매 중단 완료")
else:
    print("   ✗ 실패 — 상세 확인 필요")

print()

# ── 2. Shopify 초안 전환 + 내용 업데이트 ──────────────────────────────────────
print("=" * 60)
print("2. Shopify 상품 초안(draft) 전환 + 내용 업데이트")
print(f"   상품 ID: {SHO_PRODUCT}")
print("=" * 60)

sho_payload = {
    "status":    "draft",
    "title":     "AI 이커머스 풀 자동화 스위트 (소싱봇·숏폼봇·SEO봇·보고봇 n8n)",
    "body_html": DETAIL_HTML,
    "images":    [{"src": MAIN_IMAGE}],
}

sho_result = post_json(f"{SHOPIFY_URL}/update-product/{SHO_PRODUCT}", sho_payload, timeout=60)
print(f"   결과: {json.dumps(sho_result, ensure_ascii=False)[:300]}")

if sho_result.get("status") == "ok":
    print("   ✓ Shopify 초안 전환 완료")
else:
    print(f"   ✗ 실패 (Shopify 서버 배포 중이면 잠시 후 재시도)")

print()
print("=" * 60)
print("완료!")
