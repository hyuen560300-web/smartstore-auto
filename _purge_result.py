import urllib.request, json

with urllib.request.urlopen("https://smartstore-auto-production.up.railway.app/products/purge-result", timeout=30) as r:
    data = json.loads(r.read().decode("utf-8"))

print(f"스캔: {data['scanned']}개 / 삭제: {data['deleted']}개 / 실패: {data['failed']}개\n")

deleted = [l for l in data["log"] if l["ok"]]
failed  = [l for l in data["log"] if not l["ok"]]

# 이유별 집계
from collections import Counter
reasons = Counter("비시즌" if l["reason"] == "비시즌" else "가격미만" for l in deleted)
print(f"비시즌: {reasons['비시즌']}개 / 가격미만: {reasons['가격미만']}개\n")

print("=== 삭제 성공 목록 ===")
for l in deleted:
    print(f"  ✅ [{l['no']}] {l['name'][:45]}  ({l['reason']})")

if failed:
    print("\n=== 실패 목록 ===")
    for l in failed:
        print(f"  ❌ [{l['no']}] {l['name'][:45]}  ({l['reason']})")
