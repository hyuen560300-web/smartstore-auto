import urllib.request, json

targets = [
    ("13563752657", "방수 털안감 주방슬리퍼 겨울용"),
    ("13563752278", "방한 방수 다용도슬리퍼 털안감"),
    ("13563532715", "에코퍼 카파바라 쁘띠 목도리"),
    ("13563381107", "기모안감 투톤 방한 스타킹 골프웨어"),
]

body = json.dumps({"product_nos": [t[0] for t in targets]}).encode("utf-8")
req = urllib.request.Request(
    "https://smartstore-auto-production.up.railway.app/products/delete-by-nos",
    data=body,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(req, timeout=120) as r:
    data = json.loads(r.read().decode("utf-8"))

print(f"삭제 성공: {data['deleted_count']}개 / 실패: {data['failed_count']}개\n")
no_map = {t[0]: t[1] for t in targets}
for no in data["deleted"]:
    print(f"  ✅ [{no}] {no_map.get(no, no)}")
for f in data["failed"]:
    print(f"  ❌ [{f['no']}] {no_map.get(f['no'], f['no'])} — {f['reason']}")
