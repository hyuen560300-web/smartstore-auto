import urllib.request, urllib.parse, json

kw = "방한,내복,발열,핫팩,난방,겨울,두꺼운,보온,히터,전기장판,온수매트,목도리,장갑,패딩,방풍,방설,스키,방한복,귀마개,워머"
url = "https://smartstore-auto-production.up.railway.app/products/keyword-scan?keywords=" + urllib.parse.quote(kw)
with urllib.request.urlopen(url, timeout=300) as r:
    data = json.loads(r.read().decode("utf-8"))

print(f"전체 스캔: {data['total_scanned']}개 / 겨울 키워드 매칭: {data['matched_count']}개\n")
for p in data["products"]:
    print(f"[{p['product_no']}] {p['name']}")
