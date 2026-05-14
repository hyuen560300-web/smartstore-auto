"""
스마트스토어 판매자 센터 - 원산지 코드 탐색 및 한국으로 변경
"""
import asyncio, os, json, sys, urllib.request
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from playwright.async_api import async_playwright
from dotenv import load_dotenv

load_dotenv()

NAVER_ID = os.getenv("SMARTSTORE_NAVER_ID", "mnm1876")
NAVER_PW = os.getenv("SMARTSTORE_NAVER_PW", "gusdn@560300")
PRODUCT_NO = "13453911344"
SS_SERVER = "https://smartstore-auto-production.up.railway.app"

LOGIN_URL = "https://accounts.commerce.naver.com/login"
SELL_BASE = "https://sell.smartstore.naver.com"


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=200)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        page = await ctx.new_page()

        # 1. 로그인
        print(">> 판매자 센터 로그인...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        await page.fill('input[type="text"]', NAVER_ID)
        await asyncio.sleep(0.5)
        await page.fill('input[type="password"]', NAVER_PW)
        await asyncio.sleep(0.5)
        await page.click('button:has-text("로그인")')
        print("   로그인 클릭 - 40초 대기...")
        await asyncio.sleep(40)  # 로그인 + 리다이렉트 대기
        print(f"   URL: {page.url}")

        if "login" in page.url:
            await page.screenshot(path="origin_page.png")
            print("   로그인 실패 - 스크린샷 저장됨")
            await browser.close()
            return

        # 팝업 닫기
        for sel in ['button:has-text("닫기")', '[class*="close"]', 'button:has-text("확인")']:
            try:
                await page.click(sel, timeout=2000)
                await asyncio.sleep(0.5)
            except:
                pass

        # 2. 상품 수정 페이지로 직접 이동
        edit_url = f"{SELL_BASE}/#/products/{PRODUCT_NO}/edit"
        print(f">> 상품 수정 페이지로 이동: {edit_url}")
        await page.goto(edit_url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(10)  # SPA 로드 대기
        print(f"   URL: {page.url}")
        await page.screenshot(path="origin_page.png")

        # 페이지 스크롤해서 원산지 로드
        await page.evaluate("window.scrollTo(0, 500)")
        await asyncio.sleep(2)
        await page.evaluate("window.scrollTo(0, 1500)")
        await asyncio.sleep(2)

        # 3. 원산지 관련 UI 탐색
        print(">> 원산지 UI 탐색...")
        # 먼저 텍스트 검색
        origin_elements = await page.evaluate("""
            () => {
                const result = [];
                // 라벨이나 제목에서 원산지 찾기
                const all = document.querySelectorAll('label, th, dt, span, div');
                for (const el of all) {
                    if (el.children.length === 0 && el.textContent.trim() === '원산지') {
                        // 주변 form 요소 찾기
                        let parent = el.parentElement;
                        for (let i = 0; i < 5; i++) {
                            const sels = parent.querySelectorAll('select');
                            if (sels.length > 0) {
                                result.push({
                                    label: el.textContent.trim(),
                                    selects: [...sels].map(s => ({
                                        id: s.id, name: s.name,
                                        value: s.value,
                                        opts: [...s.options].map(o => ({v: o.value, t: o.textContent.trim()}))
                                    }))
                                });
                                break;
                            }
                            parent = parent.parentElement;
                            if (!parent) break;
                        }
                    }
                }
                return result;
            }
        """)
        print(f"   원산지 관련 elements: {json.dumps(origin_elements, ensure_ascii=False)[:3000]}")

        # 4. 모든 select에서 한국/국내 옵션 찾기
        all_korea = await page.evaluate("""
            () => {
                const results = [];
                for (const s of document.querySelectorAll('select')) {
                    for (const o of s.options) {
                        const t = o.textContent.trim();
                        if (t.includes('한국') || t.includes('국내산') || t === '국내') {
                            results.push({sel_id: s.id, sel_name: s.name, val: o.value, text: t});
                        }
                    }
                }
                return results;
            }
        """)
        print(f"   한국 옵션: {all_korea}")

        # 국내 원산지 관련 radio/checkbox
        domestic = await page.evaluate("""
            () => {
                const inputs = [...document.querySelectorAll('input[type=radio], input[type=checkbox]')];
                return inputs.filter(i => {
                    const lbl = document.querySelector('label[for="' + i.id + '"]');
                    return lbl && (lbl.textContent.includes('국내') || lbl.textContent.includes('한국'));
                }).map(i => ({id: i.id, value: i.value, label: document.querySelector('label[for="'+i.id+'"]')?.textContent.trim()}));
            }
        """)
        print(f"   국내 radio/checkbox: {domestic}")

        await page.screenshot(path="origin_page.png")
        print("   스크린샷: origin_page.png")

        # 5. 발견한 코드로 업데이트
        korea_code = None
        if all_korea:
            korea_code = all_korea[0]["val"]
        elif origin_elements:
            for item in origin_elements:
                for sel in item.get("selects", []):
                    for opt in sel.get("opts", []):
                        if "한국" in opt.get("t", "") or "국내" in opt.get("t", ""):
                            korea_code = opt["v"]
                            break

        if korea_code:
            print(f">> 한국산 코드 발견: {korea_code}")
            with open(r"C:\Users\USER\Desktop\CEO-AI\docs\pamphlet_full_suite.html", encoding="utf-8") as f:
                pamphlet = f.read()
            payload = {
                "name": "AI 이커머스 풀 자동화 스위트 소싱봇 숏폼봇 SEO봇 보고봇 n8n 워크플로우",
                "price": 129000, "stock": 9999,
                "image": "https://res.cloudinary.com/dev11ovzx/image/upload/v1778475908/beelfhj5xlo94q1hlcvq.png",
                "detailContent": pamphlet,
                "originAreaCode": korea_code,
            }
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                f"{SS_SERVER}/update-digital/{PRODUCT_NO}",
                data=data, headers={"Content-Type": "application/json"}, method="POST"
            )
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    result = json.loads(r.read())
                print(f">> API 업데이트 완료: {result}")
            except urllib.error.HTTPError as e:
                print(f">> API 에러: {e.code} {e.read().decode('utf-8','replace')[:500]}")
        else:
            print(">> 한국산 코드 미발견. 스크린샷 확인 필요.")

        input("브라우저를 닫으려면 Enter...")
        await browser.close()

asyncio.run(main())
