"""
pamphlet_full_suite.html 섹션 스크린샷 → Cloudinary 업로드
"""
import asyncio, sys, os, json, tempfile, urllib.request
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from playwright.async_api import async_playwright
from dotenv import load_dotenv
load_dotenv()

HTML_PATH = r"C:\Users\USER\Desktop\CEO-AI\docs\pamphlet_full_suite.html"
CLOUDINARY_CLOUD = os.getenv("CLOUDINARY_CLOUD_NAME", "dev11ovzx")
CLOUDINARY_PRESET = os.getenv("CLOUDINARY_UPLOAD_PRESET", "shortform_video_unsigned")


def upload_cloudinary_file(img_path: str) -> str:
    """파일을 multipart/form-data로 Cloudinary 업로드"""
    boundary = "----CloudinaryBoundary"
    with open(img_path, "rb") as f:
        img_data = f.read()

    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="upload_preset"\r\n\r\n'
        f"{CLOUDINARY_PRESET}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="img.png"\r\n'
        f"Content-Type: image/png\r\n\r\n"
    ).encode() + img_data + f"\r\n--{boundary}--\r\n".encode()

    req = urllib.request.Request(
        f"https://api.cloudinary.com/v1_1/{CLOUDINARY_CLOUD}/image/upload",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())["secure_url"]


async def main():
    file_url = "file:///" + HTML_PATH.replace("\\", "/")
    urls = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 800, "height": 1000})
        await page.goto(file_url, wait_until="networkidle")
        await asyncio.sleep(2)

        # 섹션 목록 - CSS 선택자로 정확하게
        sections_js = await page.evaluate("""
            () => [...document.querySelectorAll('.section')].map((s,i) => ({
                idx: i,
                class: s.className,
                text: s.textContent.trim().substring(0,40)
            }))
        """)
        print(f"섹션 {len(sections_js)}개: {sections_js[:6]}")

        # 스크린샷 찍을 영역
        capture_targets = [
            ("hero",  ".hero"),
        ]
        # section들 중 처음 4개
        for item in sections_js[:4]:
            capture_targets.append((f"section_{item['idx']}", f".section:nth-child({item['idx']+1})"))

        tmpdir = tempfile.mkdtemp()
        for name, sel in capture_targets:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    fpath = os.path.join(tmpdir, f"{name}.png")
                    await el.screenshot(path=fpath, type="png")
                    url = upload_cloudinary_file(fpath)
                    urls.append(url)
                    print(f"[{name}] OK: {url}")
                else:
                    print(f"[{name}] selector 없음: {sel}")
            except Exception as e:
                print(f"[{name}] 실패: {e}")

        # 전체 상단 (800x600)
        fpath = os.path.join(tmpdir, "top.png")
        await page.screenshot(path=fpath, clip={"x":0,"y":0,"width":800,"height":700})
        try:
            url = upload_cloudinary_file(fpath)
            urls.append(url)
            print(f"[top] OK: {url}")
        except Exception as e:
            print(f"[top] 실패: {e}")

        await browser.close()

    with open(r"C:\Users\USER\Desktop\smartstore_auto\captured_images.json", "w") as f:
        json.dump(urls, f, indent=2)
    print(f"\n총 {len(urls)}개 완료")
    for u in urls:
        print(u)

asyncio.run(main())
