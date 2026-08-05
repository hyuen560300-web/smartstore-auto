"""
로컬 DG API → Railway PostgreSQL dedup → Naver Smartstore 등록
Railway IP rate-limit 우회용 로컬 파이프라인
"""
import asyncio, os, sys

# .env 로드
sys.path.insert(0, os.path.dirname(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:
    pass

TARGET = int(os.environ.get("DG_TARGET", "50"))

async def run():
    # main.py 전체 import (환경변수 필요)
    import main as m

    print(f"[로컬DG] DOMEGGOOK_API_KEY 설정: {bool(m.DOMEGGOOK_API_KEY)}")
    print(f"[로컬DG] 목표 등록 수: {TARGET}")

    # 현재 Naver 상품 수 확인
    cur_sale = await m.naver_api.count_sale_products()
    print(f"[로컬DG] 현재 판매중: {cur_sale}개")

    max_limit = int(os.getenv("MAX_PRODUCTS_LIMIT", "1000"))
    remaining_slots = max_limit - cur_sale
    limit = min(TARGET, remaining_slots)
    print(f"[로컬DG] 등록 가능 슬롯: {remaining_slots}개 → 이번 등록 목표: {limit}개")

    if limit <= 0:
        print("[로컬DG] 슬롯 없음 — 종료")
        return

    # 여름 키워드 (7월) — 다양한 카테고리
    keywords = [
        "쿨링수건", "아이스팩", "캠핑쿨러", "휴대선풍기", "자외선차단",
        "비치타올", "수영고글", "물놀이튜브", "아쿠아슈즈", "래쉬가드",
        "여름모자", "선글라스", "에코백", "미니선풍기", "쿨매트",
        "보냉백", "텀블러", "캠핑테이블", "원터치텐트", "해먹",
    ]

    result = await m.pipeline_register_from_domeggook(
        limit=limit,
        keywords=keywords,
        min_price=5000,
        max_price=100000,
        start_page=1,
    )
    print("\n[결과]", result)
    print(f"  성공: {result.get('success',0)}개")
    print(f"  중복: {result.get('duplicate',0)}개")
    print(f"  스킵: {result.get('skip',0)}개")
    print(f"  실패: {result.get('fail',0)}개")

if __name__ == "__main__":
    asyncio.run(run())
