# -*- coding: utf-8 -*-
"""전자책 상품 네이버 스마트스토어 등록 스크립트."""
import asyncio, sys, os, json
sys.stdout.reconfigure(encoding="utf-8")

# .env 로드
from pathlib import Path
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

from main import NaverCommerceAPI

EBOOK_PAYLOAD = {
    "originProduct": {
        "statusType": "SALE",
        "saleType": "NEW",
        "leafCategoryId": "50005582",  # 전자책 카테고리
        "name": "AI 이커머스 자동화 실전 가이드 v2 (전자책 PDF) — 스마트스토어 무인운영 시스템",
        "detailContent": """
<div style="font-family:sans-serif;max-width:700px;margin:0 auto;color:#222;">
<h2 style="color:#4f46e5;">📘 잠자는 동안 스마트스토어가 팔린다</h2>
<p style="font-size:16px;line-height:1.8;">
3,000만원 날리고 1년 반 만에 완성한 AI 자동화 시스템.<br>
1인 셀러가 AI 팀을 갖는 법을 17페이지에 담았습니다.
</p>

<h3>📋 목차</h3>
<ul style="line-height:2.2;font-size:15px;">
<li>CHAPTER 01 — 왜 1인 셀러에게 자동화가 필수인가</li>
<li>CHAPTER 02 — AI 자동화의 4가지 핵심 영역</li>
<li>CHAPTER 03 — 스마트스토어 상품 자동 소싱·등록</li>
<li>CHAPTER 04 — AI 숏폼 영상 자동 제작·업로드</li>
<li>CHAPTER 05 — 네이버 SEO 타이틀 자동 최적화</li>
<li>CHAPTER 06 — 텔레그램 AI 일일 보고봇</li>
<li>CHAPTER 07 — 실전 운영 후기 & 달라진 것들</li>
<li>CHAPTER 08 — 지금 당장 시작하는 법</li>
<li>FAQ — 자주 묻는 질문</li>
</ul>

<h3>✅ 구매 후 이용 방법</h3>
<p style="font-size:15px;line-height:1.8;background:#f3f4f6;padding:16px;border-radius:8px;">
결제 완료 후 아래 링크에서 즉시 PDF 다운로드 가능합니다.<br>
<strong>▶ 다운로드 링크: https://cafe24-auto-production.up.railway.app/ebook/download</strong>
</p>

<h3>📌 이런 분께 추천합니다</h3>
<ul style="line-height:2.2;font-size:15px;">
<li>스마트스토어를 시작했지만 상품 등록이 너무 힘든 분</li>
<li>매일 반복 업무에 지쳐가는 1인 셀러</li>
<li>AI로 자동화하고 싶은데 어디서 시작할지 모르는 분</li>
<li>숏폼 마케팅을 자동화하고 싶은 분</li>
</ul>

<p style="margin-top:24px;font-size:13px;color:#888;">
※ 디지털 상품 특성상 결제 완료 후 환불이 어렵습니다. 구매 전 목차를 꼭 확인해주세요.<br>
※ 문의: 카카오톡 mnm1876
</p>
</div>
""",
        "images": {
            "representativeImage": {
                "url": "https://images.unsplash.com/photo-1611532736597-de2d4265fba3?w=800&auto=format&fit=crop&q=80"
            },
            "optionalImages": [],
        },
        "salePrice": 19900,
        "stockQuantity": 9999,
        "deliveryInfo": {
            "deliveryType": "NOTHING",
            "deliveryAttributeType": "NORMAL",
            "deliveryFee": {
                "deliveryFeeType": "FREE",
                "deliveryFeePayType": "PREPAID",
                "baseFee": 0,
                "freeConditionalAmount": 0,
            },
            "claimDeliveryInfo": {
                "returnDeliveryFee": 0,
                "exchangeDeliveryFee": 0,
                "deliveryCompany": "CJGLS",
                "returnDeliveryCompany": "CJGLS",
            },
        },
        "detailAttribute": {
            "minorPurchasable": False,
            "naverShoppingSearchInfo": {
                "modelInfo": "AI 이커머스 자동화 실전 가이드 v2",
                "manufacturerName": "The Pick",
                "brandName": "The Pick",
            },
            "afterServiceInfo": {
                "afterServiceTelephoneNumber": "010-9299-9666",
                "afterServiceGuideContent": "카카오톡 mnm1876으로 문의해주세요.",
            },
            "originAreaInfo": {
                "originAreaCode": "0200037",
                "content": "대한민국",
                "plural": False,
                "importer": "해당없음",
            },
            "productInfoProvidedNotice": {
                "productInfoProvidedNoticeType": "ETC",
                "etc": {
                    "returnCostReason": "단순변심",
                    "noRefundReason": "디지털 콘텐츠 특성상 다운로드 후 환불 불가",
                    "qualityAssuranceStandard": "PDF 파일 정상 다운로드",
                    "compensationProcedure": "카카오톡 mnm1876 문의",
                    "troubleShootingContents": "다운로드 링크 재발송",
                },
            },
            "isbnInfo": {},
            "releaseDate": "",
        },
        "customerBenefit": {
            "immediateDiscountPolicy": {},
            "purchasePointPolicy": {},
            "giftPolicy": {},
            "freebiePolicy": {},
        },
    },
    "smartstoreChannelProduct": {
        "naverShoppingRegistration": True,
        "channelProductDisplayStatusType": "ON",
    },
}

async def main():
    api = NaverCommerceAPI()
    token = await api.get_token()
    print(f"토큰 획득: {token[:20]}...")

    # 먼저 카테고리 존재 확인
    print("카테고리 확인 중...")
    result = await api.register_product(EBOOK_PAYLOAD)
    print("등록 결과:", json.dumps(result, ensure_ascii=False, indent=2))

asyncio.run(main())
