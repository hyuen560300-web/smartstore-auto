"""공통 등록 가드 — SS·쿠팡 양쪽 레포에 동일하게 배포."""
import os


def normalize_product_id(code: str) -> str:
    """DG_/DG 접두사 + _N suffix 제거 → 순수 숫자 코드 반환.

    Examples
    --------
    "66692100"     → "66692100"
    "DG66692100"   → "66692100"
    "DG_66692100"  → "66692100"
    "DG66692100_1" → "66692100"
    """
    s = str(code).strip()
    upper = s.upper()
    if upper.startswith("DG_"):
        s = s[3:]
    elif upper.startswith("DG"):
        s = s[2:]
    return s.split("_")[0]


def assert_can_register(
    code: str,
    registered: set,
    image_url: str = "",
) -> None:
    """등록 전 공통 가드. 통과 못하면 ValueError 발생.

    1. SOURCING_PAUSED=true → 차단
    2. 정규화된 코드 중복 → 차단
    3. 이미지 URL 무효 + ALLOW_IMAGE_FALLBACK=false → 차단 (임의 대체 절대금지)
    """
    if os.getenv("SOURCING_PAUSED", "false").lower() == "true":
        raise ValueError("SOURCING_PAUSED=true — 소싱 정지 중")

    norm = normalize_product_id(code)
    norm_registered = {normalize_product_id(r) for r in registered}
    if norm in norm_registered:
        raise ValueError(f"중복 코드: {code} (정규화={norm})")

    allow_fallback = os.getenv("ALLOW_IMAGE_FALLBACK", "false").lower() == "true"
    if not allow_fallback:
        if not image_url or not str(image_url).strip().startswith("http"):
            raise ValueError(f"이미지 URL 무효/누락: {image_url!r} — ALLOW_IMAGE_FALLBACK=false")
