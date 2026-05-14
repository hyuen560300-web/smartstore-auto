# 스마트스토어 자동화 (smartstore_auto)

Railway 배포 서비스: https://smartstore-auto-production.up.railway.app

## 역할
네이버 스마트스토어 상품 자동 소싱 및 등록 시스템.

## 주요 파일
- `main.py` — 진입점
- `employees.py` — 자동화 작업 모듈

## 실행 환경
- Python, Railway 배포
- 환경변수: NAVER_CLIENT_ID, NAVER_CLIENT_SECRET, SMARTSTORE_NAVER_ID, SMARTSTORE_NAVER_PW

## 주의사항
- Windows 환경. find/grep/xargs 대신 Python pathlib/glob 사용
- 수정 후 Railway 자동 배포됨
