# school_breakfast

학교급식 공급-수요 불균형 조기탐지 파이프라인

공공데이터 보다가 만들었다. 급식 계약은 월초에 묶어서 하는데 실제 소비는 날마다 달라서, 월말에 특정 품목이 바닥나는 경우가 생긴다. 카테고리별로 계약금액(공급)이랑 급식 인원 × 단가(수요)를 누적으로 비교해서 위험 등급을 잡는 게 핵심이다.

수요 예측은 LR이랑 RF 둘 다 돌려서 RMSE 낮은 쪽 자동 선택. 피처는 lag 1·3일, 7일 이동평균, 학생 수, 요일이고 날짜 순 80/20 split.

## 구조

```
main.py          파이프라인 진입점
config.py        DB 설정, 상수
connector.py     MySQL 연결
loader.py        CSV 전처리, 피처 생성
inserter.py      DB 적재
predictor.py     LR / RF 수요 예측
detector.py      공급-수요 균형 계산, 위험 판정
visualizer.py    차트 생성 (output/ 저장)
init_schools.py  학교 마스터 초기 적재
```

## 실행

```bash
pip install -r requirements.txt

mysql -u root -p < school_inventory.sql
python init_schools.py

python main.py
python main.py --skip-insert  # CSV -> DB 적재 건너뛰고 재분석만
```

`--skip-insert`는 예측은 CSV 기준, 공급-수요 계산은 DB 기준으로 돌린다.

## 환경 설정

`.env.example`을 `.env`로 복사하고 DB 비밀번호 넣으면 된다.

## 데이터

`data/raw/`에 CSV 파일 필요. 수집 범위는 서울 초등학교 기준.

- `meal_records.csv` - 급식 운영 기록 (한국농수산식품유통공사)
- `contracts.csv` - 학교급식 계약정보 (한국농수산식품유통공사)
- `학교기본정보.csv` - 학교명, 지역, 종류 (교육통계서비스)
- `학교학생수.csv` - 학교별 학생 수 (교육통계서비스)

학교 데이터는 `python init_schools.py`로 먼저 DB에 넣어야 한다.

`menu_recipe` 테이블은 `school_inventory.sql`에 `중식`/`석식` 레시피가 들어 있다. 메뉴 유형이 추가되면 SQL에 직접 넣으면 된다.

## 출력

`output/`에 저장됨

- `01_risk_heatmap.png` - 학교 × 카테고리 위험 히트맵
- `02_model_comparison.png` - LR vs RF 예측 비교
- `03_stock_balance.png` - 공급 vs 수요 비용 월별 비교
- `predictions.csv` - 전체 데이터 수요 예측 결과
- `model_comparison.csv` - 모델 성능 수치
