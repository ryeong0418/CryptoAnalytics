## **📌Project Name: CryptoAnalytics**


## **🧾 Overview**
이 프로젝트는 Airflow를 활용하여 Upbit OpenAPI에서 암호화폐 시세 데이터를 주기적으로 수집하고, 이를 Azure Data Lake Storage Gen2에 적재한 후, Databricks에서 메달리온 아키텍처(Medallion Architecture) 기법으로 처리하여 Tableau로 시각화하는 End-to-End 데이터 파이프라인입니다.


## 🏗️ Architecture

![architecture](/img/architecture.jpg)

### 1️⃣ Data Source
- Upbit OpenAPI
  - 암호화폐 시세, 거래량, 거래대금 등의 시계열 데이터 제공
  - JSON 형식 응답

### 2️⃣ Extract
- Apache Airflow DAG에서 Upbit OpenAPI를 호출
- HTTP 요청 → JSON 데이터 수신
- Python 스크립트를 통해 구조화(DataFrame 변환)
- 원본 데이터의 무결성을 보존하기 위해 변환 없이 저장 준비

### 3️⃣ Load
- 수집한 데이터를 Azure Data Lake Storage Gen2에 업로드
- 저장 구조 예시:
>/KRW-BTC/KRW-BTC-2024-01-01.json

### 4️⃣ Transform
- Pyspark를 활용한 대용량 병렬 처리
- Databricks에서 Medallion Architecture 적용
  1. **Bronze Layer**
  - 원본 데이터 저장 (Raw)
  2. **Silver Layer**
  - 결측치 처리, 불필요 컬럼 제거, 시간대 변환(UTC→KST)
  3. **Gold Layer**
  - 집계 지표 생성 (월별 평균 수익률, 변동성, 거래량/거래대금 변화율 등)


### 5️⃣ Batch Processing
- Airflow에서 Databricks Jon API를 호출하여 변환 작업 자동화
- DAG 스케줄:
  - 특정 시간에 실행
  - Extract → Load → Databricks Transform 순차 실행
- Delta Lake 포맷 사용으로 ACID 보장 및 데이터 품질 관리

### 6️⃣ Visualization
![visualization](/img/Tableau%20Visualization.jpg)
- Tableau에서 Gold Layer 데이터 셋 연결
- 주요 대시보드:
  - 월별 평균 수익률 분석
  - 가격 vs 거래량/거래대금 상관 분석
  - 일별 변동성 지표(고가·저가 기반)
  - 거래량/거래대금 증감률 모니터링
  

## 🛠️ Tech Stack
- Orchestration: Apache Airflow (Python)
- Data Source: Upbit OpenAPI
- Storage: Azure Data Lake Storage Gen2
- Processing: Azure Databricks (PySpark, Medallion Architecture)
- Visualization: Tableau



