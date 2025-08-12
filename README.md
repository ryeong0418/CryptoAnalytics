## **📌Project Name: CryptoAnalytics**


## **🧾 Overview**
이 프로젝트는 Airflow를 활용하여 Upbit OpenAPI에서 암호화폐 시세 데이터를 주기적으로 수집하고, 이를 Azure Data Lake Storage Gen2에 적재한 후, Databricks에서 메달리온 아키텍처(Medallion Architecture) 기법으로 처리하여 Tableau로 시각화하는 End-to-End 데이터 파이프라인입니다.


## 🏗️ Architecture

![architecture](/img/architecture.jpg)

### 1️⃣ Data Source

### 2️⃣ Extract

### 3️⃣ Load

### 4️⃣ Transform

### 5️⃣ Batch Processing

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



