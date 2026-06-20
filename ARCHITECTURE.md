# 🏗️ AMFI Analytics Engine – Architecture

## 📌 Overview
The AMFI Analytics Engine is a batch data pipeline that ingests, processes, and analyzes mutual fund NAV data to generate financial insights such as returns, volatility, and risk metrics.

The system is designed for scheduled execution, ensuring fresh analytics with minimal manual intervention.

---

## 🧱 System Architecture (Execution-Oriented)

```mermaid
graph TD
    A[AMFI NAV Source] --> B[Ingestion Layer]
    B --> C[Raw Data]

    C --> D[Processing Layer]
    D --> E[Clean Dataset]

    E --> F[Analytics Layer]
    F --> G[Computed Metrics]

    G --> H[Output Storage]
    H --> I[Consumers / APIs / Dashboards]

    S[Scheduler (GitHub Actions / Cron)] --> B
```

---

## ⚙️ Pipeline Layers

### 1. 🧲 Ingestion Layer
**Purpose:** Fetch latest NAV data from AMFI

**Responsibilities:**
- Pull raw data (daily basis)
- Handle failures & retries
- Ensure consistent input format

**Output:** Raw dataset

---

### 2. 🧹 Processing Layer
**Purpose:** Clean and normalize data

**Responsibilities:**
- Remove invalid rows
- Normalize scheme names
- Parse date formats
- Ensure schema consistency

**Output:** Clean dataset

---

### 3. 🧠 Analytics Layer
**Purpose:** Generate financial metrics

**Metrics:**
- Returns
- Volatility
- Drawdown
- Risk metrics

**Output:** Analytics dataset

---

### 4. 💾 Storage Layer
- JSON / CSV (current)
- Database (future)

---

### 5. 🌐 Consumption Layer
- API / Dashboards
- External tools

---

### 6. ⏱️ Scheduler
- GitHub Actions / Cron

---

## 🔄 Data Flow

1. Scheduler triggers run  
2. Ingestion fetches data  
3. Processing cleans data  
4. Analytics computes metrics  
5. Storage saves output  
6. Consumers use data  

---

## 🧠 Design Principles

- Modular pipeline
- Reproducible computations
- Scalable architecture
- Low coupling

---

## 🚀 Future Enhancements

- Historical database
- Caching (Redis)
- Workflow orchestration (Airflow)
- Advanced quant analytics
