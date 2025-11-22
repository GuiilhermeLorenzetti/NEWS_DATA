# Market Intelligence: Multivariate Equity Analysis

## Project Overview: Architecture Demonstration

**This project serves as a technical demonstration of a modern, cost-effective data engineering architecture.**

While the subject matter is financial analysis (correlating Price Action, News Sentiment, and Insider Trading), the primary goal is to showcase a robust **Medallion Architecture** pipeline that integrates disparate data sources into a cohesive analytical dataset.

The **Analytics** component (`analytics/` folder) serves primarily as a **Proof of Concept (PoC)** to validate the pipeline's data integrity and potential. It demonstrates that the architecture successfully transforms raw, unstructured data into actionable insights.

### Note on Data Volume & Free Tier Constraints

> **Architectural Decision**: To maintain this project as a **zero-cost demonstration**, I have exclusively used the **Free Tier** of the Groq API for LLM-based sentiment analysis.

For a production-grade financial analysis, a significantly larger volume of news data would be required to achieve statistical significance. However, the current implementation is intentionally throttled to respect the rate limits of the free plans. This demonstrates the **technical viability** of the pipeline itself—proving that the architecture can handle the logic and flow—while acknowledging that scaling the *volume* for deeper analysis would simply be a matter of upgrading to paid API tiers.

### 🛠️ Tech Stack & Cost Efficiency

*   **Language**: Python 3.12
*   **Database**: PostgreSQL (Render/Local)
*   **Transformation**: dbt (Data Build Tool) for reliable data modeling.
*   **Orchestration**:  (Next steps)
*   **AI**: **Groq API** using open-source models (e.g., Llama 3, Mixtral) for high-speed, zero-cost sentiment analysis.
*   **Data Source**: **NewsAPI** for fetching global financial news.
*   **Market Data**: `yfinance` for historical price data.

### Data Pipeline Flow

1.  **Ingestion (`get_data/`)**:
    *   Fetches stock prices (`stocks.py`).
    *   Fetches news articles and performs **real-time sentiment analysis** using Large Language Models via Groq (`news_sentiment_integrated.py`).
    *   Loads raw data into the **Bronze** layer of the Data Warehouse.
2.  **Transformation (`dbt_process/`)**:
    *   Cleans, deduplicates, and models data.
    *   **Silver Layer**: Standardized schemas.
    *   **Gold Layer**: Analytical tables ready for BI and correlation analysis.
3.  **Validation (`analytics/`)**:
    *   Python scripts to query the Gold layer.
    *   Generating preliminary insights on Insider vs. Retail behavior.

## Project Structure

```
News_data/
├── analytics/                  # Analysis scripts & insights
│   ├── analytics.py            # Main analysis logic
│   └── readme.md               # Detailed analysis findings
├── dbt_process/                # dbt project for data transformation
│   ├── newsdata/               # dbt models (Bronze/Silver/Gold)
│   └── dbt_project.yml         # dbt configuration
├── get_data/                   # Data ingestion scripts
│   ├── news_sentiment_integrated.py # News fetcher + LLM Sentiment Analysis
│   ├── stocks.py               # Stock price fetcher
│   └── insider_transactions.py # Insider trading data fetcher
└── requirements.txt            # Project dependencies
```