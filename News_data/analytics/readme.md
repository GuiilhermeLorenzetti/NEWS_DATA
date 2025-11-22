📊 Market Intelligence: Multivariate Equity Analysis (Sentiment & Insiders)

🔎 Project Context In this project, I developed a financial data analysis pipeline to investigate the correlation between three fundamental pillars of the stock market: Price Action, News Sentiment, and Insider Trading.

The goal was not just to plot price charts, but to understand the invisible "drivers" that anticipate market movements. The data was processed using a Medallion Architecture (Bronze/Silver/Gold) and queried via Python directly from a PostgreSQL Data Warehouse.

🧠 Key Analysis Insights By cross-referencing transactional data with news sentiment and executive (C-Level) operations, three clear patterns emerged:

1. The Insider Battle: Warning Sign or Profit Taking? A superficial analysis would suggest that insiders only sold shares during the period. However, by diving deeper into the Net Value Flow, we identified a nuanced behavior:

Massive Sell Volume: There is a predominant selling pressure, especially in high-performance stocks like NVDA and META. In NVDA, sell volume outweighed buy volume by nearly 10x.

"Smart Money" on the Buy Side: Contrary to common belief, there were strategic purchases (green bars in the charts). Although smaller in financial volume, these trades are statistically more significant as they occur against the internal liquidity trend. When an insider buys while peers are selling, it signals strong long-term confidence (discounted valuation).

2. The "Attention Economy" and Liquidity I identified a direct positive correlation (0.56) between the daily news count and trading volume, regardless of the news bias.

Insight: The market reacts to the presence of information, not just its quality. Days with news peaks ("Hype") attract immediate liquidity, validating the thesis that HFT algorithms and Day Traders use media flow as a volatility trigger, creating entry/exit opportunities regardless of whether the news is "Good" or "Bad."

3. Sentiment Intensity as a Volatility Vector Using boxplots to measure price dispersion based on sentiment_score, it was proven that extreme news (highly positive or highly negative) widens the daily price range.

Unlike days with "neutral sentiment" (where price tends to move sideways), days with high sentiment intensity show the largest percentage variations (price_change_pct). This suggests that Long/Short or options strategies (Volatility Arbitrage) are more efficient when filtered by news flow intensity.

🛠️ Tech Stack Used

Language: Python 3.12

Database: PostgreSQL (Render) via SQLAlchemy

Data Analysis: Pandas & NumPy for vector manipulation.

Visualization: Matplotlib & Seaborn (focused on dual-axis charts for correlation).

Engineering: Use of environment variables (.env) for credential security and direct connection to Gold tables.

💡 Conclusion This analysis demonstrates that trading or analyzing price in isolation is inefficient. The integration of alternative data (News and Insiders) offers a competitive advantage ("Alpha"), allowing one to anticipate volatility spikes and understand if a price drop is retail panic or a structured exit by the board.