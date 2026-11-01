# LinkedIn post (copy and paste)

I just built an end-to-end data platform in Microsoft Fabric, and I documented every step so I can rebuild it without help.

The project is "Northstar Retail". It takes messy store (POS) and e-commerce data and turns it into a decision-ready report.

What I built:
- Bronze, Silver and Gold medallion layers in PySpark and Delta, on a Fabric Lakehouse
- Silver cleaning of messy IDs, dates and cancelled orders
- 25 automated data-quality checks (18 on Gold, 7 reconciliation checks back to Silver)
- A Data Factory pipeline that orchestrates the notebooks
- A Direct Lake semantic model with DAX measures
- A 10-page Power BI report with churn scoring, written commentary and a natural-language Q&A page

The numbers: 391,306 sales lines, £38.8M net sales, and 9,428 high-risk customers carrying about £3.45M of revenue at risk.

Honest notes: the churn model's AUC is only 0.58, so I treat it as a ranking aid, not a promise. Fabric Data Agents and Copilot need a paid capacity, so they were blocked on my trial. I ran the notebooks one by one and have not yet run the pipeline end to end.

Biggest lesson: most of the work is in the data quality and the reconciliation, not the charts.

Code, docs and a build-it-yourself playbook: https://github.com/QuestionMarketing/northstar-retail-medallion

#MicrosoftFabric #DataEngineering #AnalyticsEngineering #PowerBI #MedallionArchitecture #PySpark #DeltaLake #Retail

---

## Short version

Built an end-to-end Microsoft Fabric medallion project (Bronze, Silver, Gold, pipeline, 25 DQ checks, Direct Lake model, 10-page Power BI report with churn and Q&A). Fully documented so I can rebuild it myself. Repo: https://github.com/QuestionMarketing/northstar-retail-medallion #MicrosoftFabric #DataEngineering
