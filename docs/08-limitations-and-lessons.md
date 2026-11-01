# 08 Limitations and lessons

A senior engineer states the weaknesses before the reviewer finds them.

## Known limitations of this build

| Area | Limitation | What I would do next |
|---|---|---|
| Data volume | Three months (Oct to Dec 2025). No year-over-year, no seasonality baseline | Load 24 months; add YoY and seasonal-index measures |
| Targets | No budget or prior-year data | Add a targets table; variance-to-plan visuals |
| Margin | 33% of products have no unit cost, so margin is not reported | Source cost from finance; report margin only where cost is complete |
| Anonymous sales | 47.7% of lines have no customer (mostly in store) | Push loyalty capture at the till; measure capture rate as a KPI (already on the report) |
| Churn model | AUC about 0.58 | More history, rolling labels, calibration, compare to baselines, MLflow |
| Outlook | Trailing 28-day run-rate projection, not a statistical forecast | Proper time-series model (Prophet, ETS) once there is a year of data |
| Load pattern | Full overwrite each run | Incremental `MERGE` with a watermark |
| Surrogate keys | `row_number()` keys are not stable across rebuilds | Persisted key map or hash keys |
| SCD | Type-1 (latest wins); no history of customer/product changes | Type-2 dimensions where history matters |
| Pipeline | Deployed and structurally validated; notebooks run individually | Scheduled end-to-end runs with alerting |
| LLM Q&A | Fabric Data Agent and Copilot blocked on a trial capacity (`UnsupportedCapacitySKU`); report has the native Q&A visual instead | Paid capacity (F2+), then Data Agent over the model with instructions and example questions |
| Security | No row-level security, no sensitivity labels | RLS by region/store; label PII columns; mask email/phone for non-CRM roles |
| PII | Customer names, emails, phones sit in Gold | Separate a PII dimension with restricted access, or hash/mask |

## Lessons learned

1. **Profile first, clean second.** Every cleaning rule should trace to something you saw in the data.
2. **Reconcile in money, not just rows.** Row counts matching with the wrong amounts is the most dangerous silent failure.
3. **Valid is not the same as rendering.** Generated report JSON can pass every lint and still show nothing. Look at screenshots.
4. **Prefer explicit over clever.** Regex-guarded date formats beat lenient parsing; explicit JSON schemas beat inference.
5. **Say what the model cannot do.** The report's About page and the churn narrative state the limits. Stakeholders trust numbers more when the caveats are visible.
6. **Know your platform limits early.** Capacity SKU, tenant switches and preview features decide what you can ship.
7. **Automate the boring parts.** Generators for notebooks, model, report and pipeline made 100+ iterations cheap.

## Interview talking points (60-second version)
> "I built a medallion lakehouse on Fabric from five messy sources. Bronze lands everything as strings with lineage and reconciles to the client's inventory. Silver applies regex-guarded parsing, key standardisation and a recency-then-completeness dedupe. Gold is a star schema with an Unknown member and 25 automated checks, reconciled to the penny across layers. On top, a Direct Lake model and a ten-page report with rules-based narratives, plus a churn model I report honestly at AUC 0.58 with the limits stated. Everything is code-generated and version-controlled."
