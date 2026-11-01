# 05 Churn model

Code: [`notebooks/04_gold_churn.py`](../notebooks/04_gold_churn.py)

## Goal
For every **identified** customer, estimate the probability they will *not* buy in the next 30 days (`churn_risk = 1 - P(buy)`), and the monthly revenue that puts at risk. Output: `gold.customer_churn_score`.

## Scope decisions
* Only customers with `customer_key > 0` (guests cannot be targeted).
* Exclude `gdpr_deleted` customers. This is both a legal and an ethical requirement: do not model people who asked to be forgotten.
* Sales only for features (returns excluded from history), return rate used as its own feature.

## Features (`features(asof, window_days=61)`)
All computed **as of a date**, using only data on or before it:

| Feature | Meaning |
|---|---|
| `recency` | days since last purchase |
| `tenure` | days since first purchase |
| `orders_all`, `spend_all` | lifetime frequency and monetary value |
| `active_days`, `avg_gap` | distinct purchase days and average days between them |
| `orders_w`, `spend_w` | activity in the last 61 days |
| `channels` | number of channels used |
| `return_rate` | share of lines that are returns |
| `aov` | average order value |

This is the classic **RFM** idea (recency, frequency, monetary) extended with behaviour.

## Avoiding leakage: how the label is built
A time split, not a random one:

| | Features as of | Label |
|---|---|---|
| Training | **30 Nov 2025** | did the customer purchase 1 to 31 Dec? (1/0) |
| Scoring | **31 Dec 2025** | predict the next 30 days |

If you computed features through 31 Dec and labelled with December purchases, the model would "see the answer". Always separate the feature window from the label window.

## Model
`GBTClassifier(maxIter=60, maxDepth=4, seed=42)` (gradient-boosted trees) on a `VectorAssembler`. 75/25 random split of the training population for evaluation, metric **AUC** (area under ROC).

## Output enrichment
* `risk_band` from relative quantiles: High = top 20% of risk, Medium = next 30%, Low = the rest.
* `rfm_segment` rules: Champions, Loyal, Promising, Needs attention, Lapsed.
* `avg_monthly_spend = lifetime_spend / 3.07` (months of data), `expected_30d_spend = p_buy * avg_monthly_spend`, `revenue_at_risk = churn_risk * avg_monthly_spend`.
* `model_auc` stored on every row so the report can display the model's own quality.

## Results and honest reading
* 39,113 customers scored; **AUC about 0.58**, only a little better than a coin flip (0.5).
* 9,428 in the High band; about £3.45M/month revenue at risk (an *expected-value ranking*, not a forecast).

Why so weak? Three months of history means "churn" is mostly noise, and one December label is a thin target. The report says so on the About page and in the Churn narrative: **use it to rank and prioritise a win-back list, not to predict an individual**. A model you over-sell is worse than no model.

## How to improve it (good interview answer)
1. 12+ months of history, a rolling label over multiple cut-off dates.
2. Seasonality features (Black Friday and Christmas distort Q4).
3. Calibrate probabilities and compare to a simple baseline (recency-only logistic regression).
4. Track the model with MLflow in Fabric; log AUC, features and the run per batch.
5. Evaluate with lift/precision at top decile, which is what a campaign team actually uses.
