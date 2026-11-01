# 03 Gold and data quality

Code: [`notebooks/03_gold.py`](../notebooks/03_gold.py)

## Gold: model for the business, not for the source

### 1. Surrogate keys
```python
def sk(col): return F.row_number().over(Window.orderBy(col)).cast("int")
```
Integer keys give small, fast joins and insulate the model from source key changes. Caveat to know for interviews: `row_number()` keys are **not stable across rebuilds** if the member set changes. In production you would persist a key map (or use a hash key). We use `xxhash64(order_line_id)` for the fact key, which *is* stable.

### 2. Dimensions
* **dim_customer**: all Silver customer columns plus an **Unknown member** (`customer_key = -1`, "Guest / anonymous") added with `unionByName(allowMissingColumns=True)`. Facts for anonymous shoppers point at -1, so no fact row is ever lost in an inner join and "% of sales we can identify" is a measure you can compute (about 52% of lines identified).
* **dim_product**: adds a derived `price_band` (Premium >= £100, Mid >= £40, Value) and `row_selection_logic` documenting which duplicate won.
* **dim_store**: derives `is_selling_location`, `is_fulfilment_node`, `is_online_node` from store type and flags. One store table with several roles instead of three copies.
* **dim_date**: generated, not loaded. `sequence(min_date, max_date, interval 1 day)` exploded into rows, with year/quarter/month/week/day-name/weekend. Bounds come from the data so the calendar is always complete and contiguous (a requirement for time-intelligence DAX).

### 3. Fact table: `fct_order_line`
Union the two Silver sales tables (POS + non-cancelled e-com) with an identical column list, then **look up surrogate keys** with left joins:
`customer_id -> customer_key` (null becomes -1 via `coalesce`), `sku -> product_key`, `store_id -> store_key`, `fulfilment_store_key -> fulfilment_store_sk`.

Derived measures-at-rest: `return_quantity`, `returned_amount` (only when `is_return`), `date_key` (`yyyyMMdd` int), `currency = GBP`.

Left joins plus later RI checks is the safe pattern: an inner join would silently drop unmatched rows and your totals would be wrong with no error.

### 4. Aggregate: Materialized Lake View
`gold.mlv_daily_channel_sales` pre-aggregates by day/channel/sub-channel/department/category. The notebook tries `CREATE MATERIALIZED LAKE VIEW` and, if the feature is unavailable, falls back to writing the same SQL as a table. The outcome is recorded in `mlv_status`. Lesson: *feature-flag your fancy features with a fallback so the pipeline stays green.*

### 5. Optimise
`OPTIMIZE gold.<table>` compacts small files; V-Order and optimize-write are enabled via Spark conf for fast Direct Lake reads.

## The data-quality framework (25 checks + info rows)

Results land in `gold.dq_results` (and a CSV in `Files/validation/` for the report/audit). Sample output: [`gold_dq_results_sample.csv`](gold_dq_results_sample.csv).

A tiny framework, reusable anywhere:

```python
def chk(layer, name, sql, expect_zero=True):
    v = spark.sql(sql).first()[0]
    res.append((layer, name, float(v), "PASS" if v == 0 else "FAIL"))
```
**Design: every check is a query that returns 0 when healthy.** That makes them uniform, easy to add, and easy to alert on.

| Category | Examples | Catches |
|---|---|---|
| **Uniqueness** | `count(*) - count(distinct key)` on every dimension key and fact key | duplicate members, fan-out joins |
| **Referential integrity** | `count(*) ... where product_key is null`; `left anti join` fact to dim | orphan facts that vanish from visuals |
| **Metric integrity** | `abs(net + vat - gross) > 0.01`; `abs(net*rate - vat) > 0.05` | wrong VAT maths |
| **Sign logic** | returns must have qty<=0 and net<=0; sales must not | the classic returns-as-positive bug |
| **Completeness** | null timestamp, null gross | unparsed dates |
| **Reconciliation** | bronze to silver rows, distinct IDs, silver to gold rows, **silver to gold net sales in £** | rows or money lost between layers |
| **Null analysis (INFO)** | % customer_key = -1 (47.67%), % unit cost null (32.98%) | reported, not failed: these are facts about the data, not bugs |

Final gate: the notebook ends with `notebookutils.notebook.exit("gold_ok" or "gold_checks_failed: [...]")`. A pipeline can branch on that string.

### Result
All 25 PASS. Fact: 391,306 rows = 260,000 POS + 153,389 e-com - 22,083 cancelled. Net £38,764,537.75; gross £45,624,952.15.

## Interview-grade answers from this layer

* *"How do you know your numbers are right?"* Layered reconciliation to the penny, plus invariants (net + VAT = gross).
* *"What happens to bad data?"* Kept and flagged (`dq_exceptions`), never silently dropped, except cancelled lines, which are excluded by an explicit rule and counted.
* *"How do you handle unknown dimension members?"* A -1 row, not NULL keys.
