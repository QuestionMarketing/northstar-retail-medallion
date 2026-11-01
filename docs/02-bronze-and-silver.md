# 02 Bronze and Silver walkthrough

Code: [`notebooks/01_bronze.py`](../notebooks/01_bronze.py), [`notebooks/02_silver.py`](../notebooks/02_silver.py)

## Bronze: land it, label it, count it

### Principle
Bronze is a *faithful copy*. No parsing, no renaming business logic, no filtering. If the source says `"  c-00012 "`, Bronze says `"  c-00012 "`.

### Step by step

**1. Create schemas.** `CREATE SCHEMA IF NOT EXISTS bronze/silver/gold`. Schemas are the physical expression of the layers.

**2. Everything is a string.** `spark.read.option("header", True).csv(...)` with no `inferSchema`, so every column is a string. Reason: inference would silently turn `"007"` into `7` or fail on a bad date, destroying evidence. Typing is Silver's job.

**3. Lineage columns on every table**

```python
def add_lineage(df, source_file):
    return (df.withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("batch_id", F.lit(batch_id))        # one uuid per run
              .withColumn("source_file", source_file))        # F.col("_metadata.file_path")
```
`_metadata.file_path` is Spark's hidden file-source column. It records which physical file each row came from, including which of the two POS part files.

**3. Format-specific handling**

| Source | Technique | Why |
|---|---|---|
| CSV / TSV | `.csv(..., header=True)`; TSV adds `sep="\t"`; glob `part*.csv` reads both POS files | trivial |
| E-com JSONL | read as **text** (`spark.read.text`), keep `raw_json`, extract only `order_no` with `get_json_object` | The nested structure is flattened in Silver. Bronze keeps the raw document so a schema change upstream cannot break ingestion |
| Marketing / store JSON | `multiLine=True`, `explode("records")`, then `to_json()` any array/struct column and cast the rest to string | Wrapped in an envelope object; nested values preserved as JSON strings |
| Excel | `pandas.read_excel(..., dtype=str)` then `createDataFrame` with an all-string schema | Spark has no native xlsx reader. Pandas on the driver is fine for 8K rows. `dtype=str` stops pandas inventing types |

**4. Reconcile to the client inventory.** The client supplied `raw_file_inventory.csv` with expected row counts:

```python
expected = {"crm_customers": 93500, "crm_marketing_prefs": 83000, "ecom_orders_raw": 70000,
            "pos_transactions": 260000, "store_master": 650, "product_master": 8180, "channel_lookup": 6}
assert all(status == "PASS" ...)   # notebook fails if any count differs
```
This is the cheapest, highest-value control in the whole project: you prove *nothing was lost on the way in*.

## Silver: one trustworthy version of each entity

Silver builds small, reusable **cleaning functions**, then applies them per table. Learn these; they generalise to any messy dataset.

### The toolbox (top of `02_silver.py`)

| Function | Problem solved | Technique |
|---|---|---|
| `t(c)` | blanks and whitespace | `nullif(trim(col), '')`: empty string becomes NULL |
| `digits(c)` | IDs like `c-00012`, `CUST 12`, `0000012` | strip all non-digits |
| `cust_key / sku_key / store_key` | standardise keys | `digits` then left-pad to fixed width and re-prefix: `C0000012`, `SKU000123`, `ST0042`. Uses last N digits so legacy long IDs collapse |
| `money(c)` | `"£1,234.50"`, `"-12.0"` | remove everything except digits, `.` and `-`, cast to double |
| `parse_ts(c)` | **11 different date formats** | a `CASE WHEN regex THEN to_timestamp(col, fmt)` chain. The regex guard matters: it makes the format explicit, so `03-04-2025` is deliberately month-first and never mis-parsed by a lenient parser |
| `to_bool(c)` | `Y/yes/true/1/T` vs `N/no/false/0/F` | explicit lists; unknown stays NULL (not silently false) |
| `vat_frac(c)` | `"20%"`, `0.2`, `20`, `"zero"` | `zero` is 0; values >= 1 are divided by 100; otherwise already a fraction |
| `country(c)` | `UK/GB/GBR/United Kingdom` | map to ISO `GB` |
| `dedupe(df, key, order_cols, completeness_cols)` | duplicates | `row_number()` over a window partitioned by key, ordered by recency then by *completeness* (count of non-null fields). Keep row 1. Also records `_dups` (how many source rows collapsed) |

### Dedupe logic, the important bit
```python
nn = sum(when(col.isNotNull(), 1).otherwise(0) for c in completeness_cols)
w  = Window.partitionBy(key).orderBy(<recency desc nulls last>, nn.desc())
df.withColumn("_rn", row_number().over(w)).filter("_rn = 1")
```
Rule stated in words (and stored in the `dedupe_method` column): *"latest `last_profile_update`, then most complete row"*. A rule you cannot state in one sentence is a rule you cannot defend.

### Table by table

* **channel_map**: lower-case/trim, `distinct`. Used as a broadcast join to turn source channel codes into `business_channel` (Online/Store) and `sub_channel`.
* **stores**: `store_key` from digits, `initcap` for names/cities, booleans for click-and-collect flags, dedupe by latest ingestion.
* **products**: key = `sku_key`; drop rows with no SKU; money parsed; `vat_frac`; dedupe by `last_updated_at`.
* **customers**: three extra ideas
  1. *Future-dated updates* (`last_profile_update > now`) are flagged `future_dated_update` and nulled, not trusted.
  2. *Marketing consent is a second source*, deduped separately then **left-joined**. Consent prefers the marketing system and falls back to the CRM flag (`coalesce`).
  3. *Orphan check*: marketing records whose customer is missing from CRM are counted into the exceptions list, not silently dropped.
* **pos_order_lines**: the hardest table.
  * `is_return = coalesce(return_ind, units < 0)`, a sign convention applied once: `sign = -1 if return else +1`, and amounts are `abs(value) * sign`. Source data had inconsistent signs, so we normalise by the flag.
  * `net = gross / (1 + vat)`, `vat_amount = gross - net`.
  * `discount_amount = abs(qty) * unit_price * signum(qty) - net_sales` (what was given away vs list price).
  * Timestamp fallback: `coalesce(parsed_timestamp, parsed_business_date)`.
  * Deterministic line key `order_line_id = "POS|order|line"`; dedupe on it.
  * Join to `channel_map` with fallback defaults (`Store` / `In-Store POS`).
* **ecom_order_lines**: parse the raw JSON with an explicit **schema string**, then `explode_outer("lineItems")` so orders with no lines are not lost. Status typos fixed (`shippped` to `Shipped`). Fulfilment type mapped to a friendly label. `store_key` is NULL for online (fulfilment store is kept separately as `fulfilment_store_key`).

### Why explicit schema for the JSON?
`from_json(raw_json, "orderNo string, ..., lineItems array<struct<...>>")`. If you let Spark infer, one odd record changes the schema for the whole batch. An explicit schema makes malformed fields NULL instead of breaking the run, and documents the contract.

### DQ exceptions table
`silver.dq_exceptions(source, record_id, issue)` lists row-level problems (unparseable timestamp, missing SKU/amount/VAT, anonymous customer). It does not remove rows; it makes the problem *visible and countable*. The 'anonymous' issue is large by design (176,309 POS lines, 11,914 web lines): in-store shoppers without a loyalty card.

## Silver checks you can reuse
After every Silver build run `count(*) vs count(distinct key)`, null rates on keys, and "bronze minus silver" row diffs. In this project: POS 260,000 in = 260,000 out; customers 93,500 raw rows resolve to 85,000 distinct people.
