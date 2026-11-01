# 07 Build-it-yourself playbook

Use this to do the whole thing again on a new dataset, with minimal AI help. Work top to bottom and tick things off.

## Phase 0: Frame it (30 minutes, no code)
- [ ] Who consumes this and what 5 decisions will they make? (Write the questions down. They become your measures.)
- [ ] List every source: system, format, size, owner, refresh cadence, expected row count.
- [ ] Decide the **grain** of the main fact in one sentence.
- [ ] Decide the metric definitions (net vs gross, returns, cancellations) and write them in the notebook header.

## Phase 1: Get data and a workspace
- [ ] Find data: Kaggle, data.gov.uk, NYC Open Data, TfL, Companies House, or generate messy data with Python (`faker`, `numpy`). Aim for 2 to 3 sources in different formats (CSV + JSON + Excel).
- [ ] Create a Fabric workspace and Lakehouse (trial is enough to start).
- [ ] Upload to `Files/landing/<source>/`.
- [ ] Keep an inventory file with expected row counts.

## Phase 2: Bronze
- [ ] `CREATE SCHEMA bronze/silver/gold`.
- [ ] Read each source **all as strings**, add `ingestion_timestamp`, `batch_id`, `source_file`.
- [ ] Write Delta tables; `assert` counts equal the inventory.

Template:
```python
df = spark.read.option("header", True).csv("Files/landing/x/*.csv")
df = (df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.col("_metadata.file_path")))
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.x")
```

## Phase 3: Profile before you clean
Before writing Silver, **look at the data**. For each column: distinct values, null %, min/max, regex patterns.
```python
df.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]).show()
df.groupBy("status").count().show()                      # find typos
df.select(F.regexp_replace("date_col","[0-9]","9")).distinct().show()   # reveals date formats
```
Every finding becomes a cleaning rule, with the evidence kept in your notes.

## Phase 4: Silver
- [ ] Write the helper functions once (`t`, `digits`, `money`, `parse_ts`, `to_bool`, key builders, `dedupe`).
- [ ] One block per entity: select + type + standardise, filter null keys, dedupe, save.
- [ ] Keep `_raw` copies of keys you transform.
- [ ] Row-level DQ exceptions table.
- [ ] Reconcile: bronze rows minus silver rows, with every difference explained.

## Phase 5: Gold
- [ ] Surrogate keys, Unknown member (-1), generated date dimension.
- [ ] Fact with left joins to the dimensions.
- [ ] DQ framework: every check is `select count(*) ... ` that must be 0.
- [ ] Reconcile in money: sum silver = sum gold.
- [ ] `OPTIMIZE`.

## Phase 6: Insight
- [ ] Semantic model (Direct Lake on the Gold tables): relationships, a Date table, 10 core measures, formats, descriptions, hidden keys.
- [ ] Report: one question per page, KPIs on top, charts in the middle, narrative at the bottom. Use the visual that suits the question.
- [ ] Add predictive value (churn, forecast, anomaly) *and state its accuracy*.
- [ ] Screenshot every page and look for overlap, clipped text, blank visuals.

## Phase 7: Operate
- [ ] Pipeline with dependencies, timeout, retry policy, failure alert.
- [ ] Schedule it.
- [ ] Git integration and Dev/Test/Prod.
- [ ] A README and an About page explaining caveats.

## Prompts that work when you do use an AI assistant (but verify the output)
* "Here is a sample of 20 distinct values of this column. Propose a cleaning rule and a regex that classifies the formats."
* "Write a PySpark `dedupe` that keeps the latest record and breaks ties by completeness. Explain the window."
* "Here is my fact grain and tables. What reconciliation checks would a reviewer expect?"
* "Review this DAX for context-transition mistakes and divide-by-zero."

## Self-test: can you answer these without notes?
1. Why is Bronze all strings?
2. What is the grain of your fact, and what breaks if it is wrong?
3. Why left join then check, instead of inner join?
4. What is a surrogate key and why is `row_number()` imperfect for it?
5. How do you prove no money was lost between layers?
6. Direct Lake vs Import: when would you choose each?
7. Why a time-based split for churn labels?
8. Why is AUC 0.58 still useful, and how should you present it?
9. How would you make this incremental instead of overwrite?
10. How would you handle a source that changes schema?

### Answers to 9 and 10 (the "senior" ones)
* **Incremental:** Bronze append with `batch_id`; Silver `MERGE INTO` on the business key keyed by latest `ingestion_timestamp` (or Delta change data feed); Gold dimension upserts and fact `MERGE` on `order_line_key`; watermark table recording the last processed batch.
* **Schema drift:** Bronze strings + `mergeSchema`, an explicit schema contract in Silver (`from_json` with schema), and a drift check that diffs the column list against the expected one and fails or alerts.
