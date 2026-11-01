# %% [markdown]
# # 04 - Gold: customer churn / retention model
# Predicts the probability that an identified customer purchases again in the next 30 days.
# Training: features as of 30 Nov 2025 (61-day window) -> label "purchased in December".
# Scoring : features as of 31 Dec 2025 (61-day window) -> churn_risk = 1 - P(purchase in next 30 days).
# Only identified (non-guest, non GDPR-deleted) customers are scored. Spark ML gradient-boosted trees, AUC on a held-out 25%.

# %%
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import datetime as dt

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

fct = spark.table("gold.fct_order_line").filter("customer_key > 0")
cust = (spark.table("gold.dim_customer").filter("customer_key > 0")
        .filter(~F.coalesce(F.col("gdpr_deleted").cast("string").isin("true", "1", "True"), F.lit(False))))
keys = cust.select("customer_key")
sales = fct.join(keys, "customer_key").filter("is_return = false") if "is_return" in fct.columns else fct.join(keys, "customer_key")
rets = fct.join(keys, "customer_key").groupBy("customer_key").agg(
    F.avg(F.when(F.col("is_return"), 1.0).otherwise(0.0)).alias("return_rate"))

def features(asof, window_days=61):
    a = F.lit(asof).cast("date")
    hist = sales.filter(F.col("order_date") <= a)
    per_day = hist.groupBy("customer_key", "order_date").agg(F.countDistinct("source_order_id").alias("o"))
    win = hist.filter(F.col("order_date") > F.date_sub(a, window_days))
    g = hist.groupBy("customer_key").agg(
        F.min("order_date").alias("first_d"), F.max("order_date").alias("last_d"),
        F.countDistinct("source_order_id").alias("orders_all"), F.sum("net_sales").alias("spend_all"),
        F.countDistinct("sales_channel").alias("channels"), F.countDistinct("order_date").alias("active_days"))
    w = win.groupBy("customer_key").agg(
        F.countDistinct("source_order_id").alias("orders_w"), F.sum("net_sales").alias("spend_w"))
    df = (g.join(w, "customer_key", "left").join(rets, "customer_key", "left")
          .withColumn("recency", F.datediff(a, "last_d"))
          .withColumn("tenure", F.datediff(a, "first_d"))
          .withColumn("avg_gap", F.when(F.col("active_days") > 1, F.col("tenure") / (F.col("active_days") - 1)).otherwise(F.lit(-1.0)))
          .withColumn("orders_w", F.coalesce("orders_w", F.lit(0))).withColumn("spend_w", F.coalesce("spend_w", F.lit(0.0)))
          .withColumn("return_rate", F.coalesce("return_rate", F.lit(0.0)))
          .withColumn("aov", F.col("spend_all") / F.greatest(F.col("orders_all"), F.lit(1))))
    return df

FEATS = ["recency", "tenure", "orders_all", "spend_all", "channels", "active_days", "orders_w", "spend_w", "return_rate", "avg_gap", "aov"]
asm = VectorAssembler(inputCols=FEATS, outputCol="features", handleInvalid="keep")

# %% [markdown]
# ## Train and evaluate

# %%
train_asof, label_from, label_to = "2025-11-30", "2025-12-01", "2025-12-31"
buyers = (sales.filter((F.col("order_date") >= label_from) & (F.col("order_date") <= label_to))
          .select("customer_key").distinct().withColumn("label", F.lit(1.0)))
tr = features(train_asof).join(buyers, "customer_key", "left").withColumn("label", F.coalesce("label", F.lit(0.0)))
tr = asm.transform(tr).select("customer_key", "features", "label").cache()
trn, tst = tr.randomSplit([0.75, 0.25], seed=42)
gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=60, maxDepth=4, seed=42)
model = gbt.fit(trn)
auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(model.transform(tst))
base = tr.agg(F.avg("label")).first()[0]
print(f"AUC={auc:.3f} base repurchase rate={base:.3f} rows={tr.count():,}")

# %% [markdown]
# ## Score customers as of 31 Dec

# %%
asof = "2025-12-31"
sc = asm.transform(features(asof))
sc = model.transform(sc).withColumn("p_buy", vector_to_array("probability")[1])
sc = sc.withColumn("churn_risk", F.round(1 - F.col("p_buy"), 4))
out = (sc.select("customer_key", F.col("last_d").alias("last_purchase_date"), F.col("recency").alias("recency_days"),
                 F.col("orders_all").alias("order_count"), F.round("spend_all", 2).alias("lifetime_spend"),
                 F.round(F.col("spend_all") / 3.07, 2).alias("avg_monthly_spend"), "churn_risk", "p_buy")
       .withColumn("risk_band", F.lit("Low"))
       .withColumn("expected_30d_spend", F.round(F.col("p_buy") * F.col("avg_monthly_spend"), 2))
       .withColumn("revenue_at_risk", F.round(F.col("churn_risk") * F.col("avg_monthly_spend"), 2))
       .withColumn("rfm_segment",
                   F.when((F.col("recency_days") <= 14) & (F.col("order_count") >= 3), "Champions")
                    .when(F.col("order_count") >= 3, "Loyal")
                    .when(F.col("recency_days") <= 30, "Promising")
                    .when(F.col("recency_days") <= 60, "Needs attention")
                    .otherwise("Lapsed"))
       .withColumn("model_auc", F.lit(round(auc, 4))).drop("p_buy"))
q50, q80 = out.approxQuantile("churn_risk", [0.5, 0.8], 0.001)
out = (out.withColumn("risk_band", F.when(F.col("churn_risk") >= q80, "High").when(F.col("churn_risk") >= q50, "Medium").otherwise("Low")))
out = out.select("customer_key", "last_purchase_date", "recency_days", "order_count", "lifetime_spend", "avg_monthly_spend",
                 "churn_risk", "risk_band", "expected_30d_spend", "revenue_at_risk", "rfm_segment", "model_auc")
out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.customer_churn_score")
n = spark.table("gold.customer_churn_score")
print(n.count())
n.groupBy("risk_band").count().show()
n.groupBy("rfm_segment").count().show()

# %%
notebookutils.notebook.exit(f"AUC={auc:.3f};rows={n.count()}")
