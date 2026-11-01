# %% [markdown]
# # 01 - Bronze ingestion (Northstar Retail)
# Lands every raw extract from `Files/landing` as a faithful Delta table in the `bronze` schema.
# No cleansing: all columns stay strings, plus lineage columns (ingestion_timestamp, batch_id, source_file).

# %%
import uuid
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark.conf.set("spark.sql.ansi.enabled", "false")

batch_id = str(uuid.uuid4())
LAND = "Files/landing"

for s in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")

def add_lineage(df, source_file):
    return (df.withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("batch_id", F.lit(batch_id))
              .withColumn("source_file", source_file))

def save(df, table):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"bronze.{table}")
    n = spark.table(f"bronze.{table}").count()
    print(f"bronze.{table}: {n:,} rows")
    return n

counts = {}
src = F.col("_metadata.file_path")

# %% [markdown]
# ## CSV / TSV sources

# %%
customers = spark.read.option("header", True).csv(f"{LAND}/crm/customer_master_export.csv")
counts["crm_customers"] = save(add_lineage(customers, src), "crm_customers")

pos = spark.read.option("header", True).csv(f"{LAND}/stores/pos_transactions_2025_q4_part*.csv")
counts["pos_transactions"] = save(add_lineage(pos, src), "pos_transactions")

channels = spark.read.option("header", True).option("sep", "\t").csv(f"{LAND}/reference/channel_lookup.tsv")
counts["channel_lookup"] = save(add_lineage(channels, src), "channel_lookup")

# %% [markdown]
# ## JSON sources (e-commerce JSONL kept as raw text, marketing preferences, store master)

# %%
ecom = (spark.read.text(f"{LAND}/commerce/ecommerce_orders_2025_q4.jsonl")
        .withColumnRenamed("value", "raw_json")
        .withColumn("order_no", F.get_json_object("raw_json", "$.orderNo")))
counts["ecom_orders_raw"] = save(add_lineage(ecom, src), "ecom_orders_raw")

mraw = spark.read.option("multiLine", True).json(f"{LAND}/crm/customer_marketing_preferences.json")
mkt = mraw.select(F.explode("records").alias("r")).select("r.*")
mkt = mkt.select(*[(F.to_json(c) if dict(mkt.dtypes)[c].startswith(("array", "struct")) else F.col(c).cast("string")).alias(c)
                   for c in mkt.columns])
counts["crm_marketing_prefs"] = save(add_lineage(mkt, F.lit("customer_marketing_preferences.json")), "crm_marketing_prefs")

sraw = spark.read.option("multiLine", True).json(f"{LAND}/reference/store_master_2025.json")
st = sraw.select(F.explode("stores").alias("s")).select("s.*")
st = st.select(*[(F.to_json(c) if dict(st.dtypes)[c].startswith(("array", "struct")) else F.col(c).cast("string")).alias(c)
                 for c in st.columns])
counts["store_master"] = save(add_lineage(st, F.lit("store_master_2025.json")), "store_master")

# %% [markdown]
# ## Excel source (product master) - read with pandas, landed as strings

# %%
pdf = pd.read_excel("/lakehouse/default/Files/landing/product/product_master_2025.xlsx", sheet_name="product_export", dtype=str)
pdf = pdf.astype(object).where(pd.notnull(pdf), None)
sch = StructType([StructField(str(c), StringType(), True) for c in pdf.columns])
products = spark.createDataFrame(pdf.values.tolist(), sch)
counts["product_master"] = save(add_lineage(products, F.lit("product_master_2025.xlsx")), "product_master")

# %% [markdown]
# ## Validation against the client inventory

# %%
expected = {"crm_customers": 93500, "crm_marketing_prefs": 83000, "ecom_orders_raw": 70000,
            "pos_transactions": 260000, "store_master": 650, "product_master": 8180, "channel_lookup": 6}
report = [(t, expected[t], counts[t], "PASS" if expected[t] == counts[t] else "FAIL") for t in expected]
spark.createDataFrame(report, "table string, expected long, actual long, status string").show(truncate=False)
assert all(r[3] == "PASS" for r in report), "Bronze row counts do not match the client inventory"
notebookutils.notebook.exit(f"bronze_ok batch={batch_id}")
