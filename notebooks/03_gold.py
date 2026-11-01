# %% [markdown]
# # 03 - Gold: dimensional model + validation
# Star schema: `gold.dim_customer`, `gold.dim_product`, `gold.dim_store`, `gold.dim_date`, `gold.fct_order_line`
# plus `gold.dq_results` (data-quality / reconciliation) and a Materialized Lake View for daily channel sales.
# Conventions: unit_price = pre-discount, ex-VAT price; net_sales = ex-VAT after discount; gross_sales = inc-VAT after discount;
# returns are negative quantity/amounts with `is_return = true`; cancelled online lines are excluded from the fact (logged in DQ);
# guest / anonymous sales map to the `Unknown customer` member (customer_key = -1).

# %%
from pyspark.sql import functions as F, Window
import os

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"gold.{name}")
    print(f"gold.{name}: {spark.table('gold.' + name).count():,}")

def sk(col):
    return F.row_number().over(Window.orderBy(col)).cast("int")

# %% [markdown]
# ## Dimensions

# %%
c = spark.table("silver.customers")
dim_customer = c.select(
    sk("customer_id").alias("customer_key"), F.col("customer_id").alias("resolved_customer_id"),
    "crm_customer_id_raw", "legacy_customer_ref", "loyalty_card_no",
    "first_name", "last_name", F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
    "email", "phone", "birth_date", "gender", "postcode", "city", "county", "country",
    "customer_status", "signup_ts", F.to_date("signup_ts").alias("signup_date"),
    F.col("home_store_key").alias("home_store_id"),
    "email_consent", "sms_consent", "push_consent", "post_consent", "preferred_channel", "gdpr_deleted",
    "record_source", "marketing_source", "source_row_count", "dedupe_method", "last_profile_update")
unknown = spark.createDataFrame([(-1, "UNKNOWN", "Unknown customer", "Guest / anonymous")],
                                "customer_key int, resolved_customer_id string, full_name string, record_source string")
dim_customer = unknown.unionByName(dim_customer, allowMissingColumns=True)
save(dim_customer, "dim_customer")

p = spark.table("silver.products")
dim_product = p.select(
    sk("sku").alias("product_key"), "sku", "sku_raw", "legacy_sku", "ean", "product_name", "brand",
    "department", "category", "subcategory", "season", "colour", "size",
    "rrp_gbp", "unit_cost_gbp", "vat_rate", F.when(F.col("is_active"), "Active").otherwise("Inactive").alias("status"),
    "supplier_code", "supplier_name", "launch_date", "last_updated_at",
    F.when(F.col("rrp_gbp") >= 100, "Premium").when(F.col("rrp_gbp") >= 40, "Mid").when(F.col("rrp_gbp").isNotNull(), "Value").alias("price_band"),
    F.lit("silver.products").alias("record_source"), "source_row_count",
    F.lit("latest last_updated_at, then most complete row").alias("row_selection_logic"))
save(dim_product, "dim_product")

s = spark.table("silver.stores")
fulfil_type = F.lower("store_type").rlike("dark|fulfil|warehouse|distribution")
dim_store = s.select(
    sk("store_key").alias("store_key"), F.col("store_key").alias("store_id"), "legacy_store_code", "store_name", "store_type",
    "city", "region", "postcode", "country", "opened_date", "closed_date", "status",
    "click_collect_enabled", "ship_from_store_enabled", "sq_ft", "latitude", "longitude",
    (~fulfil_type).alias("is_selling_location"),
    (fulfil_type | F.coalesce("ship_from_store_enabled", F.lit(False)) | F.coalesce("click_collect_enabled", F.lit(False))).alias("is_fulfilment_node"),
    (fulfil_type | F.coalesce("ship_from_store_enabled", F.lit(False))).alias("is_online_node"),
    F.lit("silver.stores").alias("record_source"))
save(dim_store, "dim_store")

# %%
bounds = spark.sql("""select min(d) lo, max(d) hi from (
    select to_date(transaction_ts) d from silver.pos_order_lines union all select to_date(transaction_ts) from silver.ecom_order_lines)""").first()
dim_date = (spark.sql(f"select explode(sequence(date'{bounds.lo}', date'{bounds.hi}', interval 1 day)) as date")
            .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
                    F.year("date").alias("year"), F.quarter("date").alias("quarter"), F.month("date").alias("month_number"),
                    F.date_format("date", "MMMM").alias("month_name"), F.weekofyear("date").alias("iso_week"),
                    F.dayofweek("date").alias("day_of_week_number"), F.date_format("date", "EEEE").alias("day_name"),
                    F.dayofweek("date").isin(1, 7).alias("is_weekend")))
save(dim_date, "dim_date")

# %% [markdown]
# ## Fact: one row per commercially reportable order line (POS + e-commerce)

# %%
cols = ["order_line_id", "source_system", "source_order_id", "source_line_id", "transaction_ts", "customer_id", "sku",
        "store_key", "fulfilment_store_key", "business_channel", "sub_channel", "fulfilment_channel", "sales_status",
        "is_return", "quantity", "unit_price", "discount_amount", "net_sales", "vat_rate", "vat_amount", "gross_sales",
        "source_file", "batch_id", "ingestion_timestamp"]
pos = spark.table("silver.pos_order_lines").select(*cols)
ecom = spark.table("silver.ecom_order_lines").filter("sales_status <> 'Cancelled'").select(*cols)
allv = pos.unionByName(ecom)

dc = spark.table("gold.dim_customer").filter("customer_key > 0").select(F.col("resolved_customer_id").alias("customer_id"), "customer_key")
dp = spark.table("gold.dim_product").select("sku", "product_key")
ds = spark.table("gold.dim_store").select(F.col("store_id").alias("sid"), "store_key")
fct = (allv.join(dc, "customer_id", "left").join(dp, "sku", "left")
       .join(ds.withColumnRenamed("store_key", "store_sk").withColumnRenamed("sid", "store_key"), "store_key", "left")
       .withColumnRenamed("store_key", "store_id_src")
       .join(ds.withColumnRenamed("store_key", "fulfilment_store_sk").withColumnRenamed("sid", "fulfilment_store_key"), "fulfilment_store_key", "left"))
fct = fct.select(
    F.xxhash64("order_line_id").alias("order_line_key"), F.col("order_line_id"), "source_system", "source_order_id", "source_line_id",
    "transaction_ts", F.to_date("transaction_ts").alias("order_date"), F.to_date("transaction_ts").alias("reporting_date"),
    F.date_format("transaction_ts", "yyyyMMdd").cast("int").alias("date_key"),
    F.coalesce("customer_key", F.lit(-1)).alias("customer_key"), "product_key",
    F.col("store_sk").alias("store_key"), F.col("fulfilment_store_sk").alias("fulfilment_store_key"),
    F.col("business_channel").alias("sales_channel"), F.col("sub_channel").alias("sales_sub_channel"), "fulfilment_channel",
    "sales_status", "is_return", F.when(F.col("is_return"), F.abs("quantity")).alias("return_quantity"),
    F.when(F.col("is_return"), F.abs("gross_sales")).alias("returned_amount"),
    "quantity", "unit_price", "discount_amount", "net_sales", "vat_rate", "vat_amount", "gross_sales",
    F.lit("GBP").alias("currency"), "source_file", "batch_id", "ingestion_timestamp")
save(fct, "fct_order_line")

# %% [markdown]
# ## Aggregate + Materialized Lake View

# %%
for stmt in ["DROP MATERIALIZED LAKE VIEW IF EXISTS gold.mlv_daily_channel_sales"]:
    try:
        spark.sql(stmt)
    except Exception as e:
        print("skip:", str(e)[:200])

mlv_sql = """CREATE MATERIALIZED LAKE VIEW gold.mlv_daily_channel_sales AS
SELECT f.date_key, f.sales_channel, f.sales_sub_channel, p.department, p.category,
       SUM(f.quantity) AS units, SUM(f.net_sales) AS net_sales, SUM(f.gross_sales) AS gross_sales,
       SUM(f.discount_amount) AS discount_amount, SUM(CASE WHEN f.is_return THEN 1 ELSE 0 END) AS return_lines, COUNT(*) AS order_lines
FROM gold.fct_order_line f LEFT JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY f.date_key, f.sales_channel, f.sales_sub_channel, p.department, p.category"""
mlv_status = "created"
try:
    spark.sql(mlv_sql)
except Exception as e:
    mlv_status = "fallback_table: " + str(e)[:150]
    save(spark.sql(mlv_sql.split(" AS\n", 1)[1]), "mlv_daily_channel_sales")
print("MLV status:", mlv_status)

# %% [markdown]
# ## Data-quality & reconciliation checks

# %%
res = []
def chk(layer, name, sql, expect_zero=True):
    v = spark.sql(sql).first()[0]
    res.append((layer, name, float(v) if v is not None else None, "PASS" if (v == 0 if expect_zero else True) else "FAIL"))

dups = lambda t, k: f"select count(*) - count(distinct {k}) from gold.{t}"
chk("gold", "dim_customer key uniqueness", dups("dim_customer", "customer_key"))
chk("gold", "dim_customer resolved id uniqueness", dups("dim_customer", "resolved_customer_id"))
chk("gold", "dim_product key uniqueness", dups("dim_product", "product_key"))
chk("gold", "dim_product sku uniqueness", dups("dim_product", "sku"))
chk("gold", "dim_store key uniqueness", dups("dim_store", "store_key"))
chk("gold", "dim_store id uniqueness", dups("dim_store", "store_id"))
chk("gold", "fct_order_line key uniqueness", dups("fct_order_line", "order_line_key"))
chk("gold", "fct_order_line source line uniqueness", dups("fct_order_line", "order_line_id"))
chk("gold", "RI fact->product (null product_key)", "select count(*) from gold.fct_order_line where product_key is null")
chk("gold", "RI fact->store (POS null store_key)", "select count(*) from gold.fct_order_line where source_system='pos' and store_key is null")
chk("gold", "RI fact->fulfilment store", "select count(*) from gold.fct_order_line where fulfilment_store_key is null")
chk("gold", "RI fact->customer", "select count(*) from gold.fct_order_line f left anti join gold.dim_customer c on f.customer_key = c.customer_key")
chk("gold", "RI fact->date", "select count(*) from gold.fct_order_line f left anti join gold.dim_date d on f.date_key = d.date_key")
chk("gold", "metric: net + vat = gross (>1p)", "select count(*) from gold.fct_order_line where abs(net_sales + vat_amount - gross_sales) > 0.01")
chk("gold", "metric: vat = net x rate (>5p)", "select count(*) from gold.fct_order_line where vat_rate is not null and abs(net_sales*vat_rate - vat_amount) > 0.05")
chk("gold", "metric: return sign (qty/net sign vs flag)", "select count(*) from gold.fct_order_line where (is_return and (quantity > 0 or net_sales > 0)) or (not is_return and (quantity < 0 or net_sales < 0))")
chk("gold", "null transaction timestamp", "select count(*) from gold.fct_order_line where transaction_ts is null")
chk("gold", "null gross_sales", "select count(*) from gold.fct_order_line where gross_sales is null")

chk("recon", "bronze->silver POS rows (diff)", "select (select count(*) from bronze.pos_transactions) - (select count(*) from silver.pos_order_lines)")
chk("recon", "bronze->silver customers (distinct ids diff)", "select (select count(distinct regexp_extract(crm_customer_id, '([0-9]+)', 1)) from bronze.crm_customers) - (select count(*) from silver.customers)")
chk("recon", "bronze->silver products (distinct sku diff)", "select (select count(distinct regexp_replace(sku,'[^0-9]','')) from bronze.product_master) - (select count(*) from silver.products)")
chk("recon", "bronze->silver stores (diff)", "select (select count(*) from bronze.store_master) - (select count(*) from silver.stores)")
chk("recon", "silver->gold POS rows (diff)", "select (select count(*) from silver.pos_order_lines) - (select count(*) from gold.fct_order_line where source_system='pos')")
chk("recon", "silver->gold ecom rows excl. cancelled (diff)", "select (select count(*) from silver.ecom_order_lines where sales_status <> 'Cancelled') - (select count(*) from gold.fct_order_line where source_system='ecommerce')")
chk("recon", "silver->gold net sales (diff, GBP)", "select round((select sum(net_sales) from silver.pos_order_lines) + (select sum(net_sales) from silver.ecom_order_lines where sales_status <> 'Cancelled') - (select sum(net_sales) from gold.fct_order_line), 2)")

for tbl, n in [("gold.dim_customer", "email"), ("gold.dim_customer", "birth_date"), ("gold.dim_customer", "email_consent"),
               ("gold.dim_product", "unit_cost_gbp"), ("gold.fct_order_line", "customer_key = -1")]:
    v = spark.sql(f"select round(100.0*sum(case when {'(' + n + ') is null' if '=' not in n else n} then 1 else 0 end)/count(*),2) from {tbl}").first()[0]
    res.append(("null-analysis", f"{tbl}: % {n if '=' in n else n + ' null'}", float(v), "INFO"))
for name, sql in [("rows cancelled ecom lines excluded", "select count(*) from silver.ecom_order_lines where sales_status = 'Cancelled'"),
                  ("fact rows", "select count(*) from gold.fct_order_line"),
                  ("fact net_sales GBP", "select round(sum(net_sales),2) from gold.fct_order_line"),
                  ("fact gross_sales GBP", "select round(sum(gross_sales),2) from gold.fct_order_line")]:
    res.append(("summary", name, float(spark.sql(sql).first()[0]), "INFO"))

dq = spark.createDataFrame(res, "layer string, check_name string, value double, status string").withColumn("run_ts", F.current_timestamp())
dq.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.dq_results")
os.makedirs("/lakehouse/default/Files/validation", exist_ok=True)
pdq = dq.toPandas(); pdq["mlv_status"] = mlv_status
pdq.to_csv("/lakehouse/default/Files/validation/gold_dq.csv", index=False)
dq.show(100, truncate=False)

# %% [markdown]
# ## Optimise

# %%
for t in ["fct_order_line", "dim_customer", "dim_product", "dim_store", "dim_date"]:
    spark.sql(f"OPTIMIZE gold.{t}")
fails = [r for r in res if r[3] == "FAIL"]
notebookutils.notebook.exit("gold_ok" if not fails else "gold_checks_failed: " + str([r[1] for r in fails]))
