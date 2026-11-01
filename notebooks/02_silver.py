# %% [markdown]
# # 02 - Silver: cleanse, conform, deduplicate
# Applies the documented cleaning rules to every Bronze table and writes typed, conformed Silver tables.
# Key rules: customer key `C#######`, SKU key `SKU######`, store key `ST####` (digits only); mixed date formats parsed with a
# regex-guarded chain (dash-numeric is month-first); money stripped of currency symbols; VAT normalised to a fraction;
# POS discount derived from gross (gross = units x rrp x (1-disc) x (1+vat)); returns are negative quantity/amounts.

# %%
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

def t(c):
    return F.nullif(F.trim(F.col(c) if isinstance(c, str) else c), F.lit(""))

def digits(c):
    return F.nullif(F.regexp_replace(c if not isinstance(c, str) else F.col(c), r"[^0-9]", ""), F.lit(""))

def cust_key(c):
    d = digits(c)
    return F.when(d.isNotNull(), F.concat(F.lit("C"), F.lpad(F.substring(d, -7, 7), 7, "0")))

def sku_key(c):
    d = digits(c)
    return F.when(d.isNotNull(), F.concat(F.lit("SKU"), F.lpad(F.substring(d, -6, 6), 6, "0")))

def store_key(c):
    d = digits(c)
    return F.when(d.isNotNull(), F.concat(F.lit("ST"), F.lpad(F.substring(d, -4, 4), 4, "0")))

def money(c):
    x = F.nullif(F.regexp_replace(c if not isinstance(c, str) else F.col(c), r"[^0-9.\-]", ""), F.lit(""))
    return x.cast("double")

def parse_ts(c):
    s = F.trim(c if not isinstance(c, str) else F.col(c))
    fmts = [
        (r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", "yyyy-MM-dd HH:mm:ss"),
        (r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$", "yyyy/MM/dd HH:mm"),
        (r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$", "dd/MM/yyyy HH:mm"),
        (r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$", "MM-dd-yyyy HH:mm:ss"),
        (r"^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$", "dd-MMM-yyyy HH:mm:ss"),
        (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
        (r"^\d{4}/\d{2}/\d{2}$", "yyyy/MM/dd"),
        (r"^\d{2}/\d{2}/\d{4}$", "dd/MM/yyyy"),
        (r"^\d{2}-\d{2}-\d{4}$", "MM-dd-yyyy"),
        (r"^\d{2}-[A-Za-z]{3}-\d{4}$", "dd-MMM-yyyy"),
    ]
    expr = None
    for rx, fmt in fmts:
        cond = s.rlike(rx)
        val = F.to_timestamp(s, fmt)
        expr = F.when(cond, val) if expr is None else expr.when(cond, val)
    return expr

def to_bool(c):
    s = F.lower(F.trim(c if not isinstance(c, str) else F.col(c).cast("string")))
    return (F.when(s.isin("y", "yes", "true", "1", "t"), F.lit(True))
             .when(s.isin("n", "no", "false", "0", "f"), F.lit(False)))

def vat_frac(c):
    s = F.lower(F.trim(c if not isinstance(c, str) else F.col(c)))
    x = F.nullif(F.regexp_replace(s, r"[^0-9.]", ""), F.lit("")).cast("double")
    return F.when(s == "zero", F.lit(0.0)).when(x.isNull(), F.lit(None).cast("double")).when(x >= 1, x / 100).otherwise(x)

def country(c):
    s = F.upper(F.trim(c))
    return F.when(s.isin("GB", "UK", "GBR", "UNITED KINGDOM"), F.lit("GB")).otherwise(s)

def dedupe(df, key, order_cols, cols_for_completeness):
    nn = sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in cols_for_completeness])
    w = Window.partitionBy(key).orderBy(*[F.col(o).desc_nulls_last() for o in order_cols], nn.desc())
    return df.withColumn("_rn", F.row_number().over(w)).withColumn("_dups", F.count("*").over(Window.partitionBy(key))).filter("_rn = 1").drop("_rn")

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"silver.{name}")
    n = spark.table(f"silver.{name}").count()
    print(f"silver.{name}: {n:,}")
    return n

counts = {}
exceptions = []

# %% [markdown]
# ## Channel map, stores

# %%
ch = spark.table("bronze.channel_lookup").select(
    F.lower(F.trim("source_system")).alias("source_system"), F.lower(F.trim("source_code")).alias("source_code"),
    F.trim("business_channel").alias("business_channel"), F.trim("sub_channel").alias("sub_channel")).distinct()
counts["channel_map"] = save(ch, "channel_map")

b = spark.table("bronze.store_master")
stores = b.select(
    store_key("storeCode").alias("store_key"),
    F.upper(t("storeCode")).alias("store_code_raw"),
    F.upper(t("legacyStoreCode")).alias("legacy_store_code"),
    F.initcap(t("storeName")).alias("store_name"),
    F.initcap(F.lower(t("type"))).alias("store_type"),
    F.initcap(t("region_name")).alias("region"),
    F.initcap(t("city")).alias("city"),
    F.upper(t("postcode")).alias("postcode"),
    country(t("country")).alias("country"),
    F.to_date(parse_ts(F.col("opened"))).alias("opened_date"),
    F.to_date(parse_ts(F.col("closed"))).alias("closed_date"),
    F.initcap(F.lower(t("status"))).alias("status"),
    F.col("sq_ft").cast("double").alias("sq_ft"),
    F.col("lat").cast("double").alias("latitude"), F.col("lon").cast("double").alias("longitude"),
    to_bool("click_collect").alias("click_collect_enabled"),
    to_bool("ship_from_store").alias("ship_from_store_enabled"),
    t("contactPhone").alias("contact_phone"),
    "source_file", "ingestion_timestamp", "batch_id")
stores = dedupe(stores, "store_key", ["ingestion_timestamp"], ["store_name", "city", "postcode"]).drop("_dups")
counts["stores"] = save(stores, "stores")

# %% [markdown]
# ## Products

# %%
b = spark.table("bronze.product_master")
p = b.select(
    sku_key("sku").alias("sku"),
    t("sku").alias("sku_raw"), t("legacy_sku").alias("legacy_sku"), t("ean").alias("ean"),
    t("product_name").alias("product_name"),
    F.initcap(t("brand")).alias("brand"),
    F.initcap(t("department")).alias("department"),
    F.initcap(t("category")).alias("category"),
    F.initcap(t("subcategory")).alias("subcategory"),
    t("season").alias("season"), F.initcap(t("colour")).alias("colour"), t("size").alias("size"),
    money("current_rrp_gbp").alias("rrp_gbp"), money("unit_cost_gbp").alias("unit_cost_gbp"),
    F.upper(t("supplier_code")).alias("supplier_code"), t("supplier_name").alias("supplier_name"),
    to_bool("active_flag").alias("is_active"),
    F.to_date(parse_ts(F.col("launch_date"))).alias("launch_date"),
    parse_ts(F.col("last_updated_at")).alias("last_updated_at"),
    vat_frac("vat_rate_raw").alias("vat_rate"),
    "source_file", "ingestion_timestamp", "batch_id").filter("sku is not null")
prod = dedupe(p, "sku", ["last_updated_at"], ["product_name", "brand", "category", "rrp_gbp", "unit_cost_gbp", "vat_rate"]) \
    .withColumn("source_row_count", F.col("_dups")).drop("_dups")
counts["products"] = save(prod, "products")

# %% [markdown]
# ## Customers (CRM resolved + marketing consent)

# %%
b = spark.table("bronze.crm_customers")
c = b.select(
    cust_key("crm_customer_id").alias("customer_id"),
    t("crm_customer_id").alias("crm_customer_id_raw"), t("legacy_customer_ref").alias("legacy_customer_ref"),
    F.initcap(t("first_name")).alias("first_name"), F.initcap(t("last_name")).alias("last_name"),
    F.lower(t("email_address")).alias("email"),
    F.regexp_replace(t("mobile_no"), r"[^0-9+]", "").alias("phone"),
    F.to_date(parse_ts(F.col("dob_raw"))).alias("birth_date"),
    F.initcap(t("gender")).alias("gender"),
    parse_ts(F.col("signup_dt")).alias("signup_ts"),
    parse_ts(F.col("last_profile_update")).alias("_lpu"),
    F.upper(t("loyalty_card_no")).alias("loyalty_card_no"),
    store_key("home_store_code").alias("home_store_key"),
    F.initcap(t("town")).alias("city"), F.initcap(t("county")).alias("county"),
    F.upper(t("postcode")).alias("postcode"), country(t("country_code")).alias("country"),
    to_bool("marketing_opt_in_raw").alias("crm_marketing_opt_in"),
    F.lower(t("preferred_channel_raw")).alias("crm_preferred_channel"),
    F.upper(t("customer_status")).alias("customer_status"),
    F.lower(t("source_system")).alias("record_source"),
    "source_file", "ingestion_timestamp", "batch_id").filter("customer_id is not null")
c = c.withColumn("last_profile_update", F.when(F.col("_lpu") > F.current_timestamp(), F.lit(None)).otherwise(F.col("_lpu"))) \
     .withColumn("future_dated_update", F.col("_lpu") > F.current_timestamp()).drop("_lpu")
cust = dedupe(c, "customer_id", ["last_profile_update"], ["first_name", "last_name", "email", "phone", "birth_date", "postcode"]) \
    .withColumn("source_row_count", F.col("_dups")).withColumn("dedupe_method", F.lit("latest last_profile_update, then most complete, key=digits of crm id")).drop("_dups")

m = spark.table("bronze.crm_marketing_prefs").select(
    cust_key("customerRef").alias("customer_id"),
    to_bool("emailOptIn").alias("email_consent"), to_bool("smsOptIn").alias("sms_consent"),
    to_bool("push_opt_in").alias("push_consent"),
    F.lower(F.regexp_extract(F.col("preferredChannels"), r'"([^"]+)"', 1)).alias("mkt_preferred_channel"),
    parse_ts(F.col("updatedAt")).alias("mkt_updated_at"),
    to_bool("gdprDeleted").alias("gdpr_deleted"),
    F.lower(t("recordSource")).alias("marketing_source")).filter("customer_id is not null")
m = dedupe(m, "customer_id", ["mkt_updated_at"], ["email_consent", "sms_consent", "push_consent"]).drop("_dups")

cust = (cust.join(m, "customer_id", "left")
        .withColumn("email_consent", F.coalesce("email_consent", "crm_marketing_opt_in"))
        .withColumn("preferred_channel", F.coalesce(F.nullif("mkt_preferred_channel", F.lit("")), "crm_preferred_channel"))
        .withColumn("post_consent", F.lit(None).cast("boolean"))
        .withColumn("has_marketing_record", F.col("marketing_source").isNotNull()))
counts["customers"] = save(cust, "customers")
mk_orphans = spark.table("bronze.crm_marketing_prefs").select(cust_key("customerRef").alias("k")).distinct() \
    .join(cust.select(F.col("customer_id").alias("k")), "k", "left_anti").count()
exceptions.append(("crm_marketing_prefs", "customer ref not found in CRM", mk_orphans))

# %% [markdown]
# ## POS order lines

# %%
b = spark.table("bronze.pos_transactions")
units = F.col("units").cast("double")
is_ret = F.coalesce(to_bool("return_ind"), units < 0)
rrp = money("unit_rrp_raw")
vat = vat_frac("vat_rate_raw") if False else vat_frac("vat_raw")
gross_raw = money("line_gross_raw")
sign = F.when(is_ret, F.lit(-1.0)).otherwise(F.lit(1.0))
gross = F.abs(gross_raw) * sign
q = F.abs(units) * sign
net = gross / (1 + vat)
pos = b.select(
    t("transaction_id").alias("source_order_id"),
    F.col("transaction_line_nbr").cast("int").alias("source_line_id"),
    parse_ts(F.col("trans_timestamp_raw")).alias("_ts"), parse_ts(F.col("business_date")).alias("_bd"),
    store_key("store_code").alias("store_key"), t("till_no").alias("till_no"), t("cashier_id").alias("cashier_id"),
    sku_key("product_sku_raw").alias("sku"), cust_key("loyalty_customer_ref").alias("customer_id"),
    q.alias("quantity"), rrp.alias("unit_price"), vat.alias("vat_rate"),
    is_ret.alias("is_return"), gross.alias("gross_sales"), net.alias("net_sales"),
    F.upper(t("promo_code")).alias("promo_code"), F.initcap(t("tender_type_raw")).alias("tender_type"),
    F.lower(t("channel_hint")).alias("channel_code"), t("extract_batch_id").alias("extract_batch_id"),
    "source_file", "ingestion_timestamp", "batch_id")
pos = (pos.withColumn("transaction_ts", F.coalesce("_ts", "_bd")).drop("_ts", "_bd")
       .withColumn("vat_amount", F.col("gross_sales") - F.col("net_sales"))
       .withColumn("discount_amount", F.abs(F.col("quantity")) * F.col("unit_price") * F.signum("quantity") - F.col("net_sales"))
       .withColumn("sales_status", F.when(F.col("is_return"), "Returned").otherwise("Completed"))
       .withColumn("source_system", F.lit("pos"))
       .withColumn("fulfilment_store_key", F.col("store_key"))
       .withColumn("fulfilment_channel", F.lit("In-Store"))
       .withColumn("order_line_id", F.concat_ws("|", F.lit("POS"), "source_order_id", F.col("source_line_id").cast("string"))))
pos = dedupe(pos, "order_line_id", ["ingestion_timestamp"], ["transaction_ts", "sku", "gross_sales"]).withColumn("source_row_count", F.col("_dups")).drop("_dups")
pos = pos.join(F.broadcast(spark.table("silver.channel_map").filter("source_system = 'pos'").select(F.col("source_code").alias("channel_code"), "business_channel", "sub_channel")),
               "channel_code", "left") \
         .withColumn("business_channel", F.coalesce("business_channel", F.lit("Store"))) \
         .withColumn("sub_channel", F.coalesce("sub_channel", F.lit("In-Store POS")))
counts["pos_order_lines"] = save(pos, "pos_order_lines")

# %% [markdown]
# ## E-commerce order lines (flatten nested JSON)

# %%
schema = ("orderNo string, orderTimestamp string, customerRef string, customerEmail string, channelCode string, status string, currency string, "
          "fulfilment struct<type:string,node_code:string,node_name:string,promise_days:string>, orderSubtotal string, shippingFee string, orderTotal string, "
          "lineItems array<struct<lineNo:string,sku:string,productName:string,qty:string,unit_price:string,line_discount:string,vat_rate:string,"
          "net_amount:string,gross_amount:string,return_flag:string,expected_ship_date:string>>, sourceFileRowId string")
b = spark.table("bronze.ecom_orders_raw")
j = b.select(F.from_json("raw_json", schema).alias("o"), "source_file", "ingestion_timestamp", "batch_id") \
     .select("o.*", "source_file", "ingestion_timestamp", "batch_id") \
     .withColumn("li", F.explode_outer("lineItems"))
ret = to_bool(F.col("li.return_flag"))
sgn = F.when(ret, F.lit(-1.0)).otherwise(F.lit(1.0))
qty = F.abs(F.col("li.qty").cast("double")) * sgn
net_e = F.abs(money(F.col("li.net_amount"))) * sgn
gross_e = F.abs(money(F.col("li.gross_amount"))) * sgn
status = F.lower(F.trim("status"))
ecom = j.select(
    F.trim("orderNo").alias("source_order_id"), F.col("li.lineNo").cast("int").alias("source_line_id"),
    parse_ts(F.col("orderTimestamp")).alias("transaction_ts"),
    cust_key("customerRef").alias("customer_id"), F.lower(t("customerEmail")).alias("customer_email"),
    F.lower(F.trim("channelCode")).alias("channel_code"),
    F.when(status.isin("complete", "completed"), "Completed").when(status.isin("shipped", "shippped"), "Shipped")
     .when(status == "infulfilment", "In Fulfilment").when(status == "returned-partial", "Returned Partial")
     .when(status == "cancelled", "Cancelled").otherwise(F.initcap("status")).alias("sales_status"),
    F.upper(F.trim("currency")).alias("currency_raw"),
    F.lower(F.col("fulfilment.type")).alias("fulfilment_type"), store_key(F.col("fulfilment.node_code")).alias("fulfilment_store_key"),
    sku_key(F.col("li.sku")).alias("sku"),
    qty.alias("quantity"), money(F.col("li.unit_price")).alias("unit_price"), vat_frac(F.col("li.vat_rate")).alias("vat_rate"),
    ret.alias("is_return"), net_e.alias("net_sales"), gross_e.alias("gross_sales"),
    "sourceFileRowId", "source_file", "ingestion_timestamp", "batch_id")
ecom = (ecom.withColumn("vat_amount", F.col("gross_sales") - F.col("net_sales"))
        .withColumn("discount_amount", F.abs(F.col("quantity")) * F.col("unit_price") * F.signum("quantity") - F.col("net_sales"))
        .withColumn("source_system", F.lit("ecommerce"))
        .withColumn("store_key", F.lit(None).cast("string"))
        .withColumn("fulfilment_channel", F.when(F.col("fulfilment_type") == "click_collect", "Click & Collect")
                    .when(F.col("fulfilment_type") == "ship_from_store", "Ship From Store")
                    .when(F.col("fulfilment_type") == "delivery", "Home Delivery").otherwise("Unknown"))
        .withColumn("order_line_id", F.concat_ws("|", F.lit("ECOM"), "source_order_id", F.col("source_line_id").cast("string"))))
ecom = dedupe(ecom, "order_line_id", ["ingestion_timestamp"], ["transaction_ts", "sku", "gross_sales"]).withColumn("source_row_count", F.col("_dups")).drop("_dups")
ecom = ecom.join(F.broadcast(spark.table("silver.channel_map").filter("source_system = 'ecommerce'").select(F.col("source_code").alias("channel_code"), "business_channel", "sub_channel")),
                 "channel_code", "left") \
           .withColumn("business_channel", F.coalesce("business_channel", F.lit("Online")))
counts["ecom_order_lines"] = save(ecom, "ecom_order_lines")

# %% [markdown]
# ## Data-quality exceptions (row-level issues observed in Silver)

# %%
def issue(df, name, cond, label):
    return df.filter(cond).select(F.lit(name).alias("source"), F.col("order_line_id").alias("record_id"), F.lit(label).alias("issue"))

pos_t = spark.table("silver.pos_order_lines"); ec_t = spark.table("silver.ecom_order_lines")
ex = None
for name, df in [("pos", pos_t), ("ecommerce", ec_t)]:
    for cond, label in [("transaction_ts is null", "unparseable timestamp"), ("sku is null", "missing sku"),
                        ("gross_sales is null", "missing gross amount"), ("vat_rate is null", "missing vat rate"),
                        ("customer_id is null", "anonymous / guest (no customer key)")]:
        d = issue(df, name, cond, label)
        ex = d if ex is None else ex.unionByName(d)
ex.write.format("delta").mode("overwrite").saveAsTable("silver.dq_exceptions")
spark.table("silver.dq_exceptions").groupBy("source", "issue").count().orderBy("source", "issue").show(truncate=False)

print(counts)
notebookutils.notebook.exit("silver_ok " + str(counts))
