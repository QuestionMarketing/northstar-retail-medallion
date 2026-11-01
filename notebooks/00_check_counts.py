# %%
q = """
select 'cust' t, count(*) n, count(distinct customer_id) d, sum(case when email is null then 1 else 0 end) x, sum(case when future_dated_update then 1 else 0 end) y from silver.customers union all
select 'prod', count(*), count(distinct sku), sum(case when vat_rate is null then 1 else 0 end), sum(case when rrp_gbp is null then 1 else 0 end) from silver.products union all
select 'store', count(*), count(distinct store_key), sum(case when click_collect_enabled is null then 1 else 0 end), 0 from silver.stores union all
select 'pos', count(*), count(distinct order_line_id), sum(case when transaction_ts is null then 1 else 0 end), sum(case when is_return then 1 else 0 end) from silver.pos_order_lines union all
select 'ecom', count(*), count(distinct order_line_id), sum(case when transaction_ts is null then 1 else 0 end), sum(case when is_return then 1 else 0 end) from silver.ecom_order_lines union all
select concat('exc:', source, ':', issue), count(*), 0, 0, 0 from silver.dq_exceptions group by source, issue union all
select concat('pos_net_vs_gross ', round(sum(net_sales),0), ' ', round(sum(gross_sales),0), ' ', min(transaction_ts), ' ', max(transaction_ts)), 0,0,0,0 from silver.pos_order_lines union all
select concat('ecom_net_vs_gross ', round(sum(net_sales),0), ' ', round(sum(gross_sales),0), ' ', min(transaction_ts), ' ', max(transaction_ts), ' disc ', round(sum(discount_amount),0)), 0,0,0,0 from silver.ecom_order_lines union all
select concat('ecom_ri_store_missing ', count(*)), 0,0,0,0 from silver.ecom_order_lines e left anti join silver.stores s on e.fulfilment_store_key = s.store_key union all
select concat('pos_ri_sku_missing ', count(*)), 0,0,0,0 from silver.pos_order_lines e left anti join silver.products s on e.sku = s.sku union all
select concat('ecom_ri_sku_missing ', count(*)), 0,0,0,0 from silver.ecom_order_lines e left anti join silver.products s on e.sku = s.sku union all
select concat('pos_store_missing ', count(*)), 0,0,0,0 from silver.pos_order_lines e left anti join silver.stores s on e.store_key = s.store_key union all
select concat('pos_cust_missing ', count(*)), 0,0,0,0 from silver.pos_order_lines e left anti join silver.customers s on e.customer_id = s.customer_id where e.customer_id is not null union all
select concat('ecom_cust_missing ', count(*)), 0,0,0,0 from silver.ecom_order_lines e left anti join silver.customers s on e.customer_id = s.customer_id where e.customer_id is not null
"""
import os, traceback
os.makedirs('/lakehouse/default/Files/validation', exist_ok=True)
try:
    df = spark.sql(q)
    df.collect()
except Exception as e:
    open('/lakehouse/default/Files/validation/err.txt','w').write(str(e)[:6000])
    raise
import os
os.makedirs("/lakehouse/default/Files/validation", exist_ok=True)
df.toPandas().to_csv("/lakehouse/default/Files/validation/silver_check.csv", index=False)
df.show(100, truncate=False)
