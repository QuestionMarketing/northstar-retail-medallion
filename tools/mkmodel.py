import json, os, subprocess, uuid, shutil

LH = "Retail_VS.Workspace/retail_lakehouse.Lakehouse"
MODEL = "Retail_Medallion_Model"
OUT = r"C:\Users\mikem\retail_work\out"
u = lambda: str(uuid.uuid4())

def fab(*a):
    return subprocess.run(["fab", *a], capture_output=True, text=True).stdout

ep = json.loads(fab("get", LH, "-q", "properties.sqlEndpointProperties"))

def schema(t):
    cols, go = [], False
    for l in fab("table", "schema", f"{LH}/Tables/gold/{t}").splitlines():
        l = l.strip()
        if l.startswith("---"):
            go = True; continue
        if go and l:
            p = l.split(); cols.append((p[0], p[1]))
    return cols

def dt(t):
    t = t.lower()
    if t in ("integer", "int", "long", "bigint", "short", "byte"): return "int64"
    if t in ("double", "float") or t.startswith("decimal"): return "double"
    if t.startswith("bool"): return "boolean"
    if t in ("date", "timestamp", "timestamp_ntz"): return "dateTime"
    return "string"

HIDE_SUFFIX = ("_key",)
tables = {  # display name: (gold table, extras)
    "Sales": "fct_order_line", "Customer": "dim_customer", "Product": "dim_product", "Store": "dim_store", "Date": "dim_date", "Churn": "customer_churn_score"}

MEAS = [
    ("Net Sales", "SUM(Sales[net_sales])", "£#,0", "Revenue excluding VAT after discount"),
    ("Gross Sales", "SUM(Sales[gross_sales])", "£#,0", "Revenue including VAT after discount"),
    ("VAT Amount", "SUM(Sales[vat_amount])", "£#,0", ""),
    ("Discount Amount", "SUM(Sales[discount_amount])", "£#,0", "Monetary discount vs pre-discount ex-VAT price"),
    ("Units Sold", "SUM(Sales[quantity])", "#,0", "Net units (returns are negative)"),
    ("Order Lines", "COUNTROWS(Sales)", "#,0", ""),
    ("Orders", "DISTINCTCOUNT(Sales[source_order_id])", "#,0", ""),
    ("Return Lines", "CALCULATE(COUNTROWS(Sales), Sales[is_return] = TRUE())", "#,0", ""),
    ("Returned Amount", "CALCULATE(SUM(Sales[returned_amount]), Sales[is_return] = TRUE())", "£#,0", ""),
    ("Return Rate", "DIVIDE([Return Lines], [Order Lines])", "0.0%", "Return lines / all lines"),
    ("Discount %", "DIVIDE([Discount Amount], [Net Sales] + [Discount Amount])", "0.0%", "Discount as share of pre-discount net"),
    ("Avg Order Value", "DIVIDE([Net Sales], [Orders])", "£#,0.00", ""),
    ("Online Share %", "DIVIDE(CALCULATE([Net Sales], Sales[sales_channel] = \"Online\"), [Net Sales])", "0.0%", ""),
    ("Identified Customer Share %", "DIVIDE(CALCULATE([Order Lines], Sales[customer_key] <> -1), [Order Lines])", "0.0%", "Lines linked to a resolved customer"),
        ("Net Sales PM", "CALCULATE([Net Sales], DATEADD('Date'[date], -1, MONTH))", "£#,0", "Net sales in the prior month"),
        ("Net Sales MoM %", "VAR d = MAX('Date'[date]) VAR cur = CALCULATE([Net Sales], ALL('Date'), 'Date'[date] >= DATE(YEAR(d), MONTH(d), 1), 'Date'[date] <= EOMONTH(d, 0)) VAR prev = CALCULATE([Net Sales], ALL('Date'), 'Date'[date] >= DATE(YEAR(d), MONTH(d) - 1, 1), 'Date'[date] <= EOMONTH(d, -1)) RETURN IF(prev > 0, DIVIDE(cur - prev, prev))", "+0.0%;-0.0%;0.0%", "Month-over-month change"),
        ("Net Sales Share %", "DIVIDE([Net Sales], CALCULATE([Net Sales], ALLSELECTED()))", "0.0%", "Share of selected net sales"),
        ("Net Sales per Store", "DIVIDE([Net Sales], DISTINCTCOUNT(Sales[store_key]))", "£#,0", ""),
        ("Net Sales Running", "CALCULATE([Net Sales], FILTER(ALLSELECTED('Date'[date]), 'Date'[date] <= MAX('Date'[date])))", "£#,0", "Cumulative net sales"),
        ("Return Rate Gap vs Avg", "[Return Rate] - CALCULATE([Return Rate], ALLSELECTED())", "+0.0%;-0.0%;0.0%", ""),
    ]

def top(col, meas="[Net Sales]"):
    return f"MAXX(TOPN(1, FILTER(VALUES({col}), {col} <> \"\" && NOT ISBLANK({meas})), {meas}), {col})"
def topv(col, meas="[Net Sales]"):
    return f"MAXX(TOPN(1, FILTER(VALUES({col}), {col} <> \"\" && NOT ISBLANK({meas})), {meas}), {meas})"
def bot(col, meas="[Net Sales]"):
    return f"MAXX(TOPN(1, FILTER(VALUES({col}), {col} <> \"\" && NOT ISBLANK({meas})), {meas}, ASC), {col})"
def M(x): return f'FORMAT({x}/1000000, "£0.0") & "M"'
def P(x): return f'FORMAT({x}, "0.0%")'
def PS(x): return f'FORMAT({x}, "+0.0%;-0.0%")'
NA = 'IF([Net Sales] = 0, "No sales match the current selection.", '
ONL = 'CALCULATE([Net Sales], Sales[sales_channel] = "Online")'
STO = 'CALCULATE([Net Sales], Sales[sales_channel] = "Store")'
IDC = 'CALCULATE(DISTINCTCOUNT(Sales[customer_key]), Sales[customer_key] <> -1)'

MEAS += [
    ("Net Sales L28D", "VAR d = MAX('Date'[date]) RETURN CALCULATE([Net Sales], ALL('Date'), 'Date'[date] > d - 28, 'Date'[date] <= d)", "£#,0", "Trailing 28 days"),
    ("Net Sales P28D", "VAR d = MAX('Date'[date]) RETURN CALCULATE([Net Sales], ALL('Date'), 'Date'[date] > d - 56, 'Date'[date] <= d - 28)", "£#,0", "Previous 28 days"),
    ("Run Rate 30d Projection", "[Net Sales L28D] / 28 * 30", "£#,0", "Trailing-28-day daily average x 30"),
    ("Run Rate Trend %", "DIVIDE([Net Sales L28D] - [Net Sales P28D], [Net Sales P28D])", "+0.0%;-0.0%;0.0%", "Last 28 days vs previous 28 days"),
    ("Customers Scored", "COUNTROWS(Churn)", "#,0", ""),
    ("Avg Churn Risk", "AVERAGE(Churn[churn_risk])", "0.0%", ""),
    ("High-Risk Customers", "CALCULATE(COUNTROWS(Churn), Churn[risk_band] = \"High\")", "#,0", ""),
    ("High-Risk Share %", "DIVIDE([High-Risk Customers], [Customers Scored])", "0.0%", ""),
    ("Revenue at Risk", "SUM(Churn[revenue_at_risk])", "£#,0", "Churn probability x average monthly spend"),
    ("Expected 30d Spend", "SUM(Churn[expected_30d_spend])", "£#,0", ""),
    ("Retention Likelihood 30d", "1 - [Avg Churn Risk]", "0.0%", ""),
    ("Model AUC", "MAX(Churn[model_auc])", "0.00", "Hold-out AUC of the churn model"),
    ("Identified Customers", IDC, "#,0", ""),
    ("Narr Summary", NA + '"Net sales of " & ' + M("[Net Sales]") + ' & " from " & FORMAT([Orders], "#,0") & " orders at an average of " & FORMAT([Avg Order Value], "£0.00") & ". Stores deliver " & ' + P(f"DIVIDE({STO}, [Net Sales])") + ' & " and online " & ' + P("[Online Share %]") + ' & ". Latest month is " & ' + PS("[Net Sales MoM %]") + ' & " vs the prior month; returns run at " & ' + P("[Return Rate]") + ' & ".")', "", ""),
    ("Act Summary", NA + 'IF([Online Share %] < 0.35, "Online is only " & ' + P("[Online Share %]") + ' & " of sales: the biggest headroom lever. Prioritise online conversion and click-and-collect. ", "Online is a healthy " & ' + P("[Online Share %]") + ' & " of sales; protect it. ") & IF([Return Rate] > 0.03, "Return rate is above 3%: review the worst departments for quality or sizing issues.", "Return rate is under control."))', "", ""),
    ("Narr Channel", NA + 'M("") ', "", ""),
]
MEAS.pop()
MEAS += [
    ("Narr Channel", NA + '"Online delivers " & ' + M(ONL) + ' & " (" & ' + P("[Online Share %]") + ' & ") at an AOV of " & FORMAT(DIVIDE(' + ONL + ', CALCULATE([Orders], Sales[sales_channel] = "Online")), "£0.00") & " versus " & FORMAT(DIVIDE(' + STO + ', CALCULATE([Orders], Sales[sales_channel] = "Store")), "£0.00") & " in store. Largest sub-channel: " & ' + top("Sales[sales_sub_channel]") + ' & " (" & ' + P(topv("Sales[sales_sub_channel]") + "/[Net Sales]") + ' & " of sales).")', "", ""),
    ("Act Channel", NA + 'IF(CALCULATE([Return Rate], Sales[sales_channel] = "Online") > CALCULATE([Return Rate], Sales[sales_channel] = "Store"), "Online return rate (" & ' + P('CALCULATE([Return Rate], Sales[sales_channel] = "Online")') + ' & ") exceeds store (" & ' + P('CALCULATE([Return Rate], Sales[sales_channel] = "Store")') + ' & "): tighten size guidance and product content online. ", "Store return rate is the higher of the two: check in-store fit and quality. ") & "Shift identified-customer capture at the till to grow CRM reach.")', "", ""),
    ("Narr Product", NA + '"Top department is " & ' + top("Product[department]") + ' & " with " & ' + M(topv("Product[department]")) + ' & " (" & ' + P(topv("Product[department]") + "/[Net Sales]") + ' & " of sales). Leading category: " & ' + top("Product[category]") + ' & ". Smallest department: " & ' + bot("Product[department]") + ' & ".")', "", ""),
    ("Act Product", NA + '"Protect availability in " & ' + top("Product[department]") + ' & " and " & ' + top("Product[category]") + ' & "; review range depth in " & ' + bot("Product[department]") + ' & " for rationalisation or promotion.")', "", ""),
    ("Narr Stores", NA + '"Top store is " & ' + top("Store[store_name]") + ' & " (" & ' + M(topv("Store[store_name]")) + ' & "). Leading region: " & ' + top("Store[region]") + ' & " at " & ' + P(topv("Store[region]") + "/[Net Sales]") + ' & " of sales. Weakest region: " & ' + bot("Store[region]") + ' & ". Average net sales per store: " & ' + M("[Net Sales per Store]") + ' & ".")', "", ""),
    ("Act Stores", NA + '"Benchmark " & ' + bot("Store[region]") + ' & " against " & ' + top("Store[region]") + ' & " on footfall conversion and staffing; replicate the practices of " & ' + top("Store[store_name]") + ' & ".")', "", ""),
    ("Narr Customer", NA + '"Identified customers account for " & ' + P("[Identified Customer Share %]") + ' & " of order lines (" & FORMAT([Identified Customers], "#,0") & " distinct customers); the rest are anonymous. Top city: " & ' + top("Customer[city]", "CALCULATE([Net Sales], Customer[customer_key] <> -1)") + ' & ". Largest preferred channel: " & ' + top("Customer[preferred_channel]", "[Identified Customers]") + ' & ".")', "", ""),
    ("Act Customer", NA + 'IF([Identified Customer Share %] < 0.6, "Nearly half of sales are anonymous: introduce loyalty capture and e-receipts at tills to lift identification above 60%.", "Identification is healthy; deepen personalisation.") )', "", ""),
    ("Narr Returns", NA + '"Returns total " & ' + M("[Returned Amount]") + ' & " (" & ' + P("[Return Rate]") + ' & " of lines). Highest return rate: " & ' + top("Product[department]", "[Return Rate]") + ' & " at " & ' + P(topv("Product[department]", "[Return Rate]")) + ' & ".")', "", ""),
    ("Act Returns", NA + '"Investigate " & ' + top("Product[department]", "[Return Rate]") + ' & " returns by reason, supplier and size; target a return rate below 3%.")', "", ""),
    ("Narr Churn", 'IF([Customers Scored] = 0, "No customers match the selection.", "Of " & FORMAT([Customers Scored], "#,0") & " identified customers, " & FORMAT([High-Risk Customers], "#,0") & " (" & ' + P("[High-Risk Share %]") + ' & ") are at high risk of not buying in the next 30 days, putting about " & ' + M("[Revenue at Risk]") + ' & " of monthly spend at risk. Average churn risk is " & ' + P("[Avg Churn Risk]") + ' & ". Model hold-out AUC: " & FORMAT([Model AUC], "0.00") & ".")', "", ""),
    ("Act Churn", 'IF([Customers Scored] = 0, "", "Launch a win-back campaign for the high-risk, high-value customers listed here, led by their preferred channel (" & ' + top("Customer[preferred_channel]", "[High-Risk Customers]") + ' & "). Each 10% of high-risk customers recovered protects roughly " & ' + M("[Revenue at Risk] * 0.1") + ' & " per month.")', "", ""),
    ("Narr Outlook", NA + '"The last 28 days delivered " & ' + M("[Net Sales L28D]") + ' & ", " & ' + PS("[Run Rate Trend %]") + ' & " vs the previous 28 days. At this run rate the next 30 days project to " & ' + M("[Run Rate 30d Projection]") + ' & ". The projection assumes the recent daily average continues.")', "", ""),
    ("Act Outlook", 'IF([Run Rate Trend %] < 0, "Momentum is softening: bring forward promotions and check stock availability in top departments.", "Momentum is building: secure inventory and staffing to capture the upside.")', "", ""),
]

root = os.path.join(OUT, MODEL + ".SemanticModel")
shutil.rmtree(root, ignore_errors=True)
d = os.path.join(root, "definition"); os.makedirs(os.path.join(d, "tables"))
json.dump({"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
           "metadata": {"type": "SemanticModel", "displayName": MODEL}, "config": {"version": "2.0", "logicalId": u()}},
          open(os.path.join(root, ".platform"), "w"), indent=1)
json.dump({"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
           "version": "4.0", "settings": {}}, open(os.path.join(root, "definition.pbism"), "w"), indent=1)

order = list(tables)
rels = [("Sales", "customer_key", "Customer", "customer_key", True), ("Sales", "product_key", "Product", "product_key", True),
        ("Sales", "store_key", "Store", "store_key", True), ("Sales", "date_key", "Date", "date_key", True),
        ("Sales", "fulfilment_store_key", "Store", "store_key", False), ("Churn", "customer_key", "Customer", "customer_key", True, True)]
model = ["model Model", "\tculture: en-US", "\tdefaultPowerBIDataSourceVersion: powerBI_V3", ""]
model += [f"ref table {t}" for t in order] + [""]
open(os.path.join(d, "model.tmdl"), "w", encoding="utf-8").write("\n".join(model))
open(os.path.join(d, "database.tmdl"), "w").write("database\n\tcompatibilityLevel: 1604\n")
open(os.path.join(d, "expressions.tmdl"), "w").write(
    f'expression DatabaseQuery =\n\t\tlet\n\t\t\tdatabase = Sql.Database("{ep["connectionString"]}", "{ep["id"]}")\n\t\tin\n\t\t\tdatabase\n\tlineageTag: {u()}\n')

rl = []
for i, r in enumerate(rels):
    ft, fc, tt, tc, active = r[:5]
    rl += [f"relationship {u()}"] + ([] if active else ["\tisActive: false"]) + (["\tfromCardinality: one", "\tcrossFilteringBehavior: bothDirections"] if len(r) > 5 else []) + [f"\tfromColumn: {ft}.{fc}", f"\ttoColumn: {tt}.{tc}", ""]
open(os.path.join(d, "relationships.tmdl"), "w").write("\n".join(rl))

for disp, gt in tables.items():
    L = [f"table {disp}", f"\tlineageTag: {u()}"]
    if disp == "Date":
        L.append("\tdataCategory: Time")
    L.append("")
    if disp == "Sales":
        for n, e, f, desc in MEAS:
            L += [f"\tmeasure '{n}' = {e}"] + ([f"\t\tformatString: {f}"] if f else []) + [f"\t\tlineageTag: {u()}", ""]
    for c, t in schema(gt):
        ty = dt(t)
        L += [f"\tcolumn {c}", f"\t\tdataType: {ty}", f"\t\tlineageTag: {u()}", f"\t\tsourceColumn: {c}"]
        if c.endswith("_key") or c in ("order_line_id", "source_file", "batch_id") or (ty == "string" and disp == "Sales" and c == "currency"):
            L.append("\t\tisHidden")
        if disp == "Date" and c == "date": L.append("\t\tisKey")
        if disp == "Date" and c == "month_name": L.append("\t\tsortByColumn: month_number")
        if disp == "Date" and c == "day_name": L.append("\t\tsortByColumn: day_of_week_number")
        if ty == "dateTime": L.append("\t\tformatString: dd/mm/yyyy" if t == "date" else "\t\tformatString: dd/mm/yyyy hh:nn:ss")
        L += ["\t\tsummarizeBy: none", ""]
    L += [f"\tpartition {disp} = entity", "\t\tmode: directLake", "\t\tsource", f"\t\t\tentityName: {gt}",
          "\t\t\tschemaName: gold", "\t\t\texpressionSource: DatabaseQuery", ""]
    open(os.path.join(d, "tables", f"{disp}.tmdl"), "w", encoding="utf-8").write("\n".join(L))
print("built", root)
