import json, os, shutil, uuid
WS = "e273bcea-e41a-4392-a66c-ceeb30ca5a26"
MODEL_ID = "76447b95-5dac-4641-b940-9d9388ffb47f"
NAME = "Retail_Medallion_Report"
root = rf"C:\Users\mikem\retail_work\out\{NAME}.Report"
shutil.rmtree(root, ignore_errors=True)
d = os.path.join(root, "definition"); os.makedirs(d)
S = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
def w(p, o):
    os.makedirs(os.path.dirname(p), exist_ok=True); json.dump(o, open(p, "w"), indent=1)

NAVY, INK, TEAL, GREY = "#0B2545", "#1F2933", "#13A89E", "#616E7C"
PAL = [NAVY, TEAL, "#5B8DB8", "#F2A541", "#8D99AE", "#C8553D", "#2D6A4F", "#A9B8C9"]
FONT = "Segoe UI"

theme = {"name": "NorthstarConsulting", "dataColors": PAL, "background": "#FFFFFF", "foreground": INK,
  "tableAccent": NAVY, "good": TEAL, "neutral": "#F2A541", "bad": "#C8553D", "maximum": NAVY, "center": "#A9B8C9", "minimum": "#E6EDF5",
  "textClasses": {
    "label": {"fontFace": FONT, "fontSize": 8, "color": {"solid": {"color": GREY}}},
    "callout": {"fontFace": "Segoe UI Semibold", "fontSize": 9, "color": {"solid": {"color": NAVY}}},
    "title": {"fontFace": "Segoe UI Semibold", "fontSize": 9, "color": {"solid": {"color": INK}}},
    "header": {"fontFace": "Segoe UI Semibold", "fontSize": 9, "color": {"solid": {"color": INK}}}},
  "visualStyles": {"*": {"*": {
    "title": [{"show": True, "fontColor": {"solid": {"color": INK}}, "fontSize": 9, "fontFamily": "Segoe UI Semibold", "alignment": "left"}],
    "background": [{"show": True, "color": {"solid": {"color": "#FFFFFF"}}, "transparency": 0}],
    "border": [{"show": True, "color": {"solid": {"color": "#E1E6EC"}}, "radius": 4}],
    "dropShadow": [{"show": False}],
    "padding": [{"top": 8, "bottom": 8, "left": 12, "right": 12}],
    "categoryAxis": [{"showAxisTitle": False, "fontSize": 8}], "valueAxis": [{"showAxisTitle": False, "fontSize": 8}], "labels": [{"fontSize": 8}],
    "legend": [{"showTitle": False, "fontSize": 8}]}},
    "card": {"*": {"labels": [{"fontSize": 9, "color": {"solid": {"color": NAVY}}}], "categoryLabels": [{"show": False}]}},
    "slicer": {"*": {"header": [{"show": False}], "items": [{"fontSize": 8}]}}}}
w(os.path.join(root, "StaticResources", "RegisteredResources", "NorthstarConsulting.json"), theme)

w(os.path.join(root, ".platform"), {"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {"type": "Report", "displayName": NAME}, "config": {"version": "2.0", "logicalId": str(uuid.uuid4())}})
w(os.path.join(root, "definition.pbir"), {"version": "4.0", "datasetReference": {"byConnection": {
  "connectionString": f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/Retail_VS;Initial Catalog=Retail_Medallion_Model;semanticmodelid={MODEL_ID}",
  "pbiServiceModelId": None, "pbiModelVirtualServerName": "sobe_wowvirtualserver", "pbiModelDatabaseName": MODEL_ID,
  "name": "EntityDataSource", "connectionType": "pbiServiceXmlaStyleLive"}}})
w(os.path.join(d, "version.json"), {"$schema": S + "versionMetadata/1.0.0/schema.json", "version": "2.0.0"})
w(os.path.join(d, "report.json"), {"$schema": S + "report/2.0.0/schema.json",
  "themeCollection": {"baseTheme": {"name": "CY24SU10", "reportVersionAtImport": "5.61", "type": "SharedResources"},
                      "customTheme": {"name": "NorthstarConsulting.json", "reportVersionAtImport": "5.61", "type": "RegisteredResources"}},
  "resourcePackages": [{"name": "RegisteredResources", "type": "RegisteredResources",
     "items": [{"name": "NorthstarConsulting.json", "path": "NorthstarConsulting.json", "type": "CustomTheme"}]}],
  "settings": {"useStylableVisualContainerHeader": True}})

lit = lambda v: {"expr": {"Literal": {"Value": v}}}
def fld(kind, ent, prop): return {kind: {"Expression": {"SourceRef": {"Entity": ent}}, "Property": prop}}
NICE = {"resolved_customer_id": "Customer ID", "sales_sub_channel": "Sub-channel", "store_name": "Store", "store_type": "Store type",
        "rfm_segment": "Segment", "recency_days": "Days since purchase", "order_count": "Orders", "lifetime_spend": "Lifetime spend",
        "churn_risk": "Churn risk", "preferred_channel": "Preferred channel", "city": "City", "category": "Category", "region": "Region"}
NOBLANK = {"store_name", "region", "city"}
SHORT = {"High-Risk Customers": "High-risk custs", "Retention Likelihood 30d": "30d retention", "Expected 30d Spend": "Exp. 30d spend", "Run Rate 30d Projection": "30d projection"}
def proj(p, nice=False):
    o = {"field": fld(*p), "queryRef": f"{p[1]}.{p[2]}", "nativeQueryRef": p[2]}
    if nice and p[0] == "Column": o["displayName"] = NICE.get(p[2], p[2].replace("_", " ").title())
    return o
M = lambda n: ("Measure", "Sales", n)
C = lambda e, n: ("Column", e, n)

class Page:
    def __init__(self, pid, title, sub):
        short = {"exec": "Summary", "channel": "Channel", "product": "Product", "stores": "Stores", "customers": "Customer", "churn": "Churn Risk", "outlook": "Outlook", "returns": "Returns", "ask": "Ask", "about": "About"}[pid]
        self.pid, self.title, self.n = pid, title, 0
        self.dir = os.path.join(d, "pages", pid)
        w(os.path.join(self.dir, "page.json"), {"$schema": S + "page/2.0.0/schema.json", "name": pid, "displayName": short,
          "displayOption": "FitToPage", "height": 720, "width": 1280})
        self.text(f"{pid}_title", 24, 4, 480, 64, [(title, "Segoe UI Semibold", 9, NAVY), (sub, FONT, 8, GREY)], pad=True)
        self.text(f"{pid}_rule", 24, 70, 1232, 3, [("", FONT, 8, NAVY)], bg=TEAL)
        self.visual(f"{pid}_nav", "pageNavigator", 520, 18, 736, 40, {}, alt="Page navigation")
    def _w(self, name, obj):
        self.n += 1
        obj["position"]["tabOrder"] = self.n; obj["position"]["z"] = self.n * 10
        w(os.path.join(self.dir, "visuals", name, "visual.json"), obj)
    def text(self, name, x, y, wd, ht, paras, bg=None, pad=False):
        runs = [{"textRuns": [{"value": t, "textStyle": {"fontFamily": f, "fontSize": f"{s}pt", "color": c}}]} for t, f, s, c in paras]
        vco = {"title": [{"properties": {"show": lit("false")}}], "border": [{"properties": {"show": lit("false")}}],
               "background": [{"properties": {"show": lit("true" if bg else "false")}}]}
        if bg: vco["background"][0]["properties"]["color"] = {"solid": {"color": lit(f"'{bg}'")}}
        if pad: vco["padding"] = [{"properties": {k: lit("0D") for k in ("top", "bottom", "left", "right")}}]
        self._w(name, {"$schema": S + "visualContainer/2.7.0/schema.json", "name": name,
          "position": {"x": x, "y": y, "width": wd, "height": ht},
          "visual": {"visualType": "textbox", "objects": {"general": [{"properties": {"paragraphs": runs}}]}, "visualContainerObjects": vco}})
    def visual(self, name, vtype, x, y, wd, ht, roles, title=None, alt=None, sort=None, sync=None, objects=None, extra=None, flt=None):
        q = {"queryState": {r: {"projections": [proj(p, vtype == "tableEx") for p in ps]} for r, ps in roles.items()}} if roles else None
        v = {"visualType": vtype, "drillFilterOtherVisuals": True}
        if q:
            if sort: q["sortDefinition"] = {"sort": [{"field": fld(*sort[0]), "direction": sort[1]}], "isDefaultSort": True}
            v["query"] = q
        vco = {"general": [{"properties": {"altText": lit(f"'{alt or title or name}'")}}]}
        if title:
            vco["title"] = [{"properties": {"show": lit("true"), "text": lit(f"'{title}'")}}]
        else:
            vco["title"] = [{"properties": {"show": lit("false")}}]
        v["visualContainerObjects"] = vco
        if extra: vco.update(extra)
        if objects: v["objects"] = objects
        o = {"$schema": S + "visualContainer/2.7.0/schema.json", "name": name,
             "position": {"x": x, "y": y, "width": wd, "height": ht}, "visual": v}
        if flt:
            o["filterConfig"] = {"filters": [{"name": f"f{i}", "field": {"Column": {"Expression": {"SourceRef": {"Entity": e}}, "Property": c}},
                "type": "Advanced", "filter": {"Version": 2, "From": [{"Name": "s", "Entity": e, "Type": 0}],
                "Where": [{"Condition": {"Not": {"Expression": {"Comparison": {"ComparisonKind": 0,
                    "Left": {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": c}}, "Right": {"Literal": {"Value": "null"}}}}}}}]}}
                for i, (e, c) in enumerate(flt)]}
        if sync: v["syncGroup"] = {"groupName": sync, "fieldChanges": True, "filterChanges": True}
        self._w(name, o)
    def slicers(self, region=False, specs=None):
        tile = {"data": [{"properties": {"mode": lit("'Basic'")}}], "general": [{"properties": {"orientation": lit("1D")}}],
                "header": [{"properties": {"show": lit("false")}}]}
        drop = {"data": [{"properties": {"mode": lit("'Dropdown'")}}], "header": [{"properties": {"show": lit("false")}}]}
        if specs is None:
            specs = [("month", C("Date", "month_name"), "Month", 520 if region else 616, drop), ("chan", C("Sales", "sales_channel"), "Channel", 340 if region else 600, tile)]
            if region: specs.append(("reg", C("Store", "region"), "Region", 340, drop))
        x = 24
        for i, (g, f, t, wd, obj) in enumerate(specs):
            self.visual(f"{self.pid}_s{i}", "slicer", x, 76, wd, 64, {"Values": [f]}, title=t, alt=f"{t} slicer", sync=g, objects=obj,
                        extra={"padding": [{"properties": {"top": lit("2D"), "bottom": lit("2D"), "left": lit("8D"), "right": lit("8D")}}]})
            x += wd + 16
    def insight(self, key):
        if os.environ.get("NONARR"): return
        self.narr(f"{self.pid}_nw", 24, 592, 760, 116, "What's happening", f"Narr {key}", "#EEF4F8")
        self.narr(f"{self.pid}_na", 800, 592, 456, 116, "Recommended action", f"Act {key}", "#FFF4E0")
    def narr(self, name, x, y, wd, ht, label, measure, bg):
        # a one-cell wrapped table is the reliable way to show long measure-driven text
        obj = {"columnHeaders": [{"properties": {"fontSize": lit("1D"), "fontColor": {"solid": {"color": lit(f"'{bg}'")}},
                                                 "backColor": {"solid": {"color": lit(f"'{bg}'")}}, "outline": lit("'None'")}}],
               "grid": [{"properties": {"gridVertical": lit("false"), "gridHorizontal": lit("false"), "rowPadding": lit("0L")}}],
               "values": [{"properties": {"fontSize": lit("8D"), "fontColor": {"solid": {"color": lit(f"'{INK}'")}},
                                          "backColor": {"solid": {"color": lit(f"'{bg}'")}}, "wordWrap": lit("true")}}],
               "total": [{"properties": {"totals": lit("false")}}]}
        extra = {"background": [{"properties": {"show": lit("true"), "color": {"solid": {"color": lit(f"'{bg}'")}}}}],
                 "padding": [{"properties": {"top": lit("4D"), "bottom": lit("4D"), "left": lit("10D"), "right": lit("10D")}}],
                 "border": [{"properties": {"show": lit("false")}}]}
        self.visual(name, "tableEx", x, y, wd, ht, {"Values": [M(measure)]}, title=label, alt=label, objects=obj, extra=extra)
    def kpis(self, ms, y=146, h=84):
        n = len(ms); wd = (1232 - 16 * (n - 1)) // n
        for i, m in enumerate(ms):
            self.visual(f"{self.pid}_k{i}", "card", 24 + i * (wd + 16), y, wd, h, {"Values": [M(m)]}, title=SHORT.get(m, m))
    def grid(self, items):
        """items: list of rows; each row list of (name,vtype,weight,roles,title,sort)"""
        rows = len(items); top, bottom = 238, 584
        rh = (bottom - top - 16 * (rows - 1)) // rows
        for r, row in enumerate(items):
            tw = sum(i[2] for i in row); avail = 1232 - 16 * (len(row) - 1); x = 24
            for k, (nm, vt, wt, roles, title, sort, *ob) in enumerate(row):
                wd = round(avail * wt / tw) if k < len(row) - 1 else 24 + 1232 - x
                flt = [(c[1], c[2]) for rl in roles.values() for c in rl if c[0] == "Column" and c[1] == "Store" and c[2] in NOBLANK]
                self.visual(f"{self.pid}_{nm}", vt, x, top + r * (rh + 16), wd, rh, roles, title=title, sort=sort, objects=ob[0] if ob else None, flt=flt or None)
                x += wd + 16

NS = M("Net Sales")
def bar(nm, ent, col, title, wt=1, meas=NS, vt="barChart"):
    return (nm, vt, wt, {"Category": [C(ent, col)], "Y": [meas]}, title, (meas, "Descending"))
def table(nm, cols, title, wt=1, sort=NS):
    return (nm, "tableEx", wt, {"Values": cols}, title, (sort, "Descending"))
def tree(nm, groups, title, wt=1, meas=NS):
    return (nm, "treemap", wt, {"Group": groups, "Values": [meas]}, title, (meas, "Descending"))
def funnel(nm, ent, col, title, wt=1, meas=NS):
    return (nm, "funnel", wt, {"Category": [C(ent, col)], "Y": [meas]}, title, (meas, "Descending"))
def scat(nm, cat, x, y, size, title, wt=1, series=None):
    r = {"Category": [cat], "X": [x], "Y": [y], "Size": [size]}
    if series: r["Series"] = [series]
    return (nm, "scatterChart", wt, r, title, None)
def donut(nm, ent, col, title, wt=1, meas=NS):
    return (nm, "donutChart", wt, {"Category": [C(ent, col)], "Y": [meas]}, title, None)
def ribbon(nm, cat, series, title, wt=1, meas=NS):
    return (nm, "ribbonChart", wt, {"Category": [cat], "Series": [series], "Y": [meas]}, title, None)
def waterfall(nm, cat, title, wt=1, meas=NS):
    return (nm, "waterfallChart", wt, {"Category": [cat], "Y": [meas]}, title, None)
def combo(nm, cat, cols, lines, title, wt=1):
    return (nm, "lineClusteredColumnComboChart", wt, {"Category": [cat], "Y": cols, "Y2": lines}, title, None)
def tile(n=None):
    return {"data": [{"properties": {"mode": lit("'Basic'")}}], "general": [{"properties": {"orientation": lit("1D")}}], "header": [{"properties": {"show": lit("false")}}]}
pages = []

p = Page("exec", "Executive Summary", "Northstar Retail  |  Q4 2025 trading performance, all channels"); pages.append(p)
p.slicers(); p.kpis(["Net Sales", "Net Sales MoM %", "Orders", "Avg Order Value", "Online Share %", "Return Rate"])
p.grid([[combo("trend", C("Date", "month_name"), [NS], [M("Return Rate")], "Monthly net sales (columns) and return rate (line)", 3),
         donut("chan", "Sales", "sales_channel", "Net sales by channel", 2)],
        [tree("dept", [C("Product", "department"), C("Product", "category")], "Where sales come from: department and category", 3),
         funnel("reg", "Store", "region", "Net sales by region", 2)]])
p.insight("Summary")

p = Page("channel", "Channel Performance", "Online vs store, and the digital touchpoints inside online"); pages.append(p)
p.slicers(); p.kpis(["Net Sales", "Online Share %", "Units Sold", "Discount %", "Return Rate", "Avg Order Value"])
p.grid([[ribbon("mon", C("Date", "month_name"), C("Sales", "sales_channel"), "Channel rank and share by month", 3),
         tree("sub", [C("Sales", "sales_sub_channel")], "Net sales by sub-channel", 2)],
        [("line", "lineChart", 3, {"Category": [C("Date", "date")], "Series": [C("Sales", "sales_channel")], "Y": [NS]}, "Daily net sales by channel", None),
         scat("tbl", C("Sales", "sales_sub_channel"), M("Orders"), M("Avg Order Value"), NS, "Sub-channels: orders vs order value (bubble = net sales)", 3)]])
p.insight("Channel")

p = Page("product", "Product & Category", "What sells, what discounts, what comes back"); pages.append(p)
p.slicers(); p.kpis(["Net Sales", "Units Sold", "Net Sales Share %", "Discount %", "Return Rate"])
p.grid([[tree("dept", [C("Product", "department"), C("Product", "category")], "Net sales by department and category", 3),
         funnel("brand", "Product", "brand", "Top brands by net sales", 2)],
        [waterfall("cat", C("Product", "category"), "Category contribution to net sales", 3),
         scat("tbl", C("Product", "category"), M("Discount %"), M("Return Rate"), NS, "Categories: discounting vs returns (bubble = net sales)", 3)]])
p.insight("Product")

p = Page("stores", "Store & Region", "Where the network earns its sales"); pages.append(p)
p.slicers(region=True); p.kpis(["Net Sales", "Net Sales per Store", "Orders", "Avg Order Value", "Return Rate"])
p.grid([[tree("reg", [C("Store", "region"), C("Store", "store_type")], "Net sales by region and store type", 3),
         donut("typ", "Store", "store_type", "Net sales by store type", 2),
         funnel("city", "Store", "city", "Net sales by city", 2)],
        [scat("tbl", C("Store", "store_name"), M("Orders"), M("Avg Order Value"), NS, "Store map: traffic vs basket (bubble = net sales, colour = region)", 1, C("Store", "region"))]])
p.insight("Stores")

p = Page("customers", "Customer View", "Who is buying, and how much of the base we can see"); pages.append(p)
p.slicers(); p.kpis(["Net Sales", "Orders", "Avg Order Value", "Identified Customer Share %"])
p.grid([[donut("gen", "Customer", "gender", "Net sales by gender", 1), donut("pref", "Customer", "preferred_channel", "Net sales by preferred channel", 1),
         funnel("stat", "Customer", "customer_status", "Net sales by customer status", 1)],
        [tree("cty", [C("Customer", "county")], "Net sales by customer county", 2),
         ("ident", "gauge", 1, {"Y": [M("Identified Customer Share %")]}, "Share of lines linked to a known customer", None)]])
p.insight("Customer")

p = Page("churn", "Churn & Retention Risk", "Predicted chance each identified customer lapses in the next 30 days"); pages.append(p)
p.slicers(specs=[("band", C("Churn", "risk_band"), "Risk band", 400, tile(3)), ("seg", C("Churn", "rfm_segment"), "Customer segment", 816, tile(5))])
p.kpis(["Customers Scored", "High-Risk Customers", "High-Risk Share %", "Revenue at Risk", "Retention Likelihood 30d", "Expected 30d Spend"])
CS = M("Customers Scored"); RAR = M("Revenue at Risk"); ACR = M("Avg Churn Risk")
p.grid([[donut("band", "Churn", "risk_band", "Customers by churn-risk band", 2, CS), tree("seg", [C("Churn", "rfm_segment")], "Revenue at risk by customer segment", 3, RAR),
         scat("pref", C("Churn", "rfm_segment"), CS, ACR, RAR, "Segments: size vs churn risk (bubble = revenue at risk)", 3)],
        [table("tbl", [C("Customer", "resolved_customer_id"), C("Customer", "city"), C("Churn", "rfm_segment"), C("Churn", "recency_days"), C("Churn", "order_count"), C("Churn", "lifetime_spend"), C("Churn", "churn_risk"), RAR],
               "Highest revenue-at-risk customers (win-back list)", 1, RAR)]])
p.insight("Churn")

p = Page("outlook", "Outlook & Run-Rate", "Trailing run-rate and a 30-day run-rate projection of net sales"); pages.append(p)
p.slicers(specs=[("chan", C("Sales", "sales_channel"), "Channel", 1232, tile(2))])
p.kpis(["Net Sales L28D", "Net Sales P28D", "Run Rate Trend %", "Run Rate 30d Projection", "Net Sales"])
fc = {"categoryAxis": [{"properties": {"show": lit("true"), "axisType": lit("'Scalar'")}}]}
p.grid([[("fc", "lineChart", 1, {"Category": [C("Date", "date")], "Y": [NS]}, "Daily net sales with trailing run-rate", None, fc)],
        [("dow", "clusteredColumnChart", 1, {"Category": [C("Date", "day_name")], "Y": [NS]}, "Net sales by day of week", None),
         combo("wk", C("Date", "month_name"), [M("Orders")], [M("Avg Order Value")], "Monthly orders (columns) and average order value (line)", 1)]])
p.insight("Outlook")

p = Page("returns", "Returns & Discounting", "Margin leakage: what is given back and what is given away"); pages.append(p)
p.slicers(); p.kpis(["Return Rate", "Returned Amount", "Return Lines", "Discount %", "Discount Amount"])
RR = M("Return Rate"); DS = M("Discount %")
p.grid([[tree("rdept", [C("Product", "department"), C("Product", "category")], "Returned amount by department and category", 1, M("Returned Amount")),
         funnel("dchan", "Sales", "sales_sub_channel", "Discount amount by sub-channel", 1, M("Discount Amount"))],
        [("rtrend", "lineChart", 3, {"Category": [C("Date", "date")], "Y": [RR]}, "Daily return rate", None),
         scat("tbl", C("Product", "category"), DS, RR, M("Returned Amount"), "Categories: discounting vs returns (bubble = returned amount)", 3)]])
p.insight("Returns")

p = Page("ask", "Ask a Question", "Type a question in plain English and get a visual answer"); pages.append(p)
p.visual("ask_qna", "qnaVisual", 24, 90, 800, 610, {}, title="Ask your data", alt="Q&A natural language question box")
p.text("ask_tips", 840, 90, 416, 610, [("Try asking", "Segoe UI Semibold", 9, NAVY),
 ("net sales by sales channel", FONT, 8, INK), ("net sales by month as a line chart", FONT, 8, INK),
 ("top 10 stores by net sales", FONT, 8, INK), ("return rate by category", FONT, 8, INK),
 ("net sales by region as a map", FONT, 8, INK), ("online share % by month", FONT, 8, INK),
 ("revenue at risk by risk band", FONT, 8, INK), ("high-risk customers by preferred channel", FONT, 8, INK),
 ("", FONT, 8, INK), ("Tips", "Segoe UI Semibold", 9, NAVY),
 ("Use the measure names from the model: Net Sales, Orders, Return Rate, Discount %, Revenue at Risk.", FONT, 8, INK),
 ("Add 'as a bar chart', 'as a treemap' or 'as a table' to change the visual type.", FONT, 8, INK),
 ("Data covers Oct to Dec 2025 only.", FONT, 8, INK)], bg="#EEF4F8")

p = Page("about", "About & Data Notes", "How this report is built and how far the numbers can be trusted"); pages.append(p)
paras = [("Lineage", "Segoe UI Semibold", 9, NAVY),
 ("Raw CRM, e-commerce and POS files land in the lakehouse, move through Bronze (as received), Silver (cleaned, deduplicated, customers resolved) and Gold (star schema). A Direct Lake model reads Gold.", FONT, 8, INK),
 ("", FONT, 8, INK), ("Scope", "Segoe UI Semibold", 9, NAVY),
 ("Oct to Dec 2025 only, so month-over-month comparisons are used. Year-over-year is not possible with this data. Values are net of discount and VAT unless labelled gross.", FONT, 8, INK),
 ("", FONT, 8, INK), ("Known data caveats", "Segoe UI Semibold", 9, NAVY),
 ("Around 48% of order lines are anonymous, mostly in store (about 32% identified vs 92% online). Cancelled online lines are excluded. About a third of products lack a unit cost, so no margin is shown. Some customer profiles carry future-dated updates and are flagged in Silver.", FONT, 8, INK),
 ("", FONT, 8, INK), ("Predictive methodology", "Segoe UI Semibold", 9, NAVY),
 ("Churn: a gradient-boosted classifier (Spark ML) trained on identified, non GDPR-deleted customers using recency, frequency, spend, tenure, channel mix and return behaviour in 61-day windows. Trained on features at 30 Nov to predict December purchase; scored at 31 Dec. Churn risk = 1 - probability of buying in the next 30 days. Hold-out AUC is about 0.58: a modest signal given only three months of history, so use it to rank and prioritise, not to forecast individuals. Retrain with 12+ months. Risk bands are relative: High = top 20% of risk. Outlook: a trailing 28-day run-rate projected 30 days forward; a simple extrapolation, not a statistical forecast.", FONT, 8, INK)]
p.text("about_body", 24, 90, 760, 560, paras)
p.text("about_defs", 808, 90, 448, 560, [("Definitions", "Segoe UI Semibold", 9, NAVY),
 ("Net Sales: after discount, before VAT.", FONT, 8, INK), ("Return Rate: return lines / all lines.", FONT, 8, INK),
 ("Discount %: discount / pre-discount net.", FONT, 8, INK), ("Online Share %: online net sales / total.", FONT, 8, INK),
 ("MoM %: change vs the prior calendar month.", FONT, 8, INK)])

w(os.path.join(d, "pages", "pages.json"), {"$schema": S + "pagesMetadata/1.0.0/schema.json",
  "pageOrder": [p.pid for p in pages], "activePageName": "exec"})
print("built", root, sum(p.n for p in pages), "visuals")

