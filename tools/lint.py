import json, glob, os, itertools
base = r"C:\Users\mikem\retail_work\out\Retail_Medallion_Report.Report\definition\pages"
bad = 0
for pg in sorted(os.listdir(base)):
    if not os.path.isdir(os.path.join(base, pg)): continue
    vs = []
    for f in glob.glob(os.path.join(base, pg, "visuals", "*", "visual.json")):
        j = json.load(open(f)); p = j["position"]; vs.append((j["name"], p["x"], p["y"], p["x"] + p["width"], p["y"] + p["height"]))
    for n, x0, y0, x1, y1 in vs:
        if x0 < 0 or y0 < 0 or x1 > 1280 or y1 > 720: print(pg, "OUT OF BOUNDS", n, x0, y0, x1, y1); bad += 1
    for a, b in itertools.combinations(vs, 2):
        if a[1] < b[3] and b[1] < a[3] and a[2] < b[4] and b[2] < a[4]: print(pg, "OVERLAP", a[0], b[0]); bad += 1
    print(pg, len(vs), "visuals")
print("issues:", bad)
