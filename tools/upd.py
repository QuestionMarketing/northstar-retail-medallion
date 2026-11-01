import base64, json, os, subprocess, sys, time
root = r"out\Retail_Medallion_Report.Report"
parts = []
for d, _, fs in os.walk(root):
    for f in fs:
        p = os.path.join(d, f)
        rel = os.path.relpath(p, root).replace("\\", "/")
        parts.append({"path": rel, "payload": base64.b64encode(open(p, "rb").read()).decode(), "payloadType": "InlineBase64"})
json.dump({"definition": {"parts": parts}}, open("upd_body.json", "w"))
ws = "e273bcea-e41a-4392-a66c-ceeb30ca5a26"; rid = "a7fc65b8-78a6-4c26-b4aa-9d749ba5899e"
r = subprocess.run(["fab", "api", "-X", "post", f"workspaces/{ws}/reports/{rid}/updateDefinition", "-i", "upd_body.json", "--show_headers"], capture_output=True, text=True)
print(r.stdout[-1500:], r.stderr[-800:])
