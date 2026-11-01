import json, os, uuid
WS = "e273bcea-e41a-4392-a66c-ceeb30ca5a26"
ids = [("Bronze_Ingest", "643134e1-581e-4b98-928b-26798bbfadd2"),
       ("Silver_Clean", "de2bb979-6286-4246-9ea5-aa34530d5e66"),
       ("Gold_Model", "9fd1de9d-721f-41d8-a329-2d98faa8c674"),
       ("Gold_Churn", "c824679d-9a3f-4eb1-8ab5-6575bbb9d665")]
acts, prev = [], None
for n, i in ids:
    acts.append({"name": n, "type": "TridentNotebook",
                 "dependsOn": [{"activity": prev, "dependencyConditions": ["Succeeded"]}] if prev else [],
                 "policy": {"timeout": "0.12:00:00", "retry": 0, "retryIntervalInSeconds": 30, "secureInput": False, "secureOutput": False},
                 "typeProperties": {"notebookId": i, "workspaceId": WS}})
    prev = n
content = {"properties": {"description": "Retail medallion: Bronze -> Silver -> Gold", "activities": acts}}
d = r"C:\Users\mikem\retail_work\out\PL_Retail_Medallion.DataPipeline"
os.makedirs(d, exist_ok=True)
json.dump(content, open(d + r"\pipeline-content.json", "w"), indent=1)
json.dump({"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
           "metadata": {"type": "DataPipeline", "displayName": "PL_Retail_Medallion"},
           "config": {"version": "2.0", "logicalId": str(uuid.uuid4())}}, open(d + r"\.platform", "w"), indent=1)
