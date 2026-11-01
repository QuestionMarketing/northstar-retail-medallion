"""Convert '# %%' py sources into Fabric notebook folders bound to retail_lakehouse."""
import json, sys, os, uuid

WS = "e273bcea-e41a-4392-a66c-ceeb30ca5a26"
LH = "fcade048-276b-4675-b0f0-d177d35b1be4"

def build(src, name, outdir):
    text = open(src, encoding="utf-8").read()
    cells = []
    for chunk in text.split("# %%")[1:]:
        md = chunk.startswith(" [markdown]")
        body = chunk[len(" [markdown]"):] if md else chunk
        body = body.strip("\n")
        if md:
            body = "\n".join(l[2:] if l.startswith("# ") else l.lstrip("#") for l in body.splitlines())
            cells.append({"cell_type": "markdown", "metadata": {}, "source": body.splitlines(True)})
        else:
            cells.append({"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": None,
                          "source": body.splitlines(True)})
    nb = {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
          "metadata": {"kernel_info": {"name": "synapse_pyspark"},
                       "kernelspec": {"name": "synapse_pyspark", "display_name": "synapse_pyspark"},
                       "language_info": {"name": "python"},
                       "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
                       "dependencies": {"lakehouse": {"default_lakehouse": LH,
                                                      "default_lakehouse_name": "retail_lakehouse",
                                                      "default_lakehouse_workspace_id": WS,
                                                      "known_lakehouses": [{"id": LH}]}}}}
    d = os.path.join(outdir, name + ".Notebook")
    os.makedirs(d, exist_ok=True)
    json.dump(nb, open(os.path.join(d, "notebook-content.ipynb"), "w", encoding="utf-8"), indent=1)
    plat = {"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "Notebook", "displayName": name},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())}}
    json.dump(plat, open(os.path.join(d, ".platform"), "w"), indent=1)
    print("built", d)

if __name__ == "__main__":
    build(sys.argv[1], sys.argv[2], sys.argv[3])
