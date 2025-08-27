# rename_by_workflow_name.py
import json, pathlib, re

def sanitize(s):
    s = (s or "").strip()
    return re.sub(r'[\\/:*?"<>|]+', "_", s) or "unnamed_workflow"

dump = pathlib.Path("./exported_workflows")
for p in dump.glob("*.json"):
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        name = sanitize(data.get("name") or data.get("workflow", {}).get("name"))
        if name and p.stem != name:
            newp = p.with_name(f"{name}.json")
            if newp.exists():
                # 防撞名
                i = 2
                while newp.exists():
                    newp = p.with_name(f"{name}_{i}.json")
                    i += 1
            p.rename(newp)
            print("renamed:", p.name, "->", newp.name)
    except Exception as e:
        print("skip:", p.name, e)


# RUN this py after running in docker exec:
# n8n export:workflow --output .\exported_workflows --separate --all