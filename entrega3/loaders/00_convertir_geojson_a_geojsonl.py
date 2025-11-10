import json, sys
src, dst = sys.argv[1], sys.argv[2]
fc = json.load(open(src, encoding="utf-8"))
with open(dst, "w", encoding="utf-8") as w:
    for f in fc["features"]:
        w.write(json.dumps({
            "geometry": f["geometry"],
            "properties": f.get("properties", {})
        }, ensure_ascii=False) + "\n")
