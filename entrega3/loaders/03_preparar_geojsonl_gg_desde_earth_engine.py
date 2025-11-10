import json
import geopandas as gpd
import pyproj
from shapely.ops import transform
from functools import partial
from pathlib import Path

src = Path("data/gg/gg_co.geojson")
dst = Path("data/gg/gg_co.geojsonl")

g = gpd.read_file(src).set_crs(4326, allow_override=True)

project = partial(pyproj.transform, pyproj.CRS("EPSG:4326"), pyproj.CRS("EPSG:3857"))
areas = []
for geom in g.geometry:
    gm = transform(project, geom) if geom is not None else None
    areas.append(gm.area if gm is not None else 0.0)
g["area_m2"] = areas

with open(dst, "w", encoding="utf-8") as w:
    for _, row in g.iterrows():
        props = {"area_m2": float(row["area_m2"])}
        # muchos dumps traen 'confidence' en el campo 'confidence'
        if "confidence" in g.columns and row["confidence"] is not None:
            props["confidence"] = float(row["confidence"])
        w.write(json.dumps({"geometry": json.loads(row.geometry.to_json()),
                            "properties": props}, ensure_ascii=False) + "\n")

print("OK:", dst)
