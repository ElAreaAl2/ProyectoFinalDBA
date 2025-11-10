import sys, json
import geopandas as gpd
import pyproj
from shapely.ops import transform
from functools import partial
from pathlib import Path

src_gj = Path("data/ms/ms_co.geojson")
dst_gjl = Path("data/ms/ms_co.geojsonl")

# calcular área en m2 usando proyección métrica (EPSG:3857)
g = gpd.read_file(src_gj)
project = partial(
    pyproj.transform,
    pyproj.CRS("EPSG:4326"),  # lon/lat
    pyproj.CRS("EPSG:3857"),
)
areas = []
for geom in g.geometry:
    if geom is None:
        areas.append(None)
        continue
    gm = transform(project, geom)
    areas.append(gm.area)

g["area_m2"] = areas

with open(dst_gjl, "w", encoding="utf-8") as w:
    for geom, area in zip(g.geometry, g["area_m2"]):
        rec = {
            "geometry": json.loads(geom.to_json()),
            "properties": {"area_m2": float(area) if area is not None else 0.0}
        }
        w.write(json.dumps(rec, ensure_ascii=False) + "\n")

print("OK GeoJSONL:", dst_gjl)
