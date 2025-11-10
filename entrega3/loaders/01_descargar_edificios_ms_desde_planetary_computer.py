import os, json, math
from pathlib import Path
from tqdm import tqdm
import geopandas as gpd
import pandas as pd
import pyogrio
import shapely
from shapely.geometry import shape
import pystac_client
import planetary_computer

# 1) Config
OUT_DIR = Path("data/ms"); OUT_DIR.mkdir(parents=True, exist_ok=True)
# BBOX Colombia aprox [minLon, minLat, maxLon, maxLat]
BBOX_CO = [-79.1, -4.3, -66.8, 13.5]

# 2) Buscar en STAC la colección de edificios
catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
search = catalog.search(collections=["ms-buildings"], bbox=BBOX_CO, limit=100)

items = list(search.get_items())
if not items:
    raise SystemExit("No se encontraron items STAC para Colombia (ajusta BBOX o revisa conexión).")

print("Items STAC:", len(items))

# 3) Descargar assets (normalmente Parquet o GeoJSON por tile)
def download_asset(href, out_path):
    import requests
    with requests.get(href, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1<<20):
                f.write(chunk)

parquets = []

for it in tqdm(items, desc="Descargando assets"):
    signed = planetary_computer.sign(it)  # firma URLs
    # intenta preferir parquet si existe; si no, usa geojson
    assets = signed.assets
    asset = None
    for k in ("parquet", "data", "geojson"):  # nombres típicos
        if k in assets:
            asset = assets[k]
            break
    if not asset:
        continue
    url = asset.href
    ext = ".parquet" if ".parquet" in url else ".geojson"
    out = OUT_DIR / f"{it.id}{ext}"
    if not out.exists():
        download_asset(url, out)
    if out.suffix == ".parquet":
        parquets.append(out)

# 4) Unificar a GeoParquet (si descargaste varios parquet) y filtrar por BBOX final
union_pq = OUT_DIR / "ms_co_union.parquet"
if parquets:
    # leer por lotes y concatenar
    parts = []
    for p in parquets:
        try:
            g = gpd.read_parquet(p)
            # Normaliza a geometría válida
            g = g.set_geometry(g.geometry.buffer(0))
            parts.append(g)
        except Exception as e:
            print("Error leyendo", p, e)
    if parts:
        g_all = pd.concat(parts, ignore_index=True)
        # Mantén solo polígonos válidos
        g_all = g_all[g_all.geometry.notna()]
        g_all.to_parquet(union_pq, index=False)
        print("Escribí", union_pq)

# 5) Exportar a GeoJSON recortado por BBOX (si prefieres)
gj = OUT_DIR / "ms_co.geojson"
if union_pq.exists():
    g = gpd.read_parquet(union_pq)
else:
    # Si no hubo parquet, intenta coleccionar los geojson descargados
    files = list(OUT_DIR.glob("*.geojson"))
    if not files:
        raise SystemExit("No se logró preparar ms_co.geojson")
    g = pd.concat([gpd.read_file(fp) for fp in files], ignore_index=True)

# Reproyecta a WGS84 por si acaso
g = g.set_crs(4326, allow_override=True)
# recorte suave por BBOX
minx, miny, maxx, maxy = BBOX_CO
g = g.cx[minx:maxx, miny:maxy]
g.to_file(gj, driver="GeoJSON")
print("OK GeoJSON:", gj)
