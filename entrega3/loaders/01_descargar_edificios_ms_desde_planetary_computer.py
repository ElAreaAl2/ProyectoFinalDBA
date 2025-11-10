# entrega3/loaders/01_descargar_edificios_ms_desde_planetary_computer.py
from pathlib import Path
from tqdm import tqdm
import geopandas as gpd
import pandas as pd
import requests
import pystac_client
import planetary_computer

OUT_DIR = Path("data/ms"); OUT_DIR.mkdir(parents=True, exist_ok=True)
# BBOX Colombia [minLon, minLat, maxLon, maxLat]
BBOX_CO = [-79.1, -4.3, -66.8, 13.5]
REQ_TIMEOUT = 120  # un poco más alto por si hay tiles pesados

def _looks_like_colombia_text(s: str) -> bool:
    """Heurística textual para 'Colombia' o Sudamérica."""
    s = (s or "").lower()
    return (
        "colombia" in s or
        "countryname=colombia" in s or
        "regionname=southamerica" in s or
        "south america" in s
    )

def _item_is_colombia(it) -> bool:
    """Filtra ítems que parecen de Colombia (por id y por assets firmados)."""
    if _looks_like_colombia_text(getattr(it, "id", "")):
        return True

    # Firma el item para que sus assets tengan SAS donde aplique
    try:
        signed = planetary_computer.sign(it)
    except Exception:
        signed = it

    for a in signed.assets.values():
        href = getattr(a, "href", "")
        if isinstance(href, str) and _looks_like_colombia_text(href):
            return True
        # como último recurso, firmanos el asset puntual y revisemos
        try:
            sa = planetary_computer.sign(a)
            sh = getattr(sa, "href", "")
            if isinstance(sh, str) and _looks_like_colombia_text(sh):
                return True
        except Exception:
            pass

    return False

def _download_https(url: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=REQ_TIMEOUT) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                if chunk:
                    f.write(chunk)

def pick_asset(item):
    """Elige el mejor asset (parquet si existe, luego data, luego geojson)."""
    for key in ("parquet", "data", "geojson"):
        if key in item.assets:
            return item.assets[key]
    # fallback: cualquiera
    return next(iter(item.assets.values()))

def _signed_asset_url(asset) -> str | None:
    """
    Devuelve una URL HTTPS firmada (con SAS). Evita alternates sin SAS.
    Si no se logra firmar o no trae 'sig=', retorna None.
    """
    try:
        sa = planetary_computer.sign(asset)  # firmar el asset concreto
        href = getattr(sa, "href", "")
    except Exception:
        href = getattr(asset, "href", "")

    if not isinstance(href, str) or not href.startswith("https://"):
        return None

    # Debe verse 'sig=' (típico SAS). Si no, probablemente no está firmada.
    if "sig=" not in href and "se=" not in href and "sv=" not in href:
        return None
    return href

def main():
    cat = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    # Pedimos un lote grande y filtramos client-side por Colombia
    search = cat.search(
        collections=["ms-buildings"],
        bbox=BBOX_CO,
        max_items=800,  # puedes subir si quieres más cobertura
        limit=200
    )
    items = list(search.items())
    if not items:
        raise SystemExit("No se encontraron items para la búsqueda. Revisa el BBOX.")

    items_co = [it for it in items if _item_is_colombia(it)]
    if not items_co:
        raise SystemExit(
            "No encontré ítems con referencias claras a Colombia. "
            "Sube max_items o elimina bbox para explorar más."
        )

    print(f"Items STAC totales: {len(items)} | Colombia detectados: {len(items_co)}")

    local_parts = []
    for it in tqdm(items_co, desc="Descargando assets (Colombia)"):
        # Firma el item y el asset elegido; usa SOLO la URL firmada con SAS.
        signed_item = planetary_computer.sign(it)
        asset = pick_asset(signed_item)

        url = _signed_asset_url(asset)
        if not url:
            # como plan B, intenta firmar el asset del item original
            try:
                url = _signed_asset_url(pick_asset(it))
            except Exception:
                url = None

        if not url:
            print(f"[WARN] {it.id}: no encontré URL https firmada (SAS) utilizable (skip)")
            continue

        # Decide extensión por la URL final
        ext = ".parquet" if ".parquet" in url else ".geojson"
        out = OUT_DIR / f"{it.id}{ext}"
        if out.exists():
            local_parts.append(out)
            continue

        try:
            _download_https(url, out)
            local_parts.append(out)
        except Exception as e:
            print(f"[WARN] {it.id}: no descargado → {e}")

    # Unión a un solo archivo del país
    union_gj = OUT_DIR / "ms_co.geojson"
    union_pq = OUT_DIR / "ms_co_union.parquet"

    parts_pq = [p for p in local_parts if p.suffix == ".parquet"]
    if parts_pq:
        frames = []
        for p in parts_pq:
            try:
                g = gpd.read_parquet(p); frames.append(g)
            except Exception as e:
                print("Error leyendo parquet", p, e)
        if frames:
            g_all = pd.concat(frames, ignore_index=True)
            g_all.to_parquet(union_pq, index=False)
            g_all = g_all.set_crs(4326, allow_override=True)
            minx, miny, maxx, maxy = BBOX_CO
            g_all = g_all.cx[minx:maxx, miny:maxy]
            g_all.to_file(union_gj, driver="GeoJSON")
            print("OK GeoJSON:", union_gj)
            return

    parts_gj = [p for p in local_parts if p.suffix == ".geojson"]
    if parts_gj:
        frames = []
        for p in parts_gj:
            try:
                frames.append(gpd.read_file(p))
            except Exception as e:
                print("Error leyendo geojson", p, e)
        if frames:
            g_all = pd.concat(frames, ignore_index=True).set_crs(4326, allow_override=True)
            minx, miny, maxx, maxy = BBOX_CO
            g_all = g_all.cx[minx:maxx, miny:maxy]
            g_all.to_file(union_gj, driver="GeoJSON")
            print("OK GeoJSON:", union_gj)
            return

    raise SystemExit("No fue posible generar ms_co.geojson")

if __name__ == "__main__":
    main()
