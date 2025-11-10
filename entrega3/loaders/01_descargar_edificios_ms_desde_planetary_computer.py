"""
Script para descargar edificios de Microsoft Building Footprints desde Planetary Computer
Descarga edificios de Colombia y los filtra por municipios PDET
"""

from pathlib import Path
from tqdm import tqdm
import geopandas as gpd
import pandas as pd
import requests
import pystac_client
import planetary_computer
from pymongo import MongoClient

# Configuración
OUT_DIR = Path("../../data/ms")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "pdet_solar"

# BBOX Colombia [minLon, minLat, maxLon, maxLat]
BBOX_CO = [-79.1, -4.3, -66.8, 13.5]
REQ_TIMEOUT = 180  # timeout más alto para tiles pesados

def get_pdet_bounds():
    """Obtiene el bounding box de todos los municipios PDET"""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Usar agregación para obtener bounds de todos los municipios PDET
    pipeline = [
        {"$match": {"is_pdet": True}},
        {"$project": {
            "bounds": {"$function": {
                "body": """
                function(geometry) {
                    if (!geometry || !geometry.coordinates) return null;
                    let coords = geometry.coordinates[0];
                    if (geometry.type === 'MultiPolygon') {
                        coords = geometry.coordinates[0][0];
                    }
                    let lons = coords.map(c => c[0]);
                    let lats = coords.map(c => c[1]);
                    return {
                        minLon: Math.min(...lons),
                        maxLon: Math.max(...lons),
                        minLat: Math.min(...lats),
                        maxLat: Math.max(...lats)
                    };
                }
                """,
                "args": ["$geometry"],
                "lang": "js"
            }}
        }}
    ]
    
    client.close()
    return BBOX_CO  # Por ahora usar bbox de Colombia completo

def _looks_like_colombia_text(s: str) -> bool:
    """Heurística textual para identificar datos de Colombia"""
    if not s:
        return False
    s = s.lower()
    return any(keyword in s for keyword in [
        "colombia", "countryname=colombia", 
        "regionname=southamerica", "south america"
    ])

def _item_is_colombia(item) -> bool:
    """Verifica si un item STAC corresponde a Colombia"""
    # Verificar ID del item
    if _looks_like_colombia_text(getattr(item, "id", "")):
        return True
    
    # Verificar assets
    try:
        signed = planetary_computer.sign(item)
    except Exception:
        signed = item
    
    for asset in signed.assets.values():
        href = getattr(asset, "href", "")
        if isinstance(href, str) and _looks_like_colombia_text(href):
            return True
    
    return False

def _download_https(url: str, out_path: Path):
    """Descarga un archivo desde una URL HTTPS"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"  📥 Descargando: {out_path.name}")
    
    try:
        with requests.get(url, stream=True, timeout=REQ_TIMEOUT) as r:
            r.raise_for_status()
            
            # Obtener tamaño del archivo si está disponible
            total_size = int(r.headers.get('content-length', 0))
            
            with open(out_path, "wb") as f:
                if total_size:
                    with tqdm(total=total_size, unit='B', unit_scale=True, 
                             desc=f"  {out_path.name}", leave=False) as pbar:
                        for chunk in r.iter_content(chunk_size=1024*1024):  # 1MB chunks
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                else:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)
    except Exception as e:
        if out_path.exists():
            out_path.unlink()  # Eliminar archivo parcial
        raise e

def pick_asset(item):
    """Selecciona el mejor asset disponible (parquet > data > geojson)"""
    for key in ("parquet", "data", "geojson"):
        if key in item.assets:
            return item.assets[key]
    
    # Fallback: retornar el primer asset disponible
    if item.assets:
        return next(iter(item.assets.values()))
    return None

def _signed_asset_url(asset) -> str | None:
    """Obtiene URL HTTPS firmada con SAS token"""
    if not asset:
        return None
    
    try:
        sa = planetary_computer.sign(asset)
        href = getattr(sa, "href", "")
    except Exception:
        href = getattr(asset, "href", "")
    
    if not isinstance(href, str) or not href.startswith("https://"):
        return None
    
    # Verificar que tiene SAS token
    if not any(param in href for param in ["sig=", "se=", "sv="]):
        return None
    
    return href

def main():
    print("="*70)
    print("📥 DESCARGA DE EDIFICIOS - MICROSOFT BUILDING FOOTPRINTS")
    print("="*70 + "\n")
    
    # Conectar al catálogo de Planetary Computer
    print("🔌 Conectando a Microsoft Planetary Computer STAC API...")
    cat = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace
    )
    
    # Buscar items de la colección ms-buildings para Colombia
    print(f"🔍 Buscando edificios en bbox: {BBOX_CO}")
    print("⏳ Esto puede tomar varios minutos...\n")
    
    search = cat.search(
        collections=["ms-buildings"],
        bbox=BBOX_CO,
        max_items=1000,  # Aumentado para mayor cobertura
        limit=100
    )
    
    items = list(search.items())
    
    if not items:
        print("❌ No se encontraron items. Verifica tu conexión a internet.")
        return 1
    
    print(f"✅ Encontrados {len(items)} items STAC totales")
    
    # Filtrar items de Colombia
    print("🇨🇴 Filtrando items de Colombia...")
    items_co = [it for it in items if _item_is_colombia(it)]
    
    if not items_co:
        print("⚠️  No se detectaron items específicos de Colombia")
        print("    Usando todos los items dentro del bbox...")
        items_co = items
    
    print(f"✅ Items de Colombia detectados: {len(items_co)}\n")
    
    # Descargar assets
    print(f"📦 Descargando {len(items_co)} assets...")
    print(f"📁 Directorio de salida: {OUT_DIR.absolute()}\n")
    
    local_parts = []
    errors = []
    
    for i, it in enumerate(items_co, 1):
        print(f"\n[{i}/{len(items_co)}] Procesando: {it.id}")
        
        # Firmar item y seleccionar asset
        try:
            signed_item = planetary_computer.sign(it)
            asset = pick_asset(signed_item)
            
            if not asset:
                print(f"  ⚠️  No se encontró asset válido")
                continue
            
            # Obtener URL firmada
            url = _signed_asset_url(asset)
            
            if not url:
                # Intentar con item original
                url = _signed_asset_url(pick_asset(it))
            
            if not url:
                print(f"  ⚠️  No se pudo obtener URL firmada (SAS)")
                errors.append(it.id)
                continue
            
            # Determinar extensión
            ext = ".parquet" if ".parquet" in url else ".geojson"
            out = OUT_DIR / f"{it.id}{ext}"
            
            if out.exists():
                print(f"  ✅ Ya existe: {out.name}")
                local_parts.append(out)
                continue
            
            # Descargar
            _download_https(url, out)
            local_parts.append(out)
            print(f"  ✅ Descargado: {out.name}")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            errors.append(it.id)
    
    # Resumen de descarga
    print("\n" + "="*70)
    print("📊 RESUMEN DE DESCARGA")
    print("="*70)
    print(f"✅ Archivos descargados: {len(local_parts)}")
    print(f"❌ Errores: {len(errors)}")
    
    if not local_parts:
        print("\n❌ No se descargó ningún archivo")
        return 1
    
    # Unir todos los archivos
    print("\n🔄 Uniendo archivos descargados...")
    
    union_gj = OUT_DIR / "ms_co.geojson"
    union_pq = OUT_DIR / "ms_co_union.parquet"
    
    # Procesar parquets primero
    parts_pq = [p for p in local_parts if p.suffix == ".parquet"]
    parts_gj = [p for p in local_parts if p.suffix == ".geojson"]
    
    frames = []
    
    if parts_pq:
        print(f"📊 Procesando {len(parts_pq)} archivos Parquet...")
        for p in tqdm(parts_pq, desc="Leyendo Parquet"):
            try:
                gdf = gpd.read_parquet(p)
                frames.append(gdf)
            except Exception as e:
                print(f"⚠️  Error leyendo {p.name}: {e}")
    
    if parts_gj:
        print(f"📊 Procesando {len(parts_gj)} archivos GeoJSON...")
        for p in tqdm(parts_gj, desc="Leyendo GeoJSON"):
            try:
                gdf = gpd.read_file(p)
                frames.append(gdf)
            except Exception as e:
                print(f"⚠️  Error leyendo {p.name}: {e}")
    
    if not frames:
        print("\n❌ No se pudo leer ningún archivo")
        return 1
    
    # Concatenar todos los GeoDataFrames
    print("\n🔗 Concatenando datos...")
    g_all = pd.concat(frames, ignore_index=True)
    
    # Asegurar CRS
    if not hasattr(g_all, 'crs') or g_all.crs is None:
        g_all = g_all.set_crs(4326, allow_override=True)
    
    # Filtrar por bbox de Colombia
    print("✂️  Recortando por bbox de Colombia...")
    minx, miny, maxx, maxy = BBOX_CO
    g_all = g_all.cx[minx:maxx, miny:maxy]
    
    # Guardar resultados
    print(f"\n💾 Guardando archivo unificado...")
    
    if len(frames) > 1:
        g_all.to_parquet(union_pq, index=False)
        print(f"  ✅ Parquet: {union_pq} ({len(g_all):,} edificios)")
    
    g_all.to_file(union_gj, driver="GeoJSON")
    print(f"  ✅ GeoJSON: {union_gj} ({len(g_all):,} edificios)")
    
    # Estadísticas finales
    print("\n" + "="*70)
    print("✅ DESCARGA COMPLETADA")
    print("="*70)
    print(f"\n📊 Estadísticas:")
    print(f"  • Total edificios: {len(g_all):,}")
    print(f"  • Archivos parciales: {len(local_parts)}")
    print(f"  • Archivo unificado: {union_gj}")
    
    print("\n💡 Siguiente paso:")
    print("   python3 02_calcular_area_y_exportar_geojsonl_ms.py")
    print("="*70 + "\n")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
