from pathlib import Path
from datetime import datetime
import json
from pymongo import MongoClient

SRC = Path("data/municipalities/MGN2024_MUNICIPIOS_PDET.geojson")

def norm_code(x):
    if x is None: return None
    s = str(x).strip()
    return s.zfill(5) if s.isdigit() else s

def main():
    cli = MongoClient(); db = cli["pdet_solar_analysis"]
    col = db["municipalities"]

    fc = json.load(open(SRC, encoding="utf-8"))
    docs = []
    for f in fc["features"]:
        p = f.get("properties", {})
        doc = {
            "codigo_dane": norm_code(p.get("codigo_dane") or p.get("cod_dane_completo") or p.get("COD_DANE")),
            "nombre_municipio": p.get("NOMBRE_MPIO") or p.get("Municipio") or p.get("municipio"),
            "nombre_departamento": p.get("NOMBRE_DPT") or p.get("Departamento") or p.get("departamento"),
            "is_pdet": True,
            "geometry": f["geometry"],
            "metadata": {"source":"DANE-MGN", "load_date": datetime.utcnow()}
        }
        if doc["codigo_dane"] and doc["geometry"]:
            docs.append(doc)

    if docs:
        col.insert_many(docs, ordered=False)
        print(f"Insertados: {len(docs)}")
    else:
        print("No se generaron documentos. Verifica los nombres de campos del GeoJSON.")

if __name__ == "__main__":
    main()
