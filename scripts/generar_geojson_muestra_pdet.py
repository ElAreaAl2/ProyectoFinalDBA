from datetime import datetime
import json, os, pathlib

out = pathlib.Path("data/municipalities/MGN2024_MUNICIPIOS_PDET.geojson")
out.parent.mkdir(parents=True, exist_ok=True)

# Dos cuadraditos como municipios ficticios
def square(lon, lat, size=0.02):
    return {
        "type":"Polygon",
        "coordinates":[[
            [lon, lat], [lon+size, lat], [lon+size, lat+size],
            [lon, lat+size], [lon, lat]
        ]]
    }

fc = {
  "type":"FeatureCollection",
  "features":[
    {
      "type":"Feature",
      "geometry": square(-75.6, 6.24),
      "properties":{
        "codigo_dane":"05001",
        "NOMBRE_MPIO":"Municipio_A",
        "NOMBRE_DPT":"Depto_A"
      }
    },
    {
      "type":"Feature",
      "geometry": square(-75.58, 6.26),
      "properties":{
        "codigo_dane":"05002",
        "NOMBRE_MPIO":"Municipio_B",
        "NOMBRE_DPT":"Depto_B"
      }
    }
  ]
}

with open(out, "w", encoding="utf-8") as f:
    json.dump(fc, f, ensure_ascii=False)

print("✔ Escribí", out)
