# scripts/create_collections.py
# Este script replica la intención de
# schemas e índices (código DANE único, 2dsphere,
# python .\scripts\create_collections.pycompuestos por municipio/área, y confidence en Google)
from pymongo import MongoClient, ASCENDING, DESCENDING, GEOSPHERE
from pymongo.errors import CollectionInvalid

DB = "pdet_solar_analysis"
cli = MongoClient(); db = cli[DB]

def ensure(name, validator, indexes):
    try:
        db.create_collection(
            name,
            validator=validator,
            validationLevel="strict",
            validationAction="error"
        )
        print(f"✓ Created: {name}")
    except CollectionInvalid:
        print(f"! Exists: {name}")

    try:
        db.command({
            "collMod": name,
            "validator": validator,
            "validationLevel": "strict",
            "validationAction": "error"
        })
        print(f"  ✓ Updated validator for: {name}")
    except Exception as e:
        print(f"  ! Skipped validator update for {name}: {e}")

    col = db[name]
    for idx in indexes:
        col.create_index(
            idx["spec"],
            name=idx["name"],
            unique=idx.get("unique", False)
        )
    print(f"✓ Indexes ensured for: {name}")


municipalities_validator = {
  "$jsonSchema": {
    "bsonType":"object",
    "required":["codigo_dane","nombre_municipio","nombre_departamento","is_pdet","geometry","metadata"],
    "properties":{
      "codigo_dane":{"bsonType":"string","pattern":"^[0-9]{5}$"},
      "nombre_municipio":{"bsonType":"string"},
      "nombre_departamento":{"bsonType":"string"},
      "is_pdet":{"bsonType":"bool"},
      "geometry":{"bsonType":"object","required":["type","coordinates"],"properties":{"type":{"enum":["Polygon","MultiPolygon"]}}},
      "metadata":{"bsonType":"object","required":["source","load_date"]}
    }
  }
}

build_ms_validator = {
  "$jsonSchema":{
    "bsonType":"object",
    "required":["municipality_code","geometry","properties","metadata"],
    "properties":{
      "municipality_code":{"bsonType":"string","pattern":"^[0-9]{5}$"},
      "geometry":{"bsonType":"object","required":["type","coordinates"],"properties":{"type":{"enum":["Polygon"]}}},
      "properties":{"bsonType":"object","required":["area_m2"],"properties":{"area_m2":{"bsonType":"double"}}},
      "metadata":{"bsonType":"object","required":["source","load_date"],"properties":{"source":{"enum":["Microsoft"]}}}
    }
  }
}

build_gg_validator = {
  "$jsonSchema":{
    "bsonType":"object",
    "required":["municipality_code","geometry","properties","metadata"],
    "properties":{
      "municipality_code":{"bsonType":"string","pattern":"^[0-9]{5}$"},
      "geometry":{"bsonType":"object","required":["type","coordinates"],"properties":{"type":{"enum":["Polygon"]}}},
      "properties":{"bsonType":"object","required":["area_m2","confidence"],"properties":{"area_m2":{"bsonType":"double"},"confidence":{"bsonType":"double"}}},
      "metadata":{"bsonType":"object","required":["source","load_date","dataset_version"],"properties":{"source":{"enum":["Google"]}}}
    }
  }
}

ensure("municipalities", municipalities_validator, [
  {"name":"codigo_dane_unique", "spec":[("codigo_dane", ASCENDING)], "unique":True},
  {"name":"geometry_2dsphere", "spec":[("geometry", GEOSPHERE)]},
  {"name":"is_pdet_index", "spec":[("is_pdet", ASCENDING)]},
  {"name":"nombre_municipio_index", "spec":[("nombre_municipio", ASCENDING)]},
])

ensure("buildings_microsoft", build_ms_validator, [
  {"name":"geometry_2dsphere", "spec":[("geometry", GEOSPHERE)]},
  {"name":"municipality_code_index", "spec":[("municipality_code", ASCENDING)]},
  {"name":"municipality_area_compound", "spec":[("municipality_code", ASCENDING), ("properties.area_m2", DESCENDING)]},
  {"name":"area_index", "spec":[("properties.area_m2", DESCENDING)]},
])

ensure("buildings_google", build_gg_validator, [
  {"name":"geometry_2dsphere", "spec":[("geometry", GEOSPHERE)]},
  {"name":"municipality_code_index", "spec":[("municipality_code", ASCENDING)]},
  {"name":"municipality_area_compound", "spec":[("municipality_code", ASCENDING), ("properties.area_m2", DESCENDING)]},
  {"name":"confidence_index", "spec":[("properties.confidence", DESCENDING)]},
  {"name":"area_index", "spec":[("properties.area_m2", DESCENDING)]},
])

print("OK")
