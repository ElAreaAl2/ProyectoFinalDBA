from pymongo import MongoClient
cli = MongoClient()
db = cli["pdet_solar_analysis"]

print("DB:", db.name)

# Conteo municipios
print("municipalities:", db.municipalities.count_documents({}))

# Índices por colección (nombres)
for name in ["municipalities", "buildings_microsoft", "buildings_google"]:
    col = db[name]
    try:
        idx = [i["name"] for i in col.list_indexes()]
        print(f"{name} indexes:", idx)
    except Exception as e:
        print(f"{name} indexes error:", e)
