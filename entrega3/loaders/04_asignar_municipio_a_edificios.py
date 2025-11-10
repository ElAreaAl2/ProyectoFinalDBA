from pymongo import MongoClient, UpdateOne
from shapely.geometry import shape

cli = MongoClient(); db = cli["pdet_solar_analysis"]

munis = [(m["codigo_dane"], shape(m["geometry"]))
         for m in db.municipalities.find({"is_pdet": True}, {"_id":0,"codigo_dane":1,"geometry":1})]

def tag(coll_name, conf_min=None, batch=1000):
    col = db[coll_name]
    q = {"municipality_code":{"$exists":False}}
    if conf_min is not None:
        q["properties.confidence"] = {"$gte": conf_min}
    cur = col.find(q, {"_id":1,"geometry":1}, no_cursor_timeout=True)
    ops=[];
    for d in cur:
        c = shape(d["geometry"]).centroid
        code=None
        for dane, poly in munis:
            if poly.contains(c):
                code = dane; break
        if code:
            ops.append(UpdateOne({"_id": d["_id"]}, {"$set": {"municipality_code": code}}))
        if len(ops)==batch:
            col.bulk_write(ops, ordered=False); ops.clear()
    if ops: col.bulk_write(ops, ordered=False)

if __name__ == "__main__":
    tag("buildings_microsoft")
    tag("buildings_google", conf_min=0.5)
