from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)
    client.admin.command("ping")  # σύντομος έλεγχος
except ServerSelectionTimeoutError as e:
    raise SystemExit(f"❌ Δεν βρήκα MongoDB στο {MONGO_URI}:\n{e}")

db = client["store_db"]
customers = db["customers"]
orders = db["orders"]


# ---- 2) Reset για reproducible run ----
customers.drop()
orders.drop()

# ---- 3) Εισαγωγή εγγράφων ----
customers.insert_many([
    {"_id": 1, "name": "Alice", "email": "alice@email.com"},
    {"_id": 2, "name": "Bob",   "email": "bob@email.com"},
])

orders.insert_many([
    {"_id": 1, "customer_id": 1,
     "items": [{"product": "Laptop", "qty": 1, "price": 999.0}]},
    {"_id": 2, "customer_id": 2,
     "items": [{"product": "Mouse", "qty": 2, "price": 25.5}]}
])

# ---- 4) Βασικά queries ----
print("\nFind one customer (Alice):")
print(customers.find_one({"name": "Alice"}))

print("\nOrders for Bob (customer_id=2):")
for o in orders.find({"customer_id": 2}):
    print(o)

# ---- 5) Aggregation: συνολικά έσοδα ανά πελάτη ----
print("\nAggregate total revenue per customer:")
pipeline = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$customer_id",
        "total_spent": {"$sum": {"$multiply": ["$items.qty", "$items.price"]}}
    }},
    {"$sort": {"_id": 1}}
]
for res in orders.aggregate(pipeline):
    print(res)
