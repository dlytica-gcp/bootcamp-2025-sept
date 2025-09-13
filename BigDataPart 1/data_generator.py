import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Create output folder
data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

# ----- 1. Customers -----
customers_file = data_dir / "customers.csv"
customer_rows = []
for cid in range(1, 51):  # 50 customers
    customer_rows.append({
        "customer_id": cid,
        "first_name": f"Customer{cid}",
        "last_name": f"Last{cid}",
        "email": f"customer{cid}@example.com",
        "signup_date": (datetime.today() - timedelta(days=random.randint(30, 365))).date()
    })

with customers_file.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=customer_rows[0].keys())
    writer.writeheader()
    writer.writerows(customer_rows)

# ----- 2. Products -----
products_file = data_dir / "products.csv"
product_names = ["Laptop", "Phone", "Tablet", "Headphones", "Camera"]
product_rows = []
for pid in range(1, 11):  # 10 products
    product_rows.append({
        "product_id": pid,
        "name": random.choice(product_names),
        "price": round(random.uniform(100, 2000), 2),
        "category": random.choice(["Electronics", "Accessories", "Home"])
    })

with products_file.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=product_rows[0].keys())
    writer.writeheader()
    writer.writerows(product_rows)

# ----- 3. Orders -----
orders_file = data_dir / "orders.csv"
order_rows = []
for oid in range(1, 301):  # 300 orders
    order_rows.append({
        "order_id": oid,
        "customer_id": random.randint(1, 50),
        "product_id": random.randint(1, 10),
        "amount": round(random.uniform(50, 5000), 2),
        "order_date": (datetime.today() - timedelta(days=random.randint(1, 180))).date()
    })

with orders_file.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=order_rows[0].keys())
    writer.writeheader()
    writer.writerows(order_rows)

print("✅ Dataset generated in the 'data/' folder")
