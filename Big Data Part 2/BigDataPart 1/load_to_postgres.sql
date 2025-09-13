# load_to_postgres.py
import os, psycopg2
from psycopg2 import sql
from pathlib import Path

HOST = os.getenv("PGHOST", "localhost")
PORT = int(os.getenv("PGPORT", "5432"))
USER = os.getenv("PGUSER", "postgres")
PASSWORD = os.getenv("PGPASSWORD", "postgres")
DBNAME = os.getenv("PGDATABASE", "retail")
DATA_DIR = Path("data")

DDL = {
    "customers": """
        CREATE TABLE IF NOT EXISTS customers (
          customer_id INT PRIMARY KEY,
          first_name  TEXT,
          last_name   TEXT,
          email       TEXT,
          signup_date DATE
        );
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products (
          product_id INT PRIMARY KEY,
          name       TEXT,
          price      NUMERIC,
          category   TEXT
        );
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
          order_id    INT PRIMARY KEY,
          customer_id INT,
          product_id  INT,
          amount      NUMERIC,
          order_date  DATE
        );
    """
}

def connect(db=DBNAME):
    return psycopg2.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD, dbname=db
    )

def ensure_db():
    with connect("postgres") as c, c.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DBNAME,))
        if not cur.fetchone():
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DBNAME)))
            print(f"Created database {DBNAME}")

def ensure_tables():
    with connect() as c, c.cursor() as cur:
        for ddl in DDL.values():
            cur.execute(ddl)
        print("Tables ensured.")

def copy_csv(table, path):
    with connect() as c, c.cursor() as cur, open(path, "r", encoding="utf-8") as f:
        cur.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(table)))
        cur.copy_expert(
            sql.SQL("COPY {} FROM STDIN WITH (FORMAT csv, HEADER true)").format(sql.Identifier(table)).as_string(cur),
            f
        )
        print(f"Loaded {table}: {path}")

def main():
    assert DATA_DIR.exists(), f"Missing data folder: {DATA_DIR}"
    ensure_db()
    ensure_tables()
    copy_csv("customers", DATA_DIR/"customers.csv")
    copy_csv("products",  DATA_DIR/"products.csv")
    copy_csv("orders",    DATA_DIR/"orders.csv")
    print("✅ Load complete.")

if __name__ == "__main__":
    main()
