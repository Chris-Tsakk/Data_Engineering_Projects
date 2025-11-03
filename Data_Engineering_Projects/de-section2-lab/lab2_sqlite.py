import sqlite3
from pathlib import Path

DB_PATH = Path("store.db")

def main():
    # Σύνδεση σε file-based SQLite DB
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cur = conn.cursor()

    # Δημιουργία πινάκων
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers(
        customer_id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT UNIQUE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS products(
        product_id INTEGER PRIMARY KEY,
        name TEXT,
        price REAL
    );
    """)

    # Απλό orders (όπως στο lab): κάθε γραμμή = ένα προϊόν σε μια παραγγελία
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders(
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        product_id INTEGER,
        qty INTEGER,
        order_date TEXT,
        FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY(product_id) REFERENCES products(product_id)
    );
    """)

    # Δεδομένα δείγματος
    cur.executemany("INSERT OR IGNORE INTO customers VALUES (?, ?, ?)", [
        (1, "Alice", "alice@email.com"),
        (2, "Bob",   "bob@email.com"),
    ])

    cur.executemany("INSERT OR IGNORE INTO products VALUES (?, ?, ?)", [
        (1, "Laptop",   999.0),
        (2, "Mouse",     25.5),
        (3, "Keyboard",  49.9),
    ])

    cur.executemany("INSERT OR IGNORE INTO orders VALUES (?, ?, ?, ?, ?)", [
        (1, 1, 1, 1, "2024-01-01"),
        (2, 2, 2, 2, "2024-01-02"),
    ])

    conn.commit()

    # Ερωτήματα
    print("\nAll customers:")
    for row in cur.execute("SELECT * FROM customers"):
        print(row)

    print("\nJoin customers + orders + products:")
    for row in cur.execute("""
        SELECT c.name AS customer, p.name AS product, o.qty,
               (o.qty * p.price) AS total
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN products  p ON o.product_id = p.product_id
        ORDER BY o.order_id;
    """):
        print(row)

    conn.close()

if __name__ == "__main__":
    main()
