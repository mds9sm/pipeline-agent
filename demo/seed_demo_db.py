"""
Create a demo SQLite database with 3 tables and sample data.

Usage:
    python demo/seed_demo_db.py

Creates: demo/demo_source.db
"""

import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "demo_source.db")


def seed():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ── Table 1: customers ────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE customers (
            customer_id   INTEGER PRIMARY KEY,
            name          TEXT NOT NULL,
            email         TEXT NOT NULL,
            plan          TEXT NOT NULL DEFAULT 'free',
            created_at    DATETIME NOT NULL,
            updated_at    DATETIME NOT NULL
        )
    """)
    cur.executemany(
        "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "Alice Chen",    "alice@example.com",    "pro",        "2025-01-15 09:30:00", "2025-06-01 14:00:00"),
            (2, "Bob Martinez",  "bob@example.com",      "enterprise", "2025-02-20 11:00:00", "2025-07-10 08:45:00"),
            (3, "Carol Wu",      "carol@example.com",    "free",       "2025-03-05 16:20:00", "2025-03-05 16:20:00"),
            (4, "Dan Okafor",    "dan@example.com",      "pro",        "2025-04-12 10:15:00", "2025-08-22 12:30:00"),
            (5, "Eve Johnson",   "eve@example.com",      "enterprise", "2025-05-01 08:00:00", "2025-09-15 17:00:00"),
        ],
    )

    # ── Table 2: orders ───────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE orders (
            order_id      INTEGER PRIMARY KEY,
            customer_id   INTEGER NOT NULL REFERENCES customers(customer_id),
            product       TEXT NOT NULL,
            amount_cents  INTEGER NOT NULL,
            status        TEXT NOT NULL DEFAULT 'pending',
            ordered_at    DATETIME NOT NULL,
            updated_at    DATETIME NOT NULL
        )
    """)
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (101, 1, "Pipeline Agent Pro",   4999,  "completed", "2025-06-01 14:05:00", "2025-06-01 14:10:00"),
            (102, 2, "Pipeline Agent Team",  14999, "completed", "2025-07-10 09:00:00", "2025-07-10 09:30:00"),
            (103, 1, "Support Add-on",       999,   "completed", "2025-08-15 11:20:00", "2025-08-15 11:25:00"),
            (104, 4, "Pipeline Agent Pro",   4999,  "pending",   "2025-09-20 15:30:00", "2025-09-20 15:30:00"),
            (105, 5, "Pipeline Agent Team",  14999, "refunded",  "2025-09-25 10:00:00", "2025-10-01 09:00:00"),
            (106, 3, "Pipeline Agent Pro",   4999,  "completed", "2025-10-05 13:45:00", "2025-10-05 13:50:00"),
        ],
    )

    # ── Table 3: events (high-volume style, good incremental candidate) ──
    cur.execute("""
        CREATE TABLE events (
            event_id      INTEGER PRIMARY KEY,
            customer_id   INTEGER NOT NULL REFERENCES customers(customer_id),
            event_type    TEXT NOT NULL,
            properties    TEXT,
            occurred_at   DATETIME NOT NULL
        )
    """)
    cur.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?)",
        [
            (1001, 1, "page_view",     '{"page": "/dashboard"}',        "2025-09-01 10:00:00"),
            (1002, 1, "pipeline_run",  '{"pipeline": "orders", "status": "complete"}', "2025-09-01 10:05:00"),
            (1003, 2, "page_view",     '{"page": "/connectors"}',       "2025-09-01 10:10:00"),
            (1004, 3, "signup",        '{"source": "google"}',          "2025-09-01 10:15:00"),
            (1005, 2, "pipeline_run",  '{"pipeline": "users", "status": "failed"}', "2025-09-01 10:20:00"),
            (1006, 4, "page_view",     '{"page": "/approvals"}',        "2025-09-01 10:25:00"),
            (1007, 1, "connector_gen", '{"type": "snowflake"}',         "2025-09-01 10:30:00"),
            (1008, 5, "page_view",     '{"page": "/freshness"}',        "2025-09-01 10:35:00"),
        ],
    )

    conn.commit()
    conn.close()

    print(f"Demo database created: {DB_PATH}")
    print()
    print("Tables:")
    print("  customers  - 5 rows  (full refresh candidate)")
    print("  orders     - 6 rows  (merge on order_id, incremental on updated_at)")
    print("  events     - 8 rows  (append, incremental on occurred_at)")
    print()
    print("Use in Pipeline Agent with:")
    print(f'  Source connector: sqlite-source-v1')
    print(f'  Connection params: {{"database": "{os.path.abspath(DB_PATH)}"}}')


if __name__ == "__main__":
    seed()
