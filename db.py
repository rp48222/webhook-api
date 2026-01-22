import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "app.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Destinations per user
    cur.execute("""
    CREATE TABLE IF NOT EXISTS destinations (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        url TEXT NOT NULL,
        active INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    # Deliveries (one per event forwarding attempt sequence)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS deliveries (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        source TEXT NOT NULL,
        destination_url TEXT NOT NULL,
        event_type TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        status TEXT NOT NULL,          -- pending, delivered, failed
        attempts INTEGER NOT NULL,
        last_error TEXT
    )
    """)

    conn.commit()
    conn.close()

def create_delivery(delivery: dict):
    """
    delivery keys expected:
    id, user_id, source, destination_url, event_type, occurred_at,
    status, attempts, last_error
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO deliveries (
            id, user_id, source, destination_url, event_type, occurred_at,
            status, attempts, last_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        delivery["id"],
        delivery["user_id"],
        delivery["source"],
        delivery["destination_url"],
        delivery["event_type"],
        delivery["occurred_at"],
        delivery["status"],
        delivery["attempts"],
        delivery.get("last_error")
    ))

    conn.commit()
    conn.close()

def get_delivery(delivery_id: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM deliveries WHERE id = ?", (delivery_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return dict(row)

def update_delivery(delivery_id: str, status: str, attempts: int, last_error: str | None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE deliveries
        SET status = ?, attempts = ?, last_error = ?
        WHERE id = ?
    """, (status, attempts, last_error, delivery_id))

    conn.commit()
    conn.close()

def list_deliveries_for_user(user_id: str, limit: int = 20):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM deliveries
        WHERE user_id = ?
        ORDER BY occurred_at DESC
        LIMIT ?
    """, (user_id, limit))

    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def create_destination_db(destination: dict):
    """
    destination keys expected:
    id, user_id, url, active, created_at
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO destinations (id, user_id, url, active, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        destination["id"],
        destination["user_id"],
        destination["url"],
        1 if destination["active"] else 0,
        destination["created_at"]
    ))

    conn.commit()
    conn.close()

def list_destinations_db(user_id: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM destinations
        WHERE user_id = ?
        ORDER BY created_at ASC
    """, (user_id,))

    rows = cur.fetchall()
    conn.close()

    # Convert SQLite int active -> bool
    results = []
    for r in rows:
        d = dict(r)
        d["active"] = bool(d["active"])
        results.append(d)

    return results

def get_active_destination_url(user_id: str):
    destinations = list_destinations_db(user_id)
    active = [d for d in destinations if d["active"]]
    if not active:
        return None
    return active[-1]["url"]
