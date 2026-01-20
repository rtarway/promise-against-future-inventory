import pytest
import sqlite3
import datetime
from src.aipe.database import reset_db, DB_NAME
from src.aipe import promising_agent

# Helper to execute SQL for setup
def run_sql(sql, params=()):
    # Ensure DB path is correct if DB_NAME is relative
    # But since we are running from root, DB_NAME="supply_chain.db" works if CWD is root.
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()
    conn.close()

def get_record(table, key_col, key_val):
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table} WHERE {key_col} = ?", (key_val,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None

def get_locks():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM replenishment_locks")
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

@pytest.fixture(autouse=True)
def setup_teardown():
    reset_db()
    yield
    # No teardown needed, reset happens at start

def test_happy_path_ss_borrow_repay():
    """
    Test Case A: The Happy Path (Borrow & Repay)
    SKU 'A', On Hand 10, SS 10. ASN in 2 days (+50).
    Config: REPLENISH_WINDOW = 5.
    Order 5 units.
    Expect: ALLOCATED, SS_BORROW_WITH_REPLENISH.
    """
    sku = 'A'
    # Inventory
    run_sql("INSERT INTO inventory (sku, on_hand_qty, safety_stock_qty) VALUES (?, ?, ?)", (sku, 10, 10))
    # ASN
    eta = (datetime.date.today() + datetime.timedelta(days=2)).isoformat()
    run_sql("INSERT INTO asns (asn_id, sku, qty, status, eta_datetime) VALUES (?, ?, ?, ?, ?)", 
            ('ASN_A', sku, 50, 'IN_TRANSIT', eta))
    # Config
    run_sql("INSERT INTO business_rules (rule_name, scope, value) VALUES (?, ?, ?)", 
            ('REPLENISH_WINDOW_DAYS', 'GLOBAL', '5'))
    # Order
    order_id = 'ORD_A'
    run_sql("INSERT INTO orders (order_id, sku, qty, due_date, status) VALUES (?, ?, ?, ?, ?)", 
            (order_id, sku, 5, '2025-12-31', 'NEW')) # Due date doesn't matter for SS borrow logic

    # Run Agent
    result = promising_agent.run_agent(order_id, sku, 5, '2025-12-31')
    
    # Verify
    assert result['status'] == 'ALLOCATED'
    assert result['strategy'] == 'SS_BORROW_WITH_REPLENISH'
    
    # Check locks
    locks = get_locks()
    assert len(locks) == 1
    assert locks[0]['qty_locked'] == 5
    assert locks[0]['asn_id'] == 'ASN_A'
    
    # Check inventory decrement
    inv = get_record('inventory', 'sku', sku)
    assert inv['on_hand_qty'] == 5 # 10 - 5

def test_too_far_rejection():
    """
    Test Case B: The "Too Far" Rejection
    SKU 'B', On Hand 10, SS 10. ASN in 20 days.
    Config: REPLENISH_WINDOW = 5. RISKY_DEPLETION = False.
    Order 5 units.
    Expect: Not Allocated (falls to Node 3), probably Backorder since ASN is far?
    Note: Node 3 Direct Inbound might pick it up if due date allows. 
    Let's assume due date is "Soon" or check Node 3 logic.
    Node 3 checks "ASNs (future dates) against Order due_date".
    Let's set Due Date < 20 days to fail Direct Inbound too for this specific test goal ("Order Status != ALLOCATED").
    """
    sku = 'B'
    run_sql("INSERT INTO inventory (sku, on_hand_qty, safety_stock_qty) VALUES (?, ?, ?)", (sku, 10, 10))
    
    eta = (datetime.date.today() + datetime.timedelta(days=20)).isoformat()
    run_sql("INSERT INTO asns (asn_id, sku, qty, status, eta_datetime) VALUES (?, ?, ?, ?, ?)", 
            ('ASN_B', sku, 50, 'IN_TRANSIT', eta))
    
    run_sql("INSERT INTO business_rules (rule_name, scope, value) VALUES (?, ?, ?)", ('REPLENISH_WINDOW_DAYS', 'GLOBAL', '5'))
    run_sql("INSERT INTO business_rules (rule_name, scope, value) VALUES (?, ?, ?)", ('ALLOW_RISKY_DEPLETION', 'GLOBAL', 'False'))
    
    order_id = 'ORD_B'
    # Due date in 10 days (before ASN arrives) -> Should fail Direct Inbound too
    due_date = (datetime.date.today() + datetime.timedelta(days=10)).isoformat()
    run_sql("INSERT INTO orders (order_id, sku, qty, due_date, status) VALUES (?, ?, ?, ?, ?)", 
            (order_id, sku, 5, due_date, 'NEW'))

    result = promising_agent.run_agent(order_id, sku, 5, due_date)
    
    # Expect Backorder
    assert result['status'] != 'ALLOCATED' 
    if result['status'] == 'BACKORDER':
        pass # Good
    
    # Verify No changes
    locks = get_locks()
    assert len(locks) == 0
    inv = get_record('inventory', 'sku', sku)
    assert inv['on_hand_qty'] == 10

def test_risky_override():
    """
    Test Case C: The "Risky" Override
    SKU 'C', On Hand 10, SS 10. ASN in 20 days.
    Config: REPLENISH_WINDOW = 5. RISKY_DEPLETION = True.
    Order 5 units.
    Expect: ALLOCATED, SS_RISKY.
    """
    sku = 'C'
    run_sql("INSERT INTO inventory (sku, on_hand_qty, safety_stock_qty) VALUES (?, ?, ?)", (sku, 10, 10))
    eta = (datetime.date.today() + datetime.timedelta(days=20)).isoformat()
    run_sql("INSERT INTO asns (asn_id, sku, qty, status, eta_datetime) VALUES (?, ?, ?, ?, ?)", ('ASN_C', sku, 50, 'IN_TRANSIT', eta))
    
    run_sql("INSERT INTO business_rules (rule_name, scope, value) VALUES (?, ?, ?)", ('REPLENISH_WINDOW_DAYS', 'GLOBAL', '5'))
    run_sql("INSERT INTO business_rules (rule_name, scope, value) VALUES (?, ?, ?)", ('ALLOW_RISKY_DEPLETION', 'GLOBAL', 'True'))
    
    order_id = 'ORD_C'
    run_sql("INSERT INTO orders (order_id, sku, qty, due_date, status) VALUES (?, ?, ?, ?, ?)", (order_id, sku, 5, '2025-12-31', 'NEW'))

    result = promising_agent.run_agent(order_id, sku, 5, '2025-12-31')
    
    assert result['status'] == 'ALLOCATED'
    assert result['strategy'] == 'SS_RISKY'
    
    locks = get_locks()
    assert len(locks) == 0
    inv = get_record('inventory', 'sku', sku)
    assert inv['on_hand_qty'] == 5

def test_hierarchy_check():
    """
    Test Case D: Hierarchy Check (Item vs Global)
    SKU 'D', On Hand 10, SS 10. ASN in 20 days.
    Config: Global RISKY_DEPLETION = False. Item Rule (SKU 'D') = True.
    Order 5 units.
    Expect: ALLOCATED (Item override).
    """
    sku = 'D'
    run_sql("INSERT INTO inventory (sku, on_hand_qty, safety_stock_qty) VALUES (?, ?, ?)", (sku, 10, 10))
    eta = (datetime.date.today() + datetime.timedelta(days=20)).isoformat()
    run_sql("INSERT INTO asns (asn_id, sku, qty, status, eta_datetime) VALUES (?, ?, ?, ?, ?)", ('ASN_D', sku, 50, 'IN_TRANSIT', eta))
    
    run_sql("INSERT INTO business_rules (rule_name, scope, value) VALUES (?, ?, ?)", ('REPLENISH_WINDOW_DAYS', 'GLOBAL', '5'))
    run_sql("INSERT INTO business_rules (rule_name, scope, value) VALUES (?, ?, ?)", ('ALLOW_RISKY_DEPLETION', 'GLOBAL', 'False'))
    run_sql("INSERT INTO business_rules (rule_name, scope, sku, value) VALUES (?, ?, ?, ?)", ('ALLOW_RISKY_DEPLETION', 'ITEM', sku, 'True'))
    
    order_id = 'ORD_D'
    run_sql("INSERT INTO orders (order_id, sku, qty, due_date, status) VALUES (?, ?, ?, ?, ?)", (order_id, sku, 5, '2025-12-31', 'NEW'))

    result = promising_agent.run_agent(order_id, sku, 5, '2025-12-31')
    
    assert result['status'] == 'ALLOCATED'
    # Should stay SS_RISKY because replenishment lookup failed (too far) but risky allowed
    assert result['strategy'] == 'SS_RISKY'
