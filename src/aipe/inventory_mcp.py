from typing import List, Dict, Optional, Any, Union
import datetime
from sqlalchemy import text
from .database import get_db, init_db

# Ensure tables exist (mostly for local run)
init_db()

def get_inventory_position(sku: str) -> Dict[str, int]:
    """Returns on_hand and safety_stock for a SKU."""
    # Use generator
    db_gen = get_db()
    db = next(db_gen)
    try:
        result = db.execute(text("SELECT on_hand_qty, safety_stock_qty FROM inventory WHERE sku = :sku"), {"sku": sku}).fetchone()
        if result:
            return {"on_hand": result.on_hand_qty, "safety_stock": result.safety_stock_qty}
        return {"on_hand": 0, "safety_stock": 0}
    finally:
        db.close()

def get_inbound_asns(sku: str) -> List[Dict[str, Any]]:
    """Returns ASNs minus any qty found in replenishment_locks."""
    db_gen = get_db()
    db = next(db_gen)
    try:
        # Get all ASNs for SKU
        rows = db.execute(text("SELECT asn_id, qty, eta_datetime FROM asns WHERE sku = :sku AND status != 'CLOSED'"), {"sku": sku}).fetchall()
        asns = [dict(row._mapping) for row in rows]
        
        # Calculate available qty for each ASN
        for asn in asns:
            locked_row = db.execute(text("SELECT SUM(qty_locked) as locked FROM replenishment_locks WHERE asn_id = :asn_id"), {"asn_id": asn["asn_id"]}).fetchone()
            locked_qty = locked_row.locked if locked_row and locked_row.locked else 0
            asn["available_qty"] = max(0, asn["qty"] - locked_qty)
        
        # Filter out ASNs with no availability
        return [asn for asn in asns if asn["available_qty"] > 0]
    finally:
        db.close()

def get_rule_config(sku: str, rule_name: str) -> Any:
    today = datetime.date.today().isoformat()
    db_gen = get_db()
    db = next(db_gen)
    try:
        # 1. Item Scope + Inside Date Range
        row = db.execute(text("""
            SELECT value FROM business_rules 
            WHERE rule_name = :rule_name AND scope = 'ITEM' AND sku = :sku 
            AND (start_date <= :today OR start_date IS NULL) 
            AND (end_date >= :today OR end_date IS NULL)
            AND (start_date IS NOT NULL OR end_date IS NOT NULL)
        """), {"rule_name": rule_name, "sku": sku, "today": today}).fetchone()
        
        if row: 
            return _parse_value(row.value)

        # 2. Item Scope (No Dates) / 3. Global
        # Fetch all candidates
        rows = db.execute(text("""
            SELECT scope, sku, start_date, end_date, value 
            FROM business_rules 
            WHERE rule_name = :rule_name AND (sku = :sku OR scope = 'GLOBAL')
        """), {"rule_name": rule_name, "sku": sku}).fetchall()
        
        rules = [dict(r._mapping) for r in rows]
        
    finally:
        db.close()
        
    # Logic (Same as before)
    # P1
    for r in rules:
        if r['scope'] == 'ITEM' and r['sku'] == sku:
            if r['start_date'] and r['end_date']:
                 if r['start_date'] <= today <= r['end_date']:
                     return _parse_value(r['value'])
            elif r['start_date'] and not r['end_date']:
                 if r['start_date'] <= today:
                     return _parse_value(r['value'])
            elif not r['start_date'] and r['end_date']:
                 if today <= r['end_date']:
                     return _parse_value(r['value'])
    
    # P2
    for r in rules:
        if r['scope'] == 'ITEM' and r['sku'] == sku:
            if not r['start_date'] and not r['end_date']:
                return _parse_value(r['value'])

    # P3
    for r in rules:
        if r['scope'] == 'GLOBAL':
            return _parse_value(r['value'])
            
    return None

def _parse_value(val):
    if val.lower() == 'true': return True
    if val.lower() == 'false': return False
    try:
        return int(val)
    except ValueError:
        return val

def execute_allocation(order_id: str, sku: str, strategy: str, qty: int, asn_id: Optional[str] = None):
    """
    Updates DB based on allocation strategy.
    """
    db_gen = get_db()
    db = next(db_gen)
    try:
        if strategy == 'SS_BORROW_WITH_REPLENISH':
            db.execute(text("UPDATE inventory SET on_hand_qty = on_hand_qty - :qty WHERE sku = :sku"), {"qty": qty, "sku": sku})
            if asn_id:
                lock_id = f"lock_{order_id}_{asn_id}"
                db.execute(text("INSERT INTO replenishment_locks (lock_id, sku, asn_id, qty_locked) VALUES (:id, :sku, :asn, :qty)"),
                               {"id": lock_id, "sku": sku, "asn": asn_id, "qty": qty})
            status = 'ALLOCATED'
            
        elif strategy == 'SS_RISKY':
            db.execute(text("UPDATE inventory SET on_hand_qty = on_hand_qty - :qty WHERE sku = :sku"), {"qty": qty, "sku": sku})
            status = 'ALLOCATED'
            
        elif strategy == 'DIRECT_INBOUND':
            status = 'ALLOCATED'
        
        else: # FREE_STOCK
             db.execute(text("UPDATE inventory SET on_hand_qty = on_hand_qty - :qty WHERE sku = :sku"), {"qty": qty, "sku": sku})
             status = 'ALLOCATED'

        db.execute(text("""
            UPDATE orders 
            SET status = :status, fulfillment_source = :strat 
            WHERE order_id = :oid
        """), {"status": status, "strat": strategy, "oid": order_id})
        
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()
    
    return {"status": "success", "strategy": strategy}

