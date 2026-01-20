import operator
from typing import Annotated, TypedDict, Union, Dict, Any, List
from langgraph.graph import StateGraph, END
import datetime

# Import local tools
from . import inventory_mcp

class AgentState(TypedDict):
    order_id: str
    sku: str
    qty: int
    due_date: str
    status: str
    strategy: str
    logs: Annotated[List[str], operator.add]

def check_free_stock(state: AgentState):
    sku = state['sku']
    order_qty = state['qty']
    
    inv_pos = inventory_mcp.get_inventory_position(sku)
    on_hand = inv_pos['on_hand']
    safety_stock = inv_pos['safety_stock']
    
    available = on_hand - safety_stock
    state['logs'].append(f"Check Free Stock: OnHand={on_hand}, SS={safety_stock}, Avail={available}")
    
    if available >= order_qty:
        inventory_mcp.execute_allocation(state['order_id'], sku, 'FREE_STOCK', order_qty)
        return {
            "status": "ALLOCATED",
            "strategy": "FREE_STOCK",
            "logs": [f"Allocated from FREE_STOCK. New Available: {available - order_qty}"]
        }
    
    # Pass to next node with context
    return {
        "status": "CHECK_SS", 
        "logs": ["Insufficient Free Stock. Proceeding to Safety Stock Check."]
    }

def evaluate_safety_stock(state: AgentState):
    sku = state['sku']
    order_qty = state['qty']
    
    inv_pos = inventory_mcp.get_inventory_position(sku)
    on_hand = inv_pos['on_hand']
    safety_stock = inv_pos['safety_stock']
    
    # Logic: Deficit = Order_Qty - Available => Wait, if we are here, Available < Order_Qty.
    # Actually we just check if OnHand (physical) covers the order.
    # If OnHand > OrderQty, we can *physically* fulfill it, but we are dipping into SS.
    
    if on_hand < order_qty:
        return {
            "status": "CHECK_DIRECT",
            "logs": [f"Physical OnHand ({on_hand}) < OrderQty ({order_qty}). Cannot borrow SS. Proceeding to Direct Inbound."]
        }

    # SS Borrowing Logic
    # Sub-Step A: Qualifying Replenishment
    window_days = inventory_mcp.get_rule_config(sku, 'REPLENISH_WINDOW_DAYS')
    if window_days is None: window_days = 7 # Default
    
    target_date = datetime.date.today() + datetime.timedelta(days=int(window_days))
    
    asns = inventory_mcp.get_inbound_asns(sku)
    qualifying_asn = None
    
    for asn in asns:
        eta = datetime.date.fromisoformat(asn['eta_datetime'].split('T')[0])
        if eta <= target_date: # Assuming ASN matches quantity needs? Simplified: just need *an* ASN? 
             # Prompt: "Is there an ASN arriving within...?" 
             # Usually we need enough qty. Let's assume we need to lock the Borrowed Amount against the ASN.
             if asn['available_qty'] >= order_qty:
                 qualifying_asn = asn
                 break
    
    if qualifying_asn:
        inventory_mcp.execute_allocation(state['order_id'], sku, 'SS_BORROW_WITH_REPLENISH', order_qty, asn_id=qualifying_asn['asn_id'])
        return {
            "status": "ALLOCATED",
            "strategy": "SS_BORROW_WITH_REPLENISH",
            "logs": [f"SS Borrow Approved. Locked against ASN {qualifying_asn['asn_id']}"]
        }

    # Sub-Step B: Risky Depletion
    allow_risky = inventory_mcp.get_rule_config(sku, 'ALLOW_RISKY_DEPLETION')
    state['logs'].append(f"No qualifying ASN. Risky Depletion allowed? {allow_risky}")
    
    if allow_risky:
        inventory_mcp.execute_allocation(state['order_id'], sku, 'SS_RISKY', order_qty)
        return {
            "status": "ALLOCATED",
            "strategy": "SS_RISKY",
            "logs": ["SS Risky Borrow Approved."]
        }
        
    return {
        "status": "CHECK_DIRECT",
        "logs": ["SS Borrow denied. Proceeding to Direct Inbound."]
    }

def direct_inbound_promising(state: AgentState):
    sku = state['sku']
    order_qty = state['qty']
    due_date = state['due_date']
    
    if due_date:
       due_date_obj = datetime.date.fromisoformat(due_date)
    else:
       # If no due date, maybe we can't promise? Or we maximize?
       # Let's assume we look for the earliest.
       due_date_obj = datetime.date.max

    asns = inventory_mcp.get_inbound_asns(sku)
    # Sort by ETA
    asns.sort(key=lambda x: x['eta_datetime'])
    
    found_asn = None
    for asn in asns:
         eta = datetime.date.fromisoformat(asn['eta_datetime'].split('T')[0])
         if eta <= due_date_obj and asn['available_qty'] >= order_qty:
             found_asn = asn
             break
             
    if found_asn:
        inventory_mcp.execute_allocation(state['order_id'], sku, 'DIRECT_INBOUND', order_qty, asn_id=found_asn['asn_id'])
        return {
            "status": "ALLOCATED",
            "strategy": "DIRECT_INBOUND",
            "logs": [f"Allocated to Future ASN {found_asn['asn_id']} arriving {found_asn['eta_datetime']}"]
        }
        
    return {
        "status": "BACKORDER",
        "strategy": "NONE",
        "logs": ["No suitable inventory source found. Backordered."]
    }

# Build Graph
builder = StateGraph(AgentState)

builder.add_node("check_free_stock", check_free_stock)
builder.add_node("evaluate_safety_stock", evaluate_safety_stock)
builder.add_node("direct_inbound_promising", direct_inbound_promising)

builder.set_entry_point("check_free_stock")

def route_failed_free_stock(state: AgentState):
    if state['status'] == 'ALLOCATED':
        return END
    return "evaluate_safety_stock"

def route_failed_ss(state: AgentState):
    if state['status'] == 'ALLOCATED':
        return END
    return "direct_inbound_promising"
    
builder.add_conditional_edges("check_free_stock", route_failed_free_stock)
builder.add_conditional_edges("evaluate_safety_stock", route_failed_ss)
builder.add_edge("direct_inbound_promising", END)

graph = builder.compile()

def run_agent(order_id, sku, qty, due_date):
    initial_state = {
        "order_id": order_id,
        "sku": sku,
        "qty": qty,
        "due_date": due_date,
        "status": "NEW",
        "strategy": "NONE",
        "logs": []
    }
    
    result = graph.invoke(initial_state)
    return result
