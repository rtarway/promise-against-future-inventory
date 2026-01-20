from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from src.aipe.promising_agent import run_agent
from src.aipe.database import init_db

app = FastAPI(title="AIPE Endpoint")

@app.on_event("startup")
def on_startup():
    init_db()

class OrderRequest(BaseModel):
    order_id: str
    sku: str
    qty: int
    due_date: str

@app.post("/allocate")
async def allocate_order(request: OrderRequest):
    try:
        result = run_agent(request.order_id, request.sku, request.qty, request.due_date)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok"}
