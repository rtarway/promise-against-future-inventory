import os
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table, text
from sqlalchemy.orm import sessionmaker, declarative_base

# Configuration
DB_URL = os.getenv("DATABASE_URL", "sqlite:///supply_chain.db")

engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define schema using SQLAlchemy Core/ORM
# We can use reflection or define models. For simplicity and consistency with existing code, 
# let's define the tables explicitly or use raw SQL via engine if we want to minimize refactor, 
# BUT best practice is ORM or Core Tables.
# Given existing code uses raw SQL strings, let's keep using text() with binding for minimal logic change, 
# or fully migrate to ORM.
# Full ORM migration is cleaner.

class Inventory(Base):
    __tablename__ = "inventory"
    sku = Column(String, primary_key=True)
    location_id = Column(String)
    on_hand_qty = Column(Integer, default=0)
    safety_stock_qty = Column(Integer, default=0)

class ASN(Base):
    __tablename__ = "asns"
    asn_id = Column(String, primary_key=True)
    sku = Column(String)
    qty = Column(Integer)
    status = Column(String)
    eta_datetime = Column(String)

class Order(Base):
    __tablename__ = "orders"
    order_id = Column(String, primary_key=True)
    sku = Column(String)
    qty = Column(Integer)
    due_date = Column(String)
    status = Column(String)
    fulfillment_source = Column(String)

class ReplenishmentLock(Base):
    __tablename__ = "replenishment_locks"
    lock_id = Column(String, primary_key=True)
    sku = Column(String)
    asn_id = Column(String)
    qty_locked = Column(Integer)

class BusinessRule(Base):
    __tablename__ = "business_rules"
    rule_name = Column(String, primary_key=True)
    scope = Column(String, primary_key=True) # Composite key part
    sku = Column(String, primary_key=True, nullable=True) # Composite key part, handled carefully
    # Use synthetic ID or carefully map composite PK
    start_date = Column(String)
    end_date = Column(String)
    value = Column(String)


def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def reset_db_func():
    # Only for SQLite or specific dev environments
    engine.dispose() # Close connections before file deletion
    if "sqlite" in DB_URL:
       if os.path.exists("supply_chain.db"):
            os.remove("supply_chain.db")
    
    # For Postgres, DROP ALL is risky, so maybe just create_all
    # But for tests we want reset. 
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

# Backward compatibility alias
reset_db = reset_db_func
DB_NAME = "supply_chain.db" # Legacy support constant if needed
