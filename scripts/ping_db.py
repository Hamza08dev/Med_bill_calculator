# scripts/ping_db.py
from neo4j import GraphDatabase
import os

uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
user = os.getenv("NEO4J_USER", "test")
pwd  = os.getenv("NEO4J_PASSWORD", "12345678")

driver = GraphDatabase.driver(uri, auth=(user, pwd))
with driver.session() as s:
    rec = s.run("RETURN 1 AS ok").single()
    print("Connected:", rec["ok"] == 1)
driver.close()
