from fastapi import FastAPI

from routes import api_router
import os
from databricks import sql
from databricks.sdk.core import Config
from fastapi import FastAPI, Query
from typing import Dict, List

DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID") or None

app = FastAPI(
    title="FastAPI & Databricks Apps",
    description="A simple FastAPI application example for Databricks Apps runtime",
    version="1.0.0",
)

databricks_cfg = Config()

def get_connection(warehouse_id: str):
    http_path = f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}"
    return sql.connect(
        server_hostname=databricks_cfg.host,
        http_path=http_path,
        credentials_provider=lambda: databricks_cfg.authenticate,
    )


def query(sql_query: str, warehouse_id: str, as_dict: bool = True) -> List[Dict]:
    conn = get_connection(warehouse_id)
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in result]

    except Exception as e:
        raise Exception(f"DBSQL Query Failed: {str(e)}")


@app.get("/api/v1/table")
def table(
    sql_query: str = Query("SELECT * FROM data_vertc_stg.lastros.silver_preco_aquisicao_contrato_calculado limit 10", description="SQL query to execute"),
):
    results = None
    try:
        results = query(sql_query, warehouse_id=DATABRICKS_WAREHOUSE_ID)
    except Exception as e:
        raise Exception(f"FastAPI Request Failed: {str(e)}")

    return {"results": results}


# Router assignment
app.include_router(api_router)