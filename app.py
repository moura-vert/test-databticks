from fastapi import FastAPI

from routes import api_router
import os
from databricks import sql
from databricks.sdk.core import Config
from fastapi import FastAPI, Query
from typing import Dict, List

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse
from jose import jwt
from authlib.integrations.starlette_client import OAuth
import os

from fastapi.responses import HTMLResponse
from fastapi.exception_handlers import RequestValidationError
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

app = FastAPI(
    title="FastAPI & Databricks Apps",
    description="A simple FastAPI application example for Databricks Apps runtime",
    version="1.0.0",
)


# Configurações
SECRET_KEY = "24&j7yo7)tm=l2v(&4b5349$*8y6elu8^7c(v0tb3a7seg^%5e"
FRONTEND_AUTH_REDIRECT = "http://localhost:8081/auth"
CAS_SERVER = "https://sso.stg.vert-tech.dev/cas"

DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID") or None

oauth = OAuth()
oauth.register(
    name='cas',
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    server_metadata_url=f"{CAS_SERVER}/.well-known/openid-configuration",
    client_kwargs={'scope': 'openid profile email'},
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


@app.get("/login")
async def login(request: Request):
    redirect_uri = request.url_for('auth')
    return await oauth.cas.authorize_redirect(request, redirect_uri)

@app.get("/auth")
async def auth(request: Request):
    token = await oauth.cas.authorize_access_token(request)
    userinfo = await oauth.cas.parse_id_token(request, token)
    jwt_token = jwt.encode({"sub": userinfo["sub"]}, SECRET_KEY, algorithm="HS256")
    refresh_token = "refresh_token_exemplo"
    # Redireciona para o dashboard
    return RedirectResponse("/dashboard")

@app.get("/protected")
async def protected(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return {"user": payload["sub"]}
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": str(exc), "type": type(exc).__name__},
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": exc.body},
    )

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return """
    <html>
        <head><title>Dashboard</title></head>
        <body>
            <h1>Bem-vindo ao Dashboard!</h1>
            <p>Login realizado com sucesso.</p>
        </body>
    </html>
    """


# Router assignment
app.include_router(api_router)