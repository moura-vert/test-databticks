# Rota root
from fastapi.responses import JSONResponse
from fastapi import FastAPI

from databricks import sql
from databricks.sdk.core import Config
from typing import Dict, List

from fastapi import FastAPI, Request, HTTPException, Query, Depends
from fastapi.responses import RedirectResponse
from jose import jwt, JWTError
from authlib.integrations.starlette_client import OAuth
import os

from fastapi.responses import HTMLResponse
from fastapi.exception_handlers import RequestValidationError
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR
from datetime import datetime, timezone
from typing import Dict

from dotenv import load_dotenv
load_dotenv()



def get_current_user(request: Request):
    if request.url.path in ["/login", "/auth", "/cas/callback", "/openapi.json", "/docs", "/redoc"]:
        return None
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        auth_header = request.cookies.get("access_token")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=307, headers={"Location": "/login"})
    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload["sub"]
    except JWTError:
        raise HTTPException(status_code=307, headers={"Location": "/login"})


app = FastAPI(
    title="FastAPI & Databricks Apps",
    description="A simple FastAPI application example for Databricks Apps runtime",
    version="1.0.0",
    dependencies=[Depends(get_current_user)]
)


# Configurações
CAS_SERVER = "https://sso.stg.vert-tech.dev/cas"
SECRET_KEY = os.getenv("SECRET_KEY", "um_secret_key_seguro")


DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID") or None

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

oauth = OAuth()
oauth.register(
    name='cas',
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    server_metadata_url=f"{CAS_SERVER}/",
    client_kwargs={'scope': 'openid profile email'},
)

@app.get("/")
async def root():
    return JSONResponse({"message": "Hello, World!"})


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


CAS_SERVER = "https://sso.stg.vert-tech.dev/cas"
SERVICE_URL = "http://localhost:8000/cas/callback"

@app.get("/login")
async def login():
    cas_login_url = f"{CAS_SERVER}/login?service={SERVICE_URL}"
    return RedirectResponse(cas_login_url)


@app.get("/auth")
async def auth(request: Request):
    token = await oauth.cas.authorize_access_token(request)
    userinfo = await oauth.cas.parse_id_token(request, token)
    jwt_token = jwt.encode({"sub": userinfo["sub"]}, SECRET_KEY, algorithm="HS256")
    response = RedirectResponse("/dashboard")
    response.set_cookie("access_token", f"Bearer {jwt_token}", httponly=True)
    return response

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
    # Buscar dados da tabela
    try:
        sql_query = "SELECT * FROM data_vertc_stg.lastros.silver_preco_aquisicao_contrato_calculado limit 10"
        results = query(sql_query, warehouse_id=DATABRICKS_WAREHOUSE_ID)
        
        # Gerar tabela HTML
        table_html = "<table border='1' style='border-collapse: collapse; width: 100%;'>"
        
        # Cabeçalho da tabela
        if results:
            table_html += "<thead><tr>"
            for column in results[0].keys():
                table_html += f"<th style='padding: 8px; background-color: #f2f2f2;'>{column}</th>"
            table_html += "</tr></thead>"
            
            # Dados da tabela
            table_html += "<tbody>"
            for row in results:
                table_html += "<tr>"
                for value in row.values():
                    table_html += f"<td style='padding: 8px;'>{value}</td>"
                table_html += "</tr>"
            table_html += "</tbody>"
        else:
            table_html += "<tr><td>Nenhum dado encontrado</td></tr>"
            
        table_html += "</table>"
        
    except Exception as e:
        table_html = f"<p style='color: red;'>Erro ao buscar dados: {str(e)}</p>"
    
    return f"""
    <html>
        <head>
            <title>Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                table {{ margin-top: 20px; }}
                th, td {{ text-align: left; }}
            </style>
        </head>
        <body>
            <h1>Bem-vindo ao Dashboard!</h1>
            <p>Login realizado com sucesso.</p>
            <h2>Dados da Tabela</h2>
            {table_html}
        </body>
    </html>
    """

@app.get("/cas/callback")
async def cas_callback(request: Request):
    # Aqui você pode processar o ticket do CAS, validar o usuário, etc.
    ticket = request.query_params.get("ticket")
    if not ticket:
        return JSONResponse({"error": "Ticket não encontrado"}, status_code=400)
    
    # Criar um JWT token para o usuário (usando ticket como identificador temporário)
    # Em um cenário real, você validaria o ticket com o servidor CAS
    jwt_token = jwt.encode({"sub": f"user_{ticket[:10]}"}, SECRET_KEY, algorithm="HS256")
    
    # Redirecionar para dashboard com cookie de autenticação
    response = RedirectResponse("/dashboard")
    response.set_cookie("access_token", f"Bearer {jwt_token}", httponly=True)
    return response

@app.get("/healthcheck")
async def healthcheck() -> Dict[str, str]:
    """Return the API status."""
    return {"status": "OK", "timestamp": datetime.now(timezone.utc).isoformat()}