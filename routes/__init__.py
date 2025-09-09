from fastapi import APIRouter

# Import routers from versioned packages

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from .v1 import router as v1_router


api_router = APIRouter()

# Rota default
@api_router.get("/")
async def root():
	return JSONResponse({"message": "Hello, World!"})

# Include versioned routers - prefix must have /api for Databricks Apps token-based auth
api_router.include_router(v1_router, prefix="/api/v1")