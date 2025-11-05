"""
Health check endpoints
"""
from fastapi import APIRouter
from datetime import datetime
from config import PROJECT_ID, ENVIRONMENT

router = APIRouter()


@router.get("/")
async def root():
    """Endpoint raíz para verificar que el servicio está funcionando"""
    return {
        "message": "WFSA BigQuery API funcionando",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }


@router.get("/health")
async def health_check():
    """Endpoint de salud para verificar que el servicio está funcionando"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "WFSA BigQuery API",
        "project_id": PROJECT_ID,
        "environment": ENVIRONMENT
    }


@router.get("/api/health")
async def api_health_check():
    """Health check para Cloud Run."""
    return {
        "status": "healthy",
        "environment": ENVIRONMENT,
        "project_id": PROJECT_ID
    }

