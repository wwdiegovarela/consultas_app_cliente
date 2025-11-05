"""
Endpoints de autenticación y permisos
"""
from fastapi import APIRouter, Depends, HTTPException
from dependencies import verify_firebase_token, verify_admin_token, get_bq_client
from google.cloud import bigquery
from google.cloud.firestore import SERVER_TIMESTAMP
from google.cloud import firestore
from config import PROJECT_ID, DATASET_APP

router = APIRouter()


@router.get("/api/auth/me")
async def get_current_user(user: dict = Depends(verify_firebase_token)):
    """
    Obtiene la información del usuario actual con sus permisos.
    Útil para que la app Flutter sepa qué screens mostrar.
    """
    return {
        "email": user["email"],
        "nombre_completo": user["nombre_completo"],
        "cliente_rol": user["cliente_rol"],
        "rol_id": user["rol_id"],
        "nombre_rol": user["nombre_rol"],
        "permisos": user["permisos"],
        "ver_todas_instalaciones": user["ver_todas_instalaciones"]
    }

