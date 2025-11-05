"""
Endpoints de autenticación y permisos
"""
from fastapi import APIRouter, Depends
from dependencies import verify_firebase_token

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

