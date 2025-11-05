"""
Dependencias de FastAPI - Autenticación y verificación de permisos
"""
from fastapi import HTTPException, Depends, Header
from typing import Optional
from google.cloud import bigquery
from firebase_admin import auth
from config import PROJECT_ID, DATASET_APP, TABLE_USUARIOS

# Cliente de BigQuery (debe ser inicializado en main.py)
bq_client = None


def set_bq_client(client):
    """Establece el cliente de BigQuery desde main.py"""
    global bq_client
    bq_client = client


async def verify_firebase_token(authorization: Optional[str] = Header(None)) -> dict:
    """
    Valida el token de Firebase y retorna los datos del usuario CON PERMISOS.
    Implementa migración automática de firebase_uid.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Token de autorización requerido")
    
    try:
        # Extraer el token del header "Bearer <token>"
        token = authorization.split("Bearer ")[-1]
        
        # Verificar el token con Firebase Admin
        decoded_token = auth.verify_id_token(token)
        firebase_uid = decoded_token["uid"]
        user_email = decoded_token.get("email")
        
        # Buscar usuario con sus permisos usando la vista
        try:
            check_query = f"""
                SELECT 
                    email_login,
                    nombre_completo,
                    cliente_rol,
                    rol_id,
                    nombre_rol,
                    puede_ver_cobertura,
                    puede_ver_encuestas,
                    puede_enviar_mensajes,
                    puede_ver_empresas,
                    puede_ver_metricas_globales,
                    puede_ver_trabajadores,
                    puede_ver_mensajes_recibidos,
                    es_admin,
                    ver_todas_instalaciones,
                    usuario_activo
                FROM `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios`
                WHERE email_login = @user_email
                LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
                ]
            )
            
            query_job = bq_client.query(check_query, job_config=job_config)
            results = list(query_job.result())
            
            if not results:
                raise HTTPException(status_code=404, detail="Usuario no encontrado en la base de datos")
            
            user_data = results[0]
            
            if not user_data.usuario_activo:
                raise HTTPException(status_code=403, detail="Usuario inactivo")
            
            # Actualizar firebase_uid si es necesario
            update_query = f"""
                UPDATE `{TABLE_USUARIOS}`
                SET firebase_uid = @firebase_uid
                WHERE email_login = @user_email
                  AND (firebase_uid IS NULL OR firebase_uid != @firebase_uid)
            """
            
            job_config_update = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("firebase_uid", "STRING", firebase_uid),
                    bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
                ]
            )
            
            bq_client.query(update_query, job_config=job_config_update).result()
        
        except HTTPException:
            raise
        except Exception as e:
            print(f"⚠️ Error en verificación de usuario: {str(e)}")
            raise HTTPException(status_code=500, detail="Error al verificar usuario")
        
        # Retornar datos del usuario CON PERMISOS
        return {
            "uid": firebase_uid,
            "email": user_email,
            "nombre_completo": user_data.nombre_completo,
            "cliente_rol": user_data.cliente_rol,
            "rol_id": user_data.rol_id,
            "nombre_rol": user_data.nombre_rol,
            "permisos": {
                "puede_ver_cobertura": user_data.puede_ver_cobertura,
                "puede_ver_encuestas": user_data.puede_ver_encuestas,
                "puede_enviar_mensajes": user_data.puede_enviar_mensajes,
                "puede_ver_empresas": user_data.puede_ver_empresas,
                "puede_ver_metricas_globales": user_data.puede_ver_metricas_globales,
                "puede_ver_trabajadores": user_data.puede_ver_trabajadores,
                "puede_ver_mensajes_recibidos": user_data.puede_ver_mensajes_recibidos,
                "es_admin": user_data.es_admin,
            },
            "ver_todas_instalaciones": user_data.ver_todas_instalaciones,
            "email_verified": decoded_token.get("email_verified", False),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token inválido: {str(e)}")


async def verify_admin_token(authorization: Optional[str] = Header(None)) -> dict:
    """
    Valida que el usuario sea administrador.
    """
    user = await verify_firebase_token(authorization)
    
    # Verificar que el usuario tenga permisos de admin
    if not user.get("permisos", {}).get("es_admin", False):
        raise HTTPException(
            status_code=403, 
            detail="Acceso denegado. Se requieren permisos de administrador."
        )
    
    return user


async def verificar_permiso_cobertura(user: dict = Depends(verify_firebase_token)) -> dict:
    """Verifica que el usuario pueda ver cobertura."""
    if not user.get("permisos", {}).get("puede_ver_cobertura", False):
        raise HTTPException(status_code=403, detail="No tienes permiso para ver cobertura")
    return user


async def verificar_permiso_encuestas(user: dict = Depends(verify_firebase_token)) -> dict:
    """Verifica que el usuario pueda ver encuestas."""
    if not user.get("permisos", {}).get("puede_ver_encuestas", False):
        raise HTTPException(status_code=403, detail="No tienes permiso para ver encuestas")
    return user


async def verificar_permiso_mensajes(user: dict = Depends(verify_firebase_token)) -> dict:
    """Verifica que el usuario pueda enviar mensajes."""
    if not user.get("permisos", {}).get("puede_enviar_mensajes", False):
        raise HTTPException(status_code=403, detail="No tienes permiso para enviar mensajes")
    return user


async def verificar_permiso_empresas(user: dict = Depends(verify_firebase_token)) -> dict:
    """Verifica que el usuario pueda ver empresas."""
    if not user.get("permisos", {}).get("puede_ver_empresas", False):
        raise HTTPException(status_code=403, detail="No tienes permiso para ver empresas")
    return user

