"""
Endpoints para el módulo de mensajería (chat)
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from dependencies import verify_firebase_token, bq_client
from config import PROJECT_ID, DATASET_APP, TABLE_USUARIO_INST, TABLE_INST_CONTACTO, TABLE_USUARIO_CONTACTOS

router = APIRouter()


@router.get("/api/contactos/usuario/{email_login}")
async def get_contactos_usuario(
    email_login: str,
    user: dict = Depends(verify_firebase_token)
):
    """
    Obtiene todos los contactos (clientes) que están asociados a las mismas instalaciones que el usuario.
    
    Para el módulo de mensajería: retorna otros clientes que comparten instalaciones con el usuario.
    """
    current_user_email = user["email"]
    
    # Verificar que el usuario solo puede ver sus propios contactos o tiene permisos
    if email_login != current_user_email and not user.get("permisos", {}).get("es_admin", False):
        raise HTTPException(
            status_code=403,
            detail="No tienes permiso para ver contactos de otros usuarios"
        )
    
    try:
        query = f"""
        -- Obtener instalaciones del usuario
        WITH instalaciones_usuario AS (
          SELECT DISTINCT instalacion_rol
          FROM `{TABLE_USUARIO_INST}`
          WHERE email_login = @email_login
        )
        -- Obtener todos los contactos de esas instalaciones
        SELECT DISTINCT
          u.email_login,
          u.firebase_uid,
          u.nombre_completo,
          u.rol_id,
          u.cliente_rol
        FROM instalaciones_usuario iu
        JOIN `{TABLE_USUARIO_INST}` ui
          ON iu.instalacion_rol = ui.instalacion_rol
        JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` u
          ON ui.email_login = u.email_login
        WHERE u.rol_id = 'CLIENTE'
          AND u.usuario_activo = TRUE
          AND u.email_login != @email_login  -- Excluir al usuario mismo
        ORDER BY u.nombre_completo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email_login", "STRING", email_login)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        contactos = []
        for row in results:
            contactos.append({
                "email_login": row.email_login,
                "firebase_uid": row.firebase_uid,
                "nombre_completo": row.nombre_completo,
                "rol_id": row.rol_id,
                "cliente_rol": row.cliente_rol
            })
        
        return {
            "contactos": contactos
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@router.get("/api/usuarios-wfsa/instalacion/{instalacion_rol}")
async def get_usuarios_wfsa_instalacion(
    instalacion_rol: str,
    user: dict = Depends(verify_firebase_token)
):
    """
    Obtiene los usuarios WFSA asociados a una instalación específica.
    
    Para el módulo de mensajería: retorna todos los usuarios WFSA que están asignados a una instalación.
    """
    try:
        query = f"""
        SELECT 
          u.email_login,
          u.firebase_uid,
          u.nombre_completo,
          u.rol_id
        FROM `{TABLE_INST_CONTACTO}` ic
        JOIN `{TABLE_USUARIO_CONTACTOS}` uc 
          ON ic.contacto_id = uc.contacto_id 
          AND ic.instalacion_rol = uc.instalacion_rol
        JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` u 
          ON uc.email_login = u.email_login
        WHERE ic.instalacion_rol = @instalacion_rol
          AND u.rol_id != 'CLIENTE'  -- Solo usuarios WFSA
          AND u.usuario_activo = TRUE  -- Solo usuarios activos
        ORDER BY u.nombre_completo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("instalacion_rol", "STRING", instalacion_rol)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        usuarios = []
        for row in results:
            usuarios.append({
                "email_login": row.email_login,
                "firebase_uid": row.firebase_uid,
                "nombre_completo": row.nombre_completo,
                "rol_id": row.rol_id
            })
        
        return {
            "usuarios": usuarios
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")

