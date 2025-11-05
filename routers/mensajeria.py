"""
Endpoints para el módulo de mensajería (chat)
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from dependencies import verify_firebase_token, get_bq_client
from config import PROJECT_ID, DATASET_APP, TABLE_USUARIO_INST, TABLE_INST_CONTACTO, TABLE_USUARIO_CONTACTOS

router = APIRouter()


@router.get("/api/contactos/usuario/{email_login}")
async def get_contactos_usuario(
    email_login: str,
    user: dict = Depends(verify_firebase_token)
):
    """
    Obtiene todos los contactos asociados a un usuario desde la tabla usuario_contactos.
    
    Para usuarios CLIENTE: retorna todos los contactos asignados al usuario (pueden ser clientes o WFSA).
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
        -- Obtener contactos del usuario desde usuario_contactos
        -- usuario_contactos relaciona: email_login (usuario) -> contacto_id -> instalacion_rol
        -- Necesitamos encontrar qué otros usuarios tienen el mismo contacto_id
        SELECT DISTINCT
          u.email_login,
          u.firebase_uid,
          u.nombre_completo,
          u.rol_id,
          u.cliente_rol
        FROM `{TABLE_USUARIO_CONTACTOS}` uc_usuario
        JOIN `{TABLE_INST_CONTACTO}` ic
          ON uc_usuario.contacto_id = ic.contacto_id
          AND uc_usuario.instalacion_rol = ic.instalacion_rol
        JOIN `{TABLE_USUARIO_CONTACTOS}` uc_contacto
          ON ic.contacto_id = uc_contacto.contacto_id
          AND ic.instalacion_rol = uc_contacto.instalacion_rol
          AND uc_contacto.email_login != @email_login  -- Excluir al usuario mismo
        JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` u
          ON uc_contacto.email_login = u.email_login
        WHERE uc_usuario.email_login = @email_login  -- Contactos asignados a este usuario
          AND u.usuario_activo = TRUE
          AND u.firebase_uid IS NOT NULL  -- Solo usuarios con firebase_uid
          AND u.firebase_uid != ''
        ORDER BY u.nombre_completo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("email_login", "STRING", email_login)
            ]
        )
        
        query_job = get_bq_client().query(query, job_config=job_config)
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
    Obtiene todos los participantes de una instalación para usuarios WFSA.
    
    Retorna:
    1. Todos los usuarios WFSA de instalacion_contacto
    2. Todos los clientes asociados a esa instalación desde usuario_instalaciones
    """
    try:
        query = f"""
        -- Usuarios WFSA desde instalacion_contacto
        -- TEMPORAL: Incluir usuarios sin firebase_uid para diagnóstico
        WITH usuarios_wfsa AS (
          SELECT DISTINCT
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
            AND u.usuario_activo = TRUE
            -- TEMPORAL: Remover filtro de firebase_uid para diagnóstico
            -- AND u.firebase_uid IS NOT NULL
            -- AND u.firebase_uid != ''
        ),
        -- Clientes desde usuario_instalaciones
        clientes_instalacion AS (
          SELECT DISTINCT
            u.email_login,
            u.firebase_uid,
            u.nombre_completo,
            u.rol_id
          FROM `{TABLE_USUARIO_INST}` ui
          JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` u
            ON ui.email_login = u.email_login
          WHERE ui.instalacion_rol = @instalacion_rol
            AND u.rol_id = 'CLIENTE'  -- Solo clientes
            AND u.usuario_activo = TRUE
            -- TEMPORAL: Remover filtro de firebase_uid para diagnóstico
            -- AND u.firebase_uid IS NOT NULL
            -- AND u.firebase_uid != ''
        )
        -- Combinar ambos resultados
        SELECT * FROM usuarios_wfsa
        UNION DISTINCT
        SELECT * FROM clientes_instalacion
        ORDER BY nombre_completo
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("instalacion_rol", "STRING", instalacion_rol)
            ]
        )
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = list(query_job.result())
        
        print(f"[DEBUG] Query ejecutada para instalacion_rol: {instalacion_rol}")
        print(f"[DEBUG] Resultados encontrados: {len(results)}")
        
        # Si no hay resultados, intentar queries de debug
        if len(results) == 0:
            # Verificar si la instalación existe en instalacion_contacto
            debug_query1 = f"""
            SELECT COUNT(*) as total
            FROM `{TABLE_INST_CONTACTO}`
            WHERE instalacion_rol = @instalacion_rol
            """
            debug_job1 = get_bq_client().query(debug_query1, job_config=job_config)
            debug_result1 = list(debug_job1.result())
            total_ic = debug_result1[0].total if debug_result1 else 0
            print(f"[DEBUG] Registros en instalacion_contacto para '{instalacion_rol}': {total_ic}")
            
            # Verificar si hay clientes en usuario_instalaciones
            debug_query2 = f"""
            SELECT COUNT(*) as total
            FROM `{TABLE_USUARIO_INST}`
            WHERE instalacion_rol = @instalacion_rol
            """
            debug_job2 = get_bq_client().query(debug_query2, job_config=job_config)
            debug_result2 = list(debug_job2.result())
            total_ui = debug_result2[0].total if debug_result2 else 0
            print(f"[DEBUG] Registros en usuario_instalaciones para '{instalacion_rol}': {total_ui}")
            
            # Verificar si hay usuarios WFSA en usuario_contactos relacionados
            if total_ic > 0:
                debug_query3 = f"""
                SELECT COUNT(DISTINCT uc.email_login) as total
                FROM `{TABLE_INST_CONTACTO}` ic
                JOIN `{TABLE_USUARIO_CONTACTOS}` uc 
                  ON ic.contacto_id = uc.contacto_id 
                  AND ic.instalacion_rol = uc.instalacion_rol
                WHERE ic.instalacion_rol = @instalacion_rol
                """
                debug_job3 = get_bq_client().query(debug_query3, job_config=job_config)
                debug_result3 = list(debug_job3.result())
                total_uc = debug_result3[0].total if debug_result3 else 0
                print(f"[DEBUG] Usuarios en usuario_contactos relacionados: {total_uc}")
                
                # Verificar cuántos tienen firebase_uid y están activos
                debug_query4 = f"""
                SELECT COUNT(DISTINCT u.email_login) as total
                FROM `{TABLE_INST_CONTACTO}` ic
                JOIN `{TABLE_USUARIO_CONTACTOS}` uc 
                  ON ic.contacto_id = uc.contacto_id 
                  AND ic.instalacion_rol = uc.instalacion_rol
                JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` u 
                  ON uc.email_login = u.email_login
                WHERE ic.instalacion_rol = @instalacion_rol
                  AND u.rol_id != 'CLIENTE'
                  AND u.usuario_activo = TRUE
                  AND u.firebase_uid IS NOT NULL
                  AND u.firebase_uid != ''
                """
                debug_job4 = get_bq_client().query(debug_query4, job_config=job_config)
                debug_result4 = list(debug_job4.result())
                total_wfsa_valid = debug_result4[0].total if debug_result4 else 0
                print(f"[DEBUG] Usuarios WFSA válidos (con firebase_uid): {total_wfsa_valid}")
            
            # Verificar clientes con firebase_uid
            if total_ui > 0:
                debug_query5 = f"""
                SELECT COUNT(DISTINCT u.email_login) as total
                FROM `{TABLE_USUARIO_INST}` ui
                JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` u
                  ON ui.email_login = u.email_login
                WHERE ui.instalacion_rol = @instalacion_rol
                  AND u.rol_id = 'CLIENTE'
                  AND u.usuario_activo = TRUE
                  AND u.firebase_uid IS NOT NULL
                  AND u.firebase_uid != ''
                """
                debug_job5 = get_bq_client().query(debug_query5, job_config=job_config)
                debug_result5 = list(debug_job5.result())
                total_clientes_valid = debug_result5[0].total if debug_result5 else 0
                print(f"[DEBUG] Clientes válidos (con firebase_uid): {total_clientes_valid}")
        
        usuarios = []
        usuarios_sin_uid = []
        for row in results:
            usuario_data = {
                "email_login": row.email_login,
                "firebase_uid": row.firebase_uid,
                "nombre_completo": row.nombre_completo,
                "rol_id": row.rol_id
            }
            # Solo incluir si tiene firebase_uid (requerido para Firestore)
            if row.firebase_uid and row.firebase_uid.strip():
                usuarios.append(usuario_data)
            else:
                usuarios_sin_uid.append(row.email_login)
        
        if usuarios_sin_uid:
            print(f"[WARNING] {len(usuarios_sin_uid)} usuarios encontrados sin firebase_uid (no incluidos): {', '.join(usuarios_sin_uid[:5])}")
            print(f"[INFO] Ejecuta el script de sincronización para asignar firebase_uid a estos usuarios")
        
        return {
            "usuarios": usuarios
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")

