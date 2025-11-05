"""
Endpoints para el módulo de mensajería (chat)
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from dependencies import verify_firebase_token, get_bq_client
from config import PROJECT_ID, DATASET_APP, TABLE_USUARIO_INST, TABLE_INST_CONTACTO, TABLE_USUARIO_CONTACTOS, TABLE_CONTACTOS

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
        -- Usuarios WFSA desde instalacion_contacto usando tabla contactos
        -- instalacion_contacto -> contactos (contacto_id) -> v_permisos_usuarios (email_usuario_app)
        -- IMPORTANTE: Solo usuarios WFSA que están en la tabla contactos
        WITH usuarios_wfsa AS (
          SELECT DISTINCT
            u.email_login,
            u.firebase_uid,
            u.nombre_completo,
            u.rol_id
          FROM `{TABLE_INST_CONTACTO}` ic
          JOIN `{TABLE_CONTACTOS}` c
            ON ic.contacto_id = c.contacto_id
          JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` u 
            ON c.email_usuario_app = u.email_login
          WHERE ic.instalacion_rol = @instalacion_rol
            AND u.rol_id != 'CLIENTE'  -- Solo usuarios WFSA
            AND u.usuario_activo = TRUE
            AND c.activo = TRUE  -- Solo contactos activos
            AND c.es_usuario_app = TRUE  -- Solo contactos que son usuarios de la app
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
        )
        -- Combinar resultados (WFSA desde contactos + clientes desde usuario_instalaciones)
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
        
        print(f"[DEBUG] Query ejecutada para instalacion_rol: '{instalacion_rol}'")
        print(f"[DEBUG] Resultados encontrados: {len(results)}")
        
        # Query simplificada para diagnosticar JOINs
        if len(results) == 0:
            print(f"[DEBUG] Ejecutando queries de diagnóstico...")
            
            # Query 1: Verificar qué hay en instalacion_contacto
            diag_query1a = f"""
            SELECT contacto_id, instalacion_rol, cliente_rol
            FROM `{TABLE_INST_CONTACTO}`
            WHERE instalacion_rol = @instalacion_rol
            LIMIT 5
            """
            diag_job1a = get_bq_client().query(diag_query1a, job_config=job_config)
            diag_results1a = list(diag_job1a.result())
            print(f"[DEBUG] Registros en instalacion_contacto: {len(diag_results1a)}")
            if diag_results1a:
                for row in diag_results1a[:3]:
                    print(f"  - contacto_id: {row.contacto_id}, cliente_rol: {row.cliente_rol}")
                    # Verificar si este contacto_id existe en contactos
                    diag_query1b = f"""
                    SELECT contacto_id, email_usuario_app, activo, es_usuario_app
                    FROM `{TABLE_CONTACTOS}`
                    WHERE contacto_id = @contacto_id
                    LIMIT 1
                    """
                    diag_job_config1b = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("contacto_id", "STRING", row.contacto_id)
                        ]
                    )
                    diag_job1b = get_bq_client().query(diag_query1b, diag_job_config1b)
                    diag_results1b = list(diag_job1b.result())
                    if diag_results1b:
                        print(f"    -> Existe en contactos: email_usuario_app={diag_results1b[0].email_usuario_app}, activo={diag_results1b[0].activo}, es_usuario_app={diag_results1b[0].es_usuario_app}")
                    else:
                        print(f"    -> NO existe en contactos")
            
            # Query 2: Verificar usuarios en contactos relacionados con instalacion_contacto
            diag_query2 = f"""
            SELECT DISTINCT
              c.email_usuario_app,
              ic.contacto_id,
              ic.instalacion_rol
            FROM `{TABLE_INST_CONTACTO}` ic
            LEFT JOIN `{TABLE_CONTACTOS}` c
              ON ic.contacto_id = c.contacto_id
            WHERE ic.instalacion_rol = @instalacion_rol
              AND c.activo = TRUE
              AND c.es_usuario_app = TRUE
            LIMIT 10
            """
            diag_job2 = get_bq_client().query(diag_query2, job_config=job_config)
            diag_results2 = list(diag_job2.result())
            print(f"[DEBUG] JOIN instalacion_contacto -> contactos: {len(diag_results2)} filas")
            if diag_results2:
                for row in diag_results2[:3]:
                    print(f"  - contacto_id: {row.contacto_id}, email_usuario_app: {row.email_usuario_app}")
            
            # Query 3: Verificar si esos emails existen en v_permisos_usuarios
            if diag_results2 and diag_results2[0].email_usuario_app:
                sample_email = diag_results2[0].email_usuario_app
                diag_query3 = f"""
                SELECT email_login, rol_id, usuario_activo, firebase_uid
                FROM `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios`
                WHERE email_login = @sample_email
                LIMIT 1
                """
                diag_job_config3 = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("sample_email", "STRING", sample_email)
                    ]
                )
                diag_job3 = get_bq_client().query(diag_query3, diag_job_config3)
                diag_results3 = list(diag_job3.result())
                if diag_results3:
                    print(f"[DEBUG] Usuario '{sample_email}' en v_permisos_usuarios:")
                    print(f"  - rol_id: {diag_results3[0].rol_id}")
                    print(f"  - usuario_activo: {diag_results3[0].usuario_activo}")
                    print(f"  - firebase_uid: {diag_results3[0].firebase_uid}")
                else:
                    print(f"[DEBUG] Usuario '{sample_email}' NO encontrado en v_permisos_usuarios")
            else:
                print(f"[DEBUG] ⚠️ PROBLEMA DETECTADO: No hay usuarios en contactos para esta instalación")
                print(f"[DEBUG] Esto significa que los contacto_id en instalacion_contacto no tienen correspondencia en contactos")
                print(f"[DEBUG] Posibles causas:")
                print(f"  1. Los contacto_id no existen en la tabla contactos")
                print(f"  2. Los contactos no tienen es_usuario_app = TRUE")
                print(f"  3. Los contactos no están activos (activo = FALSE)")
                print(f"  4. Los contactos no tienen email_usuario_app asignado")
        
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
            
            # Verificar si hay usuarios WFSA en contactos relacionados
            if total_ic > 0:
                debug_query3 = f"""
                SELECT COUNT(DISTINCT c.email_usuario_app) as total
                FROM `{TABLE_INST_CONTACTO}` ic
                JOIN `{TABLE_CONTACTOS}` c
                  ON ic.contacto_id = c.contacto_id
                WHERE ic.instalacion_rol = @instalacion_rol
                  AND c.activo = TRUE
                  AND c.es_usuario_app = TRUE
                """
                debug_job3 = get_bq_client().query(debug_query3, job_config=job_config)
                debug_result3 = list(debug_job3.result())
                total_contactos = debug_result3[0].total if debug_result3 else 0
                print(f"[DEBUG] Usuarios en contactos relacionados: {total_contactos}")
                
                # Verificar cuántos tienen firebase_uid y están activos
                debug_query4 = f"""
                SELECT COUNT(DISTINCT u.email_login) as total
                FROM `{TABLE_INST_CONTACTO}` ic
                JOIN `{TABLE_CONTACTOS}` c
                  ON ic.contacto_id = c.contacto_id
                JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` u 
                  ON c.email_usuario_app = u.email_login
                WHERE ic.instalacion_rol = @instalacion_rol
                  AND u.rol_id != 'CLIENTE'
                  AND u.usuario_activo = TRUE
                  AND c.activo = TRUE
                  AND c.es_usuario_app = TRUE
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

