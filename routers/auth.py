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


@router.post("/api/admin/sync-users-to-firestore")
async def sync_users_to_firestore(admin: dict = Depends(verify_admin_token)):
    """
    Sincroniza todos los usuarios activos de BigQuery a Firestore.
    Solo puede ser ejecutado por un administrador.
    
    Este endpoint:
    1. Obtiene todos los usuarios activos de v_permisos_usuarios que tengan firebase_uid
    2. Crea/actualiza documentos en Firestore colección 'users'
    """
    try:
        bq_client = get_bq_client()
        firestore_client = firestore.Client(project=PROJECT_ID)
        
        # Query para obtener todos los usuarios activos con firebase_uid
        query = f"""
            SELECT 
                email_login,
                firebase_uid,
                nombre_completo,
                rol_id,
                nombre_rol,
                cliente_rol,
                usuario_activo
            FROM `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios`
            WHERE usuario_activo = TRUE
              AND firebase_uid IS NOT NULL
              AND firebase_uid != ''
            ORDER BY email_login
        """
        
        print(f"[INFO] Obteniendo usuarios de BigQuery...")
        query_job = bq_client.query(query)
        results = list(query_job.result())
        
        print(f"[INFO] Encontrados {len(results)} usuarios activos con firebase_uid")
        
        # Sincronizar a Firestore
        users_collection = firestore_client.collection('users')
        synced = 0
        errors = []
        
        for row in results:
            try:
                user_doc_ref = users_collection.document(row.firebase_uid)
                
                user_data = {
                    'uid': row.firebase_uid,
                    'email': row.email_login,
                    'nombre_completo': row.nombre_completo or '',
                    'role': row.rol_id or 'CLIENTE',
                    'rol_nombre': row.nombre_rol or '',
                    'cliente_rol': row.cliente_rol,
                    'updatedAt': SERVER_TIMESTAMP,
                }
                
                # Usar set con merge para actualizar o crear
                user_doc_ref.set(user_data, merge=True)
                synced += 1
                
            except Exception as e:
                error_msg = f"Error sincronizando {row.email_login}: {str(e)}"
                print(f"[ERROR] {error_msg}")
                errors.append(error_msg)
        
        print(f"[OK] Sincronización completada: {synced} usuarios sincronizados")
        if errors:
            print(f"[WARNING] {len(errors)} errores durante la sincronización")
        
        return {
            "success": True,
            "total_users": len(results),
            "synced": synced,
            "errors": len(errors),
            "error_details": errors if errors else None,
            "message": f"Se sincronizaron {synced} de {len(results)} usuarios exitosamente"
        }
        
    except Exception as e:
        print(f"[ERROR] Error en sync_users_to_firestore: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error sincronizando usuarios: {str(e)}"
        )

