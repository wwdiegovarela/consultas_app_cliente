"""
Endpoints de contactos WhatsApp
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from dependencies import verify_firebase_token, bq_client
from config import TABLE_USUARIO_INST, TABLE_INST_CONTACTO, TABLE_CONTACTOS

router = APIRouter()


@router.get("/api/contactos/{instalacion_rol}")
async def get_contactos_instalacion(
    instalacion_rol: str,
    user: dict = Depends(verify_firebase_token)
):
    """
    Obtiene los contactos de WhatsApp de una instalación.
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          c.contacto_id,
          c.nombre_contacto,
          c.telefono,
          c.cargo,
          c.email
        FROM `{TABLE_USUARIO_INST}` ui
        INNER JOIN `{TABLE_INST_CONTACTO}` ic 
          ON ui.cliente_rol = ic.cliente_rol 
          AND ui.instalacion_rol = ic.instalacion_rol
        INNER JOIN `{TABLE_CONTACTOS}` c ON ic.contacto_id = c.contacto_id
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
          AND ui.instalacion_rol = @instalacion_rol
          AND c.activo = TRUE
        ORDER BY c.nombre_contacto
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                bigquery.ScalarQueryParameter("instalacion_rol", "STRING", instalacion_rol)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        contactos = []
        for row in results:
            contactos.append({
                "contacto_id": row.contacto_id,
                "nombre": row.nombre_contacto,
                "telefono": row.telefono,
                "cargo": row.cargo,
                "email": row.email
            })
        
        return {
            "instalacion": instalacion_rol,
            "total_contactos": len(contactos),
            "contactos": contactos
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")

