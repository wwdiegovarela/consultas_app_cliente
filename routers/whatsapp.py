"""
Endpoints de mensajes WhatsApp
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
import uuid
from dependencies import verify_firebase_token, verificar_permiso_mensajes, get_bq_client
from models.schemas import EnviarMensajeRequest
from config import PROJECT_ID, DATASET_APP, TABLE_CONTACTOS, TABLE_USUARIO_CONTACTOS, TABLE_MENSAJES

router = APIRouter()


@router.post("/api/whatsapp/enviar-mensaje")
async def enviar_mensaje_whatsapp(
    request: EnviarMensajeRequest,
    user: dict = Depends(verificar_permiso_mensajes)
):
    """
    Envía un mensaje de WhatsApp a los contactos asignados de las instalaciones seleccionadas.
    """
    user_email = user["email"]
    cliente_rol = user["cliente_rol"]
    
    try:
        mensajes_enviados = []
        
        for instalacion_rol in request.instalaciones:
            # Obtener contactos que el usuario puede contactar
            query = f"""
                SELECT DISTINCT 
                    c.contacto_id, 
                    c.telefono,
                    c.nombre_contacto
                FROM `{TABLE_CONTACTOS}` c
                INNER JOIN `{TABLE_USUARIO_CONTACTOS}` uc
                  ON c.contacto_id = uc.contacto_id
                WHERE uc.email_login = @user_email
                  AND uc.instalacion_rol = @instalacion_rol
                  AND uc.puede_contactar = TRUE
                  AND c.activo = TRUE
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                    bigquery.ScalarQueryParameter("instalacion_rol", "STRING", instalacion_rol)
                ]
            )
            
            query_job = get_bq_client().query(query, job_config=job_config)
            contactos = list(query_job.result())
            
            # Enviar WhatsApp a cada contacto
            for contacto in contactos:
                # TODO: Integrar con API de WhatsApp (Twilio, WhatsApp Business API, etc.)
                # Por ahora solo registramos el mensaje
                
                mensaje_id = str(uuid.uuid4())
                
                # Registrar en mensajes_whatsapp
                insert_query = f"""
                    INSERT INTO `{TABLE_MENSAJES}` 
                    (mensaje_id, email_usuario, cliente_rol, instalacion_rol, contacto_id, mensaje, estado, fecha_envio)
                    VALUES 
                    (@mensaje_id, @email_usuario, @cliente_rol, @instalacion_rol, @contacto_id, @mensaje, 'pendiente', CURRENT_TIMESTAMP())
                """
                
                job_config_insert = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("mensaje_id", "STRING", mensaje_id),
                        bigquery.ScalarQueryParameter("email_usuario", "STRING", user_email),
                        bigquery.ScalarQueryParameter("cliente_rol", "STRING", cliente_rol),
                        bigquery.ScalarQueryParameter("instalacion_rol", "STRING", instalacion_rol),
                        bigquery.ScalarQueryParameter("contacto_id", "STRING", contacto.contacto_id),
                        bigquery.ScalarQueryParameter("mensaje", "STRING", request.mensaje)
                    ]
                )
                
                get_bq_client().query(insert_query, job_config=job_config_insert).result()
                
                mensajes_enviados.append({
                    'mensaje_id': mensaje_id,
                    'contacto_id': contacto.contacto_id,
                    'instalacion': instalacion_rol,
                    'estado': 'pendiente'
                })
        
        return {
            "success": True,
            "message": "Mensajes registrados correctamente",
            "total_enviados": len(mensajes_enviados)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al enviar mensajes: {str(e)}")


@router.get("/api/whatsapp/mensajes-recibidos")
async def get_mensajes_recibidos(user: dict = Depends(verify_firebase_token)):
    """
    Obtiene los mensajes recibidos por un contacto WFSA.
    Solo disponible para usuarios con rol CONTACTO_WFSA o superior.
    """
    user_email = user["email"]
    
    # Verificar que el usuario pueda ver mensajes recibidos
    if not user.get("permisos", {}).get("puede_ver_mensajes_recibidos", False):
        raise HTTPException(
            status_code=403, 
            detail="No tienes permiso para ver mensajes recibidos"
        )
    
    try:
        query = f"""
            SELECT 
                mensaje_id,
                remitente_email,
                remitente_nombre,
                remitente_cliente,
                instalacion_rol,
                instalacion_direccion,
                instalacion_comuna,
                mensaje,
                estado,
                fecha_envio,
                fecha_lectura,
                leido
            FROM `{PROJECT_ID}.{DATASET_APP}.v_mensajes_recibidos`
            WHERE destinatario_email_app = @user_email
            ORDER BY fecha_envio DESC
            LIMIT 100
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ]
        )
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = list(query_job.result())
        
        mensajes = []
        for row in results:
            mensajes.append({
                "mensaje_id": row.mensaje_id,
                "remitente": {
                    "email": row.remitente_email,
                    "nombre": row.remitente_nombre,
                    "cliente": row.remitente_cliente
                },
                "instalacion": {
                    "rol": row.instalacion_rol,
                    "direccion": row.instalacion_direccion,
                    "comuna": row.instalacion_comuna
                },
                "mensaje": row.mensaje,
                "estado": row.estado,
                "fecha_envio": row.fecha_envio.isoformat() if row.fecha_envio else None,
                "fecha_lectura": row.fecha_lectura.isoformat() if row.fecha_lectura else None,
                "leido": row.leido
            })
        
        return {
            "success": True,
            "total": len(mensajes),
            "mensajes": mensajes
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener mensajes: {str(e)}")

