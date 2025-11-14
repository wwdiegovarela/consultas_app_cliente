"""
Endpoints de FCM (Firebase Cloud Messaging)
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from firebase_admin import messaging
from dependencies import verify_firebase_token, get_bq_client
from models.schemas import FCMTokenRequest, SendMessageNotificationRequest
from config import TABLE_USUARIOS, PROJECT_ID, DATASET_APP
from typing import List, Optional

router = APIRouter()


@router.post("/api/fcm/update-token")
async def update_fcm_token(
    request: FCMTokenRequest,
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Actualiza el FCM token del usuario en BigQuery.
    """
    try:
        user_email = user_data["email"]
        fcm_token = request.fcm_token
        
        print(f"📱 Actualizando FCM token para: {user_email}")
        print(f"🔑 Token: {fcm_token[:20]}...")
        
        # Actualizar token en BigQuery
        query = f"""
        UPDATE `{TABLE_USUARIOS}`
        SET 
            fcm_token = @fcm_token,
            ultima_sesion = CURRENT_TIMESTAMP()
        WHERE email_login = @email_login
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("fcm_token", "STRING", fcm_token),
                bigquery.ScalarQueryParameter("email_login", "STRING", user_email),
            ]
        )
        
        query_job = get_bq_client().query(query, job_config=job_config)
        query_job.result()  # Esperar a que termine
        
        print(f"✅ FCM token actualizado para {user_email}")
        
        return {
            "success": True,
            "message": "Token FCM actualizado correctamente"
        }
        
    except Exception as e:
        print(f"❌ Error actualizando FCM token: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al actualizar token FCM: {str(e)}"
        )


@router.post("/api/fcm/send-message-notification")
async def send_message_notification(
    request: SendMessageNotificationRequest,
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Envía notificaciones push a los participantes de una conversación cuando se crea un mensaje.
    No envía notificación al remitente ni a clientes si el mensaje no es visible para ellos.
    """
    try:
        conversation_id = request.conversation_id
        message_id = request.message_id
        sender_id = request.sender_id
        sender_name = request.sender_name
        message_text = request.message_text
        visible_para_cliente = request.visible_para_cliente
        participant_user_ids = request.participant_user_ids
        
        print(f"📬 Enviando notificaciones push para mensaje: {message_id}")
        print(f"   Conversación: {conversation_id}")
        print(f"   Remitente: {sender_name} ({sender_id})")
        
        # Obtener los tokens FCM de los participantes
        # Necesitamos obtener tokens junto con el rol para filtrar clientes si el mensaje no es visible
        if participant_user_ids:
            # Si se proporcionan los IDs de participantes, obtener solo esos tokens con sus roles
            query = f"""
            SELECT 
                u.email_login,
                u.fcm_token,
                u.firebase_uid,
                p.rol_id
            FROM `{TABLE_USUARIOS}` u
            LEFT JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` p
              ON u.email_login = p.email_login
            WHERE u.fcm_token IS NOT NULL
              AND u.fcm_token != ''
              AND (p.usuario_activo = TRUE OR p.usuario_activo IS NULL)
              AND u.firebase_uid IS NOT NULL
              AND u.firebase_uid != @sender_id
              AND u.firebase_uid IN UNNEST(@participant_ids)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sender_id", "STRING", sender_id),
                    bigquery.ArrayQueryParameter("participant_ids", "STRING", participant_user_ids)
                ]
            )
        else:
            # Si no se proporcionan, obtener todos los tokens activos (excepto el remitente)
            query = f"""
            SELECT 
                u.email_login,
                u.fcm_token,
                u.firebase_uid,
                p.rol_id
            FROM `{TABLE_USUARIOS}` u
            LEFT JOIN `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios` p
              ON u.email_login = p.email_login
            WHERE u.fcm_token IS NOT NULL
              AND u.fcm_token != ''
              AND (p.usuario_activo = TRUE OR p.usuario_activo IS NULL)
              AND u.firebase_uid IS NOT NULL
              AND u.firebase_uid != @sender_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sender_id", "STRING", sender_id)
                ]
            )
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = list(query_job.result())
        
        # Filtrar tokens válidos y aplicar lógica de visibilidad
        tokens = []
        for row in results:
            if not row.fcm_token:
                continue
            
            # Si el mensaje no es visible para clientes, no enviar notificación a clientes
            if not visible_para_cliente and row.rol_id == "CLIENTE":
                print(f"   ⏭️ Saltando cliente {row.email_login} (mensaje no visible)")
                continue
            
            tokens.append(row.fcm_token)
        
        if not tokens:
            print("⚠️ No hay tokens FCM disponibles para enviar notificaciones")
            return {
                "success": True,
                "message": "No hay tokens FCM disponibles",
                "sent_count": 0
            }
        
        print(f"📱 Encontrados {len(tokens)} tokens FCM para enviar notificaciones")
        
        # Preparar el mensaje de notificación
        # Truncar el texto del mensaje si es muy largo
        notification_body = message_text[:100] + "..." if len(message_text) > 100 else message_text
        
        # Crear el mensaje de notificación
        message = messaging.MulticastMessage(
            notification=messaging.Notification(
                title=f"Nuevo mensaje de {sender_name}",
                body=notification_body,
            ),
            data={
                "tipo": "nuevo_mensaje",
                "conversationId": conversation_id,
                "messageId": message_id,
                "senderId": sender_id,
                "senderName": sender_name,
                "visibleParaCliente": str(visible_para_cliente).lower(),
            },
            tokens=tokens,
        )
        
        # Enviar notificaciones
        try:
            response = messaging.send_multicast(message)
            
            print(f"✅ Notificaciones enviadas: {response.success_count} exitosas, {response.failure_count} fallidas")
            
            if response.failure_count > 0:
                print(f"⚠️ Algunas notificaciones fallaron:")
                for idx, resp in enumerate(response.responses):
                    if not resp.success:
                        print(f"   Token {idx}: {resp.exception}")
            
            return {
                "success": True,
                "message": "Notificaciones enviadas",
                "sent_count": response.success_count,
                "failed_count": response.failure_count,
                "total_tokens": len(tokens)
            }
        except Exception as fcm_error:
            print(f"❌ Error en Firebase Admin SDK al enviar notificaciones: {str(fcm_error)}")
            print(f"   Tipo de error: {type(fcm_error).__name__}")
            import traceback
            print(f"   Traceback: {traceback.format_exc()}")
            # Re-lanzar para que se capture en el except externo
            raise
        
    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__
        print(f"❌ Error enviando notificaciones push: {error_type}: {error_msg}")
        import traceback
        print(f"   Traceback completo: {traceback.format_exc()}")
        
        # No lanzar excepción, solo loguear (no es crítico para el funcionamiento)
        return {
            "success": False,
            "message": f"Error enviando notificaciones: {error_type}: {error_msg}",
            "sent_count": 0,
            "error_type": error_type
        }

