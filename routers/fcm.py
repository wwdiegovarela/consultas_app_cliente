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


@router.post("/api/fcm/test-notification")
async def test_notification(
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Endpoint de prueba para diagnosticar problemas con FCM.
    Intenta enviar una notificación de prueba a un token específico.
    """
    try:
        print("🧪 ===== PRUEBA DE NOTIFICACIÓN FCM =====")
        
        # Verificar que Firebase Admin esté inicializado
        import firebase_admin
        if not firebase_admin._apps:
            return {
                "success": False,
                "error": "Firebase Admin no está inicializado",
                "step": "initialization_check"
            }
        print("✅ Firebase Admin está inicializado")
        
        # Obtener el token FCM del usuario actual desde BigQuery
        user_email = user_data["email"]
        query = f"""
        SELECT fcm_token
        FROM `{TABLE_USUARIOS}`
        WHERE email_login = @user_email
          AND fcm_token IS NOT NULL
          AND fcm_token != ''
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ]
        )
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = list(query_job.result())
        
        if not results or not results[0].fcm_token:
            return {
                "success": False,
                "error": "No se encontró token FCM para el usuario",
                "step": "token_lookup",
                "user_email": user_email
            }
        
        test_token = results[0].fcm_token
        print(f"📱 Token FCM encontrado: {test_token[:20]}...")
        
        # Intentar enviar una notificación de prueba
        try:
            message = messaging.Message(
                notification=messaging.Notification(
                    title="Prueba de Notificación",
                    body="Esta es una notificación de prueba desde el backend",
                ),
                data={
                    "tipo": "test",
                    "timestamp": str(int(__import__('time').time())),
                },
                token=test_token,
            )
            
            print("📤 Intentando enviar notificación de prueba...")
            response = messaging.send(message)
            print(f"✅ Notificación enviada exitosamente: {response}")
            
            return {
                "success": True,
                "message": "Notificación de prueba enviada exitosamente",
                "message_id": response,
                "step": "send_success"
            }
            
        except Exception as send_error:
            error_type = type(send_error).__name__
            error_msg = str(send_error)
            print(f"❌ Error al enviar notificación: {error_type}: {error_msg}")
            import traceback
            print(f"   Traceback: {traceback.format_exc()}")
            
            return {
                "success": False,
                "error": f"{error_type}: {error_msg}",
                "error_type": error_type,
                "step": "send_error",
                "traceback": traceback.format_exc()
            }
            
    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)
        print(f"❌ Error en endpoint de prueba: {error_type}: {error_msg}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        
        return {
            "success": False,
            "error": f"{error_type}: {error_msg}",
            "error_type": error_type,
            "step": "general_error",
            "traceback": traceback.format_exc()
        }


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
        
        # Preparar datos del mensaje
        message_data = {
            "tipo": "nuevo_mensaje",
            "conversationId": conversation_id,
            "messageId": message_id,
            "senderId": sender_id,
            "senderName": sender_name,
            "visibleParaCliente": str(visible_para_cliente).lower(),
        }
        
        # Enviar notificaciones individualmente (send_multicast tiene problemas con /batch)
        # Usamos send() que ya sabemos que funciona correctamente
        success_count = 0
        failure_count = 0
        errors = []
        
        print(f"📤 Enviando {len(tokens)} notificaciones individualmente...")
        
        try:
            for idx, token in enumerate(tokens):
                try:
                    message = messaging.Message(
                        notification=messaging.Notification(
                            title=f"Nuevo mensaje de {sender_name}",
                            body=notification_body,
                        ),
                        data=message_data,
                        token=token,
                    )
                    
                    response = messaging.send(message)
                    success_count += 1
                    if (idx + 1) % 10 == 0:
                        print(f"   ✅ Progreso: {idx + 1}/{len(tokens)} enviadas")
                    
                except Exception as e:
                    failure_count += 1
                    error_msg = f"Token {idx}: {type(e).__name__}: {str(e)}"
                    errors.append(error_msg)
                    print(f"   ❌ {error_msg}")
            
            print(f"✅ Notificaciones enviadas: {success_count} exitosas, {failure_count} fallidas")
            
            if errors:
                print(f"⚠️ Errores detallados:")
                for error in errors[:5]:  # Mostrar solo los primeros 5 errores
                    print(f"   {error}")
                if len(errors) > 5:
                    print(f"   ... y {len(errors) - 5} errores más")
            
            return {
                "success": True,
                "message": "Notificaciones enviadas",
                "sent_count": success_count,
                "failed_count": failure_count,
                "total_tokens": len(tokens)
            }
        
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

