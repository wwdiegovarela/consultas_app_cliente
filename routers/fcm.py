"""
Endpoints de FCM (Firebase Cloud Messaging)
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from dependencies import verify_firebase_token, bq_client
from models.schemas import FCMTokenRequest
from config import TABLE_USUARIOS

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
        
        query_job = bq_client.query(query, job_config=job_config)
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

