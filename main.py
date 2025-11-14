"""
Main entry point para la API WFSA BigQuery
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from firebase_admin import credentials, initialize_app
import os

# Importar routers
from routers import (
    health, auth, mensajeria, cobertura, encuestas,
    contactos, whatsapp, ppc, fcm
)

# Importar dependencias para inicializar el cliente BigQuery
from dependencies import set_bq_client

# ============================================
# INICIALIZACIÓN
# ============================================

# Inicializar Firebase Admin
try:
    # En Cloud Run, Firebase Admin usa las credenciales de la cuenta de servicio automáticamente
    # Especificar el proyecto explícitamente para evitar problemas
    import firebase_admin
    if not firebase_admin._apps:
        initialize_app(options={
            'projectId': os.getenv('GCP_PROJECT_ID', 'worldwide-470917'),
        })
        print("[OK] Firebase Admin inicializado correctamente")
    else:
        print("[INFO] Firebase Admin ya estaba inicializado")
except ValueError as e:
    # Ya está inicializado
    print(f"[INFO] Firebase Admin ya estaba inicializado: {e}")
    pass
except Exception as e:
    print(f"[ERROR] Error inicializando Firebase Admin: {e}")
    import traceback
    print(f"[ERROR] Traceback: {traceback.format_exc()}")
    # No bloquear el inicio del servidor, pero loguear el error
    pass

# Inicializar API
app = FastAPI(
    title="WFSA BigQuery API",
    version="1.0.0",
    description="API para la app WFSA - Cobertura de guardias en tiempo real"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cliente de BigQuery con manejo de errores
try:
    bq_client = bigquery.Client()
    set_bq_client(bq_client)  # Pasar el cliente a dependencies
    print("[OK] BigQuery client inicializado correctamente")
except Exception as e:
    print(f"[ERROR] Error inicializando BigQuery client: {e}")
    bq_client = None

# ============================================
# REGISTRAR ROUTERS
# ============================================

# Health checks
app.include_router(health.router, tags=["Health"])

# Autenticación
app.include_router(auth.router, tags=["Auth"])

# Mensajería (nuevos endpoints)
app.include_router(mensajeria.router, tags=["Mensajería"])

# Cobertura
app.include_router(cobertura.router, tags=["Cobertura"])

# PPC
app.include_router(ppc.router, tags=["PPC"])

# Contactos WhatsApp
app.include_router(contactos.router, tags=["Contactos"])

# Mensajes WhatsApp
app.include_router(whatsapp.router, tags=["WhatsApp"])

# Encuestas
app.include_router(encuestas.router, tags=["Encuestas"])

# FCM
app.include_router(fcm.router, tags=["FCM"])

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    print(f"[INFO] Iniciando servidor en puerto {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)

