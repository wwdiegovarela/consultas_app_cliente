"""
Configuración del backend - Variables de entorno y constantes
"""
import os

# ============================================
# CONFIGURACIÓN DE BIGQUERY
# ============================================

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "worldwide-470917")
DATASET_REPORTES = os.getenv("DATASET_REPORTES", "cr_reportes")
DATASET_APP = os.getenv("DATASET_APP", "app_clientes")

# Tablas del sistema origen
TABLE_COBERTURA = f"{PROJECT_ID}.{DATASET_REPORTES}.cobertura_instantanea"
TABLE_HISTORICO = f"{PROJECT_ID}.{DATASET_REPORTES}.cr_asistencia_hist_tb"
TABLE_INSTALACIONES = f"{PROJECT_ID}.{DATASET_APP}.cr_info_instalaciones"
TABLE_PPC = f"{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia"  # Puestos Por Cubrir

# Tablas de gestión
TABLE_USUARIOS = f"{PROJECT_ID}.{DATASET_APP}.usuarios_app"
TABLE_USUARIO_INST = f"{PROJECT_ID}.{DATASET_APP}.usuario_instalaciones"
TABLE_CONTACTOS = f"{PROJECT_ID}.{DATASET_APP}.contactos"
TABLE_INST_CONTACTO = f"{PROJECT_ID}.{DATASET_APP}.instalacion_contacto"
TABLE_MENSAJES = f"{PROJECT_ID}.{DATASET_APP}.mensajes_whatsapp"
TABLE_AUDITORIA = f"{PROJECT_ID}.{DATASET_APP}.auditoria"
TABLE_ROLES = f"{PROJECT_ID}.{DATASET_APP}.roles"
TABLE_USUARIO_CONTACTOS = f"{PROJECT_ID}.{DATASET_APP}.usuario_contactos"

# Tablas de Encuestas
TABLE_ENCUESTAS_CONFIG = f"{PROJECT_ID}.{DATASET_APP}.encuestas_configuracion"
TABLE_ENCUESTAS_PREGUNTAS = f"{PROJECT_ID}.{DATASET_APP}.encuestas_preguntas"
TABLE_ENCUESTAS_SOLICITUDES = f"{PROJECT_ID}.{DATASET_APP}.encuestas_solicitudes"
TABLE_ENCUESTAS_RESPUESTAS = f"{PROJECT_ID}.{DATASET_APP}.encuestas_respuestas"
TABLE_ENCUESTAS_NOTIF_PROG = f"{PROJECT_ID}.{DATASET_APP}.encuestas_notificaciones_programadas"
TABLE_ENCUESTAS_NOTIF_LOG = f"{PROJECT_ID}.{DATASET_APP}.encuestas_notificaciones_log"

# Configuración de semáforos (pueden ser variables de entorno)
SEMAFORO_VERDE = float(os.getenv("SEMAFORO_VERDE", "0.95"))     # 95% o más
SEMAFORO_AMARILLO = float(os.getenv("SEMAFORO_AMARILLO", "0.80")) # 80% a 94%

# Días históricos por defecto
DIAS_HISTORICO_DEFAULT = int(os.getenv("DIAS_HISTORICO_DEFAULT", "90"))

# Ambiente de ejecución
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

