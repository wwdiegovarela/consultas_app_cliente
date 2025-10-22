from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from firebase_admin import auth, credentials, initialize_app
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, EmailStr
import os
from datetime import datetime, timedelta
import uuid

# ============================================
# INICIALIZACIÓN
# ============================================

# Inicializar Firebase Admin
try:
    initialize_app()
except ValueError:
    # Ya está inicializado
    pass

# Inicializar FastAPI
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
    print("✅ BigQuery client inicializado correctamente")
except Exception as e:
    print(f"❌ Error inicializando BigQuery client: {e}")
    bq_client = None

# ============================================
# CONFIGURACIÓN DE BIGQUERY (VARIABLES DE ENTORNO)
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


# ============================================
# MODELOS PYDANTIC
# ============================================

class UsuarioCreate(BaseModel):
    email: EmailStr
    password: str
    nombre_completo: str
    cliente_rol: str
    rol_id: str = "CLIENTE"
    cargo: Optional[str] = None
    telefono: Optional[str] = None
    ver_todas_instalaciones: bool = False
    instalaciones_permitidas: List[str] = []


class ContactoCreate(BaseModel):
    nombre_contacto: str
    telefono: str
    cargo: Optional[str] = None
    email: Optional[str] = None


class EnviarMensajeRequest(BaseModel):
    instalaciones: List[str]
    mensaje: str


class RespuestaEncuestaRequest(BaseModel):
    respuestas: List[Dict[str, Any]]  # [{"pregunta_id": "P001", "respuesta_valor": "5", "comentario": "..."}]


# ============================================
# DEPENDENCIAS - AUTENTICACIÓN
# ============================================

async def verify_firebase_token(authorization: Optional[str] = Header(None)) -> dict:
    """
    Valida el token de Firebase y retorna los datos del usuario CON PERMISOS.
    Implementa migración automática de firebase_uid.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Token de autorización requerido")
    
    try:
        # Extraer el token del header "Bearer <token>"
        token = authorization.split("Bearer ")[-1]
        
        # Verificar el token con Firebase Admin
        decoded_token = auth.verify_id_token(token)
        firebase_uid = decoded_token["uid"]
        user_email = decoded_token.get("email")
        
        # Buscar usuario con sus permisos usando la vista
        try:
            check_query = f"""
                SELECT 
                    email_login,
                    nombre_completo,
                    cliente_rol,
                    rol_id,
                    nombre_rol,
                    puede_ver_cobertura,
                    puede_ver_encuestas,
                    puede_enviar_mensajes,
                    puede_ver_empresas,
                    puede_ver_metricas_globales,
                    puede_ver_trabajadores,
                    puede_ver_mensajes_recibidos,
                    es_admin,
                    ver_todas_instalaciones,
                    usuario_activo
                FROM `{PROJECT_ID}.{DATASET_APP}.v_permisos_usuarios`
                WHERE email_login = @user_email
                LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
                ]
            )
            
            query_job = bq_client.query(check_query, job_config=job_config)
            results = list(query_job.result())
            
            if not results:
                raise HTTPException(status_code=404, detail="Usuario no encontrado en la base de datos")
            
            user_data = results[0]
            
            if not user_data.usuario_activo:
                raise HTTPException(status_code=403, detail="Usuario inactivo")
            
            # Actualizar firebase_uid si es necesario
            update_query = f"""
                UPDATE `{TABLE_USUARIOS}`
                SET firebase_uid = @firebase_uid
                WHERE email_login = @user_email
                  AND (firebase_uid IS NULL OR firebase_uid != @firebase_uid)
            """
            
            job_config_update = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("firebase_uid", "STRING", firebase_uid),
                    bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
                ]
            )
            
            bq_client.query(update_query, job_config=job_config_update).result()
        
        except HTTPException:
            raise
        except Exception as e:
            print(f"⚠️ Error en verificación de usuario: {str(e)}")
            raise HTTPException(status_code=500, detail="Error al verificar usuario")
        
        # Retornar datos del usuario CON PERMISOS
        return {
            "uid": firebase_uid,
            "email": user_email,
            "nombre_completo": user_data.nombre_completo,
            "cliente_rol": user_data.cliente_rol,
            "rol_id": user_data.rol_id,
            "nombre_rol": user_data.nombre_rol,
            "permisos": {
                "puede_ver_cobertura": user_data.puede_ver_cobertura,
                "puede_ver_encuestas": user_data.puede_ver_encuestas,
                "puede_enviar_mensajes": user_data.puede_enviar_mensajes,
                "puede_ver_empresas": user_data.puede_ver_empresas,
                "puede_ver_metricas_globales": user_data.puede_ver_metricas_globales,
                "puede_ver_trabajadores": user_data.puede_ver_trabajadores,
                "puede_ver_mensajes_recibidos": user_data.puede_ver_mensajes_recibidos,
                "es_admin": user_data.es_admin,
            },
            "ver_todas_instalaciones": user_data.ver_todas_instalaciones,
            "email_verified": decoded_token.get("email_verified", False),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token inválido: {str(e)}")


async def verify_admin_token(authorization: Optional[str] = Header(None)) -> dict:
    """
    Valida que el usuario sea administrador.
    """
    user = await verify_firebase_token(authorization)
    
    # Verificar que el usuario tenga permisos de admin
    if not user.get("permisos", {}).get("es_admin", False):
        raise HTTPException(
            status_code=403, 
            detail="Acceso denegado. Se requieren permisos de administrador."
        )
    
    return user


async def verificar_permiso_cobertura(user: dict = Depends(verify_firebase_token)) -> dict:
    """Verifica que el usuario pueda ver cobertura."""
    if not user.get("permisos", {}).get("puede_ver_cobertura", False):
        raise HTTPException(status_code=403, detail="No tienes permiso para ver cobertura")
    return user


async def verificar_permiso_encuestas(user: dict = Depends(verify_firebase_token)) -> dict:
    """Verifica que el usuario pueda ver encuestas."""
    if not user.get("permisos", {}).get("puede_ver_encuestas", False):
        raise HTTPException(status_code=403, detail="No tienes permiso para ver encuestas")
    return user


async def verificar_permiso_mensajes(user: dict = Depends(verify_firebase_token)) -> dict:
    """Verifica que el usuario pueda enviar mensajes."""
    if not user.get("permisos", {}).get("puede_enviar_mensajes", False):
        raise HTTPException(status_code=403, detail="No tienes permiso para enviar mensajes")
    return user


async def verificar_permiso_empresas(user: dict = Depends(verify_firebase_token)) -> dict:
    """Verifica que el usuario pueda ver empresas."""
    if not user.get("permisos", {}).get("puede_ver_empresas", False):
        raise HTTPException(status_code=403, detail="No tienes permiso para ver empresas")
    return user


def calcular_estado_semaforo(porcentaje: float) -> str:
    """
    Calcula el estado del semáforo según el porcentaje de cobertura.
    """
    if porcentaje >= SEMAFORO_VERDE * 100:
        return "VERDE"
    elif porcentaje >= SEMAFORO_AMARILLO * 100:
        return "AMARILLO"
    else:
        return "ROJO"


# ============================================
# ENDPOINTS - HEALTH CHECK
# ============================================

@app.get("/")
async def root():
    """Endpoint de salud del servicio."""
    return {
        "status": "ok",
        "service": "WFSA BigQuery API",
        "version": "1.0.0",
        "environment": ENVIRONMENT,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/health")
async def health_check():
    """Health check para Cloud Run."""
    return {
        "status": "healthy",
        "environment": ENVIRONMENT,
        "project_id": PROJECT_ID
    }


# ============================================
# ENDPOINTS - COBERTURA INSTANTÁNEA
# ============================================

@app.get("/api/cobertura/instantanea/general")
async def get_cobertura_general(user: dict = Depends(verificar_permiso_cobertura)):
    """
    Obtiene el % de cobertura general del cliente (todos los turnos activos ahora).
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          COUNT(*) as total_turnos_activos,
          SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END) as turnos_cubiertos,
          COUNT(*) - SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END) as turnos_descubiertos,
          
          -- Porcentaje general
          ROUND(SAFE_DIVIDE(
            SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END),
            COUNT(*)
          ) * 100, 2) as porcentaje_cobertura_general,
          
          -- Timestamp de actualización
          MAX(ci.hora_actual) as ultima_actualizacion,
          
          -- Total de PPC (Puestos Por Cubrir)
          (
            SELECT COUNT(*)
            FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia` ppc
            INNER JOIN `{TABLE_USUARIO_INST}` ui2 
              ON ppc.instalacion_rol = ui2.instalacion_rol
            WHERE ui2.email_login = @user_email
              AND ui2.puede_ver = TRUE
          ) as total_ppc

        FROM `{TABLE_COBERTURA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.cliente_rol = ui.cliente_rol 
          AND ci.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result())
        
        if not results or results[0].total_turnos_activos == 0:
            return {
                "total_turnos_activos": 0,
                "turnos_cubiertos": 0,
                "turnos_descubiertos": 0,
                "porcentaje_cobertura_general": 0,
                "estado_semaforo": "GRIS",
                "ultima_actualizacion": None,
                "total_ppc": 0
            }
        
        row = results[0]
        porcentaje = float(row.porcentaje_cobertura_general) if row.porcentaje_cobertura_general else 0
        
        return {
            "total_turnos_activos": row.total_turnos_activos,
            "turnos_cubiertos": row.turnos_cubiertos,
            "turnos_descubiertos": row.turnos_descubiertos,
            "porcentaje_cobertura_general": porcentaje,
            "estado_semaforo": calcular_estado_semaforo(porcentaje),
            "ultima_actualizacion": row.ultima_actualizacion.isoformat() if row.ultima_actualizacion else None,
            "proxima_actualizacion": (row.ultima_actualizacion + timedelta(minutes=5)).isoformat() if row.ultima_actualizacion else None,
            "total_ppc": row.total_ppc if hasattr(row, 'total_ppc') else 0
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@app.get("/api/cobertura/instantanea/por-instalacion")
async def get_cobertura_por_instalacion(user: dict = Depends(verificar_permiso_cobertura)):
    """
    Obtiene la cobertura instantánea por instalación con semáforo.
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ci.instalacion_rol,
          ci.zona,
          ci.cliente_rol,
          
          -- Contadores
          COUNT(*) as total_guardias_requeridos,
          SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END) as guardias_presentes,
          COUNT(*) - SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END) as guardias_ausentes,
          
          -- Porcentaje de cobertura
          ROUND(SAFE_DIVIDE(
            SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END),
            COUNT(*)
          ) * 100, 2) as porcentaje_cobertura,
          
          -- Cantidad de turnos por estado COB
          COUNTIF(ci.COB = 'CUBIERTO') as turnos_cubiertos,
          COUNTIF(ci.COB = 'DESCUBIERTO') as turnos_descubiertos,
          
          -- Turnos activos
          COUNT(DISTINCT ci.turno) as cantidad_turnos_activos,
          
          -- PPC (Puestos Por Cubrir) - Optimizado con LEFT JOIN
          COALESCE(ppc.cantidad_ppc, 0) as ppc,
          
          -- FaceID (verificar si la instalación tiene equipo Face ID)
          CASE 
            WHEN faceid.nombre IS NOT NULL THEN TRUE 
            ELSE FALSE 
          END as tiene_faceid,
          faceid.numero as faceid_numero,
          faceid.ult_conexion as faceid_ultima_conexion

        FROM `{TABLE_COBERTURA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.cliente_rol = ui.cliente_rol 
          AND ci.instalacion_rol = ui.instalacion_rol
        LEFT JOIN `{PROJECT_ID}.{DATASET_REPORTES}.cr_equipos_faceid` faceid
          ON ci.instalacion_rol = faceid.nombre
        LEFT JOIN (
          SELECT instalacion_rol, COUNT(*) as cantidad_ppc
          FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia`
          GROUP BY instalacion_rol
        ) ppc ON ci.instalacion_rol = ppc.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        GROUP BY ci.instalacion_rol, ci.zona, ci.cliente_rol, faceid.nombre, faceid.numero, faceid.ult_conexion, ppc.cantidad_ppc
        ORDER BY guardias_ausentes DESC, porcentaje_cobertura ASC, ci.instalacion_rol
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ],
            # Optimizaciones de rendimiento
            use_query_cache=True,
            use_legacy_sql=False,
            # Timeout más largo para consultas complejas
            job_timeout_ms=300000,  # 5 minutos
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        instalaciones = []
        for row in results:
            porcentaje = float(row.porcentaje_cobertura) if row.porcentaje_cobertura else 0
            
            instalaciones.append({
                "instalacion_rol": row.instalacion_rol,
                "zona": row.zona,
                "cliente_rol": row.cliente_rol,
                "total_guardias_requeridos": row.total_guardias_requeridos,
                "guardias_presentes": row.guardias_presentes,
                "guardias_ausentes": row.guardias_ausentes,
                "porcentaje_cobertura": porcentaje,
                "estado_semaforo": calcular_estado_semaforo(porcentaje),
                "turnos_cubiertos": row.turnos_cubiertos,
                "turnos_descubiertos": row.turnos_descubiertos,
                "cantidad_turnos_activos": row.cantidad_turnos_activos,
                "ppc": row.ppc,
                "tiene_faceid": bool(row.tiene_faceid),
                "faceid_numero": row.faceid_numero if row.faceid_numero else None,
                "faceid_ultima_conexion": row.faceid_ultima_conexion.isoformat() if row.faceid_ultima_conexion else None
            })
        
        return {
            "total_instalaciones": len(instalaciones),
            "instalaciones": instalaciones
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@app.get("/api/cobertura/instantanea/por-instalacion-fast")
async def get_cobertura_por_instalacion_fast(user: dict = Depends(verify_firebase_token)):
    """
    Versión optimizada del endpoint de instalaciones.
    - Elimina subqueries
    - Usa JOINs optimizados
    - Incluye caché de BigQuery
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ci.instalacion_rol,
          ci.zona,
          ci.cliente_rol,
          
          -- Contadores básicos
          COUNT(*) AS total_guardias_requeridos,
          SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END) AS guardias_presentes,
          COUNT(*) - SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END) AS guardias_ausentes,
          
          -- Porcentaje de Cobertura
          ROUND(SAFE_DIVIDE(
            SUM(CASE WHEN ci.asistencia = 1 THEN 1 ELSE 0 END),
            COUNT(*)
          ) * 100, 1) AS porcentaje_cobertura,
          
          -- PPC desde JOIN optimizado (alias renombrado para evitar conflicto)
          COALESCE(ppc.cantidad_ppc, 0) AS cantidad_ppc_total,
          
          -- FaceID básico
          CASE WHEN faceid.nombre IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_faceid

        FROM `{TABLE_COBERTURA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.cliente_rol = ui.cliente_rol 
          AND ci.instalacion_rol = ui.instalacion_rol
        LEFT JOIN `{PROJECT_ID}.{DATASET_REPORTES}.cr_equipos_faceid` faceid
          ON ci.instalacion_rol = faceid.nombre
        LEFT JOIN (
          -- Subconsulta optimizada para contar PPC por instalación
          SELECT instalacion_rol, COUNT(*) AS cantidad_ppc
          FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia`
          GROUP BY instalacion_rol
        ) ppc ON ci.instalacion_rol = ppc.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        GROUP BY ci.instalacion_rol, ci.zona, ci.cliente_rol, faceid.nombre, ppc.cantidad_ppc
        ORDER BY guardias_ausentes DESC, porcentaje_cobertura ASC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ],
            use_query_cache=True,
            use_legacy_sql=False,
            job_timeout_ms=120000,  # 2 minutos
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        instalaciones = []
        for row in results:
            porcentaje = float(row.porcentaje_cobertura) if row.porcentaje_cobertura else 0
            
            instalaciones.append({
                "instalacion_rol": row.instalacion_rol,
                "zona": row.zona,
                "cliente_rol": row.cliente_rol,
                "total_guardias_requeridos": row.total_guardias_requeridos,
                "guardias_presentes": row.guardias_presentes,
                "guardias_ausentes": row.guardias_ausentes,
                "porcentaje_cobertura": porcentaje,
                "estado_semaforo": calcular_estado_semaforo(porcentaje),
                "ppc": row.cantidad_ppc_total,
                "tiene_faceid": bool(row.tiene_faceid),
            })
        
        return {
            "total_instalaciones": len(instalaciones),
            "instalaciones": instalaciones,
            "optimized": True
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@app.get("/api/cobertura/instantanea/detalle-todas")
async def get_detalle_todas_instalaciones(
    user: dict = Depends(verificar_permiso_cobertura)
):
    """
    Obtiene el detalle de turnos Y PPC de TODAS las instalaciones del usuario en una sola consulta.
    Optimizado para precarga - ahora incluye PPC.
    """
    user_email = user["email"]
    
    try:
        # Consulta 1: Detalle de turnos
        query_turnos = f"""
        SELECT 
          ci.instalacion_rol,
          ci.turno as codigo_turno,
          ci.cargo,
          FORMAT_DATETIME('%H:%M', ci.her) as hora_entrada_planificada,
          FORMAT_DATETIME('%H:%M', ci.hsr) as hora_salida_planificada,
          
          -- Información del guardia planificado
          ci.rutrol as rut_planificado,
          
          -- Información del guardia que asistió
          ci.rutasi as rut_asistente,
          FORMAT_DATETIME('%H:%M', ci.entrada) as hora_entrada_real,
          FORMAT_DATETIME('%H:%M', ci.salida) as hora_salida_real,
          
          -- Estado
          ci.asistencia,
          ci.COB as estado_cobertura,
          ci.tvf as turno_extra,
          ci.relevo,
          ci.tipo,
          ci.motivoppc as motivo_incumplimiento,
          
          -- Indicador de retraso (si asistió)
          CASE 
            WHEN ci.asistencia = 1 AND ci.entrada > ci.her THEN 
              CONCAT('Retraso: ', CAST(DATETIME_DIFF(ci.entrada, ci.her, MINUTE) AS STRING), ' minutos')
            WHEN ci.asistencia = 1 THEN 'A tiempo'
            ELSE NULL
          END as puntualidad

        FROM `{TABLE_COBERTURA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.cliente_rol = ui.cliente_rol 
          AND ci.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        ORDER BY ci.instalacion_rol, ci.turno, ci.her
        """
        
        # Consulta 2: PPC por instalación
        query_ppc = f"""
        SELECT 
          ppc.instalacion_rol,
          ppc.turno,
          ppc.jornada,
          FORMAT_DATETIME('%H:%M', ppc.her) as hora_entrada,
          FORMAT_DATETIME('%H:%M', ppc.hsr) as hora_salida,
          CONCAT(
            FORMAT_DATETIME('%H:%M', ppc.her),
            ' - ',
            FORMAT_DATETIME('%H:%M', ppc.hsr)
          ) as horario,
          COUNT(*) as cantidad_ppc
        FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia` ppc
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ppc.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        GROUP BY ppc.instalacion_rol, ppc.turno, ppc.jornada, ppc.her, ppc.hsr
        ORDER BY ppc.instalacion_rol, ppc.her, ppc.hsr
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ]
        )
        
        # Ejecutar ambas consultas en paralelo
        query_job_turnos = bq_client.query(query_turnos, job_config=job_config)
        query_job_ppc = bq_client.query(query_ppc, job_config=job_config)
        
        results_turnos = list(query_job_turnos.result())
        results_ppc_list = list(query_job_ppc.result())
        
        print(f"📊 Resultados: {len(results_turnos)} turnos, {len(results_ppc_list)} grupos de PPC")
        
        # Agrupar turnos por instalación
        instalaciones_detalle = {}
        for row in results_turnos:
            instalacion = row.instalacion_rol
            
            if instalacion not in instalaciones_detalle:
                instalaciones_detalle[instalacion] = {
                    "instalacion": instalacion,
                    "turnos": [],
                    "total_ppc": 0,
                    "ppc_por_turno": []
                }
            
            instalaciones_detalle[instalacion]["turnos"].append({
                "codigo_turno": row.codigo_turno,
                "cargo": row.cargo,
                "hora_entrada_planificada": row.hora_entrada_planificada,
                "hora_salida_planificada": row.hora_salida_planificada,
                "rut_planificado": row.rut_planificado,
                "rut_asistente": row.rut_asistente,
                "hora_entrada_real": row.hora_entrada_real,
                "hora_salida_real": row.hora_salida_real,
                "asistio": bool(row.asistencia),
                "estado_cobertura": row.estado_cobertura,
                "turno_extra": row.turno_extra,
                "relevo": row.relevo,
                "tipo": row.tipo,
                "motivo_incumplimiento": row.motivo_incumplimiento,
                "puntualidad": row.puntualidad
            })
        
        # Agregar PPC por instalación
        print(f"📊 Procesando PPC...")
        for row in results_ppc_list:
            instalacion = row.instalacion_rol
            print(f"  🔸 PPC para {instalacion}: turno={row.turno}, cantidad={row.cantidad_ppc}")
            
            # Si la instalación no existe (no tiene turnos activos pero sí PPC), crearla
            if instalacion not in instalaciones_detalle:
                instalaciones_detalle[instalacion] = {
                    "instalacion": instalacion,
                    "turnos": [],
                    "total_ppc": 0,
                    "ppc_por_turno": []
                }
            
            instalaciones_detalle[instalacion]["ppc_por_turno"].append({
                "turno": row.turno,
                "jornada": row.jornada,
                "hora_entrada": row.hora_entrada,
                "hora_salida": row.hora_salida,
                "horario": row.horario,
                "cantidad_ppc": row.cantidad_ppc
            })
            instalaciones_detalle[instalacion]["total_ppc"] += row.cantidad_ppc
            print(f"  ✅ Total PPC acumulado para {instalacion}: {instalaciones_detalle[instalacion]['total_ppc']}")
        
        # Agregar total_turnos a cada instalación
        for detalle in instalaciones_detalle.values():
            detalle["total_turnos"] = len(detalle["turnos"])
        
        return {
            "total_instalaciones": len(instalaciones_detalle),
            "instalaciones": list(instalaciones_detalle.values())
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@app.get("/api/cobertura/instantanea/detalle/{instalacion_rol}")
async def get_detalle_instalacion(
    instalacion_rol: str,
    user: dict = Depends(verificar_permiso_cobertura)
):
    """
    Obtiene el detalle de turnos de una instalación específica (para el pop-up).
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ci.turno as codigo_turno,
          ci.cargo,
          FORMAT_DATETIME('%H:%M', ci.her) as hora_entrada_planificada,
          FORMAT_DATETIME('%H:%M', ci.hsr) as hora_salida_planificada,
          
          -- Información del guardia planificado
          ci.rutrol as rut_planificado,
          
          -- Información del guardia que asistió
          ci.rutasi as rut_asistente,
          FORMAT_DATETIME('%H:%M', ci.entrada) as hora_entrada_real,
          FORMAT_DATETIME('%H:%M', ci.salida) as hora_salida_real,
          
          -- Estado
          ci.asistencia,
          ci.COB as estado_cobertura,
          ci.tvf as turno_extra,
          ci.relevo,
          ci.tipo,
          ci.motivoppc as motivo_incumplimiento,
          
          -- Indicador de retraso (si asistió)
          CASE 
            WHEN ci.asistencia = 1 AND ci.entrada > ci.her THEN 
              CONCAT('Retraso: ', CAST(DATETIME_DIFF(ci.entrada, ci.her, MINUTE) AS STRING), ' minutos')
            WHEN ci.asistencia = 1 THEN 'A tiempo'
            ELSE NULL
          END as puntualidad

        FROM `{TABLE_COBERTURA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.cliente_rol = ui.cliente_rol 
          AND ci.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
          AND ci.instalacion_rol = @instalacion_rol
        ORDER BY ci.turno, ci.her
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                bigquery.ScalarQueryParameter("instalacion_rol", "STRING", instalacion_rol)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        turnos = []
        for row in results:
            turnos.append({
                "codigo_turno": row.codigo_turno,
                "cargo": row.cargo,
                "hora_entrada_planificada": row.hora_entrada_planificada,
                "hora_salida_planificada": row.hora_salida_planificada,
                "rut_planificado": row.rut_planificado,
                "rut_asistente": row.rut_asistente,
                "hora_entrada_real": row.hora_entrada_real,
                "hora_salida_real": row.hora_salida_real,
                "asistio": bool(row.asistencia),
                "estado_cobertura": row.estado_cobertura,
                "turno_extra": row.turno_extra,
                "relevo": row.relevo,
                "tipo": row.tipo,
                "motivo_incumplimiento": row.motivo_incumplimiento,
                "puntualidad": row.puntualidad
            })
        
        return {
            "instalacion": instalacion_rol,
            "total_turnos": len(turnos),
            "turnos": turnos
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


# ============================================
# ENDPOINTS - COBERTURA HISTÓRICA
# ============================================

@app.get("/api/cobertura/historico/semanal")
async def get_cobertura_historica_semanal(
    dias: int = DIAS_HISTORICO_DEFAULT,
    user: dict = Depends(verificar_permiso_cobertura)
):
    """
    Obtiene la cobertura histórica acumulada por semana (últimos N días).
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ah.semana,
          ah.isoweek,
          ah.ano,
          MIN(ah.dia) as fecha_inicio,
          MAX(ah.dia) as fecha_fin,
          CONCAT(
            FORMAT_DATE('%d/%m', MIN(ah.dia)),
            ' - ',
            FORMAT_DATE('%d/%m', MAX(ah.dia))
          ) as periodo,
          
          SUM(ah.horas_planificadas) as horas_presupuestadas,
          SUM(ah.horas_entregadas) as horas_entregadas,
          SUM(ah.horas_planificadas) - SUM(ah.horas_entregadas) as horas_faltantes,
          
          ROUND(SAFE_DIVIDE(
            SUM(ah.horas_entregadas),
            SUM(ah.horas_planificadas)
          ) * 100, 2) as porcentaje_cumplimiento,
          
          COUNT(*) as total_registros,
          SUM(ah.asistencia) as total_asistencias,
          COUNT(*) - SUM(ah.asistencia) as total_ausencias,
          COUNT(DISTINCT ah.instalacion_rol) as num_instalaciones

        FROM `{TABLE_HISTORICO}` ah
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ah.cliente_rol = ui.cliente_rol 
          AND ah.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
          AND ah.dia >= DATE_SUB(CURRENT_DATE(), INTERVAL @dias DAY)
          AND ah.dia <= CURRENT_DATE()
        GROUP BY ah.semana, ah.isoweek, ah.ano
        ORDER BY ah.ano ASC, ah.isoweek ASC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                bigquery.ScalarQueryParameter("dias", "INT64", dias)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        semanas = []
        for row in results:
            porcentaje = float(row.porcentaje_cumplimiento) if row.porcentaje_cumplimiento else 0
            
            semanas.append({
                "semana": row.semana,
                "isoweek": row.isoweek,
                "ano": row.ano,
                "fecha_inicio": row.fecha_inicio.isoformat() if row.fecha_inicio else None,
                "fecha_fin": row.fecha_fin.isoformat() if row.fecha_fin else None,
                "periodo": row.periodo,
                "horas_presupuestadas": float(row.horas_presupuestadas) if row.horas_presupuestadas else 0,
                "horas_entregadas": float(row.horas_entregadas) if row.horas_entregadas else 0,
                "horas_faltantes": float(row.horas_faltantes) if row.horas_faltantes else 0,
                "porcentaje_cumplimiento": porcentaje,
                "estado_semaforo": calcular_estado_semaforo(porcentaje),
                "total_registros": row.total_registros,
                "total_asistencias": row.total_asistencias,
                "total_ausencias": row.total_ausencias,
                "num_instalaciones": row.num_instalaciones
            })
        
        return {
            "dias_consultados": dias,
            "total_semanas": len(semanas),
            "semanas": semanas
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@app.get("/api/cobertura/historico/por-instalacion")
async def get_cobertura_historica_por_instalacion(
    dias: int = DIAS_HISTORICO_DEFAULT,
    user: dict = Depends(verificar_permiso_cobertura)
):
    """
    Obtiene la cobertura histórica por instalación y semana.
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ah.semana,
          ah.isoweek,
          ah.ano,
          ah.instalacion_rol,
          ah.zona,
          
          CONCAT('Semana ', CAST(ah.isoweek AS STRING), ' - ', CAST(ah.ano AS STRING)) as periodo,
          
          SUM(ah.horas_planificadas) as horas_presupuestadas,
          SUM(ah.horas_entregadas) as horas_entregadas,
          SUM(ah.horas_planificadas) - SUM(ah.horas_entregadas) as horas_faltantes,
          
          ROUND(SAFE_DIVIDE(
            SUM(ah.horas_entregadas),
            SUM(ah.horas_planificadas)
          ) * 100, 2) as porcentaje_cumplimiento,
          
          COUNT(DISTINCT ah.rutrol) as guardias_planificados,
          SUM(ah.asistencia) as asistencias_registradas,
          
          COUNTIF(ah.tvf IS NOT NULL AND ah.tvf != '') as cantidad_turnos_extra

        FROM `{TABLE_HISTORICO}` ah
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ah.cliente_rol = ui.cliente_rol 
          AND ah.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
          AND ah.dia >= DATE_SUB(CURRENT_DATE(), INTERVAL @dias DAY)
          AND ah.dia <= CURRENT_DATE()
        GROUP BY ah.semana, ah.isoweek, ah.ano, ah.instalacion_rol, ah.zona
        ORDER BY ah.ano DESC, ah.isoweek DESC, ah.instalacion_rol
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                bigquery.ScalarQueryParameter("dias", "INT64", dias)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        datos = []
        for row in results:
            porcentaje = float(row.porcentaje_cumplimiento) if row.porcentaje_cumplimiento else 0
            
            datos.append({
                "semana": row.semana,
                "isoweek": row.isoweek,
                "ano": row.ano,
                "periodo": row.periodo,
                "instalacion_rol": row.instalacion_rol,
                "zona": row.zona,
                "horas_presupuestadas": float(row.horas_presupuestadas) if row.horas_presupuestadas else 0,
                "horas_entregadas": float(row.horas_entregadas) if row.horas_entregadas else 0,
                "horas_faltantes": float(row.horas_faltantes) if row.horas_faltantes else 0,
                "porcentaje_cumplimiento": porcentaje,
                "estado_semaforo": calcular_estado_semaforo(porcentaje),
                "guardias_planificados": row.guardias_planificados,
                "asistencias_registradas": row.asistencias_registradas,
                "cantidad_turnos_extra": row.cantidad_turnos_extra
            })
        
        return {
            "dias_consultados": dias,
            "total_registros": len(datos),
            "datos": datos
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


# ============================================
# ENDPOINTS - PUESTOS POR CUBRIR (PPC)
# ============================================

@app.get("/api/ppc/total")
async def get_ppc_total(user: dict = Depends(verificar_permiso_cobertura)):
    """
    Obtiene el total de Puestos Por Cubrir (PPC) del cliente.
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          COUNT(*) as total_ppc
        FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia` ppc
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ppc.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result())
        
        if not results:
            return {"total_ppc": 0}
        
        row = results[0]
        
        return {
            "total_ppc": row.total_ppc
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar PPC: {str(e)}")


@app.get("/api/ppc/todas-instalaciones")
async def get_ppc_todas_instalaciones(
    user: dict = Depends(verificar_permiso_cobertura)
):
    """
    Obtiene los Puestos Por Cubrir (PPC) de TODAS las instalaciones del usuario en una sola consulta.
    Optimizado para precarga.
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ppc.instalacion_rol,
          ppc.turno,
          FORMAT_DATETIME('%H:%M', ppc.her) as hora_entrada,
          FORMAT_DATETIME('%H:%M', ppc.hsr) as hora_salida,
          CONCAT(
            FORMAT_DATETIME('%H:%M', ppc.her),
            ' - ',
            FORMAT_DATETIME('%H:%M', ppc.hsr)
          ) as horario,
          COUNT(*) as cantidad_ppc
        FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia` ppc
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ppc.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        GROUP BY ppc.instalacion_rol, ppc.turno, ppc.her, ppc.hsr
        ORDER BY ppc.instalacion_rol, ppc.her, ppc.hsr
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        # Agrupar por instalación
        instalaciones_ppc = {}
        for row in results:
            instalacion = row.instalacion_rol
            
            if instalacion not in instalaciones_ppc:
                instalaciones_ppc[instalacion] = {
                    "instalacion": instalacion,
                    "total_ppc": 0,
                    "ppc_por_turno": []
                }
            
            instalaciones_ppc[instalacion]["ppc_por_turno"].append({
                "turno": row.turno,
                "hora_entrada": row.hora_entrada,
                "hora_salida": row.hora_salida,
                "horario": row.horario,
                "cantidad_ppc": row.cantidad_ppc
            })
            instalaciones_ppc[instalacion]["total_ppc"] += row.cantidad_ppc
        
        return {
            "total_instalaciones": len(instalaciones_ppc),
            "instalaciones": list(instalaciones_ppc.values())
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar PPC: {str(e)}")


@app.get("/api/ppc/por-instalacion/{instalacion_rol}")
async def get_ppc_por_instalacion(
    instalacion_rol: str,
    user: dict = Depends(verificar_permiso_cobertura)
):
    """
    Obtiene los Puestos Por Cubrir (PPC) de una instalación específica,
    agrupados por horario de turno (her - hsr).
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ppc.turno,
          FORMAT_DATETIME('%H:%M', ppc.her) as hora_entrada,
          FORMAT_DATETIME('%H:%M', ppc.hsr) as hora_salida,
          CONCAT(
            FORMAT_DATETIME('%H:%M', ppc.her),
            ' - ',
            FORMAT_DATETIME('%H:%M', ppc.hsr)
          ) as horario,
          COUNT(*) as cantidad_ppc
        FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia` ppc
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ppc.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
          AND ppc.instalacion_rol = @instalacion_rol
        GROUP BY ppc.turno, ppc.her, ppc.hsr
        ORDER BY ppc.her, ppc.hsr
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                bigquery.ScalarQueryParameter("instalacion_rol", "STRING", instalacion_rol)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        ppc_por_turno = []
        total_ppc = 0
        
        for row in results:
            ppc_por_turno.append({
                "turno": row.turno,
                "hora_entrada": row.hora_entrada,
                "hora_salida": row.hora_salida,
                "horario": row.horario,
                "cantidad_ppc": row.cantidad_ppc
            })
            total_ppc += row.cantidad_ppc
        
        return {
            "instalacion": instalacion_rol,
            "total_ppc": total_ppc,
            "ppc_por_turno": ppc_por_turno
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar PPC: {str(e)}")


# ============================================
# ENDPOINTS - CONTACTOS WHATSAPP
# ============================================

@app.get("/api/contactos/{instalacion_rol}")
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


# ============================================
# ENDPOINTS - AUTENTICACIÓN Y PERMISOS
# ============================================

@app.get("/api/auth/me")
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


# ============================================
# ENDPOINTS - MENSAJES WHATSAPP
# ============================================

@app.post("/api/whatsapp/enviar-mensaje")
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
            
            query_job = bq_client.query(query, job_config=job_config)
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
                
                bq_client.query(insert_query, job_config=job_config_insert).result()
                
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


@app.get("/api/whatsapp/mensajes-recibidos")
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
        
        query_job = bq_client.query(query, job_config=job_config)
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


# ============================================
# ENDPOINTS - ENCUESTAS
# ============================================

@app.get("/api/encuestas/mis-encuestas")
async def obtener_mis_encuestas(user_data: dict = Depends(verify_firebase_token)):
    """
    Obtiene todas las encuestas del usuario agrupadas por instalación.
    
    Reglas:
    - CLIENTE: Ve encuestas compartidas + sus encuestas individuales
    - WFSA: Ve todas las encuestas (compartidas + individuales de todos)
    
    Retorna:
    {
        "instalaciones": [
            {
                "cliente_rol": "INACAP",
                "instalacion_rol": "INACAP MAIPU",
                "instalacion_nombre": "INACAP MAIPU",
                "total_encuestas": 3,
                "respondidas": 2,
                "pendientes": 1,
                "fecha_vencimiento_proxima": "2024-10-15T23:59:59",
                "encuestas": [...]
            }
        ]
    }
    """
    try:
        user_email = user_data["email"]
        rol = user_data["rol_id"]
        
        # Determinar si es usuario WFSA (puede ver todas las encuestas individuales)
        es_wfsa = rol in ['ADMIN_WFSA', 'SUBGERENTE_WFSA', 'JEFE_WFSA', 'SUPERVISOR_WFSA']
        
        # Calcular períodos válidos basado en la nueva lógica:
        # - Las encuestas se generan cada 2 meses (meses pares: 2,4,6,8,10,12)
        # - Solo mostrar período en curso y período anterior
        # - Ejemplo: Si estamos en enero 2025, mostramos diciembre 2024 y octubre 2024
        
        from datetime import datetime
        ahora = datetime.now()
        year_actual = ahora.year
        month_actual = ahora.month
        
        # Encontrar el último mes par (período en curso)
        if month_actual % 2 == 0:  # Mes actual es par
            periodo_en_curso = f"{year_actual:04d}{month_actual:02d}"
            # Período anterior: restar 2 meses
            if month_actual == 2:
                periodo_anterior = f"{year_actual-1:04d}12"  # Diciembre del año anterior
            else:
                periodo_anterior = f"{year_actual:04d}{month_actual-2:02d}"
        else:  # Mes actual es impar
            # Buscar el último mes par
            if month_actual == 1:
                periodo_en_curso = f"{year_actual-1:04d}12"  # Diciembre anterior
                periodo_anterior = f"{year_actual-1:04d}10"  # Octubre anterior
            else:
                mes_par_anterior = month_actual - 1
                periodo_en_curso = f"{year_actual:04d}{mes_par_anterior:02d}"
                # Período anterior: restar 2 meses más
                if mes_par_anterior == 2:
                    periodo_anterior = f"{year_actual-1:04d}12"
                else:
                    periodo_anterior = f"{year_actual:04d}{mes_par_anterior-2:02d}"
        
        periodos_validos = [periodo_en_curso, periodo_anterior]
        
        # Crear condición IN para los períodos
        periodos_condition = "', '".join(periodos_validos)
        
        # Debug: mostrar períodos calculados (bimestral - solo meses pares)
        print(f"🔍 Períodos válidos (bimestral): {periodos_validos}")
        print(f"📅 Período en curso: {periodo_en_curso}, Período anterior: {periodo_anterior}")
        
        # Query para obtener encuestas del usuario
        if es_wfsa:
            # WFSA: Ver todas las encuestas de sus instalaciones
            query = f"""
            WITH mis_instalaciones AS (
                SELECT DISTINCT cliente_rol, instalacion_rol
                FROM `{TABLE_USUARIO_INST}`
                WHERE email_login = @user_email
                  AND puede_ver = TRUE
            )
            SELECT 
                s.encuesta_id,
                s.periodo,
                s.cliente_rol,
                s.instalacion_rol,
                s.modo,
                s.email_destinatario,
                s.estado,
                s.fecha_creacion,
                s.fecha_limite,
                s.respondido_por_email,
                s.respondido_por_nombre,
                s.tipo_respuesta,
                s.fecha_respuesta
            FROM `{TABLE_ENCUESTAS_SOLICITUDES}` s
            INNER JOIN mis_instalaciones mi
                ON s.cliente_rol = mi.cliente_rol
                AND s.instalacion_rol = mi.instalacion_rol
            WHERE s.periodo IN ('{periodos_condition}')
            ORDER BY s.instalacion_rol, s.modo, s.fecha_creacion DESC
            """
        else:
            # CLIENTE: Solo encuestas compartidas + sus individuales
            query = f"""
            WITH mis_instalaciones AS (
                SELECT DISTINCT cliente_rol, instalacion_rol
                FROM `{TABLE_USUARIO_INST}`
                WHERE email_login = @user_email
                  AND puede_ver = TRUE
            )
            SELECT 
                s.encuesta_id,
                s.periodo,
                s.cliente_rol,
                s.instalacion_rol,
                s.modo,
                s.email_destinatario,
                s.estado,
                s.fecha_creacion,
                s.fecha_limite,
                s.respondido_por_email,
                s.respondido_por_nombre,
                s.tipo_respuesta,
                s.fecha_respuesta
            FROM `{TABLE_ENCUESTAS_SOLICITUDES}` s
            INNER JOIN mis_instalaciones mi
                ON s.cliente_rol = mi.cliente_rol
                AND s.instalacion_rol = mi.instalacion_rol
            WHERE s.periodo IN ('{periodos_condition}')
              AND (
                  s.modo = 'compartida'
                  OR (s.modo = 'individual' AND s.email_destinatario = @user_email)
              )
            ORDER BY s.instalacion_rol, s.modo, s.fecha_creacion DESC
            """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
            ]
        )
        
        results = list(bq_client.query(query, job_config=job_config).result())
        
        # Agrupar por instalación
        instalaciones_dict = {}
        
        for row in results:
            inst_key = row.instalacion_rol
            
            # Inicializar instalación si no existe
            if inst_key not in instalaciones_dict:
                instalaciones_dict[inst_key] = {
                    "cliente_rol": row.cliente_rol,
                    "instalacion_rol": row.instalacion_rol,
                    "instalacion_nombre": row.instalacion_rol,
                    "total_encuestas": 0,
                    "respondidas": 0,
                    "pendientes": 0,
                    "fecha_vencimiento_proxima": None,
                    "encuestas": []
                }
            
            # Determinar permisos del usuario sobre esta encuesta
            puede_responder = False
            puede_ver_respuestas = False
            
            if row.estado == 'pendiente':
                if row.modo == 'compartida':
                    # Cualquiera puede responder encuestas compartidas pendientes
                    puede_responder = True
                elif row.modo == 'individual':
                    # Solo el destinatario puede responder encuestas individuales
                    puede_responder = (row.email_destinatario == user_email)
                
                # Actualizar fecha de vencimiento más próxima
                if (instalaciones_dict[inst_key]["fecha_vencimiento_proxima"] is None or
                    row.fecha_limite < instalaciones_dict[inst_key]["fecha_vencimiento_proxima"]):
                    instalaciones_dict[inst_key]["fecha_vencimiento_proxima"] = row.fecha_limite
            
            if row.estado == 'completada':
                # Puede ver respuestas si:
                # - Es su encuesta individual
                # - Es encuesta compartida de su instalación
                # - Es usuario WFSA
                if es_wfsa:
                    puede_ver_respuestas = True
                elif row.modo == 'compartida':
                    puede_ver_respuestas = True
                elif row.modo == 'individual' and row.email_destinatario == user_email:
                    puede_ver_respuestas = True
            
            # Agregar encuesta
            encuesta_data = {
                "encuesta_id": row.encuesta_id,
                "periodo": row.periodo,
                "modo": row.modo,
                "estado": row.estado,
                "email_destinatario": row.email_destinatario,
                "fecha_creacion": row.fecha_creacion.isoformat() if row.fecha_creacion else None,
                "fecha_limite": row.fecha_limite.isoformat() if row.fecha_limite else None,
                "respondido_por_email": row.respondido_por_email,
                "respondido_por_nombre": row.respondido_por_nombre,
                "tipo_respuesta": row.tipo_respuesta,
                "fecha_respuesta": row.fecha_respuesta.isoformat() if row.fecha_respuesta else None,
                "puede_responder": puede_responder,
                "puede_ver_respuestas": puede_ver_respuestas
            }
            
            instalaciones_dict[inst_key]["encuestas"].append(encuesta_data)
            instalaciones_dict[inst_key]["total_encuestas"] += 1
            
            if row.estado == 'completada':
                instalaciones_dict[inst_key]["respondidas"] += 1
            elif row.estado == 'pendiente':
                instalaciones_dict[inst_key]["pendientes"] += 1
        
        # Convertir a lista y formatear fechas
        instalaciones_list = []
        for inst in instalaciones_dict.values():
            if inst["fecha_vencimiento_proxima"]:
                inst["fecha_vencimiento_proxima"] = inst["fecha_vencimiento_proxima"].isoformat()
            instalaciones_list.append(inst)
        
        return {
            "success": True,
            "instalaciones": instalaciones_list
        }
        
    except Exception as e:
        print(f"❌ Error obteniendo encuestas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener encuestas: {str(e)}")


@app.get("/api/encuestas/{encuesta_id}/preguntas")
async def obtener_preguntas_encuesta(
    encuesta_id: str,
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Obtiene las preguntas de una encuesta específica.
    
    Retorna:
    {
        "encuesta": {...},
        "preguntas": [
            {
                "pregunta_id": "P001",
                "orden": 1,
                "texto_pregunta": "¿Cómo considera...",
                "tipo_respuesta": "escala_1_5",
                "requiere_comentario": true,
                "obligatoria": true
            }
        ]
    }
    """
    try:
        user_email = user_data["email"]
        
        # Verificar que el usuario tenga acceso a esta encuesta
        query_encuesta = f"""
        SELECT 
            s.*,
            ui.puede_ver
        FROM `{TABLE_ENCUESTAS_SOLICITUDES}` s
        LEFT JOIN `{TABLE_USUARIO_INST}` ui
            ON s.cliente_rol = ui.cliente_rol
            AND s.instalacion_rol = ui.instalacion_rol
            AND ui.email_login = @user_email
        WHERE s.encuesta_id = @encuesta_id
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("encuesta_id", "STRING", encuesta_id),
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
            ]
        )
        
        encuesta_result = list(bq_client.query(query_encuesta, job_config=job_config).result())
        
        if not encuesta_result:
            raise HTTPException(status_code=404, detail="Encuesta no encontrada")
        
        encuesta = encuesta_result[0]
        
        if not encuesta.puede_ver:
            raise HTTPException(status_code=403, detail="No tiene acceso a esta encuesta")
        
        # Obtener preguntas activas
        query_preguntas = f"""
        SELECT 
            pregunta_id,
            orden,
            texto_pregunta,
            tipo_respuesta,
            requiere_comentario,
            obligatoria,
            categoria
        FROM `{TABLE_ENCUESTAS_PREGUNTAS}`
        WHERE activa = TRUE
        ORDER BY orden ASC
        """
        
        preguntas_result = list(bq_client.query(query_preguntas).result())
        
        preguntas = []
        for row in preguntas_result:
            preguntas.append({
                "pregunta_id": row.pregunta_id,
                "orden": row.orden,
                "texto_pregunta": row.texto_pregunta,
                "tipo_respuesta": row.tipo_respuesta,
                "requiere_comentario": row.requiere_comentario,
                "obligatoria": row.obligatoria,
                "categoria": row.categoria
            })
        
        return {
            "success": True,
            "encuesta": {
                "encuesta_id": encuesta.encuesta_id,
                "periodo": encuesta.periodo,
                "instalacion_rol": encuesta.instalacion_rol,
                "modo": encuesta.modo,
                "estado": encuesta.estado,
                "fecha_limite": encuesta.fecha_limite.isoformat() if encuesta.fecha_limite else None
            },
            "preguntas": preguntas
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error obteniendo preguntas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener preguntas: {str(e)}")


@app.post("/api/encuestas/{encuesta_id}/responder")
async def responder_encuesta(
    encuesta_id: str,
    request: RespuestaEncuestaRequest,
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Guarda las respuestas de una encuesta.
    
    Validaciones:
    - Solo el destinatario puede responder encuestas individuales
    - Solo la primera respuesta cuenta para encuestas compartidas
    - No se puede responder encuestas vencidas
    
    Body:
    {
        "respuestas": [
            {
                "pregunta_id": "P001",
                "respuesta_valor": "5",
                "comentario": "Excelente servicio"
            },
            {
                "pregunta_id": "P009",
                "respuesta_texto": "Todo muy bien"
            }
        ]
    }
    """
    try:
        user_email = user_data["email"]
        user_nombre = user_data.get("nombre_completo", user_email)
        
        # Obtener encuesta
        query_encuesta = f"""
        SELECT *
        FROM `{TABLE_ENCUESTAS_SOLICITUDES}`
        WHERE encuesta_id = @encuesta_id
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("encuesta_id", "STRING", encuesta_id),
            ]
        )
        
        encuesta_result = list(bq_client.query(query_encuesta, job_config=job_config).result())
        
        if not encuesta_result:
            raise HTTPException(status_code=404, detail="Encuesta no encontrada")
        
        encuesta = encuesta_result[0]
        
        # Validación 1: Encuesta individual - solo el destinatario puede responder
        if encuesta.modo == 'individual' and encuesta.email_destinatario != user_email:
            raise HTTPException(
                status_code=403,
                detail="Esta encuesta es individual y solo puede ser respondida por el destinatario"
            )
        
        # Validación 2: Encuesta compartida - solo el primero puede responder
        if encuesta.modo == 'compartida' and encuesta.estado == 'completada':
            raise HTTPException(
                status_code=400,
                detail=f"Esta encuesta ya fue respondida por {encuesta.respondido_por_nombre}"
            )
        
        # Validación 3: Encuesta vencida
        from datetime import datetime, timezone
        ahora_utc = datetime.now(timezone.utc)
        if encuesta.fecha_limite < ahora_utc:
            raise HTTPException(
                status_code=400,
                detail="Esta encuesta ya expiró"
            )
        
        # Validación 4: Ya fue respondida por este usuario
        if encuesta.estado == 'completada' and encuesta.respondido_por_email == user_email:
            raise HTTPException(
                status_code=400,
                detail="Ya ha respondido esta encuesta"
            )
        
        # Preparar respuestas para insertar
        ahora = datetime.now(timezone.utc)
        respuestas_para_insertar = []
        
        for resp in request.respuestas:
            respuesta_data = {
                "respuesta_id": str(uuid.uuid4()),
                "encuesta_id": encuesta_id,
                "pregunta_id": resp.get("pregunta_id"),
                "respuesta_valor": resp.get("respuesta_valor"),  # Para escala 1-5 o texto libre
                "comentario_adicional": resp.get("comentario"),
                "fecha_respuesta": ahora.isoformat()  # Convertir a string ISO format
            }
            respuestas_para_insertar.append(respuesta_data)
        
        # Insertar respuestas en BigQuery
        errors = bq_client.insert_rows_json(TABLE_ENCUESTAS_RESPUESTAS, respuestas_para_insertar)
        
        if errors:
            raise HTTPException(
                status_code=500,
                detail=f"Error al guardar respuestas: {errors}"
            )
        
        # Actualizar estado de la encuesta
        query_update = f"""
        UPDATE `{TABLE_ENCUESTAS_SOLICITUDES}`
        SET estado = 'completada',
            respondido_por_email = @user_email,
            respondido_por_nombre = @user_nombre,
            tipo_respuesta = @tipo_respuesta,
            fecha_respuesta = CURRENT_TIMESTAMP()
        WHERE encuesta_id = @encuesta_id
          AND estado = 'pendiente'
        """
        
        # Determinar tipo de respuesta (cliente o WFSA que respondió por cliente)
        rol_usuario = user_data["rol_id"]
        tipo_respuesta = 'cliente' if rol_usuario == 'CLIENTE' else 'wfsa'
        
        job_config_update = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("encuesta_id", "STRING", encuesta_id),
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                bigquery.ScalarQueryParameter("user_nombre", "STRING", user_nombre),
                bigquery.ScalarQueryParameter("tipo_respuesta", "STRING", tipo_respuesta),
            ]
        )
        
        bq_client.query(query_update, job_config=job_config_update).result()
        
        return {
            "success": True,
            "message": "Encuesta respondida exitosamente",
            "encuesta_id": encuesta_id,
            "respuestas_guardadas": len(respuestas_para_insertar)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error al responder encuesta: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al responder encuesta: {str(e)}")


@app.get("/api/encuestas/{encuesta_id}/respuestas")
async def ver_respuestas_encuesta(
    encuesta_id: str,
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Obtiene las respuestas de una encuesta completada.
    
    Permisos:
    - WFSA: Puede ver todas las respuestas
    - CLIENTE: Solo sus encuestas individuales o compartidas de su instalación
    """
    try:
        user_email = user_data["email"]
        rol = user_data["rol_id"]
        es_wfsa = rol in ['ADMIN_WFSA', 'SUBGERENTE_JEFE_WFSA', 'SUPERVISOR_WFSA']
        
        # Obtener encuesta
        query_encuesta = f"""
        SELECT *
        FROM `{TABLE_ENCUESTAS_SOLICITUDES}`
        WHERE encuesta_id = @encuesta_id
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("encuesta_id", "STRING", encuesta_id),
            ]
        )
        
        encuesta_result = list(bq_client.query(query_encuesta, job_config=job_config).result())
        
        if not encuesta_result:
            raise HTTPException(status_code=404, detail="Encuesta no encontrada")
        
        encuesta = encuesta_result[0]
        
        # Validar permisos
        if not es_wfsa:
            # Cliente solo puede ver si es compartida o su individual
            if encuesta.modo == 'individual' and encuesta.email_destinatario != user_email:
                raise HTTPException(status_code=403, detail="No tiene permiso para ver estas respuestas")
        
        if encuesta.estado != 'completada':
            raise HTTPException(status_code=400, detail="Esta encuesta aún no ha sido respondida")
        
        # Obtener respuestas
        query_respuestas = f"""
        SELECT 
            r.respuesta_id,
            r.pregunta_id,
            r.respuesta_valor,
            r.comentario_adicional,
            r.fecha_respuesta,
            p.texto_pregunta,
            p.tipo_respuesta,
            p.orden
        FROM `{TABLE_ENCUESTAS_RESPUESTAS}` r
        INNER JOIN `{TABLE_ENCUESTAS_PREGUNTAS}` p
            ON r.pregunta_id = p.pregunta_id
        WHERE r.encuesta_id = @encuesta_id
        ORDER BY p.orden ASC
        """
        
        respuestas_result = list(bq_client.query(query_respuestas, job_config=job_config).result())
        
        respuestas = []
        for row in respuestas_result:
            respuestas.append({
                "pregunta_id": row.pregunta_id,
                "texto_pregunta": row.texto_pregunta,
                "tipo_respuesta": row.tipo_respuesta,
                "respuesta_valor": row.respuesta_valor,
                "comentario": row.comentario_adicional,
                "orden": row.orden
            })
        
        return {
            "success": True,
            "encuesta": {
                "encuesta_id": encuesta.encuesta_id,
                "periodo": encuesta.periodo,
                "instalacion_rol": encuesta.instalacion_rol,
                "modo": encuesta.modo,
                "respondido_por": encuesta.respondido_por_nombre,
                "fecha_respuesta": encuesta.fecha_respuesta.isoformat() if encuesta.fecha_respuesta else None
            },
            "respuestas": respuestas
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error al obtener respuestas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener respuestas: {str(e)}")


# ============================================
# ENDPOINT: ACTUALIZAR FCM TOKEN
# ============================================

class FCMTokenRequest(BaseModel):
    fcm_token: str

@app.post("/api/fcm/update-token")
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


# ============================================
# HEALTH CHECK
# ============================================

@app.get("/")
async def root():
    """Endpoint raíz para verificar que el servicio está funcionando"""
    return {
        "message": "WFSA BigQuery API funcionando",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """Endpoint de salud para verificar que el servicio está funcionando"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "WFSA BigQuery API",
        "project_id": PROJECT_ID
    }

# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    print(f"🚀 Iniciando servidor en puerto {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
