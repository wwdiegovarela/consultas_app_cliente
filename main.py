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

# Cliente de BigQuery
bq_client = bigquery.Client()

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
          SUM(CASE WHEN asistencia = 1 THEN 1 ELSE 0 END) as turnos_cubiertos,
          COUNT(*) - SUM(CASE WHEN asistencia = 1 THEN 1 ELSE 0 END) as turnos_descubiertos,
          
          -- Porcentaje general
          ROUND(SAFE_DIVIDE(
            SUM(CASE WHEN asistencia = 1 THEN 1 ELSE 0 END),
            COUNT(*)
          ) * 100, 2) as porcentaje_cobertura_general,
          
          -- Timestamp de actualización
          MAX(hora_actual) as ultima_actualizacion

        FROM `{TABLE_COBERTURA}` ci
        WHERE ci.cliente_rol = (
            SELECT cliente_rol 
            FROM `{TABLE_USUARIOS}` 
            WHERE email_login = @user_email
            LIMIT 1
          )
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
                "ultima_actualizacion": None
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
            "proxima_actualizacion": (row.ultima_actualizacion + timedelta(minutes=5)).isoformat() if row.ultima_actualizacion else None
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
          COUNT(DISTINCT ci.turno) as cantidad_turnos_activos

        FROM `{TABLE_COBERTURA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.cliente_rol = ui.cliente_rol 
          AND ci.instalacion_rol = ui.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        GROUP BY ci.instalacion_rol, ci.zona
        ORDER BY guardias_ausentes DESC, porcentaje_cobertura ASC, ci.instalacion_rol
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ]
        )
        
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()
        
        instalaciones = []
        for row in results:
            porcentaje = float(row.porcentaje_cobertura) if row.porcentaje_cobertura else 0
            
            instalaciones.append({
                "instalacion_rol": row.instalacion_rol,
                "zona": row.zona,
                "total_guardias_requeridos": row.total_guardias_requeridos,
                "guardias_presentes": row.guardias_presentes,
                "guardias_ausentes": row.guardias_ausentes,
                "porcentaje_cobertura": porcentaje,
                "estado_semaforo": calcular_estado_semaforo(porcentaje),
                "turnos_cubiertos": row.turnos_cubiertos,
                "turnos_descubiertos": row.turnos_descubiertos,
                "cantidad_turnos_activos": row.cantidad_turnos_activos
            })
        
        return {
            "total_instalaciones": len(instalaciones),
            "instalaciones": instalaciones
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
        INNER JOIN `{TABLE_USUARIOS}` u ON ah.cliente_rol = u.cliente_rol
        WHERE u.email_login = @user_email
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
        FROM `{TABLE_PPC}` ppc
        WHERE ppc.nombrerol = (
            SELECT cliente_rol 
            FROM `{TABLE_USUARIOS}` 
            WHERE email_login = @user_email
            LIMIT 1
          )
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
        FROM `{TABLE_PPC}` ppc
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ppc.nombrerol = ui.cliente_rol 
          AND ppc.instalacion_rol = ui.instalacion_rol
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
          ON ui.instalacion_rol = ic.instalacion_rol
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
# MAIN
# ============================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
