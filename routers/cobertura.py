"""
Endpoints de cobertura - Instantánea e histórica
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from datetime import timedelta
from dependencies import verify_firebase_token, verificar_permiso_cobertura, get_bq_client
from config import (
    PROJECT_ID, DATASET_REPORTES, DATASET_APP,
    TABLE_COBERTURA, TABLE_COBERTURA_AGREGADA,
    TABLE_HISTORICO, TABLE_USUARIO_INST,
    DIAS_HISTORICO_DEFAULT
)
from utils.semaforo import calcular_estado_semaforo

router = APIRouter()


@router.get("/api/cobertura/instantanea/general")
async def get_cobertura_general(user: dict = Depends(verificar_permiso_cobertura)):
    """
    Obtiene el % de cobertura general del cliente (todos los turnos activos ahora).
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          SUM(ci.total_guardias_requeridos) AS total_turnos_activos,
          SUM(ci.guardias_presentes) AS turnos_cubiertos,
          SUM(ci.total_guardias_requeridos) - SUM(ci.guardias_presentes) AS turnos_descubiertos,
          ROUND(
            SAFE_DIVIDE(
              SUM(ci.guardias_presentes),
              NULLIF(SUM(ci.total_guardias_requeridos), 0)
            ) * 100,
            2
          ) AS porcentaje_cobertura_general,
          MAX(ci.ultima_actualizacion) AS ultima_actualizacion,
          ARRAY_AGG(DISTINCT ci.empresa IGNORE NULLS) AS empresas,
          (
            SELECT COUNT(*)
            FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia` ppc
            INNER JOIN `{TABLE_USUARIO_INST}` ui2 
              ON ppc.instalacion_rol = ui2.instalacion_rol
            WHERE ui2.email_login = @user_email
              AND ui2.puede_ver = TRUE
          ) AS total_ppc
        FROM `{TABLE_COBERTURA_AGREGADA}` ci
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
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = list(query_job.result())
        
        if not results or results[0].total_turnos_activos == 0:
            return {
                "total_turnos_activos": 0,
                "turnos_cubiertos": 0,
                "turnos_descubiertos": 0,
                "porcentaje_cobertura_general": 0,
                "estado_semaforo": "GRIS",
                "ultima_actualizacion": None,
                "total_ppc": 0,
                "empresas": []
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
            "total_ppc": row.total_ppc if hasattr(row, 'total_ppc') else 0,
            "empresas": list(row.empresas) if hasattr(row, "empresas") and row.empresas is not None else []
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@router.get("/api/cobertura/instantanea/por-instalacion")
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
          ci.empresa,
          SUM(ci.total_guardias_requeridos) AS total_guardias_requeridos,
          SUM(ci.guardias_presentes) AS guardias_presentes,
          SUM(ci.total_guardias_requeridos) - SUM(ci.guardias_presentes) AS guardias_ausentes,
          ROUND(
            SAFE_DIVIDE(
              SUM(ci.guardias_presentes),
              NULLIF(SUM(ci.total_guardias_requeridos), 0)
            ) * 100,
            2
          ) AS porcentaje_cobertura,
          SUM(ci.turnos_cubiertos) AS turnos_cubiertos,
          SUM(ci.turnos_descubiertos) AS turnos_descubiertos,
          COALESCE(ppc.cantidad_ppc, 0) AS ppc,
          CASE 
            WHEN faceid.nombre IS NOT NULL THEN TRUE 
            ELSE FALSE 
          END AS tiene_faceid,
          faceid.numero AS faceid_numero,
          faceid.ult_conexion AS faceid_ultima_conexion
        FROM `{TABLE_COBERTURA_AGREGADA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.cliente_rol = ui.cliente_rol 
          AND ci.instalacion_rol = ui.instalacion_rol
        LEFT JOIN `{PROJECT_ID}.{DATASET_REPORTES}.cr_equipos_faceid` faceid
          ON ci.instalacion_rol = faceid.nombre
        LEFT JOIN (
          SELECT instalacion_rol, COUNT(*) AS cantidad_ppc
          FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia`
          GROUP BY instalacion_rol
        ) ppc ON ci.instalacion_rol = ppc.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        GROUP BY ci.instalacion_rol, ci.zona, ci.cliente_rol, ci.empresa, faceid.nombre, faceid.numero, faceid.ult_conexion, ppc.cantidad_ppc
        ORDER BY guardias_ausentes DESC, porcentaje_cobertura ASC, ci.instalacion_rol
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ],
            use_query_cache=True,
            use_legacy_sql=False,
            job_timeout_ms=300000,  # 5 minutos
        )
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = query_job.result()
        
        instalaciones = []
        for row in results:
            porcentaje = float(row.porcentaje_cobertura) if row.porcentaje_cobertura else 0
            
            instalaciones.append({
                "instalacion_rol": row.instalacion_rol,
                "zona": row.zona,
                "cliente_rol": row.cliente_rol,
                "empresa": row.empresa,
                "total_guardias_requeridos": row.total_guardias_requeridos,
                "guardias_presentes": row.guardias_presentes,
                "guardias_ausentes": row.guardias_ausentes,
                "porcentaje_cobertura": porcentaje,
                "estado_semaforo": calcular_estado_semaforo(porcentaje),
                "turnos_cubiertos": row.turnos_cubiertos,
                "turnos_descubiertos": row.turnos_descubiertos,
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


@router.get("/api/cobertura/instantanea/por-instalacion-fast")
async def get_cobertura_por_instalacion_fast(user: dict = Depends(verify_firebase_token)):
    """
    Versión optimizada del endpoint de instalaciones (v1 - Legacy).
    - Elimina subqueries
    - Usa JOINs optimizados
    - Incluye caché de BigQuery
    - NO incluye tipo_de_servicio (para compatibilidad con apps antiguas)
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ci.instalacion_rol,
          ci.zona,
          ci.cliente_rol,
          ci.empresa,
          SUM(ci.total_guardias_requeridos) AS total_guardias_requeridos,
          SUM(ci.guardias_presentes) AS guardias_presentes,
          SUM(ci.total_guardias_requeridos) - SUM(ci.guardias_presentes) AS guardias_ausentes,
          ROUND(
            SAFE_DIVIDE(
              SUM(ci.guardias_presentes),
              NULLIF(SUM(ci.total_guardias_requeridos), 0)
            ) * 100,
            1
          ) AS porcentaje_cobertura,
          SUM(ci.turnos_cubiertos) AS turnos_cubiertos,
          SUM(ci.turnos_descubiertos) AS turnos_descubiertos,
          COALESCE(ppc.cantidad_ppc, 0) AS cantidad_ppc_total,
          CASE WHEN faceid.nombre IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_faceid,
          faceid.numero AS faceid_numero,
          faceid.ult_conexion AS faceid_ultima_conexion
        FROM `{TABLE_COBERTURA_AGREGADA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.instalacion_rol = ui.instalacion_rol
         AND ci.cliente_rol = ui.cliente_rol
        LEFT JOIN `{PROJECT_ID}.{DATASET_REPORTES}.cr_equipos_faceid` faceid
          ON ci.instalacion_rol = faceid.nombre
        LEFT JOIN (
          SELECT instalacion_rol, COUNT(*) AS cantidad_ppc
          FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia`
          GROUP BY instalacion_rol
        ) ppc ON ci.instalacion_rol = ppc.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        GROUP BY ci.instalacion_rol, ci.zona, ci.cliente_rol, ci.empresa, faceid.nombre, faceid.numero, faceid.ult_conexion, ppc.cantidad_ppc
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
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = query_job.result()
        
        instalaciones = []
        for row in results:
            porcentaje = float(row.porcentaje_cobertura) if row.porcentaje_cobertura else 0
            
            instalaciones.append({
                "instalacion_rol": row.instalacion_rol,
                "zona": row.zona,
                "cliente_rol": row.cliente_rol,
                "empresa": row.empresa,
                "total_guardias_requeridos": row.total_guardias_requeridos,
                "guardias_presentes": row.guardias_presentes,
                "guardias_ausentes": row.guardias_ausentes,
                "porcentaje_cobertura": porcentaje,
                "estado_semaforo": calcular_estado_semaforo(porcentaje),
                "ppc": row.cantidad_ppc_total,
                "tiene_faceid": bool(row.tiene_faceid),
                "faceid_numero": row.faceid_numero if hasattr(row, "faceid_numero") else None,
                "faceid_ultima_conexion": row.faceid_ultima_conexion.isoformat() if getattr(row, "faceid_ultima_conexion", None) else None,
            })
        
        return {
            "total_instalaciones": len(instalaciones),
            "instalaciones": instalaciones,
            "optimized": True
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@router.get("/api/cobertura/instantanea/por-instalacion-fast/v2")
async def get_cobertura_por_instalacion_fast_v2(user: dict = Depends(verify_firebase_token)):
    """
    Versión v2 del endpoint optimizado con soporte para tipo_de_servicio.
    - Incluye el campo tipo_de_servicio
    - Puede retornar múltiples filas por instalación (una por cada tipo_de_servicio)
    - Mantiene compatibilidad con el frontend que agrupa por instalacion_rol
    """
    user_email = user["email"]
    
    try:
        query = f"""
        SELECT 
          ci.instalacion_rol,
          ci.zona,
          ci.cliente_rol,
          ci.empresa,
          ci.tipo_de_servicio,
          ci.total_guardias_requeridos,
          ci.guardias_presentes,
          ci.total_guardias_requeridos - ci.guardias_presentes AS guardias_ausentes,
          ROUND(
            SAFE_DIVIDE(
              ci.guardias_presentes,
              NULLIF(ci.total_guardias_requeridos, 0)
            ) * 100,
            1
          ) AS porcentaje_cobertura,
          ci.turnos_cubiertos,
          ci.turnos_descubiertos,
          COALESCE(ppc.cantidad_ppc, 0) AS cantidad_ppc_total,
          CASE WHEN faceid.nombre IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_faceid,
          faceid.numero AS faceid_numero,
          faceid.ult_conexion AS faceid_ultima_conexion

        FROM `{TABLE_COBERTURA_AGREGADA}` ci
        INNER JOIN `{TABLE_USUARIO_INST}` ui 
          ON ci.cliente_rol = ui.cliente_rol 
          AND ci.instalacion_rol = ui.instalacion_rol
        LEFT JOIN `{PROJECT_ID}.{DATASET_REPORTES}.cr_equipos_faceid` faceid
          ON ci.instalacion_rol = faceid.nombre
        LEFT JOIN (
          SELECT instalacion_rol, COUNT(*) AS cantidad_ppc
          FROM `{PROJECT_ID}.cr_vistas_reporte.cr_ppc_dia`
          GROUP BY instalacion_rol
        ) ppc ON ci.instalacion_rol = ppc.instalacion_rol
        WHERE ui.email_login = @user_email
          AND ui.puede_ver = TRUE
        ORDER BY ci.instalacion_rol, ci.tipo_de_servicio, guardias_ausentes DESC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email)
            ],
            use_query_cache=True,
            use_legacy_sql=False,
            job_timeout_ms=120000,  # 2 minutos
        )
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = query_job.result()
        
        instalaciones = []
        for row in results:
            porcentaje = float(row.porcentaje_cobertura) if row.porcentaje_cobertura else 0
            
            instalaciones.append({
                "instalacion_rol": row.instalacion_rol,
                "zona": row.zona,
                "cliente_rol": row.cliente_rol,
                "empresa": row.empresa,
                "tipo_de_servicio": row.tipo_de_servicio if row.tipo_de_servicio else "1 Servicio",
                "total_guardias_requeridos": row.total_guardias_requeridos,
                "guardias_presentes": row.guardias_presentes,
                "guardias_ausentes": row.guardias_ausentes,
                "porcentaje_cobertura": porcentaje,
                "estado_semaforo": calcular_estado_semaforo(porcentaje),
                "ppc": row.cantidad_ppc_total,
                "tiene_faceid": bool(row.tiene_faceid),
                "faceid_numero": row.faceid_numero if hasattr(row, "faceid_numero") else None,
                "faceid_ultima_conexion": row.faceid_ultima_conexion.isoformat() if getattr(row, "faceid_ultima_conexion", None) else None,
            })
        
        return {
            "total_instalaciones": len(instalaciones),
            "instalaciones": instalaciones,
            "version": "v2",
            "optimized": True
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@router.get("/api/cobertura/instantanea/detalle-todas")
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
          ci.empresa,
          ci.turno as codigo_turno,
          ci.cargo,
          FORMAT_DATETIME('%H:%M', ci.her) as hora_entrada_planificada,
          FORMAT_DATETIME('%H:%M', ci.hsr) as hora_salida_planificada,
          
          -- Información del guardia planificado
          ci.rutrol as rut_planificado,
          ci.nombrerol as nombre_planificado,
          -- Información del guardia que asistió
          ci.rutasi as rut_asistente,
          TRIM(ARRAY_REVERSE(SPLIT(ci.relevo, ' | '))[OFFSET(0)]) as nombre_asistente,
          FORMAT_DATETIME('%H:%M', ci.entrada) as hora_entrada_real,
          FORMAT_DATETIME('%H:%M', ci.salida) as hora_salida_real,
          
          -- Estado
          ci.asistencia,
          ci.COB as estado_cobertura,
          ci.tvf as turno_extra,
          
          ci.tipo,
          ci.tipo_de_servicio,
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
          ON ci.instalacion_rol = ui.instalacion_rol
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
        query_job_turnos = get_bq_client().query(query_turnos, job_config=job_config)
        query_job_ppc = get_bq_client().query(query_ppc, job_config=job_config)
        
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
                    "empresa": row.empresa,
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
                "nombre_planificado": row.nombre_planificado,
                "nombre_asistente": row.nombre_asistente,
                "hora_entrada_real": row.hora_entrada_real,
                "hora_salida_real": row.hora_salida_real,
                "asistio": bool(row.asistencia),
                "estado_cobertura": row.estado_cobertura,
                "turno_extra": row.turno_extra,
                "tipo": row.tipo,
                "tipo_de_servicio": row.tipo_de_servicio if hasattr(row, 'tipo_de_servicio') and row.tipo_de_servicio else (row.tipo if hasattr(row, 'tipo') and row.tipo else "1 Servicio"),
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


@router.get("/api/cobertura/instantanea/detalle/{instalacion_rol}")
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
          ci.empresa,
          
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
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = query_job.result()
        
        turnos = []
        empresa = None
        for row in results:
            if empresa is None and hasattr(row, "empresa"):
                empresa = row.empresa
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
            "empresa": empresa,
            "total_turnos": len(turnos),
            "turnos": turnos
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar BigQuery: {str(e)}")


@router.get("/api/cobertura/historico/semanal")
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
        
        query_job = get_bq_client().query(query, job_config=job_config)
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


@router.get("/api/cobertura/historico/por-instalacion")
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
          ah.empresa,
          
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
        GROUP BY ah.semana, ah.isoweek, ah.ano, ah.instalacion_rol, ah.zona, ah.empresa
        ORDER BY ah.ano DESC, ah.isoweek DESC, ah.instalacion_rol
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_email", "STRING", user_email),
                bigquery.ScalarQueryParameter("dias", "INT64", dias)
            ]
        )
        
        query_job = get_bq_client().query(query, job_config=job_config)
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
                "empresa": row.empresa,
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

