"""
Endpoints de Puestos Por Cubrir (PPC)
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from dependencies import verificar_permiso_cobertura, get_bq_client
from config import PROJECT_ID, TABLE_USUARIO_INST

router = APIRouter()


@router.get("/api/ppc/total")
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
        
        query_job = get_bq_client().query(query, job_config=job_config)
        results = list(query_job.result())
        
        if not results:
            return {"total_ppc": 0}
        
        row = results[0]
        
        return {
            "total_ppc": row.total_ppc
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar PPC: {str(e)}")


@router.get("/api/ppc/todas-instalaciones")
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
        
        query_job = get_bq_client().query(query, job_config=job_config)
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


@router.get("/api/ppc/por-instalacion/{instalacion_rol}")
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
        
        query_job = get_bq_client().query(query, job_config=job_config)
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

