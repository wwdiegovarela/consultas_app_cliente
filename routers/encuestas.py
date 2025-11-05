"""
Endpoints de encuestas
"""
from fastapi import APIRouter, HTTPException, Depends
from google.cloud import bigquery
from datetime import datetime, timezone
import uuid
from dependencies import verify_firebase_token, get_bq_client
from models.schemas import RespuestaEncuestaRequest
from config import (
    TABLE_ENCUESTAS_SOLICITUDES, TABLE_ENCUESTAS_PREGUNTAS,
    TABLE_ENCUESTAS_RESPUESTAS, TABLE_USUARIO_INST
)

router = APIRouter()


@router.get("/api/encuestas/mis-encuestas")
async def obtener_mis_encuestas(user_data: dict = Depends(verify_firebase_token)):
    """
    Obtiene todas las encuestas del usuario agrupadas por instalación.
    
    Reglas:
    - CLIENTE: Ve encuestas compartidas + sus encuestas individuales
    - WFSA: Ve todas las encuestas (compartidas + individuales de todos)
    """
    try:
        user_email = user_data["email"]
        rol = user_data["rol_id"]
        
        # Determinar si es usuario WFSA
        es_wfsa = rol in ['ADMIN_WFSA', 'SUBGERENTE_WFSA', 'JEFE_WFSA', 'SUPERVISOR_WFSA']
        
        # Calcular períodos válidos (bimestral - solo meses pares)
        ahora = datetime.now()
        year_actual = ahora.year
        month_actual = ahora.month
        
        if month_actual % 2 == 0:
            periodo_en_curso = f"{year_actual:04d}{month_actual:02d}"
            if month_actual == 2:
                periodo_anterior = f"{year_actual-1:04d}12"
            else:
                periodo_anterior = f"{year_actual:04d}{month_actual-2:02d}"
        else:
            if month_actual == 1:
                periodo_en_curso = f"{year_actual-1:04d}12"
                periodo_anterior = f"{year_actual-1:04d}10"
            else:
                mes_par_anterior = month_actual - 1
                periodo_en_curso = f"{year_actual:04d}{mes_par_anterior:02d}"
                if mes_par_anterior == 2:
                    periodo_anterior = f"{year_actual-1:04d}12"
                else:
                    periodo_anterior = f"{year_actual:04d}{mes_par_anterior-2:02d}"
        
        periodos_validos = [periodo_en_curso, periodo_anterior]
        periodos_condition = "', '".join(periodos_validos)
        
        print(f"🔍 Períodos válidos (bimestral): {periodos_validos}")
        
        # Query para obtener encuestas
        if es_wfsa:
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
        
        results = list(get_bq_client().query(query, job_config=job_config).result())
        
        # Agrupar por instalación
        instalaciones_dict = {}
        
        for row in results:
            inst_key = row.instalacion_rol
            
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
            
            puede_responder = False
            puede_ver_respuestas = False
            
            if row.estado == 'pendiente':
                if row.modo == 'compartida':
                    puede_responder = True
                elif row.modo == 'individual':
                    puede_responder = (row.email_destinatario == user_email)
                
                if (instalaciones_dict[inst_key]["fecha_vencimiento_proxima"] is None or
                    row.fecha_limite < instalaciones_dict[inst_key]["fecha_vencimiento_proxima"]):
                    instalaciones_dict[inst_key]["fecha_vencimiento_proxima"] = row.fecha_limite
            
            if row.estado == 'completada':
                if es_wfsa:
                    puede_ver_respuestas = True
                elif row.modo == 'compartida':
                    puede_ver_respuestas = True
                elif row.modo == 'individual' and row.email_destinatario == user_email:
                    puede_ver_respuestas = True
            
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


@router.get("/api/encuestas/{encuesta_id}/preguntas")
async def obtener_preguntas_encuesta(
    encuesta_id: str,
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Obtiene las preguntas de una encuesta específica.
    """
    try:
        user_email = user_data["email"]
        
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
        
        encuesta_result = list(get_bq_client().query(query_encuesta, job_config=job_config).result())
        
        if not encuesta_result:
            raise HTTPException(status_code=404, detail="Encuesta no encontrada")
        
        encuesta = encuesta_result[0]
        
        if not encuesta.puede_ver:
            raise HTTPException(status_code=403, detail="No tiene acceso a esta encuesta")
        
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
        
        preguntas_result = list(get_bq_client().query(query_preguntas).result())
        
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


@router.post("/api/encuestas/{encuesta_id}/responder")
async def responder_encuesta(
    encuesta_id: str,
    request: RespuestaEncuestaRequest,
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Guarda las respuestas de una encuesta.
    """
    try:
        user_email = user_data["email"]
        user_nombre = user_data.get("nombre_completo", user_email)
        
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
        
        encuesta_result = list(get_bq_client().query(query_encuesta, job_config=job_config).result())
        
        if not encuesta_result:
            raise HTTPException(status_code=404, detail="Encuesta no encontrada")
        
        encuesta = encuesta_result[0]
        
        if encuesta.modo == 'individual' and encuesta.email_destinatario != user_email:
            raise HTTPException(
                status_code=403,
                detail="Esta encuesta es individual y solo puede ser respondida por el destinatario"
            )
        
        if encuesta.modo == 'compartida' and encuesta.estado == 'completada':
            raise HTTPException(
                status_code=400,
                detail=f"Esta encuesta ya fue respondida por {encuesta.respondido_por_nombre}"
            )
        
        ahora_utc = datetime.now(timezone.utc)
        if encuesta.fecha_limite < ahora_utc:
            raise HTTPException(
                status_code=400,
                detail="Esta encuesta ya expiró"
            )
        
        if encuesta.estado == 'completada' and encuesta.respondido_por_email == user_email:
            raise HTTPException(
                status_code=400,
                detail="Ya ha respondido esta encuesta"
            )
        
        ahora = datetime.now(timezone.utc)
        respuestas_para_insertar = []
        
        for resp in request.respuestas:
            respuesta_data = {
                "respuesta_id": str(uuid.uuid4()),
                "encuesta_id": encuesta_id,
                "pregunta_id": resp.get("pregunta_id"),
                "respuesta_valor": resp.get("respuesta_valor"),
                "comentario_adicional": resp.get("comentario"),
                "fecha_respuesta": ahora.isoformat()
            }
            respuestas_para_insertar.append(respuesta_data)
        
        errors = get_bq_client().insert_rows_json(TABLE_ENCUESTAS_RESPUESTAS, respuestas_para_insertar)
        
        if errors:
            raise HTTPException(
                status_code=500,
                detail=f"Error al guardar respuestas: {errors}"
            )
        
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
        
        get_bq_client().query(query_update, job_config=job_config_update).result()
        
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


@router.get("/api/encuestas/{encuesta_id}/respuestas")
async def ver_respuestas_encuesta(
    encuesta_id: str,
    user_data: dict = Depends(verify_firebase_token)
):
    """
    Obtiene las respuestas de una encuesta completada.
    """
    try:
        user_email = user_data["email"]
        rol = user_data["rol_id"]
        es_wfsa = rol in ['ADMIN_WFSA', 'SUBGERENTE_JEFE_WFSA', 'SUPERVISOR_WFSA']
        
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
        
        encuesta_result = list(get_bq_client().query(query_encuesta, job_config=job_config).result())
        
        if not encuesta_result:
            raise HTTPException(status_code=404, detail="Encuesta no encontrada")
        
        encuesta = encuesta_result[0]
        
        if not es_wfsa:
            if encuesta.modo == 'individual' and encuesta.email_destinatario != user_email:
                raise HTTPException(status_code=403, detail="No tiene permiso para ver estas respuestas")
        
        if encuesta.estado != 'completada':
            raise HTTPException(status_code=400, detail="Esta encuesta aún no ha sido respondida")
        
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
        
        respuestas_result = list(get_bq_client().query(query_respuestas, job_config=job_config).result())
        
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

