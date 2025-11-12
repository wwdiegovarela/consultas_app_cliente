"""
Modelos Pydantic para requests y responses
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, EmailStr


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
    encuestado_nombre: Optional[str] = None


class FCMTokenRequest(BaseModel):
    fcm_token: str


class InstalacionesRequest(BaseModel):
    instalaciones: List[str]  # Lista de instalacion_rol


class SendMessageNotificationRequest(BaseModel):
    conversation_id: str
    message_id: str
    sender_id: str
    sender_name: str
    message_text: str
    visible_para_cliente: bool = True
    participant_user_ids: Optional[List[str]] = None  # Lista de firebase_uid de participantes (opcional)

