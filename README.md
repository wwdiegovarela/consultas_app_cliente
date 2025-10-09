# WFSA BigQuery API

API REST para la aplicación móvil WFSA - Gestión de cobertura de guardias en tiempo real.

## 📋 Descripción

Backend desarrollado en FastAPI que conecta la app móvil Flutter con BigQuery para mostrar:
- Cobertura instantánea de guardias (actualizada cada 5 minutos)
- Histórico de cobertura por semana
- Contactos de WhatsApp por instalación
- Autenticación con Firebase

## 🚀 Despliegue en Cloud Run

### 1. Configurar proyecto de GCP

```bash
gcloud config set project worldwide-470917
```

### 2. Habilitar APIs necesarias

```bash
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable bigquery.googleapis.com
```

### 3. Desplegar a Cloud Run

```bash
gcloud run deploy wfsa-api \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GCP_PROJECT_ID=worldwide-470917,DATASET_REPORTES=cr_reportes,DATASET_APP=app_clientes,ENVIRONMENT=production
```

### 4. Variables de entorno en Cloud Run

Configurar en la consola de Cloud Run o con el comando:

```bash
gcloud run services update wfsa-api \
  --region us-central1 \
  --set-env-vars \
    GCP_PROJECT_ID=worldwide-470917,\
    DATASET_REPORTES=cr_reportes,\
    DATASET_APP=app_clientes,\
    SEMAFORO_VERDE=0.95,\
    SEMAFORO_AMARILLO=0.80,\
    DIAS_HISTORICO_DEFAULT=90,\
    ENVIRONMENT=production
```

## 🔧 Desarrollo Local

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar credenciales de GCP

```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
```

### 3. Copiar variables de entorno

```bash
cp env.example .env
# Editar .env con tus valores
```

### 4. Ejecutar servidor

```bash
python main.py
```

El servidor estará disponible en: http://localhost:8080

## 📚 Documentación de la API

Una vez desplegado, la documentación interactiva está disponible en:
- Swagger UI: `https://tu-servicio.run.app/docs`
- ReDoc: `https://tu-servicio.run.app/redoc`

## 🔐 Variables de Entorno

| Variable | Descripción | Valor por defecto |
|----------|-------------|-------------------|
| `GCP_PROJECT_ID` | ID del proyecto de GCP | `worldwide-470917` |
| `DATASET_REPORTES` | Dataset con datos origen | `cr_reportes` |
| `DATASET_APP` | Dataset con tablas de gestión | `app_clientes` |
| `SEMAFORO_VERDE` | Umbral para semáforo verde (decimal) | `0.95` |
| `SEMAFORO_AMARILLO` | Umbral para semáforo amarillo (decimal) | `0.80` |
| `DIAS_HISTORICO_DEFAULT` | Días históricos por defecto | `90` |
| `CORS_ORIGINS` | Dominios permitidos (separados por coma) | `*` |
| `ENVIRONMENT` | Ambiente de ejecución | `development` |
| `PORT` | Puerto del servidor | `8080` |

## 📡 Endpoints Principales

### Cobertura Instantánea
- `GET /api/cobertura/instantanea/general` - % de cobertura general
- `GET /api/cobertura/instantanea/por-instalacion` - Cobertura por instalación
- `GET /api/cobertura/instantanea/detalle/{instalacion}` - Detalle de turnos

### Cobertura Histórica
- `GET /api/cobertura/historico/semanal?dias=90` - Histórico semanal
- `GET /api/cobertura/historico/por-instalacion?dias=90` - Histórico por instalación

### Contactos
- `GET /api/contactos/{instalacion}` - Contactos de WhatsApp

### Health Check
- `GET /` - Estado del servicio
- `GET /api/health` - Health check

## 🔒 Autenticación

Todos los endpoints (excepto `/` y `/api/health`) requieren autenticación con Firebase:

```bash
curl -H "Authorization: Bearer <firebase-token>" \
  https://tu-servicio.run.app/api/cobertura/instantanea/general
```

## 📊 Estructura de BigQuery

### Datasets:
- `cr_reportes` - Datos del sistema origen (cobertura_instantanea, cr_asistencia_hist_tb)
- `app_clientes` - Tablas de gestión (usuarios_app, contactos, etc.)

### Tablas principales:
- `cr_reportes.cobertura_instantanea` - Cobertura en tiempo real (actualización cada 5 min)
- `cr_reportes.cr_asistencia_hist_tb` - Histórico de asistencias
- `app_clientes.cr_info_instalaciones` - Metadata de instalaciones
- `app_clientes.usuarios_app` - Usuarios de la app
- `app_clientes.usuario_instalaciones` - Control de acceso
- `app_clientes.contactos` - Contactos de WhatsApp
- `app_clientes.instalacion_contacto` - Relación instalaciones-contactos

## 🛠️ Stack Tecnológico

- **Framework**: FastAPI 0.115.0
- **Base de datos**: Google BigQuery
- **Autenticación**: Firebase Admin SDK
- **Despliegue**: Google Cloud Run
- **Python**: 3.11

## 📝 Licencia

Propiedad de WFSA - Todos los derechos reservados
