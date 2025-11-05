# Usar imagen oficial de Python
FROM python:3.11-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivos de dependencias
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la aplicación (modular)
COPY main.py .
COPY config.py .
COPY dependencies.py .
COPY models/ ./models/
COPY routers/ ./routers/
COPY utils/ ./utils/

# Exponer puerto
EXPOSE 8080

# Variables de entorno por defecto (se sobrescriben en Cloud Run)
ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV ENVIRONMENT=production
ENV GCP_PROJECT_ID=worldwide-470917
ENV DATASET_REPORTES=cr_reportes
ENV DATASET_APP=app_clientes
ENV CORS_ORIGINS=*

# Comando para ejecutar la aplicación
CMD ["python", "main.py"]
