# Usar imagen oficial de Python
FROM python:3.11-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivos de dependencias
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la aplicación
COPY main.py .

# Exponer puerto
EXPOSE 8080

# Variables de entorno por defecto (se sobrescriben en Cloud Run)
ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV ENVIRONMENT=production

# Comando para ejecutar la aplicación
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT}
