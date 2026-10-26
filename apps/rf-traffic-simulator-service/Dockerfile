FROM registry.access.redhat.com/ubi9/python-39:latest

WORKDIR /app

# Instalación de librerías requeridas
RUN pip install --no-cache-dir confluent-kafka prometheus_client

# Copia desde la carpeta local src/ hacia el WORKDIR /app
COPY src/traffic_generator.py /app/traffic_generator.py

EXPOSE 8000

# Ejecuta el script dentro de /app
CMD ["python", "/app/traffic_generator.py"]