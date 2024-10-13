import boto3
import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta
import time

# Configuración de S3
s3 = boto3.client('s3')
bucket_name = 'investing.com-predictions-project-bucket'

# Función para cargar el archivo en S3 con fecha
def upload_to_s3():
    date_str = datetime.now().strftime('%d%m%Y')
    s3_file_name = f'logs/log_{date_str}.log'
    try:
        s3.upload_file("log_latest.log", bucket_name, s3_file_name)
        print(f"Archivo log_latest.log subido a S3 como {s3_file_name} exitosamente.")
    except Exception as e:
        print(f"Error al subir el archivo a S3: {e}")

# Ejecutar la carga cada 24 horas
while True:
    # Esperar hasta medianoche
    next_run = datetime.combine(datetime.now().date(), datetime.min.time()) + timedelta(days=1)
    wait_time = (next_run - datetime.now()).total_seconds()
    time.sleep(wait_time)
    # Cargar el archivo a S3
    upload_to_s3()
