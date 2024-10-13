import boto3
from datetime import datetime

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

upload_to_s3()

