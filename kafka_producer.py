import time
import json
import random
import csv
import os
from kafka import KafkaProducer

csv_file = "sensores_historicos.csv"

# Crear el archivo CSV y escribir el encabezado si no existe
file_exists = os.path.exists(csv_file)
if not file_exists:
    with open(csv_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sensor_id", "temperature", "humidity", "timestamp"])

# Configurar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())
    }

print("Iniciando productor y guardando datos en CSV. Presiona Ctrl + C para detener.")

try:
    while True:
        sensor_data = generate_sensor_data()
        
        # 1. Enviar a Kafka (Streaming)
        producer.send('sensor_data', value=sensor_data)
        
        # 2. Guardar en el archivo CSV (Batch)
        with open(csv_file, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                sensor_data["sensor_id"],
                sensor_data["temperature"],
                sensor_data["humidity"],
                sensor_data["timestamp"]
            ])
            
        print(f"Sent & Saved: {sensor_data}")
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")
