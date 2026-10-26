# Big-Data-Sensores---Tarea-3
# Sistema de Monitoreo IoT con Apache Kafka y Apache Spark (Batch & Streaming)

## Descripción del proyecto
Este proyecto desarrolla procesos de análisis de datos por lotes (batch processing) y procesamiento de datos en tiempo real (stream processing) mediante herramientas del ecosistema Big Data. Se modificó y adaptó el código de los scripts de Python originales para integrar un flujo completo donde se generan datos de sensores IoT en tiempo real, se transmiten mediante Apache Kafka y se procesan tanto de forma continua con Spark Structured Streaming como mediante análisis histórico por lotes (Batch) con PySpark.

## Objetivos
- Modificar y adaptar los scripts base en Python (`kafka_producer.py`) para simular, transmitir y persistir de forma concurrente eventos de sensores IoT (temperatura y humedad).
- Implementar la arquitectura de streaming en tiempo real con Apache Kafka y Apache Spark Structured Streaming, calculando promedios por ventanas de tiempo.
- Desarrollar un script de procesamiento por lotes (`batch_sensors.py`) para la limpieza, validación de esquemas y análisis exploratorio de datos (EDA) del histórico generado.

## Tecnologías utilizadas
- **Apache Kafka (v3.9.2):** Sistema de mensajería distribuida para la transmisión de eventos de sensores.
- **Apache Spark (v3.5.3):** Motor de procesamiento unificado para análisis por lotes y streaming.
- **Python / PySpark:** Desarrollo de scripts de automatización, producción de datos y análisis estadístico.
- **Ubuntu Server / Máquina Virtual:** Entorno operativo de ejecución del proyecto.

## Conjunto de datos y persistencia unificada
El proyecto utiliza una fuente unificada de datos simulados de sensores IoT. El script `kafka_producer.py` genera registros con los siguientes campos:
- `sensor_id`: Identificador numérico del sensor (del 1 al 10).
- `temperature`: Valor de temperatura registrado.
- `humidity`: Porcentaje de humedad relativa.
- `timestamp`: Marca de tiempo del evento.

Estos registros se envían simultáneamente al tópico `sensor_data` de Kafka y se persisten de forma concurrente en un archivo local llamado `sensores_historicos.csv` para su posterior análisis por lotes.

## Procesamiento por lotes con Spark (`batch_sensors.py`)
El script de procesamiento batch realiza las siguientes operaciones sobre el archivo histórico:
- Carga el archivo CSV aplicando inferencia de esquemas y cabeceras de manera local.
- Elimina registros duplicados para asegurar la integridad de los datos.
- Realiza el cast estricto de tipos de datos (`sensor_id` a entero, `temperature` y `humidity` a double).
- Filtra valores nulos o inconsistentes en las métricas de los sensores.
- Ejecuta un Análisis Exploratorio de Datos (EDA) utilizando estadísticas descriptivas (`describe()`).
- Agrupa los datos por `sensor_id` calculando el promedio de temperatura, el promedio de humedad y el total de lecturas por sensor.

## Procesamiento en tiempo real con Kafka y Spark Streaming (`spark_streaming_consumer.py`)
- **Productor (`kafka_producer.py`):** Emite eventos JSON continuos al tópico `sensor_data` de Kafka y escribe de manera concurrente en el archivo CSV local.
- **Consumidor (`spark_streaming_consumer.py`):** Lee en tiempo real desde Kafka, parsea la estructura JSON aplicando un esquema estructurado, convierte el `timestamp` a tipo fecha/hora de Spark y calcula estadísticas agregadas utilizando ventanas deslizantes de un minuto (`window` de 1 minuto) agrupadas por `sensor_id`.

## Archivos del proyecto
- `kafka_producer.py`: Productor de eventos IoT para Kafka y generador concurrente del archivo CSV histórico.
- `spark_streaming_consumer.py`: Consumidor de Spark Structured Streaming para el procesamiento al vuelo de los datos de Kafka.
- `batch_sensors.py`: Script de análisis batch y estadísticas descriptivas del histórico de sensores mediante PySpark.
- `README.md`: Documentación general del proyecto.

## Ejecución

Asegúrate de tener activos los servicios de Zookeeper y Kafka, y luego ejecuta el productor:
python3 kafka_producer.py

En otra terminal, ejecuta el consumidor de Spark Streaming con el paquete del conector de Kafka compatible con Spark 3.5.3:

Bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py

Procesamiento por lotes (Batch)
Una vez que el productor haya recolectado suficientes registros en el archivo local sensores_historicos.csv (puedes detener el productor con Ctrl + C), ejecuta el script de análisis batch:
spark-submit batch_sensors.py

## Conclusión

La actividad permitió integrar con éxito componentes del ecosistema Big Data en una arquitectura coherente. El uso de Apache Kafka y Spark Structured Streaming garantizó la captura y agregación de eventos en tiempo real por ventanas de tiempo, mientras que el procesamiento batch con PySpark demostró la capacidad de limpieza, validación de esquemas y análisis exploratorio sobre un repositorio de datos históricos generado por los mismos dispositivos de sensores.
