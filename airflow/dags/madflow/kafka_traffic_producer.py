from kafka import KafkaProducer

import os
import time
import json
import csv

from settings import config as conf


# Configurar y crear el productor kafka
kafka_broker_hostname = 'kafka'
kafka_consumer_port = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_consumer_port
kafka_topic = 'traffic-topic'
kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

file_path = conf.TRAFFIC_CSV_PATH


def send_traffic_events():
    # Descargamos el fichero de tráfico y lo convertimos a csv
    #files_download_format.traffic_to_CSV()

    # Lectura del csv de tráfico
    infile = open(file_path, 'r')

    columns = ("fecha_hora", "idelem", "intensidad", "ocupacion", "carga", "nivelServicio", "error", "st_x", "st_y")
    reader = csv.DictReader(infile, columns, delimiter=';')
    i = 0
    # Lectura de cada fila
    for row in reader:
        # Descartamos la cabecera
        if i != 0:

            json_row = json.dumps(row).encode('utf-8')

            #print(strLineJson)

            # Envío del mensaje a kafka
            kafka_producer.send(topic=kafka_topic, key=row['idelem'].encode('utf-8'), value=json_row)
            kafka_producer.flush()

        i = i + 1
        #Ajustamos el tiempo para que tarde unos 10 minutos en procesar cada csv y asegurarnos de que se actualiza
        time.sleep(0.14)
    # Cierre del fichero
    infile.close()

# Ejecutar continuamente hasta parada manual
while True:
    send_traffic_events()
    time.sleep(5)