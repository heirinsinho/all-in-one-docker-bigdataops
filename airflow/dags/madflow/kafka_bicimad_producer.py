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
kafka_topic = "bicimad-topic"
kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

bicimad_json = conf.BICIMAD_JSON_PATH
bicimad_csv = conf.BICIMAD_CSV_PATH
file_path = conf.BICIMAD_CSV_PATH


def send_bicimad_events():
    # Descargamos el fichero de bicimad y lo convertimos a csv
    #files_download_format.get_bicimad_csv(bicimad_json, bicimad_csv)

    # Lectura del csv de bicimad
    infile = open(file_path, 'r')

    columns = ("id", "name", "light", "number", "address", "activate", "no_available", "total_bases", "dock_bikes",
                  "free_bases", "reservations_count", "geometry", "datetime")
    reader = csv.DictReader(infile, columns, delimiter=';')
    i = 0
    # Lectura de cada fila
    for row in reader:
        # Descartamos la cabecera
        if i != 0:

            json_row = json.dumps(row).encode('utf-8')

            #print(json_row)

            # Envío del mensaje a kafka
            kafka_producer.send(topic=kafka_topic, key=row['id'].encode('utf-8'), value=json_row)
            kafka_producer.flush()

        i = i + 1

        # Ajustamos para actualizar aproximádamente cada 2 minutos
        time.sleep(0.5)
    # Cierre del fichero
    infile.close()

# Ejecutar continuamente hasta parada manual
while True:
    send_bicimad_events()
    time.sleep(3)
