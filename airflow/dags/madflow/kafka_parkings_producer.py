from kafka import KafkaProducer

import os
import time
import json
import csv

from utils import _json_to_csv
from settings import config as conf


# Configurar y crear el productor kafka
kafka_broker_hostname = 'kafka'
kafka_consumer_port = '9092'
kafka_broker = kafka_broker_hostname + ':' + kafka_consumer_port
kafka_topic = "parkings-topic"
kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

parkings_json = conf.PARKINGS_JSON_PATH
parkings_csv = conf.PARKINGS_CSV_PATH
file_path = conf.PARKINGS_CSV_PATH


def send_parking_events():
    # Descargamos el fichero de parkings y lo convertimos a csv
    _json_to_csv(parkings_json, parkings_csv)

    # Lectura del csv de parkings
    infile = open(file_path, 'r')

    columns = ("id", "name", "address", "postalCode", "isEmtPark", "lastUpd", "freeParking", "geometry", "datetime")
    reader = csv.DictReader(infile, columns, delimiter=';')
    i = 0
    # Lectura de cada fila
    for row in reader:
        # Descartamos la cabecera
        if i != 0:

            # Formateo del mensajea JSON
            json_row = json.dumps(row).encode('utf-8')

            #print(json_row)

            # Envío del mensaje a kafka
            kafka_producer.send(topic=kafka_topic, key=row['id'].encode('utf-8'), value=json_row)
        i = i + 1

        time.sleep(1)
    # Cierre del fichero
    infile.close()


# Ejecutar continuamente hasta parada manual
while True:
    send_parking_events()
    # Paramos 5 minutos ya que los datos no se actualizan constantemente
    time.sleep(300)

