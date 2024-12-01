import csv
import json
import time

from kafka import KafkaProducer
from settings import config as conf


def _json_to_csv(json_input, csv_output):
    # Opening JSON file and loading the data
    # into the variable data
    with open(json_input) as json_file:
        data = json.load(json_file)

    json_datetime = data['datetime']
    json_data = data['data']

    # now we will open a file for writing
    data_file = open(csv_output, 'w')

    # create the csv writer object
    csv_writer = csv.writer(data_file, delimiter=';')

    # Counter variable used for writing
    # headers to the CSV file
    count = 0

    for emp in json_data:
        emp["datetime"] = json_datetime
        if count == 0:
            # Writing headers of CSV file
            header = emp.keys()

            csv_writer.writerow(header)
            count += 1

        # Writing data of CSV file
        csv_writer.writerow(emp.values())

    data_file.close()


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

            # print(json_row)

            # Envío del mensaje a kafka
            kafka_producer.send(topic=kafka_topic, key=row['id'].encode('utf-8'), value=json_row)
            kafka_producer.flush()

        i = i + 1

        time.sleep(1)
    # Cierre del fichero
    infile.close()


# Ejecutar continuamente hasta parada manual
while True:
    send_parking_events()
    # Paramos 5 minutos ya que los datos no se actualizan constantemente
    time.sleep(300)
