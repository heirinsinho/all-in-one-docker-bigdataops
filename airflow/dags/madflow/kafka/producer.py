from xml.etree import ElementTree
from kafka import KafkaProducer
import concurrent.futures
import time
import requests
import json

url_parking_details = "https://openapi.emtmadrid.es/v1/citymad/places/parking/{parking_id}/ES"
url_parkings_availability = "https://openapi.emtmadrid.es/v3/citymad/places/parkings/availability"
url_traffic = "https://informo.madrid.es/informo/tmadrid/pm.xml"
url_bicimad_stations = "https://openapi.emtmadrid.es/v2/transport/bicimad/stations"


def get_access_token():
    url = "https://openapi.emtmadrid.es/v2/mobilitylabs/user/login/"

    headers = {
        "X-ClientId": "4614aca0-0a67-4bb9-b912-87b9bd62003c",
        "passKey": "6D42B608EFDB34D1EE2623846AF4CFB3B88161A28C6CF6973D9569FA746757BF907C058CA211FD8F5787A918E928F30D82F0EFFE8F219F4A54BF53BA10549F06"
    }

    response = requests.request("GET", url, headers=headers)
    return response.json()["data"][0]["accessToken"]


headers = {"accessToken": get_access_token()}


def fetch_traffic_data():
    response = requests.request("GET", url_traffic)
    root = ElementTree.fromstring(response.text)
    all_data = zip(root.findall(".//pm/idelem"),
                   root.findall(".//pm/intensidad"),
                   root.findall(".//pm/ocupacion"),
                   root.findall(".//pm/carga"))

    for x, y, z, t in all_data:
        x = {
            "id": x.text,
            "intensity": y.text,
            "occupancy": z.text,
            "load": t.text
        }
        yield json.dumps(x)


def fetch_parkings_data():
    response = requests.request("GET", url_parkings_availability, headers=headers)
    for record in response.json()["data"]:
        x = {"id": record["id"],
             "datetime": response.json()["datetime"],
             "free_slots": record.get("freeParking")}

        yield json.dumps(x)


def fetch_bicimad_data():
    response = requests.request("GET", url_bicimad_stations, headers=headers)

    data = response.json()["data"]
    datetime = response.json()["datetime"]

    for record in data:
        x = {"id": record["id"],
             "name": record["name"],
             "longitude": record["geometry"]["coordinates"][0],
             "latitude": record["geometry"]["coordinates"][1],
             "free_bases": record["free_bases"],
             "total_bases": record["total_bases"],
             "reservations": record["reservations_count"],
             "active": record["activate"],
             "datetime": datetime}

        yield json.dumps(x)


topic_dict = {
    "traffic": fetch_traffic_data,
    "parkings": fetch_parkings_data,
    "bicimad": fetch_bicimad_data
}


# Function to produce messages to a specific topic
def produce_messages(topic, getter_func):
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: str(v).encode('utf-8')
    )
    while True:

        msg_iterator = getter_func()

        for msg in msg_iterator:
            print(topic, msg)
            producer.send(topic, value=msg)
            producer.flush()
            time.sleep(0.1)


def start_producers():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each topic
        futures = [
            executor.submit(produce_messages, topic, getter_func)
            for topic, getter_func in topic_dict.items()
        ]
        # Wait for all tasks to complete
        concurrent.futures.wait(futures)
        print("All producers have finished.")


if __name__ == '__main__':

    start_producers()