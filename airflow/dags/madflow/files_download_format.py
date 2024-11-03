import os
import wget
import requests
import csv
import json
import xml.etree.ElementTree as ET
from settings import config as conf

traffic_xml_path = conf.TRAFFIC_XML_PATH
traffic_csv_path = conf.TRAFFIC_CSV_PATH
bicimad_json_path = conf.BICIMAD_JSON_PATH
parkings_json_path = conf.PARKINGS_JSON_PATH
bicimad_csv_path = conf.BICIMAD_CSV_PATH
parkings_csv_path = conf.PARKINGS_CSV_PATH


def _download_traffic_xml():

    url = conf.TRAFFIC_URL
    wget.download(url, traffic_xml_path)

def _read_traffic_xml():
    '''
        Checks if file exists and if not downloads it, parses the file and extracts the needed data
    '''
    if os.path.exists(traffic_xml_path):
        os.remove(traffic_xml_path)
    _download_traffic_xml()

    tree = ET.parse(traffic_xml_path)
    root = tree.getroot()
    #you may need to adjust the keys based on your file structure
    dict_keys = ["idelem","intensidad","ocupacion","carga",
                 "nivelServicio","error","st_x","st_y"] #all keys to be extracted from xml
    #"subarea","intensidadSat","accesoAsociado","descripcion"
    mdlist = []
    head = dict_keys.copy()
    head.insert(0, "fecha_hora")
    mdlist.append(head)
    date = root.find('fecha_hora').text

    for child in root.findall('pm'):
        temp = []
        for key in dict_keys:
            temp.append(child.find(key).text)

        temp.insert(0, date)
        mdlist.append(temp)
    return mdlist


def traffic_to_CSV():
    '''
        Generates csv file with given data
    '''
    traffic_xml = _read_traffic_xml()

    fh = open(traffic_csv_path, "w")
    writer = csv.writer(fh, delimiter=';', quotechar='|') #each value is separated by ;
    for row in traffic_xml:
        writer.writerow(row)
    fh.close()


def _get_access_token():
    login_url = "https://openapi.emtmadrid.es/v1/mobilitylabs/user/login/"
    login_payload = {}
    login_headers = {
    'X-ClientId': conf.API_X_CLIENT_ID,
    'passKey': conf.API_PASSKEY
    }
    login_response = requests.request("GET", login_url, headers=login_headers, data=login_payload)
    access_token = login_response.json()['data'][0]["accessToken"]
    #print(access_token)
    return access_token

#access_token = _get_access_token()




def get_bicimad_json():
    bicimad_url = conf.BICIMAD_URL
    bicimad_payload = {}
    headers = {
    'accessToken': _get_access_token()
    }
    if os.path.exists(bicimad_json_path):
        os.remove(bicimad_json_path)
    response_bicimad = requests.request("GET", bicimad_url, headers=headers, data=bicimad_payload)
    #print(response_bicimad.json())
    with open(bicimad_json_path, 'w') as bicimad_json:
        json.dump(response_bicimad.json(), bicimad_json)


def get_parkings_json():
    parkings_url = conf.PARKINGS_URL
    parkings_payload = {}
    headers = {
    'accessToken': _get_access_token()
    }
    if os.path.exists(parkings_json_path):
        os.remove(parkings_json_path)
    parkings_response = requests.request("GET", parkings_url, headers=headers, data=parkings_payload)
    #print(parkings_response.json())
    with open(parkings_json_path, 'w') as parkings_json:
        json.dump(parkings_response.json(), parkings_json)


def get_bicimad_csv(json_input, csv_output):
    get_bicimad_json()
    _json_to_csv(json_input, csv_output)


def get_parkings_csv(json_input, csv_output):
    get_parkings_json()
    _json_to_csv(json_input, csv_output)
