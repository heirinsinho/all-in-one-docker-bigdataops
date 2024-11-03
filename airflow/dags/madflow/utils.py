import json

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