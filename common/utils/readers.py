import json
import csv

# Function to read data from a CSV file
def read_csv(file_path, columns=None, delimiter=","):
    """
    Reads data from a CSV file and yields rows as dictionaries.
    """
    with open(file_path, 'r') as infile:
        reader = csv.DictReader(infile, fieldnames=columns, delimiter=delimiter)
        # Skip the header if column names are provided
        if not columns:
            columns = reader.fieldnames
        else:
            next(reader)  # Skip header if custom columns are specified
        for row in reader:
            yield row

# Function to read data from a JSON file
def read_json(file_path):
    """
    Reads data from a JSON file and yields rows as dictionaries.
    """
    with open(file_path, 'r') as infile:
        data = json.load(infile)
        if isinstance(data, list):  # If it's a list of records
            for row in data:
                yield row
        else:  # If it's a single JSON object
            yield data
