import argparse
import time
import json

from common.utils.readers import read_csv, read_json
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_kafka_producer(broker):
    """
    Create a Kafka producer connected to the given broker.
    """
    try:
        return KafkaProducer(bootstrap_servers=broker)
    except KafkaError as e:
        print(f"Failed to create Kafka producer: {e}")
        raise

def send_events_to_kafka(producer, topic, data_iterator, sleep_time=1):
    """
    Sends data from an iterator to a Kafka topic.
    """
    for i, record in enumerate(data_iterator):
        try:
            # Format the record as JSON
            json_record = json.dumps(record).encode('utf-8')
            # Send the message to Kafka
            producer.send(topic=topic, key=str(i).encode('utf-8'), value=json_record)
            print(f"Sent message {i+1} to topic {topic}: {record}")
        except KafkaError as e:
            print(f"Failed to send message {i+1}: {e}")
        time.sleep(sleep_time)

# Main function for the CLI
def main():
    """
    Entry point for the CLI application.
    """
    parser = argparse.ArgumentParser(description="Kafka Producer CLI for sending data to Kafka topics.")

    # Kafka arguments
    parser.add_argument("--broker", required=True, help="Kafka broker address (e.g., localhost:9092)")
    parser.add_argument("--topic", required=True, help="Kafka topic name")

    # Input file arguments
    parser.add_argument("--file-path", required=True, help="Path to the input file (CSV or JSON)")
    parser.add_argument("--columns", nargs="*", help="List of column names if the CSV lacks a header row (CSV only)")
    parser.add_argument("--delimiter", default=",", help="Delimiter used in the CSV file (default: ',')")

    # Other options
    parser.add_argument("--sleep-time", type=float, default=1, help="Delay (in seconds) between messages (default: 1)")
    parser.add_argument("--cycle-delay", type=float, default=300, help="Delay (in seconds) between cycles (default: 300)")

    args = parser.parse_args()

    # Kafka producer setup
    kafka_producer = create_kafka_producer(args.broker)

    file_format = args.file_path.split(".")[-1].lower()

    try:
        while True:
            # Choose the appropriate reader based on format
            if file_format == "csv":
                data_iterator = read_csv(args.file_path, columns=args.columns, delimiter=args.delimiter)
            elif file_format == "json":
                data_iterator = read_json(args.file_path)
            else:
                raise ValueError("Unsupported format specified.")

            # Send data to Kafka
            send_events_to_kafka(
                producer=kafka_producer,
                topic=args.topic,
                data_iterator=data_iterator,
                sleep_time=args.sleep_time
            )

            print(f"Waiting for {args.cycle_delay} seconds before the next cycle.")
            time.sleep(args.cycle_delay)

    except KeyboardInterrupt:
        print("Shutting down gracefully.")

    finally:
        kafka_producer.close()
        print("Kafka producer closed.")

if __name__ == "__main__":
    # EXAMPLE OF SCRIPT CALL
    # python kafka_producer_cli.py --broker localhost:9092 --topic my-topic --file-path data.csv --format csv --delimiter ";" --sleep-time 2 --cycle-delay 300
    main()
