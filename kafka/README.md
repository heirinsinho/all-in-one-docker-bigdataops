# CREATE A TOPIC

kafka-topics --create --topic device-data --bootstrap-server kafka:9092

# LIST TOPICS

kafka-topics --list --bootstrap-server kafka:9092

# SEND MESSAGES TO A TOPIC FROM THE TERMINAL (Example from /data/device_data/device_data_samples.txt)

kafka-console-producer --topic device-data --bootstrap-server kafka:9092
> {"eventId": "692e9999-1110-4441-a20e-fd76692e2c17", "eventOffset": 10014, "eventPublisher": "device", "customerId": "
> CI00109", "data": {"devices": [{"deviceId": "D003", "temperature": 18, "measure": "C", "status": "ERROR"}]}, "
> eventTime": "2023-01-05 11:13:53.643895"}