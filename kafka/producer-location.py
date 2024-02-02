from confluent_kafka import Producer
import csv
import json

class LocationData:
    def __init__(self, x, y, t):
        self.x = x
        self.y = y
        self.t = t

    def serialize(self):
        return json.dumps(self.__dict__)

p = Producer({
    'bootstrap.servers': 'glider.srvs.cloudkafka.com:9094',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'ozlwmnls',
    'sasl.password': 'nd4YYjvGiOsZgzlHRUG9cedDoPJJOyfQ'
})

with open('../textInputSources/locationSource.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(row.keys())  # Print the keys of the row
        location_data = LocationData(int(row['x']), int(row['y']), int(row['t']))
        p.produce('ozlwmnls-location-topic', location_data.serialize())

p.flush()