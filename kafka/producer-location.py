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

p = Producer({'bootstrap.servers': 'localhost:9092'})

with open('../textInputSources/locationSource.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        location_data = LocationData(row['x'], row['y'], row['t'])
        p.produce('location-topic', location_data.serialize())

p.flush()