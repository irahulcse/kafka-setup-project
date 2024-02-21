
import csv
import json
from confluent_kafka import Producer

class SpeedData:
    def __init__(self, v):
        self.v = v

    def serialize(self):
        return json.dumps(self.__dict__)

# Initialize the Producer
p = Producer({'bootstrap.servers': 'localhost:9092'})

# Produce SpeedData
with open('../../textInputSources/generatedSpeed.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        speed_data = SpeedData(float(row['v']))
        p.produce('speed-topic', speed_data.serialize())

p.flush()