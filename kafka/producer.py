import os
import csv
import json
from confluent_kafka import Producer

class LocationData:
    def __init__(self, x, y, t):
        self.x = x
        self.y = y
        self.t = t

    def serialize(self):
        return json.dumps(self.__dict__)

class LidarData:
    def __init__(self, x, y, z, CosAngle, ObjIdx, ObjTag):
        self.x = x
        self.y = y
        self.z = z
        self.CosAngle = CosAngle
        self.ObjIdx = ObjIdx
        self.ObjTag = ObjTag

    def serialize(self):
        return json.dumps(self.__dict__)


class SpeedData:
    def __init__(self, v):
        self.v = v

    def serialize(self):
        return json.dumps(self.__dict__)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Initialize the Producer
p = Producer({
    'bootstrap.servers': 'glider.srvs.cloudkafka.com:9094',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'ozlwmnls',
    'sasl.password': 'nd4YYjvGiOsZgzlHRUG9cedDoPJJOyfQ',
})

# Produce LocationData
with open('../textInputSources/locationSource.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        location_data = LocationData(int(row['x']), int(row['y']), int(row['t']))
        p.produce('ozlwmnls-location-topic', location_data.serialize())

# Produce LidarData
for filename in os.listdir('../Lidar_data'):
    if filename.endswith('.ply'):
        with open(os.path.join('../Lidar_data', filename), 'r') as f:
            lines = f.readlines()[10:]  # Skip header
            for line in lines:
                x, y, z, CosAngle, ObjIdx, ObjTag = map(float, line.split())
                lidar_data = LidarData(x, y, z, CosAngle, int(ObjIdx), int(ObjTag))
                p.produce('ozlwmnls-lidar-topic', lidar_data.serialize())
                p.flush()

p.flush()

# Produce SpeedData
with open('../textInputSources/generatedSpeed.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        speed_data = SpeedData(float(row['v']))
        p.produce('ozlwmnls-speed-topic', speed_data.serialize(), callback=delivery_report)
        p.flush()

p.flush()