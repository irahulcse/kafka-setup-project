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

# Initialize the Producer
p = Producer({
    'bootstrap.servers': 'localhost:9092', 
    'queue.buffering.max.messages': 200000,
})

# Produce LocationData
with open('../textInputSources/locationSource.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        location_data = LocationData(int(row['x']), int(row['y']), int(row['t']))
        p.produce('location-topic', location_data.serialize())

# Produce LidarData
for filename in os.listdir('../Lidar_data'):
    if filename.endswith('.ply'):
        with open(os.path.join('../Lidar_data', filename), 'r') as f:
            lines = f.readlines()[10:]  # Skip header
            for line in lines:
                x, y, z, CosAngle, ObjIdx, ObjTag = map(float, line.split())
                lidar_data = LidarData(x, y, z, CosAngle, int(ObjIdx), int(ObjTag))
                p.produce('lidar-topic', lidar_data.serialize())

p.flush()