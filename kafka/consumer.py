from confluent_kafka import Consumer, KafkaError
import json

class LocationData:
    def __init__(self, x, y, t):
        self.x = x
        self.y = y
        self.t = t

    @classmethod
    def deserialize(cls, data):
        params = json.loads(data)
        return cls(**params)

class LidarData:
    def __init__(self, x, y, z, CosAngle, ObjIdx, ObjTag):
        self.x = x
        self.y = y
        self.z = z
        self.CosAngle = CosAngle
        self.ObjIdx = ObjIdx
        self.ObjTag = ObjTag

    @classmethod
    def deserialize(cls, data):
        params = json.loads(data)
        return cls(**params)

class SpeedData:
    def __init__(self, v):
        self.v = v

    @classmethod
    def deserialize(cls, data):
        params = json.loads(data)
        return cls(**params)


c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['location-topic', 'lidar-topic', 'speed-topic'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    if msg.topic() == 'location-topic':
        data = LocationData.deserialize(msg.value().decode('utf-8'))
    elif msg.topic() == 'lidar-topic':
        data = LidarData.deserialize(msg.value().decode('utf-8'))
    elif msg.topic() == 'speed-topic':
        data = SpeedData.deserialize(msg.value().decode('utf-8'))

    print(f'Received message on topic {msg.topic()}: {data.__dict__}')

c.close()