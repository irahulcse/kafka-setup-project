from confluent_kafka import Consumer, KafkaError
import json

class XData:
    def __init__(self, type, x):
        self.type = type
        self.x = x

    @classmethod
    def deserialize(cls, data):
        params = json.loads(data)
        return cls(**params)

class YData:
    def __init__(self, type, y):
        self.type = type
        self.y = y

    @classmethod
    def deserialize(cls, data):
        params = json.loads(data)
        return cls(**params)

class TData:
    def __init__(self, type, t):
        self.type = type
        self.t = t

    @classmethod
    def deserialize(cls, data):
        params = json.loads(data)
        return cls(**params)

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['location-topic'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    data = json.loads(msg.value().decode('utf-8'))

    if data.get('type') == 'x':
        x_data = XData.deserialize(json.dumps(data))
        print('Received XData: {}'.format(x_data.__dict__))
    elif data.get('type') == 'y':
        y_data = YData.deserialize(json.dumps(data))
        print('Received YData: {}'.format(y_data.__dict__))
    elif data.get('type') == 't':
        t_data = TData.deserialize(json.dumps(data))
        print('Received TData: {}'.format(t_data.__dict__))

c.close()
