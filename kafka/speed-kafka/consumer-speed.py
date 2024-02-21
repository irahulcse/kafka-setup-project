from confluent_kafka import Consumer, KafkaError
import json

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

c.subscribe(['speed-topic'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    data = SpeedData.deserialize(msg.value().decode('utf-8'))
    print(f'Received message on topic {msg.topic()}: {data.__dict__}')

c.close()