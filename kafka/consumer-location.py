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

c = Consumer({
    'bootstrap.servers': 'glider.srvs.cloudkafka.com:9094',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'ozlwmnls',
    'sasl.password': 'nd4YYjvGiOsZgzlHRUG9cedDoPJJOyfQ',
    'group.id': 'ozlwmnls-mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['ozlwmnls-location-topic'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    location_data = LocationData.deserialize(msg.value().decode('utf-8'))
    print('Received message: {}'.format(location_data.__dict__))

c.close()