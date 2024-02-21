from confluent_kafka import Consumer, KafkaError
import json
import base64

class ImageData:
    def __init__(self, filename, content):
        self.filename = filename
        self.content = content  # Base64-encoded image content

    @classmethod
    def deserialize(cls, data):
        params = json.loads(data)
        return cls(**params)

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['image-topic'])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    if msg.topic() == 'image-topic':
        data = ImageData.deserialize(msg.value().decode('utf-8'))
        # Decode the base64 content back into bytes
        content = base64.b64decode(data.content)
        # Now you can write the content to a file, or process it directly in your application

    print(f'Received message on topic {msg.topic()}: {data.filename}')

c.close()