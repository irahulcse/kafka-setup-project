import os
import json
import base64
from confluent_kafka import Consumer, KafkaError

class ImageData:
    def __init__(self, filename, content):
        self.filename = filename
        self.content = content  # Base64-encoded image content

    @classmethod
    def deserialize(cls, data):
        params = json.loads(data)
        return cls(**params)

c = Consumer({
    'bootstrap.servers': '192.168.221.213:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['image-topic'])

while True:
    msg = c.poll(0.1)  # Reduced the timeout to make polling more frequent
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    try:
        image_data = ImageData.deserialize(msg.value().decode('utf-8'))
        print('Received message: {}'.format(image_data.__dict__))

        # Save the image to a file
        with open(f'../../getImage/{image_data.filename}', 'wb') as f:
            f.write(base64.b64decode(image_data.content))
    except json.decoder.JSONDecodeError:
        print(f"Unable to parse message: {msg.value()}")

c.close()

# from confluent_kafka import Consumer, KafkaError
# import json
# import base64

# class ImageData:
#     def __init__(self, filename, content):
#         self.filename = filename
#         self.content = content  # Base64-encoded image content

#     @classmethod
#     def deserialize(cls, data):
#         params = json.loads(data)
#         return cls(**params)

# c = Consumer({
#     'bootstrap.servers': 'localhost:9092',
#     'group.id': 'mygroup',
#     'auto.offset.reset': 'earliest'
# })

# c.subscribe(['image-topic'])

# while True:
#     msg = c.poll(1.0)
#     if msg is None:
#         continue
#     if msg.error():
#         print("Consumer error: {}".format(msg.error()))
#         continue

#     if msg.topic() == 'image-topic':
#         data = ImageData.deserialize(msg.value().decode('utf-8'))
#         # Decode the base64 content back into bytes
#         content = base64.b64decode(data.content)
#         # Now you can write the content to a file, or process it directly in your application

#     print(f'Received message on topic {msg.topic()}: {data.filename}')

# c.close()