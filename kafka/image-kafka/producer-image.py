import os
import json
import base64
from confluent_kafka import Producer

class ImageData:
    def __init__(self, filename, content):
        self.filename = filename
        self.content = content  # Base64-encoded image content

    def serialize(self):
        return json.dumps(self.__dict__)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

p = Producer({
    'bootstrap.servers': '192.168.221.213:9092', 
    'queue.buffering.max.messages': 200000,
})

for filename in os.listdir('../../image_resources/images/'):
    if filename.endswith('.png'):  # Assuming the images are PNGs
        with open(os.path.join('../../image_resources/images/', filename), 'rb') as f:
            content = base64.b64encode(f.read()).decode('utf-8')
        image_data = ImageData(filename, content)
        p.produce('image-topic', image_data.serialize(), callback=delivery_report)
        p.flush()  # Ensure the message is immediately sent

p.flush()