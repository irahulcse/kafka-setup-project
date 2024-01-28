import carla
from confluent_kafka import Producer
import json

class XData:
    def __init__(self, x):
        self.type = 'x'
        self.x = x

    def serialize(self):
        return json.dumps(self.__dict__)

# similar classes for YData and TData...

p = Producer({'bootstrap.servers': 'localhost:9092'})

client = carla.Client('localhost', 2000)
client.set_timeout(2.0) # seconds

world = client.get_world()
actor_list = world.get_actors()

for actor in actor_list:
    location = actor.get_location()
    x_data = XData(location.x)
    # create y_data and t_data...
    p.produce('location-topic', x_data.serialize())
    # produce y_data and t_data...

p.flush()
