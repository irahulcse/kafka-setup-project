from confluent_kafka import Producer
import csv
import json

class XData:
    def __init__(self, x):
        self.type = 'x'
        self.x = x

    def serialize(self):
        return json.dumps(self.__dict__)

class YData:
    def __init__(self, y):
        self.type = 'y'
        self.y = y

    def serialize(self):
        return json.dumps(self.__dict__)

class TData:
    def __init__(self, t):
        self.type = 't'
        self.t = t

    def serialize(self):
        return json.dumps(self.__dict__)

p = Producer({'bootstrap.servers': 'localhost:9092'})

with open('../textInputSources/locationSource.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(row.keys())  # Print the keys of the row
        x_data = XData(int(row['x']))
        y_data = YData(int(row['y']))
        t_data = TData(int(row['t']))
        p.produce('location-topic', x_data.serialize())
        p.produce('location-topic', y_data.serialize())
        p.produce('location-topic', t_data.serialize())

p.flush()