from confluent_kafka import Producer
import csv
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

class LocationData:
    def __init__(self, x, y, t):
        self.x = x
        self.y = y
        self.t = t

    def serialize(self):
        return json.dumps(self.__dict__)

class FileChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        p = Producer({'bootstrap.servers': 'localhost:9092'})
        with open('../textInputSources/locationSource.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                location_data = LocationData(int(row['x']), int(row['y']), int(row['t']))
                serialized_data = location_data.serialize()
                print('Sending message: {}'.format(serialized_data))  # Print the data being sent
                p.produce('location-topic', serialized_data)
        p.flush()

observer = Observer()
event_handler = FileChangeHandler()
observer.schedule(event_handler, path='../textInputSources/locationSource.csv')
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()