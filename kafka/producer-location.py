
from confluent_kafka import Producer
import csv

p = Producer({'bootstrap.servers': 'localhost:9092'})

with open('../textInputSources/locationSource.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        p.produce('location-topic', str(row))

p.flush()


# class
#  seriailzation and deserialisation sending data in byte format
#  streaming of the data through csv
# 
