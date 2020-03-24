from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker
import json

producer = KafkaProducer(bootstrap_servers=['35.208.65.122:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

fake = Faker()
random_int = fake.random_int(0, 100)
random_name = fake.name()
customer_id = str(fake.random_int(0, 10))
# produce asynchronously with callbacks
producer.send('orders', {'random-name': random_name, 'random-int': random_int}, str.encode(customer_id)).add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()