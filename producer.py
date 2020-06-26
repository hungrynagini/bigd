from kafka import KafkaProducer
from json import dumps, loads
import requests


def create_producer(address):
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=address,
                                 value_serializer=lambda x:
                                 dumps(x).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer


def publish_message(producer_instance, topic, value):
    try:
        producer_instance.send(topic, value=value)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


if __name__ == '__main__':
    topic = 'meetups'
    address = ['172.31.64.213:9092']
    producer = create_producer(address)
    print('producer done')
    stream = 'http://stream.meetup.com/2/rsvps'
    try:
        r = requests.get(stream, stream=True)

        if r.encoding is None:
            r.encoding = 'utf-8'

        for line in r.iter_lines(decode_unicode=True):
            if line:
                publish_message(producer, topic, loads(line))

    except KeyboardInterrupt:
        print("Interrupt")
        if producer is not None:
            producer.close()
