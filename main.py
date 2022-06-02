import threading, time
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

kafkaBrokers = 'my-cluster-kafka-bootstrap-myproject.192.168.99.108.nip.io:443'
topic = 'my-topic'
cert = 'ca.crt'
password = 'password'

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=kafkaBrokers,
			          security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 ssl_cafile=cert,
                                 ssl_certfile=cert,
                                 #ssl_keyfile=keyLocation,
                                 ssl_password=password)

        while not self.stop_event.is_set():
            #producer.send(topic, b"test")
            producer.send(topic, b"Hello, World!")
            time.sleep(1)

        producer.close()


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=kafkaBrokers,
			          auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000,
                                 security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 ssl_cafile=cert,
                                 ssl_certfile=cert,
                                 #ssl_keyfile=keyLocation,
                                 ssl_password=password)
        consumer.subscribe([topic])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    # Create 'my-topic' Kafka topic
    #try:
        #admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        #topic = NewTopic(name='my-topic',
        #                 num_partitions=1,
        #                 replication_factor=1)
        #admin.create_topics([topic])
        #print('topic created')
    #except Exception e:
        #print(e)

    tasks = [
        #Producer(),
        Consumer()
    ]

    # Start threads of a publisher/producer and a subscriber/consumer to 'my-topic' Kafka topic
    for t in tasks:
        t.start()

    time.sleep(10)

    # Stop threads
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()
