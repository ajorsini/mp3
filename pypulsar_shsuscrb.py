import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe('my-topic',
                            subscription_name='my-sub_2',
                            consumer_type=pulsar.ConsumerType.Shared)

m='start'
while m != 'exit':
    msg = consumer.receive()
    m = msg.data()
    print("Received message: '{}'".format(m))
    consumer.acknowledge(msg)

client.close()
