from kafka import KafkaConsumer

# Kafka broker configuration
bootstrap_servers = '135.125.219.92:9092'
group_id = 'my_consumer_group'
topics = ['employees', 'data', 'mec-xdr']

# Create Kafka consumer instance
consumer = KafkaConsumer(
    *topics,
    group_id=group_id,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode('utf-8')  # Deserialize message value
)

# Start consuming messages
try:
    for message in consumer:
        # Process the message
        print(f"Received message: {message.value}")
except KeyboardInterrupt:
    # Close the consumer on keyboard interrupt
    consumer.close()
