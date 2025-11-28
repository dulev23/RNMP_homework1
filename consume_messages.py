from kafka import KafkaConsumer

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = ["result1", "result2"]
# TODO change the name of the topic here (result1 or result2) to verify that Flink produces the wanted results

# Create a Kafka consumer
consumer = KafkaConsumer(*topic,
                         bootstrap_servers=bootstrap_servers,
                         group_id='my_consumer_group',
                         auto_offset_reset='earliest',  # Start reading from the beginning if no offset is stored
                         enable_auto_commit=False)  # Disable auto-commit to manually control offsets

try:
	for message in consumer:
		print(f"Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
	pass
finally:
	# Close the consumer
	consumer.close()
