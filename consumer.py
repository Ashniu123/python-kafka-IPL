from kafka import KafkaConsumer
from threading import Thread
class Consumer:
	def __init__(self):
		print('Starting Consumer...\n\n')
		kafka_consumer = KafkaConsumer('ipl-topic', group_id='ipl-group')
		for message in kafka_consumer:
			decoded_message = message.value.decode('utf-8')
			# decoded_key = message.key.decode('utf-8')
			print('TOPIC: {}\nPARTITION: {}\nOFFSET: {}\nMESSAGE: {}\n\n\n' \
			.format(message.topic, message.partition, message.offset, decoded_message))

if __name__ == '__main__':
	consumer_thread1 = Thread(target=Consumer)
	consumer_thread2 = Thread(target=Consumer)
	consumer_thread1.start()
	consumer_thread2.start()