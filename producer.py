import tweepy
from kafka import KafkaProducer
from kafka.errors import KafkaError
from threading import Timer

TWITTER_CONSUMER_KEY = 'Ep01jtgTs4RsE7ReF31KPw55k'
TWITTER_CONSUMER_SECRET = 'LOPzfNaefiGmVBXZlk6VA8KvsNEAhbkk6Z6aUCwrXPzCWfZU9U'
TWITTER_ACCESS_TOKEN = '2184475730-8SbLhAvgHKIrDwnaB6m17vNbG6FktlUDitPeO6a'
TWITTER_TOKEN_SECRET = 'Q154rynXIYv9apz0u64YxRtyc3BhweV3CCCtiwMcZBGQT'

class IplStreamListener(tweepy.StreamListener):
	def __init__(self):
		self.kafka_producer = KafkaProducer(retries=5)
		self.flush_buffer()
		super().__init__()

	def on_status(self, status):
		print(status.text)
		self.kafka_producer.send('ipl-topic', status.text.encode()).add_callback(self.on_send_success).add_errback(self.on_send_error)
		
	
	def on_error(self, status_code):
		if status_code == 420:
			print('Disconnecting Twitter stream due to an Error!')
			return False

	def on_send_success(self, record_metadata):
		topic, partition, offset = record_metadata
		print('TOPIC: {}\t\tPARTITION: {}\t\tOFFSET: {}'.format(topic, partition, offset))

	def on_send_error(self, excp):
		print('Exception Occured', excp)

	def flush_buffer(self):
		flush_timer = Timer(60, self.flush_buffer) # Flush buffer async every 60 seconds
		flush_timer.daemon = True
		flush_timer.start()
		print('============================================================')
		print('!!!!!!!!!!!!!!!!!!!!!!!FLUSHING BUFFER!!!!!!!!!!!!!!!!!!!!!!!')
		print('============================================================')
		self.kafka_producer.flush()

class Producer:
	def __init__(self):
		print('Starting Producer...\n\n')
		auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
		auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_TOKEN_SECRET)
		self.api = tweepy.API(auth)
		print('Twitter API initialized')
		iplStreamListener = IplStreamListener()
		self.iplStream = tweepy.Stream(auth=self.api.auth, listener=iplStreamListener)
		print('Realtime Feed started!')
		self.iplStream.filter(track=['#RCBvsKXIP'], async=True)	

if __name__ == '__main__':
	Producer()