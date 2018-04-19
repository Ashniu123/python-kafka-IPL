import tweepy
from kafka import KafkaProducer
from kafka.errors import KafkaError
from threading import Timer
from wordcloud import WordCloud
import sys

TWITTER_CONSUMER_KEY = 'YOUR CONSUMER KEY'
TWITTER_CONSUMER_SECRET = 'YOUR CONSUMER SECRET'
TWITTER_ACCESS_TOKEN = 'YOUR ACCESS TOKEN'
TWITTER_TOKEN_SECRET = 'YOUR ACCESS TOKEN SECRET'

class IplStreamListener(tweepy.StreamListener):
	def __init__(self):
		
		self.kafka_producer = KafkaProducer(retries=5,api_version=(1,1,0))
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
		# search topic has to be specified thorough cli args
		self.iplStream.filter(track=sys.argv[1:], async=True)

if __name__ == '__main__':
	if len(sys.argv[1:]) == 0:
		print('Please specify topic(s)\n\n \
			USAGE:\n \
			python producer.py topic1 topic2 ...\n \
			Exiting')
		sys.exit(1)
	Producer()