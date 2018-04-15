from kafka import KafkaConsumer
from threading import Thread
import sched, time
from wordcloud import WordCloud

class Consumer:
	def __init__(self):
		print('Starting Consumer...\n\n')
		kafka_consumer = KafkaConsumer('ipl-topic', group_id='ipl-group')
		for message in kafka_consumer:
			decoded_message = message.value.decode('utf-8')
			with open("tweets.txt", "a",encoding='utf-8') as text_file:
    				print(decoded_message, file=text_file)
			# decoded_key = message.key.decode('utf-8')
			print('TOPIC: {}\nPARTITION: {}\nOFFSET: {}\nMESSAGE: {}\n\n\n' \
			.format(message.topic, message.partition, message.offset, decoded_message))


def do_something(sc): 
	
		stopWords = frozenset([
		"a", "about", "above", "across", "after", "afterwards", "again", "against",
		"all", "almost", "alone", "along", "already", "also", "although", "always",
		"am", "among", "amongst", "amoungst", "amount", "an", "and", "another",
		"any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are",
		"around", "as", "at", "back", "be", "became", "because", "become",
		"becomes", "becoming", "been", "before", "beforehand", "behind", "being",
		"below", "beside", "besides", "between", "beyond", "bill", "both",
		"bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con",
		"could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
		"down", "due", "during", "each", "eg", "eight", "either", "eleven", "else",
		"elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone",
		"everything", "everywhere", "except", "few", "fifteen", "fifty", "fill",
		"find", "fire", "first", "five", "for", "former", "formerly", "forty",
		"found", "four", "from", "front", "full", "further", "get", "give", "go",
		"had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter",
		"hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his",
		"how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed",
		"interest", "into", "is", "it", "its", "itself", "keep", "last", "latter",
		"latterly", "least", "less", "ltd", "made", "many", "may", "me",
		"meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly",
		"move", "much", "must", "my", "myself", "name", "namely", "neither",
		"never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone",
		"nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on",
		"once", "one", "only", "onto", "or", "other", "others", "otherwise", "our",
		"ours", "ourselves", "out", "over", "own", "part", "per", "perhaps",
		"please", "put", "rather", "re", "same", "see", "seem", "seemed",
		"seeming", "seems", "serious", "several", "she", "should", "show", "side",
		"since", "sincere", "six", "sixty", "so", "some", "somehow", "someone",
		"something", "sometime", "sometimes", "somewhere", "still", "such",
		"system", "take", "ten", "than", "that", "the", "their", "them",
		"themselves", "then", "thence", "there", "thereafter", "thereby",
		"therefore", "therein", "thereupon", "these", "they", "thick", "thin",
		"third", "this", "those", "though", "three", "through", "throughout",
		"thru", "thus", "to", "together", "too", "top", "toward", "towards",
		"twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
		"very", "via", "was", "we", "well", "were", "what", "whatever", "when",
		"whence", "whenever", "where", "whereafter", "whereas", "whereby",
		"wherein", "whereupon", "wherever", "whether", "which", "while", "whither",
		"who", "whoever", "whole", "whom", "whose", "why", "will", "with",
		"within", "without", "would", "yet", "you", "your", "yours", "yourself",
		"yourselves","https","co","RT"])

		# for w in list(word_dic):
		#     if w in stopWords:
		#         word_dic.pop(w,None)

		# print(len(word_dic))


		text = open('tweets.txt',encoding='utf-8').read()

		import matplotlib.pyplot as plt
		# lower max_font_size
		wordcloud = WordCloud(stopwords=stopWords,max_font_size=40).generate(text)
		plt.figure()
		plt.imshow(wordcloud, interpolation="bilinear")
		plt.axis("off")
		plt.show()
		s.enter(5, 1, do_something, (sc,)) #5ms


if __name__ == '__main__':
	consumer_thread1 = Thread(target=Consumer)
	consumer_thread2 = Thread(target=Consumer)
	consumer_thread1.start()
	consumer_thread2.start()
	
	s = sched.scheduler(time.time, time.sleep)
	s.enter(5, 1, do_something, (s,)) # 5ms
	s.run()
