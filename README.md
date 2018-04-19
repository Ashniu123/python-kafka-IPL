# python-kafka-IPL

Use Twitter feed to create a wordcloud from tweets.

## Usage
1. Run zookeeper and kafka broker
2. Create & activate a virtualenv and install dependencies by typing `pip install -r requirements.txt`
3. Insert **your** twitter API key and token in _producer.py_
4. Run `python producer.py #<sometrendyhashtag>` in one terminal
5. Run `python consumer.py` in the other terminal

![Generated Wordcloud](https://github.com/Ashniu123/python-kafka-IPL/blob/master/screenshot.png "Generated Wordcloud")