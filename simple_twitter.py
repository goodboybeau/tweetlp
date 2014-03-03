"""
set up mongodb client
"""
from my_mongo import MyMongoClient
global MONGO_CLIENT
MONGO_CLIENT = MyMongoClient()

"""
set up the rest
"""
from twitter import *
import json

import sys, time

OAUTH_TOKEN="2331234997-CqgYy3oQQHeQRMPn2vTnu6YMerxfsIcqY6FAT79"
OAUTH_SECRET="Sn9uQtW7mnLt3qlv7kZyuzSfZ0QgqcHw6mrCsR7z8s0ve"
CONSUMER_KEY="YXETsoXQHwmAiTAEATs8wA"
CONSUMER_SECRET="EbShWrfFxfsCkLHCWp5DO64djbKLResDdm6pv5M9s"

def get_stream():
	stream = TwitterStream(auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET, 
			CONSUMER_KEY, CONSUMER_SECRET))
	return stream

def store_all_tweets_by(stream, ways=[]):
	start = time.time()
	count = 0
	f = open('tweets.json','wb')
	iterator = stream.statuses.sample()
	for tweet in iterator:
		if time.time() - start > 60:
			return count
		count += 1
		print count
		print iterator
		json.dump(tweet, f)
		if count > 10:
			exit()
		"""
		print json.dumps(tweet)
		
		try:
			if tweet['lang'] == "en":
				MONGO_CLIENT.store(tweet)
				for i in range(0,len(ways)):
					print "trying",ways[i]
					MONGO_CLIENT.store_by(ways[i], tweet)
		except Exception as e: 
			print "DB_CLIENT failed to store tweet:", e
		"""
if __name__ == "__main__":
	stream = get_stream()
	count = store_all_tweets_by(stream, ["coordinates", ["user", "id_str"], ["user", "location"]])
	print count * 60, "tweets per hour."
