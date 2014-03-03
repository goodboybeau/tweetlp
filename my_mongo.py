import pymongo
import json

MONGO_HOST='localhost'
MONGO_PORT=27017

class MyMongoClient(object):
	def __init__(self):
		self._client = pymongo.MongoClient(MONGO_HOST,MONGO_PORT)
		self._db = self._client["TestTweets"]
		self._collection = self._db["CollectedTweets"]
		print "created"
	def store(self, tweet):
		self._collection.insert(tweet)
	
	# stores the tweet in a collection named
	#  tweet[key]
	def store_by(self, key, tweet):
		print "storing", key, tweet.keys()
		if type(key) == type([]):
			collection, val = self._rec_key(key, json.dumps(tweet))
			self._db[collection].insert({val:tweet})
			print "Stored tweet in",collection
			return

		if key not in tweet or tweet[key] == None:
			print "key not in tweet"
			return
		
		self._db[key].insert({tweet[key]:tweet})
		print "Stored tweet in", key

	def _rec_key(self, keys, tweet):
		if len(keys) == 1:
			return keys[0], tweet[keys]
		else:
			key = keys[0]
			db_collection_name_part, db_entry = self._rec_key(keys.remove(key),tweet)
			print key
			return key+"_"+db_collection_name_part, db_entry
