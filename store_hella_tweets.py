from twitter import *
import json, threading, zmq

import sys, time, os, datetime

"""
set up mongodb client
"""
'''
from my_mongo import MyMongoClient
global MONGO_CLIENT
MONGO_CLIENT = MyMongoClient()
'''
#OAuth Stuff
OAUTH_TOKEN="2331234997-CqgYy3oQQHeQRMPn2vTnu6YMerxfsIcqY6FAT79"
OAUTH_SECRET="Sn9uQtW7mnLt3qlv7kZyuzSfZ0QgqcHw6mrCsR7z8s0ve"
CONSUMER_KEY="YXETsoXQHwmAiTAEATs8wA"
CONSUMER_SECRET="EbShWrfFxfsCkLHCWp5DO64djbKLResDdm6pv5M9s"

global TWEETS_PER_FILE
TWEETS_PER_FILE = 1000

# @return a handle to the twitter api
def get_twitter():
	twit = Twitter(auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET, 
			CONSUMER_KEY, CONSUMER_SECRET))
	return twit

# @return a handle to the public twitter stream sample
def get_stream():
	stream = TwitterStream(auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET, 
			CONSUMER_KEY, CONSUMER_SECRET))
	return stream

def FILTER_OUT_DELETE_TWEETS(tweet):
	return not tweet.get('delete')

# @return 	
# 	true if tweet is not a delete tweet and is in english
#	false otherwise
def FILTER_ENGLISH_TWEETS(tweet):
	if not tweet.get('delete'):
		return tweet['lang'] == 'en'
	return False

# @return
#	true if FILTER_ENGLISH_TWEETS and has coordinates
#	false otherise
def FILTER_LOCATION_TWEETS(tweet):
	return FILTER_ENGLISH_TWEETS(tweet) and tweet.get('coordinates') is not None

# writes file to filename as a json
def write_tweet_to_file(tweet, filename):
	filename.write(json.dumps(tweet))

# non-blocking socket... timeout of value 'timetick'
# @return json from socket
#	  None if timeout
def poll_socket(socket, timetick = 100):
	global STOP
	poller = zmq.Poller()
	poller.register(socket, zmq.POLLIN)
	# wait up to 100msec
	try:
		while True:
			obj = dict(poller.poll(timetick))
			if socket in obj and obj[socket] == zmq.POLLIN:
				yield socket.recv()
			else:
				yield None
	except KeyboardInterrupt:
		exit()
	# Escape while loop if there's a keyboard interrupt.

class TweetPusher(threading.Thread):

	STOP_FLAG 	= False
	PAUSE_FLAG 	= False

	def __init__(self, stream, down_addr, count=None, time=None):
		if count is None and time is None:
			print ("count and time is none in TweetPusher")
			self._time_limit = 10
			self._count_limit = 10
		else:
			self._time_limit = time
			self._count_limit = count

		threading.Thread.__init__(self)
			
		self._twitter_stream 	= stream
		self._downstream_addr 	= down_addr
		
		self._context 		= zmq.Context()
		
		self._downstream_socket = self._context.socket(zmq.PUSH)
		self._downstream_socket.bind(self._downstream_addr)
		print "Pusher created"
	
	def __delete__(self):
		try:
			self._downstream_socket.close()
		except Exception as e:
			print str(e)
		try:
			del self._twitter_stream
		except Exception as e:
			print str(e)

		threading.Thread.__delete__(self)

	@staticmethod
	def stop():
		TweetPusher.STOP_FLAG = True
		TweetPusher.PAUSE_FLAG = True
	@staticmethod
	def resume():
		TweetPusher.PAUSE_FLAG = False
	@staticmethod
	def pause():
		TweetPusher.PAUSE_FLAG = True
	
	def run(self):
		count = 0
	 	start = time.time()
		try:
			statuses = self._twitter_stream.statuses.sample()
			while not TweetPusher.STOP_FLAG:
				while not TweetPusher.PAUSE_FLAG:
					while time.time() - start < self._time_limit or count < self._count_limit:
						try:
							tweet = statuses.next()
						except Exception as e:
							print 'couldnt get next in pusher'
							try:
								self._twitter_stream = get_stream()
								statuses = self._twitter_stream.statuses.sample()
							except Exception as e:
								print ('No stream available!')
								SHUTDOWN()
						else:
							if FILTER_OUT_DELETE_TWEETS((tweet)):
								self._downstream_socket.send(json.dumps(tweet))
								count += 1
					
					if time.time() - start > self._time_limit or count >= 10:
						TweetPusher.PAUSE_FLAG = True
						TweetPusher.STOP_FLAG = True
				time.sleep(1)
		except Exception as e:
			print "Exception in TweetPusher:", str(e)

		print "Pusher pushed %d tweets downstream" % count
		TweetFilter.STOP_WHEN_READY = True

PATH_TO_WRITE = '/home/ubuntu/usable_tweets/timelines/'

class TweetFilter(threading.Thread):
	
	STOP_WHEN_READY = False
	STOP_FLAG 	= False
	PAUSE_FLAG	= False

	def __init__(self, up_addr, down_addr, _filter):
		threading.Thread.__init__(self)
		self._filter 		= _filter
		self._upstream_addr 	= up_addr
		self._downstream_addr	= down_addr
		
		self._context		= zmq.Context()
		
		self._upstream_socket 	= self._context.socket(zmq.PULL)
		self._upstream_socket.connect(self._upstream_addr)
		
		self._downstream_socket = self._context.socket(zmq.PUSH)
		self._downstream_socket.connect(self._downstream_addr)
		
		print 'TweetFilter created'

	def __delete__(self):
		try:
			self._upstream_socket.close()
		except Exception as e:
			print str(e)
		try:
			self._downstream_socket.close()
		except Exception as e:
			print str(e)
		threading.Thread.__delete__(self)
	@staticmethod
	def stop():
		TweetFilter.STOP_FLAG = True
		TweetFilter.PAUSE_FLAG = True

	@staticmethod
	def pause():
		TweetFilter.PAUSE_FLAG = True
	
	@staticmethod
	def resume():
		TweetFilter.PAUSE_FLAG = False

	def run(self):
		count = 0
		start = time.time()
		try:
			while not TweetFilter.STOP_FLAG:
				non_blocking_socket = poll_socket(self._upstream_socket)
				while not TweetFilter.PAUSE_FLAG:
					tweet = non_blocking_socket.next()
					if tweet is not None:
						tweet = json.loads(tweet)
						if self._filter(tweet):
							self._downstream_socket.send(str(tweet['user']['screen_name']))
							count += 1
					else:
						if TweetFilter.STOP_WHEN_READY:
							TweetConsumer.STOP_WHEN_READY = True
							TweetFilter.stop()
						time.sleep(1)
		except Exception as e:
			print "Exception in TweetFilter:",str(e)
		print "TweetFilter pushed %d tweets downstream" % count
		#self._callback_when_done()

class TweetConsumer(threading.Thread):
	
	STOP_WHEN_READY = False
	STOP_FLAG	= False
	PAUSE_FLAG	= False

	def __init__(self, up_addr, location):
		threading.Thread.__init__(self)
		self._location		= location
		self._upstream_addr	= up_addr
		
		self._context		= zmq.Context()
		self._upstream_socket	= self._context.socket(zmq.PULL)
		self._upstream_socket.bind(self._upstream_addr)
		
		self._twitter		= get_twitter()
	
		print 'TweetConsumer created binded to',self._upstream_addr
	@staticmethod
	def stop():
		TweetConsumer.STOP_FLAG = True
		TweetConsumer.PAUSE_FLAG = True

	@staticmethod
	def pause():
		TweetConsumer.PAUSE_FLAG = True
	
	@staticmethod
	def resume():
		TweetConsumer.PAUSE_FLAG = False

	def __del__(self):
		self._upstream_socket.close()
		del self._location

	def run(self):
		timeline_count = 0
		tweet_count = 0
		start = time.time()
		try:
			while not TweetConsumer.STOP_FLAG:
				non_blocking_socket = poll_socket(self._upstream_socket)
				while not TweetConsumer.PAUSE_FLAG:
					user = non_blocking_socket.next()
					if user is not None:
						FILENAME = PATH_TO_WRITE+user+'.twitter'
						if not os.path.isfile(FILENAME):
							with open(FILENAME,'w') as FILE:
								for tweet in self._twitter.statuses.user_timeline(screen_name=user, count = 200):
									FILE.write(json.dumps(tweet)+'\n')
									tweet_count += 1
						timeline_count += 1
					else:
						if TweetConsumer.STOP_WHEN_READY:
							TweetConsumer.stop()
						time.sleep(1)
		except Exception as e:
			print "Exception in TweetConsumer:",str(e)
		print "TweetConsumer wrote %d tweets from %d timelines in %d seconds" % (tweet_count, timeline_count, int(time.time()-start))

def SHUTDOWN():
	print "SHUTDOWN called!"
	TweetPusher.PAUSE_FLAG 		= True
	TweetPusher.STOP_FLAG 		= True
	TweetFilter.PAUSE_FLAG		= True
	TweetFilter.STOP_FLAG		= True
	TweetConsumer.PAUSE_FLAG	= True
	TweetConsumer.STOP_FLAG		= True

# locations to store tweets
class FileSaver(object):
	@staticmethod
	def get_unique_filename(prefix='', suffix=''):
		return prefix + (''.join(str(datetime.datetime.now()).split())).replace('.','').replace('-','').replace(':','') + suffix

	def __init__(self, prefix='tweets', suffix='.json'):
		self._prefix 	= prefix
		self._suffix	= suffix
		self._count 	= 1
		self._filename 	= FileSaver.get_unique_filename(self._prefix, self._suffix)

		try:
			self._file = open(self._filename,'w')
		except Exception as e:
			raise("couldn't open file: %s!" % self._filename)
		self._start_time = time.time()
		print 'FileSaver created'

	def __delete__(self):
		try:
			self._file.close()
		except Exception as e:
			print str(e)
		finally:
			print 'wrote %d tweets in %d seconds' % (self._count, int(time.time() - self._start_time))
	
	def store(self, tweet):
		self._file.write(tweet)
		self._count += 1
		if self._count % TWEETS_PER_FILE == 0:
			try:
				self._file.close()
				self._filename = FileSaver.get_unique_filename(self._prefix, self._suffix)
				self._file.open(self._filename, 'w')
			except Exception as e:
				print str(e)
				SHUTDOWN()		
'''
class MongoSaver(object):
	def __init__(self, db, collection):
'''

# attempts to store the tweet if 'delete' is not in the keys
#  @return 
#  a,b where 
#  a is the number of stored tweets and
#  b is the number of tweets with coordinates
def store_tweet(tweet):
	global WRITE_FILE
	global TWEET_FILE
	# if the tweet as the delete key, then fuck it
	if tweet.get('delete') is not None:
		return 0, 0

	if tweet['lang'] == 'en':
		print str(e)
	else:
		return 0,0

	# result of this store attempt
	stored_tweet, has_coord = 1, 0
	
	# if coordinates field has an entry
	if tweet.get('coordinates') is not None:
		#MONGO_CLIENT.store_loc(tweet)
		WRITE_FILE.write(json.dumps(tweet, indent=4) + '\n')
		has_coord = 1

	# store the tweet no matter what
	#MONGO_CLIENT.store(tweet)
	TWEET_FILE.write(json.dumps(tweet))
	return stored_tweet, has_coord

# collects until SECONDS seconds has gone by...
# attempts to collect 10 tweets at a time
def collect_until(stream, SECONDS):
	start = time.time()
	tweets, locations = 0, 0
	while time.time() - start < SECONDS:
		print "tweets: %d, with_loc:%d" %(tweets, locations)
		result = collect_many(stream, 10)
		tweets += result[0]
		locations += result[1]
	return tweets, locations

def collect_many(stream, COUNT):
	start = time.time()
	tweets, locations = 0, 0
	x = 0
	while x < COUNT:
		tweet = stream.next()
		result = store_tweet(tweet)
		tweets += result[0]
		x += result[0]
		locations += result[1]
	return tweets, locations

if __name__ == "__main__":
	try:
		# default time to run in seconds
		TIME 		= 30
		if '-t' in sys.argv:
			try:
				TIME 	= int(sys.argv[sys.argv.index('-t')+1])
			except Exception as e:
				print 'using default time of %d seconds' % TIME

		# default number of filters
		NO_OF_FILTERS = 2
		# user number of filters
		if '-c' in sys.argv:
			try:
				NO_OF_FILTERS = int(sys.argv[sys.argv.index('-c')+1])
			except Exception as e:
				print 'using default # of filters %d' % NO_OF_FILTERS 
		
		# default filename
		FILENAME = 'tweets'
		# user filename
		if '-f' in sys.argv:
			try:
				FILENAME = sys.argv[sys.argv.index('-f')+1]
			except Exception as e:
				print 'using dfault filename %s' % FILENAME
		
		# collector push
		COLLECTOR_PUSH 	= 'ipc://127.0.0.1:9991'
		# filter listen
		FILTER_LISTEN	= COLLECTOR_PUSH
		# filter push
		FILTER_PUSH	= 'ipc://127.0.0.1:9992'
		# writer listen
		WRITER_LISTEN 	= FILTER_PUSH
		# writer push
		WRITER_PUSH	= FileSaver(FILENAME)
		
		Collectors = []
		# 1 collector for now
		Collector = TweetPusher    (get_stream(),  COLLECTOR_PUSH, time = TIME, count=100)
		# many filters
		Filters		= []
		for x in range(NO_OF_FILTERS):
			Filters.append(TweetFilter(FILTER_LISTEN, FILTER_PUSH, FILTER_ENGLISH_TWEETS))
		
		# one writer for now
		Writer		= TweetConsumer (WRITER_LISTEN, WRITER_PUSH)
		
		# start all players
		Collector.start()
		for Filter in Filters:
			Filter.start()
		Writer.start()
		
		# join all players
		Collector.join()
		for Filter in Filters:
			Filter.join()

		Writer.join()
		
	except Exception as e:
		print str(e)
	
		'''
		global WRITE_FILE
		global TWEET_FILE
		if len(sys.argv) < 2:
			print "-c specifies count of tweets"
			print "-t time in seconds to collect tweets"
			exit()
		
		func, val = None, None
		if sys.argv[1] == '-t':
			if sys.argv[2].isdigit():
				func, val = collect_until, int(sys.argv[2])
		else:
			if sys.argv[2].isdigit():
				func, val = collect_many, int(sys.argv[2])
		if len(sys.argv) > 2:
			try:
				if sys.argv[3] == '-f':
					TWEET_FILE = sys.argv[4]
			except Exception as e:
				print str(e)
	
		if func is not None:
			WRITE_FILE = open('location_'+TWEET_FILE,'w')
			TWEET_FILE = open(TWEET_FILE,'w')
			stream = get_stream()
			sample = stream.statuses.sample()
			func(sample, val)
		'''
