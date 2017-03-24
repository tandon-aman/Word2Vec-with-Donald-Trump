import argparse
import sys,time

parser = argparse.ArgumentParser()
parser.add_argument("start", type=str, choices=['start'], help="This will start the scrapping script")

#if 
args = parser.parse_args()
if args.start:
	print "Starting the crawler"
	for i in range(3):
		sys.stdout.write(".")
		sys.stdout.flush()
		time.sleep(.4)
	print

#initialilizing mongodb connection & creating the database
from pymongo import MongoClient

#creating the mongo client
client = MongoClient('localhost:27017')

#creating the sentiments database 
db = client["sentiments"]
twitterdb = db["donaltrump"]

#setting the prompt variable
prompt = '> '

options = {'1':'Twitter'}

#"".join(key,val) for key, val in options.items()
	
print('Which of the following you wants to crawl %s ?' %(options) )
choice = raw_input(prompt)

print("Starting the crawler for %s " % (options.get(str(choice))))
for i in range(3):
	sys.stdout.write(".")
	sys.stdout.flush()
	time.sleep(.21)
print

################################################

def getMonth(month):
	months = {'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06',
			'JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}
	return months.get(month.upper())


#################################################

def getDateInTZFormat(date_value):
	new_date_value = ''
	#Sat Feb 25 17:27:23 +0000 2017
	if(date_value != ''):
		date = date_value.split(' ')
		time = date[3]
#		time = time.replace(':','')
		year = date[-1]
		month = getMonth(date[1])
		month_date = date[2]
		new_date_value = str(year)+'-'+str(month)+'-'+str(month_date)+'T'+str(time)+'Z'
	return new_date_value 

################################################### TWITTER ##################################################

def retriveTwitterStatuses():
	import twitter
	import json

	total_number_of_tweets = 0
	tweet_texts = {}

	############################  AUTHORIZATION KEYS(PLEASE CHANGE WITH YOURS)  #################################

	CONSUMER_KEY = 'RJ1pTbwuz'
	CONSUMER_SECRET = 'DjWCME0lMg'
	OAUTH_TOKEN = 'QAxr3L1DBKvV7G'
	OAUTH_TOKEN_SECRET = 'YpRtdX4vFwNCkSb1y'

	auth = twitter.oauth.OAuth(OAUTH_TOKEN,OAUTH_TOKEN_SECRET,CONSUMER_KEY,CONSUMER_SECRET)

	###########################  TWITTER API  #################################

	twitter_api = twitter.Twitter(auth=auth)
	
	########################## PROMPTING THE HASHTAG FROM USER  ################################
	print('Please provide the hash tag to find the statuses for.. e.g. #MakeInIndia')
	hashtag = raw_input(prompt)

	print('Please provide the number of hashtags to crawl e.g. 100')
	count = raw_input(prompt)

	print('Getting the %s statuses from %s for the hashtag %s ..' % (count, options.get(choice), hashtag))


	##########################  SEARCHING HASHTAG ON TWITTER  ##########################
	while (total_number_of_tweets < int(count)):
		try:
			try:
				tweets = []
				print "tweet length is now %d and count is %d " % (total_number_of_tweets, int(count))
		
				search_results = twitter_api.search.tweets(q=hashtag, count=count)
			
				for result in search_results['statuses']:
					tweet = {}
					tweet['text'] = result['text']
					if(tweet['text'] != '' and result['lang']=='en' and tweet['text'] not in tweet_texts):
						tweet['user_screen_name'] = result.get('user',{}).get('screen_name','')
						tweet['user_name'] = result.get('user',{}).get('name','')
						tweet['retweet_count'] = result.get('retweeted_status',{}).get('retweet_count','')
						tweet['retweeted_name'] = result.get('retweeted_status',{}).get('user',{}).get('name',"")
						tweet['retweeted_screen_name'] = result.get('retweeted_status',{}).get('user',{}).get('screen_name','')	
						tweet['timestamp'] = getDateInTZFormat(result['created_at'])
						usermention = result.get('entities',{}).get('user_mentions',{})
						if len(usermention) > 0:
							tweet['tag_screen_name'] = usermention[0].get('screen_name','')
							tweet['tag_name'] = usermention[0].get('name','')
						else :
							tweet['tag_screen_name'] = ''
							tweet['tag_name'] = ''
							
						if(result["geo"]):
							latitude = result["geo"]["coordinates"][0]
							longitude = result["geo"]["coordinates"][1]
							tweet['location'] = "%s,%s" %(latitude,longitude) 
					
						tweet['source']="twitter"
						tweet_texts[tweet['text']] = 1
						tweets.append(tweet)
				total_number_of_tweets = total_number_of_tweets + len(tweets)

				#insert data in sentiments database
				try:
					twitterdb.insert(tweets)
				except:
					print "Error occured while inserting the data %s" % (str(e))

			except Exception as e:
				print 'some exception occured'+str(e)

			print "Going for sleep for 10 secs"
			time.sleep(10)
		except KeyboardInterrupt as e:
			print "\nCTRL+C event occured, exiting now.."
			sys.exit(0) 

#	print json.dumps(tweets,indent=1)
#	print json.dumps(search_results['statuses'],indent=1)
	print "\n\nTotal numbers of tweets collected is %d" %(len(tweets))
	print "\n\nTotal numbers of original tweets collected is %d" %(len(search_results['statuses']))
	return tweets

if options.get(choice) == 'Twitter':
	retriveTwitterStatuses()

