from elasticsearch import Elasticsearch
from textblob import TextBlob

es = Elasticsearch()
res = es.search(index="twitter", doc_type="statuses", size=10000, body={"query": {
        "match_all": {}
    }})


for doc in res['hits']['hits']:
    text_analysis = TextBlob(doc['_source']['text'])
    print doc['_source']['text']
    print "Sentiment {}".format(text_analysis.sentiment)
    print

    ### RETRIEVING THE SENTIMENTS
    sentiment = text_analysis.sentiment

    ###MAKING SENTIMENT OF A TWEET 
    if sentiment.polarity < 0:
        analysis ='negative'
    elif sentiment.polarity > 0:
        analysis = 'positive'
    else:
        analysis = 'neutral'
    
    ###########################  INDEXING THE SENTIMENTS #######################
    
    es.index(index="sentiment_analysis",doc_type="tweets",
                 body={"author": doc['_source']['user_screen_name'],
                       "date": doc['_source']['timestamp'],
                       "message": doc['_source']['text'],
                       "source": doc['_source']['source'],
                       "polarity": sentiment.polarity,
                       "subjectivity": sentiment.subjectivity,
                       "sentiment": analysis})
