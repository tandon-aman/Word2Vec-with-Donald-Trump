from pymongo import MongoClient
import elasticsearch
import sys

prompt = "> "

## Getting the mongodb database name from user
print('Which database you wants to connect?')
db_name = raw_input(prompt)

## Getting the mongodb collection name from user
print('Which collection/table of %s database you wants to query on?' %(db_name))
table_name = raw_input(prompt)

## Getting the elasticsearch collection name from user
print('Elasticsearch Collection Name ?')
collection_name = raw_input(prompt)

#### CREATING CONNECTION WITH MONGO DB ######################

client = MongoClient('localhost:27017')
db = client[db_name]


#######  QUERYING AND RETRIEVING THE CURSOR  #######################

cursor = db[table_name].find({},{'_id': False})

###### CONNECT TO ES #############
es = elasticsearch.Elasticsearch([{'host':'localhost', 'port':9200}])

i = 1
for doc in cursor:
	es.index(index=collection_name, doc_type='statuses', id=i, body=doc)
	i=i+1
