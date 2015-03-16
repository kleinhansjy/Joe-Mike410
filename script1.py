#script to upload my bulk data to Elastic Search
#used the following site as reference:
#http://blog.qbox.io/building-an-elasticsearch-index-with-python

from elasticsearch import Elasticsearch
import json
import codecs

from elasticsearch.connection import RequestsHttpConnection
gonnell2_index = "gonnell2_index"
es = Elasticsearch(connection_class=RequestsHttpConnection) #localhost 9200 by default


#delete index if already exists
if es.indices.exists(gonnell2_index):
	print("deleting gonnell2_index")
	res = es.indices.delete(index = gonnell2_index)
	print("response: '%s'" % (res))

request_body = {
	"settings" : {
		"number_of_shard": 1,
		"number_of_replicas":0
	}
}

print("creating gonnell2_index")
res = es.indices.create(index = gonnell2_index, body = request_body)
print( "response: '%s'" % (res))

#define mapping
print("defining mapping")
es.indices.put_mapping(
	index = gonnell2_index,
	doc_type='doc',
	body={
	'doc': {
		"properties" : {
		"doc_id" : {
			"type" : "string",
			"index": "not_analyzed"
		},
		"url" : {
			"type" : "string",
			"index": "not_analyzed"
		},
		"title" : {
			"type" : "string",
			"index": "analyzed",
			"analyzer" : "english",
			"fields": {
				"title_bm25": { "type": "string", "index": "analyzed", "similarity": "BM25"},
				"title_lmd": { "type": "string", "index": "analyzed", "similarity": "LMDirichlet"}
			}
		},
		"body" : {
			"type" : "string",
			"index": "analyzed",
			"analyzer" : "english",
			"fields": {
				"body_bm25": { "type": "string", "index": "analyzed", "similarity": "BM25"},
				"body_lmd": { "type": "string", "index": "analyzed", "similarity": "LMDirichlet"}
			}
		}
		}
	}
	}
)
#bulk index
path = "testData.json"
json_input = open(path, 'r')
json_data = json.loads(json_input.read())
print("finished reading json")
const_line = '{ "create": { "_index": "gonnell2_bigindex", "_type": "doc"}}'
count = 0
for i in range(0,3920):     #last is short (61)
    bulk_data = ''
    line_in = ''
    doc = json_data[count]
    if i == 3919:
        for k in range(0,60):
            line_in  = json.dumps(doc)
            line_in = line_in.decode('utf-8')
            bulk_data += const_line
            bulk_data += line_in
            count += 1
    else:    
        for j in range(0,100):
            line_in  = json.dumps(doc)
            line_in = line_in.decode('utf-8')
            bulk_data += const_line
            bulk_data += line_in
            count += 1
            
	print("bulk indexing...") 
	print(i)
	res = es.bulk(index = gonnell2_index, body = bulk_data, refresh = True, request_timeout = 100)
