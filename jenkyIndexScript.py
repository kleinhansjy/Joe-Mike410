#script to upload my bulk data to Elastic Search
#used the following site as reference:
#http://blog.qbox.io/building-an-elasticsearch-index-with-python


def put_in_one_line(file_descriptor):
    one_line  = ''
    temp = ''
    for x in range(0,6):
        temp = file_descriptor.readline()
        temp = temp.replace('\n', '').replace('\r','').replace('\t', '').replace("    ",'')
        one_line += temp
    one_line = one_line[:-1]
    one_line += '\n'
    return one_line

if __name__ == '__main__':
    from elasticsearch import Elasticsearch
    import json
    import codecs

    from elasticsearch.connection import RequestsHttpConnection
    klenhns2_bigindex = "klenhns2_bigindex"

    print("pre connection")
    es = Elasticsearch(connection_class=RequestsHttpConnection) #localhost 9200 by default
    print("connected")

    #delete index if already exists
    if es.indices.exists(klenhns2_bigindex):
        print("deleting klenhns2_index")
        res = es.indices.delete(index = klenhns2_bigindex)
        print("response: '%s'" % (res))

    request_body = {
        "settings" : {
            "number_of_shard": 1,
            "number_of_replicas":0
        }
    }

    print("creating klenhns2_index")
    res = es.indices.create(index = klenhns2_bigindex, body = request_body)
    print( "response: '%s'" % (res))

    #define mapping
    print("defining mapping")
    es.indices.put_mapping(
        index = klenhns2_bigindex,
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
    path_old = "C:\Users\Joe Kleinhans\Desktop\New folder\Docs.txt"
    path = "testData.json"
    fd = open(path_old, 'r')
    fd.readline()               #throw out first and implicitly last so   2396819 left
    const_line = '{ "create": { "_index": "klenhns2_bigindex", "_type": "doc"}}'
    for i in range(0,2396820):     #throw out the last line
        bulk_data = ''
        line_in = ''
        if i == 3919:
            for k in range(0,61):
                line_in  = put_in_one_line(fd)
                line_in = line_in.decode('utf-8')
                bulk_data += const_line
                bulk_data += line_in
        else:
            for j in range(0,100):
                line_in  = put_in_one_line(fd)
                line_in = line_in.decode('utf-8')
                bulk_data += const_line
                bulk_data += line_in

        print("bulk indexing...")
        print(i)
        res = es.bulk(index = klenhns2_bigindex, body = bulk_data, refresh = True, request_timeout = 100)
