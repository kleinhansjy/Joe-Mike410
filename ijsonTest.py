__author__ = 'Joe Kleinhans'

import json
import ijson
from ijson import backends

from ijson import items



path = "testData.json"
json_input = open(path, 'r')

for item in ijson.items(json_input, "item"):
    print('{"doc_id": "' + item['doc_id'] + '", "url": "' + item['url'] + '", "title": "' + item['title'] + '", "body": "'\
    + item['body'] + '"}\n')


"""
data = json.loads(json_input.read())
bulk_index = ''
for doc in data:
    # one_line = '{"doc_id": "' + doc.doc_id + '", "url": "' + doc.url + '", "title": "' + doc.title + '", "body": "' + doc.body + '"},'
    print(json.dumps(doc))
"""