from pyspark.sql import Row
from xml.etree.ElementTree import fromstring
from datetime import datetime

def filter_headers(RDD, main_tag):
    def filter_crap(s):
        start = "<" + main_tag + ">"
        end = "</" + main_tag + ">"
        if s.startswith("<?xml version=") or s == start or s == end:
            return False
        else:
            return True
    filtered_RDD = RDD.filter(filter_crap)
    return filtered_RDD

def xml_to_dict(s, required_keys=[]):
    tree = fromstring(s)
    temp = dict((x, y) for x, y in tree.items())
    temp_keys = temp.keys()
    for key in required_keys:
        if key['name'] in temp_keys:
            if key['type'] == 'string':
                temp[key['name']] = str(temp[key['name']])
            elif key['type'] == 'float':
                temp[key['name']] = float(temp[key['name']])
            elif key['type'] == 'integer':
                temp[key['name']] = float(temp[key['name']])
            elif key['type'] == 'date':
                temp[key['name']] = datetime.strptime(temp[key['name']].split('T')[0], '%Y-%m-%d')
        else:
            temp[key['name']] = None
    return temp

def parse(sparkContext, path, SCHEMA, MAIN_TAG):
    text = sparkContext.textFile(path)
    no_headers = filter_headers(text, MAIN_TAG)
    parsed_to_dict = no_headers.map(lambda x: xml_to_dict(x, SCHEMA.jsonValue()['fields']))
    parsed = parsed_to_dict.map(lambda x: Row(**x))
    return parsed.toDF(schema=SCHEMA)

if __name__ == "__main__":
    a = fromstring('Comments.xml')
