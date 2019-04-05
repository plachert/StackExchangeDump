from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
from xml.etree.ElementTree import fromstring

'''Loads xml as DataFrame'''
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
        if key in temp_keys:
            pass
        else:
            temp[key] = None
    return temp

def parse_users(sparkContext, path):
    SCHEMA = StructType([ \
        StructField("Id", StringType(), True), \
        StructField("Reputation", StringType(), True), \
        StructField("CreationDate", StringType(), True), \
        StructField("DisplayName", StringType(), True), \
        StructField("LastAccessDate", StringType(), True), \
        StructField("WebsiteUrl", StringType(), True), \
        StructField("Location", StringType(), True), \
        StructField("AboutMe", StringType(), True), \
        StructField("Views", StringType(), True), \
        StructField("UpVotes", StringType(), True),
        StructField("DownVotes", StringType(), True), \
        StructField("EmailHash", StringType(), True), \
        StructField("Age", StringType(), True)])
    MAIN_TAG = 'users'
    text = sparkContext.textFile(path)
    no_headers = filter_headers(text, MAIN_TAG)
    parsed = no_headers.map(lambda x: Row(**xml_to_dict(x, SCHEMA.fieldNames())))
    return parsed.toDF(schema=SCHEMA)


if __name__ == "__main__":
    sc = SparkSession.builder \
    .appName('DataFrame') \
    .master('local[*]') \
    .getOrCreate()
    a = parse_users(sc.sparkContext, 'Users.xml')