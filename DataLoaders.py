from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
from abc import ABCMeta, abstractmethod
from xml.etree.ElementTree import fromstring

'''Loads xml as DataFrame'''

class StackExchangeParser:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        #self.spark = spark_session
        self.schema = self.__class__.SCHEMA
        self.fieldnames = self.schema.fieldNames()
        self.main_tag = self.__class__.MAIN_TAG

    def filter_headers(self, RDD):
        def filter_crap(s):
            start = "<" + self.main_tag + ">"
            end = "</" + self.main_tag + ">"
            if s.startswith("<?xml version=") or s == start or s == end:
                return False
            else:
                return True
        filtered_RDD = RDD.filter(filter_crap)

        return filtered_RDD

    def parse(self, rdd):
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

        #text = self.spark.sparkContext.textFile(path)
        #no_headers = self.filter_headers(text)
        parsed = rdd.map(lambda x: Row(**xml_to_dict(x, self.fieldnames)))
        #df = parsed.map(lambda x: Row(**x))
        #parsed.toDF(schema=self.schema)
        return parsed.toDF(schema=self.schema)

class UsersParser(StackExchangeParser):
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

def helper(sc, path):
    p = UsersParser()
    text = sc.sparkContext.textFile(path)
    rdd = p.filter_headers(text)
    rdd2 = p.parse(rdd)
    return rdd2
if __name__ == "__main__":
    sc = spark = SparkSession.builder \
    .appName('DataFrame') \
    .master('local[*]') \
    .getOrCreate()
    a = helper(sc, 'Users.xml')