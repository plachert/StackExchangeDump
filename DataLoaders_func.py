from pyspark.sql import SparkSession
from file_parsers import parse_badges, parse_comments, parse_posts


if __name__ == "__main__":
    sc = SparkSession.builder \
    .appName('DataFrame') \
    .master('local[*]') \
    .getOrCreate()
    a = parse_badges(sc.sparkContext, 'Badges.xml')