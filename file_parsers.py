from pyspark.sql.types import StructType, StructField, StringType, FloatType
from parser_utils import parse

def parse_badges(sparkContext, path):
    SCHEMA = StructType([ \
        StructField("UserId", FloatType(), True), \
        StructField("Name", StringType(), True), \
        StructField("Date", StringType(), True)])
    MAIN_TAG = 'badges'
    return parse(sparkContext, path, SCHEMA, MAIN_TAG)

def parse_comments(sparkContext, path):
    SCHEMA = StructType([ \
        StructField("Id", FloatType(), True), \
        StructField("PostId", FloatType(), True), \
        StructField("Score", FloatType(), True), \
        StructField("Text", StringType(), True), \
        StructField("CreationDate", StringType(), True), \
        StructField("UserID", FloatType(), True)])
    MAIN_TAG = 'comments'
    return parse(sparkContext, path, SCHEMA, MAIN_TAG)

def parse_posts(sparkContext, path):

    SCHEMA = StructType([ \
        StructField("Id", FloatType(), True), \
        StructField("PostTypeId", FloatType(), True), \
        StructField("ParentID", FloatType(), True), \
        StructField("AcceptedAnswerId", FloatType(), True), \
        StructField("CreationDate", StringType(), True), \
        StructField("Score", FloatType(), True), \
        StructField("ViewCount", FloatType(), True), \
        StructField("Body", StringType(), True), \
        StructField("OwnerUserId", FloatType(), True), \
        StructField("LastEditorUserId", FloatType(), True),
        StructField("LastEditorDisplayName", StringType(), True), \
        StructField("LastEditDate", StringType(), True), \
        StructField("LastActivityDate", StringType(), True),
        StructField("CommunityOwnedDate", StringType(), True), \
        StructField("ClosedDate", StringType(), True), \
        StructField("Title", StringType(), True),
        StructField("Tags", StringType(), True), \
        StructField("AnswerCount", FloatType(), True), \
        StructField("CommentCount", FloatType(), True), \
        StructField("FavoriteCount", FloatType(), True)])
    MAIN_TAG = 'posts'
    return parse(sparkContext, path, SCHEMA, MAIN_TAG)

def parse_posthistory(sparkContext, path):

    SCHEMA = StructType([ \
        StructField("Id", FloatType(), True), \
        StructField("PostHistoryTypeId", FloatType(), True), \
        StructField("PostId", FloatType(), True), \
        StructField("RevisionGUID", StringType(), True), \
        StructField("CreationDate", StringType(), True), \
        StructField("UserId", FloatType(), True), \
        StructField("UserDisplayName", StringType(), True), \
        StructField("Body", StringType(), True), \
        StructField("Comment", StringType(), True), \
        StructField("Text", StringType(), True),
        StructField("CloseReasonId", FloatType(), True)])
    MAIN_TAG = 'posthistory'
    return parse(sparkContext, path, SCHEMA, MAIN_TAG)

def parse_postlinks(sparkContext, path):

    SCHEMA = StructType([ \
        StructField("Id", FloatType(), True), \
        StructField("CreationDate", StringType(), True), \
        StructField("PostId", FloatType(), True), \
        StructField("RelatedPostId", FloatType(), True),
        StructField("PostLinkTypeId", FloatType(), True)])
    MAIN_TAG = 'postlinks'
    return parse(sparkContext, path, SCHEMA, MAIN_TAG)

def parse_users(sparkContext, path):
    SCHEMA = StructType([ \
        StructField("Id", FloatType(), True), \
        StructField("Reputation", FloatType(), True), \
        StructField("CreationDate", StringType(), True), \
        StructField("DisplayName", StringType(), True), \
        StructField("LastAccessDate", StringType(), True), \
        StructField("WebsiteUrl", StringType(), True), \
        StructField("Location", StringType(), True), \
        StructField("AboutMe", StringType(), True), \
        StructField("Views", FloatType(), True), \
        StructField("UpVotes", FloatType(), True),
        StructField("DownVotes", FloatType(), True), \
        StructField("EmailHash", StringType(), True), \
        StructField("Age", FloatType(), True)])
    MAIN_TAG = 'users'
    return parse(sparkContext, path, SCHEMA, MAIN_TAG)

def parse_votes(sparkContext, path):
    SCHEMA = StructType([ \
        StructField("Id", FloatType(), True), \
        StructField("PostId", FloatType(), True), \
        StructField("VoteTypeId", FloatType(), True), \
        StructField("CreationDate", StringType(), True), \
        StructField("UserId", FloatType(), True), \
        StructField("BountyAmount", FloatType(), True)])
    MAIN_TAG = 'votes'
    return parse(sparkContext, path, SCHEMA, MAIN_TAG)