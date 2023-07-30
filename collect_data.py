import sys
import time
from jikanpy import Jikan
assert sys.version_info >= (3, 8)
from pyspark.sql import SparkSession, functions, types, Row

reddit_submissions_path = '/courses/datasets/reddit_submissions_repartitioned/'
reddit_comments_path = '/courses/datasets/reddit_comments_repartitioned/'
output = 'reddit-subset'

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])

submissions_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('created', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('domain', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.BooleanType()),
    types.StructField('from', types.StringType()),
    types.StructField('from_id', types.StringType()),
    types.StructField('from_kind', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('hide_score', types.BooleanType()),
    types.StructField('id', types.StringType()),
    types.StructField('is_self', types.BooleanType()),
    types.StructField('link_flair_css_class', types.StringType()),
    types.StructField('link_flair_text', types.StringType()),
    types.StructField('media', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('num_comments', types.LongType()),
    types.StructField('over_18', types.BooleanType()),
    types.StructField('permalink', types.StringType()),
    types.StructField('quarantine', types.BooleanType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('saved', types.BooleanType()),
    types.StructField('score', types.LongType()),
    types.StructField('secure_media', types.StringType()),
    types.StructField('selftext', types.StringType()),
    types.StructField('stickied', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('thumbnail', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('ups', types.LongType()),
    types.StructField('url', types.StringType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])

anime_schema = types.StructType([
    types.StructField('subreddit', types.StringType()),
    types.StructField('year', types.IntegerType()),
    types.StructField('month', types.IntegerType()),
])

def insert_jikan_anime(code, wait_time=0):
    jikan = Jikan()
    anime = jikan.anime(code)
    time.sleep(wait_time)
    return anime

def filter_date(date):
    date = date.split("-")
    year = int(date[0])
    month = int(date[1])
    return year, month

def main():
    spark = SparkSession.builder.appName("reddit data").getOrCreate()
    assert spark.version >= '3.2'
    spark.sparkContext.setLogLevel('WARN')
    
    # Testing for reddit-subset in local data
    reddit_submissions = spark.read.json("reddit-subset/submissions", schema=submissions_schema)
    reddit_comments = spark.read.json("reddit-subset/comments", schema=comments_schema)
    
    # Get anime releases from MyAnimeList #
    
    # Need to get anime releases manually as there is no formula to it
    jujutsu_kaisen = insert_jikan_anime(40748)
    my_hero_academia_fifth = insert_jikan_anime(41587)
    spy_x_family = insert_jikan_anime(50265)
    # JikanPy has a rate limit of 3 requests per second and won't work with more than 3 requests unless we wait/sleep for a second
    rezero_season_two = insert_jikan_anime(39587,1)
    kaguya_sama_ultra_romantic = insert_jikan_anime(43608,1)
    demon_slayer_entertainment_district = insert_jikan_anime(47778,1)
    attack_on_titan_final_season_two = insert_jikan_anime(48583,1)
    komi_san = insert_jikan_anime(48926,1)
    
    # Format: Subreddit_name: MyAnimeList data
    anime_list = [
        ("JuJutsuKaisen", jujutsu_kaisen),
        ("BokuNoHeroAcademia", my_hero_academia_fifth),
        ("SpyxFamily", spy_x_family),
        ("Re_Zero", rezero_season_two),
        ("Kaguya_sama", kaguya_sama_ultra_romantic),
        ("KimetsuNoYaiba", demon_slayer_entertainment_district),
        ("ShingekiNoKyojin", attack_on_titan_final_season_two),
        ("Komi_san", komi_san)
    ]
    
    # 2 loops are needed to filter the data from JikanPy to get release dates as the library does not have any built in functions to handle this type of task
    anime_list = [(subreddit, filter_date(data["data"]["aired"]["from"])) 
                  for subreddit, data in anime_list]
    
    anime_list = [(subreddit, year, month) for subreddit, (year, month) in anime_list]
    
    anime_df = spark.createDataFrame(anime_list, schema=anime_schema).cache()
    # anime_df.show()
    
    # print(anime_list)
    reddit_submissions = spark.read.json(reddit_submissions_path, schema=submissions_schema)
    reddit_comments = spark.read.json(reddit_comments_path, schema=comments_schema)
    
    subs = anime_df.select(functions.collect_list(anime_df.subreddit)).first()[0] # get the subreddits from my dataframe
    # print(subs)
    reddit_submissions.where(reddit_submissions['subreddit'].isin(subs)) \
        .where(reddit_submissions['year'] >= 2020 & reddit_submissions['month'] >= 6) \
        .write.json(output + '/submissions', mode='overwrite', compression='gzip')
    
    reddit_comments.where(reddit_comments['subreddit'].isin(subs)) \
        .where(reddit_comments['year'] >= 2020 & reddit_comments['month'] >= 6) \
        .write.json(output + '/comments', mode='overwrite', compression='gzip')
    
main()