import sys
import time
from jikanpy import Jikan
assert sys.version_info >= (3, 8)
from pyspark.sql import SparkSession, functions, types

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
    types.StructField('day', types.IntegerType()),
])

### Helper functions to do small tasks ###
def insert_jikan_anime(code, wait_time=1):
    jikan = Jikan()
    anime = jikan.anime(code)
    time.sleep(wait_time)
    return anime

def filter_date(date):
    date = date.split("-")
    year = int(date[0])
    month = int(date[1])
    day = int((date[2].split("T"))[0])
    return year, month, day
        

def main():
    # Spark Setup
    spark = SparkSession.builder.appName("reddit anime/manga data").getOrCreate()
    assert spark.version >= '3.2'
    spark.sparkContext.setLogLevel('WARN')
    
    # Directory Paths
    reddit_submissions_path = '/courses/datasets/reddit_submissions_repartitioned/'
    reddit_comments_path = '/courses/datasets/reddit_comments_repartitioned/'
    output = 'reddit-subset'
    
    ### Get anime releases from MyAnimeList ###
    
    # Need to get anime releases manually as there is no formula to it
    # JikanPy has a rate limit of 3 requests per second and won't work with more than 3 requests unless we wait/sleep for a second
    jujutsu_kaisen = insert_jikan_anime(40748)
    spy_x_family = insert_jikan_anime(50265)
    komi_san = insert_jikan_anime(48926)
    
    # Format: Subreddit_name: MyAnimeList data
    anime_list = [
        ("JuJutsuKaisen", jujutsu_kaisen),
        ("SpyxFamily", spy_x_family),
        ("Komi_san", komi_san)
    ]
    
    # 2 loops are needed to filter the data from JikanPy to get release dates as the library does not have any built in functions to handle this type of task
    anime_list = [(subreddit, filter_date(data["data"]["aired"]["from"])) 
                  for subreddit, data in anime_list]
    
    anime_list = [(subreddit, year, month, day) for subreddit, (year, month, day) in anime_list]
    
    anime_df = spark.createDataFrame(anime_list, schema=anime_schema).cache()
    
    # Get the previous month and year, if possible, if an anime was released within 10 days of the beginning of the month
    anime_df = anime_df.withColumn("previous_month", (functions.when(functions.col("day") <= 10, functions.col("month") - 1).otherwise(-1))) # get the previous month
    
    anime_df = anime_df.withColumn("previous_year", functions.when(functions.col("previous_month") == 0, functions.col("year") - 1).when(functions.col("previous_month") == -1, -1).otherwise(functions.col("year"))) # If our month is #0, get the previous year. Else if, our month is null, keep it at null. Else, set the previous_year as the current year (needed for filtering from the Cluster)
    
    anime_df = anime_df.withColumn("previous_month", (functions.when(functions.col("previous_month") == 0, 12)).otherwise(functions.col("previous_month"))) # ensure that we have a valid month (e.g. Jan -> Dec)

    ### Getting data from the SFU Reddit cluster ###
    
    # Read the submissions files and filter based on the month and year of an anime release from anime_df
    reddit_submissions = spark.read.json(reddit_submissions_path, schema=submissions_schema)
    reddit_comments = spark.read.json(reddit_comments_path, schema=comments_schema)

    subs = anime_df.select(functions.collect_list(anime_df.subreddit)).first()[0] # get the subreddits from my dataframe
    
    # Used to write to an output folder if you're running the program on the cluster
    reddit_submissions = reddit_submissions.where(
        reddit_submissions['subreddit'].isin(subs) &
        (
            (reddit_submissions['year'] == 2016) |
            (reddit_submissions['year'] >= 2019)
        ) &
        (
            (reddit_submissions["month"] == 3) |
            (reddit_submissions["month"] == 4) |
            (reddit_submissions["month"] == 9) |
            (reddit_submissions["month"] == 10)
        )
    )
    reddit_submissions.write.json(output + "/submissions", mode='overwrite', compression='gzip')
    
    reddit_comments = reddit_comments.where(
        reddit_comments['subreddit'].isin(subs) &
        (
            (reddit_comments['year'] == 2016) |
            (reddit_comments['year'] >= 2019)
        ) &
        (
            (reddit_comments["month"] == 3) |
            (reddit_comments["month"] == 4) |
            (reddit_comments["month"] == 9) |
            (reddit_comments["month"] == 10)
        )
    )
    reddit_comments.write.json(output + "/comments", mode='overwrite', compression='gzip')
    
main()