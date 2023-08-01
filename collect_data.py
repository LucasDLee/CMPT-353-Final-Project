import sys
import time
from jikanpy import Jikan
assert sys.version_info >= (3, 8)
from pyspark.sql import SparkSession, functions, Row
import schema

reddit_submissions_path = '/courses/datasets/reddit_submissions_repartitioned/'
reddit_comments_path = '/courses/datasets/reddit_comments_repartitioned/'
output = 'reddit-subset'

def insert_jikan_anime(code, wait_time=0):
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
    spark = SparkSession.builder.appName("reddit data").getOrCreate()
    assert spark.version >= '3.2'
    spark.sparkContext.setLogLevel('WARN')
    
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
    
    anime_list = [(subreddit, year, month, day) for subreddit, (year, month, day) in anime_list]
    
    anime_df = spark.createDataFrame(anime_list, schema=schema.anime_schema).cache()
    
    # Get the previous month and year, if possible, if an anime was released within 10 days of the beginning of the month
    anime_df = anime_df.withColumn("previous_month", (functions.when(functions.col("day") <= 10, functions.col("month") - 1).otherwise(None))) # get the previous month
    anime_df = anime_df.withColumn("previous_year", functions.when(functions.col("previous_month") == 0, functions.col("year") - 1).otherwise(None))
    anime_df = anime_df.withColumn("previous_month", (functions.when(functions.col("previous_month") == 0, 12)))
    
    # Get the next month and year, if possible, if an anime was released within 21 days of the end of the month
    anime_df = anime_df.withColumn("next_month", (functions.when(functions.col("day") >= 21, functions.col("month") + 1).otherwise(None))) # get the next month
    anime_df = anime_df.withColumn("next_year", functions.when(functions.col("next_month") == 13, functions.col("year") + 1).otherwise(None))
    anime_df = anime_df.withColumn("next_month", (functions.when(functions.col("next_month") == 13, 1)))
    
    
    # Testing for reddit-subset in local data
    reddit_submissions = spark.read.json("reddit-subset/submissions", schema=schema.submissions_schema)
    reddit_comments = spark.read.json("reddit-subset/comments", schema=schema.comments_schema)
    # reddit_submissions = spark.read.json(reddit_submissions_path, schema=submissions_schema)
    # reddit_comments = spark.read.json(reddit_comments_path, schema=comments_schema)
    
    subs = anime_df.select(functions.collect_list(anime_df.subreddit)).first()[0] # get the subreddits from my dataframe
    
    reddit_submissions.where(reddit_submissions['subreddit'].isin(subs)) \
        .where(reddit_submissions['year'] >= 2020 & reddit_submissions['month'] >= 6) \
        .write.json(output + '/submissions', mode='overwrite', compression='gzip')
    
    reddit_comments.where(reddit_comments['subreddit'].isin(subs)) \
        .where(reddit_comments['year'] >= 2020 & reddit_comments['month'] >= 6) \
        .write.json(output + '/comments', mode='overwrite', compression='gzip')
    
main()