import sys
import time
from jikanpy import Jikan
assert sys.version_info >= (3, 8)
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from scipy.stats import normaltest

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

# JikanPy has a rate limit of 3 requests per second and won't work with more than 3 requests unless we wait/sleep for a second
def insert_jikan_anime(code, wait_time=1.05/3):
    jikan = Jikan()
    anime = jikan.anime(code)
    time.sleep(wait_time)
    return anime

def f(anime):
    name = anime[0]
    jj = anime[1]
    aired = jj["data"]["aired"]["from"]
    date = aired.split("-")
    year = int(date[0])
    month = int(date[1])
    day = int((date[2].split("T"))[0])
    return year, month, day
    
def perform_statistic_test(num_comments_list):
    print(normaltest(num_comments_list))
        
def main():
    spark = SparkSession.Builder.appName(SparkSession.builder, "reddit data").getOrCreate()
    assert spark.version >= '3.2'
    spark.sparkContext.setLogLevel('WARN')
    
    # Directory Paths
    reddit_submissions_path = sys.argv[1]
    
    ### Get anime releases from MyAnimeList ###
    
    # Need to get anime releases manually as there is no formula to it
    # JikanPy has a rate limit of 3 requests per second and won't work with more than 3 requests unless we wait/sleep for a second
    jujutsu_kaisen = insert_jikan_anime(40748)
    spy_x_family = insert_jikan_anime(50265)
    rezero = insert_jikan_anime(31240)
    demon_slayer = insert_jikan_anime(38000)
    komi_san = insert_jikan_anime(48926)
    
    # Format: Subreddit_name: MyAnimeList data
    # 2 loops are needed to filter the data from JikanPy to get release dates as the library does not have any built in F to handle this type of task

    # ike: can do this in one like this
    anime_list = list(map(f, [
        ("JuJutsuKaisen", jujutsu_kaisen),
        ("SpyxFamily", spy_x_family),
        ("Re_Zero", rezero),
        ("KimetsuNoYaiba", demon_slayer),
        ("Komi_san", komi_san)
    ]))
    
    anime_df = spark.createDataFrame(anime_list, schema=anime_schema).cache()
    
    # Get the previous month and year, if possible, if an anime was released within 10 days of the beginning of the month
    anime_df = anime_df.withColumn("previous_month",
        F.when(F.col("day") <= 10, F.col("month") - 1)
         .otherwise(-1)) # get the previous month
    
    anime_df = anime_df.withColumn("previous_year",
        F.when(F.col("previous_month") == 0, F.col("year") - 1)
         .when(F.col("previous_month") == -1, -1)
         .otherwise(F.col("year"))) # If our month is #0, get the previous year. Else if, our month is null, keep it at null. Else, set the previous_year as the current year (needed for filtering from the Cluster)
    
    anime_df = anime_df.withColumn("previous_month",
        F.when(F.col("previous_month") == 0, 12)
         .otherwise(F.col("previous_month"))) # ensure that we have a valid month (e.g. Jan -> Dec)

    ### Getting data from the SFU Reddit cluster ###
    
    # Read the submissions files and filter based on the month and year of an anime release from anime_df
    reddit_submissions = spark.read.json(reddit_submissions_path, schema=submissions_schema)
    
    release_month_submissions = (reddit_submissions
        .join(anime_df.distinct(),
            on=((reddit_submissions.subreddit == anime_df.subreddit) & (reddit_submissions.year == anime_df.year) & (reddit_submissions.month == anime_df.month)),
            how="inner")
        .drop(anime_df.year, anime_df.month, anime_df.subreddit))
    
    previous_month_submissions = (reddit_submissions
        .join(anime_df.distinct(),
            on=((reddit_submissions.subreddit == anime_df.subreddit) & (reddit_submissions.year == anime_df.previous_year) & (reddit_submissions.month == anime_df.previous_month)),
            how="inner")
        .drop(anime_df.year, anime_df.month, anime_df.subreddit))
    
    # Used to write to an output folder if you're running the program on the cluster
    if len(sys.argv) > 2:
        output = sys.argv[2]
        combined_submissions = release_month_submissions.union(previous_month_submissions)
        combined_submissions.write.json(output + "/submissions", mode='overwrite', compression='gzip')
    
    # Combine the number of comments from each post in a subreddit into a list by grouping it based on the subreddit's name
    release_month_sub_grouped_comments = (release_month_submissions
        .groupBy("subreddit")
        .agg(F.collect_list("num_comments").alias("num_comments")))
    previous_month_sub_grouped_comments = (previous_month_submissions
        .groupBy("subreddit")
        .agg(F.collect_list("num_comments").alias("num_comments")))
    
    ### Calculating p-values ###
    release_month_sub_grouped_comments.foreach(
        lambda row: perform_statistic_test(row["num_comments"])
    )
    
    previous_month_sub_grouped_comments.foreach(
        lambda row: perform_statistic_test(row["num_comments"])
    )
    
main()