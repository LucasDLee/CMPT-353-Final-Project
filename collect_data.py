import sys
import time
from jikanpy import Jikan

assert sys.version_info >= (3, 8)
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from scipy.stats import mannwhitneyu, chi2_contingency

import matplotlib.pyplot as plt
import seaborn

reddit_submissions_path = 'Reddit-Cluster-Files/reddit-subset/submissions'
reddit_comments_path = 'Reddit-Cluster-Files/reddit-subset/comments'

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
    return name, year, month, day
        
def main():
    ### Spark Setup ###
    spark = SparkSession.Builder.appName(SparkSession.builder, "reddit anime/manga data").getOrCreate()
    assert spark.version >= '3.2'
    spark.sparkContext.setLogLevel('WARN')
    
    ### Getting anime data ###
    
    # Need to get anime releases manually as there is no formula to it
    # JikanPy has a rate limit of 3 requests per second and won't work with more than 3 requests unless we wait/sleep for a second
    jujutsu_kaisen = insert_jikan_anime(40748)
    spy_x_family = insert_jikan_anime(50265)
    komi_san = insert_jikan_anime(48926)
    
    # Format: Subreddit_name: MyAnimeList data
    anime_info = [
        ("JuJutsuKaisen", jujutsu_kaisen),
        ("SpyxFamily", spy_x_family),
        ("Komi_san", komi_san)
    ]
    anime_list = list(map(f, anime_info))
    
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
    r_sub = spark.read.json(reddit_submissions_path, schema=submissions_schema)
    r_com = spark.read.json(reddit_comments_path, schema=comments_schema)
    
    # Filter the data from r_sub to match with our anime data
    release_month_submissions = (r_sub
        .join(anime_df.distinct(),
            on=((r_sub.subreddit == anime_df.subreddit) & (r_sub.year == anime_df.year) & (r_sub.month == anime_df.month)),
            how="inner")
        .drop(anime_df.year, anime_df.month, anime_df.subreddit))

    previous_month_submissions = (r_sub
        .join(anime_df.distinct(),
            on=((r_sub.subreddit == anime_df.subreddit) & (r_sub.year == anime_df.previous_year) & (r_sub.month == anime_df.previous_month)),
            how="inner")
        .drop(anime_df.year, anime_df.month, anime_df.subreddit))

    # Combine the number of comments from each post in a subreddit into a list by grouping it based on the subreddit's name
    release_month_sub_grouped_comments = (release_month_submissions
        .groupBy("subreddit")
        .agg(F.count("num_comments").alias("release_total_comments"))
        )
    previous_month_sub_grouped_comments = (previous_month_submissions
        .groupBy("subreddit")
        .agg(F.count("num_comments").alias("previous_total_comments"))
        )
    grouped_sub_comments = release_month_sub_grouped_comments.join(previous_month_sub_grouped_comments, on="subreddit").toPandas()
    
    ### Visualizations ###
    seaborn.set()
    plt.figure(figsize=(10, 6))
    bar_width = 0.4

    index = range(len(grouped_sub_comments))
    plt.bar(index, grouped_sub_comments['release_total_comments'], width=bar_width, label='After Release')
    plt.bar(index, grouped_sub_comments['previous_total_comments'], width=bar_width, label='Before Release', alpha=0.7)

    plt.xlabel('Subreddit')
    plt.ylabel('Comment Count')
    plt.title('Comments from Subreddits 1 Month Before and After the Anime Adapataion')
    plt.xticks(index, grouped_sub_comments['subreddit'], rotation=0)
    plt.legend()
    plt.savefig("All Comment Changes")
    
    for _, row in grouped_sub_comments.iterrows():
        plt.figure(figsize=(7,5))
        plt.bar(["Before Release", "After Release"], [row["previous_total_comments"], row["release_total_comments"]])
        plt.xlabel("Release Months")
        plt.ylabel("Comment Count")
        plt.title(f'Number of Comments for {row["subreddit"]}')
        output = f'Subreddit: {row["subreddit"]}.png'
        plt.savefig(output)
    ### Visualizations finished ###
    
    ### Calculating p-values ###
    contingency = [[],[]]
    for anime in anime_list:
        name = anime[0]
        year = anime[1]
        month = anime[2]
        day = anime[3]

        sbms = r_sub.where(r_sub['subreddit'] == name).cache()
        coms = r_com.where(r_com['subreddit'] == name).cache()

        # all submissions and comments before and after release
        sbms_bf = sbms.where((sbms['year'] < year) | ((sbms['year'] == year) & (sbms['month'] < month))).cache()
        sbms_af = sbms.where((sbms['year'] > year) | ((sbms['year'] == year) & (sbms['month'] >= month))).cache()
        coms_bf = coms.where((coms['year'] < year) | ((coms['year'] == year) & (coms['month'] < month))).cache()
        coms_af = coms.where((coms['year'] > year) | ((coms['year'] == year) & (coms['month'] >= month))).cache()

        # how many submissions / comments are there per month, separated before and after release?
        sbms_byMonth_bf = (sbms_bf
            .groupBy('year', 'month')
            .agg(F.count("*").alias("count")))
        sbms_byMonth_af = (sbms_af
            .groupBy('year', 'month')
            .agg(F.count("*").alias("count")))

        coms_byMonth_bf = (coms_bf
            .groupBy('year', 'month')
            .agg(F.count("*").alias("count")))
        coms_byMonth_af = (coms_af
            .groupBy('year', 'month')
            .agg(F.count("*").alias("count")))

        # mann whiteney
        # by month
        # x = byMonth_bf, y = byMonth_af, Ha = any x < any y
        x1 = sbms_byMonth_bf.select('count').rdd.flatMap(lambda x: x).collect()
        y1 = sbms_byMonth_af.select('count').rdd.flatMap(lambda x: x).collect()
        x2 = coms_byMonth_bf.select('count').rdd.flatMap(lambda x: x).collect()
        y2 = coms_byMonth_af.select('count').rdd.flatMap(lambda x: x).collect()
        mw_month_sbm = mannwhitneyu(x1, y1, alternative='less')
        mw_month_com = mannwhitneyu(x2, y2, alternative='less')
        print("\nSubreddit:", name, "\n")
        print(x1,y1,x2,y2)
        print(mw_month_com, mw_month_sbm)

        # append to contingency, add pre release month activity to [0] and after release to [1]
        previous_month_submissions = (sbms_bf
            .join(anime_df.distinct(),
                on=((sbms_bf.year == anime_df.previous_year) & (sbms_bf.month == anime_df.previous_month)),
                how="inner")
            .drop(anime_df.year, anime_df.month, anime_df.subreddit))
        total = previous_month_submissions.select(F.sum(previous_month_submissions['num_comments'])).rdd.flatMap(lambda x: x).collect()[0]
        contingency[0].append(total)

        release_month_subs = sbms_af.filter((sbms_af['year'] == year) & (sbms_af['month'] == month))
        total = release_month_subs.select(F.sum(release_month_subs['num_comments'])).rdd.flatMap(lambda x: x).collect()[0]
        contingency[1].append(total)
        print("\nFinish tests for", name, "\n")

    # chi contingency
    print("\nChi2 Contingency Test")
    chi = chi2_contingency(contingency)
    print(contingency, chi)
    
main()