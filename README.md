# Analyzing Reddit Activity from Manga Subreddits

This repository is the final project for Simon Fraser University's *CMPT 353: Computational Data Science (Summer 2023)* by [Ike Chan](#authors) and [Lucas Lee](#authors).

The goal of our project is to use the Reddit Submissions and Comments cluster to answer the question:

> ***Are manga subreddits more active when anime adaptations are made?***

In this project, we applied statistical models to analyze shifts in p-values to determine whether it matches with our hypotheses or not. Our hypotheses are:

**Null Hypothesis**: There is no difference in the amount of activity in a subreddit when anime adaptation based on a manga is released.

**Alternate Hypothesis**: There is a difference in the amount of activity in a subreddit when anime adaptation based on a manga is released.

## Setup

You will need the following tools to run the program:

- Git
- Python (3.8+)
- Spark DataFrames (3.4.1+)
- [JikanPy](https://github.com/abhinavk99/jikanpy): A Python wrapper for Jikan, an unofficial PHP API for MyAnimeList which holds anime release dates and more

## Running the Program

As getting SFU Cluster data takes a while, we have devised two ways to run this program. Using the following command lines, you can run each one depending on what tools you are have:

**Locally**:

> spark-submit collect_data.py reddit-subset/submissions

**SFU Cluster**:

> spark-submit collect_data.py /courses/datasets/reddit_submissions_repartitioned/ reddit-subset

## Authors

- **Ike Chan** - [ikec@sfu.ca](mailto:ikec@sfu.ca) / [GitHub](https://github.com/Jack41784090)
- **Lucas Lee** - [ldl5@sfu.ca](mailto:ldl5@sfu.ca) / [GitHub](https://github.com/LucasDLee)

## Acknowledgments

- README template adapated from [https://gist.github.com/PurpleBooth/109311bb0361f32d87a2](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
- Reddit data from the [SFU cluster](cluster.cs.sfu.ca)
