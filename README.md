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
- Python (3.8+) and its corresponding libraries
  - `sys`: Helped in setting the version info
  - `scipy.stats`: Needed for our statistical tests
  - `matplotlib.pyplot`: Display our visualizations
  - `seaborn`: Enhance our visualizations
- Spark DataFrames (3.4.1+)
- [JikanPy](https://github.com/abhinavk99/jikanpy): A Python wrapper for Jikan, an unofficial PHP API for MyAnimeList which holds anime release dates and more
- [Reddit-Cluster-Files](https://github.sfu.ca/ldl5/Reddit-Cluster-Files): Our downloaded files from the SFU Reddit and Submissions cluster. Necessary in running `collect_data.py`

## Running the Program

As we needed to get data from SFU's Reddit cluster, we devised a couple of different programs that do different tasks to ensure our code is up and running.

- `download_cluster_data.py`: Used to download our data from SFU's Reddit cluster
- `collect_data.py`: Used to develop our visualizations and get the results from our data. Please ensure you have downloaded the Reddit files into this repository for the code to work. The files can be found in the [Reddit-Cluster-Files](https://github.sfu.ca/ldl5/Reddit-Cluster-Files) repository

Both programs can be run like so: `spark-submit [insert your Python program here]`

## Authors

- **Ike Chan** - [ikec@sfu.ca](mailto:ikec@sfu.ca) / [GitHub](https://github.com/Jack41784090)
- **Lucas Lee** - [ldl5@sfu.ca](mailto:ldl5@sfu.ca) / [GitHub](https://github.com/LucasDLee)

## Acknowledgments

- README template adapated from [https://gist.github.com/PurpleBooth/109311bb0361f32d87a2](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
- Reddit data from the SFU cluster (cluster.cs.sfu.ca)
