import praw

reddit = praw.Reddit(client_id='Um7_K2-030cBPakJlkDXnA',
                     client_secret='tRRUTXNS6UJtQ27DThGaPi0zwoUwZw',
                     user_agent='ikec421')

# Example 1: Fetch the 10 hottest posts from a subreddit
subreddit = reddit.subreddit('python')
for submission in subreddit.hot(limit=10):
    print(submission.title)

# Example 2: Search for a specific post
search_query = "python"
for submission in reddit.subreddit('all').search(search_query, limit=5):
    print(submission.title, submission.url)

# Example 3: Get comments from a post
post_id = 'POST_ID'  # Replace with the actual post ID
post = reddit.submission(id=post_id)
post.comments.replace_more(limit=None)  # Retrieve all comments (beyond the immediate top-level comments)
for comment in post.comments.list():
    print(comment.body)

# Note: Depending on your use case, you might need to use Reddit's OAuth2 authentication to perform actions that require a user account (e.g., posting, commenting).

