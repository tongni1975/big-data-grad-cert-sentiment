import pandas as pd
import praw
import time

reddit_fields = ["title", "score", "num_comments", "body"]

reddit = praw.Reddit(client_id='JkgjG1OarUJx6g8874ivVQ',
                     client_secret='N_2DvHX-MHddsgfutbpjbR-C5WqmIQ',
                     user_agent='MacOS:bda_r_reader:0.1 (by /u/Ok-Dress-1379)',
                     ratelimit_seconds=300)

# get 10 hot posts from the CryptoCurrency subreddit
crypto_dit = reddit.subreddit('CryptoCurrency')
hot_posts = crypto_dit.hot(limit=100)

# for post in hot_posts:
#     print(post.title)


posts = []

for post in hot_posts:
    posts.append([post.title, post.score, post.id, post.created,
                 post.url, post.num_comments, post.selftext])
posts = pd.DataFrame(posts, columns=[
                     'title', 'score', 'id', 'create_date', 'url', 'num_comments', 'body'])

# print(posts)

name = time.strftime('reddit_%d%m%Y_%H_%M.csv')
posts.to_csv('input/reddit/{}'.format(name))
