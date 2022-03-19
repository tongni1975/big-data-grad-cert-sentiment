import praw

reddit = praw.Reddit(client_id='JkgjG1OarUJx6g8874ivVQ', client_secret='N_2DvHX-MHddsgfutbpjbR-C5WqmIQ', user_agent='bda_r')

# get 10 hot posts from the CryptoCurrency subreddit
crypto_dit = reddit.subreddit('CryptoCurrency')
hot_posts = crypto_dit.hot(limit=10)

# for post in hot_posts:
#     print(post.title)


import pandas as pd
posts = []

for post in hot_posts:
    posts.append([post.title, post.score, post.id, post.created, post.url, post.num_comments, post.selftext])
posts = pd.DataFrame(posts,columns=['title', 'score', 'id', 'create_date', 'url', 'num_comments', 'body'])

print(posts)