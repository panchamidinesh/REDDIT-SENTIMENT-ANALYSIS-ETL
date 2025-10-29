from prefect import flow, task
import praw
import pandas as pd
import boto3
from datetime import datetime

@task(retries=3, retry_delay_seconds=10)
def fetch_reddit_posts(limit=200):
    reddit = praw.Reddit(
        client_id="2w-k5CLwVHoO0etZmeqzxQ",
        client_secret="mDP_AIey2zdDBM3TLiQQXrpnB3yOaw",
        user_agent="airline_sentiment_app:v1.0 (by u/Long-Lifeguard3019)"
    )
    
    airline_keywords = [
        "Lufthansa", "Emirates", "KLM", "Delta Airlines",
        "American Airlines", "United Airlines", "Qatar Airways",
        "Air France", "Singapore Airlines", "Cathay Pacific"
    ]
    query = " OR ".join(airline_keywords)
    
    print("Searching Reddit for:", query)
    posts = []
    for post in reddit.subreddit("all").search(query, sort="new", limit=limit, time_filter='week'):
        posts.append({
            "id": post.id,
            "subreddit": post.subreddit.display_name,
            "title": post.title,
            "author": str(post.author),
            "created_utc": datetime.fromtimestamp(post.created_utc).isoformat(),
            "score": post.score,
            "num_comments": post.num_comments,
            "url": post.url,
            "text": post.selftext
        })
    
    if not posts:
        raise ValueError("No posts fetched from Reddit.")
    
    df = pd.DataFrame(posts)
    csv_file = f"reddit_airline_posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_file, index=False)
    print(f"Saved {len(df)} posts to {csv_file}")
    return csv_file

@task(retries=2)
def upload_to_s3(csv_file: str):
    s3 = boto3.client("s3")
    bucket_name = "fde-twi-bucket1"
    key = f"reddit_posts/{csv_file}"
    
    with open(csv_file, "rb") as f:
        s3.upload_fileobj(f, bucket_name, key)
    
    s3_path = f"s3://{bucket_name}/{key}"
    print("✅ Uploaded to", s3_path)
    return s3_path

@flow(name="Reddit→S3 ETL")
def reddit_to_s3_flow():
    csv_file = fetch_reddit_posts()
    upload_to_s3(csv_file)

# Optional: run manually
if __name__ == "__main__":
    reddit_to_s3_flow()