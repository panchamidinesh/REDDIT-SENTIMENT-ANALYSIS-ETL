from prefect import flow
from reddit_to_s3 import reddit_to_s3_flow  # your actual flow

if __name__ == "__main__":
    reddit_to_s3_flow.serve(
        name="reddit-flow",
        cron="0 * * * *"  # optional: runs every hour
    )
