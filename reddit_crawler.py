import praw
import datetime
import pandas as pd
import logging
import time
# log config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("reddit_crawler.log", mode='w', encoding='utf-8'),
        logging.StreamHandler()  # 控制台输出
    ]
)

#
for logger_name in ("praw", "prawcore"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.FileHandler("reddit_crawler.log"))


class RedditCrawler:
    # init
    def __init__(self, client_id, client_secret, user_agent,
                 subreddits_with_dates, post_limit):
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            ratelimit_seconds=300
        )
        self.subreddits = subreddits_with_dates
        self.post_limit = post_limit
        self.records = []
    # deal with media
    def extract_media_from_post(self, post):
        media_urls = []
        media_types = []

        # Image
        if hasattr(post, "url") and post.url.endswith((".jpg", ".png", ".gif")):
            media_urls.append(post.url)
            media_types.append("image")

        # Video
        if hasattr(post, "media") and post.media and "reddit_video" in post.media:
            video_url = post.media["reddit_video"].get("fallback_url")
            if video_url:
                media_urls.append(video_url)
                media_types.append("video")

        # Gallery
        if hasattr(post, "is_gallery") and post.is_gallery:
            if hasattr(post, "media_metadata") and hasattr(post, "gallery_data"):
                for item in post.gallery_data["items"]:
                    media_id = item["media_id"]
                    if media_id in post.media_metadata:
                        media = post.media_metadata[media_id]
                        if "s" in media and "u" in media["s"]:
                            media_urls.append(media["s"]["u"])
                            media_types.append("image")

        return "; ".join(media_urls), "; ".join(media_types)

    def crawl(self):
        for sub, (start_dt, end_dt) in self.subreddits.items():
            date_start = start_dt.timestamp()
            date_end = end_dt.timestamp()

            logging.info(f"🔍 Crawling subreddit: r/{sub}")
            subreddit = self.reddit.subreddit(sub)

            raw_posts = list(subreddit.new(limit=self.post_limit))
            logging.info(f"🧾 Subreddit: r/{sub} Number of original posts requested: {len(raw_posts)}")

            count_valid = 0
            for post in raw_posts:
                if date_start <= post.created_utc <= date_end:
                    time.sleep(0.3)
                    count_valid += 1
                    logging.debug(f"📥 Post ID: {post.id} | Title: {post.title}")

                    media_urls, media_types = self.extract_media_from_post(post)
                    post.comments.replace_more(limit=None)

                    def clean_text(text):
                        return text.replace('\xa0', ' ').replace('&nbsp;', ' ').strip()

                    for comment in post.comments.list():
                        self.records.append({
                            "subreddit": sub,
                            "post_id": post.id,
                            "post_title": clean_text(post.title),
                            "post_body": clean_text(post.selftext),
                            "post_author": str(post.author) if post.author else "deleted",
                            "post_timestamp": datetime.datetime.fromtimestamp(post.created_utc),
                            "post_score": post.score,
                            "media_urls": media_urls,
                            "media_types": media_types,
                            "comment_id": comment.id,
                            "parent_id": comment.parent_id.split("_")[-1],
                            "parent_raw": comment.parent_id,
                            "is_top_level": int(comment.parent_id.startswith("t3_")),
                            "depth": comment.depth,
                            "comment_author": str(comment.author) if comment.author else "deleted",
                            "comment_timestamp": datetime.datetime.fromtimestamp(comment.created_utc),
                            "comment_body": clean_text(comment.body),
                            "comment_score": comment.score,
                            "permalink": f"https://www.reddit.com{comment.permalink}"
                        })

            logging.info(f"Number of Valid Posts: {count_valid} / {len(raw_posts)}")
            logging.info(f"Finished subreddit: r/{sub}, total posts/comments collected: {len(self.records)}")

    # save to csv
    def save_to_csv(self, filename="reddit_comments_with_media.csv"):
        df = pd.DataFrame(self.records)
        df.to_csv(filename, index=False)
        print(f"✅ Saved {len(df)} records to {filename}")

    def run(self, output_file="reddit_comments_with_media.csv"):
        logging.info("Start collecting all subreddits data")
        self.crawl()
        logging.info("Start saving records to csv")
        self.save_to_csv(output_file)

def main():
    # log
    logging.basicConfig(
        filename="reddit_crawler.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Reddit Crawler Started")

    try:
        # Reddit
        client_id = "C_mRKflOUDLRfd5E9iDh5w"
        client_secret = "M-RTqq6lbJCrzFtqeyJrLv8ZQAop9g"
        user_agent = "macOS:aphantasia-research:1.0 (by u/Zilululu)"

        # Set crawling parameters
        subreddits_with_dates = {
            "Aphantasia": (datetime.datetime(2025, 3, 10), datetime.datetime(2025, 7, 1)),
            "Hyperphantasia": (datetime.datetime(2021, 6, 18), datetime.datetime(2025, 7, 1)),
            "Anauralia": (datetime.datetime(2021, 6, 18), datetime.datetime(2025, 7, 1)),
            "silentminds": (datetime.datetime(2021, 6, 18), datetime.datetime(2025, 7, 1)),
        }
        post_limit = None

        # Instantiate crawler
        crawler = RedditCrawler(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            subreddits_with_dates=subreddits_with_dates,
            post_limit=post_limit
        )

        # start
        crawler.run(output_file="reddit_crawler_20250701.csv")

        logging.info("Reddit Crawler Completed Successfully")

    except Exception as e:
        logging.error(f"Reddit Crawler Running Failed：{e}", exc_info=True)


# main
if __name__ == "__main__":
    main()

