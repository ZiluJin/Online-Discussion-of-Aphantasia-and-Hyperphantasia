import os
import time
import math
import json
import csv
import requests
from datetime import datetime, timedelta, timezone

# 1 client access
CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY", "aw3ht866wkyivq7y")
CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET", "bHZsyAHeQg0OS2a661ukSDIXy3KLfYDj")

# 2 basic config
HASHTAG = ["aphantasia", "hyperphantasia"]
YEARS_BACK = 1 # recently 3 years
WINDOW_DAYS =30
VIDEO_FIELDS = ",".join([
    "id","username","create_time","region_code",
    "video_description","view_count","like_count",
    "comment_count","share_count","hashtag_names","video_duration"
])
COMMENT_FIELDS = "id,video_id,text,like_count,reply_count,parent_comment_id,create_time"  # :contentReference[oaicite:3]{index=3}
# save as csv
VIDEOS_OUT = "tiktok_videos_0712.csv"
COMMENTS_OUT = "tiktok_comments_0712.csv"

# 3 client access token
def get_access_token(client_key, client_secret):
    url = "https://open.tiktokapis.com/v2/oauth/token/"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "client_key": client_key,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    r = requests.post(url, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j["access_token"]


# 全局保存当前 token，便于刷新后更新
TOKEN_BOX = {"access_token": None}

def post_with_retry_and_refresh(url, headers, body, allow_4xx=False, max_retry=6):
    """
    统一的 POST 调用：
    - 401 自动刷新 token 并重试
    - 429 读取 Retry-After 或指数退避
    - 5xx 重试
    - 4xx（400/403/404）在 allow_4xx=True 时直接返回给上层决定跳过
    """
    for retry in range(max_retry):
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=60)
            sc = resp.status_code

            if sc == 401:
                # token 失效，刷新后重试
                try:
                    new_token = get_access_token(CLIENT_KEY, CLIENT_SECRET)
                    TOKEN_BOX["access_token"] = new_token
                    headers["Authorization"] = f"Bearer {new_token}"
                except Exception as e:
                    raise RuntimeError(f"401且刷新token失败: {e}")
                time.sleep(1)  # 稍微歇一下再重试
                continue

            if sc == 429:
                wait = int(resp.headers.get("Retry-After", 0)) or min(60, 2 ** retry)
                time.sleep(wait)
                continue

            if sc in (400, 403, 404):
                if allow_4xx:
                    return resp
                # 打印一点上下文帮你定位
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text
                raise RuntimeError(f"{sc} {url} detail={str(detail)[:300]}")

            if sc >= 500:
                time.sleep(min(60, 2 ** retry))
                continue

            resp.raise_for_status()
            return resp

        except requests.RequestException as e:
            time.sleep(min(60, 2 ** retry))

    # 最终失败时，带上最后一次返回体的片段，便于定位
    try:
        snippet = resp.text[:300]
    except Exception:
        snippet = "no-body"
    raise RuntimeError(f"Request failed after retries: {url}; last_status={resp.status_code if 'resp' in locals() else 'N/A'}; body~{snippet}")





# tools function
def yyyymmdd(dt):
    return dt.strftime("%Y%m%d")

def backoff_sleep(retry):
    time.sleep(min(60, 2 ** retry))  # 指数退避，最大 60s

def build_video_url(username, video_id):
    # Research API 不直接给 share_url；可用这条规范链接形成本页地址
    # https://www.tiktok.com/@{username}/video/{video_id}
    return f"https://www.tiktok.com/@{username}/video/{video_id}"

def safe_request(method, url, **kwargs):
    for retry in range(6):
        try:
            resp = requests.request(method, url, timeout=60, **kwargs)

            # 限流：尊重 Retry-After 或指数退避
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 0)) or min(60, 2 ** retry)
                time.sleep(wait)
                continue

            # 不可重试的 4xx：直接返回给上层决定是否跳过
            if resp.status_code in (400, 403, 404):
                return resp

            # 5xx：重试
            if resp.status_code >= 500:
                backoff_sleep(retry)
                continue

            resp.raise_for_status()
            return resp
        except requests.RequestException:
            backoff_sleep(retry)
    # 最终失败
    raise RuntimeError(f"Request failed after retries: {url}")


# ====== 5) 查询某个 30 天窗口内的 hashtag 视频，处理分页 ======
def query_videos_in_window(token, start_str, end_str, max_per_call=100):
    url = f"https://open.tiktokapis.com/v2/research/video/query/?fields={VIDEO_FIELDS}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    cursor = 0
    search_id = None

    # 查询条件：hashtag_name IN 两个词 或 keyword IN 两个词
    query_block = {
        "or": [
            {
                "operation": "IN",
                "field_name": "hashtag_name",
                "field_values": HASHTAG
            },
            {
                "operation": "IN",
                "field_name": "keyword",
                "field_values": HASHTAG
            }
        ]
    }

    while True:
        payload = {
            "query": query_block,
            "start_date": start_str,
            "end_date": end_str,
            "max_count": max_per_call,
            "cursor": cursor
        }
        if search_id:
            payload["search_id"] = search_id
        resp = post_with_retry_and_refresh(url, headers, payload, allow_4xx=False)

        j = resp.json()
        data = j.get("data", {})
        videos = data.get("videos", [])
        if not videos:
            break

        for v in videos:
            yield v
        time.sleep(0.15)  # 轻微节流，保护配额

        has_more = data.get("has_more", False)
        cursor = data.get("cursor", 0)
        search_id = data.get("search_id", search_id)
        if not has_more:
            break


# ====== 6) 拉取某条视频的所有（最多前1000条）评论 ======
def fetch_all_comments(token, video_id, page_size=100):
    url = f"https://open.tiktokapis.com/v2/research/video/comment/list/?fields={COMMENT_FIELDS}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    cursor = 0
    fetched = 0

    # 先尝试把 video_id 当字符串（不少 API 实际更稳）
    body_base = {"video_id": str(video_id), "max_count": page_size}

    while True:
        body = dict(body_base)
        body["cursor"] = cursor

        resp = post_with_retry_and_refresh(url, headers, body, allow_4xx=True)


        # 如果是不可重试的 4xx，打印并跳过该视频
        if resp.status_code in (400, 403, 404):
            try:
                msg = resp.json()
            except Exception:
                msg = resp.text
            print(f"[SKIP COMMENTS] video_id={video_id} status={resp.status_code} detail={str(msg)[:200]}")
            return  # 直接结束该视频的评论抓取

        j = resp.json()
        data = j.get("data", {})
        comments = data.get("comments", [])
        for c in comments:
            yield c

        has_more = data.get("has_more", False)
        cursor = data.get("cursor", 0)
        fetched += len(comments)
        if not has_more or fetched >= 1000:
            break

# ====== 7) 主流程 ======
def main():
    assert CLIENT_KEY != "YOUR_CLIENT_KEY" and CLIENT_SECRET != "YOUR_CLIENT_SECRET", \
        "请先填入 CLIENT_KEY / CLIENT_SECRET"
    token = get_access_token(CLIENT_KEY, CLIENT_SECRET)
    TOKEN_BOX["access_token"] = token

    print("Got access token.")

    utc_now = datetime.now(timezone.utc)
    start_total = datetime(2025, 2, 13).date()
    end_total = utc_now.date()

    # 准备 CSV
    with open(VIDEOS_OUT, "w", newline="", encoding="utf-8") as fv, \
         open(COMMENTS_OUT, "w", newline="", encoding="utf-8") as fc:
        vwriter = csv.DictWriter(fv, fieldnames=[
            "video_id","username","create_time","region_code",
            "video_description","view_count","like_count","comment_count",
            "share_count","video_duration","hashtag_names","video_url"
        ])
        vwriter.writeheader()

        cwriter = csv.DictWriter(fc, fieldnames=[
            "video_id","comment_id","text","like_count","reply_count",
            "parent_comment_id","create_time"
        ])
        cwriter.writeheader()

        # 滚动 30 天窗口
        window_start = start_total
        while window_start < end_total:
            window_end = min(window_start + timedelta(days=WINDOW_DAYS-1), end_total)
            s = yyyymmdd(datetime.combine(window_start, datetime.min.time()))
            e = yyyymmdd(datetime.combine(window_end, datetime.min.time()))
            print(f"Query window {s} ~ {e}")

            for v in query_videos_in_window(token, s, e):
                vid = v.get("id") or v.get("video_id")
                username = v.get("username", "")
                video_url = build_video_url(username, vid) if (vid and username) else ""
                vwriter.writerow({
                    "video_id": vid,
                    "username": username,
                    "create_time": v.get("create_time"),
                    "region_code": v.get("region_code"),
                    "video_description": v.get("video_description",""),
                    "view_count": v.get("view_count"),
                    "like_count": v.get("like_count"),
                    "comment_count": v.get("comment_count"),
                    "share_count": v.get("share_count"),
                    "video_duration": v.get("video_duration"),
                    "hashtag_names": ",".join(v.get("hashtag_names", [])) if isinstance(v.get("hashtag_names"), list) else v.get("hashtag_names"),
                    "video_url": video_url
                })

                # 拉取评论
                if v.get("comment_count", 0) and vid:
                    try:
                        for c in fetch_all_comments(token, vid):
                            cwriter.writerow({
                                "video_id": c.get("video_id"),
                                "comment_id": c.get("id"),
                                "text": c.get("text", ""),
                                "like_count": c.get("like_count"),
                                "reply_count": c.get("reply_count"),
                                "parent_comment_id": c.get("parent_comment_id"),
                                "create_time": c.get("create_time"),
                            })
                    except Exception as e:
                        print(f"[SKIP VIDEO COMMENTS] video_id={vid} error={e}")

            window_start = window_end + timedelta(days=1)

    print(f"Done. Saved {VIDEOS_OUT} and {COMMENTS_OUT}")

if __name__ == "__main__":
    main()
