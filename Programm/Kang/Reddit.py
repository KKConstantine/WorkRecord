import praw
import time
import json
from datetime import datetime
import logging
import pandas as pd
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import concurrent.futures
import sys
import os
from pymongo import MongoClient
from prawcore import exceptions as prawcore_exceptions
from urllib.parse import quote_plus

# 设置Windows控制台编码为UTF-8
if sys.platform == 'win32':
    os.system('chcp 65001')

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reddit_scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

username = 'admin'
password = 'YYKJ@2025'

encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

connection_string = f'mongodb://{encoded_username}:{encoded_password}@20.163.114.179:27017/?authSource=admin'

class MongoDBHandler:
    def __init__(self):
        self.client =  MongoClient(connection_string)
        self.db = self.client['reddit_db']
        self.collection = self.db['posts']
        self.counter = self.get_last_counter() + 1

    def get_last_counter(self) -> int:
        """获取最后一条记录的计数器值"""
        try:
            last_record = self.collection.find_one(sort=[("counter", -1)])
            return last_record['counter'] if last_record else 0
        except Exception as e:
            logging.error(f"获取最后计数器时出错: {e}")
            return 0

    def save_posts(self, posts: List[Dict]):
        """保存帖子到MongoDB"""
        try:
            for post in posts:
                post['counter'] = self.counter
                self.collection.insert_one(post)
                self.counter += 1
            logging.info(f"已保存 {len(posts)} 条记录到MongoDB，当前计数: {self.counter - 1}")
        except Exception as e:
            logging.error(f"保存帖子到MongoDB时出错: {e}")

class UserPool:
    def __init__(self):
        self.users = [
            {
                "client_id": "Gx7nSI8Vlv02716EnBR2WA",
                "client_secret": "6R3jkaRkGsP7vGuV5wlY1ZYm77FzMQ",
                "username": "KKConstantine",
                "password": "loki1314",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            {
                "client_id": "GEKZlOPcIIw27DvftU85Xg",
                "client_secret": "Oz44IWDUSZk6bRhkSuJM3xzv1Cbnug",
                "username": "Ambitious-Cup8681",
                "password": "loki1314",
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/100.0"
            },
        ]
        self.current_index = 0
        self.lock = Lock()

    def get_user(self) -> Dict:
        with self.lock:
            user = self.users[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.users)
            return user.copy()

class RedditScraper:
    def __init__(self, max_workers: int = 20):
        # 创建data文件夹
        if not os.path.exists('data'):
            os.makedirs('data')

        self.user_pool = UserPool()
        self.posts_data = []
        self.data_lock = Lock()
        self.seen_posts = set()
        self.max_workers = max_workers
        self.batch_size = 100
        self.reddit_instances = []
        self.mongodb = MongoDBHandler()
        self.initialize_reddit_instances()
        self.all_posts = []  # 用于存储所有帖子数据，避免在scrape_subreddit中无限增长
        self.all_posts_lock = Lock()  # 线程锁，用于保护all_posts
        self.initialize_seen_posts() # 初始化seen_posts

    def initialize_reddit_instances(self):
        """预先初始化Reddit实例"""
        for _ in range(self.max_workers):
            user = self.user_pool.get_user()
            reddit = praw.Reddit(**user)
            self.reddit_instances.append(reddit)
        logging.info(f"已初始化 {self.max_workers} 个Reddit实例")

    def initialize_seen_posts(self):
        """从数据库初始化seen_posts"""
        try:
            # 从数据库读取所有已保存帖子的ID
            all_ids = self.mongodb.collection.distinct("id")
            with self.data_lock:
                self.seen_posts.update(all_ids)
            logging.info(f"已从数据库加载 {len(self.seen_posts)} 个帖子ID到 seen_posts")
        except Exception as e:
            logging.error(f"初始化 seen_posts 时出错: {e}")

    def fetch_posts_batch(self, reddit: praw.Reddit, subreddit_name: str,
                            after_id: str = None) -> List[Dict]:
        """获取一批帖子"""
        try:
            subreddit = reddit.subreddit(subreddit_name)
            posts = []

            for post in subreddit.new(limit=self.batch_size,
                                        params={'after': f"t3_{after_id}" if after_id else None}):
                post_id = post.id
                with self.data_lock:
                    if post_id not in self.seen_posts:
                        self.seen_posts.add(post_id)
                        posts.append({
                            'id': post_id,
                            'title': post.title,
                            'author': post.author.name if post.author else '[deleted]',
                            'created_utc': datetime.fromtimestamp(post.created_utc),
                            'score': post.score,
                            'num_comments': post.num_comments,
                            'url': post.url,
                            'selftext': post.selftext
                        })
            time.sleep(0.5) # 避免过于频繁的请求
            return posts
        except prawcore_exceptions.TooManyRequests as e:
            logging.warning("达到请求频率限制，等待10秒后重试...")
            time.sleep(10)
            return self.fetch_posts_batch(reddit, subreddit_name, after_id)  # 重试
        except Exception as e:
            logging.error(f"获取帖子时出错: {str(e)}")
            return []

    def process_posts(self, reddit: praw.Reddit, subreddit_name: str,
                        after_id: str = None) -> str:
        """处理一批帖子并返回最后一个帖子ID"""
        posts = self.fetch_posts_batch(reddit, subreddit_name, after_id)
        if posts:
            last_id = posts[-1]['id']
            with self.all_posts_lock:
                self.all_posts.extend(posts)  # 将新抓取的帖子添加到all_posts
            return last_id
        return after_id

    def save_data_batch(self, batch_data: List[Dict], batch_num: int):
        """保存批次数据"""
        try:
            # 保存到MongoDB
            self.mongodb.save_posts(batch_data)

            # 使用固定文件名，这样可以持续更新同一个文件
            csv_path = f"data/reddit_data_all.csv"
            json_path = f"data/reddit_data_all.json"

            # 保存CSV - 追加模式
            df = pd.DataFrame(batch_data)
            if batch_num == 0: # 第一次写入包含表头
                df.to_csv(csv_path, index=False, encoding='utf-8-sig', mode='w')
            else: # 后续追加不包含表头
                df.to_csv(csv_path, index=False, encoding='utf-8-sig', mode='a', header=False)

            # 保存JSON - 读取现有数据并更新
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                existing_data = []

            existing_data.extend(batch_data)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=4)  # 使用indent使JSON更易读
        except Exception as e:
            logging.error(f"保存批次数据时出错: {str(e)}")

    def scrape_subreddit(self, subreddit_name: str):
        """使用线程池并发爬取"""
        logging.info(f"开始爬取子版块 {subreddit_name}")
        batch_num = 0
        start_time = time.time()
        after_ids = [None] * self.max_workers  # 初始化每个worker的after_id

        try:
            while True:  # 无限循环爬取
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self.process_posts, self.reddit_instances[i], subreddit_name, after_ids[i])
                        for i in range(self.max_workers)
                    ]

                    for i, future in enumerate(futures):
                        try:
                            last_id = future.result()
                            if last_id:
                                after_ids[i] = last_id  # 更新对应的after_id
                        except Exception as e:
                            logging.error(f"处理批次时出错: {str(e)}")

                # 从all_posts中取出数据进行保存
                batch_data = []
                with self.all_posts_lock:
                    if len(self.all_posts) >= self.batch_size:
                        batch_data = self.all_posts[:self.batch_size]
                        self.all_posts = self.all_posts[self.batch_size:]
                    elif self.all_posts:
                        batch_data = self.all_posts
                        self.all_posts = []  # 清空

                if batch_data:
                    self.save_data_batch(batch_data, batch_num)
                    batch_num += 1

                elapsed_time = time.time() - start_time
                total_posts = 0
                try:
                    with open("data/reddit_data_all.csv", 'r', encoding='utf-8-sig') as f:
                        total_posts = sum(1 for row in f) - 1
                except FileNotFoundError:
                    logging.warning("CSV文件未找到，无法统计数据量")
                speed = total_posts / elapsed_time if elapsed_time > 0 else 0
                logging.info(f"已爬取 {total_posts} 个帖子，速度：{speed:.2f} 帖子/秒")

                time.sleep(1)  # 每次循环后暂停1秒，避免过于频繁的请求

        except KeyboardInterrupt:
            logging.info("程序被用户中断")
        except Exception as e:
            logging.error(f"程序出现错误: {str(e)}")
        finally:
            # 确保程序结束时，剩余的数据被保存
            if self.all_posts:
                self.save_data_batch(self.all_posts, batch_num)
                logging.info("程序结束，剩余数据已保存")

if __name__ == "__main__":
    SUBREDDIT_NAME = "java"  # 要爬取的子版块
    MAX_WORKERS = 20  # 并发数量

    try:
        scraper = RedditScraper(max_workers=MAX_WORKERS)
        scraper.scrape_subreddit(
            subreddit_name=SUBREDDIT_NAME
        )
    except KeyboardInterrupt:
        logging.info("程序被用户中断")
    except Exception as e:
        logging.error(f"程序出现错误: {str(e)}")