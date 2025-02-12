from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import praw
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import time
from bson import ObjectId

load_dotenv()

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client['reddit_collector']
tasks_collection = db['tasks']
posts_collection = db['posts']

# Reddit API setup
reddit = praw.Reddit(
    client_id='Gx7nSI8Vlv02716EnBR2WA',
    client_secret='6R3jkaRkGsP7vGuV5wlY1ZYm77FzMQ',
    user_agent='RedditDataCollector/1.0'
)

def update_progress(task_id, progress, status="in_progress"):
    tasks_collection.update_one(
        {"_id": ObjectId(task_id)},
        {"$set": {"progress": progress, "status": status}}
    )
    socketio.emit('progress_update', {'task_id': str(task_id), 'progress': progress, 'status': status})

@app.route('/api/tasks', methods=['POST'])
def create_task():
    data = request.json
    collection_type = data['type']  # 'keyword', 'user', or 'subreddit'
    target = data['target']
    limit = int(data['limit'])

    task = {
        "type": collection_type,
        "target": target,
        "limit": limit,
        "progress": 0,
        "status": "pending",
        "created_at": time.time()
    }
    
    task_id = tasks_collection.insert_one(task).inserted_id
    
    # Start collection process
    if collection_type == 'keyword':
        socketio.start_background_task(collect_by_keyword, str(task_id), target, limit)
    elif collection_type == 'user':
        socketio.start_background_task(collect_by_user, str(task_id), target, limit)
    elif collection_type == 'subreddit':
        socketio.start_background_task(collect_by_subreddit, str(task_id), target, limit)

    return jsonify({"task_id": str(task_id)})

def collect_by_keyword(task_id, keyword, limit):
    collected = 0
    try:
        for submission in reddit.subreddit('all').search(keyword, limit=limit):
            post_data = {
                "task_id": task_id,
                "title": submission.title,
                "author": str(submission.author),
                "subreddit": submission.subreddit.display_name,
                "score": submission.score,
                "url": submission.url,
                "created_utc": submission.created_utc
            }
            posts_collection.insert_one(post_data)
            collected += 1
            progress = (collected / limit) * 100
            update_progress(task_id, progress)
        
        update_progress(task_id, 100, "completed")
    except Exception as e:
        update_progress(task_id, collected/limit * 100, "error")

def collect_by_user(task_id, username, limit):
    collected = 0
    try:
        for submission in reddit.redditor(username).submissions.new(limit=limit):
            post_data = {
                "task_id": task_id,
                "title": submission.title,
                "author": str(submission.author),
                "subreddit": submission.subreddit.display_name,
                "score": submission.score,
                "url": submission.url,
                "created_utc": submission.created_utc
            }
            posts_collection.insert_one(post_data)
            collected += 1
            progress = (collected / limit) * 100
            update_progress(task_id, progress)
        
        update_progress(task_id, 100, "completed")
    except Exception as e:
        update_progress(task_id, collected/limit * 100, "error")

def collect_by_subreddit(task_id, subreddit_name, limit):
    collected = 0
    try:
        for submission in reddit.subreddit(subreddit_name).new(limit=limit):
            post_data = {
                "task_id": task_id,
                "title": submission.title,
                "author": str(submission.author),
                "subreddit": submission.subreddit.display_name,
                "score": submission.score,
                "url": submission.url,
                "created_utc": submission.created_utc
            }
            posts_collection.insert_one(post_data)
            collected += 1
            progress = (collected / limit) * 100
            update_progress(task_id, progress)
        
        update_progress(task_id, 100, "completed")
    except Exception as e:
        update_progress(task_id, collected/limit * 100, "error")

@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    tasks = list(tasks_collection.find())
    for task in tasks:
        task['_id'] = str(task['_id'])
    return jsonify(tasks)

@app.route('/api/tasks/<task_id>/posts', methods=['GET'])
def get_task_posts(task_id):
    posts = list(posts_collection.find({"task_id": task_id}))
    for post in posts:
        post['_id'] = str(post['_id'])
    return jsonify(posts)

if __name__ == '__main__':
    socketio.run(app, debug=True, port=5000)
