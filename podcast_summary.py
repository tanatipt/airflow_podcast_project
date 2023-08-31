from airflow import DAG
from datetime import datetime
from airflow.decorators import task
import requests
import xmltodict
from airflow.providers.mongo.hooks.mongo import MongoHook
import os

mongo_id = "mongoid"
with DAG("podcast_summary", start_date=datetime(2022, 5, 31), catchup=False) as dag:

    @task
    def get_episodes(task_id="get_episodes"):
        shows = {}
        shows_name = ["marketplace", "make-me-smart", "the-uncertain-hour"]
        for show in shows_name:
            data = requests.get(
                "https://www.marketplace.org/feed/podcast/" + show)
            feed = xmltodict.parse(data.text)
            episodes = feed["rss"]["channel"]["item"]
            shows[show] = episodes

        return shows

    def compute_sec(time_str):
        h, m, s = time_str.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

    @task
    def load_episodes(ti=None, task_id="load_episodes"):
        shows = ti.xcom_pull(
            key='return_value', task_ids="get_episodes")

        hook = MongoHook(conn_id=mongo_id)
        client = hook.get_conn()
        db = client["podcast_data"]

        for show, episodes in shows.items():
            collection = db[show]

            for episode in episodes:
                duration = compute_sec(episode["itunes:duration"])

                filename = f"{episode['link'].split('/')[-1]}.mp3"
                episode_document = {"link": episode["link"], "title": episode["title"], "pubDate": episode["pubDate"], "description": episode["description"], "episodesType": episode["itunes:episodeType"], "explicit":
                                    episode["itunes:explicit"], "duration": duration, "author": episode["itunes:author"], "filename": filename}

                collection.update_one({"title": episode["title"]},
                                      {"$set": episode_document}, upsert=True)

    loading = load_episodes()

    @task
    def download_episodes(ti=None, task_id="download_episodes"):
        shows = ti.xcom_pull(
            key='return_value', task_ids="get_episodes")

        for show, episodes in shows.items():
            for episode in episodes:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                audio_path = f"/mnt/c/Users/User/Documents/airflow_podcast_project/{show}/{filename}"

                if not os.path.exists(audio_path):
                    audio = requests.get(episode["enclosure"]["@url"])

                    with open(audio_path, "wb+") as f:
                        f.write(audio.content)

    @task
    def show_statistics(task_id="show_statistics"):
        hook = MongoHook(conn_id=mongo_id)
        client = hook.get_conn()
        db = client["podcast_data"]
        shows_data = db["show_statistics"]
        shows_name = ["marketplace", "make-me-smart", "the-uncertain-hour"]

        for show in shows_name:
            pipeline = [{"$match": {"description": {"$ne": None}, "title": {"$ne": None}}}, {"$set": {"description_words":  {"$size": {"$split": ["$description", " "]}}, "title_words":  {"$size": {"$split": ["$title", " "]}}}},
                        {"$group": {"_id": None, "avg_duration": {"$avg": "$duration"}, "min_duration": {"$min": "$duration"}, "max_duration": {"$max": "$duration"}, "avg_title": {"$avg": "$title_words"}, "min_title": {
                            "$min": "$title_words"}, "max_title": {"$max": "$title_words"}, "avg_description": {"$avg": "$description_words"}, "min_description": {"$min": "$description_words"}, "max_description": {"$max": "$description_words"}}},
                        {"$set": {"show_name": show}}, {"$project": {"_id": 0}}]
            statistics = list(db[show].aggregate(pipeline))[0]
            shows_data.update_one({"show_name": show},
                                  {"$set": statistics}, upsert=True)

    get_episodes() >> [loading, download_episodes()]
    loading >> show_statistics()
