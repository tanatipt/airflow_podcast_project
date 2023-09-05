from airflow import DAG
from datetime import datetime
from airflow.decorators import task
import requests
import xmltodict
from bs4 import BeautifulSoup
from podcast_producer import run_producer
from podcast_consumer import run_consumer
from airflow.providers.mongo.hooks.mongo import MongoHook
from topic_definition import delete_topic, create_topic
import os

with DAG("podcast_summary", start_date=datetime(2022, 5, 31), catchup=False) as dag:

    @task(task_id="get_shows")
    def get_shows():
        data = requests.get("https://www.marketplace.org/")
        soup = BeautifulSoup(data.text)

        heading = soup.find("li",  {"class": "heading"})
        show_tags = heading.find_next_siblings()
        show_names = []

        for tag in show_tags:
            a_tag = tag.find("a")
            link = a_tag["href"].split("/")
            show_names.append(link[4])

        return show_names

    @task(task_id="get_episodes")
    def get_episodes(ti=None):
        shows = {}
        shows_name = ti.xcom_pull(key="return_value", task_ids="get_shows")
        for show in shows_name:
            data = requests.get(
                "https://www.marketplace.org/feed/podcast/" + show)
            feed = xmltodict.parse(data.text)
            episodes = feed["rss"]["channel"]["item"]
            shows[show] = episodes

        return shows

    @task(task_id="download_episodes")
    def download_episodes(ti=None):
        shows = ti.xcom_pull(
            key='return_value', task_ids="get_episodes")

        for show, episodes in shows.items():
            for episode in episodes:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                audio_path = f"/mnt/c/Users/User/Documents/personal_projects/podcast_etl_project/{show}/{filename}"

                if not os.path.exists(audio_path):
                    audio = requests.get(episode["enclosure"]["@url"])

                    with open(audio_path, "wb+") as f:
                        f.write(audio.content)

    @task(task_id="show_statistics")
    def show_statistics(ti=None):
        hook = MongoHook(conn_id="mongoid")
        client = hook.get_conn()
        db = client["podcast_data"]
        shows_data = db["show_statistics"]
        shows_name = ti.xcom_pull(key="return_value", task_ids="get_shows")

        for show in shows_name:
            pipeline = [{"$match": {"description": {"$ne": None}, "title": {"$ne": None}}}, {"$set": {"description_words":  {"$size": {"$split": ["$description", " "]}}, "title_words":  {"$size": {"$split": ["$title", " "]}}}},
                        {"$group": {"_id": None, "avg_duration": {"$avg": "$duration"}, "min_duration": {"$min": "$duration"}, "max_duration": {"$max": "$duration"}, "avg_title": {"$avg": "$title_words"}, "min_title": {
                            "$min": "$title_words"}, "max_title": {"$max": "$title_words"}, "avg_description": {"$avg": "$description_words"}, "min_description": {"$min": "$description_words"}, "max_description": {"$max": "$description_words"}}},
                        {"$set": {"show_name": show}}, {"$project": {"_id": 0}}]
            statistics = list(db[show].aggregate(pipeline))[0]
            shows_data.update_one({"show_name": show},
                                  {"$set": statistics}, upsert=True)

    topic_create = create_topic()
    topic_delete = delete_topic()
    producer_run = run_producer()
    consumer_run = run_consumer()
    retrieve_episodes = get_episodes()

    get_shows() >> retrieve_episodes >> topic_create >> [
        producer_run, consumer_run] >> topic_delete
    topic_create >> topic_delete
    consumer_run >> show_statistics()
    retrieve_episodes >> download_episodes()
