from airflow.decorators import task
from kafka import KafkaConsumer
from json import dumps, loads
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import timedelta
from airflow.exceptions import AirflowTaskTimeout


def connect_consumer(id):
    try:
        consumer = KafkaConsumer("podcast_project", bootstrap_servers=id, client_id="podcast_etl", key_deserializer=lambda x: x.decode('utf-8'),
                                 value_deserializer=lambda x: loads(x.decode('utf-8')), group_id="podcast_group")
    except Exception as e:
        print("An error has occured while creating the consumer : ", e)
        consumer = None

    return consumer


def compute_sec(time_str):
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


@task(task_id="run_consumer", execution_timeout=timedelta(minutes=2))
def run_consumer():
    consumer = connect_consumer("13.213.68.5:9092")
    print(consumer)

    if consumer is not None:
        accumulator = []
        while True:
            try:
                records = consumer.poll(timeout_ms=1000)
                if len(records) > 0:
                    for record in list(records.values())[0]:
                        accumulator.append(record)

            except AirflowTaskTimeout as e:
                break
            except Exception as e:
                print("An error has occured while receiving message from Kafka : ", e)

        consumer.close()

        for record in accumulator:
            show_name = record.key
            episode = record.value
            hook = MongoHook(conn_id="mongoid")
            client = hook.get_conn()
            db = client["podcast_data"]
            collection = db[show_name]

            duration = compute_sec(
                episode["itunes:duration"])

            filename = f"{episode['link'].split('/')[-1]}.mp3"
            episode_document = {"link": episode["link"], "title": episode["title"], "pubDate": episode["pubDate"], "description": episode["description"], "episodesType": episode["itunes:episodeType"], "explicit":
                                episode["itunes:explicit"], "duration": duration, "author": episode["itunes:author"], "filename": filename}

            collection.update_one({"title": episode["title"]},
                                  {"$set": episode_document}, upsert=True)
