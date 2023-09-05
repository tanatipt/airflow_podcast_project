from airflow.decorators import task
from kafka import KafkaProducer
from json import dumps
import sys
import random
from datetime import timedelta
from airflow.exceptions import AirflowTaskTimeout


def connect_producer(id):
    try:
        producer = KafkaProducer(bootstrap_servers=id, client_id="podcast_etl", key_serializer=lambda x: x.encode('utf-8'),
                                 value_serializer=lambda x: dumps(x).encode('utf-8'), acks="all", retries=sys.maxsize, batch_size=8000, linger_ms=1000)
    except Exception as e:
        print("An error has occured while creating the producer : ", e)
        producer = None

    return producer


@task(task_id="run_producer", execution_timeout=timedelta(minutes=2))
def run_producer(ti=None):
    shows = ti.xcom_pull(key="return_value", task_ids="get_episodes")

    producer = connect_producer("13.213.68.5:9092")

    if producer is not None:
        while True:
            try:
                shows_name = list(shows.keys())
                random_show = random.choice(shows_name)
                episodes_list = shows[random_show]
                random_episode = random.choice(episodes_list)
                producer.send("podcast_project", key=random_show,
                              value=random_episode)
                producer.flush(timeout=1)

            except AirflowTaskTimeout as e:
                break
            except Exception as e:
                print("An error has occured while sending message to Kafka : ", e)

        producer.close()
