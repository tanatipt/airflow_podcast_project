from airflow.decorators import task
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.cluster import ClusterMetadata
from time import sleep


def connect_client(id):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=id, client_id="podcast_etl")
    except Exception as e:
        print("An error has occured while connecting to admin client in Kafka : ", e)
        admin_client = None

    return admin_client


@task(task_id="create_topic")
def create_topic():
    admin_client = connect_client("13.213.68.5:9092")

    if admin_client is not None:
        try:
            topic = NewTopic("podcast_project", 1, 1)

            admin_client.create_topics([topic], 1000)
        except Exception as e:
            print("An error has occured while creating topics in Kafka : ", e)
        finally:
            admin_client.close()


@task(task_id="delete_topic", trigger_rule="one_success")
def delete_topic():

    admin_client = connect_client("13.213.68.5:9092")

    print(admin_client)
    if admin_client is not None:
        try:
            admin_client.delete_topics(
                ["podcast_project"], timeout_ms=5000)
        except Exception as e:
            print("An error has occured while deleting topics in Kafka : ", e)

        admin_client.close()
