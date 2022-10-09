from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from tweepy import OAuthHandler
from kafka import KafkaProducer

import tweepy

default_args = {
    'owner': 'TD',
    'start_date': datetime(2022, 8, 23),
    'schedule_interval': None,
}

dag = DAG('KAFKA_PRODUCER',
          schedule_interval='@yearly',
          default_args=default_args,
          catchup=False)


def produce_data():
    producer = KafkaProducer(bootstrap_servers='192.168.45.158:9092')

    topic_name = "twitter_kafka"

    try:
        access_token = "2412070568-WR0AEiydRU9ZHmO3La7LFNztybuygWcIuCynqF8"
        access_token_secret =  "b86c5XVXx1O9iaBeo667toEUVqGtCrP35KFDN6I3WceXc"
        consumer_key =  "gmP6tD54SoSuk3V3giiJ1VySP"
        consumer_secret =  "OEWCwnCDfc4I4eU0EJ7RcEzlZZsLQmXui4G54cjace1L1HgOLv"

        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        api = tweepy.API(auth)
        for tweet in api.search_tweets(q="ราคาที่ดิน", lang="th", count=100):
            producer.send(topic_name, str.encode(tweet.text))
            print(f"Message: {tweet.text} produced!")
    except:
        print("Produce failed")

with dag:
    produce_data = PythonOperator(
        task_id='produce_data',
        python_callable=produce_data,
    )

produce_data