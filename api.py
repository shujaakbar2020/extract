from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
# from confluent_kafka import Producer
from typing import Optional
from pydantic import BaseModel
from kafka import KafkaProducer
import os

import json
app = FastAPI()
import psycopg2

# Database connection parameters
host = os.environ.get('POSTGRES_DB_HOST')
port = os.environ.get('POSTGRES_DB_POSRT')
database = os.environ.get('POSTGRES_DB_NAME')
username = os.environ.get('POSTGRES_DB_USERNAME')
password = os.environ.get('POSTGRES_DB_PASSWORD')

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host=host,
    port=port,
    database='db1',
    user=username,
    password=password
)

# Define a Pydantic model for the data to be received in the POST request
class User(BaseModel):
    username: str
    email: str



def publish_message(kafka_producer, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        kafka_producer.send(topic_name, key=key_bytes, value=value_bytes)
        kafka_producer.flush()
        print('Message published successfully.')
    except Exception as ex:
        print(str(ex))


@app.post("/send-to-kafka")
async def send_message(user: User):
    kafka_producer = KafkaProducer(bootstrap_servers=['135.125.219.92:9092'], api_version=(0, 10))

    cursor = conn.cursor()
    select_data_query = """
        SELECT * FROM test_one;
        """

    cursor.execute(select_data_query)
    rows = cursor.fetchall()
    employees_list=[]
    for row in rows:
        temp={}
        temp['id']=str(row[0])
        temp['name']=row[1]
        temp['age']=row[2]
        employees_list.append(temp)
    print("List",employees_list)
    for employee in employees_list:
        print("MAnzoor")
        publish_message(
        kafka_producer=kafka_producer,
        topic_name='hussain_test',
        key=employee['id'],
        value=json.dumps(employee)
        )
    if kafka_producer is not None:
        kafka_producer.close()
    return {"message": "Sent to Kafka successfully!"}