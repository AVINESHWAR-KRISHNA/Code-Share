from flask import Flask, request
from google.cloud import pubsub_v1
from queue import Queue
import threading
import os
import pandas as pd
from sqlalchemy import create_engine, text, bindparam
import json
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

SERVER_NAME = ''
DATABASE = ''
DRIVER = 'SQL+Server'
TABLE_NAME = 'SSM'
credentials_path = ""
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
topic_path = ""
subscription_path =  ""
database_url = f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}'

app = Flask(__name__)
message_queue = Queue()
connection_pool = Queue(maxsize=20)
executor = ThreadPoolExecutor()
start_event = threading.Event()
NUM_THREADS = 20

def initialize_connection_pool():
    for _ in range(20):
        engine = create_engine(database_url,fast_executemany=True)
        connection = engine.connect()
        connection_pool.put(connection)

def get_database_connection():
    return connection_pool.get()

def release_database_connection(connection):
    connection_pool.put(connection)

def callback(message):

    message_queue.put(message.data)
    message.ack()

@app.route('/', methods=['POST'])
def publish_message():
    try:
        data = json.dumps(request.json)
        data = data.encode('utf-8')

        # print(f"Received data: {data}")
        attributes = {
            "client": "SSM",
            "eventType": "Update"
        }
        future = publisher.publish(topic_path, data, **attributes)
        print(f"Published message with ID: {future.result()}")
        return future.result()
    except Exception as e:
        print(f"Error publishing message: {str(e)}")
        return "Error"

def normalize_data(data):
    normalized_data = []

    for item in data:
        row = {}

        for key, value in item.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    row[f"{key}_{sub_key}"] = sub_value
            elif isinstance(value, list):
                for idx, sub_item in enumerate(value, start=1):
                    if isinstance(sub_item, dict):
                        for sub_key, sub_value in sub_item.items():
                            if isinstance(sub_value, dict):
                                for sub_sub_key, sub_sub_value in sub_value.items():
                                    row[f"{key}_{idx}_{sub_key}_{sub_sub_key}"] = sub_sub_value
                            else:
                                row[f"{key}_{idx}_{sub_key}"] = sub_value
                    else:
                        row[f"{key}_{idx}"] = sub_item
            else:
                row[key] = value

        normalized_data.append(row)
    return normalized_data

def process_message(DATA):

    z = json.loads(DATA.decode('utf-8'))

    try:
        N_DATA = normalize_data(z)
    except:
        N_DATA = normalize_data([z])

    df = pd.DataFrame(data=N_DATA)
    df = df.astype(str)

    cnx = get_database_connection()

    insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(df.columns)}) VALUES ({', '.join([':' + col for col in df.columns])})"
    try:
        with cnx.begin() as transaction:
            stmt = text(insert_query)
            stmt = stmt.bindparams(*[bindparam(col) for col in df.columns])
            cnx.execute(stmt, df.to_dict(orient='records'))
            transaction.commit()
    
        release_database_connection(cnx)
        print(f"Data inserted successfully")

    except Exception as e:
        print(e)

def process_messages():

    start_event.wait()
    while True:
        DATA = message_queue.get()
        executor.submit(process_message, DATA)

# def shutdown_handler(signal, frame):
#     print("Shutting down the application...")
#     executor.shutdown(wait=True)
#     sys.exit(0)

if __name__ == "__main__":
    try:
        subscriber = pubsub_v1.SubscriberClient()
        publisher = pubsub_v1.PublisherClient()

        initialize_connection_pool()

        future = subscriber.subscribe(subscription_path, callback=callback)

        for _ in range(NUM_THREADS):
            queue_thread = threading.Thread(target=process_messages)
            queue_thread.start()

        start_event.set()

        # signal.signal(signal.SIGINT, shutdown_handler)
        # signal.signal(signal.SIGTERM, shutdown_handler)

        app.run(port=9000, debug=True)
    except Exception as e:
        print(f"Error starting the application: {str(e)}")
