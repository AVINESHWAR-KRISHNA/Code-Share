Certainly, here's the complete code with the modifications:

```python
from flask import Flask, request
from google.cloud import pubsub_v1
import json
import threading
import queue
from tenacity import retry, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor
import os
import pandas as pd
from sqlalchemy import create_engine, text, bindparam
import logging
from prometheus_client import start_http_server, Counter

app = Flask(__name__)

class PubSubToSql:
    def __init__(self, topic_path, subscription_path, SERVER_NAME, DATABASE, TABLE_NAME, DRIVER, SQL_POOL_SIZE=20, ProcessingThread=10):

        self.SERVER_NAME = SERVER_NAME
        self.DATABASE = DATABASE
        self.TABLE_NAME = TABLE_NAME
        self.DRIVER = DRIVER
        self.SQL_POOL_SIZE = SQL_POOL_SIZE
        self.ProcessingThread = ProcessingThread

        self.message_queue = queue.Queue()
        self.connection_pool = queue.Queue(maxsize=20)
        
        self.database_url = f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}'

        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = topic_path
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(subscription_path)

        self.shutdown_requested = False

        self._configure_logging()
        self._configure_metrics()

    def _configure_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _configure_metrics(self):
        start_http_server(8001)  # Start Prometheus metrics server on port 8001

        self.processed_messages_counter = Counter('processed_messages', 'Number of processed messages')
        self.message_processing_errors_counter = Counter('message_processing_errors', 'Number of message processing errors')
    
    def initialize_connection_pool(self):
        for _ in range(self.SQL_POOL_SIZE):
            engine = create_engine(self.database_url, fast_executemany=True)
            connection = engine.connect()
            self.connection_pool.put(connection)

    def get_database_connection(self):
        return self.connection_pool.get()

    def release_database_connection(self, connection):
        self.connection_pool.put(connection)
    
    def normalize_data(self, data):
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

    @retry(stop=stop_after_attempt(3))
    def insert_data_into_sql(self, data):
        z = json.loads(data.decode('utf-8'))

        try:
            N_DATA = self.normalize_data(z)
        except:
            N_DATA = self.normalize_data([z])

        df = pd.DataFrame(data=N_DATA)
        df = df.astype(str)

        cnx = self.get_database_connection()

        insert_query = f"INSERT INTO {self.TABLE_NAME} ({', '.join(df.columns)}) VALUES ({', '.join([':' + col for col in df.columns])})"
        try:
            with cnx.begin() as transaction:
                stmt = text(insert_query)
                stmt = stmt.bindparams(*[bindparam(col) for col in df.columns])
                cnx.execute(stmt, df.to_dict(orient='records'))
                transaction.commit()
        
            self.release_database_connection(cnx)
            print(f"Data inserted successfully")

        except Exception as e:
            self.release_database_connection(cnx)
            print(e)

    def acknowledge_message(self, message):
        message.ack()

    @app.route('/', methods=['POST'])
    def receive_message(self):
        try:
            if not self.shutdown_requested:
                data = json.loads(request.data.decode('utf-8'))
                self.message_queue.put((data, None))  # Put data in the queue for processing

                # Publish the message
                data = json.dumps(data)
                data = data.encode('utf-8')

                attributes = {
                    "client": "SSM",
                    "eventType": "Update"
                }
                future = self.publisher.publish(self.topic_path, data, **attributes)
                print(f"Published message with ID: {future.result()}")
                return future.result()

        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)
            return "Error"

    def message_receiver(self):
        def callback(message):
            try:
                if not self.shutdown_requested:
                    data = json.loads(message.data.decode('utf-8'))
                    self.message_queue.put((data, message))

            except Exception as e:
                self.logger.error(f"Error processing message: {e}", exc_info=True)

        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=callback)

        try:
            streaming_pull_future.result()
        except Exception as e:
            self.logger.error(f"Error in message_receiver: {e}", exc_info=True)

    def data_processor(self):
        while not self.shutdown_requested:
            data, message = self.message_queue.get() 

            if data is not None:
                try:
                    self.insert_data_into_sql(data)
                    self.acknowledge_message(message)
                    self.processed_messages_counter.inc()

                except Exception as e:
                    self.logger.error(f"Error processing data: {e}", exc_info=True)
                    self.message_processing_errors_counter.inc()

    def start_processing(self):
        self.initialize_connection_pool()
        receiver_thread = threading.Thread(target=self.message_receiver)
        receiver_thread.start()

        with ThreadPoolExecutor(max_workers=self.ProcessingThread) as executor:
            executor.map(self.data_processor)

if __name__ == "__main__":
    credentials_path = "PYTHON/Win_Service/gcp-service-account.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    topic_path = "authorization.prod" 
    subscription_path = "projects/authorization.prod" 
    SERVER_NAME = ''
    DATABASE = ''
    TABLE_NAME = 'SSM'
    DRIVER = 'SQL+Server'

    num_instances = 3

    # Start the Flask app in a separate thread
    flask_thread = threading.Thread(target=lambda: app.run(port=8000))
    flask_thread.start()

    # Start the Pub/Sub to SQL processing threads
    with ThreadPoolExecutor(max_workers=num_instances) as executor:
        for _ in range(num_instances):
            pubsub_to_sql = PubSubToSql(topic_path, subscription_path, SERVER_NAME, DATABASE, TABLE_NAME, DRIVER)
            executor.submit(pubsub_to_sql.start_processing)
```

This code should work as a Flask app handling Pub/Sub messages while inserting the data into a SQL database. Make sure to adjust the configurations, such as database credentials and Pub/Sub topics,

 to match your specific setup.
