from google.cloud import pubsub_v1
import json
import pyodbc
import threading
import queue
from tenacity import retry, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor
import time
from prometheus_client import start_http_server, Counter
from flask import Flask, request
import os
from sqlalchemy import create_engine, text, bindparam
import pandas as pd

class PubSubToSql:
    def __init__(self, project_id, subscription_id, server, database, username, password, table_name, min_pool_size=1, max_pool_size=5):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.table_name = table_name
        self.message_queue = queue.Queue()
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size

        self.conn_pool = pyodbc.pooling.SimpleConnectionPool(self.min_pool_size, self.max_pool_size, f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}')

        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)

        self.shutdown_requested = False

        # Configure logging
        self._configure_logging()

        # Configure Prometheus metrics
        self._configure_metrics()

    def _configure_logging(self):
        import logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _configure_metrics(self):
        start_http_server(8000)  # Start Prometheus metrics server on port 8000

        # Define metrics
        self.processed_messages_counter = Counter('processed_messages', 'Number of processed messages')
        self.message_processing_errors_counter = Counter('message_processing_errors', 'Number of message processing errors')

    @retry(stop=stop_after_attempt(3))  # Retry up to 3 times on exceptions
    def insert_data_into_sql(self, data):
        try:
            conn = self.conn_pool.getconn()
            cursor = conn.cursor()
            for item in data:
                cursor.execute(f"INSERT INTO {self.table_name} (column1, column2) VALUES (?, ?)",
                               item['column1'], item['column2'])
            conn.commit()
        finally:
            self.conn_pool.putconn(conn)

    def acknowledge_message(self, message):
        message.ack()

    def message_receiver(self):
        def callback(message):
            try:
                if not self.shutdown_requested:
                    data = json.loads(message.data.decode('utf-8'))
                    self.message_queue.put((data, message))  # Put data and message in the queue for processing
            except Exception as e:
                self.logger.error(f"Error processing message: {e}", exc_info=True)

        # Subscribe to the Pub/Sub topic and start receiving messages
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=callback)
        try:
            streaming_pull_future.result()
        except Exception as e:
            self.logger.error(f"Error in message_receiver: {e}", exc_info=True)

    def data_processor(self):
        while not self.shutdown_requested:
            data, message = self.message_queue.get()  # Get data and message from the queue
            if data is not None:
                try:
                    self.process_message(data)  # Call the process_message method
                    self.acknowledge_message(message)  # Acknowledge successful message processing
                    self.processed_messages_counter.inc()  # Increment processed messages count
                except Exception as e:
                    self.logger.error(f"Error processing data: {e}", exc_info=True)
                    self.message_processing_errors_counter.inc()  # Increment error count

            # Introduce rate limiting by sleeping between iterations
            time.sleep(1)  # Sleep for 1 second between iterations (adjust as needed)

    def start_processing(self, num_processing_threads=10):
        # Create and start threads for message receiving and data processing
        receiver_thread = threading.Thread(target=self.message_receiver)
        receiver_thread.start()

        # Create a ThreadPoolExecutor for data processing
        with ThreadPoolExecutor(max_workers=num_processing_threads) as executor:  # You can adjust the number of workers as needed
            executor.map(self.data_processor)

if __name__ == "__main__":
    project_id = 'your-project-id'
    subscription_id = 'your-subscription-id'
    server = 'your-sql-server'
    database = 'your-database'
    username = 'your-username'
    password = 'your-password'
    table_name = 'your-table-name'

    num_instances = 3  # Number of instances to run

    with ThreadPoolExecutor(max_workers=num_instances) as executor:
        for _ in range(num_instances):
            pubsub_to_sql = PubSubToSql(project_id, subscription_id, server, database, username, password, table_name)
            executor.submit(pubsub_to_sql.start_processing)
