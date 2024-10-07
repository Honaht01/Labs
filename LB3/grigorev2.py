import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os
import numpy as np
import json
import random
import uuid


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
 }
dag = DAG(
    'data_consolidation_grigorev2',
    default_args=default_args,
    description='Consolidate data from CSV, Excel, JSON and save to SQLite',
    schedule_interval='@daily',
 )
 
def generate_orders_and_returns():
    def random_date(start, end):
        return start + timedelta(
         seconds=random.randint(0, int((end - start).total_seconds()))
        )
    start_order = datetime(2023, 1, 1)
    end_order = datetime(2023, 12, 31)
    orders = []

    for i in range(1000):
        order = {
            'order_id': str(uuid.uuid4()),
            'client': random.randint(1, 500),
            'sum': random.randint(5, 100),
            'date': random_date(start_order, end_order).strftime('%Y-%m-%d')
        }
        orders.append(order)
    orders_df = pd.DataFrame(orders)
    orders_df.to_csv(f'orders1.csv', index=False)

    returns = []
    reason= ['Брак', 'Долгая доставка', 'Нашли дешевле', 'Иная причина']
    for a in range(50):
        order = random.choice(orders)
        returno = {
            'return_id': str(uuid.uuid4()),
            'order_id': order['order_id'],
            'reason': random.choice(reason)
        }
        returns.append(returno)
    returns_df = pd.DataFrame(returns)
    returns_df.to_csv(f'returns1.csv', index=False)

def generate_deliveries():
    deliveries = []
    status = ['Запланирована', 'В процессе', 'Выполнена']
    orders_df = pd.read_csv(f'orders1.csv')
    orders_list = orders_df['order_id'].tolist()
    for b in orders_list:
        delivery = {
            'delivery_id': str(uuid.uuid4()),
            'order_id': b,
            'status': random.choice(status)
        }
        deliveries.append(delivery)

    with open(f'deliveries1.json', 'w', encoding='utf-8') as f:
        json.dump(deliveries, f, ensure_ascii=False, indent=4)

def extract_and_transform():
 # Пути к файлам данных
    csv_file = (f'orders1.csv')
    excel_file = (f'returns1.csv')
    json_file = (f'deliveries1.json')

 # Загрузка данных
    csv_data = pd.read_csv(csv_file)
    excel_data = pd.read_csv(excel_file)
    json_data = pd.read_json(json_file)
# Merging
    merged_data1 = pd.merge(csv_data, excel_data, on='order_id', how='left')
    merged_data2 = pd.merge(merged_data1, json_data, on='order_id', how='left')
    merged_data2.to_csv('final1.csv', index=False)

def send_email_notification():
 # Псевдокод для отправки email
    print("Отправить email уведомление")

def confirm_creation():
    print("Base created")


confirm_creation_task = PythonOperator(
    task_id='confirm_creation',
    python_callable=confirm_creation,
    dag=dag
)

generate_orders_and_returns_task = PythonOperator(
    task_id='generate_orders_and_returns',
    python_callable=generate_orders_and_returns,
    dag=dag,
 )


generate_deliveries_task = PythonOperator(
    task_id='generate_deliveries',
    python_callable=generate_deliveries,
    dag=dag,
 )

extract_transform_task = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform,
    dag=dag,
 )

send_email_notification_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_notification,
    dag=dag
)

generate_orders_and_returns_task >> generate_deliveries_task
generate_orders_and_returns_task >> confirm_creation_task 
[confirm_creation_task, generate_deliveries_task ] >> extract_transform_task >> send_email_notification_task