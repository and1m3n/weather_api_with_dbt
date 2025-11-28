import psycopg2
import os
from api_request import fetch_data
from dotenv import load_dotenv

def connect_to_bd():
    print("Connecting to the PostgresSQL db")
    try:
        load_dotenv('/opt/airflow/dags/.env') 
        conn = psycopg2.connect(
            host=os.getenv("host"),
            port=os.getenv("port"),
            dbname=os.getenv('dbname'),
            user=os.getenv('dbuser'),
            password=os.getenv('dbpassword')
        )
        return conn
    except psycopg2.Error as e:
        print(f'error conection to DB :{e}')
        raise

def crate_table(conn):
    print('Creating table if not exists...')
    try:
        cursor=conn.cursor()
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS dev;
        CREATE TABLE IF NOT EXISTS dev.raw_weather_data(
            id SERIAL PRIMARY KEY,
            city TEXT,
            temperature FLOAT,
            weather_desc TEXT,
            wind_speed FLOAT,
            time TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
        """ )
        conn.commit()
        print('Table was created')
    except psycopg2.Error as e:
        print(f'Failed to create: {e}')

def insert_records(conn,data):
    print('Insert data to bd')
    try:
        weather = data['current']
        location= data['location']
        cursor=conn.cursor()
        cursor.execute("""
            INSERT INTO dev.raw_weather_data(
                    city,
                    temperature,
                    weather_desc,
                    wind_speed,
                    time) VALUES(%s,%s,%s,%s,%s)
        """,(
        location['name'],
        weather['temperature'],
        weather['weather_descriptions'][0],
        weather['wind_speed'],
        location['localtime']
        ))
        conn.commit()
        print('data successfuly iserted')
    except psycopg2.Error as e:
        print(f'Error inserting data into db: {e}')


def main():
    conn = None       
    try:
        data = fetch_data()
        conn = connect_to_bd()
        crate_table(conn)
        insert_records(conn,data)
    except Exception as e:
        print(f'error as e:{e}')
    finally:
        if conn is not None:
            conn.close()
            print('Database connetion closed.')
