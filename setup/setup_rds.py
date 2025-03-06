from dotenv import load_dotenv
import os
import pg8000
import pandas as pd

load_dotenv()


def create_table(conn):
    query = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        age INT,
        city VARCHAR(100),
        zip_code VARCHAR(20),
        latitude DECIMAL(10, 6),
        longitude DECIMAL(10, 6),
        number_of_referrals INT,
        offer VARCHAR(50),
        avg_monthly_long_distance_charges DECIMAL(10, 2),
        avg_monthly_gb_download DECIMAL(10, 2),
        streaming_music VARCHAR(5),
        unlimited_data VARCHAR(5),
        total_refunds DECIMAL(10, 2),
        total_extra_data_charges DECIMAL(10, 2),
        total_long_distance_charges DECIMAL(10, 2),
        total_revenue DECIMAL(10, 2)
    );
    """

    try:
        conn.cursor().execute(query)
        conn.commit()
        print("Table 'customers' is ready!")
    except Exception as e:
        print(f"Failed to create table: {e}")
        conn.rollback()


def insert_data(conn):
    try:
        df = pd.read_csv("data/db_data_source/telecom_customer_churn.csv", nrows=100)
        print("Csv file loaded")

    except Exception as e:
        print(f"Error reading CSV: {e}")

    data = [tuple(row.astype(object)) for _, row in df.iterrows()]

    query = """
    INSERT INTO customers (
        customer_id, age, city, zip_code, latitude, longitude, number_of_referrals, 
        offer, avg_monthly_long_distance_charges, avg_monthly_gb_download, 
        streaming_music, unlimited_data, total_refunds, total_extra_data_charges, 
        total_long_distance_charges, total_revenue
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING;
    """

    try:
        conn.cursor().executemany(query, data)
        conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()



def connect_rds():
    try:
        conn = pg8000.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
        )

        cur = conn.cursor()
        cur.execute("SELECT version();")
        print("Connected to:", cur.fetchone())

        create_table(conn)
        insert_data(conn)

        cur.execute("SELECT * FROM customers LIMIT 5;")
        rows = cur.fetchall()
        print("\nSample data from RDS:")
        for row in rows:
            print(row)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Failed to connect: {e}")


connect_rds()
