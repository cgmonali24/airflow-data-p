
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def store_in_db(POSTGRES_CONNECTION_ID,**kwargs):
    """
    Store the cleaned data in the Postgres database

    Args:
        POSTGRES_CONNECTION_ID (str): Postgres connection ID
    """
    
    cleaned_data = kwargs['ti'].xcom_pull(task_ids='clean_data', key='cleaned_data')  
    
    df = pd.DataFrame(cleaned_data)
    
   
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_data (
            customer_id VARCHAR(255) PRIMARY KEY,
            customer_name VARCHAR(255),
            phone_number VARCHAR(255),
            products_purchased TEXT,
            discount_code VARCHAR(50),
            date DATE,
            amount DECIMAL(10, 2)
        )
        """
    )
    cursor.executemany(
        """
        INSERT INTO customer_data (customer_id, customer_name, phone_number, products_purchased, discount_code, date, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            phone_number = EXCLUDED.phone_number,
            products_purchased = EXCLUDED.products_purchased,
            discount_code = EXCLUDED.discount_code,
            date = EXCLUDED.date,
            amount = EXCLUDED.amount
        """,
        df.values.tolist()
    )
    conn.commit()
    cursor.close()
    conn.close()
