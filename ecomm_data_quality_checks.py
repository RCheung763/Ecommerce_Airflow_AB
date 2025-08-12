from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import warnings

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2016, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ecomm_data_quality_checks',
    default_args=default_args,
    description='Run data quality checks on e-commerce PostgreSQL data',
    schedule='@weekly',
    catchup=False
)

def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        database='ecomm_cust_db',
        user='rubyc', 
        password='*****'  
    )

def check_nulls_in_customers():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM customers 
        WHERE customer_id IS NULL OR customer_unique_id IS NULL
    """)
    result = cur.fetchone()
    if result[0] > 0:
        warnings.warn(f"{result[0]} nulls found in critical fields.")
    cur.close()
    conn.close()

def check_referential_integrity():
    conn = get_db_connection()
    cur = conn.cursor()

    # Find orders where the customer_id does not exist in customers table
    cur.execute("""
        SELECT COUNT(*) 
        FROM orders_dataset o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    
    result = cur.fetchone()

    if result[0] > 0:
        raise ValueError(f"Referential integrity check failed: {result[0]} orders have invalid customer_id references.")

    cur.close()
    conn.close()

def validate_order_date_ranges():
    conn = get_db_connection()
    cur = conn.cursor()

    # Check for invalid date logic
    cur.execute("""
        SELECT COUNT(*) FROM orders_dataset
        WHERE order_delivered_customer_date < order_purchase_timestamp
           OR order_purchase_timestamp < DATE '2016-01-01'
           OR order_delivered_customer_date > CURRENT_DATE
    """)
    result = cur.fetchone()
    if result[0] > 0:
        warnings.warn(f"Date range validation failed: {result[0]} records have invalid date ranges.")
    
    cur.close()
    conn.close()

# Define tasks
check_customers_task = PythonOperator(
    task_id='check_nulls_in_customers',
    python_callable=check_nulls_in_customers,
    dag=dag
)

check_referential_integrity_task = PythonOperator(
    task_id='check_referential_integrity',
    python_callable=check_referential_integrity,
    dag=dag
)

validate_dates_task = PythonOperator(
    task_id='validate_order_date_ranges',
    python_callable=validate_order_date_ranges,
    dag=dag
)

# Set task dependencies
[check_customers_task, check_referential_integrity_task, validate_dates_task]
