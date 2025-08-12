#!/usr/bin/env python3
"""
E-commerce Monthly Tasks DAG
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# Get the parent directory 
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
utils_dir = os.path.join(parent_dir, 'utils')

if utils_dir not in sys.path:
    sys.path.append(utils_dir)

from monthly_bayesian_update import airflow_monthly_bayesian_task

# DAG Configuration
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 1),  
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'ecomm_monthly_tasks',
    default_args=default_args,
    description='Monthly e-commerce data processing and Bayesian A/B testing',
    schedule='@monthly',  
    catchup=True, 
    max_active_runs=1,  
    tags=['ecommerce', 'monthly', 'bayesian']
)

# Database connection
def get_db_connection():
    import psycopg2
    return psycopg2.connect(
        host='localhost',
        database='ecomm_cust_db',
        user='rubyc',
        password='****'
    )

# Task 1: Monthly Bayesian A/B Test Update
bayesian_update_task = PythonOperator(
    task_id='monthly_bayesian_update',
    python_callable=airflow_monthly_bayesian_task,
    dag=dag
)

# Task 2: Data Quality Check 
def monthly_data_quality_check(**context):
    """Basic data quality checks"""
    print("Running monthly data quality checks...")
    
    # Add your quality check logic here
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Check: Count A/B test events
        cur.execute("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT customer_key) as unique_customers
            FROM ecommerce_analytics.fact_ab_test_events
        """)
        
        result = cur.fetchone()
        total_events, unique_customers = result
        
        print(f"Total A/B test events: {total_events:,}")
        print(f"Unique customers: {unique_customers:,}")
        
        # Quality check
        if total_events == 0:
            raise ValueError("No A/B test events found!")
        
        return {
            'total_events': total_events,
            'unique_customers': unique_customers,
            'status': 'passed'
        }
        
    except Exception as e:
        print(f"Data quality check failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=monthly_data_quality_check,
    dag=dag
)

# Task 3: Generate Monthly Report 
def generate_monthly_summary(**context):
    """Generate monthly business summary"""
    execution_date = context['ds']
    year = int(execution_date[:4])
    month = int(execution_date[5:7])
    
    conn = get_db_connection()
    
    print(f"\n{'='*80}")
    print(f"Monthly BI Report")
    print(f"Period: {year}-{month:02d}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    try:
        # 1. Summary
        exec_query = """
            SELECT 
                COUNT(DISTINCT fs.order_id) as total_orders,
                COUNT(DISTINCT fs.customer_key) as unique_customers,
                COALESCE(SUM(fs.total_value), 0) as total_revenue,
                COALESCE(AVG(fs.total_value), 0) as avg_order_value
            FROM ecommerce_analytics.fact_sales fs
            WHERE fs.order_date_key >= %s AND fs.order_date_key < %s
        """
        
        exec_df = pd.read_sql(exec_query, conn, params=[
            int(f"{year}{month:02d}01"), 
            int(f"{year}{month:02d}31")
        ])
        
        if not exec_df.empty:
            metrics = exec_df.iloc[0]
            print(f"\n Summary:")
            print(f"Total Revenue: ${metrics['total_revenue'] or 0:,.2f}")
            print(f"Total Orders: {metrics['total_orders'] or 0:,}")
            print(f"Unique Customers: {metrics['unique_customers'] or 0:,}")
            print(f"Average Order Value: ${metrics['avg_order_value'] or 0:.2f}")
        else:
            metrics = {'total_revenue': 0, 'total_orders': 0}
        
        # 2. A/B test insights
        print(f"\n A/B test performance:")
        ab_query = """
            SELECT 
                v.variant_name,
                COALESCE(p.data_points, 0) as users,
                COALESCE(p.conversions, 0) as conversions,
                COALESCE(ROUND(p.conversions::decimal / NULLIF(p.data_points, 0) * 100, 2), 0) as conversion_rate
            FROM ecommerce_analytics.bayesian_priors_store p
            JOIN ecommerce_analytics.dim_test_variant v ON p.variant_id = v.variant_id
            JOIN ecommerce_analytics.dim_ab_test t ON v.test_key = t.test_key
            WHERE p.test_id = 'coupon_promo_v1' 
            AND p.value_type = 'posterior'
            ORDER BY p.period_date DESC, v.variant_id
            LIMIT 2
        """
        
        ab_df = pd.read_sql(ab_query, conn)
        if not ab_df.empty:
            for _, row in ab_df.iterrows():
                name = row['variant_name'] or 'Unknown'
                rate = row['conversion_rate'] or 0
                convs = row['conversions'] or 0
                users = row['users'] or 0
                print(f"{name}: {rate}% ({convs:,}/{users:,})")
        else:
            print("No A/B test data available")
        
        return {
            'report_period': f"{year}-{month:02d}",
            'total_revenue': float(metrics.get('total_revenue', 0) or 0),
            'total_orders': int(metrics.get('total_orders', 0) or 0),
            'report_status': 'completed'
        }
        
    except Exception as e:
        print(f"Error in report generation: {e}")
        return {'report_status': 'failed', 'error': str(e)}
    finally:
        conn.close()

monthly_report_task = PythonOperator(
    task_id='generate_monthly_report',
    python_callable=generate_monthly_summary,
    dag=dag
)

# Define Task Dependencies
# Quality check runs first, then Bayesian update, then report
quality_check_task >> bayesian_update_task >> monthly_report_task
