from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import numpy as np
from scipy import stats

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2016, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ecomm_analytics_pipeline',
    default_args=default_args,
    description='Ecommerce monthly analytics pipeline for Bayesian A/B testing',
    schedule='@monthly', 
    catchup=True
)

# Database connection function
def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        database='ecomm_cust_db',
        user='rubyc',
        password='tHvGUE8QbQWNNum4'
    )

# 1. Extract and Load all dimension tables 

def load_customers_dimension():
    """Extract customers from operational table and load into dimension"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Extract and transform customers
    extract_query = """
        SELECT DISTINCT
            c.customer_id,
            c.customer_unique_id,
            c.customer_zip_code_prefix,
            c.customer_city,
            c.customer_state,
            -- Calculate derived fields
            CASE 
                WHEN order_count = 1 THEN 'new'
                WHEN order_count BETWEEN 2 AND 5 THEN 'returning'
                WHEN total_spent > 1000 THEN 'vip'
                ELSE 'regular'
            END as customer_segment,
            first_order_date,
            last_order_date,
            COALESCE(order_count, 0) as total_orders, -- Returns the first non-null values 
            COALESCE(total_spent, 0) as lifetime_value
        FROM customers c
        LEFT JOIN (
            SELECT 
                customer_id,
                COUNT(*) as order_count,
                SUM(payment_value) as total_spent,
                MIN(order_purchase_timestamp::date) as first_order_date,
                MAX(order_purchase_timestamp::date) as last_order_date
            FROM orders o
            JOIN order_payments p ON o.order_id = p.order_id
            GROUP BY customer_id
        ) customer_stats ON c.customer_id = customer_stats.customer_id
    """
    
    # Load into dimension table
    insert_query = """
        INSERT INTO ecommerce_analytics.dim_customer 
        (customer_id, customer_unique_id, customer_zip_code_prefix, 
         customer_city, customer_state, customer_segment, first_order_date, 
         last_order_date, total_orders, lifetime_value)
        SELECT * FROM temp_customers
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_segment = EXCLUDED.customer_segment,
            total_orders = EXCLUDED.total_orders,
            lifetime_value = EXCLUDED.lifetime_value,
            last_order_date = EXCLUDED.last_order_date,
            updated_at = CURRENT_TIMESTAMP
    """
    
    cur.execute(f"CREATE TEMP TABLE temp_customers AS ({extract_query})")
    cur.execute(insert_query)
    conn.commit()
    
    cur.close()
    conn.close()
    print("Customers dimension loaded successfully")

def load_products_dimension():
    """Load products dimension with calculated fields"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO ecommerce_analytics.dim_product 
        (product_id, product_category_name, product_name_length, 
         product_description_length, product_photos_qty, product_weight_g,
         product_length_cm, product_height_cm, product_width_cm,
         product_volume_cm3, is_heavy_item, is_large_item)
        SELECT 
            product_id,
            product_category_name,
            product_name_lenght as product_name_length,
            product_description_lenght as product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            -- Calculated fields
            (product_length_cm * product_height_cm * product_width_cm) as product_volume_cm3,
            (product_weight_g > 1000) as is_heavy_item,
            ((product_length_cm * product_height_cm * product_width_cm) > 10000) as is_large_item
        FROM products_dataset
        ON CONFLICT (product_id) DO UPDATE SET
            product_category_name = EXCLUDED.product_category_name,
            product_volume_cm3 = EXCLUDED.product_volume_cm3,
            is_heavy_item = EXCLUDED.is_heavy_item,
            is_large_item = EXCLUDED.is_large_item,
            updated_at = CURRENT_TIMESTAMP
    """
    
    cur.execute(insert_query)
    conn.commit()
    
    cur.close()
    conn.close()
    print("Products dimension loaded successfully")

def load_sellers_dimension():
    """Load sellers dimension"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO ecommerce_analytics.dim_seller 
        (seller_id, seller_zip_code_prefix, seller_city, seller_state,
         seller_segment, first_sale_date, last_sale_date, total_sales, total_revenue)
        SELECT 
            s.seller_id,
            s.seller_zip_code_prefix,
            s.seller_city,
            s.seller_state,
            -- Calculate seller segment
            CASE 
                WHEN sales_count < 10 THEN 'small'
                WHEN sales_count BETWEEN 10 AND 100 THEN 'medium'
                ELSE 'large'
            END as seller_segment,
            first_sale_date,
            last_sale_date,
            COALESCE(sales_count, 0) as total_sales,
            COALESCE(total_revenue, 0) as total_revenue
        FROM sellers s
        LEFT JOIN (
            SELECT 
                seller_id,
                COUNT(*) as sales_count,
                SUM(price + freight_value) as total_revenue,
                MIN(o.order_purchase_timestamp::date) as first_sale_date,
                MAX(o.order_purchase_timestamp::date) as last_sale_date
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            GROUP BY seller_id
        ) seller_stats ON s.seller_id = seller_stats.seller_id
        ON CONFLICT (seller_id) DO UPDATE SET
            seller_segment = EXCLUDED.seller_segment,
            total_sales = EXCLUDED.total_sales,
            total_revenue = EXCLUDED.total_revenue,
            last_sale_date = EXCLUDED.last_sale_date,
            updated_at = CURRENT_TIMESTAMP
    """
    
    cur.execute(insert_query)
    conn.commit()
    
    cur.close()
    conn.close()
    print("Sellers dimension loaded successfully")

# 2. LOAD FACT TABLES

def load_sales_fact():
    """Load fact_sales table"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO ecommerce_analytics.fact_sales 
        (customer_key, product_key, seller_key, order_date_key, 
         order_id, order_item_id, price, freight_value, total_value,
         is_first_purchase, is_repeat_customer)
        SELECT 
            dc.customer_key,
            dp.product_key,
            ds.seller_key,
            TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INTEGER as order_date_key,
            oi.order_id,
            oi.order_item_id,
            oi.price,
            oi.freight_value,
            (oi.price + oi.freight_value) as total_value,
            (customer_order_rank = 1) as is_first_purchase,
            (customer_order_rank > 1) as is_repeat_customer
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
        JOIN ecommerce_analytics.dim_customer dc ON o.customer_id = dc.customer_id
        JOIN ecommerce_analytics.dim_product dp ON oi.product_id = dp.product_id
        JOIN ecommerce_analytics.dim_seller ds ON oi.seller_id = ds.seller_id
        LEFT JOIN (
            SELECT 
                customer_id,
                order_id,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) as customer_order_rank
            FROM orders
        ) customer_orders ON o.order_id = customer_orders.order_id
        WHERE DATE_TRUNC('month', o.order_purchase_timestamp) = DATE_TRUNC('month', '{{ ds }}'::date)
        ON CONFLICT DO NOTHING
    """
    
    cur.execute(insert_query)
    conn.commit()
    
    cur.close()
    conn.close()
    print("Sales fact table loaded successfully")

# 3. A/B TEST SETUP AND ASSIGNMENT

def setup_ab_test():
    """Set up A/B test for coupon promotions"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create the test if it doesn't exist
    cur.execute("""
        INSERT INTO ecommerce_analytics.dim_ab_test 
        (test_id, test_name, test_type, hypothesis, start_date, status, success_metric)
        VALUES 
        ('coupon_promo_v1', 'Coupon Promotion Test', 'promotion', 
         'Free shipping offer will drive higher conversion than percentage discount', 
         CURRENT_DATE, 'active', 'conversion_rate')
        ON CONFLICT (test_id) DO NOTHING
    """)
    
    # Create variants
    cur.execute("""
        INSERT INTO ecommerce_analytics.dim_test_variant 
        (test_key, variant_id, variant_name, description, is_control, traffic_allocation)
        SELECT 
            t.test_key, 'A', '10% Discount', '10% off your entire order', TRUE, 50.00
        FROM ecommerce_analytics.dim_ab_test t WHERE t.test_id = 'coupon_promo_v1'
        ON CONFLICT (test_key, variant_id) DO NOTHING;
        
        INSERT INTO ecommerce_analytics.dim_test_variant 
        (test_key, variant_id, variant_name, description, is_control, traffic_allocation)
        SELECT 
            t.test_key, 'B', 'Free Shipping + 5%', 'Free shipping plus 5% discount', FALSE, 50.00
        FROM ecommerce_analytics.dim_ab_test t WHERE t.test_id = 'coupon_promo_v1'
        ON CONFLICT (test_key, variant_id) DO NOTHING;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("A/B test setup completed")

def assign_customers_to_variants():
    """Assign customers to A/B test variants"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Assign customers to variants and log exposure events
    cur.execute("""
        INSERT INTO ecommerce_analytics.fact_ab_test_events 
        (customer_key, test_key, variant_key, event_date_key, 
         event_type, event_timestamp, is_conversion)
        SELECT 
            dc.customer_key,
            t.test_key,
            v.variant_key,
            TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER as event_date_key,
            'exposure' as event_type,
            CURRENT_TIMESTAMP as event_timestamp,
            FALSE as is_conversion
        FROM ecommerce_analytics.dim_customer dc
        CROSS JOIN ecommerce_analytics.dim_ab_test t
        JOIN ecommerce_analytics.dim_test_variant v ON t.test_key = v.test_key
        WHERE t.test_id = 'coupon_promo_v1'
        AND v.variant_id = CASE 
            WHEN MOD(ABS(HASHTEXT(dc.customer_id)), 2) = 0 THEN 'A'
            ELSE 'B'
        END
        AND NOT EXISTS (
            SELECT 1 FROM ecommerce_analytics.fact_ab_test_events e 
            WHERE e.customer_key = dc.customer_key 
            AND e.test_key = t.test_key 
            AND e.event_type = 'exposure'
        )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Customers assigned to A/B test variants")

# 4. BAYESIAN A/B TEST ANALYSIS

def bayesian_ab_analysis():
    """Perform Bayesian analysis of A/B test results"""
    conn = get_db_connection()
    
    # Get A/B test results
    query = """
        SELECT 
            v.variant_id,
            v.variant_name,
            v.is_control,
            COUNT(DISTINCT e.customer_key) as users,
            SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END) as conversions
        FROM ecommerce_analytics.fact_ab_test_events e
        JOIN ecommerce_analytics.dim_ab_test t ON e.test_key = t.test_key
        JOIN ecommerce_analytics.dim_test_variant v ON e.variant_key = v.variant_key
        WHERE t.test_id = 'coupon_promo_v1'
        GROUP BY v.variant_id, v.variant_name, v.is_control
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if len(df) < 2:
        print("Not enough data for A/B test analysis")
        return
    
    # Bayesian analysis using Beta-Binomial model
    results = {}
    
    for _, row in df.iterrows():
        variant = row['variant_id']
        conversions = row['conversions']
        users = row['users']
        
        # Beta prior (uninformative: alpha=1, beta=1)
        alpha_prior = 1
        beta_prior = 1
        
        # Posterior parameters
        alpha_posterior = alpha_prior + conversions
        beta_posterior = beta_prior + users - conversions
        
        # Generate samples from posterior
        samples = np.random.beta(alpha_posterior, beta_posterior, 10000)
        
        results[variant] = {
            'conversion_rate': conversions / users if users > 0 else 0,
            'credible_interval_95': np.percentile(samples, [2.5, 97.5]),
            'posterior_mean': alpha_posterior / (alpha_posterior + beta_posterior),
            'samples': samples
        }
    
    # Compare variants
    if 'A' in results and 'B' in results:
        # Probability that B > A
        prob_b_better = np.mean(results['B']['samples'] > results['A']['samples'])
        
        print(f"Bayesian A/B Test Results:")
        print(f"Variant A: {results['A']['conversion_rate']:.3f} conversion rate")
        print(f"Variant B: {results['B']['conversion_rate']:.3f} conversion rate")
        print(f"Probability that B > A: {prob_b_better:.3f}")
        
        if prob_b_better > 0.95:
            print("Strong evidence that variant B is better")
        elif prob_b_better < 0.05:
            print("Strong evidence that variant A is better")
        else:
            print("No conclusive evidence of difference")

# Define tasks
load_customers_task = PythonOperator(
    task_id='load_customers_dimension',
    python_callable=load_customers_dimension,
    dag=dag
)

load_products_task = PythonOperator(
    task_id='load_products_dimension',
    python_callable=load_products_dimension,
    dag=dag
)

load_sellers_task = PythonOperator(
    task_id='load_sellers_dimension',
    python_callable=load_sellers_dimension,
    dag=dag
)

load_sales_task = PythonOperator(
    task_id='load_sales_fact',
    python_callable=load_sales_fact,
    dag=dag
)

setup_ab_test_task = PythonOperator(
    task_id='setup_ab_test',
    python_callable=setup_ab_test,
    dag=dag
)

assign_variants_task = PythonOperator(
    task_id='assign_customers_to_variants',
    python_callable=assign_customers_to_variants,
    dag=dag
)

bayesian_analysis_task = PythonOperator(
    task_id='bayesian_ab_analysis',
    python_callable=bayesian_ab_analysis,
    dag=dag
)

# Define task dependencies
[load_customers_task, load_products_task, load_sellers_task] >> load_sales_task
load_customers_task >> setup_ab_test_task >> assign_variants_task >> bayesian_analysis_task