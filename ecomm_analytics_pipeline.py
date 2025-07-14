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

# Customer dimension 

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

# Products dimension 

def load_products_dimension():
    """Load products dimension with calculated fields"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO ecommerce_analytics.dim_product 
        (product_id, product_category_name, product_name_length, 
         product_description_length, product_photos_qty, product_weight_g,
         product_length_cm, product_height_cm, product_width_cm,
         is_heavy_item, is_large_item)
        SELECT 
            product_id,
            product_category_name,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm,
            -- Calculated fields
            (product_weight_g > 1000) as is_heavy_item,
            ((product_length_cm * product_height_cm * product_width_cm) > 10000) as is_large_item
        FROM products
        ON CONFLICT (product_id) DO UPDATE SET
            product_category_name = EXCLUDED.product_category_name,
            is_heavy_item = EXCLUDED.is_heavy_item,
            is_large_item = EXCLUDED.is_large_item,
            updated_at = CURRENT_TIMESTAMP
    """
    
    cur.execute(insert_query)
    conn.commit()
    
    cur.close()
    conn.close()
    print("Products dimension loaded successfully")

# Sellers dimension 

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

# 2. Load facts table 

# Sales Facts

def load_sales_fact(**context):
    """Load fact_sales table"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get execution date from context
    execution_date = context['ds']
    print(f"DEBUG: Execution date is: {execution_date}")
    
    # Check what date range your data covers
    cur.execute("SELECT MIN(order_purchase_timestamp), MAX(order_purchase_timestamp) FROM orders")
    date_range = cur.fetchone()
    print(f"DEBUG: Orders date range: {date_range[0]} to {date_range[1]}")
    
    insert_query = """
        INSERT INTO ecommerce_analytics.fact_sales 
        (customer_key, product_key, seller_key, order_date_key, 
         order_id, order_item_id, price, freight_value, total_value,
         is_first_purchase, is_repeat_customer)
        SELECT 
            dc.customer_key,
            dp.product_key,
            ds.seller_key,
            TO_CHAR(o.order_purchase_timestamp::timestamp, 'YYYYMMDD')::INTEGER as order_date_key,
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
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp::timestamp) as customer_order_rank
            FROM orders
        ) customer_orders ON o.order_id = customer_orders.order_id
        -- Temporarily removed date filter
        ON CONFLICT DO NOTHING
    """
    
    cur.execute(insert_query)
    conn.commit()
    
    cur.close()
    conn.close()
    print("Sales fact table loaded successfully")

# Review facts 
 
def load_reviews_fact():
    """Load fact_reviews table"""
    conn = get_db_connection()
    cur = conn.cursor()

    insert_query = """
        INSERT INTO ecommerce_analytics.fact_reviews
        (customer_key, product_key, seller_key, review_date_key, 
        review_id, order_id, review_score, review_comment_title,
        review_comment_message, has_comment, comment_length)
        SELECT 
            dc.customer_key,
            dp.product_key,
            ds.seller_key,
            TO_CHAR(r.review_creation_date::timestamp, 'YYYYMMDD')::INTEGER as review_date_key,
            r.review_id,
            r.order_id,
            r.review_score,
            r.review_comment_title,
            r.review_comment_message,
            (r.review_comment_message IS NOT NULL AND LENGTH(r.review_comment_message) > 0) as has_comment,
            COALESCE(LENGTH(r.review_comment_message), 0) as comment_length
        FROM order_reviews r
        JOIN orders o ON r.order_id = o.order_id
        JOIN ecommerce_analytics.dim_customer dc ON o.customer_id = dc.customer_id
        LEFT JOIN order_items oi ON r.order_id = oi.order_id
        LEFT JOIN ecommerce_analytics.dim_product dp ON oi.product_id = dp.product_id
        LEFT JOIN ecommerce_analytics.dim_seller ds ON oi.seller_id = ds.seller_id
        ON CONFLICT DO NOTHING
            
"""

    cur.execute(insert_query)
    conn.commit()
    
    cur.close()
    conn.close()
    print("Reviews fact table loaded successfully")

# Orders facts 

def load_orders_fact():
    """Load fact_orders table"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO ecommerce_analytics.fact_orders 
        (customer_key, purchase_date_key, order_id, total_order_value, 
         total_freight_value, item_count)
        SELECT 
            dc.customer_key,
            TO_CHAR(o.order_purchase_timestamp::timestamp, 'YYYYMMDD')::INTEGER as purchase_date_key,
            o.order_id,
            COALESCE(order_totals.total_value, 0) as total_order_value,
            COALESCE(order_totals.total_freight, 0) as total_freight_value,
            COALESCE(order_totals.item_count, 0) as item_count
        FROM orders o
        JOIN ecommerce_analytics.dim_customer dc ON o.customer_id = dc.customer_id
        LEFT JOIN (
            SELECT 
                order_id,
                SUM(price + freight_value) as total_value,
                SUM(freight_value) as total_freight,
                COUNT(*) as item_count
            FROM order_items
            GROUP BY order_id
        ) order_totals ON o.order_id = order_totals.order_id
        ON CONFLICT (order_id) DO NOTHING
    """
    
    cur.execute(insert_query)
    conn.commit()
    cur.close()
    conn.close()
    print("Orders fact table loaded successfully")

# Payments facts
def load_payments_fact():
    """Load fact_payments table"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO ecommerce_analytics.fact_payments 
        (customer_key, payment_date_key, order_id, payment_sequential,
         payment_type, payment_installments, payment_value,
         is_installment_payment, is_credit_card)
        SELECT 
            dc.customer_key,
            TO_CHAR(o.order_purchase_timestamp::timestamp, 'YYYYMMDD')::INTEGER as payment_date_key,
            p.order_id,
            p.payment_sequential,
            p.payment_type,
            p.payment_installments,
            p.payment_value,
            (p.payment_installments > 1) as is_installment_payment,
            (p.payment_type = 'credit_card') as is_credit_card
        FROM order_payments p
        JOIN orders o ON p.order_id = o.order_id
        JOIN ecommerce_analytics.dim_customer dc ON o.customer_id = dc.customer_id
        ON CONFLICT DO NOTHING
    """
    
    cur.execute(insert_query)
    conn.commit()
    cur.close()
    conn.close()
    print("Payments fact table loaded successfully")


# 3. A/B TEST SETUP AND ASSIGNMENT

def setup_ab_test():
    """Set up A/B test for coupon promotions"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create the test
    cur.execute("""
        INSERT INTO ecommerce_analytics.dim_ab_test 
        (test_id, test_name, test_type, hypothesis, start_date, status, success_metric)
        VALUES 
        ('coupon_promo_v1', 'Coupon Promotion Test', 'promotion', 
         'Free shipping offer will drive higher order values than percentage discount', 
         CURRENT_DATE, 'active', 'high_value_conversion')
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
    """Assign customers to A/B test variants and track conversions"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Define conversion as 20%  higher than average order, using a overall average across time
    # Get average order value
    cur.execute("""
        SELECT AVG(total_value) as avg_order_value 
        FROM ecommerce_analytics.fact_sales
    """)
    result = cur.fetchone()
    if result[0] is None:
            print("No sales data found for calculating average order value")
            return
            
    avg_order_value = float(result[0])  
    high_value_threshold = avg_order_value * 1.2 
        
    print(f"Average order value: ${avg_order_value:.2f}")
    print(f"High-value threshold: ${high_value_threshold:.2f}")
    
    # Assign customers to variants and log exposure events
    cur.execute("""
        INSERT INTO ecommerce_analytics.fact_ab_test_events 
        (customer_key, test_key, variant_key, event_date_key, 
         event_type, event_timestamp, is_conversion, conversion_value)
        SELECT 
            dc.customer_key,
            t.test_key,
            v.variant_key,
            TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER as event_date_key,
            'exposure' as event_type,
            CURRENT_TIMESTAMP as event_timestamp,
            -- Conversion logic: high-value orders OR repeat customers
            CASE 
                WHEN fs.total_value > %s OR dc.total_orders > 1 THEN TRUE
                ELSE FALSE
            END as is_conversion,
            COALESCE(fs.total_value, 0) as conversion_value
        FROM ecommerce_analytics.dim_customer dc
        CROSS JOIN ecommerce_analytics.dim_ab_test t
        JOIN ecommerce_analytics.dim_test_variant v ON t.test_key = v.test_key
        LEFT JOIN ecommerce_analytics.fact_sales fs ON dc.customer_key = fs.customer_key
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
    """ % high_value_threshold)
    
    # Get conversion stats for each variant
    cur.execute("""
        SELECT 
            v.variant_name,
            COUNT(*) as total_exposures,
            SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END) as conversions,
            ROUND(SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END)::DECIMAL / COUNT(*) * 100, 2) as conversion_rate
        FROM ecommerce_analytics.fact_ab_test_events e
        JOIN ecommerce_analytics.dim_test_variant v ON e.variant_key = v.variant_key
        WHERE e.event_type = 'exposure'
        GROUP BY v.variant_name
        ORDER BY v.variant_name
    """)
    
    results = cur.fetchall()
    print("\nA/B Test Assignment Results:")
    for variant, exposures, conversions, rate in results:
        print(f"{variant}: {exposures:,} exposures, {conversions:,} conversions ({rate}%)")
    
    conn.commit()
    cur.close()
    conn.close()
    print("\nCustomers assigned to A/B test variants")

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
            SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END) as conversions,
            ROUND(AVG(e.conversion_value), 2) as avg_conversion_value
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
    
    print("="*60)
    print("BAYESIAN A/B TEST ANALYSIS")
    print("="*60)
    print("Conversion Definition: High-value orders (20% above average) OR repeat customers")
    print()
    
    # Bayesian analysis using Beta-Binomial model
    results = {}
    
    for _, row in df.iterrows():
        variant = row['variant_id']
        conversions = row['conversions']
        users = row['users']
        avg_value = row['avg_conversion_value']
        
        # Beta prior (uninformative: alpha=1, beta=1)
        alpha_prior = 1
        beta_prior = 1
        
        # Posterior parameters
        alpha_posterior = alpha_prior + conversions
        beta_posterior = beta_prior + users - conversions
        
        # Generate samples from posterior
        samples = np.random.beta(alpha_posterior, beta_posterior, 10000)
        
        conversion_rate = conversions / users if users > 0 else 0
        
        results[variant] = {
            'variant_name': row['variant_name'],
            'users': users,
            'conversions': conversions,
            'conversion_rate': conversion_rate,
            'avg_conversion_value': avg_value,
            'credible_interval_95': np.percentile(samples, [2.5, 97.5]),
            'posterior_mean': alpha_posterior / (alpha_posterior + beta_posterior),
            'samples': samples
        }
        
        print(f"Variant {variant} ({row['variant_name']}):")
        print(f"  Users: {users:,}")
        print(f"  High-Value Conversions: {conversions:,}")
        print(f"  Conversion Rate: {conversion_rate:.3f} ({conversion_rate*100:.1f}%)")
        print(f"  Avg Conversion Value: ${avg_value:.2f}")
        print(f"  95% Credible Interval: [{samples.mean():.3f} ± {samples.std()*1.96:.3f}]")
        print()
    
    # Compare variants
    if 'A' in results and 'B' in results:
        # Probability that B > A
        prob_b_better = np.mean(results['B']['samples'] > results['A']['samples'])
        
        # Calculate effect size (relative lift)
        if results['A']['conversion_rate'] > 0:
            relative_lift = ((results['B']['conversion_rate'] - results['A']['conversion_rate']) / 
                           results['A']['conversion_rate']) * 100
        else:
            relative_lift = 0
        
        # Value difference
        value_diff = results['B']['avg_conversion_value'] - results['A']['avg_conversion_value']
        
        print("COMPARISON RESULTS:")
        print("-" * 30)
        print(f"Probability that B > A: {prob_b_better:.3f}")
        print(f"Relative Lift (B vs A): {relative_lift:+.1f}%")
        print(f"Average Value Difference: ${value_diff:+.2f}")
        print()
        
        # Business interpretation
        print("BUSINESS IMPACT:")
        print("-" * 30)
        total_b_users = results['B']['users']
        if relative_lift > 0:
            additional_conversions = (results['B']['conversion_rate'] - results['A']['conversion_rate']) * total_b_users
            revenue_impact = additional_conversions * results['B']['avg_conversion_value']
            print(f"Additional high-value customers: {additional_conversions:.0f}")
            print(f"Estimated revenue impact: ${revenue_impact:,.2f}")
        print()
        
        # Results
        print("STATISTICAL CONCLUSION:")
        print("-" * 30)
        if prob_b_better > 0.95:
            print("Strong evidence that Free Shipping + 5% drives more high-value orders")
        elif prob_b_better < 0.05:
            print("Strong evidence that 10% Discount drives more high-value orders")
        elif prob_b_better > 0.9:
            print("Moderate evidence favoring Free Shipping + 5%")
        elif prob_b_better < 0.1:
            print("Moderate evidence favoring 10% Discount")
        else:
            print("❓ Inconclusive - continue testing or collect more data")
        
        print()
        print("RECOMMENDATION:")
        print("-" * 30)
        if prob_b_better > 0.9:
            print("Implement Free Shipping + 5% promotion")
            print("   Expected to increase high-value customer rate")
        elif prob_b_better < 0.1:
            print("Stick with 10% Discount promotion")
            print("   Proven to drive better high-value customer behavior")
        else:
            print("Continue testing - need more data for confident decision")
            print("   Consider longer test period or larger sample size")

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

load_orders_task = PythonOperator(
    task_id='load_orders_fact',
    python_callable=load_orders_fact,
    dag=dag
)

load_payments_task = PythonOperator(
    task_id='load_payments_fact', 
    python_callable=load_payments_fact, 
    dag=dag
)

load_reviews_task = PythonOperator(
    task_id='load_reviews_fact',
    python_callable=load_reviews_fact,
    dag=dag
)

# Define task dependencies
[load_customers_task, load_products_task, load_sellers_task] >> load_sales_task
[load_customers_task, load_products_task, load_sellers_task] >> load_orders_task
[load_customers_task, load_products_task, load_sellers_task] >> load_payments_task  
[load_customers_task, load_products_task, load_sellers_task] >> load_reviews_task
load_customers_task >> setup_ab_test_task >> assign_variants_task >> bayesian_analysis_task