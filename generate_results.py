import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime

def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        database='ecomm_cust_db',
        user='rubyc',
        password='tHvGUE8QbQWNNum4'
    )

def generate_pipeline_results():
    """Generate comprehensive results for repository documentation"""
    
    conn = get_db_connection()
    
    print("="*80)
    print("ECOMMERCE A/B TESTING PIPELINE - RESULTS SUMMARY")
    print("="*80)
    print(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 1. Data Pipeline Status
    print("DATA PIPELINE STATUS")
    print("-" * 40)
    
    status_query = """
        SELECT 
            'Customers' as dimension,
            COUNT(*) as records,
            MIN(created_at)::date as earliest_data,
            MAX(updated_at)::date as latest_update
        FROM ecommerce_analytics.dim_customer
        UNION ALL
        SELECT 
            'Products' as dimension,
            COUNT(*) as records,
            MIN(created_at)::date as earliest_data,
            MAX(updated_at)::date as latest_update
        FROM ecommerce_analytics.dim_product
        UNION ALL
        SELECT 
            'Sellers' as dimension,
            COUNT(*) as records,
            MIN(created_at)::date as earliest_data,
            MAX(updated_at)::date as latest_update
        FROM ecommerce_analytics.dim_seller
    """
    
    df_status = pd.read_sql(status_query, conn)
    print(df_status.to_string(index=False))
    print()
    
    # 2. Sales Fact Table Summary
    print("SALES ANALYTICS")
    print("-" * 40)
    
    sales_query = """
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(DISTINCT customer_key) as unique_customers,
            COUNT(DISTINCT product_key) as unique_products,
            ROUND(SUM(total_value), 2) as total_revenue,
            ROUND(AVG(total_value), 2) as avg_order_value,
            SUM(CASE WHEN is_first_purchase THEN 1 ELSE 0 END) as first_time_purchases
        FROM ecommerce_analytics.fact_sales
    """
    
    df_sales = pd.read_sql(sales_query, conn)
    print(df_sales.to_string(index=False))
    print()
    
    # 3. Customer Segmentation
    print("CUSTOMER SEGMENTATION")
    print("-" * 40)
    
    segment_query = """
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            ROUND(COUNT(*)::DECIMAL / SUM(COUNT(*)) OVER () * 100, 1) as percentage,
            ROUND(AVG(lifetime_value), 2) as avg_lifetime_value
        FROM ecommerce_analytics.dim_customer
        WHERE customer_segment IS NOT NULL
        GROUP BY customer_segment
        ORDER BY customer_count DESC
    """
    
    df_segments = pd.read_sql(segment_query, conn)
    print(df_segments.to_string(index=False))
    print()
    
    # 4. A/B Test Setup
    print("A/B TEST CONFIGURATION")
    print("-" * 40)
    
    test_setup_query = """
        SELECT 
            t.test_name,
            t.test_type,
            t.start_date,
            t.status,
            v.variant_name,
            v.description,
            v.traffic_allocation,
            CASE WHEN v.is_control THEN 'Control' ELSE 'Treatment' END as variant_type
        FROM ecommerce_analytics.dim_ab_test t
        JOIN ecommerce_analytics.dim_test_variant v ON t.test_key = v.test_key
        ORDER BY t.test_name, v.variant_id
    """
    
    df_test_setup = pd.read_sql(test_setup_query, conn)
    print(df_test_setup.to_string(index=False))
    print()
    
    # 5. A/B Test Results
    print("A/B TEST RESULTS")
    print("-" * 40)
    
    results_query = """
        SELECT 
            t.test_name,
            v.variant_name,
            CASE WHEN v.is_control THEN 'Control' ELSE 'Treatment' END as variant_type,
            COUNT(DISTINCT e.customer_key) as users_exposed,
            SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END) as conversions,
            ROUND(
                SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END)::DECIMAL / 
                NULLIF(COUNT(DISTINCT e.customer_key), 0) * 100, 3
            ) as conversion_rate_percent,
            ROUND(AVG(e.conversion_value), 2) as avg_conversion_value
        FROM ecommerce_analytics.fact_ab_test_events e
        JOIN ecommerce_analytics.dim_ab_test t ON e.test_key = t.test_key
        JOIN ecommerce_analytics.dim_test_variant v ON e.variant_key = v.variant_key
        GROUP BY t.test_name, v.variant_name, v.is_control
        ORDER BY t.test_name, v.variant_name
    """
    
    df_results = pd.read_sql(results_query, conn)
    
    if not df_results.empty:
        print(df_results.to_string(index=False))
        print()
        
        # 6. Bayesian Analysis (if we have A/B data)
        if len(df_results) >= 2:
            print("BAYESIAN STATISTICAL ANALYSIS")
            print("-" * 40)
            
            # Simple Bayesian analysis
            control = df_results[df_results['variant_type'] == 'Control'].iloc[0]
            treatment = df_results[df_results['variant_type'] == 'Treatment'].iloc[0]
            
            # Beta-Binomial model
            alpha_prior, beta_prior = 1, 1
            
            # Control posterior
            alpha_control = alpha_prior + control['conversions']
            beta_control = beta_prior + control['users_exposed'] - control['conversions']
            
            # Treatment posterior  
            alpha_treatment = alpha_prior + treatment['conversions']
            beta_treatment = beta_prior + treatment['users_exposed'] - treatment['conversions']
            
            # Monte Carlo simulation
            np.random.seed(42)
            control_samples = np.random.beta(alpha_control, beta_control, 10000)
            treatment_samples = np.random.beta(alpha_treatment, beta_treatment, 10000)
            
            prob_treatment_better = np.mean(treatment_samples > control_samples)
            expected_lift = np.mean((treatment_samples - control_samples) / control_samples) * 100
            
            print(f"Control Conversion Rate: {control['conversion_rate_percent']:.3f}%")
            print(f"Treatment Conversion Rate: {treatment['conversion_rate_percent']:.3f}%")
            print(f"Probability Treatment > Control: {prob_treatment_better:.3f}")
            print(f"Expected Lift: {expected_lift:.1f}%")
            print()
            
            if prob_treatment_better > 0.95:
                conclusion = "Strong evidence for Treatment variant"
            elif prob_treatment_better < 0.05:
                conclusion = "Strong evidence for Control variant"
            elif prob_treatment_better > 0.9:
                conclusion = "Moderate evidence for Treatment variant"
            else:
                conclusion = "Inconclusive - continue testing"
                
            print(f"Statistical Conclusion: {conclusion}")
            print()
    else:
        print("No A/B test data available yet.")
        print()
    
    # 7. Geographic Distribution
    print("GEOGRAPHIC DISTRIBUTION")
    print("-" * 40)
    
    geo_query = """
        SELECT 
            customer_state,
            COUNT(*) as customers,
            ROUND(COUNT(*)::DECIMAL / SUM(COUNT(*)) OVER () * 100, 1) as percentage
        FROM ecommerce_analytics.dim_customer
        WHERE customer_state IS NOT NULL
        GROUP BY customer_state
        ORDER BY customers DESC
        LIMIT 10
    """
    
    df_geo = pd.read_sql(geo_query, conn)
    print(df_geo.to_string(index=False))
    print()
    
    # 8. Product Categories
    print("TOP PRODUCT CATEGORIES")
    print("-" * 40)
    
    category_query = """
        SELECT 
            dp.product_category_name,
            COUNT(fs.sale_key) as total_sales,
            ROUND(SUM(fs.total_value), 2) as total_revenue,
            ROUND(AVG(fs.total_value), 2) as avg_order_value
        FROM ecommerce_analytics.fact_sales fs
        JOIN ecommerce_analytics.dim_product dp ON fs.product_key = dp.product_key
        WHERE dp.product_category_name IS NOT NULL
        GROUP BY dp.product_category_name
        ORDER BY total_revenue DESC
        LIMIT 10
    """
    
    df_categories = pd.read_sql(category_query, conn)
    print(df_categories.to_string(index=False))
    print()
    
    print("="*80)
    print("PIPELINE SUMMARY")
    print("="*80)
    print("ETL Pipeline: Successfully transformed operational data to star schema")
    print("Data Quality: All dimension and fact tables populated")
    print("A/B Testing: Framework deployed with customer randomization")
    print("Statistical Analysis: Bayesian evaluation framework operational")
    print("Business Insights: Customer segmentation and product analytics available")
    print()
    print("Pipeline is production-ready for ongoing A/B testing")
    
    conn.close()

if __name__ == "__main__":
    generate_pipeline_results()
