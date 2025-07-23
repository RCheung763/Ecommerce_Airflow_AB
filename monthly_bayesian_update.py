#!/usr/bin/env python3
"""
Monthly Bayesian Update
Efficient incremental Bayesian updating for monthly DAG runs
Uses stored priors + new monthly data to calculate posteriors
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import logging
from calendar import monthrange

# Import our prior management functions
from initial_priors import (
    get_stored_prior, 
    save_monthly_posterior, 
    copy_posterior_as_next_prior
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        database='ecomm_cust_db',
        user='rubyc',
        password='tHvGUE8QbQWNNum4'
    )

def get_monthly_ab_data(test_id, year, month):
    """Get A/B test data for a specific month"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Calculate month boundaries
        month_start = date(year, month, 1)
        _, last_day = monthrange(year, month)
        month_end = date(year, month, last_day)
        
        logger.info(f"Getting A/B test data for {year}-{month:02d} ({month_start} to {month_end})")
        
        cur.execute("""
            SELECT 
                v.variant_id,
                v.variant_name,
                COUNT(DISTINCT e.customer_key) as monthly_users,
                SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END) as monthly_conversions
            FROM ecommerce_analytics.fact_ab_test_events e
            JOIN ecommerce_analytics.dim_ab_test t ON e.test_key = t.test_key
            JOIN ecommerce_analytics.dim_test_variant v ON e.variant_key = v.variant_key
            WHERE t.test_id = %s 
            AND e.event_type = 'exposure'
            AND TO_DATE(e.event_date_key::text, 'YYYYMMDD') >= %s
            AND TO_DATE(e.event_date_key::text, 'YYYYMMDD') <= %s
            GROUP BY v.variant_id, v.variant_name
            ORDER BY v.variant_id
        """, (test_id, month_start, month_end))
        
        monthly_data = cur.fetchall()
        
        logger.info(f"Found data for {len(monthly_data)} variants in {year}-{month:02d}")
        
        return monthly_data, month_start
        
    except Exception as e:
        logger.error(f"Error getting monthly data: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def get_latest_full_month_data(test_id):
    """Get A/B test data for the latest complete month available"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Find the latest full month with A/B test data
        cur.execute("""
            SELECT 
                TO_CHAR(TO_DATE(event_date_key::text, 'YYYYMMDD'), 'YYYY-MM') as month,
                COUNT(*) as events,
                MIN(TO_DATE(event_date_key::text, 'YYYYMMDD')) as first_date,
                MAX(TO_DATE(event_date_key::text, 'YYYYMMDD')) as last_date
            FROM ecommerce_analytics.fact_ab_test_events e
            JOIN ecommerce_analytics.dim_ab_test t ON e.test_key = t.test_key
            WHERE t.test_id = %s AND e.event_type = 'exposure'
            GROUP BY month
            ORDER BY month DESC
            LIMIT 1
        """, (test_id,))
        
        latest_month_info = cur.fetchone()
        
        if not latest_month_info:
            logger.error(f"No A/B test data found for {test_id}")
            return None, None, None
        
        month_str, event_count, first_date, last_date = latest_month_info
        year, month = int(month_str[:4]), int(month_str[5:7])
        
        logger.info(f"Found latest month: {year}-{month:02d} with {event_count:,} events")
        logger.info(f"   Date range: {first_date} to {last_date}")
        
        # Get the actual A/B test data for this month
        cur.execute("""
            SELECT 
                v.variant_id,
                v.variant_name,
                COUNT(DISTINCT e.customer_key) as monthly_users,
                SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END) as monthly_conversions
            FROM ecommerce_analytics.fact_ab_test_events e
            JOIN ecommerce_analytics.dim_ab_test t ON e.test_key = t.test_key
            JOIN ecommerce_analytics.dim_test_variant v ON e.variant_key = v.variant_key
            WHERE t.test_id = %s 
            AND e.event_type = 'exposure'
            AND TO_CHAR(TO_DATE(e.event_date_key::text, 'YYYYMMDD'), 'YYYY-MM') = %s
            GROUP BY v.variant_id, v.variant_name
            ORDER BY v.variant_id
        """, (test_id, month_str))
        
        monthly_data = cur.fetchall()
        period_date = date(year, month, 1)
        
        logger.info(f"Found data for {len(monthly_data)} variants in {year}-{month:02d}")
        
        return monthly_data, period_date, (year, month)
        
    except Exception as e:
        logger.error(f"Error getting latest month data: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def calculate_monthly_posterior(test_id, variant_id, period_date, monthly_users, monthly_conversions):
    """Calculate posterior using stored prior + new monthly data"""
    
    try:
        # Get the stored prior for this period
        prior = get_stored_prior(test_id, variant_id, period_date, 'prior')
        
        if not prior:
            logger.warning(f"No prior found for {variant_id} on {period_date}, using uninformative")
            alpha_prior, beta_prior = 1.0, 1.0
            prior_method = "uninformative_fallback"
        else:
            alpha_prior = prior['alpha']
            beta_prior = prior['beta']
            prior_method = prior['method']
        
        # Bayesian update: Prior + Data = Posterior
        alpha_posterior = alpha_prior + monthly_conversions
        beta_posterior = beta_prior + (monthly_users - monthly_conversions)
        
        # Calculate some useful statistics
        prior_mean = alpha_prior / (alpha_prior + beta_prior)
        posterior_mean = alpha_posterior / (alpha_posterior + beta_posterior)
        monthly_rate = monthly_conversions / monthly_users if monthly_users > 0 else 0
        
        result = {
            'variant_id': variant_id,
            'alpha_prior': alpha_prior,
            'beta_prior': beta_prior,
            'monthly_users': monthly_users,
            'monthly_conversions': monthly_conversions,
            'alpha_posterior': alpha_posterior,
            'beta_posterior': beta_posterior,
            'prior_mean': prior_mean,
            'posterior_mean': posterior_mean,
            'monthly_rate': monthly_rate,
            'prior_method': prior_method,
            'belief_update': posterior_mean - prior_mean
        }
        
        logger.info(f"   {variant_id}: Prior μ={prior_mean:.3f} + Data {monthly_conversions}/{monthly_users} ({monthly_rate:.3f}) → Posterior μ={posterior_mean:.3f}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating posterior for {variant_id}: {e}")
        raise

def monthly_bayesian_update(test_id, use_latest_month=True, year=None, month=None, force_reprocess=False):
    """
    Complete monthly Bayesian update process
    
    Args:
        test_id: A/B test identifier
        use_latest_month: If True, automatically finds latest complete month
        year, month: Specific month to process (ignored if use_latest_month=True)
        force_reprocess: If True, reprocess even if already done
    """
    
    if use_latest_month:
        logger.info(f"Starting Bayesian update for latest available month: {test_id}")
        
        # Get the latest full month of data
        monthly_data, period_date, (year, month) = get_latest_full_month_data(test_id)
        
        if not monthly_data:
            logger.warning(f"No A/B test data found for {test_id}")
            return None
    else:
        logger.info(f"Starting monthly Bayesian update: {test_id} {year}-{month:02d}")
        
        # Use the original specific month logic 
        monthly_data, period_date = get_monthly_ab_data(test_id, year, month)
        
        if not monthly_data:
            logger.warning(f"No A/B test data found for {year}-{month:02d}")
            return None
    
    try:
        print(f"\n{'='*70}")
        print(f"MONTHLY BAYESIAN UPDATE - {test_id}")
        print(f"Period: {year}-{month:02d}")
        print(f"{'='*70}")
        
        results = {}
        
        # 2. For each variant, calculate posterior using stored prior + new data
        for variant_id, variant_name, monthly_users, monthly_conversions in monthly_data:
            
            print(f"\nVariant {variant_id} ({variant_name}):")
            print(f"   Monthly Data: {monthly_conversions:,} conversions from {monthly_users:,} users")
            
            # Calculate posterior
            result = calculate_monthly_posterior(
                test_id, variant_id, period_date, monthly_users, monthly_conversions
            )
            
            results[variant_id] = result
            
            print(f"   Prior: α={result['alpha_prior']:.2f}, β={result['beta_prior']:.2f} (μ={result['prior_mean']:.3f})")
            print(f"   Posterior: α={result['alpha_posterior']:.2f}, β={result['beta_posterior']:.2f} (μ={result['posterior_mean']:.3f})")
            print(f"   Belief Update: {result['belief_update']:+.3f}")
            
            # 3. Save the posterior
            save_monthly_posterior(
                test_id, variant_id, period_date,
                result['alpha_posterior'], result['beta_posterior'],
                monthly_users, monthly_conversions,
                f"Monthly update for {year}-{month:02d}: {monthly_conversions}/{monthly_users} conversions"
            )
        
        # 4. Statistical comparison between variants
        print(f"\nVARIANT COMPARISON:")
        print("-" * 40)
        
        if len(results) >= 2:
            variants = list(results.keys())
            
            for i in range(len(variants)):
                for j in range(i + 1, len(variants)):
                    var_a, var_b = variants[i], variants[j]
                    
                    # Generate samples for comparison
                    samples_a = np.random.beta(results[var_a]['alpha_posterior'], 
                                             results[var_a]['beta_posterior'], 10000)
                    samples_b = np.random.beta(results[var_b]['alpha_posterior'], 
                                             results[var_b]['beta_posterior'], 10000)
                    
                    prob_b_better = np.mean(samples_b > samples_a)
                    
                    # Effect size
                    if results[var_a]['posterior_mean'] > 0:
                        lift = ((results[var_b]['posterior_mean'] - results[var_a]['posterior_mean']) / 
                               results[var_a]['posterior_mean']) * 100
                    else:
                        lift = 0
                    
                    print(f"P({var_b} > {var_a}): {prob_b_better:.3f}")
                    print(f"Expected Lift: {lift:+.1f}%")
        
        # 5. Set up next month's priors 
        try:
            next_month = month + 1 if month < 12 else 1
            next_year = year if month < 12 else year + 1
            next_period_date = date(next_year, next_month, 1)
            
            print(f"\nSetting up priors for {next_year}-{next_month:02d}:")
            
            for variant_id in results.keys():
                success = copy_posterior_as_next_prior(
                    test_id, variant_id, period_date, next_period_date
                )
                if success:
                    print(f"   {variant_id}: Posterior → Next month's prior")
        except Exception as e:
            logger.warning(f"Could not set up next month's priors: {e}")
        
        print(f"\nMonthly Bayesian update completed for {year}-{month:02d}")
        
        return results
        
    except Exception as e:
        logger.error(f"Monthly Bayesian update failed: {e}")
        raise

def bayesian_analysis_report(test_id, year, month):
    """Generate a detailed Bayesian analysis report for a specific month"""
    
    try:
        period_date = date(year, month, 1)
        
        # Get stored posteriors for this month
        conn = get_db_connection()
        
        query = """
            SELECT 
                variant_id,
                alpha_value as alpha_posterior,
                beta_value as beta_posterior,
                data_points,
                conversions
            FROM ecommerce_analytics.bayesian_priors_store
            WHERE test_id = %s AND period_date = %s AND value_type = 'posterior'
            ORDER BY variant_id
        """
        
        df = pd.read_sql(query, conn, params=[test_id, period_date])
        conn.close()
        
        if df.empty:
            print(f"No posterior data found for {test_id} {year}-{month:02d}")
            return None
        
        print(f"\n{'='*70}")
        print(f"BAYESIAN ANALYSIS REPORT - {test_id}")
        print(f"Period: {year}-{month:02d}")
        print(f"{'='*70}")
        
        # Generate detailed analysis
        for _, row in df.iterrows():
            variant_id = row['variant_id']
            alpha = row['alpha_posterior']
            beta = row['beta_posterior']
            
            # Generate samples from posterior
            samples = np.random.beta(alpha, beta, 10000)
            
            # Calculate statistics
            mean = alpha / (alpha + beta)
            variance = (alpha * beta) / ((alpha + beta)**2 * (alpha + beta + 1))
            std = np.sqrt(variance)
            
            # Credible intervals
            ci_95 = np.percentile(samples, [2.5, 97.5])
            ci_90 = np.percentile(samples, [5, 95])
            
            print(f"\n🔸 Variant {variant_id}:")
            print(f"   Posterior: α={alpha:.2f}, β={beta:.2f}")
            print(f"   Mean: {mean:.3f} ({mean*100:.1f}%)")
            print(f"   Std: {std:.3f}")
            print(f"   95% CI: [{ci_95[0]:.3f}, {ci_95[1]:.3f}]")
            print(f"   90% CI: [{ci_90[0]:.3f}, {ci_90[1]:.3f}]")
            print(f"   Monthly Data: {row['conversions']}/{row['data_points']} conversions")
        
        return df
        
    except Exception as e:
        logger.error(f"Error generating analysis report: {e}")
        raise

# Function for use in Airflow DAG
def airflow_monthly_bayesian_task(**context):
    """
    Airflow wrapper function
    """
    test_id = 'coupon_promo_v1'  # Or get from DAG params
    
    # Check if we should force reprocessing 
    force_reprocess = context.get('params', {}).get('force_reprocess', False)
    
    # Run the monthly update for the latest available month
    results = monthly_bayesian_update(test_id, use_latest_month=True, force_reprocess=force_reprocess)
    
    # Generate analysis report if we have results
    if results:
        # Extract year and month from the period_date in results
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            # Get the most recent period processed
            cur.execute("""
                SELECT 
                    EXTRACT(YEAR FROM period_date) as year,
                    EXTRACT(MONTH FROM period_date) as month
                FROM ecommerce_analytics.bayesian_priors_store
                WHERE test_id = %s AND value_type = 'posterior'
                ORDER BY period_date DESC
                LIMIT 1
            """, (test_id,))
            
            latest_period = cur.fetchone()
            if latest_period:
                year, month = int(latest_period[0]), int(latest_period[1])
                bayesian_analysis_report(test_id, year, month)
        
        except Exception as e:
            logger.warning(f"Could not generate analysis report: {e}")
        finally:
            cur.close()
            conn.close()
    
    return results

def main():
    """Demo function"""
    test_id = 'coupon_promo_v1'
    
    print("Running demo with latest available month...")
    
    # Process the latest available month
    results = monthly_bayesian_update(test_id, use_latest_month=True)
    
    if results:
        print("\nLatest month processed successfully!")
        
        # Also show example of processing specific month
        print("\n" + "="*50)
        print("You can also process specific months:")
        print("monthly_bayesian_update('coupon_promo_v1', use_latest_month=False, year=2018, month=10)")
    else:
        print("No data found to process")

if __name__ == "__main__":
    main()