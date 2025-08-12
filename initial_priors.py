#!/usr/bin/env python3
"""
Initial Priors Calculator
One-time script to calculate informed priors from historical A/B test data
and save them as starting point for following monthly updates
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, date
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        database='ecomm_cust_db',
        user='rubyc',
        password='****'
    )

def create_prior_storage_table():
    """Create table to store calculated priors"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ecommerce_analytics.bayesian_priors_store (
                prior_id SERIAL PRIMARY KEY,
                test_id VARCHAR(50) NOT NULL,
                variant_id VARCHAR(10) NOT NULL,
                period_date DATE NOT NULL,
                alpha_value DECIMAL(15,6) NOT NULL,
                beta_value DECIMAL(15,6) NOT NULL,
                value_type VARCHAR(20) NOT NULL, -- 'prior' or 'posterior'
                data_points INTEGER DEFAULT 0,
                conversions INTEGER DEFAULT 0,
                calculation_method VARCHAR(50), -- 'historical', 'updated', 'uninformative'
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(test_id, variant_id, period_date, value_type)
            )
        """)
        
        # Create indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_priors_store_test_variant_date 
            ON ecommerce_analytics.bayesian_priors_store(test_id, variant_id, period_date);
            
            CREATE INDEX IF NOT EXISTS idx_priors_store_type 
            ON ecommerce_analytics.bayesian_priors_store(value_type);
        """)
        
        conn.commit()
        logger.info("Priors storage table created/verified")
        
    except Exception as e:
        logger.error(f"Error creating priors storage table: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def calculate_historical_priors(test_id, historical_end_date):
    """
    Calculate informed priors from historical data up to a specific date
    This gives us a starting point 
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        logger.info(f"Calculating historical priors for {test_id} up to {historical_end_date}")
        
        # Get historical A/B test data up to the specified date
        cur.execute("""
            SELECT 
                v.variant_id,
                v.variant_name,
                COUNT(DISTINCT e.customer_key) as total_users,
                SUM(CASE WHEN e.is_conversion THEN 1 ELSE 0 END) as total_conversions
            FROM ecommerce_analytics.fact_ab_test_events e
            JOIN ecommerce_analytics.dim_ab_test t ON e.test_key = t.test_key
            JOIN ecommerce_analytics.dim_test_variant v ON e.variant_key = v.variant_key
            WHERE t.test_id = %s 
            AND e.event_type = 'exposure'
            AND TO_DATE(e.event_date_key::text, 'YYYYMMDD') <= %s
            GROUP BY v.variant_id, v.variant_name
            ORDER BY v.variant_id
        """, (test_id, historical_end_date))
        
        historical_data = cur.fetchall()
        
        if not historical_data:
            logger.warning(f"No historical data found for {test_id}")
            return None
        
        print(f"\n{'='*60}")
        print(f"HISTORICAL PRIOR CALCULATION - {test_id}")
        print(f"Data up to: {historical_end_date}")
        print(f"{'='*60}")
        
        priors_calculated = {}
        
        for variant_id, variant_name, total_users, total_conversions in historical_data:
            # Use historical data to inform our priors
            # Method: Use the historical data as if it came from a previous experiment
            
            # Start with weak uninformative prior, then add historical evidence
            alpha_uninformative = 1.0
            beta_uninformative = 1.0
            
            # Add historical evidence crating informed prior
            alpha_prior = alpha_uninformative + total_conversions
            beta_prior = beta_uninformative + (total_users - total_conversions)
            
            # Calculate statistics
            prior_mean = alpha_prior / (alpha_prior + beta_prior)
            prior_variance = (alpha_prior * beta_prior) / ((alpha_prior + beta_prior)**2 * (alpha_prior + beta_prior + 1))
            
            priors_calculated[variant_id] = {
                'variant_name': variant_name,
                'alpha_prior': alpha_prior,
                'beta_prior': beta_prior,
                'historical_users': total_users,
                'historical_conversions': total_conversions,
                'prior_mean': prior_mean,
                'prior_variance': prior_variance
            }
            
            print(f"\nVariant {variant_id} ({variant_name}):")
            print(f"   Historical Evidence: {total_conversions:,} conversions from {total_users:,} users")
            print(f"   Historical Rate: {total_conversions/total_users:.3f} ({100*total_conversions/total_users:.1f}%)")
            print(f"   Calculated Prior: α={alpha_prior:.2f}, β={beta_prior:.2f}")
            print(f"   Prior Mean: {prior_mean:.3f}")
            print(f"   Prior Std: {np.sqrt(prior_variance):.3f}")
            
            # Save the calculated prior
            cur.execute("""
                INSERT INTO ecommerce_analytics.bayesian_priors_store 
                (test_id, variant_id, period_date, alpha_value, beta_value, value_type, 
                 data_points, conversions, calculation_method, notes)
                VALUES (%s, %s, %s, %s, %s, 'prior', %s, %s, 'historical', %s)
                ON CONFLICT (test_id, variant_id, period_date, value_type) 
                DO UPDATE SET
                    alpha_value = EXCLUDED.alpha_value,
                    beta_value = EXCLUDED.beta_value,
                    data_points = EXCLUDED.data_points,
                    conversions = EXCLUDED.conversions,
                    notes = EXCLUDED.notes,
                    created_at = CURRENT_TIMESTAMP
            """, (test_id, variant_id, historical_end_date, 
                  alpha_prior, beta_prior, total_users, total_conversions,
                  f'Historical prior from {total_users} users, {total_conversions} conversions up to {historical_end_date}'))
        
        conn.commit()
        
        print(f"Historical priors calculated and saved for {len(priors_calculated)} variants")
        print(f"Starting priors from {historical_end_date}")
        
        return priors_calculated
        
    except Exception as e:
        logger.error(f"Error calculating historical priors: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def get_stored_prior(test_id, variant_id, period_date, value_type='prior'):
    """Retrieve stored prior/posterior for a specific period"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT alpha_value, beta_value, data_points, conversions, calculation_method, notes
            FROM ecommerce_analytics.bayesian_priors_store
            WHERE test_id = %s AND variant_id = %s 
            AND period_date = %s AND value_type = %s
        """, (test_id, variant_id, period_date, value_type))
        
        result = cur.fetchone()
        if result:
            return {
                'alpha': float(result[0]),
                'beta': float(result[1]),
                'data_points': result[2],
                'conversions': result[3],
                'method': result[4],
                'notes': result[5]
            }
        else:
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving stored prior: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def save_monthly_posterior(test_id, variant_id, period_date, alpha_posterior, beta_posterior, 
                          data_points, conversions, notes="Monthly posterior update"):
    """Save calculated posterior for a month"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO ecommerce_analytics.bayesian_priors_store 
            (test_id, variant_id, period_date, alpha_value, beta_value, value_type, 
             data_points, conversions, calculation_method, notes)
            VALUES (%s, %s, %s, %s, %s, 'posterior', %s, %s, 'updated', %s)
            ON CONFLICT (test_id, variant_id, period_date, value_type) 
            DO UPDATE SET
                alpha_value = EXCLUDED.alpha_value,
                beta_value = EXCLUDED.beta_value,
                data_points = EXCLUDED.data_points,
                conversions = EXCLUDED.conversions,
                notes = EXCLUDED.notes,
                created_at = CURRENT_TIMESTAMP
        """, (test_id, variant_id, period_date, alpha_posterior, beta_posterior, 
              data_points, conversions, notes))
        
        conn.commit()
        logger.info(f"Posterior saved for {variant_id} on {period_date}: α={alpha_posterior:.2f}, β={beta_posterior:.2f}")
        
    except Exception as e:
        logger.error(f"Error saving posterior: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def copy_posterior_as_next_prior(test_id, variant_id, current_period_date, next_period_date):
    """Copy this month's posterior as next month's prior"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get current posterior
        posterior = get_stored_prior(test_id, variant_id, current_period_date, 'posterior')
        
        if not posterior:
            logger.warning(f"No posterior found for {variant_id} on {current_period_date}")
            return False
        
        # Save as next month's prior
        cur.execute("""
            INSERT INTO ecommerce_analytics.bayesian_priors_store 
            (test_id, variant_id, period_date, alpha_value, beta_value, value_type, 
             data_points, conversions, calculation_method, notes)
            VALUES (%s, %s, %s, %s, %s, 'prior', %s, %s, 'updated', %s)
            ON CONFLICT (test_id, variant_id, period_date, value_type) 
            DO UPDATE SET
                alpha_value = EXCLUDED.alpha_value,
                beta_value = EXCLUDED.beta_value,
                data_points = EXCLUDED.data_points,
                conversions = EXCLUDED.conversions,
                notes = EXCLUDED.notes,
                created_at = CURRENT_TIMESTAMP
        """, (test_id, variant_id, next_period_date, 
              posterior['alpha'], posterior['beta'], 
              posterior['data_points'], posterior['conversions'],
              f'Prior derived from {current_period_date} posterior'))
        
        conn.commit()
        logger.info(f"Posterior from {current_period_date} copied as prior for {next_period_date}")
        return True
        
    except Exception as e:
        logger.error(f"Error copying posterior as prior: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def view_prior_evolution(test_id, variant_id=None):
    """View the evolution of priors and posteriors over time"""
    conn = get_db_connection()
    
    try:
        where_clause = "WHERE test_id = %s"
        params = [test_id]
        
        if variant_id:
            where_clause += " AND variant_id = %s"
            params.append(variant_id)
        
        query = f"""
            SELECT 
                variant_id,
                period_date,
                value_type,
                alpha_value,
                beta_value,
                ROUND(alpha_value / (alpha_value + beta_value), 4) as mean_estimate,
                data_points,
                conversions,
                calculation_method
            FROM ecommerce_analytics.bayesian_priors_store
            {where_clause}
            ORDER BY variant_id, period_date, value_type
        """
        
        df = pd.read_sql(query, conn, params=params)
        
        if df.empty:
            print(f"No prior evolution data found for {test_id}")
            return None
        
        print(f"\n{'='*80}")
        print(f"PRIOR EVOLUTION - {test_id}")
        if variant_id:
            print(f"Variant: {variant_id}")
        print(f"{'='*80}")
        
        # Group by variant
        for variant in df['variant_id'].unique():
            variant_data = df[df['variant_id'] == variant]
            print(f"\nVariant {variant}:")
            print(variant_data.to_string(index=False))
        
        return df
        
    except Exception as e:
        logger.error(f"Error viewing prior evolution: {e}")
        raise
    finally:
        conn.close()

def main():
    """Setup and calculate initial priors"""
    print("Initial Priors Calculator")
    print("="*50)
    
    # Create storage table
    create_prior_storage_table()
    
    # Calculate historical priors from your existing A/B test data
    test_id = 'coupon_promo_v1'
    
    # Use the last date of your historical data as the cutoff
    historical_end_date = date(2018, 10, 17) 
    
    print(f"\nCalculating historical priors for {test_id}...")
    print(f"Using data up to: {historical_end_date}")
    
    priors = calculate_historical_priors(test_id, historical_end_date)
    
    if priors:
        print(f"Initial priors calculated and saved!")
        print("\nNext steps:")
        print("1. Your monthly DAG can now use get_stored_prior() to get current priors")
        print("2. Calculate monthly posteriors using new data + stored priors")
        print("3. Save posteriors using save_monthly_posterior()")
        print("4. Copy posteriors as next month's priors using copy_posterior_as_next_prior()")
        
        print(f"\nExample usage in monthly DAG:")
        print(f"prior = get_stored_prior('{test_id}', 'A', '{historical_end_date}')")
        print(f"# Use prior['alpha'] and prior['beta'] for Bayesian calculation")
    
    # Show current state
    view_prior_evolution(test_id)

if __name__ == "__main__":
    main()
