-- Implement Star Schema
CREATE SCHEMA IF NOT EXISTS ecommerce_analytics;
SET search_path TO ecommerce_analytics;  -- Uses ecommerce_analytics as default schema 

-- Creating Dimension tables 
-- Customer Dimension table 
CREATE TABLE dim_customer(
	customer_key SERIAL PRIMARY KEY,
    	customer_id VARCHAR(50) UNIQUE NOT NULL,
   	customer_unique_id VARCHAR(50),
    	customer_zip_code_prefix VARCHAR(10),
    	customer_city VARCHAR(100),
    	customer_state VARCHAR(10),
	-- Derived 
    	customer_segment VARCHAR(50), -- 'new', 'returning', 'vip', etc.
   	 first_order_date DATE,
    	last_order_date DATE,
   	 total_orders INTEGER DEFAULT 0,
   	 lifetime_value DECIMAL(10,2) DEFAULT 0,
   	 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   	 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Product Dimension Table 
CREATE TABLE dim_product (
	product_key SERIAL PRIMARY KEY,
    	product_id VARCHAR(50) UNIQUE NOT NULL,
    	product_category_name VARCHAR(100),
   	product_name_length INTEGER,
    	product_description_length INTEGER,
	product_photos_qty INTEGER,
   	product_weight_g DECIMAL(10,2),
   	product_length_cm DECIMAL(8,2),
   	product_height_cm DECIMAL(8,2),
   	product_width_cm DECIMAL(8,2),
	-- Derived 
    	is_heavy_item BOOLEAN,
    	is_large_item BOOLEAN, 
    	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seller Dimension Table 
CREATE TABLE dim_seller (
    	seller_key SERIAL PRIMARY KEY,
    	seller_id VARCHAR(50) UNIQUE NOT NULL,
    	seller_zip_code_prefix VARCHAR(10),
    	seller_city VARCHAR(100),
    	seller_state VARCHAR(10),
	-- Derived
    	seller_segment VARCHAR(50),
    	first_sale_date DATE,
    	last_sale_date DATE,
    	total_sales INTEGER DEFAULT 0,
    	total_revenue DECIMAL(12,2) DEFAULT 0,
    	avg_rating DECIMAL(3,2),
    	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date Dimension
CREATE TABLE dim_date (
	date_key INTEGER PRIMARY KEY, 
    	full_date DATE UNIQUE NOT NULL,
    	year INTEGER,
    	quarter INTEGER,
    	month INTEGER,
    	month_name VARCHAR(20),
    	week INTEGER,
    	day_of_month INTEGER,
    	day_of_week INTEGER,
    	day_name VARCHAR(20),
    	is_weekend BOOLEAN
);

-- A/B Test Dimension
CREATE TABLE dim_ab_test (
	test_key SERIAL PRIMARY KEY,
    	test_id VARCHAR(50) UNIQUE NOT NULL,
    	test_name VARCHAR(255),
   	 test_type VARCHAR(50), 
   	 hypothesis TEXT,
    	start_date DATE,
    	end_date DATE,
    	status VARCHAR(20), 
   	 success_metric VARCHAR(100), 
   	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- A/B Test Variant Dimension
CREATE TABLE dim_test_variant (
    	variant_key SERIAL PRIMARY KEY,
    	test_key INTEGER REFERENCES dim_ab_test(test_key),
  	variant_id VARCHAR(50),
    	variant_name VARCHAR(100),
    	description TEXT,
    	is_control BOOLEAN DEFAULT FALSE,
    	traffic_allocation DECIMAL(5,2), 
    	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    	UNIQUE(test_key, variant_id)
);

-- Order Status Dimension 
CREATE TABLE dim_order_status (
    	status_key SERIAL PRIMARY KEY,
    	status_value VARCHAR(50) UNIQUE NOT NULL,
    	status_description TEXT,
    	is_final_status BOOLEAN,
    	status_order INTEGER 
);


-- Facts tables 

-- Main sales
CREATE TABLE fact_sales (
    	sale_key SERIAL PRIMARY KEY,
    	customer_key INTEGER REFERENCES dim_customer(customer_key),
    	product_key INTEGER REFERENCES dim_product(product_key),
    	seller_key INTEGER REFERENCES dim_seller(seller_key),
    	order_date_key INTEGER REFERENCES dim_date(date_key),
    	shipped_date_key INTEGER REFERENCES dim_date(date_key),
    	delivered_date_key INTEGER REFERENCES dim_date(date_key),
    	test_key INTEGER REFERENCES dim_ab_test(test_key),
    	variant_key INTEGER REFERENCES dim_test_variant(variant_key),
    
	-- From original database
    	order_id VARCHAR(50),
    	order_item_id INTEGER,
    	price DECIMAL(10,2),
    	freight_value DECIMAL(10,2),
    
	-- Calculated
    	total_value DECIMAL(10,2), -- price + freight
    
	-- Derived
    	is_first_purchase BOOLEAN,
    	is_repeat_customer BOOLEAN,
    
   	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders
CREATE TABLE fact_orders (
	order_key SERIAL PRIMARY KEY,
    	customer_key INTEGER REFERENCES dim_customer(customer_key),
    	purchase_date_key INTEGER REFERENCES dim_date(date_key),
    	approved_date_key INTEGER REFERENCES dim_date(date_key),
    	delivered_date_key INTEGER REFERENCES dim_date(date_key),
    	estimated_delivery_date_key INTEGER REFERENCES dim_date(date_key),
   	test_key INTEGER REFERENCES dim_ab_test(test_key),
   	variant_key INTEGER REFERENCES dim_test_variant(variant_key),
   	status_key INTEGER REFERENCES dim_order_status(status_key),
    	
	-- From original database
	order_id VARCHAR(50) UNIQUE,
    
    	-- Calculated 
    	total_order_value DECIMAL(12,2),
    	total_freight_value DECIMAL(10,2),
   	item_count INTEGER,
    	unique_sellers INTEGER,
    
   	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Payment
CREATE TABLE fact_payments (
    	payment_key SERIAL PRIMARY KEY,
    	customer_key INTEGER REFERENCES dim_customer(customer_key),
    	payment_date_key INTEGER REFERENCES dim_date(date_key),
    	test_key INTEGER REFERENCES dim_ab_test(test_key),
    	variant_key INTEGER REFERENCES dim_test_variant(variant_key),
    
	-- From original database
    	order_id VARCHAR(50),
    	payment_sequential INTEGER,
    	payment_type VARCHAR(50),
    	payment_installments INTEGER,
    	payment_value DECIMAL(10,2),
    
	-- Flags
    	is_installment_payment BOOLEAN,
    	is_credit_card BOOLEAN,
    
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Review Fact Table
CREATE TABLE fact_reviews (
    review_key SERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    seller_key INTEGER REFERENCES dim_seller(seller_key),
    review_date_key INTEGER REFERENCES dim_date(date_key),
    answer_date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Review details
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INTEGER,
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    
    -- Derived measures
    has_comment BOOLEAN,
    comment_length INTEGER,
    sentiment_score DECIMAL(3,2), -- You can add sentiment analysis later
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- A/B Test Events Fact Table
CREATE TABLE fact_ab_test_events (
    event_key SERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    test_key INTEGER REFERENCES dim_ab_test(test_key),
    variant_key INTEGER REFERENCES dim_test_variant(variant_key),
    event_date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Event details
    order_id VARCHAR(50),
    event_type VARCHAR(50), -- 'exposure', 'purchase', 'cart_add', etc.
    event_timestamp TIMESTAMP,
    session_id VARCHAR(100),
    
    -- Measures for A/B testing
    conversion_value DECIMAL(10,2),
    is_conversion BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- INDEXES for analytics schema 
-- Customer dimension
CREATE INDEX idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_city_state ON dim_customer(customer_city, customer_state);

-- Product dimension  
CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(product_category_name);

-- Seller dimension
CREATE INDEX idx_dim_seller_id ON dim_seller(seller_id);
CREATE INDEX idx_dim_seller_city_state ON dim_seller(seller_city, seller_state);

-- Date dimension
CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- Fact table indexes
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_seller ON fact_sales(seller_key);
CREATE INDEX idx_fact_sales_date ON fact_sales(order_date_key);
CREATE INDEX idx_fact_sales_test ON fact_sales(test_key, variant_key);
CREATE INDEX idx_fact_sales_order_id ON fact_sales(order_id);

CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_key);
CREATE INDEX idx_fact_orders_date ON fact_orders(purchase_date_key);
CREATE INDEX idx_fact_orders_test ON fact_orders(test_key, variant_key);

-- POPULATE REFERENCE DATA

-- Insert order statuses
INSERT INTO dim_order_status (status_value, status_description, is_final_status, status_order) VALUES
('created', 'Order created', FALSE, 1),
('approved', 'Payment approved', FALSE, 2),
('processing', 'Order being processed', FALSE, 3),
('shipped', 'Order shipped', FALSE, 4),
('delivered', 'Order delivered', TRUE, 5),
('unavailable', 'Product unavailable', TRUE, 6),
('canceled', 'Order canceled', TRUE, 7),
('invoiced', 'Order invoiced', FALSE, 8);

-- Function to populate date dimension
CREATE OR REPLACE FUNCTION populate_date_dimension(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO dim_date (
            date_key, full_date, year, quarter, month, month_name,
            week, day_of_month, day_of_week, day_name, is_weekend
        ) VALUES (
            TO_CHAR(curr_date, 'YYYYMMDD')::INTEGER,
            curr_date,
            EXTRACT(YEAR FROM curr_date),
            EXTRACT(QUARTER FROM curr_date),
            EXTRACT(MONTH FROM curr_date),
            TO_CHAR(curr_date, 'Month'),
            EXTRACT(WEEK FROM curr_date),
            EXTRACT(DAY FROM curr_date),
            EXTRACT(DOW FROM curr_date),
            TO_CHAR(curr_date, 'Day'),
            EXTRACT(DOW FROM curr_date) IN (0, 6)
        )
        ON CONFLICT (full_date) DO NOTHING;
        
        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension
SELECT populate_date_dimension('2016-01-01', '2025-12-31');
