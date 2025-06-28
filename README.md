# **Ecommerce A/B Testing Analytics Pipeline**

A data pipeline built with Apache Airflow for performing Bayesian A/B testing on Brazilian ecommerce data using the Olist dataset.

## **Project Overview**

This project implements an end-to-end analytics pipeline that:

- Transforms operational ecommerce data into an optimized star schema
- Enables sophisticated A/B testing 
- Provides automated Bayesian analysis for experiment evaluation
- Delivers actionable insights through scheduled data processing

## **Data Architecture**

Operational Tables (Olist Dataset)  
↓ ETL Pipeline (Airflow)  
Star Schema (PostgreSQL)  
↓ A/B Testing Framework  
Bayesian Statistical Analysis  
↓ Automated Insights  
Business Decisions

## **Tech Stack**

Schedule tasks: Apache Airflow 3.x  
Database: PostgreSQL  
Analytics: Python (pandas, numpy, scipy)  
Statistical Methods: Bayesian A/B Testing  
Data Source: Olist Brazilian E-Commerce Dataset (2016-2018)  

## **Star Schema Design** 

ecomm_cust_db.public schema:  
├── customers  
├── orders_dataset    
├── order_items  
├── order_payments  
├── order_reviews  
├── products_dataset  
└── sellers  

ecomm_cust_db.ecommerce_analytics schema:  
├── dim_customer  
├── dim_product  
├── dim_seller  
├── fact_sales  
├── fact_orders  
└── etc.  

### Dimension Tables  

dim_customer: Customer demographics, segments, and lifetime value  
dim_product: Product catalog with calculated metrics (volume, weight categories)  
dim_seller: Seller information and performance metrics  
dim_date: Date dimension with Brazilian holidays  
dim_ab_test: A/B test definitions and metadata  
dim_test_variant: Test variants (control/treatment groups)  

### Fact Tables

fact_sales: Granular sales transactions  
fact_orders: Order-level aggregations  
fact_payments: Payment method analysis  
fact_reviews: Customer review analytics  
fact_ab_test_events: A/B test exposure and conversion tracking  

## A/B Testing Framework
### Features

Randomized Assignment: Hash-based customer allocation to variants (A hash function is a special type of function that takes an input (like a string, number, or file) 
and returns a fixed-size string of characters of numbers and letters.)  

Bayesian Analysis: Beta-Binomial modeling for statistical inference  
Approach is used for modeling binary outcomes(Bernoulli), allows for updates on belief as more data comes   
p Beta(α,β)  
α = prior successes + 1  
β = prior failures + 1  
Beta distribution is flexible and confined to [0, 1], making it perfect for probabilities  

Automated Evaluation: Credible intervals and probability calculations

Multi-variate Support: This framework can support A/B/C/.. testing

### Example Test Case
Coupon Promotion Experiment

Variant A (Control): 10% discount offer  
Variant B (Treatment): Free shipping + 5% discount  
Metric: Conversion rate  
Analysis: Bayesian posterior probability that B > A  

## Pipeline Components  
### 1. ETL Pipeline (ecomm_analytics_pipeline)    
Schedule: Monthly (@monthly)   
Tasks:  

load_customers_dimension - Extract and enrich customer data  
load_products_dimension - Process product catalog with calculated fields  
load_sellers_dimension - Aggregate seller performance metrics  
load_sales_fact - Transform transactional data  
setup_ab_test - Initialize experiment framework  
assign_customers_to_variants - Randomized group assignment  
bayesian_ab_analysis - Statistical evaluation and recommendations  

### 2. Data Quality Pipeline (ecomm_data_quality_checks)  
Schedule: Monthly (@monthly)  
Validations:  

Null value detection in critical fields  
Referential integrity checks  
Date range validation  
Data freshness monitoring  

## Statistical Methodology
### Bayesian A/B Testing  

Prior: Uninformative Beta(1,1) prior  
Likelihood: Binomial for conversion events  
Posterior: Beta distribution for conversion rates  
Decision Rule: P(Treatment > Control) > 95% for significance  

### Advantages over Frequentist Testing

Interpretable Results: Direct probability statements  
Early Stopping: Continuous monitoring without multiple testing issues  
Practical Significance: Incorporates business context  
Uncertainty Quantification: Credible intervals for effect sizes  

## Future Enhancements  
Real-time Processing: Stream processing for live experiments  
Advanced Segmentation: Machine learning-based customer clustering  
Multi-armed Bandits: Dynamic traffic allocation  
Causal Inference: Treatment effect estimation with confounding control  
Automated Alerting: Slack/email notifications for significant results  
