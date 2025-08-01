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
├── dim_bayesian_priors_store  
├── dim_ab_test  
├── dim_date  
├── dim_order_status  
├── dim_test_variant  
├── fact_sales  
├── fact_orders  
├── fact_reviews  
├── fact_ab_test_events  
└── fact_payments  


### Dimension Tables  

dim_customer: Customer demographics, segments, and lifetime value  
dim_product: Product catalog with calculated metrics (volume, weight categories)  
dim_seller: Seller information and performance metrics  
dim_date: Date dimension with Brazilian holidays  
dim_ab_test: A/B test definitions and metadata  
dim_test_variant: Test variants (control/treatment groups)
bayesian_priors_store: Stores prior for Bayesian A/B test

### Fact Tables

fact_sales: Granular sales transactions  
fact_orders: Order-level aggregations  
fact_payments: Payment method analysis  
fact_reviews: Customer review analytics  
fact_ab_test_events: A/B test exposure and conversion tracking  

### Airflow file structure  
├── dags/  
&nbsp;&nbsp;&nbsp;&nbsp;└── ecomm_monthly_tasks.py          # ← Monthly script: A/B test, data quality checks  
└── utils/  
&nbsp;&nbsp;&nbsp;&nbsp;├── monthly_bayesian_update.py      # ← Bayesian update  
&nbsp;&nbsp;&nbsp;&nbsp;├── calculate_initial_priors.py     # ← This file was used after initial load and calculating posteriors from historical data   

## A/B Testing Framework
### Features

Randomized Assignment: Hash-based customer allocation to variants for test simulation

Bayesian Analysis: Beta-Binomial modeling for statistical inference  
Approach is used for modeling binary outcomes(Bernoulli), allows for updates on belief as more data comes   
p Beta(α,β)  
α = prior successes + 1  
β = prior failures + 1  
Beta distribution is flexible and confined to [0, 1], interpretable as probabilities  


### Test Case
Coupon Promotion Experiment

## Pipeline Components  
### 1. ETL Pipeline (updates hypothetical)  
Scripts include the initial historical data load into database and star schema

Schedule: Daily (@daily)    
Daily ingestion of data 

### 2. Data Quality Pipeline (ecomm_data_quality_checks)  
Schedule: Monthly (@monthly)  
Validations:  
Null value detection in critical fields  
Referential integrity checks  
Date range validation  
Data freshness monitoring  

### 3. Bayesian A/B Testing
Schedule Monthly (@monthly)

## Statistical Methodology
### Bayesian A/B Testing  

Prior: Uninformative Beta(1,1) prior  
Likelihood: Binomial for conversion events  
Posterior: Beta distribution for conversion rates  
Decision Rule: P(Treatment > Control) > 95% for significance  

Prior Distribution
θ ~ Beta(α₀, β₀)  
θ = true conversion rate for a variant  
α₀ = prior alpha parameter  
β₀ = prior beta parameter  

### Advantages over Frequentist Testing

Interpretable Results: Direct probability statements  
Early Stopping: Continuous monitoring without multiple testing issues  
Practical Significance: Incorporates business context  
Uncertainty Quantification: Credible intervals for effect sizes  

## Future Enhancements  
Real-time Processing: Stream processing for live experiments, build the daily ingestion of data portion of the pipeline
Advanced Segmentation: Machine learning-based customer clustering 
