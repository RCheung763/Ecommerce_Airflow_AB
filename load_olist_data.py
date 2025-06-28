import pandas as pd
from sqlalchemy import create_engine

db_name = 'ecomm_cust_db'
user = 'rubyc'
password = ''
host = 'localhost'
port = 5432

# SQLAlchemy connection string
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

# read in data 
customers = pd.read_csv('olist_brazilian_ecommerce/olist_customers_dataset.csv')
geolocation = pd.read_csv('olist_brazilian_ecommerce/olist_geolocation_dataset.csv')
order_items = pd.read_csv('olist_brazilian_ecommerce/olist_order_items_dataset_clean.csv')
order_payments = pd.read_csv('olist_brazilian_ecommerce/olist_order_payments_dataset_clean.csv')
order_reviews = pd.read_csv('olist_brazilian_ecommerce/olist_order_reviews_dataset.csv')
orders_dataset = pd.read_csv('olist_brazilian_ecommerce/olist_orders_dataset.csv')
products_dataset = pd.read_csv('olist_brazilian_ecommerce/olist_products_dataset_clean.csv')
sellers = pd.read_csv('olist_brazilian_ecommerce/olist_sellers_dataset.csv')
product_category_name = pd.read_csv('olist_brazilian_ecommerce/product_category_name_translation.csv')

# Load into PostgreSQL
customers.to_sql('customers', engine, if_exists='replace', index=False)
geolocation.to_sql('geolocation', engine, if_exists='replace', index=False)
order_items.to_sql('order_items', engine, if_exists='replace', index=False)
order_payments.to_sql('order_payments', engine, if_exists='replace', index=False)
order_reviews.to_sql('order_reviews', engine, if_exists='replace', index=False)
orders_dataset.to_sql('orders', engine, if_exists='replace', index=False)
products_dataset.to_sql('products', engine, if_exists='replace', index=False)
sellers.to_sql('sellers', engine, if_exists='replace', index=False)
product_category_name.to_sql('product_category_name_translation', engine, if_exists='replace', index=False)

