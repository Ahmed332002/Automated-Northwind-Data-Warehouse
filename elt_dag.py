import logging
import os
import re
import shutil
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Setup Logger
logger = logging.getLogger("airflow.task")

# --- Configurations ---
CONNECTION_ID = 'postgres_default'
SOURCE_FOLDER = '/usr/local/airflow/include/NORTHWIND'

def to_snake_case(name):
    """Converts PascalCase/CamelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

@dag(
    dag_id='northwind_final_optimized_pipeline',
    default_args={
        'owner': 'ahmed_nabil',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['production', 'incremental_files', 'data_quality']
)
def northwind_etl():

    @task()
    def run_staging_layer():
        """Task 1: Process NEW files and move them to 'processed' folder."""
        logger.info("Starting Staging Layer (File-based Incremental)...")
        try:
            hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
            engine = hook.get_sqlalchemy_engine()
            
            # make sure processed folder exists
            processed_folder = os.path.join(SOURCE_FOLDER, 'processed')
            os.makedirs(processed_folder, exist_ok=True)
            
            files = [f for f in os.listdir(SOURCE_FOLDER) if f.endswith(('.csv', '.xlsx'))]
            
            if not files:
                logger.info("No new files found in SOURCE_FOLDER.")
                return

            for file in files:
                table_name = os.path.splitext(file)[0].lower()
                full_path = os.path.join(SOURCE_FOLDER, file)
                
                # read file into DataFrame
                df = pd.read_csv(full_path) if file.endswith('.csv') else pd.read_excel(full_path)
                
                # write to staging schema
                df.to_sql(table_name, engine, schema='staging', if_exists='replace', index=False)
                
                # move file to processed folder
                shutil.move(full_path, os.path.join(processed_folder, file))
                logger.info(f"Successfully staged and archived: {file}")
                
        except Exception as e:
            logger.error(f"Staging failed: {str(e)}")
            raise

    @task()
    def run_transformation_layer():
        """Task 2: Advanced Cleaning, Deduplication & Loading to DW."""
        logger.info("Starting Transformation Layer...")
        try:
            hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
            engine = hook.get_sqlalchemy_engine()

            # Mapping of table labels to actual staging table names
            tables = {
                "Customers": "customers", "Products": "products", "Employees": "employees",
                "Orders": "orders", "Details": "order_details", "Categories": "categories",
                "Suppliers": "suppliers", "Shippers": "shippers"
            }
            
            # Load all staging tables into DataFrames
            all_dfs = {}
            for label, t in tables.items():
                try:
                    all_dfs[label] = pd.read_sql(f"SELECT * FROM staging.{t}", engine)
                except:
                    logger.warning(f"Table staging.{t} not found. Skipping.")
                    continue

            if not all_dfs:
                logger.info("No data found in Staging to transform.")
                return

            # Data Cleaning  
            final_cleaned = {}
            keys_map = {
                "Customers": "customer_id", "Products": "product_id",
                "Employees": "employee_id", "Shippers": "shipper_id"
            }

            for name, df in all_dfs.items():
                temp_df = df.copy()
                temp_df.columns = [to_snake_case(col) for col in temp_df.columns]
                
                # 1. Deduplication 
                if name in keys_map:
                    temp_df = temp_df.drop_duplicates(subset=[keys_map[name]], keep='last')
                else:
                    temp_df = temp_df.drop_duplicates()

                # 2. String Cleaning & Null Handling
                for col in temp_df.select_dtypes(include=['object']):
                    temp_df[col] = temp_df[col].str.strip().fillna('Unknown')
                
                # 3. Phone/Fax Regex Cleaning
                for col in ['phone', 'fax']:
                    if col in temp_df.columns:
                        temp_df[col] = temp_df[col].str.replace(r'[\(\)\s\-\.]', '', regex=True)
                
                # 4. Dates & Numerics
                date_cols = [c for c in temp_df.columns if 'date' in c]
                for col in date_cols:
                    temp_df[col] = pd.to_datetime(temp_df[col], errors='coerce')
                
                num_cols = temp_df.select_dtypes(include=['number']).columns
                temp_df[num_cols] = temp_df[num_cols].fillna(0)

                final_cleaned[name] = temp_df

            # --- Modeling (Star Schema) ---
            # Dim Products
            dim_p = pd.merge(final_cleaned['Products'], final_cleaned['Categories'], on='category_id', how='left')
            dim_p = pd.merge(dim_p, final_cleaned['Suppliers'], on='supplier_id', how='left')
            dim_products = dim_p[['product_id', 'product_name', 'category_name', 'company_name', 'unit_price']].rename(columns={'company_name': 'supplier_name'})

            # Fact Sales
            fact_sales = pd.merge(final_cleaned['Details'], final_cleaned['Orders'], on='order_id', how='inner')
            fact_sales['net_amount'] = (fact_sales['unit_price'] * fact_sales['quantity']) * (1 - fact_sales['discount'])

            # --- Loading to DW (With CASCADE Reset) ---
            with engine.begin() as conn:
                conn.execute(sqlalchemy.text("DROP SCHEMA IF EXISTS dw CASCADE;"))
                conn.execute(sqlalchemy.text("CREATE SCHEMA dw;"))

            final_cleaned['Customers'].to_sql('dim_customers', engine, schema='dw', if_exists='replace', index=False)
            final_cleaned['Employees'].to_sql('dim_employees', engine, schema='dw', if_exists='replace', index=False)
            final_cleaned['Shippers'].to_sql('dim_shippers', engine, schema='dw', if_exists='replace', index=False)
            dim_products.to_sql('dim_products', engine, schema='dw', if_exists='replace', index=False)
            fact_sales.to_sql('fact_sales', engine, schema='dw', if_exists='replace', index=False)
            
            logger.info("Transformation and DW Load completed successfully.")

        except Exception as e:
            logger.error(f"Transformation Layer failed: {str(e)}")
            raise

    run_staging_layer() >> run_transformation_layer()

northwind_etl_dag = northwind_etl()