import yaml
from sqlalchemy import create_engine
import pandas as pd
import tabula
import requests
import os
import boto3

print("Current Working Directory:", os.getcwd())

class DatabaseConnector:
   
    def read_db_creds(self):
        file_path = 'C:/Users/DELL7390/Documents/AICP2/db_creds.yaml'  
        with open(file_path, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(f'postgresql://{creds["RDS_USER"]}:{creds["RDS_PASSWORD"]}@{creds["RDS_HOST"]}:{creds["RDS_PORT"]}/{creds["RDS_DATABASE"]}')
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        return engine.table_names()

    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        df.to_sql(table_name, engine, index=False, if_exists='replace')

class DataExtractor:
    def __init__(self):
        # Initialization code goes here
        pass

    def extract_from_csv(self, file_path):
        # Method to extract data from CSV files
        pass

    def extract_from_api(self, api_url):
        # Method to extract data from an API
        pass

    def extract_from_s3(self, bucket_name, file_name):
        # Method to extract data from an S3 bucket
        pass

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        return pd.read_sql_table(table_name, engine)

    def retrieve_pdf_data(self, pdf_link):
        tables = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        df = pd.concat(tables, ignore_index=True)
        return df
    
    def list_number_of_stores(self, api_url, headers):
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # Ensure the data is an integer or can be converted to an integer
            try:
                return int(data) if isinstance(data, int) else int(data.get('number_of_stores', 0))
            except (ValueError, TypeError):
                print("Error: Invalid response format")
                return 0
        else:
            print(f"Error: API request failed with status code {response.status_code}")
            return 0
    def retrieve_stores_data(self, store_details_url, headers, number_of_stores):
        all_stores_data = []
        for store_number in range(1, number_of_stores + 1):
            response = requests.get(store_details_url.format(store_number=store_number), headers=headers)
            if response.status_code == 200:
                all_stores_data.append(response.json())
            else:
                # Handle error
                pass
        return pd.DataFrame(all_stores_data)
    def extract_from_s3_json(self, s3_url):
        bucket_name = s3_url.split('/')[2]
        object_key = '/'.join(s3_url.split('/')[3:])
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        return pd.read_json(obj['Body'])

class DataCleaning:
    def __init__(self):
        # Initialization code goes here
        pass

    def clean_csv_data(self, data):
        # Method to clean data from CSV files
        pass

    def clean_api_data(self, data):
        # Method to clean data from API responses
        pass

    def clean_s3_data(self, data):
        # Method to clean data from S3 bucket
        pass

    def clean_user_data(self, df):
        # Implement data cleaning logic here
        return df

    def clean_card_data(self, df):
        # Perform cleaning operations
        # Example: df.dropna(), df.replace(), etc.
        return df
    
    def convert_product_weights(self, df):
        for i, row in df.iterrows():
            weight = row['weight']
            # Assume weights are stored as strings like '500g' or '2.5kg'
            if 'ml' in weight:
                # Convert ml to g (1:1 ratio)
                df.at[i, 'weight'] = float(weight.replace('ml', ''))
            elif 'g' in weight:
                # Convert g to kg
                df.at[i, 'weight'] = float(weight.replace('g', '')) / 1000
            elif 'kg' in weight:
                # Already in kg, just convert to float
                df.at[i, 'weight'] = float(weight.replace('kg', ''))
            # Add more conditions if there are other units
        return df
    def clean_orders_data(self, df):
        # Remove specific columns
        df.drop(columns=['first_name', 'last_name', '1'], errors='ignore', inplace=True)
        return df
    def clean_json_data(self, df):
        # Implement your JSON data cleaning logic here
        # Example: df.dropna(), df.drop_duplicates(), etc.
        return df

def main():
    # Initialize class instances
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()

    # AWS S3 and API setup
    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    headers = {'x-api-key': api_key}

    # Extract and upload product data from S3 (CSV)
    products_url = "s3://data-handling-public/products.csv"
    products_df = data_extractor.extract_from_s3(products_url)
    products_df = data_cleaner.convert_product_weights(products_df)
    cleaned_products_df = data_cleaner.clean_products_data(products_df)
    db_connector.upload_to_db(cleaned_products_df, 'dim_products')

    # Extract and upload store data from API
    number_stores_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    number_of_stores = data_extractor.list_number_of_stores(number_stores_url, headers)
    if number_of_stores > 0:
        store_details_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}"
        store_data = data_extractor.retrieve_stores_data(store_details_url, headers, number_of_stores)
        cleaned_store_data = data_cleaner.clean_store_data(store_data)
        db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')
    else:
        print("No stores to retrieve or error occurred.")

    # Extract and upload orders data from RDS
    orders_table_name = "your_orders_table_name"  # Replace with the actual table name
    orders_df = data_extractor.read_rds_table(db_connector, orders_table_name)
    cleaned_orders_df = data_cleaner.clean_orders_data(orders_df)
    db_connector.upload_to_db(cleaned_orders_df, 'orders_table')

    # Extract and upload date details data from S3 (JSON)
    json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    json_data = data_extractor.extract_from_s3_json(json_url)
    cleaned_json_data = data_cleaner.clean_json_data(json_data)
    db_connector.upload_to_db(cleaned_json_data, 'dim_date_times')

# Execute the main function
if __name__ == "__main__":
    main()