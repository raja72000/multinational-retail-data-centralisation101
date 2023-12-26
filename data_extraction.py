import pandas as pd
import tabula
import requests

class DataExtractor:
    
    def __init__(self):
        # Initialization code goes here
        pass
    # Method to extract data from CSV files
    def extract_from_csv(self, file_path):
        pass

    # Method to extract data from an API
    def extract_from_api(self, api_url):
        pass

    # Method to extract data from an S3 bucket
    def extract_from_s3(self, bucket_name, file_name):
        pass

    def read_rds_table(self, db_connector, table_name):
    engine = db_connector.init_db_engine()
    return pd.read_sql_tabke(table_name, engine)

    def retrieve_pdf_data(self, pdf_link):
        tables = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        df = pd.concat(tables, ignore_index=True)
        return df
    
    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            return response.json()  # Assuming the API returns the number directly
        else:
            return None  # Or handle errors as appropriate









  


 