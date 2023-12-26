import yaml
from sqlalchemy import create_engine
class DatabaseConnector:
    def __init__(self, db_config):
        # Initialization code goes here, db_config can hold database connection details

    # Method to connect to the database
    def connect(self):
        pass

    # Method to upload data to the database
    def upload_data(self, data):
        pass

# Create a method read_db_creds this will read the
# credentials yaml file and return a dictionary 
# of the credentials.

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials
    
# create a method init_db_engine which will 
# read the credentials from the return of 
# read_db_creds and initialise and return an 
# sqlalchemy database engine

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(f'postgresql://{creds[aicore_admin]}:{creds[AiCore2022]}@{creds[data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com]}:{creds[5432]}/{creds[postgres]}')
        return engine
    
#method to retrieve list of all table names in database
    def list_db_tables(self)
        engine = self.init_db_engine
        return engine.table_names()

#method to upload a DataFrame
    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine