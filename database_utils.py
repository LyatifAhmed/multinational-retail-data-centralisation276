import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    '''
    Class DatabaseConnector which you will use to connect with and upload data to the database.
    '''
    def __init__(self):

        pass

    def read_db_creds(self):
        '''
        Reads a credential yaml file to return a dictonary of the database credentials.
        '''
        with open('db_creds.yaml', 'r') as db_creds:
            db_creds = yaml.safe_load(db_creds)
            #print(db_creds)
            return db_creds
        
    def init_db_engine(self):
        '''
        Read database credentials to initialise and return a sqlalchemy database engine.
        '''

        cred = self.read_db_creds()
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{cred['RDS_USER']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}:{cred['RDS_PORT']}/{cred['RDS_DATABASE']}")
        return engine   
        
    def list_db_tables(self):

        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = '******'
        DATABASE = 'sales_data'
        PORT = 5432
        local_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        df.to_sql(table_name, local_engine, if_exists='replace')

    
if __name__ == "__main__":
    
    db = DatabaseConnector()
    #engine = db.init_db_engine()
    #print(db.list_db_tables())
    #['legacy_store_details', 'dim_card_details', 'legacy_users', 'orders_table']

    '''
    #Test connection
    with engine.connect() as con:
        print("connection succesful")
    '''
    
        

    
    
    
   
    

    
    
    
    

    

