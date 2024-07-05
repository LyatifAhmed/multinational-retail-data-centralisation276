import pandas as pd
import tabula as tb
import requests
import json
import boto3
import io

from database_utils import DatabaseConnector

class DataExtractor:
    '''This class will work as a utility class, It has methods that help extract data from different data sources.
    These sources will include CSV files, an API and an S3 bucket.
    '''
    def __init__(self):
        pass

    def read_rds_table(self, table_name):
        '''
        Creates connection using an sqlalchemy database engine to extract and return a dataframe for a table from an AWS RDS database.
        '''
        db_connector = DatabaseConnector()
        source_engine = db_connector.init_db_engine()
        data = pd.read_sql_table(table_name, source_engine)
        return data
    
    def retrieve_pdf_data(self, pdf_path):
        
        df_pdf = tb.read_pdf(pdf_path, multiple_tables=True , pages = "all")
        #Getting the dataframe of df_pdf which is a list type.
        df_pdf = pd.concat(df_pdf)
        return df_pdf
    
    def list_number_of_stores(self, endpoint, apikey):
        
        response = requests.get(endpoint, headers=apikey) #reponse str type
        api_data = response.json() #return a dic
        number_stores = api_data['number_stores']
        return number_stores
        
    
    def retrive_stores_data(self, stores_num, endpoint, apikey):
        # declaring an empty list variable 'stores_data'
        stores_data = []
        for store_number in range(1, stores_num):
            #print("stores_num is ",store_number)
            response = requests.get(f'{endpoint}{store_number}', headers=apikey)
            
            store_content_list = response.json()  #return dic
            stores_data.append(store_content_list)
        df_stores_data = pd.DataFrame(stores_data)
        #print("Retrieved stores df:---->\n",df_stores_data)
        return df_stores_data
    
    def extract_from_s3(self, s3_address):
        # Initialize boto3 S3 resource
        s3 = boto3.resource('s3')

        # Remove any protocol prefix from the s3_address
        
        s3_address = s3_address.replace('s3://', '')
        
        
        # Split the remaining address into bucket_name and file_key
        bucket_name, file_key = s3_address.split('/',1)
        print('bucket name ' ,bucket_name )
        print('file_key ' ,file_key )
        # Create an S3 Object instance
        '''
        obj = s3.Object(bucket_name, file_key) initializes an S3 object instance but does not actually retrieve any data from S3. 
        Instead, it creates a high-level Object resource that represents the S3 object located at the 
        specified bucket_name and file_key.
        '''
        obj = s3.Object(bucket_name, file_key)
        
        # Get the content stream from the S3 object
        '''
        obj.get() performs a GET request to the S3 service to fetch the object's data and metadata.
        The get() method returns a dictionary containing several key-value pairs, including 'Body', which contains the actual content of the file in the form of a StreamingBody object.
        obj.get()['Body'] extracts the 'Body' from this dictionary, which is a stream of the file's contents.
        '''
        body = obj.get()['Body']
        df = pd.read_csv(body)
        df = df.reset_index(drop=True) 
        df.to_csv("products.csv") 
        return df
    
    def extract_from_s3_json(self, json_link):

        s3 = boto3.resource('s3')
        json_link = json_link.replace('https://', '')
        bucket_name, file_key = json_link.split('/', 1)
        if '.' in bucket_name:

            bucket_name = bucket_name.split('.')[0]

        obj = s3.Object(bucket_name, file_key)
        body = obj.get()['Body']
        date_data = pd.read_json(body)
        date_data = date_data.reset_index(drop=True)
        date_data.to_csv("date_data.csv") 
        return date_data



        
        
        
       


        

        
        
        
       
        
        
        

        
            


        






if __name__ == "__main__":
    extractor = DataExtractor()
    '''
    db = DatabaseConnector()
    print(db.list_db_tables())
    '''
    #print(extractor.read_rds_table('legacy_users'))
    '''pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    print(extractor.retrieve_pdf_data(pdf_link))
    '''
    '''
    endpoint_store_details = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/" 
    endpoint_store_count = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    }
    stores_number = extractor.list_number_of_stores(endpoint_store_count, headers)
    
    stores_data = extractor.retrive_stores_data(stores_number, endpoint_store_details, headers)
    print(stores_data)'''
    '''
    s3_address = 's3://data-handling-public/products.csv'
    product_df = extractor.extract_from_s3(s3_address)
    print(product_df)
    '''
    
    s3_address = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_data = extractor.extract_from_s3_json(s3_address)
    print(date_data)
    

    
    
     
    
    