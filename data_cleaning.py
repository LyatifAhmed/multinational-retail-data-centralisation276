import pandas as pd
import numpy as np

from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning:
    '''
    to clean data from each of the data sources.
    '''
    def __init__(self):

        pass

    def clean_user_data(self,legacy_users_table):
       legacy_users_table.replace('NULL', np.NaN, inplace=True)
       legacy_users_table.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)

       legacy_users_table['date_of_birth'] = pd.to_datetime(legacy_users_table['date_of_birth'], errors = 'coerce')
       legacy_users_table['join_date'] = pd.to_datetime(legacy_users_table['join_date'], errors ='coerce')
       legacy_users_table = legacy_users_table.dropna(subset=['join_date'])

       legacy_users_table['phone_number'] = legacy_users_table['phone_number'].str.replace('/W', '')
       legacy_users_table = legacy_users_table.drop_duplicates(subset=['email_address'])
        
       legacy_users_table.drop(legacy_users_table.columns[0], axis=1, inplace=True)
       legacy_users_table.to_csv("users.csv")
       return legacy_users_table
        
    def clean_card_data(self):
        #extract pdf data using link from method in data extractor
        df = DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        df['card_number'] = df['card_number'].apply(str)#converts card_number column to type string
        df['card_number'] = df['card_number'].str.replace('?','')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.replace('NULL', np.nan).dropna()
        df.to_csv("card_details.csv")
        return df

    def clean_store_data(self, store_data):

        store_data = store_data.reset_index(drop=True)
        store_data.drop(columns='lat',inplace=True)
        store_data.replace('NULL', np.NaN, inplace=True)
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors ='coerce')
        store_data['staff_numbers'] = store_data['staff_numbers'].str.replace(r'\D', '')# replaces special characters in staff numbers column with empty space
        store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors='coerce')
        store_data.dropna(subset=['staff_numbers'], axis=0, inplace=True)

        store_data['continent'] = store_data['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica', 'America')
        return store_data
    
    def convert_product_weights(self,x):
        '''
        isnull(x) and isinstnce(),check if x is null or x is string type. 
        '''
        if pd.isnull(x) or not isinstance(x, str):
         return np.nan
        # Normalize the input (convert to lowercase and strip whitespace)
        x = x.lower().strip().replace(' .', '').strip()
        if 'kg' in x:
            x = x.replace('kg', '')
            x = float(x)         
        elif 'ml' in x:
            x = x.replace('ml', '')
            x = float(x)/1000
        elif 'g' in x:
            x = x.replace('g', '')
            x = float(x)/1000  
        elif 'lb' in x:
            x = x.replace('lb', '')
            x = float(x) * 0.453592  # Convert lb to kg

        elif 'oz' in x:
            x = x.replace('oz', '').strip()
            x = float(x) * 0.0283495  # Convert oz to kg        
        else:
            x= np.nan
        return x
    
    
    def  clean_products_data(self, df_product):
        # Reset index of DataFrame and Intitial cleaning of DataSet
        df_product = df_product.reset_index(drop=True)
        df_product.drop_duplicates()
        df_product.replace('NULL', np.NaN, inplace=True)
        df_product['date_added'] = pd.to_datetime(df_product['date_added'],errors='coerce')
        df_product.dropna(subset=['date_added'],how = 'any', axis=0,inplace=True)

        # Working with weight column, it has values like [.,12X100,9chd005hg]
        # Removing 'X' multiplication sign
        wt_value_x = df_product.loc[df_product.weight.str.contains('x'),'weight'].str.split('x',expand=True)
        numeric_cols = wt_value_x.apply(lambda x: pd.to_numeric(x.str.extract(r'(\d+\.?\d*)', expand=False)), axis=1)
        final_weight = numeric_cols.prod(axis=1)
        df_product.loc[df_product.weight.str.contains('x'),'weight'] = final_weight
        to_lower_unit = lambda value:str(value).lower().strip()

        df_product['weight'] = df_product['weight'].apply(to_lower_unit) 
        df_product['weight'] = df_product['weight'].apply(lambda x: self.convert_product_weights(x))
        
        # Droping the unnamed column, containing duplicate index column values
        df_product.drop(df_product.columns[0], axis=1, inplace=True)  
        return df_product
    
    def clean_orders_data(self, data):
        data.drop("level_0", axis=1, inplace=True) 
        data.drop("1", axis=1, inplace=True) 
        data.drop(data.columns[0], axis=1, inplace=True)
        data.drop('first_name', axis=1, inplace=True)
        data.drop('last_name', axis=1, inplace=True)
        data = data.drop_duplicates('user_uuid')
        data.reset_index(drop=True)
        return data
    
    def clean_date_time(self, date_data):
        date_data['day'] = pd.to_numeric(date_data['day'], errors='coerce')
        date_data.dropna(subset=['day', 'year', 'month'], inplace=True)#drops any rown which contain null values in the following columns
        date_data['timestamp'] = pd.to_datetime(date_data['timestamp'], format='%H:%M:%S', errors='coerce')# timestamp in form hour minute and seconds
        return date_data

    

        

        




    

if __name__ == "__main__":
    
    cleaning = DataCleaning()
    extractor = DataExtractor()
    connector = DatabaseConnector()
    '''
    # User Data
    legacy_users_table = extractor.read_rds_table('legacy_users')
    clean_data_user = cleaning.clean_user_data(legacy_users_table)
    clean_data_user.info()
    print(clean_data_user)
    '''
    
    #Save cleaned data to a csv file
    #clean_data_user.to_csv('users.csv')
    #connector.upload_to_db(clean_data_user, 'dim_users')
    
    '''
    # Card Data
    
    #cleaned_card_data = cleaning.clean_card_data()
    #connector.upload_to_db(cleaned_card_data, 'dim_card_details')
    '''

    #Store Data
    # API and API key
    endpoint_store_details = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/" 
    endpoint_store_count = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    }
    '''
    # #print("  check this ",type(endpoint_store_count))
    stores_number = extractor.list_number_of_stores(endpoint_store_count,headers)
    # #print("end points called total_stores  ")
    stores_data = extractor.retrive_stores_data(stores_number, endpoint_store_details,headers)
    
    
    #stores_data.to_csv('store_outputs.csv')
    # #clean_store_table get the cleaned store details from clean_store_data()
    clean_stores_data = cleaning.clean_store_data(stores_data)
    clean_stores_data.info()
    
    #connector.upload_to_db(clean_stores_data, 'dim_store_details')
    '''
    '''
    # product data
    s3_address = 's3://data-handling-public/products.csv'
    product_df = extractor.extract_from_s3(s3_address)
    df = cleaning.clean_products_data(product_df)
    df.to_csv('orders.csv')
    connector.upload_to_db(df,'dim_products')
    '''
    '''
    # orders table
    list = connector.list_db_tables()
    print(list)
    orders_table = extractor.read_rds_table('orders_table')
    clean_orders_table = cleaning.clean_orders_data(orders_table)
    clean_orders_table.info()
    #orders_table.to_csv('orders_table.csv')
    #connector.upload_to_db(clean_orders_table, 'orders_table')
    '''
    
    '''
    # Date Data
    date_data = extractor.extract_from_s3_json("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
    clean_date_data_table = cleaning.clean_date_time(date_data)
    clean_date_data_table.to_csv('date_data.csv')
    print(clean_date_data_table.info())
    connector.upload_to_db(clean_date_data_table,'dim_date_times')
    '''
    

    
    