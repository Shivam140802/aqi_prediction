import os
import sys
import json
from dotenv import load_dotenv
import certifi

load_dotenv()  

mongo_db_url = os.getenv("MONGO_DB_URL")
ca = certifi.where()

import pandas as pd
import pymongo
from src.exception.exception import CustomException
from src.logging.logger import logger

class DataExtractor:
    def __init__(self):
        try:
            self.mongo_client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)
        except Exception as e:  
            logger.error(f"Error connecting to MongoDB: {e}")
            raise CustomException(e, sys)

    def csv_to_json(self, csv_file_path: str):
        try:
            df = pd.read_csv(csv_file_path)
            df.columns = df.columns.str.replace('.', '_', regex=False)
            df = df.where(pd.notna(df), None)
            json_data = df.to_dict(orient='records')
            return json_data
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise CustomException(e, sys)
    
    def insert_data_mongodb(self, records, database, collections):
        try:
            db = self.mongo_client[database]
            collection = db[collections]
            collection.insert_many(records)
            logger.info(f"{len(records)} records inserted into MongoDB")
            return len(records)
        except Exception as e:
            logger.error(f"Error inserting data into MongoDB: {e}")
            raise CustomException(e, sys)

if __name__ == '__main__':
    FILE_PATH = "data/city_day.csv"  
    DATABASE = "ShivamAI"
    COLLECTIONS = "AQI_data"  

    aqi_obj = DataExtractor()
    records = aqi_obj.csv_to_json(csv_file_path=FILE_PATH) 
    no_of_records = aqi_obj.insert_data_mongodb(records, DATABASE, COLLECTIONS)
    print(no_of_records)
