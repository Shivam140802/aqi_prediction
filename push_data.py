import os
import sys
from dotenv import load_dotenv
import certifi
import shutil

load_dotenv()

mongo_db_url = os.getenv("MONGO_DB_URL")
ca = certifi.where()

os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY")

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
            raise CustomException(e, sys) from e

    def download_from_kaggle(self, dataset: str, file_name: str, save_path: str):
        try:
            import kaggle
            kaggle.api.authenticate()
            kaggle.api.dataset_download_files(dataset=dataset, path=save_path, unzip=True)
            logger.info(f"Downloaded and extracted {file_name} from Kaggle to {save_path}")
        
        except Exception as e:
            logger.error(f"Error downloading data from Kaggle: {e}")
            raise CustomException(e, sys) from e

    def csv_to_json(self, csv_file_path: str):
        try:
            df = pd.read_csv(csv_file_path, encoding="ISO-8859-1")
            df.columns = df.columns.str.replace('.', '_', regex=False)
            df = df.where(pd.notna(df), None)
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise CustomException(e, sys) from e

    def insert_data_mongodb(self, records, database, collections):
        try:
            db = self.mongo_client[database]
            collection = db[collections]
            collection.insert_many(records)
            logger.info(f"{len(records)} records inserted into MongoDB")
            return len(records)
        except Exception as e:
            logger.error(f"Error inserting data into MongoDB: {e}")
            raise CustomException(e, sys) from e


if __name__ == '__main__':
    DATASET = "rohanrao/air-quality-data-in-india"
    FILE_NAME = "city_day.csv"
    SAVE_PATH = "kaggle_api_data"
    CSV_FILE_PATH = os.path.join(SAVE_PATH, FILE_NAME)
    DATABASE = "ShivamAI"
    COLLECTIONS = "AQI_data"

    aqi_obj = DataExtractor()
    aqi_obj.download_from_kaggle(DATASET, FILE_NAME, SAVE_PATH)
    records = aqi_obj.csv_to_json(csv_file_path=CSV_FILE_PATH)
    no_of_records = aqi_obj.insert_data_mongodb(records, DATABASE, COLLECTIONS)
    print(no_of_records)
    
    try:
        shutil.rmtree(SAVE_PATH)
        print(f"Directory '{SAVE_PATH}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting directory: {e}")