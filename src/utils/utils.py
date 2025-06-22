import os
import certifi
import sys
from pymongo import MongoClient
from dotenv import load_dotenv
from src.exception.exception import CustomException 
from src.logging.logger import logger

class MongoDBUtils:
    def __init__(self):
        try:
            load_dotenv()
            self.mongo_db_url = os.getenv("MONGO_DB_URL")
            self.client = MongoClient(self.mongo_db_url, tls=True, tlsCAFile=certifi.where())
            logger.info("MongoDB client initialized.")
        except Exception as e:
            logger.error("MongoDB connection failed.")
            raise CustomException("MongoDBUtils initialization failed", sys) from e

    def get_collection(self, db_name: str, collection_name: str):
        try:
            db=self.client[db_name]
            collection = db[collection_name]
            logger.info(f"Accessed collection '{collection_name}' in database '{db_name}'.")
            return collection
        except Exception as e:
            logger.error("Failed to get MongoDB collection.")
            raise CustomException("get_collection failed", sys) from e
    
