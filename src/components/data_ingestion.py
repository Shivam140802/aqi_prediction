import pandas as pd
import os
import sys
from sklearn.model_selection import train_test_split
from src.exception.exception import CustomException
from src.logging.logger import logger
from src.utils.utils import MongoDBUtils

class LoadAndSaveData:
    def __init__(self):
        try:
            mongo_utils = MongoDBUtils()
            self.collection = mongo_utils.get_collection("ShivamAI", "AQI_data")
        except Exception as e:
            logger.error("Error while connecting to MongoDB.")
            raise CustomException("MongoDB connection failed", sys) from e

    def load_data(self):
        try:
            data = list(self.collection.find())
            if not data:
                logger.warning("No documents found in MongoDB collection.")
            logger.info(f"Loaded {len(data)} records from MongoDB.")
            return pd.DataFrame(data)
        except Exception as e:
            logger.error("Error while loading data from MongoDB.")
            raise CustomException("Data loading failed", sys) from e

    def save_data(self, df, artifact_path="artifact"):
        try:
            os.makedirs(artifact_path, exist_ok=True)
            df = df.drop(columns=['Date', 'Year', 'Month'], errors='ignore')
            train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)
            train_df.to_csv(os.path.join(artifact_path, "train.csv"), index=False)
            test_df.to_csv(os.path.join(artifact_path, "test.csv"), index=False)
            logger.info("Train and test datasets saved to artifact folder.")
        except Exception as e:
            logger.error("Error while saving data to CSV.")
            raise CustomException("Data saving failed", sys) from e

class TransformData:
    def __init__(self, df):
        self.df = df

    def preprocess(self):
        try:
            df = self.df.copy()
            logger.info("Preprocessing started.")

            if '_id' in df.columns:
                df.drop(columns=['_id'], inplace=True)
            df.drop(columns=['AQI_Bucket'], inplace=True, errors='ignore')

            city_counts = df['City'].value_counts()
            threshold = 1000
            minor_cities = city_counts[city_counts < threshold].index
            df['City'] = df['City'].apply(lambda x: 'Other Cities' if x in minor_cities else x)

            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df.sort_values(by=['City', 'Date'], inplace=True)

            def interpolate_city_group(group):
                try:
                    city = group.name 
                    group = group.set_index('Date')
                    numeric_cols = group.select_dtypes(include='number')
                    non_numeric_cols = group.select_dtypes(exclude='number').drop(columns='City', errors='ignore')
                    numeric_filled = numeric_cols.interpolate(method='time', limit_direction='both')
                    filled_group = pd.concat([non_numeric_cols, numeric_filled], axis=1)
                    filled_group['City'] = city
                    return filled_group.reset_index()
                except Exception as e:
                    logger.error("Interpolation failed for a group.")
                    raise CustomException("Interpolation error", sys) from e

            df = df.groupby('City', group_keys=False).apply(interpolate_city_group, include_groups=False).reset_index(drop=True)


            df['Year'] = df['Date'].dt.year
            df['Month'] = df['Date'].dt.month
            numeric_cols = df.select_dtypes(include='number').columns

            # filling missing values with median of the respective city, year, and month
            df[numeric_cols] = df.groupby(['City', 'Year', 'Month'])[numeric_cols].transform(lambda x: x if x.dropna().empty else x.fillna(x.median()))

            # filling missing values with median of the respective city and year
            df[numeric_cols] = df.groupby(['City', 'Year'])[numeric_cols].transform(lambda x: x if x.dropna().empty else x.fillna(x.median()))
            
            # filling missing values with median of the respective city
            df[numeric_cols] = df.groupby(['City'])[numeric_cols].transform(lambda x: x if x.dropna().empty else x.fillna(x.median()))
            
            # filling missing values with median of the entire dataset
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

            logger.info("Preprocessing completed.")
            return df
        except Exception as e:
            logger.error("Preprocessing failed.")
            raise CustomException("Preprocessing error", sys) from e

def main():
    try:
        loader = LoadAndSaveData()
        raw_df = loader.load_data()
        transformer = TransformData(raw_df)
        processed_df = transformer.preprocess()
        loader.save_data(processed_df)
        logger.info("ETL pipeline executed successfully.")
    except Exception as e:
        logger.critical("ETL pipeline failed.")
        raise CustomException("Pipeline failed", sys) from e

if __name__ == "__main__":
    main()
