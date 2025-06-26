import os
import sys
import pandas as pd
from src.exception.exception import CustomException
from src.logging.logger import logger
from src.utils.utils import MongoDBUtils
from sklearn.model_selection import train_test_split

class DataLoader:
    def __init__(self):
        try:
            mongo_utils=MongoDBUtils()
            self.collection=mongo_utils.get_collection("ShivamAI", "AQI_data")
        except Exception as e:
            logger.error("Error while connecting to MongoDB.")
            raise CustomException("MongoDB connection failed", sys) from e

    def load(self):
        try:
            data=list(self.collection.find())
            if not data:
                logger.warning("No documents found in MongoDB collection.")
            logger.info(f"Loaded {len(data)} records from MongoDB.")
            return pd.DataFrame(data)
        except Exception as e:
            logger.error("Error while loading data from MongoDB.")
            raise CustomException("Data loading failed", sys) from e


import pandas as pd
import sys
from src.exception.exception import CustomException
from src.logging.logger import logger

class DataTransformer:
    def __init__(self, df):
        self.df = df

    def transform(self):
        try:
            df = self.df.copy()
            logger.info("Preprocessing started.")

            # Drop unwanted columns
            if '_id' in df.columns:
                df.drop(columns=['_id'], inplace=True)
            df.drop(columns=['AQI_Bucket'], inplace=True, errors='ignore')

            # Replace minor cities with "Other Cities"
            city_counts = df['City'].value_counts()
            threshold = 1000
            minor_cities = city_counts[city_counts < threshold].index
            df['City'] = df['City'].apply(lambda x: 'Other Cities' if x in minor_cities else x)

            # Convert Date to datetime and sort
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df.sort_values(by=['City', 'Date'], inplace=True)

            # Interpolation grouped by City
            def interpolate_city_group(group):
                try:
                    city=group.name
                    group = group.set_index('Date')
                    group = group.sort_index()
                    numeric_cols = group.select_dtypes(include='number')
                    non_numeric_cols = group.select_dtypes(exclude='number').drop(columns='City', errors='ignore')
                    numeric_filled = numeric_cols.interpolate(method='time', limit_direction='both')
                    non_numeric_filled = non_numeric_cols.ffill().bfill()
                    filled_group = pd.concat([non_numeric_filled, numeric_filled], axis=1)
                    filled_group['City'] = city
                    return filled_group.reset_index()
                except Exception as e:
                    logger.error("Interpolation failed for a group.")
                    raise CustomException("Interpolation error", sys) from e

            df['City'] = df['City'].ffill().bfill()
            df = df.groupby('City', group_keys=False).apply(interpolate_city_group, include_groups=False).reset_index(drop=True)

            # Extract Year and Month
            df['Year'] = df['Date'].dt.year
            df['Month'] = df['Date'].dt.month
            numeric_cols = df.select_dtypes(include='number').columns

            # Filling missing values with median [(city, year, month), (city, year), (city) ,(overall)]
            df[numeric_cols] = df.groupby(['City', 'Year', 'Month'])[numeric_cols].transform(lambda x: x.fillna(x.median()) if not x.dropna().empty else x)
            df[numeric_cols] = df.groupby(['City', 'Year'])[numeric_cols].transform(lambda x: x.fillna(x.median()) if not x.dropna().empty else x)
            df[numeric_cols] = df.groupby(['City'])[numeric_cols].transform(lambda x: x.fillna(x.median()) if not x.dropna().empty else x)
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

            # Downsample "Other Cities"
            def downsample_city(df_city, target_count):
                df_city = df_city.sort_values('Date')
                total = len(df_city)
                if total <= target_count:
                    return df_city
                step = total / target_count
                indices = [int(i * step) for i in range(target_count)]
                return df_city.iloc[indices]

            other_cities_df = df[df['City'] == 'Other Cities']
            rest_df = df[df['City'] != 'Other Cities']
            downsampled_other = downsample_city(other_cities_df, 2000)

            # Concatenate and sort again
            df = pd.concat([rest_df, downsampled_other], ignore_index=True)
            df = df.sort_values(by=['City', 'Date']).reset_index(drop=True)

            # Drop Year and Month columns
            df.drop(columns=['Date', 'Year', 'Month', 'YearMonth'], inplace=True, errors='ignore')
            
            logger.info("Preprocessing completed.")
            return df

        except Exception as e:
            logger.error("Preprocessing failed.")
            raise CustomException("Preprocessing error", sys) from e


class DataSaver:
    def __init__(self, artifact_path="artifact"):
        self.artifact_path = artifact_path
        os.makedirs(artifact_path, exist_ok=True)

    def save(self, df):
        try:
            df = df.drop(columns=['Date', 'Year', 'Month'], errors='ignore')
            train, test = train_test_split(df, test_size=0.2, random_state=42)
            train.to_csv(os.path.join(self.artifact_path, "train.csv"), index=False)
            test.to_csv(os.path.join(self.artifact_path, "test.csv"), index=False)

            logger.info("Train and test datasets saved successfully.")
        except Exception as e:
            logger.error("Error while saving data.")
            raise CustomException("Saving error", sys) from e
