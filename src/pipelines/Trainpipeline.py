import pandas as pd
from catboost import CatBoostRegressor
from sklearn.metrics import r2_score
from src.logging.logger import logger
from src.exception.exception import CustomException
import joblib
import os
import sys

class TrainPipeline:
    def __init__(self, train_path: str, test_path: str):
        self.train_path=train_path
        self.test_path=test_path
        self.model = None

    def load_data(self):
        try:
            train=pd.read_csv(self.train_path)
            test=pd.read_csv(self.test_path)
            X_train=train.drop('AQI', axis=1)
            y_train=train['AQI']
            X_test = test.drop('AQI', axis=1)
            y_test = test['AQI']
            return X_train,y_train, X_test, y_test
        except Exception as e:
            logger.error("Error loading data for training.")
            raise CustomException("Error in loading training data", sys) from e

    def train_model(self, X_train, y_train, X_test, y_test):
        try:
            cat_cols = X_train.select_dtypes(include=['object', 'category']).columns.tolist()
            self.model = CatBoostRegressor(verbose=0,learning_rate=0.1,depth=8,iterations=300)
            self.model.fit(X_train, y_train, cat_features=cat_cols)
            y_pred = self.model.predict(X_test)
            r2 = r2_score(y_test, y_pred)
            print(f"R² score (test): {r2:.4f}")
            logger.info("Model training completed.")
        except Exception as e:
            logger.error("Error during model training.")
            raise CustomException("Error in model training", sys) from e

    def save_model(self, output_path='artifact/model.pkl'):
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            joblib.dump(self.model, output_path)
            logger.info(f"Model saved to {output_path}")
        except Exception as e:
            logger.error("Error saving the trained model.")
            raise CustomException("Error saving model", sys) from e

    def run(self):
        try:
            logger.info("Starting training pipeline...")
            X_train, y_train, X_test, y_test=self.load_data()
            self.train_model(X_train, y_train, X_test, y_test)
            self.save_model()
            logger.info("Training pipeline finished successfully.")
        except Exception as e:
            logger.critical("Training pipeline failed.")
            raise CustomException("Training pipeline failed", sys) from e
