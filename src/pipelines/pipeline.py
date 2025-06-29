from src.pipelines.ETLpipeline import DataLoader, DataTransformer, DataSaver
from src.pipelines.Trainpipeline import TrainPipeline
from src.logging.logger import logger
from src.exception.exception import CustomException
import sys


def run_etl():
    try:
        logger.info("Starting ETL pipeline...")
        loader=DataLoader()
        raw_df=loader.load()

        transformer = DataTransformer(raw_df)
        processed_df = transformer.transform()

        saver=DataSaver()
        saver.save(processed_df)
        logger.info("ETL pipeline completed.")
    except Exception as e:
        logger.critical("ETL pipeline failed.")
        raise CustomException("ETL failed", sys) from e


def run_training():
    try:
        logger.info("Starting Training pipeline...")
        trainer=TrainPipeline("artifacts/train.csv","artifacts/test.csv")
        trainer.run()
        logger.info("Training pipeline completed.")
    except Exception as e:
        logger.critical("Training pipeline failed.")
        raise CustomException("Training failed", sys) from e


def main():
    run_etl()
    run_training()


if __name__ == "__main__":
    main()
