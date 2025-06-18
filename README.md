# AQI Prediction using Machine Learning

This project focuses on predicting the **Air Quality Index (AQI)** using historical air pollutant data collected from various Indian cities. It leverages machine learning models to estimate AQI based on features like PM2.5, NO2, CO, SO2, etc.

## Project Structure

aqi-prediction/
    .github\workflow\main.yml # For deployment
    data/ # Raw and cleaned datasets
    notebooks/ # Jupyter notebooks for EDA, training
    src/ # source code
        cloud/  # Cloud storage handlers 
        components/  # Model training and evaluation
        constants/  #static constants(path/keys)
        entity/   # Custom classes
        exception/ # Custom error handler
        logging/  # Contain logs
        pipeline/ # For prediction
        utils/  # Utility functions
    .env # For environment varibales
    .gitignore # Contains files to be ignored by git during commit 
    Dockerfiles # For docker image
    README.md # This file
    requirements.txt # Project dependencies
    setup.py # For packaging(optional)

## Features Used

- PM2.5
- PM10
- NO, NO2, NOx
- NH3, CO, SO2, O3
- Benzene, Toluene, Xylene
- Date & City (processed for time-based patterns)

## Installation

```bash
git clone https://github.com/Shivam140802/aqi_prediction.git
cd aqi_prediction
pip install -r requirements.txt