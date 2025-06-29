"""
Setup script for the AQI Prediction project.
"""

from setuptools import setup, find_packages
from typing import List

def get_requirements(filename: str = "requirements.txt") -> List[str]:
    """
    Reads the requirements file and returns a list of dependencies.
    """
    try:
        with open(filename, "r") as file:
            return [
                line.strip()
                for line in file.readlines()
                if line.strip() and not line.startswith("-e")
            ]
    except FileNotFoundError:
        print("requirements.txt not found.")
        return []

setup(
    name="aqi-prediction",
    version="0.0.1",
    author="Shivam Gupta",
    author_email="sg4781778@gmail.com",
    description="AQI Prediction using CatBoost and Flask",
    packages=find_packages(),
    install_requires=get_requirements(),
    include_package_data=True,
    python_requires=">=3.11",
)