"""
Setup script for the Python package.
This script uses setuptools to package the project and manage dependencies.
"""

from setuptools import setup, find_packages
from typing import List

def get_requirements() -> List[str]:
    """
    Reads a requirements file and returns a list of requirements.
    """
    try:
        requirement_list:List[str] = []
        with open('requirements.txt', 'r') as file:
            lines = file.readlines()
            for line in lines:
                requirement=line.strip()
                if requirement and requirement!= '-e .':
                    requirement_list.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found. Please ensure it exists.")
    return requirement_list

setup(
    name='aqi-prediction',
    version='0.0.1',
    author='Shivam Gupta',
    author_email='sg4781778@gmail.com',
    packages=find_packages(),
    install_requires=get_requirements()
)