# SberAutoSubscription
from setuptools import setup, find_packages

setup(
    name="SberAutoSubscription",
    version="1.0.0",
    description="SberAvtopodpiska data analysis project",
    author="Data Science Student",
    python_requires=">=3.8",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.5.0",
        "numpy>=1.23.0",
        "matplotlib>=3.6.0",
        "seaborn>=0.12.0",
        "scikit-learn>=1.1.0",
        "scipy>=1.9.0",
        "flask>=2.0.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)