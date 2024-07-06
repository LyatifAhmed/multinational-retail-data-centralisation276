# multinational-retail-data-centralisation276
## Overview

This project was developed as part of the scenario-based assignments provided by Ai Core, contributing to the Data Engineer Career Accelerator program.

## Project Goals

- Centralize Sales Data: Integrate sales data from various sources into a single, unified database.
- Improve Data Accessibility: Ensure that the centralized data is easily accessible to all team members.
- Enhance Data Analysis: Provide a clean and structured data set that can be used for advanced analytics and reporting.
- Support Data-Driven Decisions: Enable the company to make informed decisions based on comprehensive and accurate data.

## Features

- Data Extraction: Collect sales data from multiple sources, including S3 buckets, databases, and API endpoints.
- Data Cleaning: Standardize and clean the data to ensure consistency and accuracy.
- Data Transformation: Convert the data into a format that is suitable for analysis, including handling various units of measurement.
- Data Loading: Store the cleaned and transformed data in a database.

## Components:

Data Extraction

- S3 Integration: Download and extract data from S3 buckets using the boto3 library.
- Database Connections: Connect to various databases to extract sales data.
- API Integration: Retrieve data from external APIs.

Data Cleaning

- Standardize Units: Convert all weight measurements to a standard unit (e.g., kilograms).
- Handle Missing Values: Identify and appropriately handle missing or invalid data.
- Data Validation: Ensure the data meets predefined quality standards.

Data Transformation

- Data Conversion: Convert data into a structured format suitable for loading into the database.
- Data Normalization: Normalize data to eliminate redundancies and improve integrity.
Data Loading

- Database Storage: Load the cleaned and transformed data into a database.

## Getting Started
```
gh repo clone LyatifAhmed/multinational-retail-data-centralisation276
```

```bash
.
├── database_utils.py
├── data_cleaning.py
├── data_extraction.py
├── db_creds_local.yaml
├── db_creds.yaml
├── __pycache__
│   ├── database_utils.cpython-311.pyc
│   ├── data_cleaning.cpython-311.pyc
│   └── data_extraction.cpython-311.pyc
└── README.md
```

## Prerequisites

- Python 3.8 or higher
- boto3 library for AWS S3 integration
- pandas library for data manipulation
- Database connector libraries (e.g., psycopg2 for PostgreSQL)