# real_time_user_data_ingestion
🚀 Real-Time User Data Pipeline with Airflow

This repository contains an ETL pipeline built with Apache Airflow to extract, process, and store user data from an external API into a PostgreSQL database. The pipeline includes:

API Availability Check: Ensures the API is responsive before ingestion.

Data Extraction: Retrieves user data via an HTTP request.

Data Processing: Normalizes and stores data in a CSV file.

Database Ingestion: Loads processed data into PostgreSQL.

Automation: Scheduled to run daily for real-time updates.

Tech Stack:
Apache Airflow 🌀
PostgreSQL 🗄️
Python (Pandas, JSON, Requests) 🐍
