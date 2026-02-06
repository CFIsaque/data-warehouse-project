# Data Warehouse Project
This project demonstrates a comprehensive Data Warehouse with BigQuery, including ETL processes, data modeling and analytics.

---

## Data Architecture

The data architecture for this project follows Medallion Architecture Bronze, Silver and Gold layers:
<img width="1101" height="754" alt="image" src="https://github.com/user-attachments/assets/decb60cd-e143-46e8-9a5f-18daffa01efb" />


1. **Bronze Layer**: Stores raw data from on-premise CRM and ERP system. Data is ingested from CSV Files into Bigquery Database.
2. **Silver Layer**: This layer includes data cleansing, standardization and normalization process to prepare data for analysis.
3. **Gold Layer**: Receive business-ready data modeled into a star schema required for reporting and analytics.
---

## Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming and loading data from source systems into the warehouse.
<img width="1170" height="770" alt="image" src="https://github.com/user-attachments/assets/4b7f0579-d600-42e6-ad59-8ae53638c78a" />

3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries. The modeling relationship used is **many to one**. 
<img width="1074" height="601" alt="image" src="https://github.com/user-attachments/assets/c1943741-298c-4fb1-9fb0-2fb7fc597cfc" />

---

## License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.
