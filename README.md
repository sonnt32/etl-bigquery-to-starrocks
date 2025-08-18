# Project Overview

This project demonstrates a simple end-to-end ETL workflow using BigQuery, Google Cloud Storage (GCS), Python, Docker, and StarRocks.

The process includes:
1. **Extract**: Export data from BigQuery to GCS using `extract.py`.
2. **Transform**: Process and convert the data into the required format using `transform.py`.
3. **Load**: Upload the transformed data back to GCS with `load.py`.
4. **Visualize**: Deploy StarRocks via Docker, query data from GCS as an external table, and visualize it in Superset.

This is a demo project designed to simulate a complete data pipeline for learning and practice purposes.

# ETL Workflow Setup

This project demonstrates an end-to-end ETL pipeline leveraging **Dockerized StarRocks**, **Superset**, and **Google Cloud Platform** services.  

## 1. Environment Setup  
- StarRocks was deployed using Docker with the configuration file `starrocks-docker.txt`.  
- Superset was also deployed in a similar manner to enable data exploration and visualization.  

## 2. Service Account Permissions  
A dedicated service account was created with the following roles to ensure smooth integration between GCP services and the ETL process:  
- **BigQuery User**  
- **BigQuery Data Viewer**  
- **Storage Admin**  

## 3. ETL Process  
- **Extract**: The script `extract.py` uses **Pandas** to export data from **BigQuery** into **Google Cloud Storage (GCS)**.  
- **Transform**: The script `transform.py` applies data transformation and restructuring logic with Pandas.  
  - The intermediate results are temporarily stored in a local directory.  
- **Load**: The transformed data is then uploaded back to **GCS**, specifically into the `transform` directory.  

## 4. Integration with StarRocks  
StarRocks connects to the transformed data in GCS using **External Tables**, allowing for efficient querying and further analytical processing.  
