## Project Overview

This project demonstrates a simple end-to-end ETL workflow using BigQuery, Google Cloud Storage (GCS), Python, Docker, and StarRocks.

The process includes:
1. **Extract**: Export data from BigQuery to GCS using `extract.py`.
2. **Transform**: Process and convert the data into the required format using `transform.py`.
3. **Load**: Upload the transformed data back to GCS with `load.py`.
4. **Visualize**: Deploy StarRocks via Docker, query data from GCS as an external table, and visualize it in Superset.

This is a demo project designed to simulate a complete data pipeline for learning and practice purposes.
