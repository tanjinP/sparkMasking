# Zero to ETL: ETL existing data into a new data warehouse

Consulting project via Insight to help transition company ([VTS](https://www.vts.com)) that has accumulated large system of records (millions of data points ~50GB) and is looking to be a data driven company by productizing their dataset for analytics.

## Motivation
Company is looking to be data driven, so we found a use case in their current architecture in which we can apply some distributed computing upon and introduce some technologies in this space. 

We agreed upon the use case to mask/scrub sensitive data on the production dataset. The data is scrubbed before being used by application developers, analysts, and business managers within the company.

## Pipeline
![alt text](https://github.com/tanjinP/zero2etl/blob/master/img/pipeline_v1.png "ETL Pipeline")
1. The pipeline starts with a PostgreSQL database (contains live production data as users are updating information on VTS product) and unloads that data in the form of CSVs into S3.
2. CSV files are then consumed by a Spark application along with a configuration file that tells it which particular data (columns) to mask and with what value, this is then output as CSV files into S3.
3. These final CSV files are then loaded into a Redshift database, this is the data warehouse in which all the internal stakeholders can access and operate upon for their daily business (such as using a BI tool like Looker).

Airflow is the scheduler (runs daily) that automates this entire workflow via tasks 1-3
- `python/mask_dag.py` contains complete workflow as well as dependencies, tasks are executed sequentially via bash operators
  - first task is `src/scripts/pg_csv_to_s3.sh`: shell script to copy CSVs locally, upload to S3, then delete from local
  - second task is `src/scripts/mask_data.sh`: shell script which executes `spark-submit` to launch application on cluster
  - final task is `src/python/load_to_redshift.py`: python script to go into S3 and copy CSV files into Redshift data store
