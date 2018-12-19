# Airflow Salesforce Operators

This script allows Airflow to iterate with Salesforce. Currently are supported:

1. Copy Salesforce Objects into csv on Amazon S3 based on a SOQL query,
2. Insert, Updtade or Delete Salesforce Object Records based on a csv file on Amazon S3

## Installing
Copy python file to Airflow Plugins folder. Your Airflow instance must have ```simple-salesforce``` package installed.

## Connections
The connection must have the following parameters:
```
Conn Type: HTTP
Host: Your Salesforce URL Instance
Login: Your Email Login
Passord: Password
Extra: {"security_token": "your-security-token"}
```

## Example
The DAG example bellow dumps all current Id records from Salesforce Account Object:
```
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.salesforce_utils import SalesforceToS3Operator

dag = DAG(
    dag_id="dump_account",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2018, 12, 11, 17, 0),
)

ds = "{{ ds }}"
with dag:
    unload_account = SalesforceToS3Operator(
        task_id="unload_account",
        sql="select Id from Account",
        dest_key=f"airflow/reports/unload_account_{ds}",
        dest_bucket="aws",
        salesforce_conn_id="salesforce",
        aws_conn_id="aws",
        include_deleted=False,
    )

    unload_account

```
