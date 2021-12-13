## Data-Pipelines-with-Airflow

This project aims to introduce more automation and monitoring of ETL pipelines to Sparkify music streaming application Data Warehouse using Apache Airflow.

Of importance is ensuring data pipelines are dynamic and built from reusable tasks,can be monitored and allows easy backfill.

## Approach

To complete the project I had to build own custom operators for creating tables in redshift, staging the data, loading dimension and fact data and finally perfoming data quality checks.

## Data Source

The data for the project resides in S3 and consists of JSON logs and JSON metadata

<ol>
  <li>s3://udacity-dend/log_data</li>
  <li>s3://udacity-dend/song_data</li>
 </ol>
 
 The data exists in region **us-west-2**
 

 ## How to run project
 
   <ol>
    <li>In directory airflow/workspace, run the command /opt/airflow/start.sh to launch Airflow </li>
    <li>Create an IAM role in AWS Management console and save or copy credentials </li>
    <li>Under Admin tab in Airflow add a connection with name aws_credentials and pass the credentials from above to login and passowrd fields </li>
    <li>Create a redshift cluster </li>
    <li>Add connection to airflow and pass cluster connection to host field, then password to password field</li>
    <li>turn the dag on and trigger it to run</li>
   </lo>
 

## Data Quality Check

To confirm excution of dag and loading of data into the their disparate tables run a "select count(*) from {replace_with_table_you_wish_to_check}"

More than zero records indicate the dag run to completion without failure

## Reference 

-[Reference #1](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/operators/postgres_operator_howto_guide.html): **How-to Guide for PostgresOperator**



 
