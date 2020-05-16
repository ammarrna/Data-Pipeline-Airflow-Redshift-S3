## Project Summary 
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.Currently they have data in JSON files but its diffcult for data analysts to work on JSON files.Sparkify wants to move there data into a data warehouse(Redshift).They want to build a ETL pipeline using some visual tool and we have selected Apache Airflow for that because Airflow has options to develop DAG based flows.We will be builing a single flow in Airflow to load staging data from s3 to redshift staging tables and then load that data from staging to Fact and dimensions.

## Summary of Sparkify Data pipeline build in Airflow
Our pipeline mainly consist of following three operations
1. Load data from s3 to redshift that will include loading both songs data and events data.
2. Load Facts and dimensions from Staging tables via provided sql queries.
3. Perform Data Quality Checks on final Facts and dimensions.

## Source Data Location
The log data is located at s3://udacity-dend/log_data and the song data is located in s3://udacity-dend/song_data

## Airflow Project Details
Project template include single DAG file and four Operators and details of each of them is as below.

### DAG File Details(Load_Data_Dag)
Please see the screenshot below for details of main dag of project.Dag file is located at airflow/dags folder
![Dag Details](MainDAG.png)

### Operator Details

Operators are placed in airflow/plugins/operators.Details of Each Operator is as below.

### StageToRedshiftOperator

1. File name for this operator is stage_redshift.py
2. This operator is used to copy data from s3 files to redshift table using copy command.
3. S3 path,redshift connection details and data format of file is passed as parameters to this operator.
4. In our project we have used this operator two times in our main dag in "Stage_events" and "Stage_songs".

### LoadDimensionOperator and LoadFactOperator

1. File name for these two operators are load_dimension.py and load_fact.py.
2. This operator is used load facts and dimensions using the sql queries inside sql_queries file.
3. Redshift connection details,tableName and sql query used to load data is passed as parameters.
4. One more parameter used is AppendMode it is used to decide that we will be inserting data in append mode or in delete and load Mode.

4. In our project we have used this operator to load songplay fact and song,Artist,time and user dimensions.

### DataQualityOperator

1. File name for this operator is data_quality.py
2. This operator is used to check data exists in tables or not.
3. Redshift connection details and Table name is passed as parameters.
4. In our project we have used this operator to test whether data exists in Facts and dimension or not.

## Execution instructions
### Prerequsite
You need to have redshift instance in AWS. You need to have Airflow on the system on which you are running the scripts

### Steps to execute
1. Open terminal and go to the project folder.
2. Run Airflow start command on terminal as below
     /opt/airflow/start.sh
3. Run create_tables.sql on redshift provided in the project.
4. Access Airflow interface and create connections to AWS using keys with name aws_credentials and connection to redshift with name redshift.
5. Trigger Main DAG and wait for its completion to load data to all tables 



## SQl Files inside Repo
1. sql_queries.py contains all the queries that we will be running in our dag to load facts and dimensions.
2. create_tables.py this file will drop the existing tables and then create the new ones.
