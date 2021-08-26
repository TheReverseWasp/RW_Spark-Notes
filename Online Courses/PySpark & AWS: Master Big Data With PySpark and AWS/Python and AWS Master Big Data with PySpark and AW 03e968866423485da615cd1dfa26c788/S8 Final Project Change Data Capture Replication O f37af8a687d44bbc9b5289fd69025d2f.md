# S8: Final Project: Change Data Capture / Replication On Going

![Untitled](S8%20Final%20Project%20Change%20Data%20Capture%20Replication%20O%20f37af8a687d44bbc9b5289fd69025d2f/Untitled.png)

![Untitled](S8%20Final%20Project%20Change%20Data%20Capture%20Replication%20O%20f37af8a687d44bbc9b5289fd69025d2f/Untitled%201.png)

# Create RDS MySQL Instance

- Create new parameter group in RDS console
    - musql8.0
    - RdsMySqlPG → Group name
    - give a description
    - Parameters

    ![Untitled](S8%20Final%20Project%20Change%20Data%20Capture%20Replication%20O%20f37af8a687d44bbc9b5289fd69025d2f/Untitled%202.png)

## Now create RDS

- Standard Create.
- MySQL Engine.
- Free Tier Template.
- Credential Settings.
    - master username: admin.
    - password: any you want.
- DB instance type: db.t2.micro.
- Storage:
    - Disable Autoscaling.
- Connectivity.
    - Public Access.
    - No preferences in availability zone.
- DB Authentication.
    - Password.
- Backup.
    - Enable automatic backup 7 days.
    - Disable monitoring.
- DB Options.
    - select the parameter group you create.
- Maintenance.
    - Disable auto upgrade.

# Creating S3 Bucket

- Search for S3 in AWS.
- Give a bucket name (unique).
- Unblock all public access (just for this project).

# Creating DMS Source Endpoint

- Search for DMS in AWS.
- Create an endpoint.
- Select RDS DB Instance.
    - Select the MySQL database.
- Provide access information manually.
- Copy and paste user and password.
- Test the endpoint before creation.
- Create it.

# Creating DMS Destination Endpoint

- Search for DMS in AWS.
- Select Endpoint.
- Refer it as a target.
- Give it a name.
- Target engine: S3.

## Create an IAM Role

- Search for IAM in AWS.
- Create a role.
- Search for DMS.
- Search for S3fullaccess.
- Give the role a name.
- Give the role a description.

- Copy the role ARN and paste in DMS Creation .
- Select the S3 Bucket.
- Test it and create it.

# Creating DMS Instance

- Search for DMS.
- Create a Replication Instance.
- Give it a name.
- Instance class give it micro.
- VPC → The default.
- Publicly accessible.
- Create.

# MySQL WordBench

- Download and install MySQL (local).

## Connecting MySQL local to AWS

- Create a new connection.
- In hostname give the endpoint of the Database, username and password.

## Creating a table

- Paste the dump file (SQL) (Course repository) on MySQL Console.

# Quering RDS

```sql
SELECT count(*) from ahmed_schema.Persons;
```

# DMS Full Load

- Go to DMS.
- Go to DB Migration Tasks.
- Create a task,
    - Provide a identifier.
    - Provide the Replication Instance.
    - Select source and target endpoints.
    - Migration type: Migrate existing data and replicate ongoing changes.
    - On Table Mappings.
        - Select if you want one or various schemas. In this case the one you create it.
        - Specify the Table.
    - Automatically start on Create.
    - And Create it.
    - Check S3 bucket.

# DMS Replication Ongoing

Note: Any data you modify, add or delete in MySQL will be replicated in S3 because DMS.

Note 2: S3 will provide you all the modified data.

# Stoping Instances

- Look for instances you want to stop.
- You need to stop the instances (or terminate them) whenyou end this tutorial.

# Glue Job (Full Load)

On Databricks...

```python
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

spark = SparkSession.appName("OK").getOrCreate()
```

```python
#1
fldf = spark.read.csv("/FileStore/tables/csv_name.csv")
fldf = fldf.withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "FullName").withColumnRenamed("_c2", "City")
fldf.write.mode("overwrite").csv("/Filestore/tables/finalFile.csv")
```

# Glue Job (Change Capture)

```python
#1
udf = spark.read.csv("/FileStore/tabñes/updates_csv_name.csv")
udf = fldf.withColumnRenamed("_c0", "action").withColumnRenamed("_c1", "id").withColumnRenamed("_c2", "FullName").withColumnRenamed("_c3", "City")
ffdf = spark.read.csv("/FileStore/tables/finalOutput/finalFile.csv")
#define column names foer ffdf
```

# Glue Job (CDC)

```python
for row in udf.collect():
	if row["action"] == 'U':
		ffdf.withColumn("FullName", when(ffdf["id" == row["id"], row["FullName"].otherwise(ffdf["FullName"])))
		ffdf.withColumn("City", when(ffdf["id" == row["id"], row["City"].otherwise(ffdf["City"])))
		
	if row["action"] == 'I':
		insertedRow = [list(row)[1:]]
		columns["id", "FullName", "City"]
		newdf = spark.createDataFrame(insertedRow, columns)
		ffdf = ffdf.union(newdf)

	if row["action"] == 'D':
		ffdf = ffdf.filter(ffdf.id != row["id"])

ffdf.write.mode("overwrite").csv("/FileStore/tables/finalOutput/finalFile.csv/tables/")
```

# Creating Lambda Function and Adding Trigger

- Go to Lambda in AWS.
- Create a Function.
    - From Scratch.
    - Give it a name.
    - Runtime: Python 3.8
    - Create a IAM Role.
        - On IAM:
            - Create a new Role.
            - Use case: Lambda.
            - Policies:
                - S3 Full Access.
                - Glue Console Full Access.
                - Cloudwatch Full Access.
            - Provide Role Name.
            - Create it.
    - Use the created IAM Role.
- Add a Trigger for S3:
    - Select the S3 bucket name.
    - On Event Type: All Object create events.
    - Add it.

# Checking Trigger

- Deploy Lambda Function.
- Now Test it.

- Search for CloudWatch on AWS.
- Go for Log groups and select lambda function name.
    - Check the log.
- You can upload files to check Lambda.
- Put a print("hello World") if you want.}
- Don't forget to deleted after test it (the file).

# Getting S3 file name in Lambda

```python
# Lambda
import json

def lambda_handler(event, context):

	print(event)	

	bucketName = event["Records"][0]["s3"]["bucket"]["name"]
	fileName = event["Records"][0]["s3"]["object"]["key"]
	
	print(bucketName)
	print(fileName)

	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda')
	}
```

# Creating Glue Job

- Search for AWS Glue in AWS.
- Go to Jobs and add a new Job.
    - Put it a name.
    - IAM role:
        - Search for IAM in AWS.
        - Create a Role.
        - Search for Glue.
        - Permissions:
            - CloudWatch Full Access.
            - S3 Full Access.
            - Put the role a name.
            - Create it.
    - Choose the created Role.
    - Type: Spark.
    - Glue Version:
        - Spark 2.4, Python 3 with improved job startup times (Glue Version 2.0)
    - This job runs:
        - A new script to be authomated by you.
    - Script file Name.
    - Number of workers: 2.
    - Save the Job.

# Adding Invoke for Glue Job

```python
#lambda
import json
import boto3

def lambda_handler(event, context):
	bucketName = event["Records"][0]["s3"]["bucket"]["name"]
	fileName = event["Records"][0]["s3"]["object"]["key"]
	
	glue = boto3.start_job_run(
		JobName = 'the Glue name you put on the creation',
		Arguments = {
				'--s3_target_path_key': fileName,
				'--s3_target_path_bucket': bucketName
		}
	)

	return {
		'statusCode': 200,
		'body': json.dumps('Hello from Lambda')
	}
```

## Editing the script (Glue Job)

```python
from awsglue.util import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ["s3_target_path_key", "s3_target_path_bucket"])
bucket = args["s3_target_path_bucket"]
fileName = args["s3_target_path_key"]

print(bucket, fileName)
```

# Testing Invoke

- Upload the dump File.
- Go to CloudWatch on AWS.

Note: Glue is not Free!!!!

- Go for Logs.
- Search for Glue outputs.

# Writing Glue Shell Job

```python
from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv,['s3_target_path_key','s3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
fileName = args['s3_target_path_key']

print(bucket, fileName)

spark = SparkSession.builder.appName("CDC").getOrCreate()
inputFilePath = f"s3a://{bucket}/{fileName}"
finalFilePath = f"s3a://cdc-ouput-pyspark/output" #Name of the output S3

if "LOAD" in fileName:
    fldf = spark.read.csv(inputFilePath)
    fldf = fldf.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    fldf.write.mode("overwrite").csv(finalFilePath)
else:
    udf = spark.read.csv(inputFilePath)
    udf = udf.withColumnRenamed("_c0","action").withColumnRenamed("_c1","id").withColumnRenamed("_c2","FullName").withColumnRenamed("_c3","City")
    ffdf = spark.read.csv(finalFilePath)
    ffdf = ffdf.withColumnRenamed("_c0","id").withColumnRenamed("_c1","FullName").withColumnRenamed("_c2","City")
    
    for row in udf.collect(): 
      if row["action"] == 'U':
        ffdf = ffdf.withColumn("FullName", when(ffdf["id"] == row["id"], row["FullName"]).otherwise(ffdf["FullName"]))      
        ffdf = ffdf.withColumn("City", when(ffdf["id"] == row["id"], row["City"]).otherwise(ffdf["City"]))
    
      if row["action"] == 'I':
        insertedRow = [list(row)[1:]]
        columns = ['id', 'FullName', 'City']
        newdf = spark.createDataFrame(insertedRow, columns)
        ffdf = ffdf.union(newdf)
    
      if row["action"] == 'D':
        ffdf = ffdf.filter(ffdf.id != row["id"])
        
    ffdf.write.mode("overwrite").csv(finalFilePath)
```

- You need to create a new bucket for the output.

# Full Load Pipeline

- (Re)Start the services.
- On local make a single select to check if DB is on. (MySQL)
- Download the output csv and check it.

# Termination

- Terminate all the services, lambda functions and glue jobs.