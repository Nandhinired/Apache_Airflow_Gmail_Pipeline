# Pipeline implementation with Amazon S3,Hive,Hbase, Spark,Airflow & Gmail for notifications

## List of components used for the pipeline implementation 
### VSCode Editor
### Docker 
### Itversity Lab for distributed environment
### Amazon S3
### Sqoop
### Hive
### Spark
### HBase
### Airflow
### Gmail SMTP Server 


## Pipleine implementation involves the below steps 


Step1: Checking if the orders file is available in the S3 bucket


Step2: Once the file is available , we are fetching the file from the Amazon S3 bucket tp the edge node

Step3: Sqoop command to fetch the customers(complete dump at once no incremental load and non partitioned) from sql to the hive

<<<<<<< HEAD
<p align="center">
  <img src="Images/Docker-compose.jpg" width="650" title="Loaded model in blender">
</p>
=======
## Steps to be followed for 

Step1: Download the repo to your local syatem and open it in VSCode

Step2: Open a terminal


docker-compose up --build
>>>>>>> 39c02edeed84a491cdb0897059dcbc4979efc18a

<p align="center">
  <img src="Images/s3_orders.jpg" width="650" title="Loaded model in blender">
</p>

<p align="center">
  <img src="Images/Airflow_pipeine.jpg" width="650" title="Loaded model in blender">
</p>
