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

## Prerequisites
### Please make sure Docker Desktop is installed and up on running 


## Steps to be followed for viewing the DAG in Airflow

Step1: Download the repo to your local system and open it in VSCode.

Step2: Edit the docker-compose.yml file and mention your Gmail SMTP Server credentials. It's different from your gmail account credentials.
       You can remove all the parameters mentioned as "AIRFLOW_SMTP_SMTP_" , if you want to skip the gmail notification part .
       

Step3: Please make sure docker desktop is up on running . Open terminal in VSCode and change the directory to project_pipeline
       and execute the  "docker-compose up --build" .

<p align="center">
  <img src="Images/Docker-compose.jpg" width="650" title="Loaded model in blender">
</p>

Step3: You would be getting the below message showing AIRFLOW which indicates that the Airflow is up on running

<p align="center">
  <img src="Images/Airflow.jpg" width="450" title="Loaded model in blender">
</p>

Step4: Open the tab with localhost:8080 where you can view the below broken DAG 

<p align="center">
  <img src="Images/broken_dag.jpg" width="450" title="Loaded model in blender">
</p>

Step5: You need to configure the new connections as mentioned below in the below screenshots

### One for the order_s3
<p align="center">
  <img src="Images/s3_orders.jpg" width="450" title="Loaded model in blender">
</p>

### Second for the itversity

<p align="center">
  <img src="Images/itversity.jpg" width="450" title="Loaded model in blender">
</p>

Step6: You have to define the pool with name "my_pool" as mentioned in the default_args. 


<p align="center">
  <img src="Images/pool.jpg" width="450" title="Loaded model in blender">
</p>

Step7: If you have followed the above mentioned steps , then you would be viewing the below DAG image 

<p align="center">
  <img src="Images/pipeline1_off.jpg" width="600" title="Loaded model in blender">
</p>

Step8: Click on the "OFF" option and change it to "ON" to turn on the DAG customer_pipeline

Step9: Once the steps mentioned in the pipeline is completed then you would be receiving the mail notifications to your gmail account.

