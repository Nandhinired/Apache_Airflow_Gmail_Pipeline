from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import timedelta
from airflow import settings
from airflow.models import Connection
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator


default_args = {
    'email_on_failure': False,
    'pool': 'my_pool'            
}

dag = DAG(
      dag_id='customer_pipeline',
      start_date=days_ago(1),
      default_args=default_args     
)

# sensor to check the availability of the file in the S3 location
sensor = HttpSensor(
    task_id='watch_for_orders_at_S3',
    http_conn_id='order_s3',
    endpoint='orders.csv',
    response_check=lambda response: response.status_code == 200,
    retry_delay=timedelta(minutes=5),
    retries=12,
    dag=dag
)

def get_order_url():
    session = settings.Session()
    connection = session.query(Connection).filter(Connection.conn_id == 'order_s3').first()
    return f'{connection.schema}://{connection.host}/orders.csv'

# creates a directory and downloads the orders.csv into it
download_order_from_s3 = f'rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_order_url()}'

# ssh connection to the itversity gateway edge node
download_to_edgenode = SSHOperator(
    task_id='download_orders',
    ssh_conn_id='itversity',
    command=download_order_from_s3,
    dag=dag
)

# sqoop command to fetch the customers(complete dump at once no incremental load and non partitioned) from sql to the hive
def fetch_customer_info_cmd():
    command_one = "hive -e 'DROP TABLE IF EXISTS airflow_itv003130.customers'"
    command_two = "sqoop-import --connect \"jdbc:mysql://ms.itversity.com:3306/retail_db\" --username retail_user --password itversity --table customers --hive-import --hive-database airflow_itv003130 --hive-table customers"
    return f'{command_one} && {command_two}'

# ssh connection to the itversity gateway edge node and run sqoop command
import_customer_info = SSHOperator(
    task_id='download_customers',
    ssh_conn_id='itversity',
    command=fetch_customer_info_cmd(),
    dag=dag
)

# hdfs command execution
upload_order_info = SSHOperator(
    task_id='upload_order_to_hdfs',
    ssh_conn_id='itversity',
    command='hdfs dfs -rm -R -f /user/itv003130/airflow_input && hdfs dfs -mkdir -p /user/itv003130/airflow_input && hdfs dfs -put /home/itv003130/airflow_pipeline/orders.csv /user/itv003130/airflow_input/',
    dag=dag
)

# spark program condition
def get_order_filter_cmd():
    # EXPORT SPARK_MAJOR_VERSION=2
    command_one = 'hdfs dfs -rm -R -f /user/itv003130/airflow_output '
    command_two = 'spark-submit --class ordersApp --master yarn --deploy-mode cluster /home/itv003130/sparkbundle.jar /user/itv003130/airflow_input/orders.csv /user/itv003130/airflow_output'
    return f'{command_one} && {command_two}'

# spark  execution
process_order_info = SSHOperator(
    task_id='process_orders',
    ssh_conn_id='itversity',
    command=get_order_filter_cmd(),
    dag=dag
)

def create_hive_table():
    command_one = '''hive -e "CREATE external table if not exists airflow_itv003130.orders(order_id int,order_date string,order_customer_id int,order_status string) row format delimited fields terminated by ',' stored as textfile location '/user/itv003130/spark_output'"'''
    return f'{command_one}'

# hive table creation
hive_order_info = SSHOperator(
    task_id='create_hive_orders',
    ssh_conn_id='itversity',
    command=create_hive_table(),
    dag=dag
)

def hbase_table_cmd():
    command_one = '''hive -e "CREATE table if not exists airflow_itv003130.airflow_hbase(customer_id int, customer_fname string, customer_lname string, order_id int, order_date string ) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with SERDEPROPERTIES('hbase.columns.mapping'=':key,personal:customer_fname, personal:customer_lname, personal:order_id, personal:order_date')"'''
    command_two = 'hive -e "insert overwrite table airflow_itv003130.airflow_hbase select c.customer_id, c.customer_fname, c.customer_lname, o.order_id, o.order_date from airflow_itv003130.customers c join airflow_itv003130.orders o ON (c.customer_id = o.order_customer_id)"'
    return f'{command_one} && {command_two}' 

# hbase table creation
load_hbase = SSHOperator(
    task_id='load_hbase_table',
    ssh_conn_id='itversity',
    command=hbase_table_cmd(),
    dag=dag
)

success_notify = EmailOperator(
    task_id='sucess_email_notify',
    to='nandhinireddynandu@gmail.com',
    subject='pipeline ingestion completed',
    html_content=""" <h1>Congratulations! Hbase data is ready.</h1> """,
    trigger_rule='all_success',
    dag=dag
)

failure_notify = EmailOperator(
    task_id='failure_email_notify',
    to='nandhinireddynandu@gmail.com',
    subject='pipeline ingestion has failed',
    html_content=""" <h1>Sorry! Hbase data is not yet ready.</h1> """,
    trigger_rule='all_failed',
    dag=dag
)

sensor >> import_customer_info 
sensor >> download_to_edgenode >> upload_order_info >> process_order_info >> hive_order_info
[import_customer_info,hive_order_info]>> load_hbase >> [success_notify,failure_notify]