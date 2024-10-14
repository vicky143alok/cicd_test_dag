from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.transfers.s3 import S3CreateObjectOperator # type: ignore
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

# Define the DAG
with DAG('upload_to_s3_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Step 1: Create tables in Snowflake
    create_tables = SnowflakeOperator(
        task_id='create_tables',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE sales_summary (
            sale_id INT,
            total_amount FLOAT,
            sale_date DATE
        );

        CREATE OR REPLACE TABLE product_info (
            product_id INT,
            product_name STRING,
            price FLOAT
        );

        CREATE OR REPLACE TABLE customer_data (
            customer_id INT,
            customer_name STRING,
            email STRING
        );

        CREATE OR REPLACE TABLE order_details (
            order_id INT,
            product_id INT,
            quantity INT
        );
        """,
        dag=dag
    )

    # Step 2: Fill tables with sample data
    fill_data = SnowflakeOperator(
        task_id='fill_data',
        snowflake_conn_id='snowflake_conn',
        sql="""
        INSERT INTO sales_summary (sale_id, total_amount, sale_date) VALUES 
           (1, 100.00, '2024-10-10'), 
           (2, 200.50, '2024-10-11'), 
           (3, 300.75, '2024-10-12');

        INSERT INTO product_info (product_id, product_name, price) VALUES
           (1, 'Laptop', 1200.00),
           (2, 'Mouse', 25.00),
           (3, 'Keyboard', 45.00);

        INSERT INTO customer_data (customer_id, customer_name, email) VALUES
           (1, 'Alice Johnson', 'alice@example.com'),
           (2, 'Bob Williams', 'bob@example.com'),
           (3, 'Carol White', 'carol@example.com');

        INSERT INTO order_details (order_id, product_id, quantity) VALUES
           (1, 1, 2),
           (2, 2, 3),
           (3, 3, 1);
        """,
        dag=dag
    )

    # Step 3: Join tables and export to Snowflake stage
    join_and_export = SnowflakeOperator(
        task_id='join_and_export',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE joined_data AS
        SELECT 
            ss.sale_id,
            ss.total_amount,
            ss.sale_date,
            pi.product_name,
            pi.price,
            cd.customer_name,
            cd.email,
            od.quantity
        FROM sales_summary ss
        JOIN order_details od ON ss.sale_id = od.order_id
        JOIN product_info pi ON od.product_id = pi.product_id
        JOIN customer_data cd ON ss.sale_id = cd.customer_id;

        COPY INTO @~/joined_data.csv
        FROM joined_data
        FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"')
        OVERWRITE=TRUE;
        """,
        dag=dag
    )

    # Step 4: Upload CSV file to S3
    upload_to_s3 = S3CreateObjectOperator(
        task_id='upload_to_s3',
        aws_conn_id='aws_s3_conn',
        bucket_name='airflow-tables-bucket',
        key='joined_data/joined_data.csv',
        data="{{ task_instance.xcom_pull(task_ids='join_and_export') }}",
        replace=True,
        dag=dag
    )

    # Task dependencies
    create_tables >> fill_data >> join_and_export >> upload_to_s3
