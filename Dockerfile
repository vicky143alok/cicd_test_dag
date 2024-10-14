FROM apache/airflow:2.7.0

# Copy DAGs to the Airflow DAGs directory
COPY ./dags /opt/airflow/dags

# Install any additional dependencies
RUN pip install apache-airflow-providers-snowflake apache-airflow-providers-amazon
