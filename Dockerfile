# Base image
FROM apache/airflow:2.9.1-python3.9

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Install PostgreSQL client (optional, for debugging)
USER root
RUN apt-get update && apt-get install -y postgresql-client && apt-get clean
USER airflow

# Copy your DAGs
COPY ./dags ${AIRFLOW_HOME}/dags

# Copy .env file (optional – if you want it inside the container)
COPY .env ${AIRFLOW_HOME}/.env

# Set working directory
WORKDIR ${AIRFLOW_HOME}
