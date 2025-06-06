python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

pip install pyodbc sqlalchemy python-dotenv

pip install pymssql


sudo apt-get update
sudo apt-get install unixodbc unixodbc-dev
sudo apt-get install msodbcsql17


pip install kagglehub pandas apache-airflow

export AIRFLOW_HOME=$(pwd)

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com


airflow scheduler

airflow webserver --port 8081