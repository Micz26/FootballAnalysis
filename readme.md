

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt


pip install kagglehub pandas apache-airflow

export AIRFLOW_HOME=~/FootballAnalysis

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com


airflow scheduler
