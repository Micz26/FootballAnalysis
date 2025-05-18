from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter

def download_and_load_dataset():
    # Ścieżka pliku wewnątrz datasetu (możesz to sprawdzić ręcznie po pobraniu raz)
    file_path = "players.csv"
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "davidcariboo/player-scores",
        file_path,
    )
    df.to_csv('/tmp/raw_players.csv', index=False)

def preprocess_data():
    df = pd.read_csv('/tmp/raw_players.csv')
    # Prosty preprocessing: wybierz tylko aktywnych graczy z ważnym ratingiem
    df_clean = df.dropna(subset=["overall", "potential", "age"])
    df_clean = df_clean[df_clean["age"] > 16]  # przykład
    df_clean.to_csv('/tmp/cleaned_players.csv', index=False)

def prepare_for_powerbi():
    df = pd.read_csv('/tmp/cleaned_players.csv')
    # Grupowanie: średnia ocena według pozycji
    summary = df.groupby("position")["overall"].mean().reset_index(name="avg_overall")
    summary.to_csv('/tmp/powerbi_summary.csv', index=False)

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG('player_scores_pipeline',
         schedule_interval=None,
         default_args=default_args,
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='download_and_load_dataset',
        python_callable=download_and_load_dataset
    )

    t2 = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data
    )

    t3 = PythonOperator(
        task_id='prepare_for_powerbi',
        python_callable=prepare_for_powerbi
    )

    t1 >> t2 >> t3
