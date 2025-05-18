from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    dag_id='football_etl_model_star',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

DATA_PATH = '/home/klako/FootballAnalysis/data/'
OUTPUT_PATH = '/home/klako/FootballAnalysis/output/'


def load_and_transform():
    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Load CSVs
    games = pd.read_csv(f'{DATA_PATH}games.csv')
    players = pd.read_csv(f'{DATA_PATH}players.csv')
    clubs = pd.read_csv(f'{DATA_PATH}clubs.csv')

    # === DIMENSION: Time ===
    dim_time = games[['date']].drop_duplicates()
    dim_time['time_id'] = dim_time['date']
    dim_time.to_csv(f'{OUTPUT_PATH}dim_time.csv', index=False)

    # === DIMENSION: Season ===
    dim_season = games[['season']].drop_duplicates()
    dim_season['season_id'] = dim_season['season']
    dim_season.to_csv(f'{OUTPUT_PATH}dim_season.csv', index=False)

    # === DIMENSION: Players ===
    dim_players = players[['player_id', 'name', 'country_of_birth', 'country_of_citizenship']]
    dim_players.to_csv(f'{OUTPUT_PATH}dim_players.csv', index=False)

    # === DIMENSION: Clubs ===
    dim_clubs = clubs[['club_id', 'name', 'total_market_value', 'squad_size']]
    dim_clubs.to_csv(f'{OUTPUT_PATH}dim_clubs.csv', index=False)

    # === DIMENSION: Competitions ===
    dim_comp = games[['competition_id']].drop_duplicates()
    dim_comp['competition_name'] = 'N/A'  # Możesz ręcznie dodać nazwy
    dim_comp.to_csv(f'{OUTPUT_PATH}dim_competitions.csv', index=False)

    # === FACT: Matches ===
    fact_matches = games[[
        'game_id',
        'date',
        'season',
        'competition_id',
        'home_club_id',
        'away_club_id',
        'home_club_goals',
        'away_club_goals'
    ]]
    fact_matches['result'] = fact_matches.apply(
        lambda row: 'Home Win' if row['home_club_goals'] > row['away_club_goals']
        else ('Away Win' if row['home_club_goals'] < row['away_club_goals'] else 'Draw'), axis=1
    )
    fact_matches.to_csv(f'{OUTPUT_PATH}fact_matches.csv', index=False)

load_task = PythonOperator(
    task_id='transform_and_save_data',
    python_callable=load_and_transform,
    dag=dag,
)
