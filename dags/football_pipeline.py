from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import logging
import os
import zipfile
import subprocess
from sqlalchemy import create_engine

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

dag = DAG(
    dag_id="`football_etl_model_star`",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    # schedule_interval='*/10 * * * *',  # co 10 minut
    # catchup=False,
)

DATA_PATH = "/home/wiktor/projects/FootballAnalysis/data/"
OUTPUT_PATH = "/home/wiktor/projects/FootballAnalysis/output/"


# DATA_PATH = "/home/klako/FootballAnalysis/data/"
# OUTPUT_PATH = "/home/klako/FootballAnalysis/output/"


def download_data():
    os.makedirs(DATA_PATH, exist_ok=True)
    os.chdir(DATA_PATH)

    zip_file = "player-scores.zip"
    extracted_folder = "player-scores"

    # Skip download if already exists
    if not os.path.exists(zip_file):
        logging.info("Downloading dataset from Kaggle...")
        subprocess.run(
            ["kaggle", "datasets", "download", "-d", "davidcariboo/player-scores"],
            check=True,
        )

    if not os.path.exists(extracted_folder):
        logging.info("Extracting dataset...")
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(extracted_folder)

    # Rename or move required files into DATA_PATH
    for fname in os.listdir(extracted_folder):
        if fname.endswith(".csv"):
            src = os.path.join(extracted_folder, fname)
            dst = os.path.join(DATA_PATH, fname)
            os.rename(src, dst)


def load_and_transform():
    try:
        os.makedirs(OUTPUT_PATH, exist_ok=True)

        games = pd.read_csv(os.path.join(DATA_PATH, "games.csv"))
        players = pd.read_csv(os.path.join(DATA_PATH, "players.csv"))
        clubs = pd.read_csv(os.path.join(DATA_PATH, "clubs.csv"))

        # === DIMENSION: Time ===
        dim_time = games[["date"]].drop_duplicates()
        dim_time["time_id"] = dim_time["date"]
        dim_time.to_csv(os.path.join(OUTPUT_PATH, "dim_time.csv"), index=False)

        # === DIMENSION: Season ===
        dim_season = games[["season"]].drop_duplicates()
        dim_season["season_id"] = dim_season["season"]
        dim_season.to_csv(os.path.join(OUTPUT_PATH, "dim_season.csv"), index=False)

        # === DIMENSION: Players ===
        dim_players = players[
            ["player_id", "name", "country_of_birth", "country_of_citizenship"]
        ]
        dim_players.to_csv(os.path.join(OUTPUT_PATH, "dim_players.csv"), index=False)

        # === DIMENSION: Clubs ===
        dim_clubs = clubs[["club_id", "name", "total_market_value", "squad_size"]]
        dim_clubs.to_csv(os.path.join(OUTPUT_PATH, "dim_clubs.csv"), index=False)

        # === DIMENSION: Competitions ===
        dim_comp = games[["competition_id"]].drop_duplicates()
        dim_comp["competition_name"] = "N/A"
        dim_comp.to_csv(os.path.join(OUTPUT_PATH, "dim_competitions.csv"), index=False)

        # === FACT: Matches ===
        fact_matches = games[
            [
                "game_id",
                "date",
                "season",
                "competition_id",
                "home_club_id",
                "away_club_id",
                "home_club_goals",
                "away_club_goals",
            ]
        ].copy()

        fact_matches["result"] = fact_matches.apply(
            lambda row: (
                "Home Win"
                if row["home_club_goals"] > row["away_club_goals"]
                else (
                    "Away Win"
                    if row["home_club_goals"] < row["away_club_goals"]
                    else "Draw"
                )
            ),
            axis=1,
        )
        fact_matches.to_csv(os.path.join(OUTPUT_PATH, "fact_matches.csv"), index=False)

        # # Zapis do plików CSV (jeśli chcesz zachować)
        # dim_time.to_csv(os.path.join(OUTPUT_PATH, 'dim_time.csv'), index=False)
        # dim_season.to_csv(os.path.join(OUTPUT_PATH, 'dim_season.csv'), index=False)
        # dim_players.to_csv(os.path.join(OUTPUT_PATH, 'dim_players.csv'), index=False)
        # dim_clubs.to_csv(os.path.join(OUTPUT_PATH, 'dim_clubs.csv'), index=False)
        # dim_comp.to_csv(os.path.join(OUTPUT_PATH, 'dim_competitions.csv'), index=False)
        # fact_matches.to_csv(os.path.join(OUTPUT_PATH, 'fact_matches.csv'), index=False)

        load_dotenv()

        server = os.getenv("SQL_SERVER")
        database = os.getenv("SQL_DATABASE")
        username = os.getenv("SQL_USERNAME")
        password = os.getenv("SQL_PASSWORD")
        driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

        if not all([server, database, username, password]):
            raise ValueError(
                "Not all variables in .env are set (SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD)"
            )

        # Escape spaces in driver name for the connection string
        driver_escaped = driver.replace(" ", "+")

        # Tworzenie connection stringa SQLAlchemy z pyodbc
        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            f"?driver={driver_escaped}"
        )

        engine = create_engine(connection_string)

        # Zapis do SQL Server
        dim_time.to_sql("dim_time", con=engine, if_exists="replace", index=False)
        dim_season.to_sql("dim_season", con=engine, if_exists="replace", index=False)
        dim_players.to_sql("dim_players", con=engine, if_exists="replace", index=False)
        dim_clubs.to_sql("dim_clubs", con=engine, if_exists="replace", index=False)
        dim_comp.to_sql(
            "dim_competitions", con=engine, if_exists="replace", index=False
        )
        fact_matches.to_sql(
            "fact_matches", con=engine, if_exists="replace", index=False
        )

        logging.info("ETL and SQL Server upload completed successfully.")
        return "ETL and SQL Server upload completed successfully."

    except Exception as e:
        logging.error("ETL process failed", exc_info=True)
        raise


# Define Airflow tasks
download_task = PythonOperator(
    task_id="download_data",
    python_callable=download_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_and_save_data",
    python_callable=load_and_transform,
    dag=dag,
)

# Set task dependencies
download_task >> transform_task
