from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import logging
import os
import zipfile
import subprocess
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    Float,
    MetaData,
    Date,
)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

dag = DAG(
    dag_id="football_etl_model_star",
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

        # === Load CSVs ===
        games = pd.read_csv(os.path.join(DATA_PATH, "games.csv"))
        clubs = pd.read_csv(os.path.join(DATA_PATH, "clubs.csv"))
        competitions = pd.read_csv(os.path.join(DATA_PATH, "competitions.csv"))

        # === Transform FACT: Match ===
        fact_match = games[
            [
                "game_id",
                "home_club_goals",
                "away_club_goals",
                "home_club_id",
                "away_club_id",
                "competition_id",
                "date",
            ]
        ].copy()
        fact_match.rename(
            columns={"game_id": "match_id", "date": "match_date"}, inplace=True
        )
        fact_match["home_goals"] = games["home_club_goals"].astype(int)
        fact_match["away_goals"] = games["away_club_goals"].astype(int)
        fact_match["match_date"] = pd.to_datetime(fact_match["match_date"]).dt.date
        fact_match = fact_match[
            [
                "match_id",
                "home_goals",
                "away_goals",
                "home_club_id",
                "away_club_id",
                "competition_id",
                "match_date",
            ]
        ]

        # === DIMENSION: Season === (SCD1)
        dim_season = games[["date"]].drop_duplicates()
        dim_season["season"] = pd.to_datetime(dim_season["date"]).dt.year.astype(str)
        dim_season = dim_season[["season"]].drop_duplicates()

        # === DIMENSION: Club === (SCD2)
        dim_club = clubs[["club_id", "pretty_name", "country_id"]].copy()
        dim_club.rename(columns={"pretty_name": "club_name"}, inplace=True)
        dim_club["club_name"] = dim_club["club_name"].str.strip()

        # === DIMENSION: Competition === (SCD2)
        dim_competition = competitions[
            ["competition_id", "name", "country_name"]
        ].copy()
        dim_competition.rename(
            columns={"name": "competition_name", "country_name": "country"},
            inplace=True,
        )

        # === DIMENSION: Time === (SCD1)
        dim_time = games[["date"]].drop_duplicates().copy()
        dim_time["match_date"] = pd.to_datetime(dim_time["date"]).dt.date
        dim_time["year"] = pd.to_datetime(dim_time["date"]).dt.year
        dim_time["month"] = pd.to_datetime(dim_time["date"]).dt.month
        dim_time["day_of_week"] = pd.to_datetime(dim_time["date"]).dt.day_name()
        dim_time = dim_time[["match_date", "year", "month", "day_of_week"]]

        # === DB CONNECTION ===
        load_dotenv()
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB")
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")

        if not all([host, port, db, user, password]):
            raise ValueError("Missing DB environment variables.")

        connection_string = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        )
        engine = create_engine(connection_string)
        metadata = MetaData()

        # === Define Tables ===
        match_table = Table(
            "fact_match",
            metadata,
            Column("match_id", Integer, primary_key=True),
            Column("home_goals", Integer),
            Column("away_goals", Integer),
            Column("home_club_id", Integer),
            Column("away_club_id", Integer),
            Column("competition_id", Integer),
            Column("match_date", Date),
        )

        season_table = Table(
            "dim_season", metadata, Column("season", String, primary_key=True)
        )

        club_table = Table(
            "dim_club",
            metadata,
            Column("club_id", Integer, primary_key=True),
            Column("club_name", String),
            Column("country_id", Integer),
        )

        comp_table = Table(
            "dim_competition",
            metadata,
            Column("competition_id", Integer, primary_key=True),
            Column("competition_name", String),
            Column("country", String),
        )

        time_table = Table(
            "dim_time",
            metadata,
            Column("match_date", Date, primary_key=True),
            Column("year", Integer),
            Column("month", Integer),
            Column("day_of_week", String),
        )

        # === Create and Insert ===
        metadata.drop_all(engine)
        metadata.create_all(engine)

        with engine.connect() as conn:
            conn.execute(match_table.insert(), fact_match.to_dict(orient="records"))
            conn.execute(season_table.insert(), dim_season.to_dict(orient="records"))
            conn.execute(club_table.insert(), dim_club.to_dict(orient="records"))
            conn.execute(comp_table.insert(), dim_competition.to_dict(orient="records"))
            conn.execute(time_table.insert(), dim_time.to_dict(orient="records"))

        logging.info("✅ ETL and upload to PostgreSQL completed successfully.")
        return "✅ ETL and upload to PostgreSQL completed successfully."

    except Exception as e:
        logging.error("❌ ETL process failed", exc_info=True)
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
