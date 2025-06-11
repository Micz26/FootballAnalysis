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
    BigInteger,
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

        # === DIMENSION: Season === (SCD1)
        season_mapping = (
            games[["season"]]
            .drop_duplicates()
            .reset_index(drop=True)
            .sort_values(by="season")
        )
        season_mapping["season_id"] = range(1, len(season_mapping) + 1)

        # Final dim_season table
        dim_season = season_mapping[["season_id", "season"]]

        dim_time = games[["date"]].drop_duplicates().copy()
        dim_time["match_date"] = pd.to_datetime(dim_time["date"]).dt.date
        dim_time["year"] = pd.to_datetime(dim_time["date"]).dt.year
        dim_time["month"] = pd.to_datetime(dim_time["date"]).dt.month
        dim_time["day"] = pd.to_datetime(dim_time["date"]).dt.day
        dim_time["day_of_week"] = pd.to_datetime(dim_time["date"]).dt.day_name()
        dim_time = dim_time.sort_values("match_date").reset_index(drop=True)
        dim_time["date_id"] = range(1, len(dim_time) + 1)
        dim_time = dim_time[
            ["date_id", "match_date", "year", "month", "day", "day_of_week"]
        ]

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
        fact_match.rename(columns={"game_id": "match_id"}, inplace=True)
        fact_match["home_goals"] = fact_match["home_club_goals"].astype("Int64")
        fact_match["away_goals"] = fact_match["away_club_goals"].astype("Int64")

        fact_match["home_club_id"] = fact_match["home_club_id"].astype("Int64")
        fact_match["away_club_id"] = fact_match["away_club_id"].astype("Int64")

        # Add season_id
        fact_match = fact_match.merge(
            games[["game_id", "season"]],
            left_on="match_id",
            right_on="game_id",
            how="left",
        )
        fact_match = fact_match.merge(season_mapping, on="season", how="left")

        # Add date_id
        fact_match["match_date"] = pd.to_datetime(fact_match["date"]).dt.date
        fact_match = fact_match.merge(
            dim_time[["date_id", "match_date"]], on="match_date", how="left"
        )

        fact_match = fact_match[
            [
                "match_id",
                "home_goals",
                "away_goals",
                "home_club_id",
                "away_club_id",
                "competition_id",
                "date_id",
                "season_id",
            ]
        ]

        # === DIMENSION: Club === (SCD2)
        dim_club = clubs[["club_id", "name"]].copy()
        dim_club["club_id"] = dim_club["club_id"].astype("Int64")

        dim_club.rename(columns={"name": "club_name"}, inplace=True)
        dim_club["club_name"] = dim_club["club_name"].str.strip()

        # === DIMENSION: Competition === (SCD2)
        dim_competition = competitions[
            ["competition_id", "name", "country_name"]
        ].copy()
        dim_competition.rename(
            columns={"name": "competition_name", "country_name": "country"},
            inplace=True,
        )

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
            Column("match_id", BigInteger, primary_key=True),
            Column("home_goals", Integer),
            Column("away_goals", Integer),
            Column("home_club_id", BigInteger),
            Column("away_club_id", BigInteger),
            Column("competition_id", String),
            Column("date_id", BigInteger),
            Column("season_id", BigInteger),
        )

        season_table = Table(
            "dim_season",
            metadata,
            Column("season_id", BigInteger, primary_key=True),
            Column("season", String),
        )

        club_table = Table(
            "dim_club",
            metadata,
            Column("club_id", BigInteger, primary_key=True),
            Column("club_name", String),
        )

        comp_table = Table(
            "dim_competition",
            metadata,
            Column("competition_id", String, primary_key=True),
            Column("competition_name", String),
            Column("country", String),
        )

        time_table = Table(
            "dim_time",
            metadata,
            Column("date_id", BigInteger, primary_key=True),
            Column("match_date", Date),
            Column("year", Integer),
            Column("month", Integer),
            Column("day", Integer),
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
