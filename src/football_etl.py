import os
import pandas as pd
import logging
from dotenv import load_dotenv
from .db_utils import get_db_connection, execute_many, execute_query

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DATA_PATH = "data"
OUTPUT_PATH = "output"


def create_tables(conn):
    """Create dimension and fact tables if they don't exist."""
    queries = [
        """
        CREATE TABLE IF NOT EXISTS dim_time (
            date DATE PRIMARY KEY,
            time_id DATE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_season (
            season VARCHAR(20) PRIMARY KEY,
            season_id VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_players (
            player_id VARCHAR(50) PRIMARY KEY,
            name TEXT,
            country_of_birth TEXT,
            country_of_citizenship TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_clubs (
            club_id VARCHAR(50) PRIMARY KEY,
            name TEXT,
            total_market_value TEXT,
            squad_size INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_competitions (
            competition_id VARCHAR(50) PRIMARY KEY,
            competition_name TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_matches (
            game_id VARCHAR(50) PRIMARY KEY,
            date DATE,
            season VARCHAR(20),
            competition_id VARCHAR(50),
            home_club_id VARCHAR(50),
            away_club_id VARCHAR(50),
            home_club_goals INTEGER,
            away_club_goals INTEGER,
            result TEXT
        );
        """,
    ]
    for query in queries:
        execute_query(conn, query)
    logging.info("✅ All required tables are ensured.")


def transform_data():
    """Reads and transforms CSV data into dimension and fact tables."""
    games = pd.read_csv(os.path.join(DATA_PATH, "games.csv"))
    players = pd.read_csv(os.path.join(DATA_PATH, "players.csv"))
    clubs = pd.read_csv(os.path.join(DATA_PATH, "clubs.csv"))

    dim_time = games[["date"]].drop_duplicates()
    dim_time["time_id"] = dim_time["date"]

    dim_season = games[["season"]].drop_duplicates()
    dim_season["season_id"] = dim_season["season"]

    dim_players = players[
        ["player_id", "name", "country_of_birth", "country_of_citizenship"]
    ]

    dim_clubs = clubs[["club_id", "name", "total_market_value", "squad_size"]]

    dim_comp = games[["competition_id"]].drop_duplicates()
    dim_comp["competition_name"] = "N/A"

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

    return {
        "dim_time": dim_time,
        "dim_season": dim_season,
        "dim_players": dim_players,
        "dim_clubs": dim_clubs,
        "dim_competitions": dim_comp,
        "fact_matches": fact_matches,
    }


def ingest_data_to_db(tables_dict):
    """Inserts transformed data into PostgreSQL."""
    conn = get_db_connection()
    try:
        create_tables(conn)
        for table_name, df in tables_dict.items():
            logging.info(f"Inserting into table: {table_name} ({len(df)} records)")
            df.to_sql(
                table_name, con=conn, if_exists="replace", index=False, method="multi"
            )
        logging.info("✅ ETL and ingestion completed successfully.")
    finally:
        conn.close()


def run_etl_pipeline():
    """Main ETL pipeline entry point."""
    try:
        os.makedirs(OUTPUT_PATH, exist_ok=True)
        tables = transform_data()
        ingest_data_to_db(tables)
    except Exception as e:
        logging.error("❌ ETL process failed", exc_info=True)
        raise
