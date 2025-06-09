import psycopg2
import os
import logging

logging.basicConfig(level=logging.INFO)


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        logging.info("🔌 Connected to PostgreSQL database.")
        return conn
    except psycopg2.Error as e:
        logging.error("❌ Failed to connect to database", exc_info=True)
        raise


def execute_query(conn, query, params=None):
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
    except Exception as e:
        logging.error("❌ Query failed", exc_info=True)
        conn.rollback()
        raise


def execute_many(conn, query, values):
    try:
        with conn.cursor() as cur:
            cur.executemany(query, values)
            conn.commit()
    except Exception as e:
        logging.error("❌ Batch query failed", exc_info=True)
        conn.rollback()
        raise
