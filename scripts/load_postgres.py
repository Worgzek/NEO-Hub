import psycopg2
from loguru import logger
from psycopg2.extras import execute_values
import sys
import os
from dotenv import load_dotenv
import pandas as pd
from db_log import log_ETL
load_dotenv()

PASS = os.getenv("PASS")

run_date = sys.argv[1]
LOG_FILE = f"/opt/airflow/logs/pipeline_{run_date}.log"
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(
    LOG_FILE,
    rotation="10 MB",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

def load_postgres(run_date):
    conn = None
    cur = None
    try:
        logger.info("dang doc .csv")
        df = pd.read_csv(f"/opt/airflow/data/processed/asteroids_{run_date}.csv")
        logger.info(f"da them {len(df)} dong")

        conn = psycopg2.connect(
            host="postgres",
            database= "nasa_neo",
            user= "airflow",
            password = PASS
        )
        cur = conn.cursor()
        logger.info("bat dau tao bang")
        cur.execute("""
            Create table if not exists Asteroids(
                asteroid_id BIGINT,
                name VARCHAR(30),
                absolute_magnitude FLOAT,
                diameter_min_m FLOAT,
                diameter_max_m FLOAT,
                velocity_km_s FLOAT,
                miss_distance_km FLOAT,
                date date,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (asteroid_id, date)
                );
            """)
        values = df[[
                'asteroid_id',
                'name',
                'absolute_magnitude',
                'diameter_min_m',
                'diameter_max_m',
                'velocity_km_s',
                'miss_distance_km',
                'date',
        ]].values.tolist()
        logger.info("dang load data")
        insert_query = """
            insert into Asteroids(asteroid_id, name, absolute_magnitude, diameter_min_m, diameter_max_m, velocity_km_s, miss_distance_km, date)
            values %s
            ON CONFLICT (asteroid_id, date)
            DO UPDATE SET
            velocity_km_s = EXCLUDED.velocity_km_s,
            miss_distance_km = EXCLUDED.miss_distance_km,
            name = EXCLUDED.name,
            absolute_magnitude = EXCLUDED.absolute_magnitude,   
            diameter_min_m = EXCLUDED.diameter_min_m,
            diameter_max_m = EXCLUDED.diameter_max_m,
            last_updated = CURRENT_TIMESTAMP;
            """
        execute_values(cur, insert_query, values)
        conn.commit()
        logger.success(f"insert thanh cong {len(values)} dong\n------------------------------------------------------")
        log_ETL(run_date, "load to DB", "success", len(values), "da load vao database thanh cong")

    except Exception as e:
        logger.exception(f"Da co loi xay ra {e}")
        log_ETL(run_date, "load to DB", "failed", len(values), f"da co loi xay ra {e}")

        if conn:
            conn.rollback()
        sys.exit(1)

    finally:
        if conn:
            conn.close()
        if cur:
            cur.close()

if __name__ == "__main__":
    run_date = sys.argv[1]
    load_postgres(run_date)