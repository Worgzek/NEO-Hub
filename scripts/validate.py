import pandas as pd
import os
import sys
from loguru import logger
from dotenv import load_dotenv
from db_log import log_ETL
import psycopg2
from psycopg2.extras import execute_values
load_dotenv()

PASS = os.getenv('PASS')
run_date = sys.argv[1]
LOG_FILE = f"/opt/airflow/logs/pipeline_{run_date}.log"
logger.remove()
logger.add(sys.stdout, level='INFO')
logger.add(
    LOG_FILE,
    rotation='7 days',
    level='INFO',
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

def validate(run_date):
    try:
        conn = None
        curr = None
        conn = psycopg2.connect(
            host="postgres",
            database= "nasa_neo",
            user= "airflow",
            password = PASS
            )
        curr = conn.cursor()

        df = pd.read_csv(f"/opt/airflow/data/processed/asteroids_{run_date}.csv")
        logger.info("dang doc file csv")

        duplicated = df[df.duplicated(subset=['asteroid_id', 'date'])]
        logger.info(f"da phat hien {len(duplicated)} dong bi duplicate")
        df = df.drop_duplicates(subset=['asteroid_id', 'date'])

        null = df.isnull().sum()
        null = null[null>0]
        null_rows = df[df.isnull().any(axis=1)]

        null_rows['reject_reason'] = 'null'
        duplicated['reject_reason'] = 'duplicate'

        if len(null) > 0:
            logger.info(f"da phat hien {null} thong tin bi null")    
            save_null = '''           
                insert into ignored(asteroid_id, name, absolute_magnitude, diameter_min_m, diameter_max_m, velocity_km_s, miss_distance_km, reject_reason, date)
                        values %s
     
            '''
            values = null_rows[[
                    'asteroid_id',
                    'name',
                    'absolute_magnitude',
                    'diameter_min_m',
                    'diameter_max_m',
                    'velocity_km_s',
                    'miss_distance_km',
                    'reject_reason',
                    'date',
            ]].values.tolist()
            execute_values(curr, save_null, values)
            conn.commit()
        
        else:
            logger.info(f"khong phat hien null")    

        if len(duplicated) > 0:
            logger.info(f"da phat hien {len(duplicated)} thong tin bi duplicate")   
            save_duplicate = '''
                insert into ignored(asteroid_id, name, absolute_magnitude, diameter_min_m, diameter_max_m, velocity_km_s, miss_distance_km, reject_reason, date)
                    values %s
            ''' 
            dup_values = duplicated[[
                'asteroid_id',
                'name',
                'absolute_magnitude',
                'diameter_min_m',
                'diameter_max_m',
                'velocity_km_s',
                'miss_distance_km',
                'reject_reason',
                'date',
            ]].values.tolist()
            execute_values(curr, save_duplicate, dup_values)
            conn.commit()

        else:
            logger.info(f"khong phat hien duplicate")  
            
        log_ETL(run_date, "validate", "success", None, f"Da luu thanh cong {len(null_rows)} dong null va {len(duplicated)} dong duplicate")

    except Exception as e:
        logger.exception(f"Da co loi xay ra {e}")
        log_ETL(run_date, "load to DB", "failed", None, f"da co loi xay ra {e}")

        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
        if curr:
            curr.close()

if __name__ == "__main__":
    run_date = sys.argv[1]
    validate(run_date)