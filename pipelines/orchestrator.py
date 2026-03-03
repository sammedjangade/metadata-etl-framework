import psycopg2
from datetime import datetime
from pipelines.extractor import extract
from pipelines.transformer import transform
from pipelines.loader import load
from config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD

def get_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

def get_active_sources(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM etl_source_config WHERE active = 'Y'")
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def log_run(conn, source_name, status, rows_extracted=0, rows_loaded=0, error=None):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO etl_run_log 
        (source_name, status, rows_extracted, rows_loaded, started_at, completed_at, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (source_name, status, rows_extracted, rows_loaded, datetime.now(), datetime.now(), error))
    conn.commit()
    cursor.close()

def run_pipeline():
    conn = get_connection()
    sources = get_active_sources(conn)

    for source in sources:
        print(f"Processing: {source['source_name']}")
        try:
            df = extract(source['source_name'], source['source_type'], source['s3_key'])
            rows_extracted = len(df)
            df = transform(df, source['source_name'])
            load(df, source['target_schema'], source['target_table'])
            log_run(conn, source['source_name'], 'success', rows_extracted, len(df))
            print(f"✅ {source['source_name']} done")
        except Exception as e:
            log_run(conn, source['source_name'], 'failed', error=str(e))
            print(f"❌ {source['source_name']} failed: {str(e)}")

    conn.close()

if __name__ == '__main__':
    run_pipeline()