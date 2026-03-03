import psycopg2
from config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD

def get_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

def load(df, target_schema, target_table):
    conn = get_connection()
    cursor = conn.cursor()
    
    # Truncate first (full load)
    cursor.execute(f"TRUNCATE TABLE {target_schema}.{target_table}")
    
    # Insert rows
    for _, row in df.iterrows():
        placeholders = ','.join(['%s'] * len(row))
        cols = ','.join(df.columns)
        sql = f"INSERT INTO {target_schema}.{target_table} ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {len(df)} rows into {target_schema}.{target_table}")