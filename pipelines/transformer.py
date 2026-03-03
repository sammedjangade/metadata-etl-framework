import pandas as pd

def transform(df, source_name):
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Remove null rows
    df = df.dropna()
    
    if source_name == 'customers':
        df['name'] = df['name'].str.upper()
        df['email'] = df['email'].str.lower()
        df['city'] = df['city'].str.strip()
    
    elif source_name == 'orders':
        df['amount'] = df['amount'].astype(float)
        df['order_date'] = pd.to_datetime(df['order_date'])
    
    return df   