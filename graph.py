import psycopg2
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

DB_CONFIG = {
    'dbname': 'server_data',
    'user': 'hask',
    'password': 'hask123',
    'host': 'localhost',
    'port': '5432'
}

def plot_simple(limit=100):
    """Простой график координат"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    query = """
        SELECT counter, latitude, longitude, time 
        FROM unified_data 
        WHERE latitude IS NOT NULL 
        AND longitude IS NOT NULL
        ORDER BY counter
        LIMIT %s
    """
    
    df = pd.read_sql(query, conn, params=(limit,))
    conn.close()
    
    if df.empty:
        print("Нет данных с координатами")
        return
    
    plt.figure(figsize=(12, 6))
    
   
    
    
    
    plt.scatter(df['longitude'], df['latitude'], c=df['counter'], cmap='viridis', s=50)
    plt.xlabel('Долгота')
    plt.ylabel('Широта')
    plt.title('Траектория движения (точки)')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    

if __name__ == "__main__":
    plot_simple(limit=50)