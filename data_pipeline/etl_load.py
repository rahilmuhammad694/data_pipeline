import pandas as pd
from sqlalchemy import create_engine

# --- MySQL connection string (matches your provided info) ---
mysql_connection_string = 'mysql+pymysql://root:password@localhost:3307/data_pipeline_db'
engine = create_engine(mysql_connection_string)

# ----- EXTRACT -----
try:
    df = pd.read_csv('data.csv', encoding='utf-8')
    print("Data Extracted Successfully:\n", df.head())
except Exception as e:
    print(f"Error reading CSV: {e}")
    exit()

# ----- TRANSFORM -----
try:
    df.drop_duplicates(inplace=True)
    df.fillna(0, inplace=True)
    print("Data Transformed Successfully")
except Exception as e:
    print(f"Error in Transformation: {e}")
    exit()

# ----- LOAD -----
try:
    # Use append to keep table schema (primary key, etc.) if table was pre-created in MySQL!
    df.to_sql('employee_data', con=engine, if_exists='append', index=False)
    print("Data Loaded into Database Successfully!")
except Exception as e:
    print(f"Error Loading Data: {e}")
