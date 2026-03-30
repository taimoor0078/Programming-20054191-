import sqlite3
import pandas as pd

class GameDatabase:
    def __init__(self, db_name="games_data.db"):
        self.db_name = db_name
    def connect(self):
        return sqlite3.connect(self.db_name)
    # Insert Function
    def insert_dataframe(self, df, table_name, if_exists="append"):
        conn = self.connect()
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        conn.close()
        print(f"Data inserted into table: {table_name}")
    # Read Data
    def read_table(self, table_name):
        conn = self.connect()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df