from sqlalchemy import create_engine, text
from configs.db_config import *
import pandas as pd


def get_connection(host,port,user,passord,database):
    connection_string = f"postgresql://{user}:{passord}@{host}/{database}"
    db_connect = create_engine(connection_string)
    try:
        with db_connect.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("\n\n---------------------Connection Successful")
        return db_connect
    except Exception as e:
        print("\n\n---------------------Connection Failed")
        print(f"{e}")


def get_transferred_files(db_connection):
    df = pd.read_sql_query("SELECT * FROM files_map_logs.transfer_logs WHERE status='transferred' and read_status=0 ORDER BY timestamp DESC",con=db_connection)
    df = df.sort_values('timestamp',ascending=False)
    return df


def get_ct_ci_map(db_connection):
    df = pd.read_sql_query("SELECT * FROM td_satellite.ct_ci_map",con=db_connection)
    return df


def update_read_files(db_connection,variable,timestamp):
    pass

def get_last_read_file(db_connection):
    df = pd.read_sql_query("SELECT * FROM files_map_logs.transfer_logs WHERE read_status=1 ORDER BY timestamp DESC",con=db_connection)
    return df

def get_latest_var_read_file(db_connection,var):
    df = pd.read_sql_query(f"SELECT * FROM files_map_logs.transfer_logs WHERE read_status=1 and variable = '{var}' ORDER BY timestamp DESC",con=db_connection)
    return df
