from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import netCDF4 as nc
import os
from sqlalchemy import text
 
from configs.db_config import data_configs_map, data_send
from db_functions import get_connection,get_transferred_files, get_ct_ci_map
from configs.variables import variable_atts,read_variables
from configs.paths import source_path,destination_path

## get the list of files transferred
## read the files and transfer to database
date_format = "%Y%m%d"
timestamp_format = "%Y-%m-%d %H:%M:%S"
db_connection = get_connection(host=data_configs_map['host'],
                               passord=data_configs_map['password'],
                               user=data_configs_map['user'],
                               database=data_configs_map['database'],
                               port=data_configs_map['port'])

data_connection = get_connection(host=data_send['host'],
                               passord=data_send['password'],
                               user=data_send['user'],
                               database=data_send['database'],
                               port=data_send['port'])

ct_ci_map = get_ct_ci_map(db_connection=db_connection)
transf_files = get_transferred_files(db_connection=db_connection)

def read_file(timestamp,db_connection,variable,file_name,data_connection):
    us_timestamp = timestamp - timedelta(hours=5,minutes=30)
    source_date_folder = us_timestamp.strftime("%Y%m%d")
    destination_date = timestamp.strftime("%Y%m%d")
    timestamp_text = timestamp.strftime(timestamp_format)
    # update status
    
    #previous_24 = timestamp - timedelta(hours=24)
    #previous_24 = previous_24.strftime(format='%Y-%m-%d %H:%M:%S')
    
    file_path = os.path.join(destination_path,source_date_folder,file_name)
    if os.path.exists(file_path):
        data = nc.Dataset(file_path)
        df = pd.DataFrame()
        df['lat'] = np.array(data.variables['lat'][:]).flatten()
        df['lon'] = np.array(data.variables['lon'][:]).flatten()
        df['timestamp'] = timestamp
        
        for i in variable_atts[variable]:
                fill_value = data.variables[i].getncattr('_FillValue') if '_FillValue' in data.variables[i].ncattrs() else None
                df[i] = np.array(data.variables[i][:]).flatten()
                df.loc[df[i]==fill_value,i] = None
                # df = df.dropna()       

        ## save CSV
        
        ## send to database
        try:
            # check if the data exists
            resp = df.to_sql(schema='data_extract',name=variable.lower(),index=False,if_exists='append',con=data_connection)
            if resp:
                with db_connection.connect() as conn:
                    conn.execute(text(f"UPDATE files_map_logs.transfer_logs SET read_status=1 where timestamp = '{timestamp_text}' AND variable='{variable}'"))
                    conn.commit()
        except Exception as e:
            print("Error Occured on Data Transfer")
            print(e)
    else:
        print("File Does not exist")
        
    

for index,row in transf_files.head(2).iterrows():
    df = read_file(timestamp=row['timestamp'],
                   variable=row['variable'],
                   file_name=row['file'],
                   db_connection=db_connection,
                   data_connection=data_connection)
    print(df)