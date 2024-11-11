import pandas as pd
import numpy as np
import os
import paramiko
from datetime import datetime, timedelta
import shutil
import numpy as np
import netCDF4 as nc
from sqlalchemy import text


from configs.paths import source_ip,key_path,source_path,destination_path, source_exim_path,destination_path_exim
from configs.db_config import data_configs_map, data_send
from db_functions import get_connection,get_transferred_files,get_last_read_file,get_latest_var_read_file
from configs.variables import read_variables,variable_atts

date_format = "%Y%m%d"
timestamp_format = "%Y-%m-%d %H:%M:%S"
file_timestamp_format = date_format+"T%H%M00Z"


file_logs_schema = 'files_map_logs'
## select the timestamp that was last read

fcst_thld = 1 # this is in hours

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

variables = ['CT','CTTH']

ct_exim_format = 'S_NWC_EXIM-CT_MSG2_IODC-VISIR'
ctth_exim_format = 'S_NWC_EXIM-CT_MSG2_IODC-VISIR'
exim_format = {'CT':ct_exim_format,"CTTH":ctth_exim_format}
# get list of exim files

## do it for ct now
def get_ssh():
    ssh = paramiko.SSHClient() ## Create the SSH object
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) # no known_hosts error
    try:
        ssh.connect(source_ip, username='ubuntu', key_filename=key_path)
    except Exception as e:
        print("There was an error")
        print(e)
    else:
        print("Connected Securely to the Source Server")
    return ssh



# get list of all the exim files available
def get_exim_files(ssh_client,usa_date):
    stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_exim_path}/{usa_date}')
    variable_files = stdout.readlines()
    variable_files = [str(x)[:-1] for x in variable_files]
    variable_files = list(filter(lambda x: x.endswith('.nc'),variable_files))
    
    ## create dataframe of files
    files_df = pd.DataFrame({'file':variable_files})
    files_df['variable'] = files_df['file'].str.split("_").str[2].str[-2:]
    files_df['timestamp'] = files_df['file'].str.split("_").str[-2]
    files_df['timestamp'] = pd.to_datetime(files_df['timestamp'],format=file_timestamp_format) + timedelta(hours=5,minutes=30)
    files_df['add_time_mins'] = files_df['file'].str.split("_").str[-1].str[:-3].astype(int)
    files_df['add_time'] = pd.to_timedelta(files_df['add_time_mins'],unit='m')
    files_df['forecasted_for'] = files_df['timestamp'] + files_df['add_time']
    files_df = files_df.loc[files_df['variable'].isin(read_variables),:]
    return files_df
    



def transfer_exim_files(ssh_client,usa_date,timestamp,file_name,forecast_timestamp,variable,db_connection,data_connection):
    sftp_client = ssh_client.open_sftp()
    usa_timestamp = timestamp - timedelta(hours=5,minutes=30)
    # usa_time_text = usa_timestamp.strftime(file_timestamp_format)
    tmstp = timestamp.strftime(file_timestamp_format)
    fcst_tmstp = forecast_timestamp.strftime(file_timestamp_format)
    if os.path.exists(f'{destination_path}/EXIM/{usa_date}') == False:
        os.mkdir(f'{destination_path}/EXIM/{usa_date}')

    try:
        if os.path.exists(f'{destination_path_exim}/{usa_date}/{file_name}')==False:
            sftp_client.get(f'{source_exim_path}/{usa_date}/{file_name}',f'{destination_path_exim}/{usa_date}/{file_name}')
            df_log = pd.DataFrame({'timestamp':[forecast_timestamp],'variable':[variable],
                                   'status':['transferred'],
                                   'log_ts':[datetime.now()],'file':[file_name],
                                   'read_status':[0],
                                   'source_timestamp':[timestamp]})
                
            df_log.to_sql(schema=file_logs_schema,
                          name='transfer_exim_logs',
                          if_exists='append',
                          con=db_connection,
                          index=False)
            ## forecast_time search with old_source timestamp if old_source < new_source delete the forecast
            last_forecast_update = pd.read_sql_query(f"SELECT * FROM files_map_logs.forecast_logs WHERE fcst_timestamp = '{forecast_timestamp}' and variable = '{variable}' order by source_time desc limit 1",con=db_connection)
            if len(last_forecast_update)>0:
                updated_source = last_forecast_update['source_time']
                if updated_source < timestamp:
                    ## delete the forecast table
                    with db_connection.connect() as conn:
                        conn.execute(text(f"DELETE FROM data_forecast.{variable.lower()} WHERE timestamp='{forecast_timestamp}'"))
                        conn.commit()
                elif updated_source == timestamp:
                    print("Latest source forecast already in place")
                    return timestamp
            
            file_path = os.path.join(destination_path_exim,usa_date,file_name)
            data = nc.Dataset(file_path)
            df = pd.DataFrame()
            df['lat'] = np.array(data.variables['lat'][:]).flatten()
            df['lon'] = np.array(data.variables['lon'][:]).flatten()
            df['timestamp'] = forecast_timestamp
        
            for i in variable_atts[variable]:
                fill_value = data.variables[i].getncattr('_FillValue') if '_FillValue' in data.variables[i].ncattrs() else None
                df[i] = np.array(data.variables[i][:]).flatten()
                df.loc[df[i]==fill_value,i] = None
            print(df)
            try:
            # check if the data exists
                resp = df.to_sql(schema='data_forecast',
                                 name=variable.lower(),
                                 index=False,
                                 if_exists='append',
                                 con=data_connection,
                                 method='multi',          # Batch inserts
                                 chunksize=1000)
                if resp:
                    df_db = pd.DataFrame({'fcst_timestamp':[forecast_timestamp],'variable':[variable],'source_time':[timestamp],
                                          'log_ts':[datetime.now()],'file':[file_name],'read_status':[1]})
                    df_db.to_sql(schema=file_logs_schema,name='forecast_logs',if_exists='append',con=db_connection,index=False)
            except Exception as e:
                print("Error Occured on Data EXIM Transfer")
                print(e)    
        
        else:
            print(f"File already exists {file_name}")
    except Exception as e:
        print("Error at EXIM transfer files")
        print(e)
    
    

ssh_client = get_ssh()
for var in variables:
    # print(var)
    read_files = get_latest_var_read_file(db_connection=db_connection,var=var)
    latest_timestamp = read_files['timestamp'].max()
    forecast_end = latest_timestamp + timedelta(hours=fcst_thld)
    
    usa_timestamp = latest_timestamp - timedelta(hours=5,minutes=30)
    usa_date = usa_timestamp.strftime("%Y%m%d")

    exim_files = get_exim_files(ssh_client,usa_date=usa_date)
    exim_files = exim_files.loc[exim_files['variable']==var,:]
    if len(exim_files)> 0:
        target_files = exim_files.loc[((exim_files['timestamp']==latest_timestamp)&(exim_files['forecasted_for']<=forecast_end)),:]
        # print(target_files)
        for index,row in target_files.iterrows():
            transfer_exim_files(ssh_client=ssh_client,usa_date=usa_date,
                           timestamp=row['timestamp'],
                           forecast_timestamp=row['forecasted_for'],
                           variable=var,file_name=row['file'],
                           db_connection=db_connection,
                           data_connection=data_connection)
            
    else:
        print(f"NO EXIM files for {var}")



