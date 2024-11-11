import pandas as pd
import numpy as np
import os
import paramiko
from datetime import datetime, timedelta
import shutil


from configs.paths import source_ip,key_path,source_path,destination_path
from configs.db_config import data_configs_map
from db_functions import get_connection,get_transferred_files
from configs.variables import read_variables

date_format = "%Y%m%d"
timestamp_format = "%Y-%m-%d %H:%M:%S"
file_timestamp_format = date_format+"T%H%M00Z"


file_logs_schema = 'files_map_logs'

## get the ssh connection
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

## choose the latest date folder available in source 
def choose_latest_date(ssh_client,source_path,folder_format):
    stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}')
    date_folder_list = stdout.readlines()
    date_folder_list = [str(x)[:-1] for x in date_folder_list]
    date_folder_list.remove("EXIM")
    date_folder_dates  = [datetime.strptime(x,folder_format) for x in date_folder_list]
    date_folder_dates.sort()
    latest_date = date_folder_dates[-1]

    choosing_latest =latest_date.strftime(folder_format)
    return choosing_latest



def get_variablle_files(ssh_client,latest_date):
    stdin, stdout, stderr = ssh_client.exec_command(f'ls {source_path}/{latest_date}')
    variable_files = stdout.readlines()
    variable_files = [str(x)[:-1] for x in variable_files]
    variable_files = list(filter(lambda x: x.endswith('Z.nc'),variable_files))
    
    ## create dataframe of files
    files_df = pd.DataFrame({'file':variable_files})
    files_df['variable'] = files_df['file'].str.split("_").str[2]
    files_df['timestamp'] = files_df['file'].str.split("_").str[-1].str[:-3]
    files_df['timestamp'] = pd.to_datetime(files_df['timestamp'],format=file_timestamp_format)
    files_df = files_df.loc[files_df['variable'].isin(read_variables),:]
    return files_df
    




def transfer_file(ssh_client,latest_date,file_name,timestamp,variable,db_connection):
    sftp_client = ssh_client.open_sftp()
    if os.path.exists(f'{destination_path}/{latest_date}') == False:
        os.mkdir(f'{destination_path}/{latest_date}')

    # create EXIM FILES
    if os.path.exists(f'{destination_path}/EXIM/{latest_date}') == False:
        os.mkdir(f'{destination_path}/EXIM/{latest_date}')

    yesterday = datetime.now().date() - timedelta(days=1)
    yesterday = yesterday.strftime("%Y%m%d")
    lst_folders = list(os.walk(f'{destination_path}'))[0][1]
    for x in lst_folders:
        if x!= latest_date and x!=yesterday and x!='EXIM':
            print(f"Removing older folder {x}")
            shutil.rmtree(f'{destination_path}/{x}', ignore_errors=True)
    tmstp = timestamp + timedelta(hours=5,minutes=30)
    try:
        if os.path.exists(f'{destination_path}/{latest_date}/{file_name}')==False:
                sftp_client.get(f'{source_path}/{latest_date}/{file_name}',f'{destination_path}/{latest_date}/{file_name}')
                df = pd.DataFrame({'timestamp':[tmstp],'variable':[variable],'status':['transferred'],'log_ts':[datetime.now()],'file':[file_name],'read_status':[0]})
                df.to_sql(schema=file_logs_schema,name='transfer_logs',if_exists='append',con=db_connection,index=False)
        else:
            print(f"File already exists {x}")
    except Exception as e:
        print("Error at transfer files")
        print(e)
        

ssh_client = get_ssh()
db_connection = get_connection(host=data_configs_map['host'],
                               passord=data_configs_map['password'],
                               user=data_configs_map['user'],
                               database=data_configs_map['database'],
                               port=data_configs_map['port'])

latest_date = choose_latest_date(ssh_client=ssh_client,
                                 source_path=source_path,
                                 folder_format=date_format)
available_source_files = get_variablle_files(ssh_client=ssh_client,
                                            latest_date=latest_date)


## get the files that are already done
done_files = get_transferred_files(db_connection=db_connection)
list_done_files = list(set(done_files['file']))

## transfer files
files_left = available_source_files.loc[~available_source_files['file'].isin(list_done_files),:]
for index,row in files_left.iterrows():
    transfer_file(ssh_client=ssh_client,
                  file_name=row['file'],
                  latest_date=latest_date,
                  timestamp=row['timestamp'],
                  variable=row['variable'],
                  db_connection=db_connection)