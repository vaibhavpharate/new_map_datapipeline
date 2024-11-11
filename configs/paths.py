import os
home_path = "/home/vicktor/vaib/new_map_datapipeline"

key_path = os.path.join(home_path,"secret",'allkey.pem')

source_ip = '34.139.159.47'
source_path = "/home/ubuntu/SAFNWC/SAFNWC_Export"
source_exim_path = "/home/ubuntu/SAFNWC/SAFNWC_Export/EXIM"

destination_path = os.path.join(home_path,'SAFNWC')
destination_path_exim = os.path.join(destination_path,"EXIM")

csv_path = os.path.join(home_path,"CSV")
csv_path_forecast = os.path.join(home_path,"CSV_Forecast")
