import sys
import os
import pyodbc as odbc
import pandas as pd
import datetime
import textwrap as indent
import json

########################## Pandas Settings ################################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
###########################################################################


##### Parms / Variables #####
Model_Name = ""
variance_days = 7
delta_days= 1
acct_code_lwr= 40000
acct_code_upr = 49999
pwd = open('C:/Users/545001/Python_Automation/disKys.txt').readline()

current_year = datetime.datetime.strftime(datetime.datetime.now() -
    datetime.timedelta(days=delta_days, hours=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%H')), minutes=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%M')), seconds=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%S'))), '%Y')#/%m/%d %H:%M:%S %p')

current_day = datetime.datetime.strftime(datetime.datetime.now() -
    datetime.timedelta(days=delta_days, hours=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%H')), minutes=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%M')), seconds=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%S'))), '%Y-%m-%d')


##########################################
##### Synapse Retail Sale Connection #####
##########################################
server = ''
database = ''
username = ''
password = pwd
driver= '{ODBC Driver 17 for SQL Server}'
conn_synapse = odbc.connect(f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password};Authentication=ActiveDirectoryPassword;Encrypt=yes;')

##################################################
##### Synapse DDL for Azure Analysis Sevices #####
##################################################
os.remove(fr'./{Model_Name} DDL.txt')
External_Tables = open(fr'./{Model_Name} External_Tables.sql','a')


schema_name = "RETAIL_DATA"
table_name= "Z3_ET_FACT_RETAIL_FORECAST"

df_base_synapse_ddl = pd.read_sql(
f'''
Select ext.name												as Table_Name,
CONCAT(Col.name,' ' ,typ.name,
case 
when typ.name in ('varchar') 
	then concat(' (',Col.max_length,')')
when typ.name in ('numeric') 
	then concat(' (',Col.precision,', ',Col.max_length,')')
else '' end
)															as Query_DDL,
Col.Column_id												as Column_Id,
Col.name													as Column_Name,
typ.name													as Data_Type,
Col.max_length												as Column_Length,
Col.precision												as Precision,
ext.Location                                                as Location,
ds.name                                                     as Data_Source_Name,
ds.location                                                 as Data_Source_URL,
fmt.name                                                    as File_Format_Id
from sys.external_tables ext
inner join sys.columns col 
	on  ext.object_id = col.object_id
inner join sys.types typ 
	on typ.system_type_id = col.system_type_id
inner join sys.external_data_sources ds 
    on ds.data_source_id = ext.data_source_id
inner join sys.external_file_formats fmt
    on fmt.file_format_id = ext.file_format_id
Order by ext.object_id asc,col.Column_id asc                         
''', con=conn_synapse)

list_of_tables = df_base_synapse_ddl["Table_Name"].unique()
for table in list_of_tables:
    ##################################################
    #####  Generate Queries for external Tables  #####
    ##################################################
    ddl_filter = f"where ext.name = '{table}'"
    df_synapse_ddl = pd.read_sql(
        f'''
    Select ext.name												as Table_Name,
    CONCAT(Col.name,' ' ,typ.name,
    case 
    when typ.name in ('varchar') 
    	then concat(' (',Col.max_length,')')
    when typ.name in ('numeric') 
    	then concat(' (',Col.precision,', ',Col.max_length,')')
    else '' end
    )															as Query_DDL,
    Col.Column_id												as Column_Id,
    Col.name													as Column_Name,
    typ.name													as Data_Type,
    Col.max_length												as Column_Length,
    Col.precision												as Precision,
    ext.Location                                                as Location,
    ds.name                                                     as Data_Source_Name,
    ds.location                                                 as Data_Source_URL,
    fmt.name                                                    as File_Format_Id
    from sys.external_tables ext
    inner join sys.columns col 
    	on  ext.object_id = col.object_id
    inner join sys.types typ 
    	on typ.system_type_id = col.system_type_id
    inner join sys.external_data_sources ds 
        on ds.data_source_id = ext.data_source_id
    inner join sys.external_file_formats fmt
        on fmt.file_format_id = ext.file_format_id
    {ddl_filter}
    Order by ext.object_id asc,col.Column_id asc                         
    ''', con=conn_synapse)

    ### parms for ddl statement ###
    location_ddl = f"'{df_synapse_ddl['Location'].head(1)[0]}'"
    datasource_ddl = f"'{df_synapse_ddl['Data_Source_Name'].head(1)[0]}'"
    filefromat_ddl = f"'{df_synapse_ddl['File_Format_Id'].head(1)[0]}'"
    column_ddl = str(json.dumps({'query': tuple(i for i in df_synapse_ddl['Query_DDL'])}["query"], indent=4)).replace(
        '"', "").replace("[", "(").replace("]", ")")
    formatted_column_ddl = column_ddl

    Create_External_Table = f'''Create External Table {schema_name}.{table}
    {formatted_column_ddl}
    With ( DATA_SOURCE = {datasource_ddl},
    LOCATION = {location_ddl},
    FILE_FORMAT = {filefromat_ddl},
    )
    GO;
    Select top 10 * from {schema_name}.{table_name};
    '''
    print(Create_External_Table,file=External_Tables)
    #################################################

    ##################################################
    #####      Generate Bim file for Model       #####
    ##################################################



