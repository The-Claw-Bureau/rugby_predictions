<<<<<<< HEAD
import pandas as pd
import psycopg2
import datetime
import sys
import numpy as np
import datetime
import traceback
import math
from pandas import NaT
from io import BytesIO
import requests
import tempfile
import importlib.util


def import_github_module(module_name, github_url):
    """
    Downloads a Python file from GitHub and imports it as a module.

    Parameters:
    - module_name (str): The name to assign to the module (e.g., 'mf').
    - github_url (str): The raw GitHub URL of the Python file.

    Returns:
    - module: The imported module.
    """
    # Fetch the script from GitHub
    response = requests.get(github_url)
    
    if response.status_code == 200:
        # Create a temporary file to store the script
        temp_script_path = tempfile.NamedTemporaryFile(delete=False, suffix=".py").name
        with open(temp_script_path, "w") as f:
            f.write(response.text)

        # Load the module dynamically
        spec = importlib.util.spec_from_file_location(module_name, temp_script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        print(f"✅ Successfully imported '{module_name}' from GitHub.")
        return module
    else:
        print("❌ Failed to fetch the script. Check the URL.")
        return None
    
    


def retrieve_files_to_dataframe(blob_parameters, container_name, prefix, strings_to_filter=[], dtypes = {}):
    
    blob_service_client = connect_to_blob(blob_parameters)
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs(name_starts_with=prefix)
    
    if len(strings_to_filter) > 0:
        filtered_blobs = []
        for blob in blobs:
            blob_name = blob.name
            if any(string in blob_name for string in strings_to_filter):
                filtered_blobs.append(blob)

        blobs = filtered_blobs
        
    file_data = []
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob)
        blob_data = blob_client.download_blob().readall()
        file_name = blob.name.split('/')[-1]  # Extract filename from blob's name
        file_data.append((file_name, BytesIO(blob_data)))  # Store filename with file data

    # Convert the list of tuples (filename, file data) into a list of DataFrames
    dfs = [pd.read_csv(file[1], dtype=dtypes) for file in file_data]

    # Add filename column to each DataFrame
    for i, df in enumerate(dfs):
        df['filename'] = file_data[i][0]

    # Concatenate all DataFrames into a single DataFrame
    concatenated_df = pd.concat(dfs, ignore_index=True)
    
    return concatenated_df


def connect_to_blob(blob_parameters):
    
    blob_account_name = blob_parameters['blob_account_name'] # fill in your blob account name
    blob_account_key = blob_parameters['blob_account_key']
    account_url = blob_parameters['account_url']

    blob_service_client = BlobServiceClient(account_url=account_url, account_name=blob_account_name, account_key=blob_account_key)
        
    return blob_service_client 




def get_blob(container, blob_name):
    
    local_start_time = datetime.datetime.now()
    print('Getting blob')
    
    downloaded_blob = container.download_blob(blob_name)
    downloaded_file = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
    
    local_end_time = datetime.datetime.now()
    print('Get complete: ' + str(local_end_time - local_start_time))

    return downloaded_file






def postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data):
    
    conn = None
    cur = None
    
    try:
        
        
        conn = psycopg2.connect(
          host = POSTGRESQL_PARAMS['host'],
          database = POSTGRESQL_PARAMS['DB'],
          user = POSTGRESQL_PARAMS['username'],
          password = POSTGRESQL_PARAMS['pass']
        )
        conn.set_client_encoding('UTF-8')
        cur = conn.cursor()
        cur.execute(sql_statement)


        if retrieve_data:
            temp = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
            for col in temp.columns:
                if (col == 'password') | (col == 'token') | (col =='session'):
                    temp.drop(col, axis = 1, inplace = True)  
            conn.commit()
            return temp
        
        else:
            conn.commit()
            return None
    
    
    except Exception as e:

        print('An error has occured inside this function - postgres_Retreive_Insert')
        print(f"An exception of type {type(e).__name__} occurred: {str(e)}")
        traceback.print_exc()
        return None


    
    finally:
        
        if cur:
            cur.close()
            conn.close()




def format_data_for_postgres(formatted_data, powerbi_table_info):
    
    # Format data to add to the database

    formatted_data.replace("'", "''", inplace = True)
    formatted_data.replace(to_replace = '', value = 'NULL', inplace = True)
    formatted_data.replace(to_replace = 'null', value = 'NULL', inplace = True)


    # Replace nulls with NULL
    for col in formatted_data.columns:
        if formatted_data[col].dtype == 'timedelta64[ns]':
            formatted_data[col] = formatted_data[col].astype(str)
            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if x == 'NaT' else x)


    formatted_data.fillna('NULL', inplace = True)
    # formatted_data.fillna(pd.NA, inplace=True)


    for col in formatted_data.columns:

        #print(col)
        column_info = powerbi_table_info[ powerbi_table_info['column_name'] == col]
        if len(column_info) == 0:
            print(col + ' has no information in this table')
            raise ValueError("This is a deliberate error.")

        else:
            column_type = column_info['data_type'].iloc[0]

        if (column_type == 'numeric') | (column_type == 'float') | (column_type == 'double precision'):

            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if x == 'NULL' else float(x))

        elif column_type == 'boolean':

            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if x == 'NULL' else False if x == 0 else True)
            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).upper() )
            #formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x).upper() + "'")


        elif column_type == 'timestamp without time zone':

            #formatted_data[col] = pd.to_datetime(formatted_data[col])
            formatted_data[col] = formatted_data[col].apply(lambda x: convert_datetimes(x))
            #formatted_data[col].apply(lambda x: 'NULL' if ( pd.isna(x) | (x == 'NULL') ) else pd.to_datetime(datetime.datetime.strptime(str(x), '%d-%m-%Y %H:%M:%S')) if str(x)[2]=='-' else pd.to_datetime(datetime.datetime.strptime(str(x), '%d/%m/%Y %H:%M:%S')) if str(x)[2]=='/' else pd.to_datetime(datetime.datetime.strptime(str(x), '%Y/%m/%d %H:%M:%S')) if str(x)[4]=='/' else pd.to_datetime(datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S')) if str(x)[4]=='-' else 'NULL')
            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == 'None') | (pd.isna(x)) else "'" + str(x) + "'")


            
        elif column_type == 'timestamp with time zone':

            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == 'None') | (pd.isna(x)) else "'" + str(x) + "'")

            
 
        elif column_type == 'time without time zone':

            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == 'None') | (pd.isna(x)) else "'" + str(x) + "'")

            
            
        elif column_type == 'date':
            formatted_data[col] = formatted_data[col].apply(lambda x: convert_datetimes(x))
            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == 'None') | (pd.isna(x)) else "'" + str(x) + "'")

            
            
        elif column_type == 'interval':

            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")


        elif column_type == 'uuid':

            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")


        elif column_type == 'ARRAY':

            array_type = powerbi_table_info[ powerbi_table_info['column_name'] == col]['udt_name'].iloc[0]

            #formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("False", 'false'))
            #formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("True", 'true'))
            #formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("'", '"'))
            #formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("\\", '\\\\'))

            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("False", 'false').replace("True", 'true').replace("'", '"').replace("\\", '\\\\'))

            if (array_type == '_json'):
                array_type_string = '::json[]'

                #formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("{", "'{"))
                #formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("}", "}'"))

                formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("{", "'{").replace("}", "}'"))
                formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'" )
                formatted_data[col] = formatted_data[col].apply(lambda x: "['{" + str(x)[1:-1] + "}']" if str(x)[1:2].isnumeric() else x)

                
#             elif (array_type == '_jsonb'):
#                 array_type_string = '::jsonb'

#                 formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("{", "'{").replace("}", "}'"))
#                 formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'" )
#                 formatted_data[col] = formatted_data[col].apply(lambda x: "['{" + str(x)[1:-1] + "}']" if str(x)[1:2].isnumeric() else x)

                
            elif array_type == '_int4':
                array_type_string = '::integer[]'


            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == '[]') else "ARRAY[" + str(x)[1:-1] + "]" + str(array_type_string))



        elif column_type == 'json':

            formatted_data[col] = formatted_data[col].replace("'", '"')
            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x).replace("'", '"') + "'")

            
        elif column_type == 'jsonb':

            formatted_data[col] = formatted_data[col].replace("'", '"')
            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x).replace("'", '"') + "'")

            

        elif column_type == 'character varying':
            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("'", '"'))
            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")


        elif column_type == 'text':

            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("'", '"'))
            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")
            

        elif column_type == 'integer':

            formatted_data[col] = formatted_data[col].apply(lambda x: x if x == str("NULL") else "NULL" if x == '' else int(float(x)))

        else:
            print('DATA TYPE IS NOT COVERED - FIX - %s'%(column_type))


    formatted_data.replace("'NULL'", 'NULL', inplace = True)
    
    return formatted_data




def convert_datetimes(datetime_obj):


        datetime_obj = str(datetime_obj)


        if datetime_obj == 'NULL':
            datetime_obj =  'NULL'
            return


        if datetime_obj.find('NaT')>=0:
            datetime_obj = None
            return
        
        # if datetime_obj == 'NaT':
        #     datetime_obj =  'NULL'
        #     return


        if datetime_obj.find(',')>0:
            datetime_obj = datetime_obj.replace(',', '')

        if datetime_obj.find('T')>0:
            datetime_obj = datetime_obj.replace('T', ' ')

        elif len(datetime_obj) == 10:

            if str(datetime_obj)[2] == '-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d-%m-%Y'))
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
            elif str(datetime_obj)[2] == '/':

                if int(str(datetime_obj)[3:5]) > 12:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%m/%d/%Y')) 
                else:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d/%m/%Y')) 
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
            elif str(datetime_obj)[4]=='-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d'))
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
            elif str(datetime_obj)[4]=='/':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d')) 
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
            else:
                datetime_obj =  'NULL'


        elif len(datetime_obj) == 32:
            
            if str(datetime_obj)[4]=='-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d %H:%M:%S.%f%z'))

            if str(datetime_obj)[4]=='/':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d %H:%M:%S.%f%z'))

            if str(datetime_obj)[2]=='-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d-%m-%Y %H:%M:%S.%f%z'))

            if str(datetime_obj)[2]=='/':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d/%m/%Y %H:%M:%S.%f%z'))

            datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')


        elif len(datetime_obj) == 25:


            # if str(datetime_obj)[4]=='-':
                
            datetime_obj =  datetime.datetime.strftime(pd.to_datetime(datetime_obj), '%Y-%m-%d %H:%M:%S')



        elif len(datetime_obj) > 10:

            if str(datetime_obj)[2] == '-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d-%m-%Y %H:%M:%S'))
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
            elif str(datetime_obj)[2] == '/':
                if int(str(datetime_obj)[3:5]) > 12:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%m/%d/%Y %H:%M:%S')) 
                else:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d/%m/%Y %H:%M:%S')) 
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
            elif str(datetime_obj)[4]=='-':
                
                if str(datetime_obj).find('.') > 0:
                    
                    datetime_obj = datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d %H:%M:%S.%f')
                    datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')

                else:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d %H:%M:%S'))
                    datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
                    
            elif str(datetime_obj)[4]=='/':
                
                if str(datetime_obj).find('.') > 0:

                    datetime_obj = datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d %H:%M:%S.%f')
                    datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
                    
                else:
                    
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d %H:%M:%S')) 
                    datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')

            else:
                datetime_obj =  'NULL'

        else:
            datetime_obj =  'NULL'


        if len(datetime_obj) == 10:
            datetime_obj = datetime_obj + ' 00:00:00'


        return datetime_obj




# Create sql statement For inserts

def insert_sql(table_name, formatted_data, POSTGRESQL_PARAMS):
    
    last_save = 0
    global_error = False
    
    sql_complete = ''
    sql_statement_part1 = 'insert into ' + table_name + ' ('

    for col in formatted_data.columns:
        sql_statement_part1 = sql_statement_part1 + '"' + col + '", '

    sql_statement_part1 = sql_statement_part1[:-2] + ') values '


    for new_row in range (0, len(formatted_data)):
        #print(new_row, len(formatted_data))

        sql_statement_part2 = ''

        for col in formatted_data.columns:
            
            sql_statement_part2 = sql_statement_part2 + str(formatted_data.iloc[new_row][col]) + ', '

        sql_statement_part2 = '(' + sql_statement_part2[:-2] + '), '

        sql_complete = sql_complete + sql_statement_part2
        
        
        
        if (new_row - last_save) >= 10000:
        
            sql_complete = sql_statement_part1 + sql_complete[:-2] + ';'
            no_data = postgres_Retreive_Insert(sql_complete, POSTGRESQL_PARAMS, retrieve_data = False)
            
            sql_complete = ''
            last_save = new_row
            
            
    # It hasn't just saved
    if (last_save != new_row) | (new_row == 0):
        
        sql_complete = sql_statement_part1 + sql_complete[:-2] + ';'
        no_data = postgres_Retreive_Insert(sql_complete, POSTGRESQL_PARAMS, retrieve_data = False)

    print(str(sys._getframe().f_code.co_name) + ' - Inserts Complete - ' + str(len(formatted_data)))
    
    return




# Create sql statement For inserts

def update_sql(table_name, formatted_data, table_id_column, POSTGRESQL_PARAMS):
    # Create sql statement For inserts
    
    global_error = False
    
    sql_queries = []
    last_save = 0
    new_row = 0

    sql_all = ''

    sql_statement_part1 = 'update ' + table_name + ' set '

    for new_row in range (0, len(formatted_data)):

        sql_complete = ''

        sql_statement_part2 = ''

        for col in formatted_data.columns:
            
            if col != table_id_column:
                sql_statement_part2 = sql_statement_part2 + '"' + str(col) + '" = ' + str(formatted_data.iloc[new_row][col]) + ', '

        #sql_statement_part2 = '(' + sql_statement_part2[:-2] + '), '
        sql_statement_part2 = sql_statement_part2[:-2]
        sql_statement_part2 = sql_statement_part2 + ' where ' + str(table_id_column) + ' = ' + str(formatted_data.iloc[new_row][table_id_column])  + "'"

        #sql_complete = sql_complete + sql_statement_part2


        sql_complete = sql_statement_part1 + sql_statement_part2[:-1] + ';'


        sql_all = sql_all + ' ' + sql_complete
        
        if ( (new_row - last_save) >= 10000) | (new_row == len(formatted_data)):
            
            no_data = postgres_Retreive_Insert(sql_all, POSTGRESQL_PARAMS, retrieve_data = False)

            
            sql_all = ''
            last_save = new_row
            
            
    if (last_save != new_row) | ( (new_row == 0) & (sql_all != '')):
        no_data = postgres_Retreive_Insert(sql_all, POSTGRESQL_PARAMS, retrieve_data = False)    

    print(str(sys._getframe().f_code.co_name) + ' - Update Complete - ' + str(len(formatted_data)))
    
    return



def get_source_last_updated(system_run_times_table, table_name, task_name, POSTGRESQL_PARAMS):
    
    sql_statement =  "SELECT min(last_updated) FROM " + system_run_times_table + " WHERE table_name = '" + str(table_name) + "' and task_name = '" + str(task_name) + "';"
    last_updated = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = True)
    last_updated = last_updated.iloc[0][0]
    
        
    if last_updated is None:
        last_updated = pd.to_datetime('1970-01-01')
        
    else:
        # We are taking off an hour for daylght savings - Need to fix this
        last_updated = last_updated - datetime.timedelta(minutes = 60)
            
    return last_updated




def set_source_last_updated(system_run_times_table, table_name, task_name, last_updated, POSTGRESQL_PARAMS):
    
    sql_statement =  "select * from " + system_run_times_table + " where table_name = '" + str(table_name) + "' and task_name = '" + str(task_name) + "';"
    row_count = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = True)
    
    if len(row_count) == 0:
        
        sql_statement =  "insert into " + system_run_times_table + " (table_name, task_name, last_updated) values ('" + str(table_name) + "', '" + str(task_name) + "', '" + str(last_updated) + "');"
        no_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = False)

    elif len(row_count) > 1:
        
        sql_statement =  "delete from " + system_run_times_table + " where table_name = '" + str(table_name) + "' and task_name = '" + str(task_name) + "';"
        no_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = False)

        sql_statement =  "insert into " + system_run_times_table + " (table_name, task_name, last_updated) values ('" + str(table_name) + "', '" + str(task_name) + "', '" + str(last_updated) + "');"
        no_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = False)

    else:
        
        sql_statement =  "update " + system_run_times_table + " set last_updated = '" + str(last_updated) + "' where table_name = '" + str(table_name) + "' and task_name = '" + str(task_name) + "';"
        no_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = False)
        
    return
    
    
    


def get_table_info(table_name, POSTGRESQL_PARAMS):
    
    sql_statement = "SELECT column_name, data_type, udt_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + str(table_name) + "' ORDER BY ORDINAL_POSITION;"
    table_info = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = True)
    
    return table_info



def compare_evals(vec):
    
    try:

        if vec[2] == True:
            return True
        
        else:

            first_value = str(vec[0])
            second_value = str(vec[1])

            if pd.isna(first_value) & pd.isna(second_value):

                return True
            
        
            elif (pd.isna(first_value) and not pd.isna(second_value)) | (pd.isna(second_value) and not pd.isna(first_value)):

                return False
            



            else:


                if (first_value is np.nan) and (second_value is np.nan):

                    return True

                elif (first_value is np.nan) and not (second_value is np.nan):

                    return False
                
                elif (second_value is np.nan) and not (first_value is np.nan):

                    return False
                
                elif (first_value == 'nan') & (second_value == 'nan'):

                    return True

                elif (first_value == 'nan') & (second_value != 'nan'):

                    return False

                elif (first_value != 'nan') & (second_value == 'nan'):

                    return False

                first_value = eval(first_value)
                second_value = eval(second_value)

                if ((type(first_value) == int) | (type(first_value) == float)) & ((type(second_value) == int) | (type(second_value) == float)):
                    
                    if math.isnan(first_value) & math.isnan(second_value):

                        return True
                    
                    else:

                        return False
                
                if ((type(first_value) == int) | (type(first_value) == float)) | ((type(second_value) == int) | (type(second_value) == float)):

                    return False


                elif (first_value) == (second_value):
                    
                    return True
                
                else:
                    
                    return False
        
        
    except Exception as e:

        # print('An error has occured inside this function - compare_evals')
        # print(f"An exception of type {type(e).__name__} occurred: {str(e)}")
        # traceback.print_exc()
        return vec[2]



    

def update_required_events(table_to_update, powerbi_table_info, events_to_update, original_df, cols_to_check, names_columns, id_column, POSTGRESQL_PARAMS):

    try:

        print('Checking for events to update')
        
        ### This is used to compare the events_to_update with their original values.  If they are the same then we don't need to upate
        ### Splitting the events_to_update and events_to_create comes before this
        
        data_updated = False

        # This was originally being passed to the function but we could calculate it using the data types of the columns?
        names_columns = []
        
        
        if len(events_to_update) > 0:
            
            events_to_update = events_to_update.replace('', None)
            events_to_update = events_to_update.replace(np.nan, None)
            
            for col in events_to_update.columns:

                col_type  = powerbi_table_info[ powerbi_table_info['column_name'] == col]['data_type']

                if len(col_type) > 0:

                    col_type = col_type.iloc[0]

                    if col_type == 'interval':

                        # Check if it already is the type we want it to be as we don't want to convert it again
                        if pd.api.types.is_timedelta64_dtype(events_to_update[col]):
                            continue
                        else:
                            # events_to_update[col] = pd.to_timedelta(events_to_update[col].astype('str') + ':00')
                            events_to_update[col] = events_to_update[col].apply(lambda x: None if pd.isna(x) else pd.to_timedelta(str(x) + ':00') if (len(str(x)) == 5) else pd.to_timedelta(str(x)) if (len(str(x)) == 8) else x)
                            
                    elif (col_type == 'text') | (col_type == 'character varying'):
                        
                        events_to_update[col] = events_to_update[col].apply(lambda x: str(x) if x is not None else None)
                        names_columns.append(col)
                        

                    elif (col_type == 'time without time zone'):
                        
                        original_df[col] = original_df[col].apply(lambda x: str(x) if x is not None else None)
                        events_to_update[col] = events_to_update[col].apply(lambda x: str(x) + ':00' if (len(str(x)) == 5 ) else str(x) if x is not None else None)

                    elif (col_type == 'timestamp without time zone'):
                        
                        events_to_update[col] = events_to_update[col].apply(lambda x: convert_datetimes(x) if x is not None else None)
                        original_df[col] = original_df[col].apply(lambda x: convert_datetimes(x) if x is not None else None)


                    elif (col_type == 'numeric') | (col_type == 'double precision'):
                        
                        events_to_update[col] = events_to_update[col].astype('float')
                        original_df[col] = original_df[col].astype('float')


                else:

                    print('Cannot find column type for one of the events we are updating: ' + str(col))
                    

            
            # The original data could contain '' instead of ' from the database so we need to replace these
            for col in names_columns:

                if col in original_df.columns:
                    original_df[col] = original_df[col].apply(lambda x: None if pd.isna(x) else x.replace("''", "'"))
                    original_df[col] = original_df[col].apply(lambda x: None if pd.isna(x) else x.replace('"', "'"))
                    events_to_update[col] = events_to_update[col].apply(lambda x: None if pd.isna(x) else x.replace('"', "'"))

                    # Slightly different apostrophes
                    # original_df[col] = original_df[col].apply(lambda x: None if pd.isna(x) else x.replace("''", "'"))


            new_columns = []
            old_columns = []

            for col in cols_to_check:
                
                if col != id_column:

                    new_col_name = col + '_old'
                    new_columns.append(new_col_name)

                    old_columns.append(col)

                    # Set the original dataframe columns to '_old' for comparison on the merge
                    original_df.rename(columns = {col:new_col_name}, inplace = True)

            events_to_update = events_to_update.merge(original_df, how = 'left', left_on = id_column, right_on = id_column)
    #         events_to_update = events_to_update.replace(np.nan, None)

            
            # Now compare to see if the columns have different values
            compare_col_names = []
            for col in cols_to_check:
                
                if col != id_column:
                    new_col_name = col + '_old'
                    compare_col_name = col + '_same'
                    compare_col_names.append(compare_col_name)
                    events_to_update[compare_col_name] = events_to_update[col] == events_to_update[new_col_name]
                    
                    # Extra check for None/NaN values
                    events_to_update[compare_col_name] = events_to_update[[col, new_col_name, compare_col_name]].apply(lambda x: x[2] if isinstance(x[0], list) else True if (pd.isna(x[0]) and pd.isna(x[1])) else x[2], axis=1)

                    # Check a literal evaluation
                    if (events_to_update[compare_col_name] == False).any():
                        events_to_update[compare_col_name] = events_to_update[[col, new_col_name, compare_col_name]].apply(lambda x: compare_evals(x), axis = 1)

            # Create two DataFrames based on the condition
            all_true_mask = events_to_update[compare_col_names].all(axis=1)
            # all_true_df = events_to_update[all_true_mask]
            any_false_df = events_to_update[~all_true_mask]

            for col in compare_col_names:
                temp = any_false_df[ any_false_df[col] == False]
                if len(temp) > 0:
                    new_col = col.replace('_same','')
                    old_col = col.replace('_same','_old')
                    print(any_false_df[[new_col, old_col]])


            if len(any_false_df)>0:
                print('Updating %s records'%(len(any_false_df)))
                formatted_data = any_false_df[cols_to_check].copy()
                formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
                try:
                    update_sql(table_to_update, formatted_data, id_column, POSTGRESQL_PARAMS)
                except:
                    print('Error updating - May need to try again')
                data_updated = True
    
        return any_false_df


    except Exception as e:

        print('An error has occured inside this function - update_required_events')
        print(f"An exception of type {type(e).__name__} occurred: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()




def create_new_events(table_to_update, powerbi_table_info, events_to_create, cols_to_insert, POSTGRESQL_PARAMS):

    
    if len(events_to_create) > 0:

        print('Inserting new events: %s'%(len(events_to_create)))
        formatted_data = events_to_create[cols_to_insert].copy()
        formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
        insert_sql(table_to_update, formatted_data, POSTGRESQL_PARAMS)
        
    return




def add_data_to_database(data_to_import, table_name, id_column, POSTGRESQL_PARAMS):
    
#     print('')
#     print('Attempting to add data to database: %s'%(table_name))
    
    try:

        updated_df = pd.DataFrame()
        
        # Make sure none of the ids are blank - report them if they are
        empty_id_rows = len(data_to_import[ pd.isna(data_to_import[id_column]) ])
        if empty_id_rows > 0:
            print('There are rows than contain empty values for the id column - Need to check this')
        
        # Make sure none of the ids are blank - report them if they are
        data_to_import = data_to_import[ pd.notna(data_to_import[id_column]) ]
        
        # This is used to show if we had to update anything - Showing if we have to run anything ellse afterwarsd (new data)
        data_updated = False
        
        ids_to_get = list(data_to_import[id_column].drop_duplicates())

        # Get the data from the database where the id is present
        sql_statement = "select * FROM " + str(table_name) + " where " + str(id_column) + " in (" + str(ids_to_get)[1:-1] + ");"
        current_table_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, True)
        current_table_data.replace('None', None, inplace = True)


        powerbi_table_info = get_table_info(table_name, POSTGRESQL_PARAMS)
        current_table_data_columns = list(powerbi_table_info['column_name'])

        # Only keep the data in both tables where the columns are present in both tables
        columns_to_keep = [x for x in data_to_import.columns if x in current_table_data_columns]
        data_to_import = data_to_import[columns_to_keep]
        current_table_data = current_table_data[columns_to_keep]

        # Notify the user of any columns being dropped from the data we want to put into the database
        columns_to_drop = [x for x in data_to_import.columns if x not in columns_to_keep]
        if len(columns_to_drop):
            print('We are dropping columns from the original dataframe as they are not in the database table: ' + table_name + ': '+ str(columns_to_drop))

        # New data to insert is any data that does not have an id in this new dataframe
        new_events = data_to_import[ ~data_to_import[id_column].isin(current_table_data[id_column])]
        events_to_update = data_to_import[ data_to_import[id_column].isin(current_table_data[id_column])]

        if (len(new_events) > 0) | (len(events_to_update) > 0):


            if len(events_to_update) > 0:
                # Use update_required_events to update the rest of the data
                updated_df = update_required_events(table_name, powerbi_table_info, events_to_update, current_table_data, columns_to_keep,  [], id_column, POSTGRESQL_PARAMS)

            if len(new_events) > 0:
                create_new_events(table_name, powerbi_table_info, new_events, new_events.columns, POSTGRESQL_PARAMS)

        print('add_data_to_database - Complete')

        return new_events, updated_df

    except Exception as e:

        print('An error has occured inside this function - add_data_to_database')
        print(f"An exception of type {type(e).__name__} occurred: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame()
=======
import pandas as pd
import psycopg2
import datetime
import sys
import numpy as np
import datetime
import traceback
import math
from pandas import NaT
from io import BytesIO
import requests
import tempfile
import importlib.util


def import_github_module(module_name, github_url):
    """
    Downloads a Python file from GitHub and imports it as a module.

    Parameters:
    - module_name (str): The name to assign to the module (e.g., 'mf').
    - github_url (str): The raw GitHub URL of the Python file.

    Returns:
    - module: The imported module.
    """
    # Fetch the script from GitHub
    response = requests.get(github_url)
    
    if response.status_code == 200:
        # Create a temporary file to store the script
        temp_script_path = tempfile.NamedTemporaryFile(delete=False, suffix=".py").name
        with open(temp_script_path, "w") as f:
            f.write(response.text)

        # Load the module dynamically
        spec = importlib.util.spec_from_file_location(module_name, temp_script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        print(f"✅ Successfully imported '{module_name}' from GitHub.")
        return module
    else:
        print("❌ Failed to fetch the script. Check the URL.")
        return None
    
    


def retrieve_files_to_dataframe(blob_parameters, container_name, prefix, strings_to_filter=[], dtypes = {}):
    
    blob_service_client = connect_to_blob(blob_parameters)
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs(name_starts_with=prefix)
    
    if len(strings_to_filter) > 0:
        filtered_blobs = []
        for blob in blobs:
            blob_name = blob.name
            if any(string in blob_name for string in strings_to_filter):
                filtered_blobs.append(blob)

        blobs = filtered_blobs
        
    file_data = []
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob)
        blob_data = blob_client.download_blob().readall()
        file_name = blob.name.split('/')[-1]  # Extract filename from blob's name
        file_data.append((file_name, BytesIO(blob_data)))  # Store filename with file data

    # Convert the list of tuples (filename, file data) into a list of DataFrames
    dfs = [pd.read_csv(file[1], dtype=dtypes) for file in file_data]

    # Add filename column to each DataFrame
    for i, df in enumerate(dfs):
        df['filename'] = file_data[i][0]

    # Concatenate all DataFrames into a single DataFrame
    concatenated_df = pd.concat(dfs, ignore_index=True)
    
    return concatenated_df


def connect_to_blob(blob_parameters):
    
    blob_account_name = blob_parameters['blob_account_name'] # fill in your blob account name
    blob_account_key = blob_parameters['blob_account_key']
    account_url = blob_parameters['account_url']

    blob_service_client = BlobServiceClient(account_url=account_url, account_name=blob_account_name, account_key=blob_account_key)
        
    return blob_service_client 




def get_blob(container, blob_name):
    
    local_start_time = datetime.datetime.now()
    print('Getting blob')
    
    downloaded_blob = container.download_blob(blob_name)
    downloaded_file = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
    
    local_end_time = datetime.datetime.now()
    print('Get complete: ' + str(local_end_time - local_start_time))

    return downloaded_file






def postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data):
    
    conn = None
    cur = None
    
    try:
        
        
        conn = psycopg2.connect(
          host = POSTGRESQL_PARAMS['host'],
          database = POSTGRESQL_PARAMS['DB'],
          user = POSTGRESQL_PARAMS['username'],
          password = POSTGRESQL_PARAMS['pass']
        )
        conn.set_client_encoding('UTF-8')
        cur = conn.cursor()
        cur.execute(sql_statement)


        if retrieve_data:
            temp = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
            for col in temp.columns:
                if (col == 'password') | (col == 'token') | (col =='session'):
                    temp.drop(col, axis = 1, inplace = True)  
            conn.commit()
            return temp
        
        else:
            conn.commit()
            return None
    
    
    except Exception as e:

        print('An error has occured inside this function - postgres_Retreive_Insert')
        print(f"An exception of type {type(e).__name__} occurred: {str(e)}")
        traceback.print_exc()
        return None


    
    finally:
        
        if cur:
            cur.close()
            conn.close()




def format_data_for_postgres(formatted_data, powerbi_table_info):
    
    # Format data to add to the database

    formatted_data.replace("'", "''", inplace = True)
    formatted_data.replace(to_replace = '', value = 'NULL', inplace = True)
    formatted_data.replace(to_replace = 'null', value = 'NULL', inplace = True)


    # Replace nulls with NULL
    for col in formatted_data.columns:
        if formatted_data[col].dtype == 'timedelta64[ns]':
            formatted_data[col] = formatted_data[col].astype(str)
            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if x == 'NaT' else x)


    formatted_data.fillna('NULL', inplace = True)
    # formatted_data.fillna(pd.NA, inplace=True)


    for col in formatted_data.columns:

        #print(col)
        column_info = powerbi_table_info[ powerbi_table_info['column_name'] == col]
        if len(column_info) == 0:
            print(col + ' has no information in this table')
            raise ValueError("This is a deliberate error.")

        else:
            column_type = column_info['data_type'].iloc[0]

        if (column_type == 'numeric') | (column_type == 'float') | (column_type == 'double precision'):

            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if x == 'NULL' else float(x))

        elif column_type == 'boolean':

            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if x == 'NULL' else False if x == 0 else True)
            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).upper() )
            #formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x).upper() + "'")


        elif column_type == 'timestamp without time zone':

            #formatted_data[col] = pd.to_datetime(formatted_data[col])
            formatted_data[col] = formatted_data[col].apply(lambda x: convert_datetimes(x))
            #formatted_data[col].apply(lambda x: 'NULL' if ( pd.isna(x) | (x == 'NULL') ) else pd.to_datetime(datetime.datetime.strptime(str(x), '%d-%m-%Y %H:%M:%S')) if str(x)[2]=='-' else pd.to_datetime(datetime.datetime.strptime(str(x), '%d/%m/%Y %H:%M:%S')) if str(x)[2]=='/' else pd.to_datetime(datetime.datetime.strptime(str(x), '%Y/%m/%d %H:%M:%S')) if str(x)[4]=='/' else pd.to_datetime(datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S')) if str(x)[4]=='-' else 'NULL')
            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == 'None') | (pd.isna(x)) else "'" + str(x) + "'")


            
        elif column_type == 'timestamp with time zone':

            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == 'None') | (pd.isna(x)) else "'" + str(x) + "'")

            
 
        elif column_type == 'time without time zone':

            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == 'None') | (pd.isna(x)) else "'" + str(x) + "'")

            
            
        elif column_type == 'date':
            formatted_data[col] = formatted_data[col].apply(lambda x: convert_datetimes(x))
            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == 'None') | (pd.isna(x)) else "'" + str(x) + "'")

            
            
        elif column_type == 'interval':

            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")


        elif column_type == 'uuid':

            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")


        elif column_type == 'ARRAY':

            array_type = powerbi_table_info[ powerbi_table_info['column_name'] == col]['udt_name'].iloc[0]

            #formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("False", 'false'))
            #formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("True", 'true'))
            #formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("'", '"'))
            #formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("\\", '\\\\'))

            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("False", 'false').replace("True", 'true').replace("'", '"').replace("\\", '\\\\'))

            if (array_type == '_json'):
                array_type_string = '::json[]'

                #formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("{", "'{"))
                #formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("}", "}'"))

                formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("{", "'{").replace("}", "}'"))
                formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'" )
                formatted_data[col] = formatted_data[col].apply(lambda x: "['{" + str(x)[1:-1] + "}']" if str(x)[1:2].isnumeric() else x)

                
#             elif (array_type == '_jsonb'):
#                 array_type_string = '::jsonb'

#                 formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("{", "'{").replace("}", "}'"))
#                 formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'" )
#                 formatted_data[col] = formatted_data[col].apply(lambda x: "['{" + str(x)[1:-1] + "}']" if str(x)[1:2].isnumeric() else x)

                
            elif array_type == '_int4':
                array_type_string = '::integer[]'


            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if (x == 'NULL') | (x == '[]') else "ARRAY[" + str(x)[1:-1] + "]" + str(array_type_string))



        elif column_type == 'json':

            formatted_data[col] = formatted_data[col].replace("'", '"')
            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x).replace("'", '"') + "'")

            
        elif column_type == 'jsonb':

            formatted_data[col] = formatted_data[col].replace("'", '"')
            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x).replace("'", '"') + "'")

            

        elif column_type == 'character varying':
            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("'", '"'))
            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")


        elif column_type == 'text':

            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).replace("'", '"'))
            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")
            

        elif column_type == 'integer':

            formatted_data[col] = formatted_data[col].apply(lambda x: x if x == str("NULL") else "NULL" if x == '' else int(float(x)))

        else:
            print('DATA TYPE IS NOT COVERED - FIX - %s'%(column_type))


    formatted_data.replace("'NULL'", 'NULL', inplace = True)
    
    return formatted_data




def convert_datetimes(datetime_obj):


        datetime_obj = str(datetime_obj)


        if datetime_obj == 'NULL':
            datetime_obj =  'NULL'
            return


        if datetime_obj.find('NaT')>=0:
            datetime_obj = None
            return
        
        # if datetime_obj == 'NaT':
        #     datetime_obj =  'NULL'
        #     return


        if datetime_obj.find(',')>0:
            datetime_obj = datetime_obj.replace(',', '')

        if datetime_obj.find('T')>0:
            datetime_obj = datetime_obj.replace('T', ' ')

        elif len(datetime_obj) == 10:

            if str(datetime_obj)[2] == '-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d-%m-%Y'))
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
            elif str(datetime_obj)[2] == '/':

                if int(str(datetime_obj)[3:5]) > 12:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%m/%d/%Y')) 
                else:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d/%m/%Y')) 
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
            elif str(datetime_obj)[4]=='-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d'))
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
            elif str(datetime_obj)[4]=='/':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d')) 
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d')
            else:
                datetime_obj =  'NULL'


        elif len(datetime_obj) == 32:
            
            if str(datetime_obj)[4]=='-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d %H:%M:%S.%f%z'))

            if str(datetime_obj)[4]=='/':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d %H:%M:%S.%f%z'))

            if str(datetime_obj)[2]=='-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d-%m-%Y %H:%M:%S.%f%z'))

            if str(datetime_obj)[2]=='/':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d/%m/%Y %H:%M:%S.%f%z'))

            datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')


        elif len(datetime_obj) == 25:


            # if str(datetime_obj)[4]=='-':
                
            datetime_obj =  datetime.datetime.strftime(pd.to_datetime(datetime_obj), '%Y-%m-%d %H:%M:%S')



        elif len(datetime_obj) > 10:

            if str(datetime_obj)[2] == '-':
                datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d-%m-%Y %H:%M:%S'))
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
            elif str(datetime_obj)[2] == '/':
                if int(str(datetime_obj)[3:5]) > 12:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%m/%d/%Y %H:%M:%S')) 
                else:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%d/%m/%Y %H:%M:%S')) 
                datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
            elif str(datetime_obj)[4]=='-':
                
                if str(datetime_obj).find('.') > 0:
                    
                    datetime_obj = datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d %H:%M:%S.%f')
                    datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')

                else:
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d %H:%M:%S'))
                    datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
                    
            elif str(datetime_obj)[4]=='/':
                
                if str(datetime_obj).find('.') > 0:

                    datetime_obj = datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d %H:%M:%S.%f')
                    datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
                    
                else:
                    
                    datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d %H:%M:%S')) 
                    datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')

            else:
                datetime_obj =  'NULL'

        else:
            datetime_obj =  'NULL'


        if len(datetime_obj) == 10:
            datetime_obj = datetime_obj + ' 00:00:00'


        return datetime_obj




# Create sql statement For inserts

def insert_sql(table_name, formatted_data, POSTGRESQL_PARAMS):
    
    last_save = 0
    global_error = False
    
    sql_complete = ''
    sql_statement_part1 = 'insert into ' + table_name + ' ('

    for col in formatted_data.columns:
        sql_statement_part1 = sql_statement_part1 + '"' + col + '", '

    sql_statement_part1 = sql_statement_part1[:-2] + ') values '


    for new_row in range (0, len(formatted_data)):
        #print(new_row, len(formatted_data))

        sql_statement_part2 = ''

        for col in formatted_data.columns:
            
            sql_statement_part2 = sql_statement_part2 + str(formatted_data.iloc[new_row][col]) + ', '

        sql_statement_part2 = '(' + sql_statement_part2[:-2] + '), '

        sql_complete = sql_complete + sql_statement_part2
        
        
        
        if (new_row - last_save) >= 10000:
        
            sql_complete = sql_statement_part1 + sql_complete[:-2] + ';'
            no_data = postgres_Retreive_Insert(sql_complete, POSTGRESQL_PARAMS, retrieve_data = False)
            
            sql_complete = ''
            last_save = new_row
            
            
    # It hasn't just saved
    if (last_save != new_row) | (new_row == 0):
        
        sql_complete = sql_statement_part1 + sql_complete[:-2] + ';'
        no_data = postgres_Retreive_Insert(sql_complete, POSTGRESQL_PARAMS, retrieve_data = False)

    print(str(sys._getframe().f_code.co_name) + ' - Inserts Complete - ' + str(len(formatted_data)))
    
    return




# Create sql statement For inserts

def update_sql(table_name, formatted_data, table_id_column, POSTGRESQL_PARAMS):
    # Create sql statement For inserts
    
    global_error = False
    
    sql_queries = []
    last_save = 0
    new_row = 0

    sql_all = ''

    sql_statement_part1 = 'update ' + table_name + ' set '

    for new_row in range (0, len(formatted_data)):

        sql_complete = ''

        sql_statement_part2 = ''

        for col in formatted_data.columns:
            
            if col != table_id_column:
                sql_statement_part2 = sql_statement_part2 + '"' + str(col) + '" = ' + str(formatted_data.iloc[new_row][col]) + ', '

        #sql_statement_part2 = '(' + sql_statement_part2[:-2] + '), '
        sql_statement_part2 = sql_statement_part2[:-2]
        sql_statement_part2 = sql_statement_part2 + ' where ' + str(table_id_column) + ' = ' + str(formatted_data.iloc[new_row][table_id_column])  + "'"

        #sql_complete = sql_complete + sql_statement_part2


        sql_complete = sql_statement_part1 + sql_statement_part2[:-1] + ';'


        sql_all = sql_all + ' ' + sql_complete
        
        if ( (new_row - last_save) >= 10000) | (new_row == len(formatted_data)):
            
            no_data = postgres_Retreive_Insert(sql_all, POSTGRESQL_PARAMS, retrieve_data = False)

            
            sql_all = ''
            last_save = new_row
            
            
    if (last_save != new_row) | ( (new_row == 0) & (sql_all != '')):
        no_data = postgres_Retreive_Insert(sql_all, POSTGRESQL_PARAMS, retrieve_data = False)    

    print(str(sys._getframe().f_code.co_name) + ' - Update Complete - ' + str(len(formatted_data)))
    
    return



def get_source_last_updated(system_run_times_table, table_name, task_name, POSTGRESQL_PARAMS):
    
    sql_statement =  "SELECT min(last_updated) FROM " + system_run_times_table + " WHERE table_name = '" + str(table_name) + "' and task_name = '" + str(task_name) + "';"
    last_updated = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = True)
    last_updated = last_updated.iloc[0][0]
    
        
    if last_updated is None:
        last_updated = pd.to_datetime('1970-01-01')
        
    else:
        # We are taking off an hour for daylght savings - Need to fix this
        last_updated = last_updated - datetime.timedelta(minutes = 60)
            
    return last_updated




def set_source_last_updated(system_run_times_table, table_name, task_name, last_updated, POSTGRESQL_PARAMS):
    
    sql_statement =  "select * from " + system_run_times_table + " where table_name = '" + str(table_name) + "' and task_name = '" + str(task_name) + "';"
    row_count = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = True)
    
    if len(row_count) == 0:
        
        sql_statement =  "insert into " + system_run_times_table + " (table_name, task_name, last_updated) values ('" + str(table_name) + "', '" + str(task_name) + "', '" + str(last_updated) + "');"
        no_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = False)

    elif len(row_count) > 1:
        
        sql_statement =  "delete from " + system_run_times_table + " where table_name = '" + str(table_name) + "' and task_name = '" + str(task_name) + "';"
        no_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = False)

        sql_statement =  "insert into " + system_run_times_table + " (table_name, task_name, last_updated) values ('" + str(table_name) + "', '" + str(task_name) + "', '" + str(last_updated) + "');"
        no_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = False)

    else:
        
        sql_statement =  "update " + system_run_times_table + " set last_updated = '" + str(last_updated) + "' where table_name = '" + str(table_name) + "' and task_name = '" + str(task_name) + "';"
        no_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = False)
        
    return
    
    
    


def get_table_info(table_name, POSTGRESQL_PARAMS):
    
    sql_statement = "SELECT column_name, data_type, udt_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + str(table_name) + "' ORDER BY ORDINAL_POSITION;"
    table_info = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = True)
    
    return table_info



def compare_evals(vec):
    
    try:

        if vec[2] == True:
            return True
        
        else:

            first_value = str(vec[0])
            second_value = str(vec[1])

            if pd.isna(first_value) & pd.isna(second_value):

                return True
            
        
            elif (pd.isna(first_value) and not pd.isna(second_value)) | (pd.isna(second_value) and not pd.isna(first_value)):

                return False
            



            else:


                if (first_value is np.nan) and (second_value is np.nan):

                    return True

                elif (first_value is np.nan) and not (second_value is np.nan):

                    return False
                
                elif (second_value is np.nan) and not (first_value is np.nan):

                    return False
                
                elif (first_value == 'nan') & (second_value == 'nan'):

                    return True

                elif (first_value == 'nan') & (second_value != 'nan'):

                    return False

                elif (first_value != 'nan') & (second_value == 'nan'):

                    return False

                first_value = eval(first_value)
                second_value = eval(second_value)

                if ((type(first_value) == int) | (type(first_value) == float)) & ((type(second_value) == int) | (type(second_value) == float)):
                    
                    if math.isnan(first_value) & math.isnan(second_value):

                        return True
                    
                    else:

                        return False
                
                if ((type(first_value) == int) | (type(first_value) == float)) | ((type(second_value) == int) | (type(second_value) == float)):

                    return False


                elif (first_value) == (second_value):
                    
                    return True
                
                else:
                    
                    return False
        
        
    except Exception as e:

        # print('An error has occured inside this function - compare_evals')
        # print(f"An exception of type {type(e).__name__} occurred: {str(e)}")
        # traceback.print_exc()
        return vec[2]



    

def update_required_events(table_to_update, powerbi_table_info, events_to_update, original_df, cols_to_check, names_columns, id_column, POSTGRESQL_PARAMS):

    try:

        print('Checking for events to update')
        
        ### This is used to compare the events_to_update with their original values.  If they are the same then we don't need to upate
        ### Splitting the events_to_update and events_to_create comes before this
        
        data_updated = False

        # This was originally being passed to the function but we could calculate it using the data types of the columns?
        names_columns = []
        
        
        if len(events_to_update) > 0:
            
            events_to_update = events_to_update.replace('', None)
            events_to_update = events_to_update.replace(np.nan, None)
            
            for col in events_to_update.columns:

                col_type  = powerbi_table_info[ powerbi_table_info['column_name'] == col]['data_type']

                if len(col_type) > 0:

                    col_type = col_type.iloc[0]

                    if col_type == 'interval':

                        # Check if it already is the type we want it to be as we don't want to convert it again
                        if pd.api.types.is_timedelta64_dtype(events_to_update[col]):
                            continue
                        else:
                            # events_to_update[col] = pd.to_timedelta(events_to_update[col].astype('str') + ':00')
                            events_to_update[col] = events_to_update[col].apply(lambda x: None if pd.isna(x) else pd.to_timedelta(str(x) + ':00') if (len(str(x)) == 5) else pd.to_timedelta(str(x)) if (len(str(x)) == 8) else x)
                            
                    elif (col_type == 'text') | (col_type == 'character varying'):
                        
                        events_to_update[col] = events_to_update[col].apply(lambda x: str(x) if x is not None else None)
                        names_columns.append(col)
                        

                    elif (col_type == 'time without time zone'):
                        
                        original_df[col] = original_df[col].apply(lambda x: str(x) if x is not None else None)
                        events_to_update[col] = events_to_update[col].apply(lambda x: str(x) + ':00' if (len(str(x)) == 5 ) else str(x) if x is not None else None)

                    elif (col_type == 'timestamp without time zone'):
                        
                        events_to_update[col] = events_to_update[col].apply(lambda x: convert_datetimes(x) if x is not None else None)
                        original_df[col] = original_df[col].apply(lambda x: convert_datetimes(x) if x is not None else None)


                    elif (col_type == 'numeric') | (col_type == 'double precision'):
                        
                        events_to_update[col] = events_to_update[col].astype('float')
                        original_df[col] = original_df[col].astype('float')


                else:

                    print('Cannot find column type for one of the events we are updating: ' + str(col))
                    

            
            # The original data could contain '' instead of ' from the database so we need to replace these
            for col in names_columns:

                if col in original_df.columns:
                    original_df[col] = original_df[col].apply(lambda x: None if pd.isna(x) else x.replace("''", "'"))
                    original_df[col] = original_df[col].apply(lambda x: None if pd.isna(x) else x.replace('"', "'"))
                    events_to_update[col] = events_to_update[col].apply(lambda x: None if pd.isna(x) else x.replace('"', "'"))

                    # Slightly different apostrophes
                    # original_df[col] = original_df[col].apply(lambda x: None if pd.isna(x) else x.replace("''", "'"))


            new_columns = []
            old_columns = []

            for col in cols_to_check:
                
                if col != id_column:

                    new_col_name = col + '_old'
                    new_columns.append(new_col_name)

                    old_columns.append(col)

                    # Set the original dataframe columns to '_old' for comparison on the merge
                    original_df.rename(columns = {col:new_col_name}, inplace = True)

            events_to_update = events_to_update.merge(original_df, how = 'left', left_on = id_column, right_on = id_column)
    #         events_to_update = events_to_update.replace(np.nan, None)

            
            # Now compare to see if the columns have different values
            compare_col_names = []
            for col in cols_to_check:
                
                if col != id_column:
                    new_col_name = col + '_old'
                    compare_col_name = col + '_same'
                    compare_col_names.append(compare_col_name)
                    events_to_update[compare_col_name] = events_to_update[col] == events_to_update[new_col_name]
                    
                    # Extra check for None/NaN values
                    events_to_update[compare_col_name] = events_to_update[[col, new_col_name, compare_col_name]].apply(lambda x: x[2] if isinstance(x[0], list) else True if (pd.isna(x[0]) and pd.isna(x[1])) else x[2], axis=1)

                    # Check a literal evaluation
                    if (events_to_update[compare_col_name] == False).any():
                        events_to_update[compare_col_name] = events_to_update[[col, new_col_name, compare_col_name]].apply(lambda x: compare_evals(x), axis = 1)

            # Create two DataFrames based on the condition
            all_true_mask = events_to_update[compare_col_names].all(axis=1)
            # all_true_df = events_to_update[all_true_mask]
            any_false_df = events_to_update[~all_true_mask]

            for col in compare_col_names:
                temp = any_false_df[ any_false_df[col] == False]
                if len(temp) > 0:
                    new_col = col.replace('_same','')
                    old_col = col.replace('_same','_old')
                    print(any_false_df[[new_col, old_col]])


            if len(any_false_df)>0:
                print('Updating %s records'%(len(any_false_df)))
                formatted_data = any_false_df[cols_to_check].copy()
                formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
                try:
                    update_sql(table_to_update, formatted_data, id_column, POSTGRESQL_PARAMS)
                except:
                    print('Error updating - May need to try again')
                data_updated = True
    
        return any_false_df


    except Exception as e:

        print('An error has occured inside this function - update_required_events')
        print(f"An exception of type {type(e).__name__} occurred: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()




def create_new_events(table_to_update, powerbi_table_info, events_to_create, cols_to_insert, POSTGRESQL_PARAMS):

    
    if len(events_to_create) > 0:

        print('Inserting new events: %s'%(len(events_to_create)))
        formatted_data = events_to_create[cols_to_insert].copy()
        formatted_data = format_data_for_postgres(formatted_data, powerbi_table_info)
        insert_sql(table_to_update, formatted_data, POSTGRESQL_PARAMS)
        
    return




def add_data_to_database(data_to_import, table_name, id_column, POSTGRESQL_PARAMS):
    
#     print('')
#     print('Attempting to add data to database: %s'%(table_name))
    
    try:

        updated_df = pd.DataFrame()
        
        # Make sure none of the ids are blank - report them if they are
        empty_id_rows = len(data_to_import[ pd.isna(data_to_import[id_column]) ])
        if empty_id_rows > 0:
            print('There are rows than contain empty values for the id column - Need to check this')
        
        # Make sure none of the ids are blank - report them if they are
        data_to_import = data_to_import[ pd.notna(data_to_import[id_column]) ]
        
        # This is used to show if we had to update anything - Showing if we have to run anything ellse afterwarsd (new data)
        data_updated = False
        
        ids_to_get = list(data_to_import[id_column].drop_duplicates())

        # Get the data from the database where the id is present
        sql_statement = "select * FROM " + str(table_name) + " where " + str(id_column) + " in (" + str(ids_to_get)[1:-1] + ");"
        current_table_data = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, True)
        current_table_data.replace('None', None, inplace = True)


        powerbi_table_info = get_table_info(table_name, POSTGRESQL_PARAMS)
        current_table_data_columns = list(powerbi_table_info['column_name'])

        # Only keep the data in both tables where the columns are present in both tables
        columns_to_keep = [x for x in data_to_import.columns if x in current_table_data_columns]
        data_to_import = data_to_import[columns_to_keep]
        current_table_data = current_table_data[columns_to_keep]

        # Notify the user of any columns being dropped from the data we want to put into the database
        columns_to_drop = [x for x in data_to_import.columns if x not in columns_to_keep]
        if len(columns_to_drop):
            print('We are dropping columns from the original dataframe as they are not in the database table: ' + table_name + ': '+ str(columns_to_drop))

        # New data to insert is any data that does not have an id in this new dataframe
        new_events = data_to_import[ ~data_to_import[id_column].isin(current_table_data[id_column])]
        events_to_update = data_to_import[ data_to_import[id_column].isin(current_table_data[id_column])]

        if (len(new_events) > 0) | (len(events_to_update) > 0):


            if len(events_to_update) > 0:
                # Use update_required_events to update the rest of the data
                updated_df = update_required_events(table_name, powerbi_table_info, events_to_update, current_table_data, columns_to_keep,  [], id_column, POSTGRESQL_PARAMS)

            if len(new_events) > 0:
                create_new_events(table_name, powerbi_table_info, new_events, new_events.columns, POSTGRESQL_PARAMS)

        print('add_data_to_database - Complete')

        return new_events, updated_df

    except Exception as e:

        print('An error has occured inside this function - add_data_to_database')
        print(f"An exception of type {type(e).__name__} occurred: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame()
>>>>>>> 46df589b1dc5a25cf1fc544c89edcdc4d7bf0739
