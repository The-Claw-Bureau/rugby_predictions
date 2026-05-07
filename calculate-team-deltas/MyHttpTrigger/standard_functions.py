import pandas as pd
import numpy as np
import psycopg2, datetime
import statistics
import itertools
from IPython.display import clear_output
import time
import sys
import datetime




def notifyTeams(message):
    channel = channels_dict['production_notifications']
    
    print(message)
    print("Notifying teams")
    payload = {
        "text": message
    }
    headers = {
        'Content-Type': 'application/json'
    }
    try:
        url = channel
        # os.environ['BBC_TEAMS_WEBHOOK']
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        print("Request sent")
    except KeyError:
        print("Request failed as channel="+str(channel)+" is not recognised")

def notifyTeamsAdvanced(
    message:str,
    summary:str, 
    error_code:str,
    additional_info_dict:dict, 
    channel:str, 
    # importance:str
    ):
    print("Notifying teams")
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0076D7",
        "summary": summary,
        "sections": [{
            "activityTitle": summary,
            "activitySubtitle": "Error code: "+str(error_code),
            # "activityImage": image_url,
            "text": message,
            "facts": [
                {"name":k,
                "value":v}
                for k,v in additional_info_dict.items()
            ],
            "markdown": True
        }],
    }
    headers = {
        'Content-Type': 'application/json'
    }
    try:
        url = channels_dict[channel]
        response = requests.post(url, headers=headers, data=json.dumps(payload))
    except KeyError:
        print("Request failed as channel="+str(channel)+" is not recognised")


def get_table_info(table_name):
    
    sql_statement = "SELECT column_name, data_type, udt_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + str(table_name) + "' ORDER BY ORDINAL_POSITION;"
    table_info = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data = True)
    
    return table_info


def insert_sql(table_name, formatted_data):
    
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

def update_sql(table_name, formatted_data, table_id_column):
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



# Format data to add to the database

def format_data_for_postgres(formatted_data, powerbi_table_info):

    formatted_data = formatted_data.replace("'", "''")
    formatted_data = formatted_data.replace(to_replace = '', value = 'NULL')
    formatted_data = formatted_data.replace(to_replace = 'null', value = 'NULL')


    # Replace nulls with NULL
    formatted_data.fillna('NULL', inplace = True)

    for col in formatted_data.columns:

        #print(col)
        column_type = powerbi_table_info[ powerbi_table_info['column_name'] == col]['data_type'].iloc[0]

        if (column_type == 'numeric') | (column_type == 'float') | (column_type == 'double precision'):

            formatted_data[col] = formatted_data[[col]].apply(lambda x: 'NULL' if x[0] == 'NULL' else float(x[0]), axis = 1)

        elif column_type == 'boolean':

            formatted_data[col] = formatted_data[col].apply(lambda x: False if x == 0 else True)
            formatted_data[col] = formatted_data[col].apply(lambda x: str(x).upper() )
            #formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x).upper() + "'")


        elif column_type == 'timestamp without time zone':

            #formatted_data[col] = pd.to_datetime(formatted_data[col])
            formatted_data[col] = formatted_data[[col]].apply(lambda x: convert_datetimes(x[0]), axis = 1)
            #formatted_data[col].apply(lambda x: 'NULL' if ( pd.isna(x) | (x == 'NULL') ) else pd.to_datetime(datetime.datetime.strptime(str(x), '%d-%m-%Y %H:%M:%S')) if str(x)[2]=='-' else pd.to_datetime(datetime.datetime.strptime(str(x), '%d/%m/%Y %H:%M:%S')) if str(x)[2]=='/' else pd.to_datetime(datetime.datetime.strptime(str(x), '%Y/%m/%d %H:%M:%S')) if str(x)[4]=='/' else pd.to_datetime(datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S')) if str(x)[4]=='-' else 'NULL')
            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if x == 'NULL' else "'" + str(x) + "'")


            
        elif column_type == 'timestamp with time zone':

            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")

            
 
        elif column_type == 'time without time zone':

            formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'")

            
            
        elif column_type == 'date':
            formatted_data[col] = formatted_data[col].apply(lambda x: convert_datetimes(x))
            formatted_data[col] = formatted_data[col].apply(lambda x: 'NULL' if x == 'NULL' else "'" + str(x) + "'")

            
            
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

            if array_type == '_json':
                array_type_string = '::json[]'

                #formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("{", "'{"))
                #formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("}", "}'"))

                formatted_data[col] = formatted_data[col].apply(lambda x: x.replace("{", "'{").replace("}", "}'"))
                formatted_data[col] = formatted_data[col].apply(lambda x: "'" + str(x) + "'" )
                formatted_data[col] = formatted_data[col].apply(lambda x: "['{" + str(x)[1:-1] + "}']" if str(x)[1:2].isnumeric() else x)

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


    formatted_data = formatted_data.replace("'NULL'", 'NULL')
    
    return formatted_data




def convert_datetimes(datetime_obj):
        
    #if datetime_obj != 'NULL':
    #    datetime_obj = str(pd.to_datetime(datetime_obj).replace(tzinfo=None))
    
    datetime_obj = str(datetime_obj)
    
    
    if datetime_obj == 'NULL':

        datetime_obj =  'NULL'
        
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
            datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y-%m-%d %H:%M:%S'))
            datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
        elif str(datetime_obj)[4]=='/':
            datetime_obj = (datetime.datetime.strptime(str(datetime_obj), '%Y/%m/%d %H:%M:%S')) 
            datetime_obj =  datetime.datetime.strftime(datetime_obj, '%Y-%m-%d %H:%M:%S')
        else:
            datetime_obj =  'NULL'
        
    else:
        datetime_obj =  'NULL'
        

    
    return datetime_obj
    
    
    
    
    
    def postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, retrieve_data):
        

    return_list = []
    try:
        
        conn = psycopg2.connect(
          host = POSTGRESQL_PARAMS['host'],
          database = POSTGRESQL_PARAMS['DB'],
          user = POSTGRESQL_PARAMS['username'],
          password = POSTGRESQL_PARAMS['pass'],
        )
        conn.set_client_encoding('UTF-8')
        cur = conn.cursor()
        cur.execute(sql_statement)


        if retrieve_data:
            temp = pd.DataFrame(cur.fetchall(), columns = [desc[0] for desc in cur.description])
            for col in temp.columns:
                if (col == 'password') | (col == 'token') | (col =='session'):
                    temp.drop(col, axis = 1, inplace = True)  
                    
            return temp, False
        
        else:
            conn.commit()
            
            return None, False

        cur.close()
        conn.close()
        
        
    except Exception as ex:
        print('Error Message - ' + str(ex))

        return None, True


