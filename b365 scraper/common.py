import collections
import csv
import datetime
from io import StringIO
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
import uuid
import json
import pytz
from sqlalchemy import create_engine
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

def convert_date_to_appropriate(match_date):
    
    try:
        new_date = datetime.datetime.strptime(str(match_date[:10]), "%d/%m/%Y")
    except:
        new_date = datetime.datetime.strptime(str(match_date[:10]), "%Y-%m-%d")
        
    return new_date

def convert_floats_to_str(vec):
    
    try:
        return str(int((vec)))
    except:
        return str(vec)


def get_postgres_cred():
    '''
    Check if the the Azure env vars are set
    '''
    TENANT_ID = '4de4e7e1-bc7f-498b-8438-ec890555edb6'
    CLIENT_ID = '63a0fba4-f822-484c-bb61-65ec379b9696'
    CLIENT_SECRET = 'UcS6dhK0r95bQuA0s8~Y-_tL9gLD-BP68F'

    _credential = ClientSecretCredential(
        tenant_id = TENANT_ID,
        client_id = CLIENT_ID,
        client_secret= CLIENT_SECRET
    )

    KVUri = 'https://blackbox-key-vault.vault.azure.net/'
    client = SecretClient(vault_url=KVUri, credential=_credential)
    id_user = 'AIRFLOW-DB-USER'
    id_pass = 'AIRFLOW-DB-PW'
    userSecret = client.get_secret(id_user)
    passwordSecret = client.get_secret(id_pass)
    return userSecret.value, passwordSecret.value

def get_sqlalchemy_connection(database_host=None, database_user=None, database_password=None, database_database=None, database_port=None, database_sslmode=None):
    if database_host is None:
        database_host = 'bbdb-prod-master.postgres.database.azure.com'
    if database_user is None:
        database_user = get_postgres_cred()[0]
        
    if database_password is None:
        database_password = get_postgres_cred()[1]
    if database_database is None:
        database_database = 'bbc'
    if database_port is None:
        database_port = '5432'
        
    if database_sslmode is None:
        database_sslmode = 'require'
        
    if database_sslmode is None:
        engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(database_user, database_password, database_host, database_port, database_database))
    else:
        engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(database_user, database_password, database_host, database_port, database_database), connect_args={'sslmode':database_sslmode})
    return engine

def get_psycopg_connection(database_host=None, database_user=None, database_password=None, database_database=None, database_port=None, database_sslmode=None):
    if database_host is None:
        database_host = 'bbdb-prod-master.postgres.database.azure.com'
    if database_user is None:
        database_user = get_postgres_cred()[0]
        
    if database_password is None:
        database_password = get_postgres_cred()[1]
    if database_database is None:
        database_database = 'bbc'
    if database_port is None:
        database_port = '5432'
        
    if database_sslmode is None:
        database_sslmode = 'require'
    
    if database_sslmode is None:
        conn = psycopg2.connect(host=database_host,database=database_database, user=database_user, password=database_password)
    else:
        conn = psycopg2.connect(host=database_host,database=database_database, user=database_user, password=database_password, sslmode=database_sslmode)
    return conn

def psql_upsert_copy(table, conn, keys, data_iter):

    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerows(data_iter)
    buf.seek(0)

    if table.schema:
        table_name = sql.SQL("{}.{}").format(
            sql.Identifier(table.schema), sql.Identifier(table.name))
    else:
        table_name = sql.Identifier(table.name)

    tmp_table_name = sql.Identifier(table.name + "_staging")
    columns = sql.SQL(", ").join(map(sql.Identifier, keys))

    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        stmt = "CREATE TEMPORARY TABLE {} ( LIKE {} ) ON COMMIT DROP"
        stmt = sql.SQL(stmt).format(tmp_table_name, table_name)
        cur.execute(stmt)

        # Populate the staging table
        stmt = "COPY {} ( {} ) FROM STDIN WITH CSV"
        stmt = sql.SQL(stmt).format(tmp_table_name, columns)
        cur.copy_expert(stmt, buf)
        stmt1 = """
               SELECT distinct kcu.column_name
               FROM information_schema.table_constraints tco
               JOIN information_schema.key_column_usage kcu 
               ON kcu.constraint_name = tco.constraint_name
               AND kcu.constraint_schema = tco.constraint_schema
               WHERE tco.constraint_type in ('UNIQUE')
               AND tco.table_name = %s
               """
        args = (table.name,)

        if table.schema:
            stmt1 += "AND tco.table_schema = %s"
            args += (table.schema,)

        cur.execute(stmt1, args)
        pk_columns = {row[0] for row in cur.fetchall()}
        # Separate "data" columns from (primary) key columns
        data_columns = [k for k in keys if k not in pk_columns]
        # Build conflict_target
        pk_columns = sql.SQL(", ").join(map(sql.Identifier, pk_columns))

        set_ = sql.SQL(", ").join([
            sql.SQL("{} = EXCLUDED.{}").format(k, k)
            for k in map(sql.Identifier, data_columns)])

        stmt1 = """
               INSERT INTO {} ( {} )
               SELECT {}
               FROM {}
               ON CONFLICT ( {} )
               DO UPDATE SET {}
               """

        stmt1 = sql.SQL(stmt1).format(
            table_name, columns, columns, tmp_table_name, pk_columns, set_)
        cur.execute(stmt1)

def psql_insert_copy(table, conn, keys, data_iter):

    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerows(data_iter)
    buf.seek(0)

    if table.schema:
        table_name = sql.SQL("{}.{}").format(
            sql.Identifier(table.schema), sql.Identifier(table.name))
    else:
        table_name = sql.Identifier(table.name)

    tmp_table_name = sql.Identifier(table.name + "_staging")
    columns = sql.SQL(", ").join(map(sql.Identifier, keys))

    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        stmt = "CREATE TEMPORARY TABLE {} ( LIKE {} ) ON COMMIT DROP"
        stmt = sql.SQL(stmt).format(tmp_table_name, table_name)
        cur.execute(stmt)

        # Populate the staging table
        stmt = "COPY {} ( {} ) FROM STDIN WITH CSV"
        stmt = sql.SQL(stmt).format(tmp_table_name, columns)
        cur.copy_expert(stmt, buf)
        stmt1 = """
               SELECT distinct kcu.column_name
               FROM information_schema.table_constraints tco
               JOIN information_schema.key_column_usage kcu 
               ON kcu.constraint_name = tco.constraint_name
               AND kcu.constraint_schema = tco.constraint_schema
               WHERE tco.constraint_type in ('UNIQUE')
               AND tco.table_name = %s
               """
        args = (table.name,)

        if table.schema:
            stmt1 += "AND tco.table_schema = %s"
            args += (table.schema,)

        cur.execute(stmt1, args)
        pk_columns = {row[0] for row in cur.fetchall()}
        # Separate "data" columns from (primary) key columns
        data_columns = [k for k in keys if k not in pk_columns]
        # Build conflict_target
        pk_columns = sql.SQL(", ").join(map(sql.Identifier, pk_columns))

        set_ = sql.SQL(", ").join([
            sql.SQL("{} = EXCLUDED.{}").format(k, k)
            for k in map(sql.Identifier, data_columns)])

        stmt1 = """
               INSERT INTO {} ( {} )
               SELECT {}
               FROM {}
               ON CONFLICT ( {} )
               DO NOTHING
               """

        stmt1 = sql.SQL(stmt1).format(
            table_name, columns, columns, tmp_table_name, pk_columns)
        cur.execute(stmt1)

def get_as_list(string_or_list):
    """
    Convert a string or string to a list

    :param string_or_list: string or list
    :type string_or_list: str/list
    """

    if isinstance(string_or_list, list):
        return string_or_list
    else:
        return [string_or_list]

def convert_to_json(x):

    if 'nan' in str(x):
        to_return = {}
    elif x == np.nan:
        to_return = {}
    elif x is None:
        to_return = {}
    else:
        to_return = json.dumps(str(x)) 
    return to_return

def check_mappings(wr_to_map, mapping_table, source_col, id_col, columns_to_check_for_changes, internal_id_col=None):
    id_col = get_as_list(id_col)
    if internal_id_col is not None:
        mapping_table[source_col] = mapping_table[source_col].apply(str)
        wr_to_map[source_col] = wr_to_map[source_col].apply(str)
        for col in id_col:
            mapping_table[col] = mapping_table[col].apply(str)
            wr_to_map[col] = wr_to_map[col].apply(str)

        wr_to_map = wr_to_map.merge(mapping_table[[source_col]+id_col+[internal_id_col]], on=[source_col]+id_col, how='left')

    init_cols = list(set(mapping_table.columns.tolist()).difference({'created_at', 'updated_at'}))
    data_source = wr_to_map[source_col].drop_duplicates().values.tolist()[0]
    existing_mapping_table = mapping_table.copy()

    
    id_col_1 = id_col[0]
    if len(id_col)==2:
        id_col_2 = id_col[1]

        
    for item in wr_to_map.index:

        if len(id_col) == 1:
            item_previously_mapped = mapping_table[ (mapping_table[source_col] == data_source) & (mapping_table[id_col_1] == wr_to_map.loc[item][id_col_1]) ]
        else:
            item_previously_mapped = mapping_table[ (mapping_table[source_col] == data_source) & (mapping_table[id_col_1] == wr_to_map.loc[item][id_col_1]) & (mapping_table[id_col_2] == wr_to_map.loc[item][id_col_2]) ]

        
        values_to_add = pd.Series(wr_to_map.loc[item].values, index=wr_to_map.loc[item].index).to_dict()


        # If the event exists then
        if item_previously_mapped[id_col_1].count() > 0:
            # Check to see if the event needs updated

            if collections.Counter(wr_to_map[columns_to_check_for_changes].loc[item]) != collections.Counter(item_previously_mapped[columns_to_check_for_changes].iloc[0]):

                index_to_change = item_previously_mapped.index

                for key in values_to_add.keys():
                    mapping_table.loc[index_to_change, key] = values_to_add[key]

        # If the event doesn't exists then add it to the mapping table
        else:
            mapping_table = pd.concat([mapping_table, pd.DataFrame(values_to_add, index=[item])], axis=0, ignore_index = True)
    if 'start_time' in columns_to_check_for_changes:
        mapping_table['temp_start_time'] = pd.to_datetime(mapping_table['start_time'], utc=True).fillna(datetime.datetime(year=1900, month=1, day=1, hour=0, minute=0, second=0, tzinfo=pytz.utc)).apply(lambda x: x.tz_convert(None).strftime('%Y-%m-%d %H:%M:%S')).apply(str)
        existing_mapping_table['temp_start_time'] = pd.to_datetime(existing_mapping_table['start_time'], utc=True).fillna(datetime.datetime(year=1900, month=1, day=1, hour=0, minute=0, second=0, tzinfo=pytz.utc)).apply(lambda x: x.tz_convert(None).strftime('%Y-%m-%d %H:%M:%S')).apply(str)
        updated_cols = columns_to_check_for_changes.copy()
        updated_cols.remove('start_time')
        updated_cols+=['temp_start_time']
    else:
        updated_cols = columns_to_check_for_changes.copy()
    
    mappings_to_update = mapping_table.merge(existing_mapping_table, on=updated_cols, how='outer', suffixes=('__keep', '__drop'), indicator=True).loc[lambda x: x['_merge']=='left_only']
    columns_to_drop = [x for x in mappings_to_update.columns.tolist() if '__drop' in x]+['_merge']
    mappings_to_update = mappings_to_update.drop(columns_to_drop, axis=1)

    mappings_to_update.columns = mappings_to_update.columns.str.replace('__keep', '')
    mappings_to_update = mappings_to_update.dropna(how='any', subset=id_col)
    
    existing_ids = mappings_to_update['id'].values.tolist()
    mappings_to_update['id'] = [uuid.uuid1() if pd.isna(x) else x for x in existing_ids]

    try:
        mappings_to_update['home_score'] = mappings_to_update['home_score'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['away_score'] = mappings_to_update['away_score'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['home_halftime_score'] = mappings_to_update['home_halftime_score'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['away_halftime_score'] = mappings_to_update['away_halftime_score'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['round'] = mappings_to_update['round'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['leg'] = mappings_to_update['leg'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['live_scores'] = mappings_to_update['live_scores'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['attendance'] = mappings_to_update['attendance'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['season_id'] = mappings_to_update['season_id'].astype('Int64')
    except KeyError:
        pass
    try:
        mappings_to_update['resource'] = mappings_to_update['resource'].apply(convert_to_json)
    except KeyError:
        pass
    try:
        mappings_to_update['event_game_status_id'] = mappings_to_update['event_game_status_id'].astype('Int64')
    except KeyError:
        pass
    
    return mappings_to_update[init_cols]

def upload_df_as_csv_to_azure_blob(data, data_location, azure_conn_string, suffix_seconds=True):
    azure_container_and_file = data_location.split(':')[-1]
    azure_container_prefix = azure_container_and_file.split('/')[0]
    azure_file = '/'.join(azure_container_and_file.split('/')[1:])

    blob_service_client = BlobServiceClient.from_connection_string(
        conn_str=azure_conn_string
    )
    if 'bbc-ds-core-airflow-storage' in data_location:            
        container_name = azure_container_prefix
    elif suffix_seconds:
        timestamp_suffix = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        container_name = azure_container_prefix + timestamp_suffix
    else:
        timestamp_suffix = datetime.datetime.now().strftime('%Y%m%d')
        container_name = azure_container_prefix + timestamp_suffix
    try:
        blob_service_client.create_container(container_name)
    except ResourceExistsError:
        pass

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=azure_file
    )
    if azure_file.endswith('.tar.gz'):
        data.to_csv(azure_file, compression='gzip', index=False)
    else:
        data.to_csv(azure_file, compression='infer', index=False)
    # output = data.to_csv (index=False, encoding = "utf-8")
    with open(azure_file, 'rb') as output_str:
        blob_client.upload_blob(output_str, overwrite=True)
    # os.remove(azure_file)


def map_teams(mapping_table_events, mapping_table_teams):
    
    mapping_table_events['home_team_external_id'] = mapping_table_events['home_team_external_id'].apply(convert_floats_to_str)
    mapping_table_events['away_team_external_id'] = mapping_table_events['away_team_external_id'].apply(convert_floats_to_str)
    
    mapping_table_teams['external_id'] = mapping_table_teams['external_id'].apply(convert_floats_to_str)
    #mapping_table_events['home_team_external_id'] = mapping_table_events['home_team_external_id'].apply(lambda x: str(x))
    #mapping_table_events['away_team_external_id'] = mapping_table_events['away_team_external_id'].apply(lambda x: str(x))

    columns_to_keep = list(mapping_table_events.columns)
    columns_to_keep.append('home_team_internal_id')
    columns_to_keep.append('away_team_internal_id')

    mapping_table_teams = mapping_table_teams.rename({'external_id':'external_team_id', 'team_id':'home_team_internal_id'}, axis = 1)
    mapping_table_events = pd.merge(
        mapping_table_events, 
        mapping_table_teams[['source_id', 'external_team_id', 'home_team_internal_id', 'competition_external_id']], 
        left_on=['source_id', 'home_team_external_id', 'competition_external_id'], 
        right_on= ['source_id', 'external_team_id', 'competition_external_id'], how = 'left')
    mapping_table_events['home_team_internal_id'] = mapping_table_events.apply(
        lambda x: x['home_team_external_id'] if (x['source_id'] == '55da127a-d8b3-4b08-866f-828d33256bef') & (pd.notna(x['home_team_external_id'])) else x['home_team_internal_id'], axis = 1)

    mapping_table_teams = mapping_table_teams.rename({'home_team_internal_id':'away_team_internal_id'}, axis = 1)
    mapping_table_events = pd.merge(mapping_table_events, mapping_table_teams[['source_id', 'external_team_id', 'away_team_internal_id', 'competition_external_id']], left_on=['source_id', 'away_team_external_id', 'competition_external_id'], right_on= ['source_id', 'external_team_id', 'competition_external_id'], how = 'left')
    mapping_table_events['away_team_internal_id'] = mapping_table_events.apply(lambda x: x['away_team_external_id'] if (x['source_id'] == '55da127a-d8b3-4b08-866f-828d33256bef') & (pd.notna(x['away_team_external_id'])) else x['away_team_internal_id'], axis = 1)

    mapping_table_events = mapping_table_events[columns_to_keep]
    
    return mapping_table_events


def map_competitions(mapping_table_events, mapping_table_competitions, master_table_competitions):
    
    mapping_table_events['competition_external_id'] = mapping_table_events['competition_external_id'].apply(convert_floats_to_str)
    #mapping_table_events['competition_external_id'] = mapping_table_events['competition_external_id'].apply(lambda x: str(x))

    mapping_table_competitions['external_id'] = mapping_table_competitions['external_id'].apply(convert_floats_to_str)
        
    columns_to_keep = list(mapping_table_events.columns)
    columns_to_keep.append('competition_internal_id')
    columns_to_keep.append('competition_level')
    columns_to_keep.append('hemisphere')

    mapping_table_competitions = mapping_table_competitions.rename({'external_id':'external_competition_id', 'competition_id':'competition_internal_id'}, axis = 1)
    master_table_competitions = master_table_competitions.rename({'level':'competition_level', 'id':'competition_id'}, axis = 1)
    mapping_table_events = pd.merge(mapping_table_events, mapping_table_competitions[['source_id', 'external_competition_id', 'competition_internal_id']], left_on=['source_id', 'competition_external_id'], right_on= ['source_id', 'external_competition_id'], how = 'left')
    mapping_table_events['competition_internal_id'] = mapping_table_events.apply(lambda x: x['competition_external_id'] if (x['source_id'] == '55da127a-d8b3-4b08-866f-828d33256bef') & (pd.notna(x['competition_external_id'])) else x['competition_internal_id'], axis = 1)

    mapping_table_events = pd.merge(mapping_table_events, master_table_competitions[['competition_id', 'competition_level', 'hemisphere']], left_on=['competition_internal_id'], right_on= ['competition_id'], how = 'left')

    
    mapping_table_events = mapping_table_events[columns_to_keep]
    
    return mapping_table_events

def map_venues(mapping_table_events, mapping_table_venues):
    
    mapping_table_events['venue_external_id'] = mapping_table_events['venue_external_id'].apply(convert_floats_to_str)
    mapping_table_events['venue_external_id'] = mapping_table_events['venue_external_id'].apply(lambda x: np.NaN if x == 'nan' else x)
    
    mapping_table_venues['external_id'] = mapping_table_venues['external_id'].apply(convert_floats_to_str)
    
    columns_to_keep = list(mapping_table_events.columns)
    columns_to_keep.append('venue_internal_id')

    mapping_table_venues = mapping_table_venues.rename({'external_id':'external_venue_id', 'venue_id':'venue_internal_id'}, axis = 1)
    mapping_table_events = pd.merge(mapping_table_events, mapping_table_venues[['source_id', 'external_venue_id', 'venue_internal_id']], left_on=['source_id', 'venue_external_id'], right_on= ['source_id', 'external_venue_id'], how = 'left')
    mapping_table_events['venue_internal_id'] = mapping_table_events.apply(lambda x: x['venue_external_id'] if (x['source_id'] == '55da127a-d8b3-4b08-866f-828d33256bef') & (pd.notna(x['venue_external_id'])) else x['venue_internal_id'], axis = 1)

    mapping_table_events = mapping_table_events[columns_to_keep]
    
    return mapping_table_events


def find_similar_fixtures(mapping_table_events, start_time, home_team_id, away_team_id, days_within, home_score, away_score):
    compare_subset = (mapping_table_events[
        (((mapping_table_events['date'] >= (start_time - datetime.timedelta(days = 0))) & (mapping_table_events['date'] <= (start_time + datetime.timedelta(days = 0))))
        &
        (
            (
            (mapping_table_events['home_team_internal_id'] == home_team_id)
            |
            (mapping_table_events['away_team_internal_id'] == home_team_id)
            )

        |

            (
            (mapping_table_events['home_team_internal_id'] == away_team_id)
            |
            (mapping_table_events['away_team_internal_id'] == away_team_id)
            )
        ))

        |    
              
            
        (((mapping_table_events['date'] >= (start_time - datetime.timedelta(days = 7))) & (mapping_table_events['date'] <= (start_time + datetime.timedelta(days = 7))))
        &
        (
            (
            (mapping_table_events['home_team_internal_id'] == home_team_id)
            &
            (mapping_table_events['away_team_internal_id'] == away_team_id)
            )

        &

            (
            (mapping_table_events['home_score'] == home_score)
            &
            (mapping_table_events['away_score'] == away_score)
            )
            
        &

            (
            (mapping_table_events['home_score'] != 0)
            &
            (mapping_table_events['away_score'] != 0)
            )
        ))
            
            
        |    
            
        (((mapping_table_events['date'] >= (start_time - datetime.timedelta(days = 1))) & (mapping_table_events['date'] <= (start_time + datetime.timedelta(days = 1))))
        &
        (
            (
            (mapping_table_events['home_team_internal_id'] == home_team_id)
            &
            (mapping_table_events['away_team_internal_id'] == away_team_id)
            )

        |

            (
            (mapping_table_events['home_team_internal_id'] == away_team_id)
            &
            (mapping_table_events['away_team_internal_id'] == home_team_id)
            )
            
        &
            (
            (
            (mapping_table_events['home_score'] == home_score)
            &
            (mapping_table_events['away_score'] == away_score)
            )
                
            |
                
            (
            (mapping_table_events['home_score'] == away_score)
            &
            (mapping_table_events['away_score'] == home_score)
            )
            )
            
        ))
        
        ])
        
    return compare_subset


def add_new_event_mappings(mapping_table_events, master_table_events):
    new_events = pd.DataFrame(columns=['id'])
    num_events_added = 0
    columns_to_keep = list(mapping_table_events.columns)
    
    mapping_table_events['date'] = mapping_table_events['start_time'].apply(lambda x: pd.to_datetime(x).date())
    # mapping_table_events = mapping_table_events[
    # (
    #     (
    #         (mapping_table_events['source_id'] == 'a3d57f79-55f4-49ed-87c7-519b67f424ad') & 
    #         (mapping_table_events['competition_level']== '1')
    #         )
    #     ) == False
    # ]


    # Map any event's that currently do not have a mapping
    events_to_map = mapping_table_events[mapping_table_events['event_id'].isna()]
    print('First Filter: ', events_to_map.shape)
    # Do not map any events where we haven't matched the internal ids for home/away team or there is no fixture date
    events_to_map = events_to_map[
        (
            (mapping_table_events['home_team_internal_id'].notna()) & 
            (mapping_table_events['away_team_internal_id'].notna()) & 
            (mapping_table_events['competition_internal_id'].notna()) & 
            (mapping_table_events['home_team_internal_id'].apply(str) !='b636bf37-08d4-46db-84ab-da929d87d6e8') &
            (mapping_table_events['away_team_internal_id'].apply(str) !='b636bf37-08d4-46db-84ab-da929d87d6e8') &
            (mapping_table_events['start_time'].notna())
        )
    ]
    print('Second Filter: ', events_to_map.shape)

    # Do not map any events where ignore is true
    events_to_map = events_to_map[ mapping_table_events['ignore'] != True ]
    print('Third Filter: ', events_to_map.shape)

    for event in events_to_map.index:


#         print(event, len(events_to_map))
        # Set variables to compare
        
        home_team_id = events_to_map.loc[event,'home_team_internal_id']
        away_team_id = events_to_map.loc[event,'away_team_internal_id']
        start_time = events_to_map.loc[event,'date']
        days_within = 2 ## Change it to 1
        home_score = events_to_map.loc[event,'home_score']
        away_score = events_to_map.loc[event,'away_score']


        compare_subset = pd.DataFrame()
        compare_subset = find_similar_fixtures(mapping_table_events, start_time, home_team_id, away_team_id, days_within, home_score, away_score)

        current_internal_id = compare_subset[ pd.notna(compare_subset['event_id']) ]['event_id']

        # If there is a current internal id then add it to the rest of the same fixtures
        if len(current_internal_id) >= 1:
            current_internal_id = current_internal_id.iloc[0]
            events_to_map.loc[event, 'event_id'] = current_internal_id

        # If there is no ID then add one to the master events table and also to each of these mappings
        else:
            print(events_to_map.loc[event,'id'])

#             print('adding a new one')
            num_events_added+=1
            new_uuid = uuid.uuid1()
            new_events = pd.concat([new_events, pd.DataFrame({'id':new_uuid}, index=[event])], ignore_index = True)
            master_table_events = pd.concat([master_table_events, new_events], axis=0, ignore_index=True)
            events_to_map.loc[event, 'event_id'] = new_uuid
            mapping_table_events.loc[event, 'event_id'] = new_uuid
            
    
    try:
        events_to_map['home_score'] = events_to_map['home_score'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['away_score'] = events_to_map['away_score'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['home_halftime_score'] = events_to_map['home_halftime_score'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['away_halftime_score'] = events_to_map['away_halftime_score'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['round'] = events_to_map['round'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['leg'] = events_to_map['leg'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['live_scores'] = events_to_map['live_scores'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['attendance'] = events_to_map['attendance'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['season_id'] = events_to_map['season_id'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
    try:
        events_to_map['resource'] = events_to_map['resource'].apply(convert_to_json)
    except KeyError:
        pass
    try:
        events_to_map['event_game_status_id'] = events_to_map['event_game_status_id'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    except KeyError:
        pass
        
    events_to_map = events_to_map[columns_to_keep]


    print('Number of events added: ', str(num_events_added))
    return events_to_map, new_events
