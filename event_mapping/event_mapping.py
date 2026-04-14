from azureml.core import Workspace, Dataset
import sys
import os
import requests
import logging
import traceback
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def run_event_mappings():
    
    try:
        
        print('run_event_mappings')

        credential = DefaultAzureCredential()  # Uses Managed Identity when in Azure
        key_vault_name = os.environ.get("KEY_VAULT_NAME")
        client = SecretClient(vault_url=key_vault_name, credential=credential)



        ###########################################################################
        ######################## Import database_functions ########################

        # GITHUB_TOKEN = get_keyvault_value(os.environ['KEY_VAULT_NAME'], 'github-common-utils-key')
        GITHUB_TOKEN = client.get_secret('github-common-utils-key').value

        # GitHub repo details
        GITHUB_USERNAME = "bbcrobh"
        REPO_NAME = "common-utils"
        BRANCH = "main"  # Change if using another branch
        FILE_PATH = "common-utils/database_functions.py"# Path to the file inside the repo
        FILE_NAME =  'database_functions.py'

        # GitHub API URL to access raw file
        github_url = f"https://raw.githubusercontent.com/{GITHUB_USERNAME}/{REPO_NAME}/refs/heads/{BRANCH}/{FILE_PATH}"

        # Request headers for authentication
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}

        # Fetch the file
        response = requests.get(github_url, headers=headers)
        # response = requests.get(github_url)

        # Save the file if request is successful
        if response.status_code == 200:
            with open(FILE_NAME, "w", encoding="utf-8") as f:  # Ensure UTF-8 encoding
                f.write(response.text)
            print(f"Successfully downloaded {FILE_NAME}!")

        else:
            print(f"Failed to download file: {response.status_code}, {response.text}")


        import database_functions as dbf


        #####################################################################
        ######################## Import common-utils ########################

        FILE_PATH = "common-utils/common.py"# Path to the file inside the repo
        FILE_NAME =  'common.py'

        # GitHub API URL to access raw file
        github_url = f"https://raw.githubusercontent.com/{GITHUB_USERNAME}/{REPO_NAME}/refs/heads/{BRANCH}/{FILE_PATH}"

        # Request headers for authentication
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}

        # Fetch the file
        response = requests.get(github_url, headers=headers)
        # response = requests.get(github_url)

        # Save the file if request is successful
        if response.status_code == 200:
            with open(FILE_NAME, "w", encoding="utf-8") as f:  # Ensure UTF-8 encoding
                f.write(response.text)
            print(f"Successfully downloaded {FILE_NAME}!")

        else:
            print(f"Failed to download file: {response.status_code}, {response.text}")

        from common import get_sqlalchemy_connection, psql_upsert_copy, map_teams, map_competitions, map_venues, convert_to_json, add_new_event_mappings




        POSTGRESQL_PARAMS = {
          'username': client.get_secret('bbdb-username').value,
          'pass': client.get_secret('bbdb-pass').value,
          'host': client.get_secret('bbdb-host').value,
          'DB': client.get_secret('bbdb-dbname').value
        }



        event_source = dbf.postgres_Retreive_Insert('select * from event_source', POSTGRESQL_PARAMS, True)

        event_source_df = dbf.postgres_Retreive_Insert('select * from event_source', POSTGRESQL_PARAMS, True)

        mapping_table_events = dbf.postgres_Retreive_Insert('select * from event_source', POSTGRESQL_PARAMS, True)

        master_table_events = dbf.postgres_Retreive_Insert('select * from event', POSTGRESQL_PARAMS, True)

        mapping_table_teams = dbf.postgres_Retreive_Insert('select * from team', POSTGRESQL_PARAMS, True)

        mapping_table_teams_competitions = dbf.postgres_Retreive_Insert('select * from team_source_comp', POSTGRESQL_PARAMS, True)

        master_table_competitions = dbf.postgres_Retreive_Insert('select * from competition', POSTGRESQL_PARAMS, True)

        mapping_table_competitions = dbf.postgres_Retreive_Insert('select * from competition_source', POSTGRESQL_PARAMS, True)

        mapping_table_venues = dbf.postgres_Retreive_Insert('select * from venue_source', POSTGRESQL_PARAMS, True)

        mapping_table_events = dbf.postgres_Retreive_Insert('select * from event_source', POSTGRESQL_PARAMS, True)

        mapping_table_init_cols = mapping_table_events.columns.tolist()

        mapping_table_events = map_teams(mapping_table_events, mapping_table_teams_competitions)


        mapping_table_events = map_competitions(mapping_table_events, mapping_table_competitions, master_table_competitions)
        mapping_table_events = map_venues(mapping_table_events, mapping_table_venues)
        # Make sure teams with the same names have their appropriate id's using their competition

        mapping_table_events_updated, master_table_events_updated = add_new_event_mappings(mapping_table_events, master_table_events)
        mapping_table_events_updated['resource'] = mapping_table_events_updated['resource'].apply(convert_to_json)
        mapping_table_events_updated = mapping_table_events_updated[mapping_table_init_cols].drop_duplicates()

        mapping_table_events_updated['resource'] = mapping_table_events_updated['resource'].apply(lambda x: '{}' if '\\\\\\' in str(x) else x)

        engine = get_sqlalchemy_connection()
        master_table_events_updated.to_sql(name='event', schema='public', con=engine, index=False, if_exists='append')
        mapping_table_events_updated.to_sql(name='event_source', schema='public', con=engine, index=False, if_exists='append', method=psql_upsert_copy)

        string_to_post = "✅ Event mapping completed.".encode("utf-8", "ignore").decode("utf-8")
        dbf.notifyTelegram(os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID"), os.environ.get("TELEGRAM_ALLOWED_USERS"), "Event Mapping", string_to_post)


    except Exception as e:
        string_to_post = "❌ Failed to update event mappings.".encode("utf-8", "ignore").decode("utf-8")
        print(string_to_post)
        logging.error(string_to_post)
        logging.error(traceback.format_exc())
        dbf.notifyTelegram(os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID"), os.environ.get("TELEGRAM_ALLOWED_USERS"), "URGENT - Event Mapping", string_to_post)

