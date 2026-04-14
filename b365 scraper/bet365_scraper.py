import os
import numpy as np
import pandas as pd
import requests
import json
from time import sleep
import datetime
import uuid

sports_mapping = {
    '1': 'football',
    '8': 'rugby_union',
}

def get_bet365_events(BETSAPI_TOKEN, SPORT_ID='8'):
    
    print('yes')
    
    upcoming_events_link = f'https://api.b365api.com/v3/events/upcoming?sport_id={str(SPORT_ID)}&token={BETSAPI_TOKEN}'
    content = requests.get(upcoming_events_link)
    all_upcoming_games_data = json.loads(content.text)
    all_upcoming_games = pd.DataFrame(all_upcoming_games_data['results'])
    all_upcoming_games['competition_external_id'] = all_upcoming_games['league'].apply(lambda x: x['id'])
    all_upcoming_games['competition_external_name'] = all_upcoming_games['league'].apply(lambda x: x['name'])
    all_upcoming_games['home_team_external_id'] = all_upcoming_games['home'].apply(lambda x: x['id'])
    all_upcoming_games['home_team_external_name'] = all_upcoming_games['home'].apply(lambda x: x['name'])
    all_upcoming_games['away_team_external_id'] = all_upcoming_games['away'].apply(lambda x: x['id'])
    all_upcoming_games['away_team_external_name'] = all_upcoming_games['away'].apply(lambda x: x['name'])
    all_upcoming_games = all_upcoming_games.drop(['league', 'home', 'away'], axis=1)
    all_upcoming_games['time'] = all_upcoming_games['time'].apply(lambda x: pd.to_datetime(datetime.datetime.utcfromtimestamp(int(x)), utc=True))
    all_upcoming_games['updated_at'] = datetime.datetime.utcnow()
    all_upcoming_games['source_id'] = '641913f8-1bf8-486d-841c-ad22d92368c5'
    all_upcoming_games['name'] = all_upcoming_games.apply(lambda x: x['home_team_external_name']+' vs '+x['away_team_external_name'], axis=1)
    all_upcoming_games['home_score'] = all_upcoming_games['ss'].apply(lambda x: np.nan if x is None else x.split('-')[0])
    all_upcoming_games['away_score'] = all_upcoming_games['ss'].apply(lambda x: np.nan if x is None else x.split('-')[1])
    all_upcoming_games['type'] = sports_mapping[SPORT_ID]
    all_upcoming_games = all_upcoming_games[~(all_upcoming_games['name'].str.contains(' 7s'))]
    all_upcoming_games = all_upcoming_games[~(all_upcoming_games['name'].str.contains(' Sevens'))]
    
    return all_upcoming_games


def prepare_bet365_events(events_df):
    events_df = events_df[
        [
            'time', 
            'id',
            'competition_external_id',
            'home_team_external_id',
            'away_team_external_id',
            'source_id',
            'name',
            'home_score',
            'away_score',
            'type',
        ]
    ].rename({'time': 'start_time', 'id': 'external_event_id'}, axis=1)

    events_df['id'] = [uuid.uuid4() for _ in events_df.index]
    events_df['ignore'] = False
    events_df['group'] = None
    events_df['status'] = None

    events_df['away_halftime_score'] = np.nan
    events_df['home_halftime_score'] = np.nan

    # events_df['away_score'] = events_df['away_score'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    # events_df['home_score'] = events_df['home_score'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    # events_df['away_halftime_score'] = events_df['away_halftime_score'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
    # events_df['home_halftime_score'] = events_df['home_halftime_score'].apply(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')


    events_df['round_type_id'] = None
    events_df['event_game_status_id'] = 0
    events_df['live_scores'] = None
    events_df['round'] = None
    events_df['season_id'] = None
    events_df['end_time'] = None
    # events_df['end_time'] = events_df['end_time'].apply(pd.to_datetime)

    # events_df['round'] = None
    # events_df['season_id'] = 0
    events_df['venue_external_id'] = None
    events_df['updated_at'] = datetime.datetime.now()
    events_df['created_at'] = datetime.datetime.now()

    events_df['leg'] = np.nan
    events_df['attendance'] = np.nan
    events_df['gmtoffset'] = np.nan

    events_df['external_id'] = None
    events_df['group_name'] = None
    events_df['resource'] = '{}'
    return events_df

def prepare_teams(all_upcoming_games):
    home_team = all_upcoming_games[
        [
            'source_id',
            'competition_external_id', 
            'home_team_external_id', 
            'home_team_external_name'
        ]
    ].rename(
        {
            'home_team_external_id': 'external_id', 
            'home_team_external_name': 'external_name'
        }, 
        axis=1
    )
    away_team = all_upcoming_games[
        [
            'source_id',
            'competition_external_id', 
            'away_team_external_id', 
            'away_team_external_name'
        ]
    ].rename(
        {
            'away_team_external_id': 'external_id', 
            'away_team_external_name': 'external_name'
        }, 
        axis=1
    )
    teams_df = pd.concat([home_team, away_team], axis=0, ignore_index=True).drop_duplicates()
    return teams_df


def prepare_competitions(all_upcoming_games):
    competition_df = all_upcoming_games[
        [
            'source_id', 
            'competition_external_id', 
            'competition_external_name'
        ]
    ].drop_duplicates().rename({'competition_external_id': 'external_id', 'competition_external_name': 'external_name'}, axis=1)
    return competition_df

def get_betsapi_event_odds(BETSAPI_TOKEN, betsapi_event_id):
    # event_odds_link = 'https://api.b365api.com/v3/bet365/prematch?token={0}&FI={1}'.format(BETSAPI_TOKEN, betsapi_event_id) 
    event_odds_link = 'https://api.b365api.com/v2/event/odds/summary?token={0}&event_id={1}'.format(BETSAPI_TOKEN, betsapi_event_id)

    try:
        content = requests.get(event_odds_link)
        incoming_data = json.loads(content.text)
    except Exception as e:
        print(e)
        incoming_data = {'results': []}
    return incoming_data

def get_odds_data(BETSAPI_TOKEN, all_upcoming_games):
    all_external_event_ids = all_upcoming_games['id'].values.tolist()
    odds_df = pd.DataFrame()
    all_bet365_odds_data = []
    for external_id in all_external_event_ids:
        all_sample_odds_data = get_betsapi_event_odds(BETSAPI_TOKEN, external_id)
        all_sample_odds_data = all_sample_odds_data['results']
        for sample_odds_data in all_sample_odds_data:
            odds_keys = ['main', 'main_2', 'score', 'half', 'team']
            for base_type in odds_keys:
                updated_at = sample_odds_data.get(base_type, {}).get('updated_at', '')
                for k, v in sample_odds_data.get(base_type, {}).get('sp', {}).items():
                    temp_odds_df = pd.DataFrame(v['odds'])
                    temp_odds_df['odds_type'] = k
                    temp_odds_df['external_event_id'] = external_id
                    temp_odds_df['updated_at'] = pd.to_datetime(datetime.datetime.utcfromtimestamp(int(updated_at)), utc=True)
                    odds_df = pd.concat([odds_df, temp_odds_df], axis=0, ignore_index=True)

            for item in sample_odds_data.get('others', {}):
                updated_at = item.get('updated_at')
                sp_data = item.get('sp', {})
                for k, v in sp_data.items():
                    temp_odds_df = pd.DataFrame(v['odds'])
                    temp_odds_df['odds_type'] = k
                    temp_odds_df['external_event_id'] = external_id
                    temp_odds_df['updated_at'] = pd.to_datetime(datetime.datetime.utcfromtimestamp(int(updated_at)), utc=True)
                    odds_df = pd.concat([odds_df, temp_odds_df], axis=0, ignore_index=True)

    odds_df_filtered = odds_df.sort_values(['external_event_id', 'updated_at'], ascending=[True, True]).fillna('NaN')
    odds_df_filtered = odds_df_filtered.groupby(['odds', 'header', 'name', 'handicap', 'odds_type', 'external_event_id']).first().reset_index()
    return odds_df_filtered

def find_home_away_draw(x):
    x = '0' if np.isnan(pd.to_numeric(x, errors='coerce')) else x
    if x=='Tie':
        return 'draw'
    elif str(int(x))=='1':
        return 'home'
    elif str(int(x))=='2':
        return 'away'
    elif str(int(x))=='0':
        return 'draw'
    else:
        return 'draw'


def prepare_1X2_odds(odds_df_filtered, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    oneXtwo_odds = odds_df_filtered[
        (odds_df_filtered['name'] == 'Result') &
        (odds_df_filtered['odds_type'] == 'game_betting_3_way')
    ]

    oneXtwo_odds.loc[:, 'type'] = oneXtwo_odds['header'].apply(find_home_away_draw)
    oneXtwo_odds.loc[:, 'market_id'] = '232c44a7-c08d-4d2a-b6fa-fa1ada61b654'

    oneXtwo_odds = oneXtwo_odds.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
    oneXtwo_odds = oneXtwo_odds[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
    return oneXtwo_odds

def prepare_2_way_handicap_odds(odds_df_filtered, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    handicap_df = odds_df_filtered[odds_df_filtered['odds_type'] == 'alternative_handicap_2_way']
    handicap_df.loc[:, 'type'] = handicap_df['header'].apply(find_home_away_draw)
    handicap_df.loc[:, 'market_id'] = 'c3132af6-4b61-455d-a724-8586bc435b09'

    handicap_df['name'] = handicap_df['name'].apply(pd.to_numeric)
    handicap_df = handicap_df.rename(
        {
            'name': 'type_value', 
            'type': 'sub_type'
        }, 
        axis=1
    ).merge(
        contract_df[
            [
                'contract_id', 
                'market_id', 
                'type_value', 
                'sub_type'
            ]
        ], 
        on=['market_id', 'sub_type', 'type_value'], 
        how='left'
    )

    handicap_df = handicap_df[handicap_df['contract_id'].notna()]
    handicap_df = handicap_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
    return handicap_df

def prepare_3_way_handicap_odds(odds_df_filtered, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    handicap_df = odds_df_filtered[odds_df_filtered['odds_type'] == 'alternative_handicap_3_way']
    handicap_df = pd.concat([handicap_df, odds_df_filtered[
            (odds_df_filtered['name'] == 'Handicap') &
            (odds_df_filtered['odds_type'] == 'game_betting_3_way')
        ], odds_df_filtered[
            (odds_df_filtered['name'] == 'Handicap') &
            (odds_df_filtered['odds_type'] == 'game_betting_2_way')
        ]], 
        ignore_index=True, 
        axis=0
    )


    handicap_df.loc[:, 'type'] = handicap_df['header'].apply(find_home_away_draw)
    handicap_df.loc[:, 'market_id'] = 'bd8bfb37-9c6c-46c6-ad76-5e30f1792c88'

    handicap_df['handicap'] = handicap_df['handicap'].apply(pd.to_numeric)
    handicap_df = handicap_df.rename(
        {
            'handicap': 'type_value', 
            'type': 'sub_type'
        }, 
        axis=1
    ).merge(
        contract_df[
            [
                'contract_id', 
                'market_id', 
                'type_value', 
                'sub_type'
            ]
        ], 
        on=['market_id', 'sub_type', 'type_value'], 
        how='left'
    )
    handicap_df = handicap_df[handicap_df['contract_id'].notna()]
    handicap_df['contract_id'] = handicap_df['contract_id'].apply(str)
    handicap_df = handicap_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']].drop_duplicates()
    handicap_df = handicap_df.groupby(['external_event_id', 'market_id', 'contract_id']).first().reset_index()
    return handicap_df

def prepare_winning_margin_odds(odds_df_filtered, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    winning_margin_df = odds_df_filtered[odds_df_filtered['odds_type'].isin(['winning_margin', 'winning_margin_13_way', 'winning_margin_17_way', 'winning_margin_4_way'])]
    winning_margin_df['header'] = winning_margin_df['header'].apply(find_home_away_draw)
    winning_margin_df['name'] = winning_margin_df['name'].apply(lambda x: x.replace('Tie', 'draw'))

    winning_margin_df['type'] = winning_margin_df[['header', 'name']].apply(lambda x: str(x['header'])+'-'+str(x['name']), axis=1)
    winning_margin_df['type'] = winning_margin_df['type'].str.replace('None-draw', 'draw')

    winning_margin_df['type'] = winning_margin_df['type'].str.replace('+', '-plus')

    winning_margin_df.loc[:, 'market_id'] = '2182ee27-bb39-43b9-bf2d-7f4c72957fbe'
    winning_margin_df = winning_margin_df.merge(
        contract_df[
            [
                'contract_id', 
                'market_id', 
                'type', 
                'sub_type'
            ]
        ], 
        on=['market_id', 'type'], 
        how='left'
    )

    winning_margin_df = winning_margin_df[winning_margin_df['contract_id'].notna()]
    winning_margin_df = winning_margin_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']].drop_duplicates()
    return winning_margin_df

def prepare_event_default_values(odds_df_filtered, event_source_df):
    event_source_df = event_source_df[event_source_df['source_id'].apply(str)=='641913f8-1bf8-486d-841c-ad22d92368c5']

    sample_df = odds_df_filtered[
            (odds_df_filtered['name'] == 'Result') &
            (odds_df_filtered['odds_type'] == 'game_betting_3_way')
        ]
    home_odds = sample_df.loc[sample_df['header']=='1', ['odds', 'external_event_id']]
    away_odds = sample_df.loc[sample_df['header']=='2', ['odds', 'external_event_id']]
    draw_odds = sample_df.loc[sample_df['header']=='Tie', ['odds', 'external_event_id']]

    all_odds = home_odds.merge(away_odds, on='external_event_id', how='outer').merge(draw_odds, on='external_event_id', how='outer')
    all_odds.columns = ['home_odds', 'external_event_id', 'away_odds', 'draw_odds']
    event_source_df['external_event_id'] = event_source_df['external_event_id'].apply(pd.to_numeric)
    all_odds['external_event_id'] = all_odds['external_event_id'].apply(pd.to_numeric)
    
    edv_1X2_odds = event_source_df.merge(all_odds, on='external_event_id', how='inner')
    sample_df = odds_df_filtered[
            (odds_df_filtered['name'] == 'Handicap') &
            (odds_df_filtered['odds_type'] == 'game_betting_3_way')
        ]
    handicap_value = sample_df.loc[sample_df['header']=='1', ['odds', 'external_event_id', 'handicap']]
    edv_handicap_value = edv_1X2_odds.merge(handicap_value[['external_event_id', 'handicap']], on='external_event_id', how='left')
    sample_df = odds_df_filtered[
            (odds_df_filtered['name'] == 'Total') &
            (odds_df_filtered['odds_type'] == 'game_betting_3_way')
        ]
    handicap_value = sample_df.loc[sample_df['header']=='1', ['odds', 'external_event_id', 'handicap']]
    handicap_value['total_points'] = handicap_value['handicap'].str.replace('O ', '')
    handicap_value = handicap_value.drop(['handicap'], axis=1)
    edv_handicap_value = edv_handicap_value.merge(handicap_value[['external_event_id', 'total_points']], on='external_event_id', how='left')
    wm_bucket_odds = odds_df_filtered.loc[odds_df_filtered['odds_type'].isin(['winning_margin_4_way']), ['odds', 'header', 'name', 'external_event_id']]
    wm_bucket_odds['odds'] = wm_bucket_odds['odds'].apply(pd.to_numeric)
    wm_bucket_odds = wm_bucket_odds.pivot_table(index='external_event_id', columns=['header', 'name'], aggfunc=np.mean).reset_index()
    wm_bucket_odds.columns = ['external_event_id', 'home_1_12', 'home_13_plus', 'away_1_12', 'away_13_plus']
    edv_handicap_value = edv_handicap_value.merge(wm_bucket_odds, on='external_event_id', how='left')

    edv_handicap_value = edv_handicap_value[edv_handicap_value['event_id'].notna()]
    edv_handicap_value = edv_handicap_value[['event_id', 'home_odds', 'away_odds', 'draw_odds', 'handicap', 'total_points', 'home_1_12', 'home_13_plus', 'away_1_12', 'away_13_plus']]
    edv_handicap_value = edv_handicap_value.rename(
        {
            'home_odds':'bookmakers_home_odds',
            'away_odds':'bookmakers_away_odds',
            'draw_odds':'bookmakers_draw_odds',
            'handicap':'bookmakers_handicap',
            'total_points': 'bookmakers_total_points',
            'home_1_12': 'bookmakers_win_margin_home_1_12', 
            'home_13_plus': 'bookmakers_win_margin_home_13_plus', 
            'away_1_12': 'bookmakers_win_margin_away_1_12', 
            'away_13_plus': 'bookmakers_win_margin_away_13_plus',
        }, 
        axis=1
    ).drop_duplicates()
    edv_handicap_value = edv_handicap_value.groupby('event_id').first().reset_index()
    return edv_handicap_value


def prepare_odds_df(odds_df):
    odds_df = odds_df.rename({'updated_at': 'time', 'odds': 'value'}, axis=1)
    odds_df['external_platform_id'] = 'bet365'
    odds_df['type'] = 'back'
    odds_df['resource'] = '{}'
    odds_df['volume'] = 0
    odds_df['source_id'] = '641913f8-1bf8-486d-841c-ad22d92368c5'
    odds_df['ladder_position'] = 1
    odds_df['id'] = [uuid.uuid4() for _ in odds_df.index]
    odds_df['created_at'] = datetime.datetime.now()
    odds_df['updated_at'] = datetime.datetime.now()
    odds_df['ignore'] = False
    odds_df['in_play'] = False
    odds_df['calculated_in_play'] = False

    necessary_columns = ['id',
     'time',
     'type',
     'external_event_id',
     'market_id',
     'contract_id',
     'external_platform_id',
     'value',
     'resource',
     'updated_at',
     'created_at',
     'volume',
     'source_id',
     'ladder_position',
     'volume_traded',
     'in_play',
     'home_score',
     'away_score',
     'match_status',
     'match_time',
     'home_quarter_by_quarter',
     'away_quarter_by_quarter',
     'ignore',
     'calculated_in_play']

    missing_cols = list(set(necessary_columns).difference(set(odds_df.columns.tolist())))
    for col in missing_cols:
        odds_df[col]=None
    return odds_df

def get_home_away(header):
    header = '0' if np.isnan(pd.to_numeric(header, errors='coerce')) else header
    if str(int(header)) =='1':
        return 'home'
    elif str(int(header)) =='2':
        return 'away'
    else:
        return 'draw'

def get_exact_winning_margin_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'exact_winning_margin']
    contract_df['id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    contract_df = contract_df[['id', 'type', 'market_id']].rename({'id': 'contract_id', 'type': 'home_away'}, axis=1)
    if sample_df.shape[0]>0:
        sample_df['market_id'] = 'ce6fe371-1c2c-431c-89d8-d98e5a1b3d14'
        sample_df['home_away'] = sample_df['header'].apply(get_home_away)
        sample_df['home_away'] = sample_df.apply(lambda x: x['home_away']+'-'+x['name'], axis=1)
        sample_df.loc[sample_df['name'] == 'Tie', 'home_away'] = 'Tie'
        sample_df = sample_df.merge(contract_df, on=['market_id', 'home_away'], how='left')
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_firsthalf_winning_margin_odds(all_odds, contract_df):
    sample_df = all_odds[(all_odds['odds_type'].isin(['1st_half_winning_margin_17_way', '1st_half_winning_margin_5_way']))]
    contract_df['id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    contract_df = contract_df[['id', 'type', 'market_id']].rename({'id': 'contract_id', 'type': 'home_away'}, axis=1)

    if sample_df.shape[0]>0:
        sample_df['market_id'] = '39d08afc-a72f-438d-881a-ccf9603bc184'
        sample_df['home_away'] = sample_df['header'].apply(get_home_away)
        sample_df['home_away'] = sample_df.apply(lambda x: x['home_away']+'-'+x['name'], axis=1)
        sample_df.loc[sample_df['name'] == 'Tie', 'home_away'] = 'Tie'

        sample_df = sample_df.merge(contract_df, on=['market_id', 'home_away'], how='left')
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_race_to_points_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='race_to_(points)']
    contract_df['id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    contract_df = contract_df[['id','market_id', 'sub_type', 'type_value']].rename({'id': 'contract_id'}, axis=1)

    if sample_df.shape[0]>0:
        sample_df['market_id'] = '84828cb7-14bb-4483-be23-10cff3553a26'
        sample_df['sub_type'] = sample_df['header'].apply(get_home_away)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df.loc[sample_df['name'] == 'Tie', 'sub_type'] = 'Tie'
        sample_df['type_value'] = sample_df['name'].apply(int)
        sample_df = sample_df.merge(contract_df, on=['market_id', 'sub_type', 'type_value'], how='left')
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_halftime_result_odds(all_odds, contract_df):
    sample_df = all_odds[
        (all_odds['name'] == 'Result') &
        (all_odds['odds_type'] == '1st_half_betting_3_way')
    ]
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df.loc[:, 'type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df.loc[:, 'market_id'] = '71d405e6-e5ed-4ccd-8ab5-eada3b318d3e'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_to_lead_after_minutes_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'to_lead_after_(minutes)']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df.loc[:, 'market_id'] = '7be631fb-31d0-4a12-a43d-faf4ce0a40bf'
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_totals_odds(all_odds, contract_df):
    sample_df1 = all_odds[all_odds['odds_type'].isin(['away_team_alternative_totals_3_way', 'away_team_alternative_totals_2_way'])]
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    sample_df1['header'] = sample_df1['header'].apply(lambda x: 'away-'.format(x))
    sample_df2 = all_odds[all_odds['odds_type'].isin(['home_team_alternative_totals_3_way', 'home_team_alternative_totals_2_way'])]
    sample_df2['header'] = sample_df2['header'].apply(lambda x: 'home-{0}'.format(x))
    sample_df = pd.concat([sample_df1, sample_df2], axis=0, ignore_index=True)
    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df.apply(lambda x: x['header']+'-'+x['name'], axis=1)
        sample_df.loc[:, 'market_id'] = '9d613243-597c-495f-8de5-7df19f6ef9d1'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_to_score_last_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'team_to_score_last']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df['name'].apply(find_home_away_draw)
        sample_df['sub_type'] = sample_df['name'].apply(find_home_away_draw)
        sample_df['type_value'] = None
        sample_df.loc[:, 'market_id'] = '7f4ef16d-979d-4260-8b90-5a1ce6b2eaeb'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_to_score_first_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'team_to_score_first']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df['name'].apply(find_home_away_draw)
        sample_df['sub_type'] = sample_df['name'].apply(find_home_away_draw)
        sample_df['type_value'] = None
        sample_df.loc[:, 'market_id'] = '4e68a443-3a2a-4459-9112-083c762a5236'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_10_minute_3_way_odds(all_odds, contract_df):
    sample_df = all_odds[(all_odds['odds_type'].isin(['10_minute_betting_3_way'])) & (all_odds['name'].isin(['To Win', 'Result']))]
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['market_id'] = '09b7776c-bc20-4de1-adc3-4bac7695cc6a'
        sample_df['type_value'] = None
        sample_df.loc[:, 'market_id'] = '4e68a443-3a2a-4459-9112-083c762a5236'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_10_minute_team_total_points(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '10_minute_team_total_points']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['sub_type'] = sample_df['type'].apply(lambda x: x.split(' ')[0])
        sample_df['type_value'] = sample_df['type'].apply(lambda x: pd.to_numeric(x.split(' ')[1]))
        sample_df['market_id'] = '3c4a5f7f-2a52-42d1-a611-6ca862f9e393'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_10_minute_team_total_tries(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '10_minute_team_tries']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['sub_type'] = sample_df['type'].apply(lambda x: x.split(' ')[0])
        sample_df['type_value'] = sample_df['type'].apply(lambda x: pd.to_numeric(x.split(' ')[1]))
        sample_df['market_id'] = '0a0f5a09-e058-4f3b-9664-b4080fd11a3b'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_10_minute_team_total_penalties(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '10_minute_total_penalties_scored']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df.apply(lambda x: x['header'].lower(), axis=1)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['type_value'] = sample_df['name'].apply(pd.to_numeric)
        sample_df['market_id'] = 'ddebbdc1-1a69-44a6-b844-c6c7d9bd3921'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_10_minute_total_points_odd_even(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '10_minute_total_points_odd_even']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df['name'].copy()
        sample_df['sub_type'] = sample_df['name'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = '55bb5745-b103-4400-ad9d-59e3207616d4'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_10_minute_total_tries(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '10_minute_total_tries']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df.apply(lambda x: x['header'].lower(), axis=1)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['type_value'] = sample_df['name'].apply(pd.to_numeric)
        sample_df['market_id'] = '73c9fc93-d339-43fe-8990-256312c29720'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_10_minute_winning_margin(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '10_minute_winning_margin']
    contract_df['id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    contract_df = contract_df[['id', 'type', 'market_id']].rename({'id': 'contract_id', 'type': 'home_away'}, axis=1)

    if sample_df.shape[0]>0:
        sample_df['market_id'] = '47a7efe6-6dab-488a-bf9f-abfa19d7145b'
        sample_df['home_away'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['home_away'] = sample_df.apply(lambda x: x['home_away']+'-'+x['name'], axis=1)
        sample_df.loc[sample_df['name'] == 'Tie', 'home_away'] = 'Tie'

        sample_df = sample_df.merge(contract_df, on=['market_id', 'home_away'], how='left')
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_1st_half_race_to_points_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '1st_half_race_to_(points)']
    contract_df['id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    contract_df = contract_df[['id','market_id', 'sub_type', 'type_value']].rename({'id': 'contract_id'}, axis=1)

    if sample_df.shape[0]>0:
        sample_df['market_id'] = '84b4d6f1-8134-47c3-95fc-c5aec8092f72'
        sample_df['sub_type'] = sample_df['header'].apply(get_home_away)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df.loc[sample_df['name'] == 'Tie', 'sub_type'] = 'Tie'
        sample_df['type_value'] = sample_df['name'].apply(int)
        sample_df = sample_df.merge(contract_df, on=['market_id', 'sub_type', 'type_value'], how='left')
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_1st_half_team_total_points(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '1st_half_team_total_points']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['sub_type'] = sample_df['type'].apply(lambda x: x.split(' ')[0])
        sample_df['type_value'] = sample_df['type'].apply(lambda x: pd.to_numeric(x.split(' ')[1]))
        sample_df['market_id'] = '964b2de9-ce27-48f1-af75-665d65086fc1'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df


def get_1st_half_team_tries(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '1st_half_team_tries']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['sub_type'] = sample_df['type'].apply(lambda x: x.split(' ')[0])
        sample_df['type_value'] = sample_df['type'].apply(lambda x: pd.to_numeric(x.split(' ')[1]))
        sample_df['market_id'] = '864d3462-23fc-4b0e-b4a6-a444bf65ea7b'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df


def get_1st_half_total_odd_even(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='1st_half_total_odd_even']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df['name'].copy()
        sample_df['sub_type'] = sample_df['name'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = '68df00f1-6543-4171-bb9e-6f66b750aa61'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_1st_half_total_tries(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == '1st_half_total_tries']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df.apply(lambda x: x['header'].lower(), axis=1)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['type_value'] = sample_df['name'].apply(pd.to_numeric)
        sample_df['market_id'] = 'cae9d614-fcaf-4167-8b69-79a1dcaed392'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_alternative_total_handicap(all_odds, contract_df):
    sample_df = all_odds[(all_odds['odds_type'].isin([ 'alternative_total_3_way', 'alternative_total_2_way']))]
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(lambda x: x.lower())
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['market_id'] = '85b7a626-5285-4082-a7ab-7464bc297b51'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_alternative_total_tries_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'].isin(['alternative_total_tries_2_way', 'alternative_total_tries_3_way'])]
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df.apply(lambda x: x['header']+'-'+str(pd.to_numeric(x['name'])), axis=1)
        sample_df['sub_type'] = sample_df['header'].copy()
        sample_df['type_value'] = sample_df['name'].apply(pd.to_numeric)
        sample_df['market_id'] = 'bc0797bf-a463-4da5-87f5-8054dd70011f'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_first_scoring_play_event_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'].isin(['first_scoring_play_4_way', 'first_scoring_play_6_way'])]
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = 'a21a6f93-6def-4fc2-9378-8d0d5a2738c9'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_first_try_converted_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'first_try_converted']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df['name'].copy()
        sample_df['sub_type'] = sample_df['name'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = 'dd65ae4b-b333-4d30-aaa0-6540dc3398da'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_highest_scoring_half_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'highest_scoring_half']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].copy()
        sample_df['type'] = sample_df['name'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = '744f94f0-fab2-405e-a2c0-c66f2efb3344'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_match_outcome_4_way(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='match_outcome_4_way']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = '1aca53be-d305-4ac3-b332-fddbe530f085'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_more_tries_or_penalties_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='more_tries_or_penalties']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df['name'].copy()
        sample_df['sub_type'] = sample_df['name'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = '39dfe211-9714-4278-b4c1-7805e6a44e91'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_most_tries_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='most_tries']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['type'] = sample_df['name'].apply(find_home_away_draw)
        sample_df['sub_type'] = sample_df['name'].apply(find_home_away_draw)
        sample_df['type_value'] = None
        sample_df['market_id'] = '3edd07ca-3652-427a-95cc-ac3c236aae76'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_most_tries_handicap_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='most_tries_handicap']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['type_value'] = sample_df['name'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+str(x['type_value']), axis=1)
        sample_df['market_id'] = 'c2d3f3bc-1019-485f-b4b0-94e8e78b6c4e'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_total_tries_odds(all_odds, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    sample_df = all_odds[all_odds['odds_type'].isin(['number_of_tries', 'total_tries_3_way'])]
    if sample_df.shape[0]>0:
        sample_df['type_value'] = sample_df['name'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        sample_df['sub_type'] = sample_df['header'].copy()
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+str(x['type_value']), axis=1)
        sample_df['market_id'] = '42d42680-02fd-4083-bdbb-7135bf208823'
    sample_df1 = all_odds[all_odds['odds_type'] == 'total_tries_(bands)']
    if sample_df1.shape[0]>0:        
        sample_df1['sub_type'] = sample_df1['name'].apply(get_over_under_between)
        sample_df1['type_value'] = sample_df1['sub_type'].apply(lambda x: x[1])
        sample_df1['sub_type'] = sample_df1['sub_type'].apply(lambda x: x[0])
        sample_df1['type'] = sample_df1.apply(lambda x: x['sub_type']+'-'+str(x['type_value']), axis=1)
        sample_df1['type_value'] = sample_df1['type_value'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        sample_df1['market_id'] = '42d42680-02fd-4083-bdbb-7135bf208823'
    sample_df = pd.concat([sample_df, sample_df1], axis=0, ignore_index=True)
    if sample_df.shape[0]>0:
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_first_scoring_play(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_first_scoring_play']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = '9213d36b-517c-41ef-a771-72e3f5ce62a3'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df


def get_more_tries_or_penalties(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_more_tries_or_penalties']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = '5b36c560-4cbe-4446-8293-a4ff6ac7fe1b'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_scoring_first_wins_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_scoring_first_wins_game']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].copy()
        sample_df['type'] = sample_df['name'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = '45d0e679-1ee0-4c20-be0f-8369fd48b791'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_time_of_first_penalty_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'team_time_of_first_penalty']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)

    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].apply(lambda x: x.split('Pen Converted Before')[0])
        sample_df['sub_type'] = sample_df['sub_type'].apply(lambda x: 'Yes' if x=='' else x)
        sample_df['type_value'] = sample_df['name'].apply(lambda x: x.split('Pen Converted Before')[1])
        sample_df['type_value'] = sample_df['type_value'].apply(lambda x: x.replace(' ', '').replace('Mins', ':').replace('Secs', ''))
        sample_df['type_value'] = sample_df['type_value'].apply(lambda x: (pd.to_numeric(x.split(':')[0])*60+ pd.to_numeric(x.split(':')[1]))/60)
        sample_df['type'] = sample_df['name'].copy()
        sample_df['market_id'] = 'f333411e-8096-4bf9-943b-97be51d170e5'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_time_of_first_penalty_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_time_of_first_try']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].apply(lambda x: x.split('Try Before')[0])
        sample_df['sub_type'] = sample_df['sub_type'].apply(lambda x: 'Yes' if x=='' else x)
        sample_df['type_value'] = sample_df['name'].apply(lambda x: x.split('Try Before')[1])
        sample_df['type_value'] = sample_df['type_value'].apply(lambda x: x.replace(' ', '').replace('Mins', ':').replace('Secs', ''))
        sample_df['type_value'] = sample_df['type_value'].apply(lambda x: (pd.to_numeric(x.split(':')[0])*60+ pd.to_numeric(x.split(':')[1]))/60)
        sample_df['type'] = sample_df['name'].copy()
        sample_df['market_id'] = 'f333411e-8096-4bf9-943b-97be51d170e5'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_to_reach_x_points(all_odds, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    sample_df1 = all_odds[all_odds['odds_type']=='team_to_reach_10_points']
    sample_df1['type_value'] = 10
    sample_df2 = all_odds[all_odds['odds_type']=='team_to_reach_20_points']
    sample_df2['type_value'] = 20
    sample_df = pd.concat([sample_df1, sample_df2], axis=0, ignore_index=True)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap']+'-'+str(int(x['type_value'])), axis=1)
        sample_df['market_id'] = '1db7a506-7e08-4b7b-86a8-ace49f053570'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_to_score_first_try_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'team_to_score_first_try']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].apply(find_home_away_draw)
        sample_df['sub_type'] = sample_df['sub_type'].apply(lambda x: 'no try' if x=='draw' else x)
        sample_df['type'] = sample_df['sub_type'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = '2afc098d-a32e-4a34-802a-cb5253e150bb'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df


def get_team_to_trail_in_match(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_to_trail_in_match']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = '92c22eee-bfd9-464b-8bec-485650d7fceb'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_to_win_both_halves_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_to_win_both_halves']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = '70ffd5da-a1ba-46e0-8a82-e98e6f1f0304'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_to_win_either_half_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'] == 'team_to_win_either_half']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = 'e2ce5e4c-af87-40ab-b1da-5a796c3220bc'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_total_points_handicap_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type'].isin( ['team_total_points_2_way', 'team_total_points_3_way'])]
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['handicap_filled'] = sample_df['handicap'].fillna(sample_df['name'])
        sample_df['over_under'] = sample_df['handicap_filled'].apply(lambda x: 'Between' if '-' in x else x.split(' ')[0])
        sample_df['points'] = sample_df['handicap_filled'].apply(lambda x: x if '-' in x else x.split(' ')[1])
        replace_dict = {
            'O': 'Over',
            'U': 'Under'
        }
        sample_df['over_under'] = sample_df['over_under'].replace(replace_dict)
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['over_under']+'-'+x['points'], axis=1)
        sample_df['type_value'] = sample_df['points'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        sample_df['market_id'] = '0344b563-32af-4522-8d39-9df6511697ee'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_total_points_odd_even_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_total_points_odd_even']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = 'b3aa56b3-9db4-4a9a-b72f-c189348aed43'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df


def get_team_total_tries_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_total_tries']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type_value'] = sample_df['handicap'].apply(lambda x: pd.to_numeric(x.split(' ')[1], errors='coerce'))
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['market_id'] = '716440cd-24be-4e15-9d56-8fe19745466c'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_with_highest_scoring_half_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='team_with_highest_scoring_half']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].apply(find_home_away_draw)
        sample_df['type'] = sample_df['sub_type'].copy()
        sample_df['type_value']= None
        sample_df['market_id']= 'e380685a-d483-4ffa-b4d7-626e21a15e05'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_time_of_first_scoring_event_odds(all_odds, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    sample_df = all_odds[all_odds['odds_type'].isin(['time_of_1st_try', 'time_of_first_penalty'])]
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].apply(lambda x: x.lower().split(' before ')[0])
        sample_df['type_value'] = sample_df['name'].apply(lambda x: x.lower().split(' before ')[1])
        sample_df['type_value'] = sample_df['type_value'].apply(lambda x: x.replace(' ', '').replace('mins', ':').replace('secs', ''))
        sample_df['type_value'] = sample_df['type_value'].apply(lambda x: (pd.to_numeric(x.split(':')[0])*60+ pd.to_numeric(x.split(':')[1]))/60)
        sample_df['type'] = sample_df['name'].copy()
        sample_df['market_id'] = '5de0f9eb-e41a-4e68-bb94-ee17a1ae5c41'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df


def get_team_to_score_3_unanswered_tries_odds(all_odds, contract_df):
    sample_df = all_odds[all_odds['odds_type']=='to_score_three_unanswered_tries']
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].apply(find_home_away_draw)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['handicap'], axis=1)
        sample_df['type_value'] = None
        sample_df['market_id'] = 'b3892c25-e840-4c97-8552-a0ba65606ae4'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_team_to_win_both_halves_odds(all_odds, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    sample_df = all_odds[(all_odds['odds_type']=='to_win_both_halves')]
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].copy()
        sample_df['type'] = sample_df['name'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = '63a60ee2-7a31-4f47-b2ec-7d1124ad07ce'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df


def get_total_penalties_scored_odds(all_odds, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    sample_df = all_odds[all_odds['odds_type'].isin(['total_penalties_scored'])]
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].copy()
        sample_df['type_value'] = sample_df['name'].apply(pd.to_numeric)
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['market_id'] = 'b5a08f5b-d2ae-4f23-bad7-6b344e0bf7da'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_over_under_between(x):
    if 'Over' in x:
        return x.split(' ')
    elif 'Under' in x:
        return x.split(' ')
    elif '+' in x:
        return ['Over', pd.to_numeric(x.replace('+', ''))]
    elif '-' in x:
        return ['Between', x]
    elif 'To' in x:
        return ['Between', x.replace(' To ', '-')]
    else:
        return [None, None]

def get_total_points_bands_odds(all_odds, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    sample_df = all_odds[all_odds['odds_type'].isin(['total_points_(bands)', 'total_points_3_way_(range)'])]
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].apply(lambda x: get_over_under_between(x)[0].lower())
        sample_df['type_value'] = sample_df['name'].apply(lambda x: get_over_under_between(x)[1])
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+str(x['type_value']), axis=1)
        sample_df['type_value'] = sample_df['type_value'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        sample_df['market_id'] = '85b7a626-5285-4082-a7ab-7464bc297b51'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_total_points_odd_even_odds(all_odds, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    sample_df = all_odds[all_odds['odds_type']=='total_points_odd_even']
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['name'].copy()
        sample_df['type'] = sample_df['name'].copy()
        sample_df['type_value'] = None
        sample_df['market_id'] = 'b0a442ab-85cb-4a9c-b084-b703ee877114'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def get_total_successful_drop_goals_odds(all_odds, contract_df):
    contract_df = contract_df[['id', 'type', 'market_id', 'sub_type', 'type_value']]
    contract_df['contract_id'] = contract_df['id'].apply(str)
    contract_df['market_id'] = contract_df['market_id'].apply(str)
    sample_df = all_odds[all_odds['odds_type'] == 'total_successful_drop_goals']
    if sample_df.shape[0]>0:
        sample_df['sub_type'] = sample_df['header'].copy()
        sample_df['type_value'] = sample_df['name'].apply(lambda x: pd.to_numeric(x))
        sample_df['type'] = sample_df.apply(lambda x: x['sub_type']+'-'+x['name'], axis=1)
        sample_df['market_id'] = '8f430a5d-395d-4aeb-b905-0a9b4e6d3f6b'
        sample_df = sample_df.merge(contract_df[['market_id', 'type', 'contract_id']], on=['market_id', 'type'], how='left')
        sample_df = sample_df[['odds', 'external_event_id', 'updated_at', 'market_id', 'contract_id']]
        sample_df = sample_df[sample_df['contract_id'].notna()]
        return sample_df

def prepare_odds_source_table(sample_df):
    sample_df['time'] = sample_df['updated_at'].copy()
    sample_df['type'] = 'back'
    sample_df['external_platform_id'] = 'bet365'
    sample_df['value'] = sample_df['odds'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    sample_df['resource'] = '{}'
    sample_df['volume'] = 0
    sample_df['source_id'] = '641913f8-1bf8-486d-841c-ad22d92368c5'
    sample_df['ladder_position'] = 1
    sample_df['volume_traded'] = None
    sample_df['in_play'] = False
    sample_df['home_score'] = 0
    sample_df['away_score'] = 0
    sample_df['match_status'] = 'None'
    sample_df['match_time'] = 0
    sample_df['home_quarter_by_quarter'] = 'None'
    sample_df['away_quarter_by_quarter'] = 'None'
    sample_df['ignore'] = False
    sample_df['calculated_in_play'] = False
    sample_df['id'] = [uuid.uuid4() for _ in sample_df.index]

    sample_df = sample_df[
        [
            'id',
            'time',
            'type',
            'external_event_id',
            'market_id',
            'contract_id',
            'external_platform_id',
            'value',
            'resource',
            'volume',
            'source_id',
            'ladder_position',
            'volume_traded',
            'in_play',
            'home_score',
            'away_score',
            'match_status',
            'match_time',
            'home_quarter_by_quarter',
            'away_quarter_by_quarter',
            'ignore',
            'calculated_in_play'
        ]
    ]
    sample_df = sample_df.groupby(['time', 'type', 'external_event_id', 'market_id', 'contract_id', 'external_platform_id', 'ladder_position', 'in_play', 'source_id']).first().reset_index()
    return sample_df


def prepare_teams(all_upcoming_games):
    home_team = all_upcoming_games[
        [
            'source_id',
            'competition_external_id', 
            'home_team_external_id', 
            'home_team_external_name'
        ]
    ].rename(
        {
            'home_team_external_id': 'external_id', 
            'home_team_external_name': 'external_name'
        }, 
        axis=1
    )
    away_team = all_upcoming_games[
        [
            'source_id',
            'competition_external_id', 
            'away_team_external_id', 
            'away_team_external_name'
        ]
    ].rename(
        {
            'away_team_external_id': 'external_id', 
            'away_team_external_name': 'external_name'
        }, 
        axis=1
    )
    teams_df = pd.concat([home_team, away_team], axis=0, ignore_index=True).drop_duplicates()
    return teams_df


def prepare_competitions(all_upcoming_games):
    competition_df = all_upcoming_games[
        [
            'source_id', 
            'competition_external_id', 
            'competition_external_name'
        ]
    ].drop_duplicates().rename({'competition_external_id': 'external_id', 'competition_external_name': 'external_name'}, axis=1)
    return competition_df
