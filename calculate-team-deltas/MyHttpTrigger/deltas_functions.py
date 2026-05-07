def get_all_events():
    
    function_start_time = datetime.datetime.now()
    print('-get_all_events')

    sql_statement = "select * from view_event order by start_time asc;"
    all_events, error_occured = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, True)
    
    # Drop Mayanks calculated columns and use my own
    for col in all_events.columns:

        if col in ['home_team_national_team_id', 'away_team_national_team_id',
           'home_team_type', 'away_team_type',
           'venue_national_team', 'home_previous_n_games_with_venue',
           'num_fix_last_20_games', 'venue_perc', 'home_venue', 'home_travelled',
           'away_travelled', 'travel_advantage']:
            all_events.drop(col, axis = 1, inplace = True)

    
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return all_events



def get_all_previous_deltas(float_columns):
    
    function_start_time = datetime.datetime.now()
    print('-get_all_previous_deltas')

    sql_statement = "select * from event_deltas order by start_time asc;"
    all_deltas, error_occured = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, True)
    
    #all_deltas = pd.read_csv('event_deltas.csv')
    for col in float_columns:
        
        all_deltas[col] = all_deltas[col].apply(lambda x: float(x) if pd.notna(x) else None)
        
    #all_deltas['away_pre_delta'] = all_deltas['away_pre_delta'].apply(lambda x: float(x))
    #all_deltas['home_post_delta'] = all_deltas['home_post_delta'].apply(lambda x: float(x))
    #all_deltas['away_post_delta'] = all_deltas['away_post_delta'].apply(lambda x: float(x))

    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return all_deltas




def fixtures_to_remove(vec):
    
    to_remove = False
    
    home_score = vec[0]
    away_score = vec[1]
    fix_date = vec[2].replace(tzinfo=None)
    
    if (home_score == 0) & (away_score == 0):
        
        to_remove = True
        
    elif pd.isna(home_score) | pd.isna(away_score):
        
        to_remove = True
        
    elif (pd.to_datetime(fix_date) > pd.to_datetime('2019-01-03')) & (pd.to_datetime(fix_date) < pd.to_datetime('2022-09-01')):
        if (home_score == 28) & (away_score == 0):
            to_remove = True
        elif (home_score == 0) & (away_score == 28):
            to_remove = True  
        
    return to_remove




def remove_faulty_fixtures(all_events):
    
    function_start_time = datetime.datetime.now()
    print('-remove_faulty_fixtures')
    
    all_events['to_remove'] = all_events[['home_score', 'away_score', 'start_time']].apply(lambda x: fixtures_to_remove(x), axis = 1)
    
    all_events = all_events[(all_events['to_remove']==False) | (all_events['start_time']>(str(pd.to_datetime(datetime.datetime.now() - datetime.timedelta(days = 1)).replace(tzinfo=None).date())))]

    all_events = all_events[ all_events['start_time'] > '2003-01-01']
    all_events.drop('to_remove', axis = 1, inplace = True)
    
    
    
    # Make sure future events are set to None and not 0 for their scores
    future_events = all_events[ all_events['start_time'] >= str(datetime.datetime.now())].index
    all_events.loc[future_events, 'home_score'] = None
    all_events.loc[future_events, 'away_score'] = None
    
    
    ######### Sort out games that have no IDs #########
    games_with_no_ids = list(all_events[ pd.isna(all_events['home_team_internal_id']) | pd.isna(all_events['away_team_internal_id']) ]['event_id'])

    if len(games_with_no_ids) > 0:

        # Alert teams with the events that don't have an id
        message = 'There are events where there are no ids for the homeor away team (' + str(games_with_no_ids) + ')'
        notifyTeams(message)
        print(message)

        all_events = all_events[ ~all_events['event_id'].isin(games_with_no_ids) ]
    ###################################################



    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return all_events




def check_fixtures_that_have_changed(all_previous_deltas, all_events):
    
    function_start_time = datetime.datetime.now()
    print('-check_fixtures_that_have_changed')
    
    cols_to_keep = all_previous_deltas.columns
    
    all_previous_deltas = all_previous_deltas.merge(all_events[['event_id', 'home_team_internal_id', 'away_team_internal_id', 'competition_internal_id', 'home_score', 'away_score', 'venue_internal_id']].rename(columns = {'event_id':'event_id_AE', 'home_team_internal_id':'home_team_internal_id_AE', 'away_team_internal_id':'away_team_internal_id_AE', 'competition_internal_id':'competition_internal_id_AE', 'home_score':'home_score_AE', 'away_score':'away_score_AE', 'venue_internal_id':'venue_internal_id_AE'}), how = 'left', left_on = 'event_id', right_on = 'event_id_AE')

    
    # Check for fixtures that have changed since ELO's have last been updated
    all_previous_deltas['competition_internal_id_different'] = all_previous_deltas['competition_internal_id'] == all_previous_deltas['competition_internal_id_AE']
    all_previous_deltas['home_team_internal_id_different'] = all_previous_deltas['home_team_internal_id'] == all_previous_deltas['home_team_internal_id_AE']
    all_previous_deltas['away_team_internal_id_different'] = all_previous_deltas['away_team_internal_id'] == all_previous_deltas['away_team_internal_id_AE']
    all_previous_deltas['home_score_different'] = all_previous_deltas['home_score'] == all_previous_deltas['home_score_AE']
    all_previous_deltas['away_score_different'] = all_previous_deltas['away_score'] == all_previous_deltas['away_score_AE']
    
    for record in all_previous_deltas.index:
        if all_previous_deltas.loc[record, 'venue_internal_id'] == all_previous_deltas.loc[record, 'venue_internal_id_AE']:
            all_previous_deltas['venue_different'] = True
        else:
            all_previous_deltas['venue_different'] = False
    #all_previous_deltas['venue_different'] = all_previous_deltas['venue_internal_id'] == all_previous_deltas['venue_internal_id_AE']

    earliest_fixture_change = all_previous_deltas[ (all_previous_deltas['competition_internal_id_different'] == False) | (all_previous_deltas['home_team_internal_id_different'] == False) | (all_previous_deltas['away_team_internal_id_different'] == False) | (all_previous_deltas['home_score_different'] == False) | (all_previous_deltas['away_score_different'] == False) | (all_previous_deltas['venue_different'] == False) ]['start_time'].min()
    
    if pd.notna(earliest_fixture_change):
        previous_deltas_to_keep = all_previous_deltas[ all_previous_deltas['start_time'] < earliest_fixture_change]
    else:
        previous_deltas_to_keep = all_previous_deltas
        
    # Check for fixtures that are no longer in the database
    earliest_fixture_change = all_previous_deltas[ pd.isna(all_previous_deltas['competition_internal_id_different']) ]['start_time'].min()
    if pd.notna(earliest_fixture_change):
        previous_deltas_to_keep = previous_deltas_to_keep[ previous_deltas_to_keep['start_time'] < earliest_fixture_change]

    
    previous_deltas_to_keep = previous_deltas_to_keep[cols_to_keep]

    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return previous_deltas_to_keep




def check_for_any_new_events(all_events, all_previous_deltas, float_columns):
    
    function_start_time = datetime.datetime.now()
    print('-check_for_any_new_events')
    
    temp_columns = float_columns.copy()
    temp_columns.append('event_id')
    all_previous_deltas = all_previous_deltas[temp_columns]
    
    all_events = all_events.merge(all_previous_deltas.rename(columns = {'event_id':'event_id_PD'}), how = 'left', left_on = 'event_id', right_on = 'event_id_PD').reset_index()    
    earliest_fixture_change = all_events[ pd.isna(all_events['home_pre_delta']) |  pd.isna(all_events['home_post_delta']) |  pd.isna(all_events['away_pre_delta']) |  pd.isna(all_events['away_post_delta']) ]['start_time'].min()

    for col in float_columns:
        all_events.loc[ all_events['start_time'] >= earliest_fixture_change, col] = None
    #all_events.loc[ all_events['start_time'] >= earliest_fixture_change, 'away_pre_delta'] = None
    #all_events.loc[ all_events['start_time'] >= earliest_fixture_change, 'home_post_delta'] = None
    #all_events.loc[ all_events['start_time'] >= earliest_fixture_change, 'away_post_delta'] = None

    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')
    
    return all_events




def get_team_fixture_numbers(all_events):
    
    function_start_time = datetime.datetime.now()
    print('-get_team_fixture_numbers')

    all_events['home_team_total_fixture_number'] = None
    all_events['home_team_home_fixture_number'] = None
    all_events['away_team_total_fixture_number'] = None
    all_events['away_team_away_fixture_number'] = None

    home_teams = all_events[['home_team_internal_id', 'start_time']].rename(columns = {'home_team_internal_id' : 'team_id'})
    home_teams['h_a'] = 'Home'
    away_teams = all_events[['away_team_internal_id', 'start_time']].rename(columns = {'away_team_internal_id' : 'team_id'})
    away_teams['h_a'] = 'Away'

    fixtures_by_team = pd.concat([home_teams, away_teams])
    #fixtures_by_team = home_teams.append(away_teams)
    fixtures_by_team.sort_values('start_time', inplace = True)
    
    fixtures_by_team['team_total_fixture_numbers'] = fixtures_by_team.groupby(['team_id'])['start_time'].rank()
    fixtures_by_team['team_ha_fixture_numbers'] = fixtures_by_team.groupby(['team_id', 'h_a'])['start_time'].rank()
    
    home_team_fix = fixtures_by_team[ fixtures_by_team['h_a'] == 'Home']
    all_events['home_team_total_fixture_number'].update(home_team_fix['team_total_fixture_numbers'])
    all_events['home_team_home_fixture_number'].update(home_team_fix['team_ha_fixture_numbers'])

    away_team_fix = fixtures_by_team[ fixtures_by_team['h_a'] == 'Away']
    all_events['away_team_total_fixture_number'].update(away_team_fix['team_total_fixture_numbers'])
    all_events['away_team_away_fixture_number'].update(away_team_fix['team_ha_fixture_numbers'])

    return all_events




def get_competition_fixture_numbers():
    
    function_start_time = datetime.datetime.now()
    print('-get_competition_fixture_numbers')

    all_events['competition_fixture_number'] = None
    all_events.sort_values('start_time', inplace = True)

    all_events['competition_fixture_number'] = all_events.groupby(['competition_internal_id'])['start_time'].rank()
    all_events['home_competition_fixture_number'] = all_events.groupby(['home_competition_group'])['start_time'].rank()

    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return all_events



def update_home_venue_on_venue_nation(vec):
    
    
    home_team_type = vec[0]
    home_team_internal_id = vec[1]
    venue_national_team = vec[2]
    home_venue = vec[3]
    home_team_national_team_id = vec[4]
    venue_id = vec[5]
    
    teams_to_leave = ['564ae346-5235-40b8-ab48-fc4f435601be']

    # Change International mens venue to True if they are playing in the same country
    if home_team_type == 'international':

        if home_team_internal_id == venue_national_team:
            home_venue = True
        elif (home_team_internal_id != venue_national_team) & pd.notna(venue_national_team):
            home_venue = False

    # Change other international teams if its in the same country
    elif home_team_type.find('international') >= 0:

        if home_team_national_team_id == venue_national_team:
            home_venue = True
        elif (home_team_internal_id != venue_national_team) & pd.notna(venue_national_team):
            home_venue = False

    elif home_team_internal_id not in teams_to_leave:

        if (home_team_national_team_id != venue_national_team) & pd.notna(home_team_national_team_id) & pd.notna(venue_national_team) & pd.notna(venue_id):
            home_venue = False


    return home_venue



def calculate_travel_advantage(vec):
    
    team_home_countries = {'564ae346-5235-40b8-ab48-fc4f435601be':'cd0d53f5-4a11-4ded-8594-641ef025842d'}
    
    home_team_type = vec[0]
    team_internal_id = vec[1]
    venue_national_team = vec[2]
    team_national_team_id = vec[3]
    
    if team_internal_id in team_home_countries.keys():
    
        if (team_home_countries['564ae346-5235-40b8-ab48-fc4f435601be'] != venue_national_team) & pd.notna(venue_national_team):
            travel_advantage = 1
        else:
            travel_advantage = 0
    
    else:
        
        if home_team_type == 'international':

            if (team_internal_id != venue_national_team) & pd.notna(venue_national_team):
                travel_advantage = 1
            else:
                travel_advantage = 0


        else:
            if (team_national_team_id != venue_national_team) & pd.notna(team_national_team_id) & pd.notna(venue_national_team):
                travel_advantage = 1
            else:
                travel_advantage = 0


    return travel_advantage




def set_home_venues(all_events, all_teams, all_venues):
    
    function_start_time = datetime.datetime.now()
    print('-set_home_venues')
    
    fixture_length = 20

    all_events.loc[ : , 'home_venue'] = True
    all_events.loc[ : , 'venue_perc'] = None


    for team_id in all_events['home_team_internal_id'].drop_duplicates():

        team_df = all_events[ all_events['home_team_internal_id'] == team_id]


        for fix in team_df.index:

            venue_id = team_df.loc[fix, 'venue_internal_id']

            if pd.notna(venue_id):

                current_fix_num = team_df.loc[fix, 'home_team_home_fixture_number']
                number_of_previous_games_at_venue = len(team_df[ (team_df['venue_internal_id'] == venue_id) & (team_df['home_team_home_fixture_number'] <= current_fix_num) & (team_df['home_team_home_fixture_number'] > (current_fix_num - fixture_length)) ])
                number_of_previous_games_with_venue = len(team_df[ pd.notna(team_df['venue_internal_id']) & (team_df['home_team_home_fixture_number'] <= current_fix_num) & (team_df['home_team_home_fixture_number'] > (current_fix_num - fixture_length)) ])

                venue_perc = number_of_previous_games_at_venue / number_of_previous_games_with_venue
                team_df.loc[fix,'venue_perc'] = venue_perc
                all_events['venue_perc'].update(team_df['venue_perc'])

                if venue_perc < 0.2:
                    team_df.loc[fix,'home_venue'] = False
                    all_events['home_venue'].update(team_df['home_venue'])
                    

    # Add national team for home and away teams 
    all_events = all_events.merge(all_teams[['id', 'type', 'national_team_id']].rename(columns = {'id':'home_team_internal_id', 'type':'home_team_type', 'national_team_id':'home_team_national_team_id'}), how = 'left', left_on = 'home_team_internal_id', right_on = 'home_team_internal_id')
    all_events = all_events.merge(all_teams[['id', 'type', 'national_team_id']].rename(columns = {'id':'away_team_internal_id', 'type':'away_team_type', 'national_team_id':'away_team_national_team_id'}), how = 'left', left_on = 'away_team_internal_id', right_on = 'away_team_internal_id')
    
    all_events['home_venue'] = all_events[['home_team_type', 'home_team_internal_id', 'venue_national_team', 'home_venue', 'home_team_national_team_id', 'venue_internal_id']].apply(lambda x: update_home_venue_on_venue_nation(x), axis = 1)
    all_events['home_travelled'] = all_events[['home_team_type', 'home_team_internal_id', 'venue_national_team', 'home_team_national_team_id']].apply(lambda x: calculate_travel_advantage(x), axis = 1)
    all_events['away_travelled'] = all_events[['away_team_type', 'away_team_internal_id', 'venue_national_team', 'away_team_national_team_id']].apply(lambda x: calculate_travel_advantage(x), axis = 1)
    all_events['travel_advantage'] =  all_events[['home_travelled', 'away_travelled']].apply(lambda x: -1 if ( (x[0] == 1) and (x[1] == 0)) else 0 if ( (x[0] == 0) and (x[1] == 0) ) else 0 if ( (x[0] == 1) and (x[1] == 1) ) else 1 if ( (x[0] == 0) and (x[1] == 1) ) else 0, axis = 1)

    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return all_events





def get_comp_standards(all_events, delta_column_to_calcuate):
    
    function_start_time = datetime.datetime.now()
    print('-get_comp_standards')
    
    comp_standards = all_events[ (all_events['start_time'] < '2010-01-01') & pd.notna(all_events[delta_column_to_calcuate]) ]

    all_base_home_win_margin = comp_standards[delta_column_to_calcuate].median()
    international_mens_base_home_win_margin = comp_standards[ comp_standards['home_competition_group'] == 'international_mens'][delta_column_to_calcuate].median()
    international_womens_base_home_win_margin = comp_standards[ comp_standards['home_competition_group'] == 'international_womens'][delta_column_to_calcuate].median()
    comp_standards = comp_standards[ (comp_standards['home_competition_group'] != 'international_mens')  & (comp_standards['home_competition_group'] != 'international_womens') ]
    
    competition_win_margin_means = comp_standards[['competition_internal_id', 'competition_name', delta_column_to_calcuate]].groupby(['competition_internal_id', 'competition_name']).median().reset_index()
    competition_win_margin_count = comp_standards[['competition_internal_id', 'competition_name', delta_column_to_calcuate]].groupby(['competition_internal_id', 'competition_name']).count().reset_index()

    competition_win_margin_means = competition_win_margin_means.merge(competition_win_margin_count[['competition_internal_id', delta_column_to_calcuate]].rename(columns={delta_column_to_calcuate:'count'}), how = 'left', left_on = 'competition_internal_id', right_on = 'competition_internal_id')
    competition_win_margin_means = competition_win_margin_means[competition_win_margin_means['count']>100]
    
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return all_base_home_win_margin, international_mens_base_home_win_margin, international_womens_base_home_win_margin, competition_win_margin_means




def check_success(all_events, validation_start, validation_end):
    
    function_start_time = datetime.datetime.now()
    print('-check_success')
    
    all_events['success'] = all_events[[delta_column_to_calcuate, pre_delta_diff_name, 'home_score', 'away_score']].apply(lambda x: None if (pd.isna(x[2]) & pd.isna(x[3])) else 1 if ((x[0] > 0) & (x[1] > 0)) | ((x[0] < 0) & (x[1] < 0)) else 0 , axis = 1 )

    validation_period = all_events[ (all_events['start_time'] >= validation_start) & (all_events['start_time'] <= validation_end) ]
    #validation_period = all_events
    correct = len(validation_period[(validation_period['success'] == 1)])
    incorrect = len(validation_period[(validation_period['success'] == 0)])

    success = correct/(correct+incorrect)
    
    end_time = datetime.datetime.now()
    print('--Complete-' + str(end_time - function_start_time))
    print('')

    return success




def generate_elo_ranks(all_events, delta_column_to_calcuate, post_delta_adjustment_name, home_pre_delta_name, home_post_delta_name, away_pre_delta_name, away_post_delta_name, pre_delta_diff_name, home_team_buffer_name, max_points_win, win_margin_buffer, level_setting, start_range, end_range, list_order_int_men, list_order_int_women_age, list_order_club, win_bonus, all_competitions, all_teams, home_team_fixture_column, away_team_fixture_column, home_error, away_error):
    


    max_points_win = 5
    win_margin_buffer = 0
    level_setting = 40
    win_bonus = 0

    loop_num = 0


    function_start_time = datetime.datetime.now()
    print('-generate_elo_ranks')

    all_events.reset_index(inplace = True, drop = True)
    #all_events['post_game_rank_home'] = None
    #all_events['post_game_rank_away'] = None
    #nall_events[home_team_buffer_name] = None
    all_events['pre_game_rank_historic_home_competition_group_min_home'] = None
    all_events['pre_game_rank_historic_home_competition_group_median_home'] = None
    all_events['pre_game_rank_senior_team_ranking_home'] = None
    all_events['pre_game_rank_int_comp_level_setting_home'] = None
    all_events['pre_game_rank_new_home_competition_group_home'] = None
    all_events['pre_game_rank_historic_home_competition_group_min_away'] = None
    all_events['pre_game_rank_historic_home_competition_group_median_away'] = None
    all_events['pre_game_rank_senior_team_ranking_away'] = None
    all_events['pre_game_rank_int_comp_level_setting_away'] = None
    all_events['pre_game_rank_new_home_competition_group_away'] = None
    #all_events[pre_delta_diff_name] = None
    #all_events[post_delta_adjustment_name] = None

    home_pre_elo = None
    away_pre_elo = None
    pre_elo_diff = None


    print(str(loop_num), '--updating from', start_range, end_range)
    for event in range(start_range,end_range+1):
    #for event in range(12562,12562+1):


            print('--', event, end_range)
        
        
            start_time = all_events.loc[event]['start_time']

#             if start_time < pd.to_datetime('2010-01-01', utc=True):
#                 home_pre_elo = all_events.loc[event]['home_pre_delta']
#                 away_pre_elo = all_events.loc[event]['away_pre_delta']
#             else:
            home_pre_elo = None
            away_pre_elo = None

            home_team_internal_id = all_events.loc[event]['home_team_internal_id']
            home_team_total_fixture_number = all_events.loc[event]['home_team_total_fixture_number']

            away_team_internal_id = all_events.loc[event]['away_team_internal_id']
            away_team_total_fixture_number = all_events.loc[event]['away_team_total_fixture_number']

            actual_win_margin = all_events.loc[event, delta_column_to_calcuate]
            
            home_venue = all_events.loc[event]['home_venue']

            start_time = all_events.loc[event]['start_time']
            competition_id = all_events.loc[event]['competition_internal_id']
            competition_fixture_number = all_events.loc[event]['competition_fixture_number']
            home_competition_group = all_events.loc[event]['home_competition_group']
            home_competition_fixture_number = all_events.loc[event]['home_competition_fixture_number']
            
            if competition_id in points_transfer_dict.keys():
                transfer_multiplier = points_transfer_dict[competition_id]
            else:
                transfer_multiplier = 1

            # Get team travelled buffer
            travel_buffer = all_events.loc[event, 'travel_advantage']
            #travel_buffer = 0

            # Home team is playing in away teams country
            # So set to home venue but reverse the impact
            if travel_buffer == -1:
                home_venue = True

            if home_venue == True:

                home_team_buffer = get_home_team_buffer(all_events, start_time, competition_id, competition_fixture_number, home_competition_group, home_competition_fixture_number, delta_column_to_calcuate)

                if travel_buffer == -1:
                    home_team_buffer = -home_team_buffer

                all_events.loc[event, home_team_buffer_name] = home_team_buffer

            else:
                home_team_buffer = 0
                all_events.loc[event, home_team_buffer_name] = 0


            #print(home_pre_elo)
            if pd.isna(home_pre_elo):
                #print('1')

                if home_team_total_fixture_number == 1:
                    #print('2')

                    if delta_column_to_calcuate in ['half_time_win_margin', 'second_half_win_margin']:

                        #home_pre_elo = all_events.loc[event, 'home_pre_delta'] / 2
                        home_pre_elo = 0

                    else:

                        if away_team_total_fixture_number >= 5:

                            win_margin_for_delta = all_events.loc[event, 'win_margin']
                            if pd.isna(win_margin_for_delta):
                                win_margin_for_delta = 0

                            away_pre_elo_temp = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name, away_post_delta_name, home_team_fixture_column, away_team_fixture_column)
                            home_pre_elo = away_pre_elo_temp + win_margin_for_delta - home_team_buffer

                        else:


                            # Get appropriate ELO to work with
                            home_pre_elo, rank_set_type, pre_game_rank_historic_home_competition_group_min, pre_game_rank_historic_home_competition_group_median, pre_game_rank_senior_team_ranking, pre_game_rank_int_comp_level_setting, pre_game_rank_new_home_competition_group, pre_game_rank_historic_competition_min, pre_game_rank_historic_competition_median = generate_predicted_elo(all_events, home_team_internal_id, home_team_total_fixture_number, competition_id, competition_fixture_number, home_competition_group, start_time, level_setting, event, list_order_int_men, list_order_int_women_age, list_order_club, home_post_delta_name, away_post_delta_name, home_pre_delta_name, away_pre_delta_name, all_competitions, all_teams, internationl_rankings_df)
                            all_events.loc[event, home_pre_delta_name] = home_pre_elo
                            all_events.loc[event, 'rank_set_type_home'] = rank_set_type
                            all_events.loc[event, 'pre_game_rank_historic_home_competition_group_min_home'] = pre_game_rank_historic_home_competition_group_min
                            all_events.loc[event, 'pre_game_rank_historic_home_competition_group_median_home'] = pre_game_rank_historic_home_competition_group_median
                            all_events.loc[event, 'pre_game_rank_senior_team_ranking_home'] = pre_game_rank_senior_team_ranking
                            all_events.loc[event, 'pre_game_rank_int_comp_level_setting_home'] = pre_game_rank_int_comp_level_setting
                            all_events.loc[event, 'pre_game_rank_new_home_competition_group_home'] = pre_game_rank_new_home_competition_group
                            all_events.loc[event, 'pre_game_rank_historic_competition_min_home'] = pre_game_rank_historic_competition_min
                            all_events.loc[event, 'pre_game_rank_historic_competition_median_home'] = pre_game_rank_historic_competition_median

                else:
                    #print('3')

                    home_pre_elo = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name, away_post_delta_name, home_team_fixture_column, away_team_fixture_column)

                all_events.loc[event, home_pre_delta_name] = home_pre_elo

            if pd.isna(home_pre_elo):
                
                print('The home delta is blank for event ', event, all_events.loc[event, 'name'])
                

                
            if pd.isna(away_pre_elo):

                #print('1')
                if away_team_total_fixture_number == 1:

                    if delta_column_to_calcuate in ['half_time_win_margin', 'second_half_win_margin']:

                        #away_pre_elo = all_events.loc[event, 'away_pre_delta'] / 2
                        away_pre_elo = 0

                    else:

                        if home_team_total_fixture_number >= 5:

                            win_margin_for_delta = all_events.loc[event, 'win_margin']
                            if pd.isna(win_margin_for_delta):
                                win_margin_for_delta = 0

                            home_pre_elo_temp = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name, away_post_delta_name, home_team_fixture_column, away_team_fixture_column)
                            away_pre_elo = home_pre_elo_temp - win_margin_for_delta + home_team_buffer

                        else:

                            # Get appropriate ELO to work with
                            away_pre_elo, rank_set_type, pre_game_rank_historic_home_competition_group_min, pre_game_rank_historic_home_competition_group_median, pre_game_rank_senior_team_ranking, pre_game_rank_int_comp_level_setting, pre_game_rank_new_home_competition_group, pre_game_rank_historic_competition_min, pre_game_rank_historic_competition_median = generate_predicted_elo(all_events, away_team_internal_id, away_team_total_fixture_number, competition_id, competition_fixture_number, home_competition_group, start_time, level_setting, event, list_order_int_men, list_order_int_women_age, list_order_club, home_post_delta_name, away_post_delta_name, home_pre_delta_name, away_pre_delta_name, all_competitions, all_teams, internationl_rankings_df)                
                            all_events.loc[event, away_pre_delta_name] = away_pre_elo
                            all_events.loc[event, 'rank_set_type_away'] = rank_set_type
                            all_events.loc[event, 'pre_game_rank_historic_home_competition_group_min_away'] = pre_game_rank_historic_home_competition_group_min
                            all_events.loc[event, 'pre_game_rank_historic_home_competition_group_median_away'] = pre_game_rank_historic_home_competition_group_median
                            all_events.loc[event, 'pre_game_rank_senior_team_ranking_away'] = pre_game_rank_senior_team_ranking
                            all_events.loc[event, 'pre_game_rank_int_comp_level_setting_away'] = pre_game_rank_int_comp_level_setting
                            all_events.loc[event, 'pre_game_rank_new_home_competition_group_away'] = pre_game_rank_new_home_competition_group
                            all_events.loc[event, 'pre_game_rank_historic_competition_min_away'] = pre_game_rank_historic_competition_min
                            all_events.loc[event, 'pre_game_rank_historic_competition_median_away'] = pre_game_rank_historic_competition_median


                else:
                    away_pre_elo = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name, away_post_delta_name, home_team_fixture_column, away_team_fixture_column)

                all_events.loc[event, away_pre_delta_name] = away_pre_elo

            if pd.isna(away_pre_elo):
                print('The away delta is blank for event ', event, all_events.loc[event, 'name'])


            if delta_column_to_calcuate in ['half_time_win_margin', 'second_half_win_margin']:
                
                # half of the expected result then adjust for half
                pre_elo_diff = ((all_events.loc[event, 'pre_delta_diff'] - all_events.loc[event, 'pre_delta_adjustment'])/2) + home_pre_elo - away_pre_elo

            else:

                pre_elo_diff = (home_pre_elo + home_team_buffer) - away_pre_elo

            if delta_column_to_calcuate == 'half_time_win_margin':
                pre_delta_adjustment = 0
                post_delta_adjustment = min(max(-5,(pre_elo_diff-4)/4),5)

            elif delta_column_to_calcuate == 'second_half_win_margin':                
                pre_delta_adjustment = 0
                post_delta_adjustment = min(max(-5,(pre_elo_diff-4)/3),5)
                
                
            else:
                
                pre_delta_adjustment = min(max(-7,(pre_elo_diff-4)/3.5),7)
                post_delta_adjustment = min(max(-5,(pre_elo_diff-5)/4),2)

                
                # New pre_elo_diff adjustment
                #pre_delta_adjustment = min(max(-7,(pre_elo_diff-2.5)/5),2)-0.22
                #post_delta_adjustment = min(max(-5,(pre_elo_diff-5)/4),2)

#                 pre_delta_adjustment = 0
#                 post_delta_adjustment = 0

                
            pre_elo_diff = pre_elo_diff - pre_delta_adjustment
            all_events.loc[event, pre_delta_diff_name] = pre_elo_diff
            all_events.loc[event, post_delta_adjustment_name] = post_delta_adjustment

            pre_delta_diff_adjusted_name = str(pre_delta_diff_name) + '_adjusted'
            all_events.loc[event, pre_delta_diff_adjusted_name] = pre_elo_diff - post_delta_adjustment

            
            if delta_column_to_calcuate in ['half_time_win_margin', 'second_half_win_margin']:
                delta_type_weight = 0.5
            else:
                delta_type_weight = 1


            home_team_home_error = get_team_homeaway_error('home', home_team_internal_id, home_team_total_fixture_number)
            away_team_home_error = get_team_homeaway_error('away', away_team_internal_id, away_team_total_fixture_number)
                
            all_events.loc[event, home_error] = home_team_home_error
            all_events.loc[event, away_error] = away_team_home_error
            

            if pd.isna(actual_win_margin):

                all_events.loc[event, home_post_delta_name] = home_pre_elo
                all_events.loc[event, away_post_delta_name] = away_pre_elo

            else:

                if (actual_win_margin < (pre_elo_diff - win_margin_buffer)):


                    all_events.loc[event, home_post_delta_name] = home_pre_elo - ((min(max_points_win, np.log(0.000001+abs(pre_elo_diff - actual_win_margin))*transfer_multiplier) - win_bonus) * delta_type_weight)
                    all_events.loc[event, away_post_delta_name] = away_pre_elo + ((min(max_points_win, np.log(0.000001+abs(pre_elo_diff - actual_win_margin))*transfer_multiplier) + win_bonus) * delta_type_weight)


                elif (actual_win_margin > (pre_elo_diff + win_margin_buffer)):


                    all_events.loc[event, home_post_delta_name] = home_pre_elo + ((min(max_points_win, np.log(0.000001+abs(pre_elo_diff - actual_win_margin))*transfer_multiplier) + win_bonus) * delta_type_weight)
                    all_events.loc[event, away_post_delta_name] = away_pre_elo - ((min(max_points_win, np.log(0.000001+abs(pre_elo_diff - actual_win_margin))*transfer_multiplier) - win_bonus) * delta_type_weight)

                else:

                    all_events.loc[event, home_post_delta_name] = home_pre_elo
                    all_events.loc[event, away_post_delta_name] = away_pre_elo

    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')




    return all_events



def generate_predicted_elo(all_events, team_internal_id, team_total_fixture_number, competition_id, competition_fixture_number, home_competition_group, start_time, level_setting, event_id, list_order_int_men, list_order_int_women_age, list_order_club, home_post_delta_name, away_post_delta_name, home_pre_delta_name, away_pre_delta_name, all_competitions, all_teams, internationl_rankings_df):
    
    
    comp_base_rate_fix_range = 50
    min_comp_base_rate_fix_range = 1
    pre_game_rank = None
    rank_set_type = None
    opp_pre_elo = None
    
    pre_game_rank_historic_home_competition_group_min = None
    pre_game_rank_historic_home_competition_group_median = None
    pre_game_rank_senior_team_ranking = None
    pre_game_rank_int_comp_level_setting = None
    pre_game_rank_new_home_competition_group = None
    pre_game_rank_historic_competition_median = None
    pre_game_rank_historic_competition_min = None

    dict_of_values = dict()
    
    if team_internal_id in other_initial_rankings.keys():
        pre_game_rank = other_initial_rankings[team_internal_id]
        rank_set_type = 'manual_entry'
        
    else:

        ###################### Get competition median

        games_to_aggregate = all_events[ (all_events['competition_internal_id'] == competition_id) & (all_events['competition_fixture_number'] < competition_fixture_number) & (all_events['competition_fixture_number'] >= (competition_fixture_number - 500)) ]
        if len(games_to_aggregate) > 1:

                home_games = games_to_aggregate.rename(columns = {home_pre_delta_name:'pre_game_rank'})
                away_games = games_to_aggregate.rename(columns = {away_pre_delta_name:'pre_game_rank'})
                all_games = pd.concat([home_games['pre_game_rank'], away_games['pre_game_rank']])
                pre_game_rank_historic_competition_min = all_games.min()
                pre_game_rank_historic_competition_median = all_games.median() 

        ############################################################################################################        
        # Get home competition group min value

        # Find home competition group if this game is currently not from a home competition
        if (pd.isna(home_competition_group) | (home_competition_group == 'na')):

            home_competition_group = all_events[ ((all_events['home_team_internal_id'] == team_internal_id) | (all_events['away_team_internal_id'] == team_internal_id)) & (all_events['start_time'] < start_time) & pd.notna(all_events['home_competition_group'])  & (all_events['home_competition_group'] != 'na') ]
            home_competition_group = home_competition_group.drop_duplicates('home_competition_group', keep = 'last')
            if len(home_competition_group)>0:
                home_competition_group = home_competition_group['home_competition_group'].iloc[0]
            else:
                home_competition_group = None




        if pd.notna(home_competition_group) & (home_competition_group != 'na'):

            # Get last game of their home competition group (it may not be this current game)
            home_competition_group_fixture_number = all_events[ (all_events['home_competition_group'] == home_competition_group) & (all_events['start_time'] < start_time)]['competition_fixture_number'].max()

            games_to_aggregate = all_events[ (all_events['home_competition_group'] == home_competition_group) & (all_events['home_competition_fixture_number'] < home_competition_group_fixture_number) & (all_events['home_competition_fixture_number'] >= (home_competition_group_fixture_number - comp_base_rate_fix_range)) ]

            ### If they are joining a new group
            if len(games_to_aggregate) > min_comp_base_rate_fix_range:

                home_games = games_to_aggregate.rename(columns = {home_pre_delta_name:'pre_game_rank'})
                away_games = games_to_aggregate.rename(columns = {away_pre_delta_name:'pre_game_rank'})
                all_games = pd.concat([home_games['pre_game_rank'], away_games['pre_game_rank']])
                pre_game_rank_historic_home_competition_group_min = all_games.min()
                pre_game_rank_historic_home_competition_group_median = all_games.median()


            ### If this is a new competition altogether then use the home_competition base set by international team rank and comp level
            comp_level = all_competitions[ all_competitions['home_competition_group'] == home_competition_group].iloc[0]['level']
            nat_team_id = all_teams[ all_teams['id'] == team_internal_id].iloc[0]['national_team_id']
            if pd.notna(nat_team_id):
                int_team_events = all_events[ ((all_events['home_team_internal_id'] == nat_team_id) | (all_events['away_team_internal_id'] == nat_team_id))  & (all_events['start_time'] < start_time)  & pd.notna(all_events[home_post_delta_name])   & pd.notna(all_events[away_post_delta_name]) ]

                if len(int_team_events)>0:

                    int_team_events = int_team_events.iloc[-1]

                    if int_team_events['home_team_internal_id'] == nat_team_id:
                        if comp_level == 1:
                            pre_game_rank_new_home_competition_group = int_team_events[home_post_delta_name]
                        else:
                            pre_game_rank_new_home_competition_group = int_team_events[home_post_delta_name] - ( (comp_level - 1) * level_setting )

                    elif int_team_events['away_team_internal_id'] == nat_team_id:
                        if comp_level == 1:
                            pre_game_rank_new_home_competition_group = int_team_events[away_post_delta_name]
                        else:
                            pre_game_rank_new_home_competition_group = int_team_events[away_post_delta_name] - ( (comp_level - 1) * level_setting )
                else:
                    # No previous games for their international team so just look up the dictionary to try and get it?
                    int_rank = internationl_rankings_df[ internationl_rankings_df['team_id'] == nat_team_id]

                    if len(int_rank)>0:
                        pre_game_rank_new_home_competition_group = int_rank.iloc[0]['pre_game_rank']

                        if comp_level > 1:
                            pre_game_rank_new_home_competition_group = pre_game_rank_new_home_competition_group - ( (comp_level - 1) * level_setting )


        # If there is no home_competition_group then just use the current competition they are playing in
        comp_level = all_competitions[ all_competitions['id'] == competition_id].iloc[0]['level']
        nat_team_id = all_teams[ all_teams['id'] == team_internal_id].iloc[0]['national_team_id']
        if pd.notna(nat_team_id):
            int_team_events = all_events[ ((all_events['home_team_internal_id'] == nat_team_id) | (all_events['away_team_internal_id'] == nat_team_id))  & (all_events['start_time'] < start_time)  & pd.notna(all_events[home_post_delta_name])   & pd.notna(all_events[away_post_delta_name]) ]

            if len(int_team_events)>0:

                int_team_events = int_team_events.iloc[-1]

                if int_team_events['home_team_internal_id'] == nat_team_id:
                    if comp_level == 1:
                        pre_game_rank_int_comp_level_setting = int_team_events[home_post_delta_name]
                    else:
                        pre_game_rank_int_comp_level_setting = int_team_events[home_post_delta_name] - ( (comp_level - 1) * level_setting )

                elif int_team_events['away_team_internal_id'] == nat_team_id:
                    if comp_level == 1:
                        pre_game_rank_int_comp_level_setting = int_team_events[away_post_delta_name]
                    else:
                        pre_game_rank_int_comp_level_setting = int_team_events[away_post_delta_name] - ( (comp_level - 1) * level_setting )

            else:
                # No previous games for their international team so just look up the dictionary to try and get it?
                int_rank = internationl_rankings_df[ internationl_rankings_df['team_id'] == nat_team_id]

                if len(int_rank)>0:
                    pre_game_rank_new_home_competition_group = int_rank.iloc[0]['pre_game_rank']

                    if comp_level > 1:
                        pre_game_rank_new_home_competition_group = pre_game_rank_new_home_competition_group - ( (comp_level - 1) * level_setting )



        ############################################################################################################        
        ############################################################################################################        




        ############################################################################################################        
        # Check to see if they are an A team
        # Using A teams etc add in from all teams
        team_details = all_teams[ all_teams['id'] == team_internal_id]
        senior_team = team_details['senior_team'].iloc[0]
        team_level = team_details['level'].iloc[0]
        team_type = team_details['type'].iloc[0]

        if not pd.isna(senior_team):

            senior_team_games = all_events[ ((all_events['home_team_internal_id'] == senior_team) | (all_events['away_team_internal_id'] == senior_team)) & (all_events['start_time'] < start_time)]
            if len(senior_team_games)>0:
                senior_team_games = senior_team_games.iloc[-1]

                if senior_team_games['home_team_internal_id'] == senior_team:
                    pre_game_rank_senior_team_ranking = senior_team_games[home_post_delta_name]
                if senior_team_games['away_team_internal_id'] == senior_team:
                    pre_game_rank_senior_team_ranking = senior_team_games[away_post_delta_name]

                if team_type != 'international_womens':
                        pre_game_rank_senior_team_ranking = pre_game_rank_senior_team_ranking - ((team_level - 1) * level_setting)
        ############################################################################################################        
        ############################################################################################################        



        ############################################################################################################
        ### Get their oppositions rank if they have one?
        if team_internal_id == all_events.loc[event_id]['home_team_internal_id']:
            opposition_id = all_events.loc[event_id]['away_team_internal_id']
            opposition_total_fixture_number = all_events.loc[event_id]['away_team_total_fixture_number']
        else:
            opposition_id = all_events.loc[event_id]['home_team_internal_id']
            opposition_total_fixture_number = all_events.loc[event_id]['home_team_total_fixture_number']

        home_events = all_events[ (all_events['home_team_internal_id'] == opposition_id) & (all_events['home_team_total_fixture_number'] == (opposition_total_fixture_number - 1)) ]
        if len(home_events)>0:
            opp_pre_elo = home_events.iloc[0][home_post_delta_name]

        else:
            away_events = all_events[ (all_events['away_team_internal_id'] == opposition_id) & (all_events['away_team_total_fixture_number'] == (opposition_total_fixture_number - 1)) ]
            if len(away_events) > 0:
                opp_pre_elo = away_events.iloc[0][away_post_delta_name]


        # Get median of all events
        home_events = all_events[ pd.notna(all_events[home_post_delta_name]) & (all_events['start_time'] < start_time) ][home_post_delta_name]
        away_events = all_events[ pd.notna(all_events[away_post_delta_name]) & (all_events['start_time'] < start_time) ][away_post_delta_name]
        all_events_median = pd.concat([home_events, away_events]).median()

        ############################################################################################################
        ############################################################################################################



        dict_of_values['pre_game_rank_historic_home_competition_group_min'] = pre_game_rank_historic_home_competition_group_min
        dict_of_values['pre_game_rank_historic_home_competition_group_median'] = pre_game_rank_historic_home_competition_group_median
        dict_of_values['pre_game_rank_new_home_competition_group'] = pre_game_rank_new_home_competition_group
        dict_of_values['pre_game_rank_senior_team_ranking'] = pre_game_rank_senior_team_ranking
        dict_of_values['pre_game_rank_int_comp_level_setting'] = pre_game_rank_int_comp_level_setting
        dict_of_values['opp_pre_elo'] = opp_pre_elo
        dict_of_values['all_events_median'] = all_events_median
        dict_of_values['pre_game_rank_historic_competition_min'] = pre_game_rank_historic_competition_min
        dict_of_values['pre_game_rank_historic_competition_median'] = pre_game_rank_historic_competition_median



        if (home_competition_group == 'international_mens'):
            list_order = list_order_int_men
        elif (home_competition_group == 'international_womens') | (home_competition_group == 'international_u21s') | (home_competition_group == 'international_u20s') | (home_competition_group == 'international_u19s'):
            list_order = list_order_int_women_age
        else:
            list_order = list_order_club

        pre_game_rank = None
        loopnum = 0
        while pd.isna(pre_game_rank):
            rank_set_type = list_order[loopnum]
            pre_game_rank = dict_of_values[list_order[loopnum]]
            loopnum = loopnum + 1

    return pre_game_rank, rank_set_type, pre_game_rank_historic_home_competition_group_min, pre_game_rank_historic_home_competition_group_median, pre_game_rank_senior_team_ranking, pre_game_rank_int_comp_level_setting, pre_game_rank_new_home_competition_group, pre_game_rank_historic_competition_min, pre_game_rank_historic_competition_median





def get_last_pre_elo(delta_type, all_events, team_internal_id, team_total_fixture_number, home_post_delta_name, away_post_delta_name, home_team_fixture_column, away_team_fixture_column):
    
    if delta_type == 'home':
        home_events = all_events[ (all_events['home_team_internal_id'] == team_internal_id) & (all_events[home_team_fixture_column] == (team_total_fixture_number-1)) ]
        post_game_rank = home_events.iloc[0][home_post_delta_name]
    if delta_type == 'away':
        away_events = all_events[ (all_events['away_team_internal_id'] == team_internal_id) & (all_events[away_team_fixture_column] == (team_total_fixture_number-1)) ]
        post_game_rank = away_events.iloc[0][home_post_delta_name]

        
    else:
        
        home_events = all_events[ (all_events['home_team_internal_id'] == team_internal_id) & (all_events[home_team_fixture_column] == (team_total_fixture_number-1)) ]

        if len(home_events)>0:
            post_game_rank = home_events.iloc[0][home_post_delta_name]

        else:

            away_events = all_events[ (all_events['away_team_internal_id'] == team_internal_id) & (all_events[away_team_fixture_column] == (team_total_fixture_number-1)) ]

            if len(away_events) > 0:
                post_game_rank = away_events.iloc[0][away_post_delta_name]


    return post_game_rank




def get_home_team_buffer(all_events, start_time, competition_id, competition_fixture_number, home_competition_group, home_competition_fixture_number, delta_column_to_calcuate):
    
    home_team_buffer = None
    all_events = all_events[ pd.notna(all_events[delta_column_to_calcuate])]
    
    if (home_competition_group != 'international_mens') & (home_competition_group != 'international_womens') & (home_competition_group != 'international_u21s') & (home_competition_group != 'international_u20s')  & (home_competition_group != 'international_u19s') & (home_competition_group != 'international_u18s'):

        temp_df = all_events[ (all_events['competition_internal_id'] == competition_id) & (all_events['competition_fixture_number'] < competition_fixture_number) & (all_events['competition_fixture_number'] >= (competition_fixture_number - 500)) ]
        
        # If there are more than 100 games in a competition then use this as the buffer
        if len(temp_df)>100:

            home_team_buffer = temp_df[delta_column_to_calcuate].median()

    else:

        if (home_competition_group != 'na') & (home_competition_group != '') & pd.notna(home_competition_group) & (home_competition_group != 'nan'):
        
            temp_df = all_events[ (all_events['home_competition_group'] == home_competition_group) & (all_events['home_competition_fixture_number'] < home_competition_fixture_number) & (all_events['home_competition_fixture_number'] >= (home_competition_fixture_number - 500)) ]


            if len(temp_df)>100:

                home_team_buffer = temp_df[delta_column_to_calcuate].median()


    if pd.isna(home_team_buffer):

            # If it is before 2010 then we can use our predefined buffers
            start_time_temp = pd.to_datetime(start_time).replace(tzinfo=None)
            if start_time_temp < pd.to_datetime('2010-01-01'):

                if home_competition_group == 'international_mens':
                    home_team_buffer = international_mens_base_home_win_margin
                elif home_competition_group == 'international_womens':
                    home_team_buffer = international_womens_base_home_win_margin
                else:
                    temp_df = competition_win_margin_means[ competition_win_margin_means['competition_internal_id'] == competition_id]

                    if len(temp_df)>0:
                        home_team_buffer = temp_df.iloc[0][delta_column_to_calcuate]
                    else:
                        home_team_buffer = all_base_home_win_margin


            else:

                # If there aren't more than 100 games in the home competition group then just use the global win_margin
                temp_df = all_events[ (all_events['start_time'] < start_time) & (all_events['start_time'] >= (start_time - datetime.timedelta(days = (365 * 5))) )]
                home_team_buffer = temp_df[delta_column_to_calcuate].median()

                
    return home_team_buffer





def team_dict_to_dataframe(team_rankings):

    function_start_time = datetime.datetime.now()
    print('-team_dict_to_dataframe')
    
    team_df = pd.DataFrame()
    team_df = pd.DataFrame(team_rankings.values(), index = team_rankings.keys()).reset_index().rename(columns = {'index':'team_id', 0:'pre_game_rank'})
        
        
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return team_df


def check_for_nonexistant_events(all_previous_deltas, all_events):

    function_start_time = datetime.datetime.now()
    print('-check_for_nonexistant_events')
    
    # Check to see if there are any events in the deltas table that are no longer events for whatever reason

    events_that_no_longer_exists = list(all_previous_deltas[ ~all_previous_deltas['event_id'].isin(all_events['event_id']) ]['event_id'])

    if len(events_that_no_longer_exists)>0:

        event_list_string = ''

        for event in events_that_no_longer_exists:

            event_list_string = event_list_string + "'" + str(event) + "',"


        sql_statement = 'delete from event_deltas where event_id in (' + event_list_string[:-1] + ');'
        postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, False)

        print('--', str(len(events_that_no_longer_exists)), ' - events removed')
    
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    return



def update_existing_events(all_events, home_pre_delta_name, home_post_delta_name, away_pre_delta_name, away_post_delta_name, pre_delta_diff_name, home_team_buffer_name, post_delta_adjustment_name):


    function_start_time = datetime.datetime.now()
    print('-update_existing_events')
    
    sql_statement_full = None
    existing_events = all_events[start_range:]
    existing_events = existing_events[ existing_events['event_id'].isin(all_previous_deltas['event_id'])]

    if len(existing_events)>0:

        sql_statement_full = ''
        loop_num = 1

        for event in existing_events.index:

            #print(loop_num, len(existing_events))

            sql_statement = 'update event_deltas set '

            event_id = existing_events.loc[event, 'event_id']
            home_team_internal_id = existing_events.loc[event, 'home_team_internal_id']
            away_team_internal_id = existing_events.loc[event, 'away_team_internal_id']
            competition_internal_id = existing_events.loc[event, 'competition_internal_id']
            venue_internal_id = existing_events.loc[event, 'venue_internal_id']
            
            #venue_internal_id = existing_events.loc[event, 'venue_internal_id']
            #if pd.isna(venue_internal_id) | (venue_internal_id == 'None') | (venue_internal_id == 'NaN'):
            #    venue_internal_id = "null"

            start_time = existing_events.loc[event, 'start_time']
            home_score = existing_events.loc[event, 'home_score']
            away_score = existing_events.loc[event, 'away_score']
            
            home_team_buffer = existing_events.loc[event, home_team_buffer_name]
            home_pre_delta = existing_events.loc[event, home_pre_delta_name]
            away_pre_delta = existing_events.loc[event, away_pre_delta_name]
            pre_delta_diff = existing_events.loc[event, pre_delta_diff_name]
            home_post_delta = existing_events.loc[event, home_post_delta_name]
            away_post_delta = existing_events.loc[event, away_post_delta_name]
            
            pre_delta_adjustment = existing_events.loc[event, post_delta_adjustment_name]

            if pd.isna(home_pre_delta):
                home_pre_delta = 'NULL'
            if pd.isna(away_pre_delta):
                away_pre_delta = 'NULL'
            if pd.isna(home_post_delta):
                home_post_delta = 'NULL'
            if pd.isna(away_post_delta):
                away_post_delta = 'NULL'
            if pd.isna(home_team_buffer):
                home_team_buffer = 'NULL'
            if pd.isna(home_score):
                home_score = 'NULL'
            if pd.isna(away_score):
                away_score = 'NULL'
            if pd.isna(pre_delta_adjustment):
                pre_delta_adjustment = 'NULL'    

            sql_statement = sql_statement + str(home_team_buffer_name) + " = "  + str(home_team_buffer) + ","
            sql_statement = sql_statement + str(home_pre_delta_name) + " = "  + str(home_pre_delta) + ","
            sql_statement = sql_statement + str(away_pre_delta_name) + " = "  + str(away_pre_delta) + ","
            sql_statement = sql_statement + str(pre_delta_diff_name) + " = "  + str(pre_delta_diff) + ","
            sql_statement = sql_statement + str(home_post_delta_name) + " = "  + str(home_post_delta) + ","
            sql_statement = sql_statement + str(away_post_delta_name) + " = "  + str(away_post_delta) + ","
            sql_statement = sql_statement + str(post_delta_adjustment_name) + " = "  + str(pre_delta_adjustment) + ","

            sql_statement = sql_statement + str('home_team_internal_id') + " = '"  + str(home_team_internal_id) + "',"
            sql_statement = sql_statement + str('away_team_internal_id') + " = '"  + str(away_team_internal_id) + "',"
            if pd.notna(venue_internal_id) & (venue_internal_id != 'None') & (venue_internal_id != 'Null'):
                sql_statement = sql_statement + str('venue_internal_id') + " = '"  + str(venue_internal_id) + "',"
            sql_statement = sql_statement + str('competition_internal_id') + " = '"  + str(competition_internal_id) + "',"
            sql_statement = sql_statement + str('start_time') + " = '"  + str(start_time) + "',"
            sql_statement = sql_statement + str('home_score') + " = "  + str(home_score) + ","
            sql_statement = sql_statement + str('away_score') + " = "  + str(away_score)

            sql_statement = sql_statement + " where event_id = '" + str(event_id) + "';"


            sql_statement_full = sql_statement_full + sql_statement
            loop_num += 1
            
            
    if pd.notna(sql_statement_full):
        postgres_Retreive_Insert(sql_statement_full, POSTGRESQL_PARAMS, False)
        
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')




def add_new_events(all_events, pre_delta_diff_name, all_previous_deltas):


    function_start_time = datetime.datetime.now()
    print('-add_new_events')
    
    new_events = all_events[ ~all_events['event_id'].isin(all_previous_deltas['event_id'])]
    #new_events = new_events[ (pd.notna(new_events['venue_internal_id'])) & (new_events['venue_internal_id'] != 'None')]

    sql_statement_full_venues = ''
    sql_statement_full_no_venues = ''
    if len(new_events)>0:

        sql_statement_start_venues = 'insert into event_deltas (event_id, home_team_internal_id, away_team_internal_id, competition_internal_id, venue_internal_id, start_time, home_score, away_score, home_team_buffer, pre_delta_adjustment, home_pre_delta, away_pre_delta, pre_delta_diff, home_post_delta, away_post_delta) values '
        sql_statement_start_no_venues = 'insert into event_deltas (event_id, home_team_internal_id, away_team_internal_id, competition_internal_id, start_time, home_score, away_score, home_team_buffer, pre_delta_adjustment, home_pre_delta, away_pre_delta, pre_delta_diff, home_post_delta, away_post_delta) values '

        for event in new_events.index:

            sql_statement = ''

            event_id = new_events.loc[event, 'event_id']
            home_team_internal_id = new_events.loc[event, 'home_team_internal_id']
            away_team_internal_id = new_events.loc[event, 'away_team_internal_id']
            competition_internal_id = new_events.loc[event, 'competition_internal_id']
            
            venue_internal_id = new_events.loc[event, 'venue_internal_id']
            venue_exists = False
            if pd.notna(venue_internal_id) & (venue_internal_id != 'None') & (venue_internal_id != 'NaN'):
                venue_exists = True

            start_time = new_events.loc[event, 'start_time']
            home_score = new_events.loc[event, 'home_score']
            away_score = new_events.loc[event, 'away_score']
            home_pre_delta = new_events.loc[event, 'home_pre_delta']
            away_pre_delta = new_events.loc[event, 'away_pre_delta']
            pre_delta_diff = new_events.loc[event, pre_delta_diff_name]
            home_post_delta = new_events.loc[event, 'home_post_delta']
            away_post_delta = new_events.loc[event, 'away_post_delta']
            home_team_buffer = new_events.loc[event, 'home_team_buffer']
            pre_delta_adjustment = new_events.loc[event, 'pre_delta_adjustment']
            #updated_at = datetime.datetime.now()
            #created_at = datetime.datetime.now()


            if pd.isna(home_score):
                home_score = 'NULL'
            if pd.isna(away_score):
                away_score = 'NULL'
            if pd.isna(home_pre_delta):
                home_pre_delta = 'NULL'
            if pd.isna(away_pre_delta):
                away_pre_delta = 'NULL'
            if pd.isna(home_post_delta):
                home_post_delta = 'NULL'
            if pd.isna(away_post_delta):
                away_post_delta = 'NULL'
            if pd.isna(pre_delta_adjustment):
                pre_delta_adjustment = 'NULL'


            sql_statement = sql_statement + "('"  + str(event_id) + "', "
            sql_statement = sql_statement + "'"  + str(home_team_internal_id) + "', "
            sql_statement = sql_statement + "'"  + str(away_team_internal_id) + "', "
            sql_statement = sql_statement + "'"  + str(competition_internal_id) + "', "
            
            if venue_exists:
                sql_statement = sql_statement + "'"  + str(venue_internal_id) + "', "
                
            sql_statement = sql_statement + "'"  + str(start_time) + "', "
            sql_statement = sql_statement + str(home_score) + ", "
            sql_statement = sql_statement + str(away_score) + ", "
            sql_statement = sql_statement + str(home_team_buffer) + ", "
            sql_statement = sql_statement + str(pre_delta_adjustment) + ", "
            sql_statement = sql_statement + str(home_pre_delta) + ", "
            sql_statement = sql_statement + str(away_pre_delta) + ", "
            sql_statement = sql_statement + str(pre_delta_diff) + ", "
            sql_statement = sql_statement + str(home_post_delta) + ", "
            sql_statement = sql_statement + str(away_post_delta) + "), "
            #sql_statement = sql_statement + "'"  + str(updated_at) + "', "
            #sql_statement = sql_statement + "'"  + str(created_at) + "'), "

            if venue_exists:
                sql_statement_full_venues = sql_statement_full_venues + sql_statement
            else:
                sql_statement_full_no_venues = sql_statement_full_no_venues + sql_statement

    if len(sql_statement_full_venues) > 0:
        sql_statement_full_venues = sql_statement_full_venues[:-2]
        sql_statement_full_venues = sql_statement_start_venues + sql_statement_full_venues + ";"
        postgres_Retreive_Insert(sql_statement_full_venues, POSTGRESQL_PARAMS, False)

    if len(sql_statement_full_no_venues) > 0:
        sql_statement_full_no_venues = sql_statement_full_no_venues[:-2]
        sql_statement_full_no_venues = sql_statement_start_no_venues + sql_statement_full_no_venues + ";"
        postgres_Retreive_Insert(sql_statement_full_no_venues, POSTGRESQL_PARAMS, False)

    
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')


def add_venue_info(all_events):
    
    sql_statement = 'Select * from venue'
    venues, error = postgres_Retreive_Insert(sql_statement, POSTGRESQL_PARAMS, True)
    all_events = all_events.merge(venues[['id', 'national_team']].rename(columns = {'id':'venue_internal_id','national_team':'venue_national_team'}), how = 'left', left_on = 'venue_internal_id', right_on = 'venue_internal_id')

    return all_events, venues



def get_all_deltas():

    float_columns = ['home_pre_delta', 'home_post_delta', 'away_pre_delta', 'away_post_delta', 'pre_delta_diff', 'home_team_buffer', 'home_pre_delta_halftime', 'home_post_delta_halftime', 'away_pre_delta_halftime', 'away_post_delta_halftime', 'pre_delta_diff_halftime', 'home_team_buffer_halftime', 'home_pre_delta_secondhalf', 'home_post_delta_secondhalf', 'away_pre_delta_secondhalf', 'away_post_delta_secondhalf', 'pre_delta_diff_secondhalf', 'home_team_buffer_secondhalf']
    all_previous_deltas = get_all_previous_deltas(float_columns)

    home_games = all_previous_deltas[['start_time','home_team_internal_id', 'home_pre_delta']].rename(columns = {'home_team_internal_id':'team_id', 'home_pre_delta':'delta'})
    away_games = all_previous_deltas[['start_time','away_team_internal_id', 'away_pre_delta']].rename(columns = {'away_team_internal_id':'team_id', 'away_pre_delta':'delta'})
    all_deltas = pd.concat([home_games,away_games])

    return all_deltas



def get_initial_delta(all_deltas, team_id, start_time, attack_defence):

    all_deltas = all_deltas[ (all_deltas['team_id'] == team_id) & (all_deltas['start_time'] <= start_time)]
    all_deltas.sort_values('start_time', ascending = False, inplace = True)

    if attack_defence == 'attack':

        delta = 1.0543565 * all_deltas.iloc[0]['delta']

    elif attack_defence == 'defence':

        delta = -0.92410307 * all_deltas.iloc[0]['delta']


    return delta



def update_event_deltas_total_points(all_events, start_range, end_range, home_pre_delta_name_1, home_pre_delta_name_2, home_pre_delta_name_3, home_pre_delta_name_4, away_pre_delta_name_1, away_pre_delta_name_2, away_pre_delta_name_3, away_pre_delta_name_4, home_post_delta_name_1, home_post_delta_name_2, home_post_delta_name_3, home_post_delta_name_4, away_post_delta_name_1, away_post_delta_name_2, away_post_delta_name_3, away_post_delta_name_4):


    function_start_time = datetime.datetime.now()
    print('-update_existing_events')
    
    sql_statement_full = None
    existing_events = all_events[start_range:]

    if len(existing_events)>0:

        sql_statement_full = ''
        loop_num = 1

        for event in existing_events.index:

            #print(loop_num, len(existing_events))

            sql_statement = 'update event_deltas set '

            event_id = existing_events.loc[event, 'event_id']
            
    
            home_pre_delta_1 = existing_events.loc[event, home_pre_delta_name_1]
            home_pre_delta_2 = existing_events.loc[event, home_pre_delta_name_2]
            home_pre_delta_3 = existing_events.loc[event, home_pre_delta_name_3]
            home_pre_delta_4 = existing_events.loc[event, home_pre_delta_name_4]

            home_post_delta_1 = existing_events.loc[event, home_post_delta_name_1]
            home_post_delta_2 = existing_events.loc[event, home_post_delta_name_2]
            home_post_delta_3 = existing_events.loc[event, home_post_delta_name_3]
            home_post_delta_4 = existing_events.loc[event, home_post_delta_name_4]

            away_pre_delta_1 = existing_events.loc[event, away_pre_delta_name_1]
            away_pre_delta_2 = existing_events.loc[event, away_pre_delta_name_2]
            away_pre_delta_3 = existing_events.loc[event, away_pre_delta_name_3]
            away_pre_delta_4 = existing_events.loc[event, away_pre_delta_name_4]

            away_post_delta_1 = existing_events.loc[event, away_post_delta_name_1]
            away_post_delta_2 = existing_events.loc[event, away_post_delta_name_2]
            away_post_delta_3 = existing_events.loc[event, away_post_delta_name_3]
            away_post_delta_4 = existing_events.loc[event, away_post_delta_name_4]


            if pd.isna(home_pre_delta_1):
                home_pre_delta_1 = 'NULL'
            if pd.isna(home_pre_delta_2):
                home_pre_delta_2 = 'NULL'
            if pd.isna(home_pre_delta_3):
                home_pre_delta_3 = 'NULL'
            if pd.isna(home_pre_delta_4):
                home_pre_delta_4 = 'NULL'

            if pd.isna(home_post_delta_1):
                home_post_delta_1 = 'NULL'
            if pd.isna(home_post_delta_2):
                home_post_delta_2 = 'NULL'
            if pd.isna(home_post_delta_3):
                home_post_delta_3 = 'NULL'
            if pd.isna(home_post_delta_4):
                home_post_delta_4 = 'NULL'

            if pd.isna(away_pre_delta_1):
                away_pre_delta_1 = 'NULL'
            if pd.isna(away_pre_delta_2):
                away_pre_delta_2 = 'NULL'
            if pd.isna(away_pre_delta_3):
                away_pre_delta_3 = 'NULL'
            if pd.isna(away_pre_delta_4):
                away_pre_delta_4 = 'NULL'

            if pd.isna(away_post_delta_1):
                away_post_delta_1 = 'NULL'
            if pd.isna(away_post_delta_2):
                away_post_delta_2 = 'NULL'
            if pd.isna(away_post_delta_3):
                away_post_delta_3 = 'NULL'
            if pd.isna(away_post_delta_4):
                away_post_delta_4 = 'NULL'


            sql_statement = sql_statement + str(home_pre_delta_name_1) + " = "  + str(home_pre_delta_1) + ","
            sql_statement = sql_statement + str(home_pre_delta_name_2) + " = "  + str(home_pre_delta_2) + ","
            sql_statement = sql_statement + str(home_pre_delta_name_3) + " = "  + str(home_pre_delta_3) + ","
            sql_statement = sql_statement + str(home_pre_delta_name_4) + " = "  + str(home_pre_delta_4) + ","
            sql_statement = sql_statement + str(home_post_delta_name_1) + " = "  + str(home_post_delta_1) + ","
            sql_statement = sql_statement + str(home_post_delta_name_2) + " = "  + str(home_post_delta_2) + ","
            sql_statement = sql_statement + str(home_post_delta_name_3) + " = "  + str(home_post_delta_3) + ","
            sql_statement = sql_statement + str(home_post_delta_name_4) + " = "  + str(home_post_delta_4) + ","
            sql_statement = sql_statement + str(away_pre_delta_name_1) + " = "  + str(away_pre_delta_1) + ","
            sql_statement = sql_statement + str(away_pre_delta_name_2) + " = "  + str(away_pre_delta_2) + ","
            sql_statement = sql_statement + str(away_pre_delta_name_3) + " = "  + str(away_pre_delta_3) + ","
            sql_statement = sql_statement + str(away_pre_delta_name_4) + " = "  + str(away_pre_delta_4) + ","
            sql_statement = sql_statement + str(away_post_delta_name_1) + " = "  + str(away_post_delta_1) + ","
            sql_statement = sql_statement + str(away_post_delta_name_2) + " = "  + str(away_post_delta_2) + ","
            sql_statement = sql_statement + str(away_post_delta_name_3) + " = "  + str(away_post_delta_3) + ","
            sql_statement = sql_statement + str(away_post_delta_name_4) + " = "  + str(away_post_delta_4)

            sql_statement = sql_statement + " where event_id = '" + str(event_id) + "';"


            sql_statement_full = sql_statement_full + sql_statement
            loop_num += 1
            
            
    if pd.notna(sql_statement_full):
        postgres_Retreive_Insert(sql_statement_full, POSTGRESQL_PARAMS, False)
        
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')

    



def get_team_homeaway_error(home_away, team_internal_id, team_total_fixture_number):
    
    if home_away == 'home':
        events = all_events[ (all_events['home_team_internal_id'] == team_internal_id) & (all_events['home_team_total_fixture_number'] < team_total_fixture_number) & (all_events['home_team_total_fixture_number'] >= (team_total_fixture_number - 10)) ].copy()
    elif home_away == 'away':
        events = all_events[ (all_events['away_team_internal_id'] == team_internal_id) & (all_events['away_team_total_fixture_number'] < team_total_fixture_number) & (all_events['away_team_total_fixture_number'] >= (team_total_fixture_number - 10)) ].copy()
    
    #events['error'] = events[pre_delta_diff_adjusted] - events[delta_column_to_calcuate]
    
    events.loc[:, 'error'] = events.loc[:, pre_delta_diff_adjusted] - events.loc[:, delta_column_to_calcuate]

    
    event_error = events['error'].median()
    
    return event_error
    



def calculate_total_points_deltas():


    all_deltas = get_all_deltas()    
    
    function_start_time = datetime.datetime.now()
    
    all_events.reset_index(inplace = True, drop = True)
    #all_events['post_game_rank_home'] = None
    #all_events['post_game_rank_away'] = None
    #all_events[home_team_buffer_name] = None
    all_events['pre_game_rank_historic_home_competition_group_min_home'] = None
    all_events['pre_game_rank_historic_home_competition_group_median_home'] = None
    all_events['pre_game_rank_senior_team_ranking_home'] = None
    all_events['pre_game_rank_int_comp_level_setting_home'] = None
    all_events['pre_game_rank_new_home_competition_group_home'] = None
    all_events['pre_game_rank_historic_home_competition_group_min_away'] = None
    all_events['pre_game_rank_historic_home_competition_group_median_away'] = None
    all_events['pre_game_rank_senior_team_ranking_away'] = None
    all_events['pre_game_rank_int_comp_level_setting_away'] = None
    all_events['pre_game_rank_new_home_competition_group_away'] = None
    all_events['base_value'] = None

    #all_events['pred_home_score_all'] = None
    #all_events['pred_home_score_ha'] = None
    #all_events['pred_away_score_all'] = None
    #all_events['pred_away_score_ha'] = None
    
    #all_events['home_adjustment'] = None
    #all_events['away_adjustment'] = None
    pre_elo_diff = None

    
    print('--updating from', start_range, end_range)
    for event in range(start_range,end_range+1):
        
            print('--', event, end_range)

            home_team_internal_id = all_events.loc[event, 'home_team_internal_id']
            home_team_home_fixture_number = all_events.loc[event, 'home_team_home_fixture_number']
            home_team_total_fixture_number = all_events.loc[event, 'home_team_total_fixture_number']

            away_team_internal_id = all_events.loc[event, 'away_team_internal_id']
            away_team_away_fixture_number = all_events.loc[event, 'away_team_away_fixture_number']
            away_team_total_fixture_number = all_events.loc[event, 'away_team_total_fixture_number']

            actual_target_value_1 = all_events.loc[event, delta_column_to_calcuate_1]
            actual_target_value_2 = all_events.loc[event, delta_column_to_calcuate_2]

            home_venue = all_events.loc[event, 'home_venue']

            start_time = all_events.loc[event, 'start_time']
            competition_id = all_events.loc[event, 'competition_internal_id']
            competition_fixture_number = all_events.loc[event, 'competition_fixture_number']
            home_competition_group = all_events.loc[event, 'home_competition_group']
            home_competition_fixture_number = all_events.loc[event, 'home_competition_fixture_number']


            home_team_buffer = 0
            if get_home_advantage == True:
                
                # Get team travelled buffer
                travel_buffer = all_events.loc[event, 'travel_advantage']
                if travel_buffer == -1:
                    home_venue = True

                        
            
            if home_team_total_fixture_number == 1:
                
#                 if away_team_total_fixture_number > 1:
                    
#                     current_score = all_events.loc[event, 'home_score']
#                     away_defence = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
#                     home_attack = current_total_points - away_defence
                    
#                     away_attack = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
#                     home_defence = current_score - away_attack

                    
#                 else:

                    home_attack = get_initial_delta(all_deltas, home_team_internal_id, start_time, 'attack')
                    home_defence = get_initial_delta(all_deltas, home_team_internal_id, start_time, 'defence')
                
                # Need to fix this bit so it pulls back home_homes when needed etc
                #if pd.isna(home_attack):
                #    home_attack, rank_set_type, pre_game_rank_historic_home_competition_group_min, pre_game_rank_historic_home_competition_group_median, pre_game_rank_senior_team_ranking, pre_game_rank_int_comp_level_setting, pre_game_rank_new_home_competition_group, pre_game_rank_historic_competition_min, pre_game_rank_historic_competition_median = generate_predicted_elo(all_events, home_team_internal_id, home_team_total_fixture_number, competition_id, competition_fixture_number, home_competition_group, start_time, level_setting, event, list_order_int_men, list_order_int_women_age, list_order_club, home_post_delta_name_1, away_post_delta_name_2, home_pre_delta_name_1, away_pre_delta_name_2, all_competitions, all_teams, internationl_rankings_df)
                #if pd.isna(home_defence):
                #    home_defence, rank_set_type, pre_game_rank_historic_home_competition_group_min, pre_game_rank_historic_home_competition_group_median, pre_game_rank_senior_team_ranking, pre_game_rank_int_comp_level_setting, pre_game_rank_new_home_competition_group, pre_game_rank_historic_competition_min, pre_game_rank_historic_competition_median = generate_predicted_elo(all_events, home_team_internal_id, home_team_total_fixture_number, competition_id, competition_fixture_number, home_competition_group, start_time, level_setting, event, list_order_int_men, list_order_int_women_age, list_order_club, home_post_delta_name_2, away_post_delta_name_1, home_pre_delta_name_2, away_pre_delta_name_1, all_competitions, all_teams, internationl_rankings_df)

            else:
                
                if home_venue & (travel_buffer != -1):
                    home_attack = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
                    home_defence = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
                else:
                    home_attack = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
                    home_defence = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')

            
            if home_team_home_fixture_number == 1:
                
#                 if away_team_total_fixture_number > 1:
                    
#                     current_score = all_events.loc[event, 'home_score']
#                     away_defence = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
#                     home_home_attack = current_total_points - away_defence
                    
#                     away_attack = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
#                     home_home_defence = current_score - away_attack

                    
#                 else:

                    home_home_attack = get_initial_delta(all_deltas, home_team_internal_id, start_time, 'attack')
                    home_home_defence = get_initial_delta(all_deltas, home_team_internal_id, start_time, 'defence')
                
            else:
                
                if home_venue & (travel_buffer != -1):
                    home_home_attack = get_last_pre_elo('home', all_events, home_team_internal_id, home_team_home_fixture_number, home_post_delta_name_3, away_post_delta_name_3, 'home_team_home_fixture_number', 'away_team_away_fixture_number')
                    home_home_defence = get_last_pre_elo('home', all_events, home_team_internal_id, home_team_home_fixture_number, home_post_delta_name_4, away_post_delta_name_4, 'home_team_home_fixture_number', 'away_team_away_fixture_number')
                else:
                    home_home_attack = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
                    home_home_defence = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')


            
            all_events.loc[event, home_pre_delta_name_1] = home_attack
            all_events.loc[event, home_pre_delta_name_2] = home_defence
            all_events.loc[event, home_pre_delta_name_3] = home_home_attack
            all_events.loc[event, home_pre_delta_name_4] = home_home_defence

            
            if away_team_total_fixture_number == 1:
                
#                 if home_team_total_fixture_number > 1:
                    
#                     current_score = all_events.loc[event, 'away_score']
#                     home_defence = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
#                     away_attack = current_score - home_defence
                    
#                     home_attack = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
#                     away_defence = current_total_points - home_attack
                    

#                 else:

                    away_attack = get_initial_delta(all_deltas, away_team_internal_id, start_time, 'attack')
                    away_defence = get_initial_delta(all_deltas, away_team_internal_id, start_time, 'defence')               
            
            else:
                
                if home_venue & (travel_buffer != -1):
                    away_attack = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
                    away_defence = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
                else:
                    away_attack = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
                    away_defence = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')

                    
            if away_team_away_fixture_number == 1:
                
#                 if home_team_total_fixture_number > 1:
                    
#                     current_score = all_events.loc[event, 'away_score']
#                     home_defence = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
#                     away_away_attack = current_score - home_defence
                    
#                     home_attack = get_last_pre_elo(None, all_events, home_team_internal_id, home_team_total_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
#                     away_away_defence = current_total_points - home_attack

#                 else:
                    
                    away_away_attack = get_initial_delta(all_deltas, away_team_internal_id, start_time, 'attack')
                    away_away_defence = get_initial_delta(all_deltas, away_team_internal_id, start_time, 'defence')
                
            else:
                
                if home_venue & (travel_buffer != -1):
                    away_away_attack = get_last_pre_elo('away', all_events, away_team_internal_id, away_team_away_fixture_number, home_post_delta_name_3, away_post_delta_name_3, 'home_team_home_fixture_number', 'away_team_away_fixture_number')
                    away_away_defence = get_last_pre_elo('away', all_events, away_team_internal_id, away_team_away_fixture_number, home_post_delta_name_4, away_post_delta_name_4, 'home_team_home_fixture_number', 'away_team_away_fixture_number')
                else:
                    away_away_attack = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_away_fixture_number, home_post_delta_name_1, away_post_delta_name_1, 'home_team_total_fixture_number', 'away_team_total_fixture_number')
                    away_away_defence = get_last_pre_elo(None, all_events, away_team_internal_id, away_team_away_fixture_number, home_post_delta_name_2, away_post_delta_name_2, 'home_team_total_fixture_number', 'away_team_total_fixture_number')

                
                
                
            all_events.loc[event, away_pre_delta_name_1] = away_attack
            all_events.loc[event, away_pre_delta_name_2] = away_defence
            all_events.loc[event, away_pre_delta_name_3] = away_away_attack
            all_events.loc[event, away_pre_delta_name_4] = away_away_defence

            
            
                
            base_value = 0
            #if get_base == True:
            #    base_value = get_base_value(buffer_type, all_events, start_time, competition_id, competition_fixture_number, home_competition_group, home_competition_fixture_number, delta_column_to_calcuate)
            
            
           


            pred_home_score_all = base_value + (home_attack + away_defence)
            pred_home_score_ha = base_value + (home_home_attack + away_away_defence)
            pred_away_score_all = base_value + (home_defence + away_attack)
            pred_away_score_ha = base_value + (home_home_defence + away_away_attack)
            

            # New home_attack adjustement
             
            if(pred_home_score_all<=20):
                home_adjustment = (pred_home_score_all/1.35)-15
            else:
                home_adjustment = (pred_home_score_all/4.5)-5
            
            pred_home_score_all = pred_home_score_all - home_adjustment
            pred_home_score_ha = pred_home_score_ha - home_adjustment
            
            if pred_away_score_all<=16:
                away_adjustement = (pred_away_score_all/1.35)-12
            else:
                away_adjustement = ((pred_away_score_all/2)-8)
                
            pred_away_score_all = pred_away_score_all - away_adjustement
            pred_away_score_ha = pred_away_score_ha - away_adjustement
                    
            
            all_events.loc[event, 'pred_home_score_all'] = pred_home_score_all
            all_events.loc[event, 'pred_home_score_ha'] = pred_home_score_ha
            all_events.loc[event, 'pred_away_score_all'] = pred_away_score_all
            all_events.loc[event, 'pred_away_score_ha'] = pred_away_score_ha
            

            all_events.loc[event, 'home_adjustment'] = home_adjustment
            all_events.loc[event, 'away_adjustement'] = away_adjustement

            
            all_events.loc[event, 'base_value'] = base_value
            all_events.loc[event, home_team_buffer_name] = home_team_buffer
            all_events.loc[event, pre_delta_diff_name] = pre_elo_diff


            if pd.isna(actual_target_value_1):

                all_events.loc[event, home_post_delta_name_1] = home_attack
                all_events.loc[event, home_post_delta_name_2] = home_defence
                all_events.loc[event, home_post_delta_name_3] = home_home_attack
                all_events.loc[event, home_post_delta_name_4] = home_home_defence
                all_events.loc[event, away_post_delta_name_1] = away_attack
                all_events.loc[event, away_post_delta_name_2] = away_defence
                all_events.loc[event, away_post_delta_name_3] = away_away_attack
                all_events.loc[event, away_post_delta_name_4] = away_away_defence

            else:

                # Comparing against the home_score (for all games and for home_home games)
                if (actual_target_value_1 < (pred_home_score_all - win_margin_buffer)):
                    all_events.loc[event, home_post_delta_name_1] = home_attack - (min(max_points_win, np.log(0.000001+abs(pred_home_score_all - actual_target_value_1))) - win_bonus)/2
                    all_events.loc[event, away_post_delta_name_2] = away_defence - (min(max_points_win, np.log(0.000001+abs(pred_home_score_all - actual_target_value_1))) + win_bonus)/2
                elif (actual_target_value_1 > (pred_home_score_all + win_margin_buffer)):
                    all_events.loc[event, home_post_delta_name_1] = home_attack + (min(max_points_win, np.log(0.000001+abs(pred_home_score_all - actual_target_value_1))) + win_bonus)/2
                    all_events.loc[event, away_post_delta_name_2] = away_defence + (min(max_points_win, np.log(0.000001+abs(pred_home_score_all - actual_target_value_1))) - win_bonus)/2
                else:
                    all_events.loc[event, home_post_delta_name_1] = home_attack
                    all_events.loc[event, away_post_delta_name_2] = away_defence
                    
                    
                if (actual_target_value_1 < (pred_home_score_ha - win_margin_buffer)):
                    all_events.loc[event, home_post_delta_name_3] = home_home_attack - (min(max_points_win, np.log(0.000001+abs(pred_home_score_ha - actual_target_value_1))) - win_bonus)/2
                    all_events.loc[event, away_post_delta_name_4] = away_away_defence - (min(max_points_win, np.log(0.000001+abs(pred_home_score_ha - actual_target_value_1))) + win_bonus)/2
                elif (actual_target_value_1 > (pred_home_score_ha + win_margin_buffer)):
                    all_events.loc[event, home_post_delta_name_3] = home_home_attack + (min(max_points_win, np.log(0.000001+abs(pred_home_score_ha - actual_target_value_1))) + win_bonus)/2
                    all_events.loc[event, away_post_delta_name_4] = away_away_defence + (min(max_points_win, np.log(0.000001+abs(pred_home_score_ha - actual_target_value_1))) - win_bonus)/2
                else:
                    all_events.loc[event, home_post_delta_name_3] = home_home_attack
                    all_events.loc[event, away_post_delta_name_4] = away_away_defence
                    
                # Comparing against the away_score (for all games and for away_away games)
                if (actual_target_value_2 < (pred_away_score_all - win_margin_buffer)):
                    all_events.loc[event, home_post_delta_name_2] = home_defence - (min(max_points_win, np.log(0.000001+abs(pred_away_score_all - actual_target_value_2))) - win_bonus)/2
                    all_events.loc[event, away_post_delta_name_1] = away_attack - (min(max_points_win, np.log(0.000001+abs(pred_away_score_all - actual_target_value_2))) + win_bonus)/2
                elif (actual_target_value_2 > (pred_away_score_all + win_margin_buffer)):
                    all_events.loc[event, home_post_delta_name_2] = home_defence + (min(max_points_win, np.log(0.000001+abs(pred_away_score_all - actual_target_value_2))) + win_bonus)/2
                    all_events.loc[event, away_post_delta_name_1] = away_attack + (min(max_points_win, np.log(0.000001+abs(pred_away_score_all - actual_target_value_2))) - win_bonus)/2
                else:
                    all_events.loc[event, home_post_delta_name_2] = home_defence
                    all_events.loc[event, away_post_delta_name_1] = away_attack
                    
                    
                if (actual_target_value_2 < (pred_away_score_ha - win_margin_buffer)):
                    all_events.loc[event, home_post_delta_name_4] = home_home_defence - (min(max_points_win, np.log(0.000001+abs(pred_away_score_ha - actual_target_value_2))) - win_bonus)/2
                    all_events.loc[event, away_post_delta_name_3] = away_away_attack - (min(max_points_win, np.log(0.000001+abs(pred_away_score_ha - actual_target_value_2))) + win_bonus)/2
                elif (actual_target_value_2 > (pred_away_score_ha + win_margin_buffer)):
                    all_events.loc[event, home_post_delta_name_4] = home_home_defence + (min(max_points_win, np.log(0.000001+abs(pred_away_score_ha - actual_target_value_2))) + win_bonus)/2
                    all_events.loc[event, away_post_delta_name_3] = away_away_attack + (min(max_points_win, np.log(0.000001+abs(pred_away_score_ha - actual_target_value_2))) - win_bonus)/2
                else:
                    all_events.loc[event, home_post_delta_name_4] = home_home_defence
                    all_events.loc[event, away_post_delta_name_3] = away_away_attack
                    
                    
                          
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')
    

    
    return all_events



def add_event_deltas(all_events, event_deltas, float_columns):
    
    function_start_time = datetime.datetime.now()
    print('-add_event_deltas')
    
    for col_name in float_columns:
        if col_name in all_events.columns:
            all_events.drop(col_name, axis = 1, inplace = True)
            
    temp_columns = float_columns.copy()
    temp_columns.append('event_id')
    event_deltas = event_deltas[temp_columns]

    all_events = all_events.merge(event_deltas, how = 'left', left_on = 'event_id', right_on = 'event_id') 

    for col in float_columns:
        all_events[col] = all_events[col].apply(lambda x: float(x))

    #all_events['home_pre_delta'] = all_events['home_pre_delta'].apply(lambda x: float(x))
    #all_events['home_post_delta'] = all_events['home_post_delta'].apply(lambda x: float(x))
    #all_events['away_pre_delta'] = all_events['away_pre_delta'].apply(lambda x: float(x))
    #all_events['away_post_delta'] = all_events['away_post_delta'].apply(lambda x: float(x))
    #all_events['home_team_buffer'] = all_events['home_team_buffer'].apply(lambda x: float(x) if pd.notna(x) else None)
    #all_events['pre_delta_diff'] = all_events['pre_delta_diff'].apply(lambda x: float(x) if pd.notna(x) else None)

    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')
    
    return all_events


# In[39]:


def add_trends(all_events, trend_name, trend_type, num_games, start_range):


    function_start_time = datetime.datetime.now()
    print('-add_trends')
    print('--', trend_name, num_games)
    
    home_trend_name = 'home_' + trend_name
    away_trend_name = 'away_' + trend_name

    trend_name_final = trend_name + '_trend' + '_' + str(num_games)
    home_trend_name_final = 'home_' + trend_name_final
    away_trend_name_final = 'away_' + trend_name_final

    if home_trend_name_final in all_events.columns:
        all_events.drop(home_trend_name_final, axis = 1, inplace = True)
    if away_trend_name_final in all_events.columns:
        all_events.drop(away_trend_name_final, axis = 1, inplace = True)

    home_fixtures = all_events[['event_id', 'home_team_internal_id', 'home_team_total_fixture_number', 'home_' + trend_type]].rename(columns = {'home_team_internal_id':'team_id', 'home_team_total_fixture_number':'team_total_fixture_number', 'home_' + trend_type:trend_name})
    away_fixtures = all_events[['event_id', 'away_team_internal_id', 'away_team_total_fixture_number', 'away_' + trend_type]].rename(columns = {'away_team_internal_id':'team_id', 'away_team_total_fixture_number':'team_total_fixture_number', 'away_' + trend_type:trend_name})

    all_team_fixtures = home_fixtures.append(away_fixtures, ignore_index = True)
    all_team_fixtures = all_team_fixtures.drop_duplicates().reset_index()
    all_team_fixtures.sort_values('team_total_fixture_number', inplace = True)

    all_team_fixtures.loc[ : , trend_name_final] = None
    
    
    for team in all_team_fixtures['team_id'].drop_duplicates():

        team_fixtures = all_team_fixtures[ all_team_fixtures['team_id'] == team]
        
        team_fixtures_to_calculate = team_fixtures[start_range :]

        for row in team_fixtures_to_calculate.index:

            current_fixture_number = team_fixtures.loc[row, 'team_total_fixture_number']
            team_trend = team_fixtures[ (team_fixtures['team_total_fixture_number'] < current_fixture_number) & (team_fixtures['team_total_fixture_number'] >= (current_fixture_number - num_games)) ][trend_name].mean()        
            team_fixtures.loc[row, trend_name_final] = team_trend


        all_team_fixtures[trend_name_final].update(team_fixtures[trend_name_final])


    
    all_events = all_events.merge(all_team_fixtures[['event_id', 'team_id', trend_name_final]].rename(columns = {'team_id':'home_team_internal_id', trend_name_final: home_trend_name_final}), how = 'left', left_on = ['event_id', 'home_team_internal_id'], right_on = ['event_id', 'home_team_internal_id'])
    all_events = all_events.merge(all_team_fixtures[['event_id', 'team_id', trend_name_final]].rename(columns = {'team_id':'away_team_internal_id', trend_name_final: away_trend_name_final}), how = 'left', left_on = ['event_id', 'away_team_internal_id'], right_on = ['event_id', 'away_team_internal_id'])
    
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')
    

    return all_events



def update_event_deltas_trends(all_events, delta_change_name, error_name):

    function_start_time = datetime.datetime.now()
    print('-update_event_deltas_trends')
    
    sql_statement_full = ''
    loop_num = 0

    if len(all_events)>0:

        for event in all_events.index:

            #print(loop_num, len(all_events))

            event_id = all_events.loc[event, 'event_id']

            home_delta_change_trend_5_name = 'home_' + delta_change_name + '_trend_5'
            home_delta_change_trend_10_name = 'home_' + delta_change_name + '_trend_10'
            home_delta_change_trend_20_name = 'home_' + delta_change_name + '_trend_20'
            
            away_delta_change_trend_5_name = 'away_' + delta_change_name + '_trend_5'
            away_delta_change_trend_10_name = 'away_' + delta_change_name + '_trend_10'
            away_delta_change_trend_20_name = 'away_' + delta_change_name + '_trend_20'
            
            home_error_trend_5_name = 'home_' + error_name + '_trend_5'
            home_error_trend_10_name = 'home_' + error_name + '_trend_10'
            home_error_trend_20_name = 'home_' + error_name + '_trend_20'
            
            away_error_trend_5_name = 'away_' + error_name + '_trend_5'
            away_error_trend_10_name = 'away_' + error_name + '_trend_10'
            away_error_trend_20_name = 'away_' + error_name + '_trend_20'
            
            
            home_delta_change_trend_5 = all_events.loc[event, home_delta_change_trend_5_name]
            home_delta_change_trend_10 = all_events.loc[event, home_delta_change_trend_10_name]
            home_delta_change_trend_20 = all_events.loc[event, home_delta_change_trend_20_name]

            away_delta_change_trend_5 = all_events.loc[event, away_delta_change_trend_5_name]
            away_delta_change_trend_10 = all_events.loc[event, away_delta_change_trend_10_name]
            away_delta_change_trend_20 = all_events.loc[event, away_delta_change_trend_20_name]

            home_error_trend_5 = all_events.loc[event, home_error_trend_5_name]
            home_error_trend_10 = all_events.loc[event, home_error_trend_10_name]
            home_error_trend_20 = all_events.loc[event, home_error_trend_20_name]

            away_error_trend_5 = all_events.loc[event, away_error_trend_5_name]
            away_error_trend_10 = all_events.loc[event, away_error_trend_10_name]
            away_error_trend_20 = all_events.loc[event, away_error_trend_20_name]

            if pd.isna(home_delta_change_trend_5):
                home_delta_change_trend_5 = 'NULL'
            if pd.isna(home_delta_change_trend_10):
                home_delta_change_trend_10 = 'NULL'
            if pd.isna(home_delta_change_trend_20):
                home_delta_change_trend_20 = 'NULL'
            if pd.isna(away_delta_change_trend_5):
                away_delta_change_trend_5 = 'NULL'
            if pd.isna(away_delta_change_trend_10):
                away_delta_change_trend_10 = 'NULL'
            if pd.isna(away_delta_change_trend_20):
                away_delta_change_trend_20 = 'NULL'

            if pd.isna(home_error_trend_5):
                home_error_trend_5 = 'NULL'
            if pd.isna(home_error_trend_10):
                home_error_trend_10 = 'NULL'
            if pd.isna(home_error_trend_20):
                home_error_trend_20 = 'NULL'
            if pd.isna(away_error_trend_5):
                away_error_trend_5 = 'NULL'
            if pd.isna(away_error_trend_10):
                away_error_trend_10 = 'NULL'
            if pd.isna(away_error_trend_20):
                away_error_trend_20 = 'NULL'

            sql_statement = 'update event_deltas set ' + str(home_delta_change_trend_5_name) + ' = ' + str(home_delta_change_trend_5) + ', ' + str(home_delta_change_trend_10_name) + ' = ' + str(home_delta_change_trend_10) + ', ' + str(home_delta_change_trend_20_name) + ' = ' + str(home_delta_change_trend_20) + ', ' + str(away_delta_change_trend_5_name) + ' = ' + str(away_delta_change_trend_5) + ', ' + str(away_delta_change_trend_10_name) + ' = ' + str(away_delta_change_trend_10) + ', ' + str(away_delta_change_trend_20_name) + ' = ' + str(away_delta_change_trend_20) + ', ' + str(home_error_trend_5_name) + ' = ' + str(home_error_trend_5) + ', ' + str(home_error_trend_10_name) + ' = ' + str(home_error_trend_10) + ', ' + str(home_error_trend_20_name) + ' = ' + str(home_error_trend_20) +  ', ' + str(away_error_trend_5_name) + ' = ' +str(away_error_trend_5) + ', ' + str(away_error_trend_10_name) + ' = ' + str(away_error_trend_10) + ', ' + str(away_error_trend_20_name) + ' = ' + str(away_error_trend_20) + " where event_id = '" + str(event_id) + "';"

            sql_statement_full = sql_statement_full + sql_statement
            loop_num += 1
            
    if len(sql_statement_full) > 0:
        postgres_Retreive_Insert(sql_statement_full, POSTGRESQL_PARAMS, False)
        
    function_end_time = datetime.datetime.now()
    print('--Complete-' + str(function_end_time - function_start_time))
    print('')




def check_for_duplicate_events(all_events):

    home_games = all_events[['event_id', 'start_time', 'home_team_internal_id']].rename( columns = {'home_team_internal_id':'team_internal_id'})
    away_games = all_events[['event_id', 'start_time', 'away_team_internal_id']].rename( columns = {'away_team_internal_id':'team_internal_id'})
    both_teams = pd.concat([home_games, away_games], ignore_index = True)

    both_teams = both_teams.groupby(['team_internal_id', 'start_time']).count().reset_index()
    events_to_check = both_teams[ both_teams['event_id'] > 1]


    duplicate_events = []

    for etc in events_to_check.index:

        start_time = events_to_check.loc[etc, 'start_time']
        team_id = events_to_check.loc[etc, 'team_internal_id']

        event_ids = all_events[ ( (all_events['home_team_internal_id'] == team_id)  | (all_events['away_team_internal_id'] == team_id) ) & (all_events['start_time'] == start_time)  ]['event_id']

        for e in event_ids:

            duplicate_events.append(e)

    duplicate_events = list(set([item for item in duplicate_events]))
    
    return duplicate_events




def check_all_teams_exist(all_events, all_teams):
    
    proceed = True

    home_teams = list(all_events['home_team_internal_id'])
    away_teams = list(all_events['away_team_internal_id'])

    both_teams = home_teams + away_teams
    both_teams = list(set(both_teams))

    db_teams = list(all_teams['id'])

    not_in_db = [team for team in both_teams if team not in db_teams]
    
    if len(not_in_db) > 0:
        print('There are no enties for the team: ' + str(not_in_db))
        proceed = False

    return proceed




def check_competitions_exist(all_events, all_teams):
    
    proceed = True

    comps = list(all_events['competition_internal_id'])
    comps = list(set(comps))

    db_comps = list(all_competitions['id'])

    not_in_db = [team for team in comps if team not in db_comps]
    
    if len(not_in_db) > 0:
        print('There are no enties for the competition: ' + str(not_in_db))
        proceed = False

    return proceed

