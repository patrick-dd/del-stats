"""

    Cleaning data

"""


from functools import reduce
from itertools import repeat
from json import JSONDecodeError
from pandas import DataFrame
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Tuple
from typing import Union
import datetime
import json
import math
import numpy as np
import os
import pandas as pd
import sys


def clean_shot_data(data_dir: str, 
                    filename: str, 
                    idx_shot: Dict[int, str]) -> Union[None, pd.DataFrame]:
    """

    cleans shot data

    """
    try:
        data: Dict[Any, Any] = json.load(open(data_dir + filename, 'rb'))
    except JSONDecodeError:
        print("JSONDecodeError", filename)
        return None
    add_columns: List[Any] = list(data['match'].keys())[:8]
    other_data: List[Any] = list(map(lambda x: [x, data['match'][x]], add_columns))
    
    if data['match']['shots'] != []:
        df = pd.DataFrame(data['match']['shots'])
        df['shot_type'] = df['match_shot_resutl_id'].map(idx_shot)
        for d in other_data: df[d[0]] = d[1]

        df['real_date'] = pd.to_datetime(df['real_date'])
    else:
        return None
        # will want to record these
    return df.set_index('real_date').assign(fname = filename)


def power_play_team(team: str) -> str:
    if team == 'home':
        return 'visitor'
    return 'home'


def get_penalty(e: Dict[Any, Any]) -> List[Any]:
    return [e['data']['team'], power_play_team(e['data']['team']),
            e['data']['time']['from']['scoreboardTime'],
            e['data']['time']['to']['scoreboardTime']]


def goal_scorer(event: Dict['str', Any]) -> Dict[str, Any]:
    longer = ['scorer', 'assistants', 'attendants']
    base = {k: v for k, v in event['data'].items() if k not in longer}
    scorer = { k + '_scorer': v for k, v in event['data']['scorer'].items()}
    return {**base, **scorer}


def update_dict(old_dict: Dict[int, Dict[str, int]],
                key: int,
                value: Dict[str, int]) -> Dict[int, Dict[str, int]]:
    # from copy import deepcopy
    # new_dict = deepcopy(old_dict)
    new_dict = old_dict
    new_dict[key] = value
    return new_dict


def create_second_dictionary(
        data_row: List[Any], 
        second_data: Dict[int, Dict[str, int]]) -> Dict[int, Dict[str, int]]:
    
    if data_row[2] == data_row[3] + 1:
        return second_data
    else:
        new_players = second_data[data_row[2]][data_row[0]] - 1
        same_players = second_data[data_row[2]][data_row[1]]

        return create_second_dictionary(
                [data_row[0], 
                 data_row[1],
                 data_row[2]+ 1,
                 data_row[3]], 
                update_dict(second_data,
                           data_row[2],
                           {data_row[0] : new_players,
                            data_row[1] : same_players})
                           )

def update_player_gkc(data: Dict['str', Any]) -> List[Any]:
    if data['data']['player'] == None:
        inbound = 0
    else:
        inbound = 1
    if data['data']['outgoingGoalkeeper'] == None:
        outbound = 0
    else:
        outbound = 1
    return [data['time'], data['data']['team'],  -inbound + outbound]


def create_gk_change_list(data: List[Any]) -> List[Any]:
    gk_change_data = list(filter(lambda x: (x['type'] == 'goalkeeperChange') & (x['time'] != 0), data))
    return list(map(update_player_gkc, gk_change_data))
    

def create_gk_change_list_with_entry(data: List[Any]) -> List[Any]:
    return list(filter(lambda x: (x['type'] == 'goalkeeperChange'), data))


def create_second_dictionary_gk(second_data: Dict[int, Dict[str, int]],
        data_row: List[Any]) -> Dict[int, Dict[str, int]]:
   
    if data_row[1] == 'home':
        home_players_change = data_row[2]
        away_players_change = 0
    else:
        home_players_change = 0
        away_players_change = data_row[2]

    return update_dict(second_data,
                       data_row[0],
                       {'home' : home_players_change,
                        'visitor' : away_players_change})


def second_dict(nested_penalties: List[List[Any]],
                gk_change_data: List[Any]) -> Dict[int, Dict[str, int]]:
    " can't quite make this functional "
    second_data = { i : {'home': 5, 'visitor': 5} for i in range(100*100)}
    
    for row in nested_penalties:
        second_data = create_second_dictionary(row, second_data)

    for row in gk_change_data:
        second_data = create_second_dictionary_gk(second_data, row)
    return second_data

def get_people_on_ice(data: List[Dict[Any, Any]],
                      filename: str) -> Dict[int, Dict[str, int]]:
 
    ## PENALTIES
    # Filter for penalties, map `get_penality` over periods
    
    nested_penalties: List[Any] = list(map(get_penalty, list(filter(lambda x:
        x['type'] == 'penalty', data))))
    
    # have to clean for penalties that have a from earlier than to
    clean_nested_pens: List[Any] = list(filter(lambda x: x[2] < x[3], nested_penalties))

    gk_change = create_gk_change_list(data)
    
    return second_dict(clean_nested_pens, gk_change)


def get_goals(data: List[Dict[Any, Any]],
              filename: str) -> DataFrame:

    ## GOALS
    data_goals: List[Dict[str, Any]] = list(filter(lambda x: x['type'] == 'goal', data))
    goals_list: List[Dict[str, Any]] = list(map(goal_scorer, data_goals))
    goals_dict: Dict[Any, Any] = {k:[d.get(k) for d in goals_list] for k in {k for d in goals_list for k in d}}
    df_goals_pre: pd.DataFrame = pd.DataFrame(goals_dict)
    return df_goals_pre.assign(fname = filename)


def get_penalty_times(data: List[Dict[Any, Any]],
                      filename: str) -> DataFrame:
    ## PENALTIES
    # Filter for penalties, map `get_penality` over periods
    
    nested_penalties: List[Any] = list(map(get_penalty, list(filter(lambda x:
        x['type'] == 'penalty', data))))

    penalty_header: List[str] = ['unterzahl', 'ueberzahl', 'penaltyFrom', 'penaltyTo'] 
    df_penalties_pre: pd.DataFrame = pd.DataFrame(nested_penalties,
                                                  columns=penalty_header)
    return df_penalties_pre.assign(fname = filename)
 

def clean_event_data(data_dir: str, filename: str) -> List[Any]:
 
    data_raw: Dict[Any, Any] = json.load(open(data_dir + filename, 'rb'))
  
    data_one: List[Dict[Any, Any]] = reduce(lambda x, y: x + y, 
                list(map(lambda p: data_raw[p], ['1', '2', '3', 'overtime']))) 

    return list(filter(lambda x: x != {}, data_one))


def get_people_on_ice_shots(on_ice_dict: Dict[Any, Any],
                            df_shots: pd.DataFrame) -> pd.DataFrame:
    
    shot_files = df_shots[['time', 'fname']].values.tolist()
    shot_files_clean = [[x[0], x[1].split('_')[1]] for x in shot_files]
    home_on_ice = [on_ice_dict.get('event_' + x[1], np.nan)[x[0]]['home'] for x
            in shot_files_clean]
    visitor_on_ice = [on_ice_dict.get('event_' + x[1], np.nan)[x[0]]['visitor'] for x
            in shot_files_clean]
    out_df = df_shots.assign(home_on_ice, home_on_ice)
    out2_df = out_df.assign(visitor_on_ice, visitor_on_ice)
    return out2_df


def add_players_on_ice(df: DataFrame, game_on_ice: Dict[int, Dict[str, int]],
        game_number_extension: str, goalie: bool = False) -> DataFrame:
  
    if goalie:
        tmp_onice: DataFrame = pd.DataFrame.from_dict(game_on_ice,
            orient='index').rename(columns={'home': 'home_goalie','visitor':
                'visitor_goalie'})
 
    else:
        tmp_onice: DataFrame = pd.DataFrame.from_dict(game_on_ice,
            orient='index').rename(columns={'home': 'home','visitor':
                'visitor'})
 
    tmp_df: DataFrame = df[df.fname == 'game' + game_number_extension]
    return pd.merge(tmp_df, tmp_onice, left_on='time', right_index=True, how='left')

def get_goalie_dict(gkdata: List[Dict[Any, Any]], 
                    max_sec: int = 60*100, 
                    goalies: Dict[Any, Any] = {i: {'home': 0, 'visitor': 0,
                        'home_name_g' : '', 'visitor_name_g' : '' } for i in range(60*100)}) -> Dict[Any, Any]:
    
    if len(gkdata) == 0:
        return goalies
    change_second = gkdata[0]['time']+1
    #
    update = get_goalies_change(gkdata[0]['data'])
    for i in range(change_second, max_sec-1):
        goalies[i].update(update)
    return get_goalie_dict(gkdata[1:], max_sec, goalies)


def get_goalies_change(data: Dict[str, Any]) -> Dict[int, Any]:
       
    if data['player'] is not None:
        incoming = True
    else:
        incoming = False
    if data['outgoingGoalkeeper'] is not None:
        outgoing = True
    else:
        outgoing = False
    
    if incoming:
        return {data['team'] : data['player']['playerId'],
                data['team'] + '_name_g': data['player']['name'] + '_' + data['player']['surname']  }
    if outgoing:
        return {data['team'] : None , data['team'] + '_name_g' : ''} 

    return {}


def get_even_strength_time(game_dict: Dict[Any, Any]) -> List[int]:
    return [1 if v['home']==v['visitor'] else 0 for k, v in game_dict.items()]


def get_uneven_strength_time_home(game_dict: Dict[Any, Any]) -> List[int]:
    # power play at home.
    return [1 if v['home']>v['visitor'] else 0 for k, v in game_dict.items()]


def get_uneven_strength_time_away(game_dict: Dict[Any, Any]) -> List[int]:
    # power play away
    return [1 if v['home']<v['visitor'] else 0 for k, v in game_dict.items()]

# EOF
