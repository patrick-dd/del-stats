"""

Cleaning DEL data from the website JSONs into something useable

"""

from itertools import repeat
from functools import reduce
from json import JSONDecodeError
from typing import Any
from typing import Dict
from typing import List
from typing import Union
from typing import Tuple
import csv
import json
import os
import sys
import pandas as pd
from cleaning_del_game_data import *
from cleaning_del_event_data import *

sys.setrecursionlimit(15000)


if __name__=="__main__":

    data_dir: str = '../data/game_data/'
    
    non_del_teams: List[str] = ['Kassel Huskies', 'Mikkelin Jukurit', 
                                'EHC Kloten', 'HC Fribourg-Gottéron', 
                                'EHC Freiburg', 'Dornbirn Bulldogs', 
                                'HC Sparta Praha']

    files: List[str] = os.listdir(data_dir)

    games: List[str] = [f for f in files if 'game' in f]

    ################################################### 
    # BASE DATA
    ################################################### 
    head: Tuple[List[str], List[str]] = get_base_headers(data_dir)

    header_home_info: List[str] = [h + '_home' for h in head[0]]
    header_away_info: List[str] = [h + '_away' for h in head[1]]

    data: List[Any] = list(map(get_base_game_data, 
                               games, 
                               repeat(data_dir)
                               ))
    
    header: List[str] = header_home_info + header_away_info 
    
    df_base: pd.DataFrame = pd.DataFrame(data=data, columns=header)
    df_base_del: pd.DataFrame = drop_non_del_games(df_base, non_del_teams)
    df_base_del.to_csv('../data/base_del_game_data.csv')

    ################################################### 
    # EXTRA DATA
    ################################################### 
    head_extra: Tuple[List[str], List[str], 
                List[str], List[str]] = get_extra_headers(data_dir)

    header_X_home_info: List[str] = [h + '_home' for h in head_extra[0]]
    header_X_home_shots: List[str] = [h + '_home' for h in head_extra[1]]
    header_X_away_info: List[str] = [h + '_away' for h in head_extra[2]]
    header_X_away_shots: List[str] = [h + '_away' for h in head_extra[3]]

    data_extra: List[Any] = list(map(get_extra_game_data, 
                               games, 
                               repeat(data_dir),
                               repeat(header_X_home_shots),
                               repeat(header_X_away_shots)
                               ))
    
    header_home: List[str] = header_X_home_info + header_X_home_shots 
    header_away: List[str] = header_X_away_info + header_X_away_shots 
    header_X: List[str] = header_home + header_away + ['date'] 
    
    df_full: pd.DataFrame = pd.DataFrame(data=data_extra, columns=header_X)
    df: pd.DataFrame = drop_non_del_games(df_full, non_del_teams)
    
    df.to_csv('../data/game_data_del.csv')

    ##################################################
    # EVENT DATA
    ##################################################
    
    shot_data_dir: str = '../data/shot_data/'
    event_data_dir: str = '../data/game_data/'

    shot_files: List[str] = os.listdir(shot_data_dir)
    event_files: List[str] = list(filter(lambda x: 'event' in x,
                                         os.listdir(event_data_dir)))

    
    shot_idx: Dict[str, int] = {'vorbei': 2, 'geblockt': 3, 'gehalten': 1, 'tore': 4}
    idx_shot: Dict[int, str] = {v: k for k, v in shot_idx.items()}

    event_data: List[List[Dict[Any, Any]]] = list(map(
            clean_event_data, repeat(event_data_dir), event_files))
    
    #######################################################################
    # GOALS
    #######################################################################
    goals: List[DataFrame] = list(map(get_goals, event_data, event_files))

    clean_goals: DataFrame = reduce(lambda x, y: pd.concat([x, y],
                                                           axis=0,
                                                           ignore_index=True,
                                                           sort=False),
                                                 goals)

    clean_goals.to_csv('../data/del_event_goal_data.csv', index=False)

    #######################################################################
    # PENALTIES
    #######################################################################
    penalties: List[DataFrame] = list(map(get_penalty_times, event_data, event_files))

    clean_pens: DataFrame = reduce(lambda x, y: pd.concat([x, y],
                                                           axis=0,
                                                           ignore_index=True,
                                                           sort=False),
                                                penalties)

    clean_pens.to_csv('../data/del_event_penalty_data.csv', index=False)
    #######################################################################
    # SHOTS
    #######################################################################
    shot_game_data: List[Any] = list(map(clean_shot_data, 
                                          repeat(shot_data_dir),
                                          shot_files,
                                          repeat(idx_shot)))



    clean_game_data: List[Any] = list(filter(lambda x: x is not None, shot_game_data))
    
    df: pd.DataFrame = reduce(lambda x, y: pd.concat([x, y], 
                                                      axis=0,
                                                      ignore_index=True,
                                                      sort=False),
                               clean_game_data)

    df.to_csv('../data/del_shot_data.csv', index=False)    
    
    #######################################################################
    # PLAYERS ON ICE
    #######################################################################
    
    # tmp
    df = pd.read_csv('../data/del_shot_data.csv')
    player_on_ice_dfs: List[Any] = []
    for di, fn in zip(list(event_data), event_files):
        game_dict: Dict[Any, Any] = get_people_on_ice(di, fn)
        game_number_extension: str = fn.replace('events', '')
        tmp: DataFrame = add_players_on_ice(df, 
                                game_dict,        
                                game_number_extension)
        player_on_ice_dfs.append(tmp)
       
        # tmp.to_csv('../data/players_on_ice/' + fn[:-4] + 'csv', index=False)
    
    df = reduce(lambda x, y: pd.concat([x, y], axis=0, ignore_index=True, sort=False), player_on_ice_dfs)


    df.to_csv('../data/del_shot_penalty_data.csv', index=False)

    #######################################################################
    # GOALIES ON ICE
    #######################################################################
    
    # tmp
    df = pd.read_csv('../data/del_shot_penalty_data.csv')
    goalie_on_ice_dfs: List[Any] = []
    for di, fn in zip(list(event_data), event_files):
        gk_di = create_gk_change_list_with_entry(di)
        g_game_dict: Dict[Any, Any] = get_goalie_dict(gk_di)
        g_game_number_extension: str = fn.replace('events', '')
        g_tmp: DataFrame = add_players_on_ice(df, 
                                g_game_dict,        
                                g_game_number_extension,
                                goalie=True)
        goalie_on_ice_dfs.append(g_tmp)
         
    df_g = reduce(lambda x, y: pd.concat([x, y], axis=0, ignore_index=True, sort=False), 
                  goalie_on_ice_dfs)

    df_g.to_csv('../data/del_goalie_on_ice.csv', index=False)

    #######################################################################
    # EVEN STRENGTH SECONDS
    #######################################################################
 
    df = pd.read_csv('../data/del_shot_penalty_data.csv')
    even_strength_seconds: List[Any] = []
    for di, fn in zip(list(event_data), event_files):
        if di != []:
            max_seconds = di[-1]['time']
            game_number_extension: str = fn.replace('events', '')
            game_dict: Dict[Any, Any] = get_people_on_ice(di, fn)
            even_strength_time = get_even_strength_time(game_dict)
            even_strength_seconds.append([fn.replace('events', 'game'),
                                          sum(even_strength_time[:max_seconds])])
    
    df_e = pd.merge(df,
                    pd.DataFrame(even_strength_seconds, columns=['fname',
                        'ev_seconds']),
                    left_on='fname',
                    right_on='fname',
                    how='left')

    df_e.to_csv('../data/del_ev_strength.csv', index=False)
    
    #######################################################################
    # UNEVEN STRENGTH SECONDS
    #######################################################################
    
    df = df_e
    uneven_strength_seconds_home: List[Any] = []
    uneven_strength_seconds_away: List[Any] = []
    for di, fn in zip(list(event_data), event_files):
        if di != []:
            max_seconds = di[-1]['time']
            game_number_extension: str = fn.replace('events', '')
            game_dict: Dict[Any, Any] = get_people_on_ice(di, fn)
            uneven_strength_time_home = get_uneven_strength_time_home(game_dict)
            uneven_strength_seconds_home.append([fn.replace('events', 'game'),
                                                sum(uneven_strength_time_home[:max_seconds])])
            uneven_strength_time_away = get_uneven_strength_time_away(game_dict)
            uneven_strength_seconds_away.append([fn.replace('events', 'game'),
                                                sum(uneven_strength_time_away[:max_seconds])])
 
    df_u1 = pd.merge(df,
                     pd.DataFrame(uneven_strength_seconds_home, 
                                  columns=['fname', 'power_play_home']),
                     left_on='fname',
                     right_on='fname',
                     how='left')
    
    df_u2 = pd.merge(df_u1,
                     pd.DataFrame(uneven_strength_seconds_away,
                                 columns=['fname', 'power_play_away']),
                     left_on='fname',
                     right_on='fname',
                     how='left')

    df_u2.to_csv('../data/del_unev_strength.csv', index=False)
 
# EOF
