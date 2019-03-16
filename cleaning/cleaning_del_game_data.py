"""

Cleaning DEL data

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
import pandas as pd


def get_date(events: Dict[Any, Any]) -> str:

    period_penalties: List[List[Any]] = list(map(lambda period: 
                                             list(filter(lambda x:
                                                  x['type'] == 'penalty', 
                                                  list(filter(lambda x: 
                                                    len(x) > 0,
                                                    events[period]))
                                                  )),
                                             ['1', '2', '3'])
                                             )

    penalties: List[Any] = reduce(lambda x, y: x + y, period_penalties)

    if len(penalties) > 0:
        penalty_time: str = list(map(
                            lambda e: e['data']['time']['from']['realTime'], 
                            penalties))[0]
        return penalty_time

    period_goals: List[List[Any]] = list(map(lambda period:
                                         list(filter(lambda x:
                                             x['type'] == 'goal',
                                             list(filter(lambda x: 
                                                    len(x) > 0,
                                                    events[period])),
                                             )),
                                         ['1', '2', '3'])
                                         )
    goals: List[Any] = reduce(lambda x, y: x + y, period_goals)
    
    if len(goals) > 0:
        goal_time: str =  list(map(lambda e: e['data']['realTime'],
                                   goals))[0]
        return goal_time
    else:
        return 'No-date'


def get_base_headers(data_dir: str) -> Tuple[List[str], List[str]]:

    # GETTING HEADER
    # HARD CODING. KNOW THAT GAME ID 1010 HAS ALL COLUMNS
    suffix: str = '1010.json' 
    game: Dict[Any, Any]  = json.load(open(data_dir + 'game_' + suffix, 'rb'))
 
    header_home_info: List[str] = [k for k, v in game['teamInfo']['home'].items()]
    header_visitor_info: List[str] = [k for k, v in game['teamInfo']['visitor'].items()]
    
    return (header_home_info, header_visitor_info) 


def get_extra_headers(data_dir: str) -> Tuple[
        List[str], List[str], List[str], List[str]]:

    # GETTING HEADER
    # HARD CODING. KNOW THAT GAME ID 1010 HAS ALL COLUMNS
    suffix: str = '1010.json' 
    game: Dict[Any, Any]  = json.load(open(data_dir + 'game_' + suffix, 'rb'))
    home_shots: Dict[Any, Any] = json.load(open(data_dir + 'home_' + suffix, 'rb'))
    visitor_shots: Dict[Any, Any] = json.load(open(data_dir + 'guest_' + suffix, 'rb'))
 
    header_home_info: List[str] = [k for k, v in game['teamInfo']['home'].items()]
    header_home_shots: List[str] = [k for k,v in home_shots.items()]
    header_visitor_info: List[str] = [k for k, v in game['teamInfo']['visitor'].items()]
    header_visitor_shots: List[str] = [k for k,v in visitor_shots.items()]
    
    return (header_home_info, 
            header_home_shots,
            header_visitor_info,
            header_visitor_shots) 


def get_base_game_data(game_i: str, data_dir: str) -> List[Any]:

    suffix: str = game_i[5:]
    game: Dict[Any, Any]  = json.load(open(data_dir + game_i, 'rb'))
    home_info: List[Any] = [v for k, v in game['teamInfo']['home'].items()]
    visitor_info: List[Any] = [ v for k, v in game['teamInfo']['visitor'].items()]
    return home_info + visitor_info


def get_extra_game_data(game_i: str, data_dir: str, 
                        columns_home: List[str],
                        columns_away: List[str]) -> List[Any]:
    
    suffix: str = game_i[5:]
    print(suffix)
    try:
        game: Dict[Any, Any]  = json.load(open(data_dir + game_i, 'rb'))
        home_info: List[Any] = [v for k, v in game['teamInfo']['home'].items()]
        visitor_info: List[Any] = [ v for k, v in game['teamInfo']['visitor'].items()]
    except FileNotFoundError:
        print("Main data not found for game",suffix)
        return []

    try:
        events: Dict[Any, Any] = json.load(open(data_dir + 'events_' + suffix, 'rb'))
        date: str = get_date(events)
        
        home_shots_dict: Dict[Any, Any] = json.load(open(data_dir + 'home_' + suffix, 'rb'))
        home_shots: List[Any] = [home_shots_dict.get(c[:-5], "nan") for c in columns_home]
        
        visitor_shots_dict: Dict[Any, Any] = json.load(open(data_dir + 'guest_' + suffix, 'rb'))
        visitor_shots: List[Any] = [visitor_shots_dict.get(c[:-5], "nan") for c in columns_away]
        
        h: List[Any] = home_info + home_shots
        v: List[Any] = visitor_info + visitor_shots
        return h + v + [date]
    except FileNotFoundError:
        return home_info + visitor_info


def drop_non_del_games(df: pd.DataFrame,
                       non_del_teams: List[str]) -> pd.DataFrame:
    
    non_del_home = list(map(lambda x: x not in set(non_del_teams),
                            df.name_home.values))
    non_del_away = list(map(lambda x: x not in set(non_del_teams),
                            df.name_away.values))
     
    to_keep = list(map(lambda h, a: h and a, 
                       non_del_home, 
                       non_del_away))
 
    return df[to_keep]
 

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
    df_base.to_csv('../data/base_game_data.csv')

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
    df_full.to_csv('../data/complete_game_data.csv')

    df: pd.DataFrame = drop_non_del_games(df_full, non_del_teams)
    
    df.to_csv('../data/game_data_del.csv')

# EOF
