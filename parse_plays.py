import numpy as np
import pandas as pd 
from sqlalchemy import create_engine
import sqlite3
import time 
from parse_funcs import made_shot, missed_shot, rebound, turnover, foul, player_sub, timeout
from unidecode import unidecode
from datetime import datetime, timedelta

# RUN FROM HOME DIRECTORY
# set Yesterday in (main) to False for full season

PATH_TO_RAW_DB = 'NBAdraft/save_to_db/nba.db'
PATH_TO_PARSED_DB = 'NBAdraft/save_to_db/nba_parsed.db'
SEASON = "2020-2021"
YEAR = 2021


def import_from_sqlite(season, yesterday=False):
    """
    option to return the season or just yesterday's games
    """

    try:
        conn = sqlite3.connect(PATH_TO_RAW_DB)
        engine = create_engine(F'sqlite:///{NBAdraft/save_to_db/nba.db}')
        dfs = pd.read_sql_table(f'pbp_{season}', con=engine, chunksize=20000)
        
    except IOError as e:
        print(e)

    finally:
        conn.close()

    if yesterday:
        for df in dfs:
            df = df[df.date == (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')]
            if not df.empty:
                return [df]

    return dfs


def save_to_sqlite(df, season):
    try:
        conn = sqlite3.connect(PATH_TO_PARSED_DB)
        engine = create_engine(F'sqlite:///{PATH_TO_PARSED_DB}')
        df.to_sql(f'pbp_{season}', con=conn, if_exists='append')
        print("Saved to database")

    except IOError as e:
        print(e)

    finally:
        conn.close()


def parse(l: list) -> object:
    """
    takes a play and classifies it into one of:
    made_shot, missed_shot, rebound, turnover, foul, player_sub, timeout
    """

    if "makes" in l:
        df = made_shot(l)
    elif "misses" in l:
        df = missed_shot(l)
    elif "rebound" in l:
        df = rebound(l)
    elif "Turnover" in l:
        df = turnover(l)
    elif ("foul" in l) or ("Violation" in l):
        df = foul(l)
    elif ("enters" in l) or ("ejected" in l):
        df = player_sub(l)
    elif ("timeout" in l) or ("Replay" in l):
        df = timeout(l)
    else:
        print(f"Error parsing:  {l}")
        return None

    return df

def primary_and_secondary_teams(row):
    """
    away_action being opposite team
    "Shooting foul by K. Irving (drawn by S. Curry)"
    """

    flips = ["Shooting foul", "Personal foul"]
    if row.away_action:
        #print(row)
        if ("Shooting foul" in row.away_action) or ("Personal" in row.away_action) \
            or ("Away" in row.away_action) or ("Flagrant" in row.away_action):
            primary_team = row.home_team
            secondary_team = row.away_team
        else:
            primary_team = row.away_team
            if row.secondary_action in ['steal', 'block', 'foul drawn']:
                secondary_team = row.home_team
            elif row.secondary_action in ['assist', 'sub out']:
                secondary_team = primary_team
            else:
                secondary_team = None
    else:
        if ("Shooting foul" in row.home_action) or ("Personal" in row.home_action) \
            or ("Away" in row.home_action) or ("Flagrant" in row.home_action):
            primary_team = row.away_team
            secondary_team = row.home_team
        else:
            primary_team = row.home_team 
            if row.secondary_action in ['steal', 'block', 'foul drawn']:
                secondary_team = row.away_team
            elif row.secondary_action in ['assist', 'sub out']:
                secondary_team = primary_team
            else:
                secondary_team = None

    return [primary_team, secondary_team]

def make_player_dict():
    df = pd.read_csv(f'players_{YEAR}.csv')
    # combine Name and Team cols
    df['key'] = df['abbr_name'] + "," + df['team']
    df = df[['key', 'full_name']].set_index('key')

    # find duplicates
    dupes = df.index[df.index.duplicated()].unique()
    #print(dupes)

    # Cody Martin and Caleb Martin creating duplicates; removing them
    df = df.drop(['C. Martin,CHO'])

    d = {}
    for i,row in df.iterrows():
        d[i] = row.values[0]

    return d

def clean_names(df1, df2):
    # remove accents
    df2['primary_player'] = df2['primary_player'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df2['secondary_player'] = df2['secondary_player'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df2.index = df1.index # fixing concat resulting in NaN

    # add the new columns
    result = pd.concat([df1, df2], axis=1).drop(columns=['index']).reset_index()

    # add primary team and secondary team cols
    psteams = pd.DataFrame(result.apply(primary_and_secondary_teams, axis=1).tolist(), \
                            columns=['primary_team', 'secondary_team'])

    result = pd.concat([result, psteams], axis=1)

    # sub in full player names
    player_dict = make_player_dict()

    # primary player
    keys = result['primary_player'] + "," + result['primary_team']
    keys.replace(player_dict, inplace=True)
    """
    for k in keys[keys.str.contains(",")].unique():
        i = keys[keys == k].index[0]
        print(f"row {i}: {k}")
    """
    result['primary_player'] = keys

    # secondary player
    keys2 = result['secondary_player'] + "," + result['secondary_team']
    keys2.replace(player_dict, inplace=True)
    result['secondary_player'] = keys2

    return result

def main():
    
    dfs = import_from_sqlite(season, yesterday=True)
    error_rows = []
    cnt = 0
    # parse each play in the dataframe and append to list
    for df in dfs:
        plays = []
        for i,row in df.iterrows():
            if row.away_action:
                play = parse(row.away_action.split()) # problem is here; when i concat the databases the first one becomes null
            else:
                play = parse(row.home_action.split())
            plays.append(play)

        # create dataframe from list of plays
        parsed_chunk = pd.DataFrame(plays, columns=['primary_player', 'primary_action', 
                                                 'description', 'shot_dist', 'shot_value', 
                                                 'secondary_player', 'secondary_action'])
        
        # replace names w/ full names and remove accents
        result = clean_names(df, parsed_chunk)

        cnt += len(df)
        print(f"Rows {cnt - len(df)} - {cnt} parsed")

        # save to sqlite
        save_to_sqlite(result, season)


if __name__ == "__main__":
    startTime = time.time()

    main()

    executionTime = (time.time() - startTime)
    print(f"Execution time: {executionTime} seconds")
    
