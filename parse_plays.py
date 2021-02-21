import numpy as np
import pandas as pd 
from sqlalchemy import create_engine
import sqlite3

def import_from_sqlite(season):
    """ 
    Get play-by-play data for 1 NBA season from 
    a sqlite database
    """
    try:
        conn = sqlite3.connect('NBAdraft/save_to_db/nba.db')
        df = pd.read_sql_table(f'pbp_{season}')
        
        print(f"Play-by-play data imported for the {season} season")
        print(df.shape)
        
    except IOError as e:
        print(e)
    finally:
        conn.close()
    
    return df

def parse(df):
    """ 
    Takes NBA play-by-play dataframe and returns one with plays
    parsed into several columns: 
    1. Primary player (or team)
    2. Primary action (made_FG, missed_FG, DREB, OREB, STL, TOV,
            shooting_foul, non_shooting_foul, made_FT, missed_FT,
            full_TO, player_sub)
    3. FG type (dunk, layup, hook shot, 2-pt jump shot,
            3-pt jump shot)
    4. FG distance (0 if rim)
    5. Secondary player (null except for foul drawers, assisters,
                      shot blockers, player leaving game for sub,)
    6. Secondary action (foul drawn, assist, block)
    """

    # we are just going to add 6 new columns to the existing db, one for each of these
    # 'Offensive rebound for Team' happens after any FT that isn't the last one; remove these

    pass

def main():
    import_from_sqlite('2014-2015') 

if __name__ == "__main__":
    main()
    
