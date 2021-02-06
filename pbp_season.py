import numpy as np 
import pandas as pd 
import time
from collections import defaultdict
from datetime import datetime, timedelta
from basketball_reference_scraper.pbp import get_pbp
import os
from sqlalchemy import create_engine
import psycopg2

# use basketball-reference-scraper to put NBA play-by-play data into a database called 'playbyplay.db'

DATABASE_ENDPOINT = 'airflow-database.ctuo1jcj04vs.us-east-2.rds.amazonaws.com'
PORT = '5432'
DATABASE_NAME = 'myDatabase'
USERNAME = 'jmoore87jr'
PASSWORD = 'johnny01'

def team_to_abbr():
    """returns dictionary of full name: abbreviation"""
    team_abbrs = ['MIL', 'TOR', 'BOS', 'IND', 'MIA', 'PHI',
                'BRK', 'ORL', 'CHO', 'WAS', 'CHI', 'NYK',
                'DET', 'ATL', 'CLE', 'LAL', 'LAC', 'DEN',
                'HOU', 'OKC', 'UTA', 'DAL', 'POR', 'MEM',
                'PHO', 'SAS', 'SAC', 'NOP', 'MIN', 'GSW']
    team_names = ['Milwaukee Bucks', 'Toronto Raptors', 'Boston Celtics',
                'Indiana Pacers', 'Miami Heat', 'Philadelphia 76ers',
                'Brooklyn Nets', 'Orlando Magic', 'Charlotte Hornets',
                'Washington Wizards', 'Chicago Bulls', 'New York Knicks',
                'Detroit Pistons', 'Atlanta Hawks', 'Cleveland Cavaliers',
                'Los Angeles Lakers', 'Los Angeles Clippers', 'Denver Nuggets',
                'Houston Rockets', 'Oklahoma City Thunder', 'Utah Jazz',
                'Dallas Mavericks', 'Portland Trail Blazers', 'Memphis Grizzlies',
                'Phoenix Suns', 'San Antonio Spurs', 'Sacramento Kings',
                'New Orleans Pelicans', 'Minnesota Timberwolves', 'Golden State Warriors']
    d = {}
    for name,abbr in zip(team_names, team_abbrs):
        d[name] = abbr

    return d

def get_season_schedule(urltail): # urltail is month at the end of url
    team_dict = team_to_abbr()
    dfs = pd.read_html('https://www.basketball-reference.com/leagues/NBA_2021_games-{}.html'.format(urltail))
    df = dfs[0][['Date', 'Visitor/Neutral', 'Home/Neutral']]
    #print("first shape: {}".format(df.shape))
    # identify end of regular season
    i = df.index[df['Date'].str.contains("Playoffs")]
    if i.empty:
        print("No playoff games this month.")
    else:
        print("Playoffs start at row {}.".format(i[0]))
        df = df.truncate(after=i[0]-1)
        print("Dataframe truncated; regular season ends during this month.")
    df['Date2'] = [ datetime.strptime(dt, '%a, %b %d, %Y').strftime('%Y-%m-%d') \
                   for dt in df['Date'] ]
    df['Visitor'] = [ team_dict[t] for t in df['Visitor/Neutral'].values ]
    df['Home'] = [ team_dict[t] for t in df['Home/Neutral'].values ]

    #print(df[['Date2', 'Visitor', 'Home']])
    
    return df[['Date2', 'Visitor', 'Home']]

def scrape_and_clean_game(date, away, home):
    # scrape game
    df = get_pbp(date, away, home)

    # add date + Id and change column names
    df.columns = ['quarter', 'time_remaining', 'away_action',
                'home_action', 'away_score', 'home_score']
    df.insert(0, 'home_team', [ home for i in range(len(df.index)) ])
    df.insert(0, 'away_team', [ away for i in range(len(df.index)) ])
    gameId = (date[5:]+away+home).replace('-', '') 
    df.insert(0, 'gameId', [ gameId for i in range(len(df.index)) ])
    df.insert(0, 'date', [ date for i in range(len(df.index)) ])

    print(df.shape)
    print(df.columns)
    print(df.head(10))

    return df 

def save_to_rds(df):
    try:
        # connect to database
        conn = psycopg2.connect(
            host=DATABASE_ENDPOINT,
            database=DATABASE_NAME,
            user=USERNAME,
            password=PASSWORD,
            port=PORT
        )
        print("Connected to database")

        # create engine
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(USERNAME, PASSWORD, DATABASE_ENDPOINT, PORT, DATABASE_NAME))

        try:
            # insert data and commit to database
            df.to_sql('play_by_play', con=engine, if_exists='append', chunksize=1000)
            conn.commit()
            print("Added {} @ {} on {} to database.".format(row.Visitor, row.Home, row.Date2))
        except:
            print("{} @ {} on {} skipped due to overtime.".format(row.Visitor, row.Home, row.Date2))

        # close connection
        conn.close()
        #cursor.close()
        print("Connection closed")

    except psycopg2.OperationalError as e:
        print(e)

if __name__ == "__main__":
    # loop through each game in each month and store in database
    months = ['december', 'january']
    for month in months:
        schedule = get_season_schedule(month)
        print("Importing games for {}".format(month))
        for i,row in schedule.iterrows(): 
            try:
                df = scrape_and_clean_game(row.Date2, row.Visitor, row.Home)
            except:
                print("No more games to import this month.")
                break
            # add game to database, creating one if it doesn't exist
            save_to_rds(df)
            # sleep; time on basketballreference.com/robots is 3 seconds
            print("Sleeping for 3 seconds...")
            time.sleep(3)
    print("Finished.")

