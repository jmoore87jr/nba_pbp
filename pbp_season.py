import numpy as np 
import pandas as pd 
import time
from collections import defaultdict
from datetime import datetime, timedelta
from basketball_reference_scraper.pbp import get_pbp
import os
from sqlalchemy import create_engine
import psycopg2

# use basketball-reference-scraper to put NBA play-by-play data into Amazon RDS postgres database

DATABASE_ENDPOINT = 'airflow-database.ctuo1jcj04vs.us-east-2.rds.amazonaws.com'
PORT = '5432'
DATABASE_NAME = 'myDatabase'
USERNAME = 'jmoore87jr'
PASSWORD = 'johnny01'

# 2013 - new 

def team_to_abbr(year):
    """returns dictionary of full name: abbreviation"""
    """
    new jersey nets: 2010-11 -- 2011-12
    brooklyn nets: 2012-13 -- present
    charlotte bobcats: 2010-11 -- 2013-14
    charlotte hornets: 2014-15 -- present
    new orleans hornets: 2010-11 -- 2013-14
    new orleans pelicans: 2014-15 -- present
    """
    
    team_abbrs = ['MIL', 'TOR', 'BOS', 'IND', 'MIA', 'PHI',
                'BRK', 'ORL', 'CHO', 'WAS', 'CHI', 'NYK',
                'DET', 'ATL', 'CLE', 'LAL', 'LAC', 'DEN',
                'HOU', 'OKC', 'UTA', 'DAL', 'POR', 'MEM',
                'PHO', 'SAS', 'SAC', 'NOP', 'MIN', 'GSW',
                'NJN', 'NOH']
    team_names = ['Milwaukee Bucks', 'Toronto Raptors', 'Boston Celtics',
                'Indiana Pacers', 'Miami Heat', 'Philadelphia 76ers',
                'Brooklyn Nets', 'Orlando Magic', 'Charlotte Hornets',
                'Washington Wizards', 'Chicago Bulls', 'New York Knicks',
                'Detroit Pistons', 'Atlanta Hawks', 'Cleveland Cavaliers',
                'Los Angeles Lakers', 'Los Angeles Clippers', 'Denver Nuggets',
                'Houston Rockets', 'Oklahoma City Thunder', 'Utah Jazz',
                'Dallas Mavericks', 'Portland Trail Blazers', 'Memphis Grizzlies',
                'Phoenix Suns', 'San Antonio Spurs', 'Sacramento Kings',
                'New Orleans Pelicans', 'Minnesota Timberwolves', 'Golden State Warriors',
                'New Jersey Nets', 'New Orleans Hornets']
    
    d = {}
    for name,abbr in zip(team_names, team_abbrs):
        d[name] = abbr
    
    # change CHO up to 2014
    if year in ['2011', '2012', '2013']:
        d['Charlotte Bobcats'] = 'CHO'

    return d

def get_season_schedule(year, urltail, playoffs): # urltail is month at the end of url
    team_dict = team_to_abbr(year)
    dfs = pd.read_html('https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'.format(year, urltail))
    df = dfs[0][['Date', 'Visitor/Neutral', 'Home/Neutral']]
    
    playoffs_col = []
    for i,row in df.iterrows():
        # change playoffs to True when we hit the column
        if row['Date'] == "Playoffs":
            print("Playoffs starting...")
            playoffs = True
            continue
        if playoffs == False:
            playoffs_col.append("Regular")
        else:
            playoffs_col.append("Playoffs")

    # drop 'Playoff' row
    df = df[df['Date'] != "Playoffs"]

    # add new columns
    df['Game_Type'] = playoffs_col
    df['Date2'] = [ datetime.strptime(dt, '%a, %b %d, %Y').strftime('%Y-%m-%d') \
                   for dt in df['Date'] ]
    df['Visitor'] = [ team_dict[t] for t in df['Visitor/Neutral'].values ]
    df['Home'] = [ team_dict[t] for t in df['Home/Neutral'].values ]
    
    return [df[['Date2', 'Game_Type', 'Visitor', 'Home']], playoffs]

def scrape_and_clean_game(date, away, home, playoffs, year):
    # scrape game
    df = get_pbp(date, away, home)

    # change colnames and add new columns
    df.columns = ['quarter', 'time_remaining', 'away_action',
                'home_action', 'away_score', 'home_score']
    df.insert(0, 'home_team', [ home for i in range(len(df.index)) ])
    df.insert(0, 'away_team', [ away for i in range(len(df.index)) ])
    gameId = (date+away+home).replace('-', '') 
    df.insert(0, 'playoffs', [ playoffs for i in range(len(df.index)) ])
    df.insert(0, 'month', [ date[5:7] for i in range(len(df.index)) ])
    season = str(int(year) - 1) + "-" + year
    df.insert(0, 'season', [ season for i in range(len(df.index)) ])
    df.insert(0, 'date', [ date for i in range(len(df.index)) ])
    df.insert(0, 'gameId', [ gameId for i in range(len(df.index)) ])

    return df 

def save_to_rds(df, month):
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
            df.to_sql('pbp', con=engine, if_exists='append', chunksize=1000)
            conn.commit()
            print(f"Games from {month} saved to database")
        except:
            print(f"Error saving {month} to database")

        # close connection
        conn.close()
        print("Connection closed")

    except psycopg2.OperationalError as e:
        print(e)


if __name__ == "__main__":
    # loop through each game in each month and store in database
    years = ['2011', '2012', '2013', '2014',
             '2015', '2016', '2017', '2018', 
             '2019', '2020', '2021']
    months = ['october', 'october-2019', 'november', 'december', 'january', 
              'february', 'march', 'april', 'may', 'june', 'july', 'august',
              'september', 'october-2020']
    playoffs = False
    for year in years:
        for month in months:
            season = str(int(year) - 1) + "-" + year
            try:
                gss = get_season_schedule(year, month, playoffs)
            except:
                print(f"Page not found for {month} in {season}")
                continue
            schedule = gss[0]
            playoffs = gss[1] # makes playoffs = True if the last month ended with a playoff game
            print(f"Importing games for {month} {season}")
            df_month = pd.DataFrame()
            for i,row in schedule.iterrows(): 
                try:
                    df = scrape_and_clean_game(row.Date2, row.Visitor, row.Home, row.Game_Type, year)
                    if df_month.empty:
                        df_month = df
                    else:
                        df_month = pd.concat([df_month, df])
                    print(f"{row.Visitor} @ {row.Home} on {row.Date2} added".format(row.Visitor, row.Home, row.Date2))
                except:
                    print("No more games to import this month")
                    break
                # sleep; time on basketballreference.com/robots is 3 seconds
                time.sleep(3)

            # add game to database, creating one if it doesn't exist
            save_to_rds(df_month, month)

    print("Finished")
