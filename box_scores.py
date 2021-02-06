import numpy as np 
import pandas as pd 
import time
from collections import defaultdict
from datetime import datetime, timedelta
from basketball_reference_scraper.box_scores import get_box_scores

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

def get_box(date, away, home):
    d = get_box_scores(date, home, away)
    df_away = d[away]
    df_home = d[home]

    # ALL THIS STUFF IS FOR ADDING TO DB, NOT SUMMARY EMAIL
    # add team, gameId, and date columns
    # change this to make unique for each row
    #gameId = (date+away+home).replace('-', '') 
    #df_away.insert(0, 'team', [ away for i in range(len(df_away.index)) ])
    #df_away.insert(0, 'gameId', [ gameId for i in range(len(df_away.index)) ])
    #df_away.insert(0, 'date', [ date for i in range(len(df_away.index)) ])
    #df_home.insert(0, 'team', [ home for i in range(len(df_home.index)) ])
    #df_home.insert(0, 'gameId', [ gameId for i in range(len(df_home.index)) ])
    #df_home.insert(0, 'date', [ date for i in range(len(df_home.index)) ])

    return [df_away, df_home]

def main():
    get_box('2021-01-27', 'BRK', 'ATL')

if __name__ == "__main__":
    main()


