import matplotlib 
import matplotlib.pyplot as plt
import numpy as np 
import pandas as pd 
import time
from collections import defaultdict
from datetime import datetime, timedelta
from basketball_reference_scraper.teams import get_roster, get_team_stats, get_opp_stats, get_roster_stats, get_team_misc
from basketball_reference_scraper.players import get_stats, get_game_logs, get_player_headshot
from basketball_reference_scraper.seasons import get_schedule, get_standings
from basketball_reference_scraper.box_scores import get_box_scores
from basketball_reference_scraper.pbp import get_pbp

matplotlib.use('TkAgg')

def plot_score_vs_time(pbp): # takes play-by-play dataframe
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
    # return the df to be plotted for each game
    city_away, city_home = pbp.columns[2].split('_')[0], pbp.columns[3].split('_')[0]
    df = pbp[['QUARTER', 'TIME_REMAINING', '{}_SCORE'.format(city_away), '{}_SCORE'.format(city_home)]]
    temp = pd.to_datetime(df['TIME_REMAINING']).dt.strftime('%H:%M').apply(lambda x: x.replace(':', ''))
    df.loc[:,'TIME_REMAINING'] = pd.to_numeric(temp).copy()

    time_dict = {1: 3600, 2: 2400, 3: 1200, 4: 0, 
                '1OT': -1200, '2OT': -2400, '3OT': -3600}

    game_time = []
    for i,row in df.iterrows():
        time_to_add = time_dict[row['QUARTER']]
        game_time.append((4800 - (row['TIME_REMAINING'] + time_to_add)) / 100)
    df.insert(2, 'MINUTES PLAYED', game_time)
    df = df[['MINUTES PLAYED', '{}_SCORE'.format(city_away), '{}_SCORE'.format(city_home)]].set_index('MINUTES PLAYED')
    df.columns = ['{}'.format(city_away), '{}'.format(city_home)]

    # create plot
    ax = df.plot.line()
    ax.set_ylabel("POINTS")
    ax.set_title("GAME FLOW")
    ax.set_xticks([0, 12, 24, 36, 48])
    ax.grid('on')
    plot_title = f'{yesterday}_{city_away}v{city_home}.png'
    plt.savefig(f'game_flow_charts/{plot_title}')

    print(f"{plot_title} game flow saved")

    #return df 



def count_shots_at_rim(pbp, away, home):
    city_away, city_home = pbp.columns[2].split('_')[0], pbp.columns[3].split('_')[0]
    away_attempts_at_rim = len(pbp[pbp[f'{city_away}_ACTION'].str.contains("dunk")].index) + \
                           len(pbp[pbp[f'{city_away}_ACTION'].str.contains("layup")].index)
    home_attempts_at_rim = len(pbp[pbp[f'{city_home}_ACTION'].str.contains("dunk")].index) + \
                           len(pbp[pbp[f'{city_home}_ACTION'].str.contains("layup")].index)

    away_makes_at_rim = len(pbp[pbp[f'{city_away}_ACTION'].str.contains("makes 2-pt dunk")].index) + \
                        len(pbp[pbp[f'{city_away}_ACTION'].str.contains("makes 2-pt layup")].index)
    home_makes_at_rim = len(pbp[pbp[f'{city_home}_ACTION'].str.contains("makes 2-pt dunk")].index) + \
                        len(pbp[pbp[f'{city_home}_ACTION'].str.contains("makes 2-pt layup")].index)
    away_pct = round((away_makes_at_rim/away_attempts_at_rim)*100,1)
    home_pct = round((home_makes_at_rim/home_attempts_at_rim)*100,1)
    away_dunks = len(pbp[pbp[f'{city_away}_ACTION'].str.contains("makes 2-pt dunk")].index)
    home_dunks = len(pbp[pbp[f'{city_home}_ACTION'].str.contains("makes 2-pt dunk")].index)

    s1 = f"{away} shot {away_makes_at_rim} for {away_attempts_at_rim} ({away_pct}%) at the rim including {away_dunks} dunks \n"
    s2 = f"{home} shot {home_makes_at_rim} for {home_attempts_at_rim} ({home_pct}%) at the rim including {home_dunks} dunks \n"
    print(s1)
    print(s2)


    return [away_makes_at_rim, away_attempts_at_rim, away_pct, away_dunks,
            home_makes_at_rim, home_attempts_at_rim, home_pct, home_dunks, s1, s2]

def count_possessions(pbp):
    """Counts true number of possessions in a game. Possessions are only 
    ended in 5 ways:
    1. FG make
    2. Made final FT
      - Make sure not to double count for and-1
    3. Missed final FT + DReb
    4. Missed FG + DReb
    5. Turnover"""

    poss = []
    for t in range(2):
        if t == 0:
            city1, city2 = pbp.columns[2].split('_')[0], pbp.columns[3].split('_')[0]
        else:
            city2, city1 = pbp.columns[2].split('_')[0], pbp.columns[3].split('_')[0]
        actioncolname1, actioncolname2 = f'{city1}_ACTION', f'{city2}_ACTION'
        
        # count1: FG makes
        made_fgs = pbp[actioncolname1][(pbp[actioncolname1].str.contains("makes") == True) & (~pbp[actioncolname1].str.contains("free throw"))]
        count1 = len(made_fgs)

        # count2: final made FTs
        # subtract 'made 1 of 1 free throws' if they follow a made FG
        and1s = 0
        made_final_ft1 = pbp[actioncolname1][(pbp[actioncolname1].str.contains("makes free throw")) & (pbp[actioncolname1].str.count('1') == 2)]
        for i in made_final_ft1.index: # for and-1s
            previous_fg = pbp[actioncolname1].iloc[i-2]
            if "makes" in previous_fg:
                and1s += 1
        made_final_ft2 = pbp[actioncolname1][(pbp[actioncolname1].str.contains("makes free throw")) & (pbp[actioncolname1].str.count('2') == 2)]
        made_final_ft3 = pbp[actioncolname1][(pbp[actioncolname1].str.contains("makes free throw")) & (pbp[actioncolname1].str.count('3') == 2)]
        count2 = len(made_final_ft1) + len(made_final_ft2) + len(made_final_ft3) - and1s

        # count3: missed final FT + DReb
        count3 = 0
        missed_final_fts = pbp[actioncolname1][(pbp[actioncolname1].str.contains("misses free throw")) & (pbp[actioncolname1].str.count('1') == 2)]
        for i in missed_final_fts.index:
            next_def_play = pbp[actioncolname2].iloc[i+1]
            if "rebound" in next_def_play:
                count3 += 1

        # count 4: missed FGs that result in DReb
        count4 = 0
        missed_fgs = pbp[actioncolname1][(pbp[actioncolname1].str.contains("misses") == True) & (~pbp[actioncolname1].str.contains("free throw"))]
        for i in missed_fgs.index:
            next_def_play = pbp[actioncolname2].iloc[i+1]
            if "rebound" in next_def_play:
                count4 += 1

        # count 5: turnovers
        tovs = pbp[actioncolname1][pbp[actioncolname1].str.contains("Turnover") == True]
        count5 = len(tovs)
        
        possessions = sum([count1, count2, count3, count4, count5])
        poss.append(possessions)

        #print("Possessions for {}: {}".format(city1, possessions))
    
    
    return poss
