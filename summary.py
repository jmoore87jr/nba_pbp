import pandas as pd 
from basketball_reference_scraper.pbp import get_pbp
from pbp_analysis import count_possessions, count_shots_at_rim, plot_score_vs_time
from pbp_yesterday import get_season_schedule
from box_scores import get_box
from datetime import datetime, timedelta
import time
import matplotlib.pyplot as plt

"""Summary info I want:
Game score
Possessions (get from pbp)
** Closing line (get from sportsbookreview)
Dunks attempted / made (get from pbp)
Shots at rim attempted / made (get from pbp)
3 pointers attempted / made
Free throws attempted / made
Top scorer on each team
Player with most pts + reb + ast
Rebounds including offensive and high rebounder
Assists including high assister
Game flow line chart
** Link to highlights
Players who were out
Overtime?

s1, s2, s3, etc. are strings to be appended to the summary text file at the bottom of each function"""

def cut_players_out(df_away_raw, df_home_raw, away, home):
    s1 = f"***{away} vs. {home}*** \n"
    print(s1)
    # players who were out and then cut them from db
    away_players_out = list(df_away_raw['PLAYER'][(df_away_raw['MP'] == 'Not With Team') | (df_away_raw['MP'] == 'Did Not Play') | (df_away_raw['MP'] == 'Did Not Dress')].values)
    home_players_out = list(df_home_raw['PLAYER'][(df_home_raw['MP'] == 'Not With Team') | (df_home_raw['MP'] == 'Did Not Play') | (df_home_raw['MP'] == 'Did Not Dress')].values)
    if away_players_out:
        s2 = f"{away} was missing: {', '.join(away_players_out)} \n"
        print(s2)
    else:
        s2 = f"{away} had their full roster available for this game \n"
        print(s2)
    if home_players_out:
        s3 = f"{home} was missing: {', '.join(home_players_out)} \n"
        print(s3)
    else:
        s3 = f"{home} had their full roster available for this game \n"
        print(s3)

    df_away = df_away_raw[~df_away_raw['PLAYER'].isin(away_players_out)]
    df_home = df_home_raw[~df_home_raw['PLAYER'].isin(home_players_out)]

    # write to text file
    write_to_txt([s1, s2, s3])

    return [df_away, df_home]

def cols_to_numeric(dfs):
    df_away = dfs[0]
    df_home = dfs[1]
    df_away.loc[:, ('FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'TRB', 'AST', 'PTS')] = \
    df_away.loc[:, ('FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'TRB', 'AST', 'PTS')].apply(pd.to_numeric).copy()

    df_home.loc[:, ('FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'TRB', 'AST', 'PTS')] = \
    df_home.loc[:, ('FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'TRB', 'AST', 'PTS')].apply(pd.to_numeric).copy()

    return [df_away, df_home]

def print_team_scoring_and_top_performers(df_away, df_home, away, home):
    # team score
    away_score = df_away['PTS'].iloc[-1]
    home_score = df_home['PTS'].iloc[-1]
    # FG%
    fgp_away = df_away.iloc[-1, 4]
    fgp_home = df_home.iloc[-1, 4]
    fga_away = df_away.iloc[-1, 3]
    fga_home = df_home.iloc[-1, 3]
    fgm_away = df_away.iloc[-1, 2]
    fgm_home = df_home.iloc[-1, 2]

    # top scorer and top pts + reb + ast performer
    away_high_box, home_high_box = 0, 0
    away_high_score, home_high_score = 0, 0
    for i,row in df_away.iterrows(): 
        if (row.PTS > away_high_score) & (row.PLAYER != "Team Totals"):
            away_high_score = row.PTS
            away_top_scorer_pts = row.PTS
            away_top_scorer = row.PLAYER
            away_top_scorer_reb = row.TRB
            away_top_scorer_ast = row.AST
    for i,row in df_away.iterrows():
        player_box_total = row.PTS + row.TRB + row.AST
        if (player_box_total > away_high_box) & (row.PLAYER != "Team Totals") & (row.PLAYER != away_top_scorer):
            away_high_box = player_box_total
            away_high_box_player = row.PLAYER
            away_high_box_pts = row.PTS
            away_high_box_reb = row.TRB
            away_high_box_ast = row.AST
    for i,row in df_home.iterrows(): 
        if (row.PTS > home_high_score) & (row.PLAYER != "Team Totals"):
            home_high_score = row.PTS
            home_top_scorer_pts = row.PTS
            home_top_scorer = row.PLAYER
            home_top_scorer_reb = row.TRB
            home_top_scorer_ast = row.AST
    for i,row in df_home.iterrows(): 
        player_box_total = row.PTS + row.TRB + row.AST
        if (player_box_total > home_high_box) & (row.PLAYER != "Team Totals") & (row.PLAYER != home_top_scorer):
            home_high_box = player_box_total
            home_high_box_player = row.PLAYER
            home_high_box_pts = row.PTS
            home_high_box_reb = row.TRB
            home_high_box_ast = row.AST

    # TODO: add attempts and makes for each team FG
    if away_score > home_score:
        s1 = f"{away} defeated {home} {away_score} - {home_score} \n"
        s2 = f"{away} shot {fgm_away} of {fga_away} ({round(fgp_away*100, 1)})% from the field. {home} shot {fgm_home} of {fga_home} ({round(fgp_home*100, 1)})% from the field \n"
        s3 = f"{away} was lead by {away_top_scorer} ({away_top_scorer_pts} points, {away_top_scorer_reb} rebounds, {away_top_scorer_ast} assists) and {away_high_box_player} ({away_high_box_pts} points, {away_high_box_reb} rebounds, {away_high_box_ast} assists) \n"
        s4 = f"{home} was lead by {home_top_scorer} ({home_top_scorer_pts} points, {home_top_scorer_reb} rebounds, {home_top_scorer_ast} assists) and {home_high_box_player} ({home_high_box_pts} points, {home_high_box_reb} rebounds, {home_high_box_ast} assists) \n"
        print(s1)
        print(s2)
        print(s3)
        print(s4)

    else:
        s1 = f"{home} defeated {away} {home_score} - {away_score} \n"
        s2 = f"{home} shot {fgm_home} of {fga_home} ({round(fgp_home*100, 1)})% from the field. {away} shot {fgm_away} of {fga_away} ({round(fgp_away*100, 1)})% from the field \n"
        s3 = f"{home} was lead by {home_top_scorer} ({home_top_scorer_pts} points, {home_top_scorer_reb} rebounds, {home_top_scorer_ast} assists) and {home_high_box_player} ({home_high_box_pts} points, {home_high_box_reb} rebounds, {home_high_box_ast} assists) \n"
        s4 = f"{away} was lead by {away_top_scorer} ({away_top_scorer_pts} points, {away_top_scorer_reb} rebounds, {away_top_scorer_ast} assists) and {away_high_box_player} ({away_high_box_pts} points, {away_high_box_reb} rebounds, {away_high_box_ast} assists) \n"
        print(s1)
        print(s2)
        print(s3)
        print(s4)
    
    # write to text file
    write_to_txt([s1, s2, s3, s4])


def print_free_throws(df_away, df_home, away, home):
    fta_away, ftm_away = df_away.iloc[-1, 9], df_away.iloc[-1, 8] 
    fta_home, ftm_home = df_home.iloc[-1, 9], df_home.iloc[-1, 8]
    s1 = f"{away} shot {ftm_away} for {fta_away} ({round((ftm_away/fta_away)*100, 1)}%) from the free throw line \n"
    s2 = f"{home} shot {ftm_home} for {fta_home} ({round((ftm_home/fta_home)*100, 1)}%) from the free throw line \n"
    print(s1)
    print(s2)
    
    # write to text file
    write_to_txt([s1, s2])

def print_three_pointers(df_away, df_home, away, home):
    tpa_away, tpm_away = df_away.iloc[-1, 6], df_away.iloc[-1, 5] 
    tpa_home, tpm_home = df_home.iloc[-1, 6], df_home.iloc[-1, 5]
    s1 = f"{away} shot {tpm_away} for {tpa_away} ({round((tpm_away/tpa_away)*100, 1)}%) from the 3-point line \n"
    s2 = f"{home} shot {tpm_home} for {tpa_home} ({round((tpm_home/tpa_home)*100, 1)}%) from the 3-point line \n"
    print(s1)
    print(s2)

    # write to text file
    write_to_txt([s1, s2])

def print_rebounding(df_away, df_home, away, home):
    oreb_away, dreb_away, treb_away = df_away.iloc[-1, 11], df_away.iloc[-1, 12], df_away.iloc[-1,13]
    oreb_home, dreb_home, treb_home = df_home.iloc[-1, 11], df_home.iloc[-1, 12], df_home.iloc[-1,13]

    if treb_away > treb_home:
        s1 = f"{away} out-rebounded {home} {treb_away} ({oreb_away} offensive) - {treb_home} ({oreb_home} offenseve) \n"
    elif treb_home > treb_away:
        s1 = f"{home} out-rebounded {away} {treb_home} ({oreb_home} offensive) - {treb_away} ({oreb_away} offenseve) \n"
    else:
        s1 = f"Both teams had {treb_home} rebounds. {away} had {oreb_away} offensive rebounds and {home} had {oreb_home} offensive rebounds \n"
    print(s1)

    # write to text file
    strings = [s1]
    write_to_txt(strings)

def print_possessions_and_ot(pbp):
    overtime = False
    if len(set(pbp['QUARTER'].values)) > 4:
        overtime = True
        s1 = "This game went to overtime \n"
    else:
        s1 = "This game did not go to overtime \n"
    print(s1)
    possessions = count_possessions(pbp)
    pace = (possessions[0] + possessions[1]) / 2
    if not overtime:
        if pace < 97.83:
            s2 = f"This was a slow paced game with a true pace of {pace} \n"
            print(s2)
        elif pace < 100.77:
            s2 = f"This was an average paced game with a true pace of {pace} \n"
            print(s2)
        else:
            s2 = f"This was a fast paced game with a true pace of {pace} \n"
            print(s2)
    else:
        s2 = "Game went to overtime; pace not calculated \n"
    print(s2)

    # write to text file
    write_to_txt([s1, s2])

def print_closing_line():
    pass 

def write_to_txt(strings):
    filename = "game_summaries/" + (datetime.now() - timedelta(1)).strftime('%Y%m%d') + "_summary.txt"
    with open(filename, "a") as text_file:
        for s in strings:
            text_file.write(s)

def send_email(addresses):
    pass

def send_tweet():
    pass

def main():
    """Generate a summary email for last night's NBA games"""

    # loop through last night's games
    yest_month = (datetime.now() - timedelta(1)).strftime('%B').lower()
    schedule = get_season_schedule(yest_month)
    # save df to create each plot 
    plot_dfs = []
    plot_titles = []
    for i,row in schedule.iterrows(): 
        if row.Date2 == (datetime.now() - timedelta(1)).strftime('%Y-%m-%d'):
            date = row.Date2
            away = row.Visitor 
            home = row.Home
            pbp = get_pbp(date, away, home).fillna('no action')
            dfs_box = get_box(date, away, home) # returns list of [away_df, home_df]
            df_away_raw = dfs_box[0]
            df_home_raw = dfs_box[1]

            # cut 'out' players and change columns to numeric
            trimmed_dfs = cols_to_numeric(cut_players_out(df_away_raw, df_home_raw, away, home))
            df_away = trimmed_dfs[0]
            df_home = trimmed_dfs[1]

            # print team scores and top performers
            print_team_scoring_and_top_performers(df_away, df_home, away, home)

            # free throws
            print_free_throws(df_away, df_home, away, home)

            # 3 pointers
            print_three_pointers(df_away, df_home, away, home)

            # shots at rim
            shots_at_rim = count_shots_at_rim(pbp, away, home)
            write_to_txt([shots_at_rim[8], shots_at_rim[9]])

            # rebounds
            print_rebounding(df_away, df_home, away, home)

            # possessions and OT
            print_possessions_and_ot(pbp)

            # add link to highlights
            url = "https://www.youtube.com/playlist?list=PLlVlyGVtvuVkIjURb-twQc1lsE4rT1wfJ"
            write_to_txt([f"Game highlights here: {url} \n"])

            # save plot df for current game
            plot_dfs.append(plot_score_vs_time(pbp))
            plot_titles.append(f"{away} v {home}")


            print("Sleeping for 3 seconds... \n")
            print("\n")
            time.sleep(3)

        # space between games in the text file
        write_to_txt(["\n"])
    

    """
    # plot score vs. time for each game
    # fig, axs = plt.subplots(x,y) [2,2] for 4, [3,2] for 6, [3,3] for 9 etc.
    num_games = len(plot_dfs)
    print(f"num_games: {num_games}")
    x, y = 0, 0
    x_len = int((num_games - 0.01)**0.5) + 1
    y_len = int((num_games - x_len) / (x_len + 0.01)) + 2
    print(f"x_len: {x_len}")
    print(f"y_len: {y_len}")
    fig, axs = plt.subplots(x_len, y_len)
    for df, title in zip(plot_dfs, plot_titles):
        x1 = df.iloc[:,0]
        x2 = df.iloc[:,1]
        _y = df.index
        axs[x,y].plot(x1, _y)
        axs[x,y].plot(x2, _y)
        axs[x,y].set_ylabel("Points")
        axs[x,y].set_title(title)
        axs[x,y].set_xlabel("Minutes played")
        axs[x,y].set_xticks([0, 12, 24, 36, 48])
        axs[x,y].grid('on')
        if x == x_len - 1:
            x = 0
            y += 1 
        else:
            x += 1
    fig.tight_layout()
    plt.savefig(f'game_flow_charts/gameflows_{date}.png')
    print("Game flows saved")
    """
    print("Finished")

if __name__ == "__main__":
    main()

