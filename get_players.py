import pandas as pd 

"""
get list of players from <season> and their teams
"""

SEASON = '2021'

def abbr(ser):
    abbrs = []
    for name in ser.items():
        l = name[1].split()
        abbr = l[0][0] + ". " + l[1]
        abbrs.append(abbr)
    
    return abbrs

def main():
    df = pd.read_html(f'https://www.basketball-reference.com/leagues/NBA_{SEASON}_totals.html')[0]

    # trim df to just players and teams, and remove the rows of headers
    df = df[['Tm', 'Player']][df['Player'] != "Player"]

    # get list of abbreviations (F. Last) that correspond to the play-by-play data
    fullnames = abbr(df['Player'])

    # modify dataframe
    df.insert(0, 'Name', fullnames)
    df.columns = ['abbr_name', 'team', 'full_name']

    # remove accents
    df['abbr_name'] = df['abbr_name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df['full_name'] = df['full_name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    # save csv
    df.to_csv(f'players_{SEASON}.csv')

    print("Players saved")


if __name__ == "__main__":
    main()