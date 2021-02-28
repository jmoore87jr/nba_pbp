import pandas as pd 

# TODO: players.json file converting "J. Wiseman" to "James Wiseman"
# TODO: add Primary Team and Secondary Team to output of each function

"""
A list of words (one line of away_action or home_action)
gets classified as a type (made fg, missed fg, made_ft, etc...)
and fed into one of these functions to turn it into a pandas dataframe row

Fuctions:

made_shot, missed_shot, rebound, turnover, foul, player_sub, timeout


Columns: 

1. primary_player (or team)
2. primary_action (made shot, missed shot, 
                   rebound, turnover, foul,
                   sub in, timeout)
3. description (dunk, layup, hook shot, jump shot,
                free throw, technical free throw)
4. shot_dist (0 if rim)
5. shot_value
6. secondary_player 
7. secondary_action (foul drawn, assist, block, 
                     steal, sub out)
"""

def made_shot(l: list) -> pd.DataFrame():
    """
    made shot (ast):
    1. ['J.', 'Wiseman', 'makes', '2-pt', 'dunk', 'from', '1', 'ft', '(assist', 'by', 'A.', 'Wiggins)']
    2. ['S.', 'Curry', 'makes', '2-pt', 'layup', 'from', '4', 'ft', '(assist', 'by', 'K.', 'Oubre)']
    3. ['K.', 'Bazemore', 'makes', '2-pt', 'layup', 'at', 'rim']
    4. ['S.', 'Curry', 'makes', '2-pt', 'jump', 'shot', 'from', '23', 'ft']
    5. ['A.', 'Wiggins', 'makes', '3-pt', 'jump', 'shot', 'from', '26', 'ft', '(assist', 'by', 'S.', 'Curry)']
    6. ['J.', 'Wiseman', 'makes', 'free', 'throw', '1', 'of', '2']
    """
    
    # 1. primary player
    # TODO: use players.json here
    primary_player = ' '.join([l[0],l[1]])
    #print(f"Primary player: {primary_player}")

    # 2. primary action
    primary_action = "made shot"
    #print(f"Primary action: {primary_action}")

    # 3. fg type
    if "dunk" in l:
        description = "dunk"
    elif "layup" in l:
        description = "layup"
    elif "hook" in l:
        description = "hook shot"
    elif "jump" in l:
        description = "jump shot"
    elif "technical" in l:
        description = "technical free throw"
    elif "free" in l:
        description = "free throw"
    else:
        description = "UNKNOWN"
    #print(f"Field goal type: {description}")

    # 4. fg distance in feet
    if "rim" in l:
        shot_dist = 0
    elif "ft" in l:
        shot_dist = l[l.index("ft") - 1]
    else: 
        shot_dist = None
    #print(f"Field goal distance: {shot_dist}")

    # 5. shot value
    if "2-pt" in l:
        shot_value = 2
    elif "3-pt" in l:
        shot_value = 3
    else:
        shot_value = 1
    #print(f"Shot value: {shot_value}")

    # 6,7. secondary player & secondary action
    # TODO: use players.json here
    if ("(" in l[-4]) and ("." in l[-2]):
        secondary_player = ' '.join([l[-2], l[-1][:-1]])
        secondary_action = l[-4][1:]
        #print(f"Secondary player: {secondary_player}")
        #print(f"Secondary action: {secondary_action}")
    else:
        secondary_player = None
        secondary_action = None

    return [primary_player, primary_action, description, shot_dist,
                shot_value, secondary_player, secondary_action]

"""
l = ['A.', 'Wiggins', 'makes', '3-pt', 'jump', 'shot', 'from', '26', 'ft', '(assist', 'by', 'S.', 'Curry)']
made_shot(l)

l = ['J.', 'Wiseman', 'makes', 'free', 'throw', '1', 'of', '2']
made_shot(l)
"""

def missed_shot(l: list) -> pd.DataFrame():
    """
    missed fg (blk):
    ['E.', 'Paschall', 'misses', '2-pt', 'layup', 'from', '3', 'ft', '(block', 'by', 'D.', 'Jordan)']
    ['K.', 'Oubre', 'misses', '3-pt', 'jump', 'shot', 'from', '23', 'ft']
    ['J.', 'Wiseman', 'makes', '2-pt', 'dunk', 'from', '1', 'ft']
    ['J.', 'Wiseman', 'makes', 'free', 'throw', '1', 'of', '2']
    """

    # 1. primary player
    # TODO: use players.json here
    primary_player = ' '.join([l[0],l[1]])
    #print(f"Primary player: {primary_player}")

    # 2. primary action
    primary_action = "missed shot"
    #print(f"Primary action: {primary_action}")

    # 3. fg type
    if "dunk" in l:
        description = "dunk"
    elif "layup" in l:
        description = "layup"
    elif "hook" in l:
        description = "hook shot"
    elif "jump" in l:
        description = "jump shot"
    elif "technical" in l:
        description = "technical free throw"
    elif "free" in l:
        description = "free throw"
    else:
        description = "UNKNOWN"
    #print(f"Field goal type: {description}")

    # 4. fg distance in feet
    if "rim" in l:
        shot_dist = 0
    elif "ft" in l:
        shot_dist = l[l.index("ft") - 1]
    else: 
        shot_dist = None
    #print(f"Field goal distance: {shot_dist}")

    # 5. points scored
    if "2-pt" in l:
        shot_value = 2
    elif "3-pt" in l:
        shot_value = 3
    else:
        shot_value = 1
    #print(f"Points scored: {shot_value}")

    # 6,7. secondary player & secondary action
    # TODO: use players.json here
    if ("(" in l[-4]) and ("." in l[-2]):
        secondary_player = ' '.join([l[-2], l[-1][:-1]])
        secondary_action = l[-4][1:]
        #print(f"Secondary player: {secondary_player}")
        #print(f"Secondary action: {secondary_action}")
    else:
        secondary_player = None
        secondary_action = None
    
    return [primary_player, primary_action, description, shot_dist,
                shot_value, secondary_player, secondary_action]

"""
l = ['E.', 'Paschall', 'misses', '2-pt', 'layup', 'from', '3', 'ft', '(block', 'by', 'D.', 'Jordan)']
missed_shot(l)

l = ['B.', 'Bogdanović', 'misses', 'technical', 'free', 'throw']
missed_shot(l)
"""

def rebound(l: list) -> pd.DataFrame():
    """
    rebound:
    ['Offensive', 'rebound', 'by', 'E.', 'Paschall']
    ['Defensive', 'rebound', 'by', 'J.', 'Wiseman']
    """
    # 1. primary player
    primary_player = ' '.join([l[-2], l[-1]])

    # 2. primary action
    primary_action = "rebound"

    if l[0] == "Offensive":
        description = "offensive" 
    else:
        description = "defensive"

    shot_dist = None 
    shot_value = None 
    secondary_player = None 
    secondary_action = None 

    return [primary_player, primary_action, description, shot_dist,
                shot_value, secondary_player, secondary_action]

"""
l = ['Offensive', 'rebound', 'by', 'E.', 'Paschall']
rebound(l)

l = ['Defensive', 'rebound', 'by', 'J.', 'Wiseman']
rebound(l)
"""


def turnover(l: list) -> pd.DataFrame():
    """
    tov (stl, non_shooting_foul):
    ['Turnover', 'by', 'K.', 'Oubre', '(offensive', 'foul)']
    ['Turnover', 'by', 'K.', 'Looney', '(bad', 'pass;', 'steal', 'by', 'J.', 'Allen)']
    ['Turnover', 'by', 'A.', 'Wiggins', '(traveling)']
    ['Turnover', 'by', 'S.', 'Curry', '(out', 'of', 'bounds', 'lost', 'ball)']
    ['Turnover', 'by', 'K.', 'Looney', '(lost', 'ball;', 'steal', 'by', 'D.', 'Jordan)']
    ['Turnover', 'by', 'L.', 'Kennard', '(step', 'out', 'of', 'bounds)']
    ['Turnover', 'by', 'P.', 'George', '(bad', 'pass)']
    -offensive
    -bad pass (some w steal)
    -traveling
    -out of bounds
    -lost ball (some w steal)
    """
    # 1. Primary player
    primary_player = ' '.join([l[2],l[3]])

    # 2. Primary action
    primary_action = "turnover"

    # 3. Description
    if "(offensive" in l:
        description = "offensive"
    elif "(traveling)" in l:
        description = "traveling"
    elif "(out" in l:
        description = "out of bounds"
    elif "(bad" in l:
        description = "bad pass"
    elif "(lost" in l:
        description = "lost ball"
    else:
        description = "UNKNOWN"

    # 4. Shot distance
    shot_dist = None

    # 5. Shot value
    shot_value = None 

    # 6,7. Secondary player, secondary action
    if l.count("by") > 1:
        secondary_player = ' '.join([l[-2], l[-1][:-1]])
        secondary_action = l[-4]
    else:
        secondary_player = None
        secondary_action = None
    
    return [primary_player, primary_action, description, shot_dist,
                shot_value, secondary_player, secondary_action]

"""
l = ['Turnover', 'by', 'K.', 'Oubre', '(offensive', 'foul)']
turnover(l)

l = ['Turnover', 'by', 'K.', 'Looney', '(bad', 'pass;', 'steal', 'by', 'J.', 'Allen)']
turnover(l)

l = ['Turnover', 'by', 'A.', 'Wiggins', '(traveling)']
turnover(l)

l = ['Turnover', 'by', 'S.', 'Curry', '(out', 'of', 'bounds', 'lost', 'ball)']
turnover(l)

l = ['Turnover', 'by', 'K.', 'Looney', '(lost', 'ball;', 'steal', 'by', 'D.', 'Jordan)']
turnover(l)
"""
    

def foul(l: list) -> pd.DataFrame():
    """ 
    shooting_foul:
    ['Shooting', 'foul', 'by', 'K.', 'Irving', '(drawn', 'by', 'S.', 'Curry)']
    ['Technical', 'foul', 'by', 'B.', 'Biyombo']
    non_shooting_foul (tov):
    ['Personal', 'foul', 'by', 'L.', 'Shamet', '(drawn', 'by', 'E.', 'Paschall)']
    ['Offensive', 'foul', 'by', 'K.', 'Oubre', '(drawn', 'by', 'J.', 'Harris)'] (also counts as turnover)
    ['Loose', 'ball', 'foul', 'by', 'J.', 'Toscano-Anderson', '(drawn', 'by', 'D.', 'Jordan)']
    ['Def', '3', 'sec', 'tech', 'foul', 'by', 'Team']
    ['Violation', 'by', 'Team', '(def', 'goaltending)']
    ['Violation', 'by', 'Team', '(off', 'goaltending)']
    ['Away', 'from', 'play', 'foul', 'by', 'C.', 'Reddish', '(drawn', 'by', 'C.', 'White)']
    ['Flagrant', 'foul', 'type', '1', 'by', 'B.', 'Forbes', '(drawn', 'by', 'R.', 'Barrett)']
    """
    # 1. Primary player
    if (l[0] == 'Def') or (l[0] == 'Violation'):
        primary_player = "Team"
    else:
        try:
            primary_player = ' '.join([l[l.index("by") + 1], l[l.index("by") + 2]])
        except:
            primary_player = "Coach"

    # 2. Primary action
    primary_action = "foul"

    # 3. Description
    if l[0] == "Shooting":
        description = "shooting"
    elif l[0] == "Technical":
        description = "technical"
    elif l[0] == "Personal":
        description = "personal"
    elif l[0] == "Offensive":
        description = "offensive"
    elif l[0] == "Loose":
        description = "loose ball"
    elif l[0] == "Def":
        description = "defensive 3 sec"
    elif l[0] == "Violation":
        description = "goaltending"
    elif l[0] == "Away":
        description = "away from play"
    elif l[0] == "Flagrant":
        description = "flagrant"
    else:
        description = "UNKNOWN"

    # 4. Shot distance
    shot_dist = None

    # 5. Shot value
    shot_value = None 

    # 6,7. Secondary player, secondary action
    if "(drawn" in l:
        secondary_player = ' '.join([l[-2], l[-1][:-1]])
        secondary_action = "foul drawn"
    else:
        secondary_player = None 
        secondary_action = None 

    return [primary_player, primary_action, description, shot_dist,
                shot_value, secondary_player, secondary_action]

"""
l = ['Violation', 'by', 'Team', '(def', 'goaltending)']
foul(l)

l = ['Technical', 'foul', 'by', 'B.', 'Biyombo']
foul(l)

l = ['Loose', 'ball', 'foul', 'by', 'J.', 'Toscano-Anderson', '(drawn', 'by', 'D.', 'Jordan)']
foul(l)
"""

def player_sub(l: list) -> pd.DataFrame():
    """
    player_sub:
    ['K.', 'Looney', 'enters', 'the', 'game', 'for', 'J.', 'Wiseman']
    ['D.', 'Cousins', 'ejected', 'from', 'game']
    """
    primary_player = ' '.join([l[0], l[1]])

    if "ejected" in l:
        primary_action = "ejection"
        secondary_player = None 
        secondary_action = None
    else:
        primary_action = "sub in"
        secondary_player = ' '.join([l[-2], l[-1]])
        secondary_action = "sub out"

    description = None
    shot_dist = None 
    shot_value = None

    return [primary_player, primary_action, description, shot_dist,
                shot_value, secondary_player, secondary_action]

#l = ['K.', 'Looney', 'enters', 'the', 'game', 'for', 'J.', 'Wiseman']
#player_sub(l)

def timeout(l: list) -> pd.DataFrame():
    """
    timeout:
    ['LA', 'Clippers', 'full', 'timeout']
    ['New', 'York', 'full', 'timeout']
    ['Miami', 'full', 'timeout']
    ['Instant', 'Replay', '(Challenge:', 'Ruling', 'Stands)']
    """
    if "timeout" in l:
        primary_player = ' '.join(l[:l.index("full")])
        primary_action = "timeout"
        description = None 
    elif "Replay" in l:
        primary_player = "Team"
        primary_action = "replay"
        description = l[-1][:-1]

    
    shot_dist = None 
    shot_value = None 
    secondary_player = None 
    secondary_action = None

    return [primary_player, primary_action, description, shot_dist,
                shot_value, secondary_player, secondary_action]

"""
l = ['LA', 'Clippers', 'full', 'timeout']
timeout(l)

l = ['Miami', 'full', 'timeout']
timeout(l)
"""