import requests
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine
import urllib
import numpy as np

driver = "{ODBC Driver 17 for SQL Server}"
server = "bxboysdbserver.database.windows.net"
database = "bxboysdb_portfolio"
username = "rfosha"
password = "Bxboys2020"

params = urllib.parse.quote_plus(
    'Driver=%s;' % driver +
    'Server=tcp:%s,1433;' % server +
    'Database=%s;' % database +
    'Uid=%s;' % username +
    'Pwd={%s};' % password +
    'Encrypt=yes;' +
    'TrustServerCertificate=no;' +
    'Connection Timeout=30;')

conn_str = 'mssql+pyodbc:///?odbc_connect=' + params
engine = create_engine(conn_str)

###ONE TIME DATA LOAD:
# df = pd.read_excel('C:/Users/ryanm/Desktop/Ryan/Coding/BXBoys_Website/matchups.xlsx')
# df.to_sql('matchups', con=engine, if_exists='replace', index=True)

###HISTORICAL SEASON SUMMARY
seasons = [2016, 2017, 2018, 2019]
df_final = pd.DataFrame()
df_team = pd.DataFrame()

for season in seasons:
    league_id = 281990
    url = "https://fantasy.espn.com/apis/v3/games/ffl/leagueHistory/" + \
          str(league_id) + "?seasonId=" + str(season)

    r = requests.get(url, params={"view": "mTeam"})
    d = r.json()[0]

    df = json_normalize(d['teams'])

    df1 = df[['id', 'primaryOwner']]
    df1['Season'] = season

    df_team = pd.concat([df1, df_team])

###PREVIOUS MATCHUPS
seasons = [2016, 2017, 2018, 2019]
df_final = pd.DataFrame()

for season in seasons:
    league_id = 281990
    url = "https://fantasy.espn.com/apis/v3/games/ffl/leagueHistory/" + \
          str(league_id) + "?seasonId=" + str(season)

    r = requests.get(url, params={"view": "mMatchupScore"},
                     cookies={"swid": "{0C2693F8-5DE9-4F6F-9633-9C978BC540AC}",
                              "espn_s2": "AEAQyJnBGAsbj0jsk03vHn7F5UEtjseo9MUOENGQA1RnIbqgiub11CfjyhQbj%2Fhqyi1C3e5SuzVEaH9wx3jFYUcWwIdDJdTwWd%2B1xvlGlM7aLPJ1cSUWIHjAYv1Wgvh5JZ4Ppkt0RF0Pqk%2BqOcJZi2UKDRRXOtRYwZLaV3rNCkCGBxRXSFqLuoKOsXyvxQWugLZo4hpb%2BN89CHSgdjHKc1f%2FxgUpC2QXuj68HGoOPFVWRexq%2Fc2p86Kww4lKDfIZxWhBtTXas8U%2FxbMc46cCgs5E"}
                     )
    d = r.json()[0]

    df = json_normalize(d['schedule'])

    df = df[['id', 'matchupPeriodId', 'playoffTierType', 'winner', 'away.teamId', 'away.totalPoints',
             'home.teamId', 'home.totalPoints']]
    df['Season'] = season
    df_final = pd.concat([df, df_final])

df_final['id'] = np.arange(df_final.shape[0])
df_final.set_index('id')

df_final = pd.merge(df_final, df_team, how='left', left_on=['away.teamId', 'Season'], right_on=['id', 'Season'])

df_final = pd.merge(df_final, df_team, how='left', left_on=['home.teamId', 'Season'], right_on=['id', 'Season'])

df_manager_id_map = pd.read_sql('SELECT * FROM matchups', conn_str)
df_final = pd.merge(df_final, df_manager_id_map, how='left', left_on=['primaryOwner_x', 'primaryOwner_y'],
                    right_on=['team1', 'team2'])

df_final['away.win'] = np.where(df_final['winner'] == 'AWAY', 1, 0)
df_final['home.loss'] = np.where(df_final['winner'] == 'AWAY', 1, 0)
df_final['away.loss'] = np.where(df_final['winner'] == 'HOME', 1, 0)
df_final['home.win'] = np.where(df_final['winner'] == 'HOME', 1, 0)
df_final['tie'] = np.where(df_final['winner'] == 'TIE', 1, 0)

df_final = df_final.groupby(['matchup', 'overall.grouping', 'reverse.flag'])['away.win', 'home.loss', 'away.loss',
                                                                             'home.win', 'tie', 'away.totalPoints',
                                                                             'home.totalPoints'].agg('sum')

df_final = df_final.reset_index(level=['reverse.flag', 'overall.grouping'])

df_flip = df_final[df_final['reverse.flag'] == 1]
df_flip['home.win.flip'] = df_flip['away.win']
df_flip['home.loss.flip'] = df_flip['away.loss']
df_flip['home.totalPoints.flip'] = df_flip['away.totalPoints']
df_flip['away.win.flip'] = df_flip['home.win']
df_flip['away.loss.flip'] = df_flip['home.loss']
df_flip['away.totalPoints.flip'] = df_flip['home.totalPoints']

df_flip = df_flip.drop(columns = ['away.win', 'away.loss', 'away.totalPoints', 'home.win', 'home.loss', 'home.totalPoints'])

df_flip = df_flip.rename(columns={"home.win.flip": "home.win",
                                  "home.loss.flip": "home.loss",
                                  "home.totalPoints.flip": "home.totalPoints",
                                  "away.win.flip": "away.win",
                                  "away.loss.flip": "away.loss",
                                  "away.totalPoints.flip": "away.totalPoints"})


df_final_0 = df_final[df_final["reverse.flag"] == 0]

df_matchup = pd.concat([df_final_0, df_flip], sort=True)

df_matchup = df_matchup.groupby(['overall.grouping'])['away.win', 'home.loss', 'away.loss',
                                                      'home.win', 'tie', 'away.totalPoints', 'home.totalPoints'].agg('sum')

df_matchup = df_matchup.drop(columns = ['home.loss', 'home.win'])

df_matchup = df_matchup.reset_index(level=['overall.grouping'])

df_matchup.index.names = ['id']

df_matchup = df_matchup.rename(columns={"away.win": "wins",
                                  "away.loss": "losses",
                                  "away.totalPoints": "points_for",
                                  "home.totalPoints": "points_against",
                                  "overall.grouping": "head_to_head_matchup"})

df_matchup.to_sql('matchup_stats', con=engine, if_exists='replace', index=True)

