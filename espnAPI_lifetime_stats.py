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

seasons = [2016, 2017, 2018, 2019]

###ONE TIME DATA LOAD:
# df = pd.read_excel('C:/Users/ryanm/Desktop/Ryan/Coding/BXBoys_Website/Managers.xlsx')
# df.to_sql('managers', con=engine, if_exists='replace', index=True)

###CREATE DF_TEAM
df_team = pd.DataFrame()

for season in seasons:
    league_id = 281990
    url = "https://fantasy.espn.com/apis/v3/games/ffl/leagueHistory/" + \
          str(league_id) + "?seasonId=" + str(season)

    r = requests.get(url, params={"view": "mTeam"})
    d = r.json()[0]

    for i in d['teams']:
        print(d['teams'])

    df = json_normalize(d['teams'])

    df1 = df[['abbrev', 'id', 'location', 'nickname', 'primaryOwner', 'playoffSeed', 'rankCalculatedFinal',
              'record.overall.losses', 'record.overall.pointsAgainst',
              'record.overall.pointsFor', 'record.overall.wins', 'transactionCounter.acquisitions',
              'transactionCounter.drops', 'transactionCounter.trades',
              ]]
    df1['Season'] = season
    print(df1)

    df_team = pd.concat([df1, df_team])

# df_team.to_excel("history.xlsx")

###HISTORICAL SEASON SUMMARY
# df_final = pd.DataFrame()

# df_agg = df_team.groupby(['primaryOwner'])['record.overall.losses', 'record.overall.pointsAgainst',
#                                       'record.overall.pointsFor', 'record.overall.wins'].agg('sum')
#
# sql = "SELECT primaryOwner, manager FROM managers"
# df_man = pd.read_sql(sql, conn_str)
#
# df_final = pd.merge(df_agg, df_man, on='primaryOwner')
# df_final = df_final.drop(['primaryOwner'], axis=1)
# print(df_final)
# df_final = df_final.rename(columns={"record.overall.losses": "record_overall_losses",
#                                     "record.overall.pointsAgainst": "record_overall_pointsAgainst",
#                                     "record.overall.pointsFor": "record_overall_pointsFor",
#                                     "record.overall.wins": "record_overall_wins"})
#
# df_final.to_excel("cumulative_stas.xlsx", index=False)
# df_final.to_sql('league_history_cumulative', con=engine, if_exists='replace', index=True)

###PREVIOUS MATCHUPS
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

    print(d)

    df = json_normalize(d['schedule'])

    df = df[['id', 'matchupPeriodId', 'playoffTierType', 'winner', 'away.teamId', 'away.totalPoints',
             'home.teamId', 'home.totalPoints']]
    df['Season'] = season
    df_final = pd.concat([df, df_final])

# df_final.to_excel("test4.xlsx")

df_final['id'] = np.arange(df_final.shape[0])
df_final.set_index('id')

df_final = pd.merge(df_final, df_team, how='left', left_on=['away.teamId', 'Season'], right_on=['id', 'Season'])
# df_final.to_excel("test7.xlsx")

df_final = pd.merge(df_final, df_team, how='left', left_on=['home.teamId', 'Season'], right_on=['id', 'Season'])
df_final.to_excel("test8.xlsx")

df_manager_id_map = pd.read_sql('SELECT * FROM managers', conn_str, index_col='index')
df_manager_id_map.to_excel("test9.xlsx")
df_final = pd.merge(df_final, df_manager_id_map, how='left', left_on=['primaryOwner_x', 'primaryOwner_y'],
                    right_on=['primaryOwner', 'primaryOwner'])
df_final.to_excel("test10.xlsx")

df_final = df_final[['id', 'matchupPeriodId', 'playoffTierType', 'winner', 'away.teamId', 'away.totalPoints',
         'home.teamId', 'home.totalPoints']]

df_final.to_excel("test11.xlsx")



df_final.to_sql('matchup_history', con=engine, if_exists='replace', index=True)

table = engine.execute("SELECT * FROM matchup_history").fetchall()

for record in table:
    print(record)
