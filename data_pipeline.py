import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3

conn = sqlite3.connect("mlb_data.db")

# -----------------------
# GET LINEUPS (Rotowire)
# -----------------------
url = "https://www.rotowire.com/baseball/daily-lineups.php"
res = requests.get(url)
soup = BeautifulSoup(res.text, "lxml")

lineups = []

games = soup.find_all("div", class_="lineup")
for game in games:
    try:
        teams = game.find_all("div", class_="lineup__team")
        
        for team in teams:
            players = team.find_all("li")
            for i, p in enumerate(players):
                name = p.text.strip()
                lineups.append({
                    "Batter": name,
                    "Order": i+1
                })
    except:
        continue

df_lineups = pd.DataFrame(lineups)
df_lineups.to_sql("lineups", conn, if_exists="replace", index=False)

# -----------------------
# LOAD LOCAL STATS (you update weekly)
# -----------------------
batters = pd.read_csv("batters.csv")
pitchers = pd.read_csv("pitchers.csv")
parks = pd.read_csv("parks.csv")

batters.to_sql("batters", conn, if_exists="replace", index=False)
pitchers.to_sql("pitchers", conn, if_exists="replace", index=False)
parks.to_sql("parks", conn, if_exists="replace", index=False)

conn.close()
