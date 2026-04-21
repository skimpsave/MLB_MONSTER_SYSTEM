import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import sqlite3

# -----------------------
# AUTH GOOGLE SHEETS
# -----------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "credentials.json",
    scope
)

client = gspread.authorize(creds)

sheet = client.open("MLB_MONSTER_SYSTEM")

# -----------------------
# LOAD DATA
# -----------------------
conn = sqlite3.connect("mlb_data.db")
df = pd.read_sql("SELECT * FROM results", conn)

# -----------------------
# PREP DATASETS
# -----------------------
dfs_rankings = df[[
    "Batter","Team","DFS_PROJ","HR_PROB","feature_score","Order"
]].sort_values("DFS_PROJ", ascending=False).head(25)

hr_targets = df[[
    "Batter","HR_PROB"
]].sort_values("HR_PROB", ascending=False).head(15)

# Mock betting EV column if not present
df["EV_EDGE"] = df["HR_PROB"] - 0.2

bets = df[[
    "Batter","HR_PROB","EV_EDGE"
]].sort_values("EV_EDGE", ascending=False).head(10)

stacks = df.groupby("Team")["DFS_PROJ"].sum().reset_index()

# -----------------------
# PUSH TO SHEETS
# -----------------------
def update_sheet(tab_name, data):
    worksheet = sheet.worksheet(tab_name)
    worksheet.clear()
    worksheet.update([data.columns.values.tolist()] + data.values.tolist())

update_sheet("DFS_RANKINGS", dfs_rankings)
update_sheet("HR_TARGETS", hr_targets)
update_sheet("DAILY_BETS", bets)
update_sheet("STACKS", stacks)

print("✅ Google Sheet Updated")
