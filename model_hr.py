import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor

def train_hr_model(df):

    features = [
        "ISO", "HardHit", "Barrel%", "FB%", 
        "HR/9", "ParkFactor", "pitcher_weakness"
    ]

    df = df.dropna()

    X = df[features]
    y = df["HR"]  # historical HRs

    model = GradientBoostingRegressor()
    model.fit(X, y)

    df["HR_PROB"] = model.predict(X)

    return df, model
