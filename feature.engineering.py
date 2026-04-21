import pandas as pd

def build_features(df):
    
    # -----------------------
    # HANDEDNESS EDGE
    # -----------------------
    df["split_wOBA"] = df.apply(
        lambda x: x["wOBA_vs_LHP"] if x["PitchHand"] == "L" else x["wOBA_vs_RHP"],
        axis=1
    )

    # -----------------------
    # POWER + CONTACT
    # -----------------------
    df["power_contact"] = df["ISO"] * df["HardHit"]

    # -----------------------
    # PITCHER ATTACK SCORE
    # -----------------------
    df["pitcher_weakness"] = (
        df["HR/9"] * 0.4 +
        df["xERA"] * 0.3 +
        (1 - df["K%"]) * 0.3
    )

    # -----------------------
    # PARK BOOST
    # -----------------------
    df["park_boost"] = df["ParkFactor"]

    # -----------------------
    # ORDER VALUE
    # -----------------------
    df["order_score"] = df["Order"].apply(lambda x: 1.15 if x <= 2 else (1.05 if x <=5 else 0.9))

    # -----------------------
    # FINAL FEATURE SCORE
    # -----------------------
    df["feature_score"] = (
        df["split_wOBA"] * 0.3 +
        df["power_contact"] * 0.25 +
        df["pitcher_weakness"] * 0.2 +
        df["park_boost"] * 0.15 +
        df["order_score"] * 0.1
    )

    return df
