def calculate_dfs_points(df):

    # DraftKings scoring example
    df["DFS_PROJ"] = (
        df["HR_PROB"] * 10 +
        df["split_wOBA"] * 8 +
        df["HardHit"] * 3 +
        df["order_score"] * 2
    )

    return df
