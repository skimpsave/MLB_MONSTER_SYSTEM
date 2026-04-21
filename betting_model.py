def calculate_ev(prob, odds):

    implied_prob = 1 / (odds / 100 + 1) if odds > 0 else abs(odds) / (abs(odds) + 100)

    edge = prob - implied_prob

    return edge


def find_bets(df):

    bets = []

    for _, row in df.iterrows():
        edge = calculate_ev(row["HR_PROB"], row["HR_ODDS"])

        if edge > 0.05:
            bets.append({
                "Player": row["Batter"],
                "Edge": edge,
                "Odds": row["HR_ODDS"]
            })

    return bets
