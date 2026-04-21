st.subheader("🔥 TOP DFS PLAYS")
st.dataframe(df.sort_values(by="DFS_PROJ", ascending=False).head(20))

st.subheader("💣 HOME RUN PICKS")
st.dataframe(df.sort_values(by="HR_PROB", ascending=False).head(15))

st.subheader("💰 BEST BETS (+EV)")
st.dataframe(pd.DataFrame(bets))

st.subheader("📊 TEAM STACKS")
st.dataframe(
    df.groupby("Team")["DFS_PROJ"].sum().sort_values(ascending=False).head(10)
)
