def run(self):

    all_data = []

    for g in self.games:

        print(f"\nCollecting: {g['name']}")

        price, discount, initial_price, total_reviews, pos_rev, neg_rev, is_free = \
            self.get_steam_store_data(g["appid"])

        players_df = self.get_steamcharts_data(g["appid"])

        if players_df is None:
            continue

        players_df = players_df.reset_index()

        # Ensure date column exists and is datetime
        players_df["date"] = pd.to_datetime(players_df["date"])

        # Derived metrics
        players_df["players_change_pct"] = players_df["avg_players"].pct_change().fillna(0)
        players_df["rolling_7d_avg_players"] = players_df["avg_players"].rolling(7).mean()
        players_df["rolling_30d_avg_players"] = players_df["avg_players"].rolling(30).mean()

        trends_df = self.get_google_trends(g["name"] + " game")

        if trends_df is not None and "date" in trends_df.columns:

            trends_df["date"] = pd.to_datetime(trends_df["date"])

            final_df = players_df.merge(trends_df, on="date", how="left")

            final_df["google_trend"] = final_df["google_trend"].ffill().fillna(0)

        else:
            final_df = players_df.copy()
            final_df["google_trend"] = 0

        final_df["game"] = g["name"]
        final_df["appid"] = g["appid"]
        final_df["price"] = price
        final_df["discount_percent"] = discount
        final_df["initial_price"] = initial_price
        final_df["is_free"] = is_free
        final_df["total_reviews"] = total_reviews
        final_df["positive_reviews_est"] = pos_rev
        final_df["negative_reviews_est"] = neg_rev
        final_df["data_collected_at"] = self.collection_time

        all_data.append(final_df)

        time.sleep(2)

    if not all_data:
        print("No data collected.")
        return None

    result = pd.concat(all_data, ignore_index=True)

    result.to_csv(self.output_file, index=False)

    print("\n🔥 ADVANCED DATASET READY")
    print("Rows:", len(result))

    return result
