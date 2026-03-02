import requests
import pandas as pd
from bs4 import BeautifulSoup
from pytrends.request import TrendReq
from datetime import datetime, timedelta
import numpy as np
import time


class AdvancedGameDataCollector:

    def __init__(self, games, years_back=3, output_file="advanced_games_dataset.csv"):
        self.games = games
        self.start_date = datetime.now() - timedelta(days=365 * years_back)
        self.end_date = datetime.now()
        self.output_file = output_file
        self.collection_time = datetime.now()

    # ==========================================================
    # STEAM STORE API
    # ==========================================================

    def get_steam_store_data(self, appid):

        url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us"

        try:
            r = requests.get(url, timeout=15).json()
        except:
            return 0,0,0,0,0,0,0

        if not r.get(str(appid)) or not r[str(appid)]["success"]:
            return 0,0,0,0,0,0,0

        data = r[str(appid)]["data"]

        price = 0
        discount = 0
        initial_price = 0

        if "price_overview" in data:
            price = data["price_overview"]["final"] / 100
            discount = data["price_overview"].get("discount_percent", 0)
            initial_price = data["price_overview"].get("initial", 0) / 100

        total_reviews = data.get("recommendations", {}).get("total", 0)

        # rough estimation (Steam doesn't give exact split here)
        positive_reviews = int(total_reviews * 0.85)
        negative_reviews = int(total_reviews * 0.15)

        is_free = int(data.get("is_free", False))

        return price, discount, initial_price, total_reviews, positive_reviews, negative_reviews, is_free

    # ==========================================================
    # STEAMCHARTS SCRAPER
    # ==========================================================

    def get_steamcharts_data(self, appid):

        url = f"https://steamcharts.com/app/{appid}"
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")

        table = soup.find("table", {"class": "common-table"})
        if not table:
            return None

        rows = []
        body = table.find("tbody")

        for tr in body.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue

            month_text = tds[0].text.strip()

            if "Last" in month_text or "Peak" in month_text:
                continue

            try:
                avg_players = float(tds[1].text.strip().replace(",", ""))
                peak_players = float(tds[4].text.strip().replace(",", ""))

                rows.append({
                    "month": month_text,
                    "avg_players": avg_players,
                    "peak_players": peak_players
                })
            except:
                continue

        df = pd.DataFrame(rows)

        df["date"] = pd.to_datetime(df["month"], format="%B %Y", errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date")
        df = df[df["date"] >= self.start_date]

        df = df.set_index("date")
        df = df[["avg_players", "peak_players"]]

        daily_df = df.resample("D").mean().interpolate()

        # Extend to today
        if daily_df.index.max() < self.end_date:

            last_row = daily_df.iloc[-1]

            future_dates = pd.date_range(
                start=daily_df.index.max() + timedelta(days=1),
                end=self.end_date,
                freq="D"
            )

            future_df = pd.DataFrame(index=future_dates)
            future_df["avg_players"] = last_row["avg_players"]
            future_df["peak_players"] = last_row["peak_players"]

            daily_df = pd.concat([daily_df, future_df])

        return daily_df

    # ==========================================================
    # GOOGLE TRENDS
    # ==========================================================

    def get_google_trends(self, keyword):

        pytrends = TrendReq(hl='en-US', tz=0)
        timeframe = f"{self.start_date.strftime('%Y-%m-%d')} {self.end_date.strftime('%Y-%m-%d')}"

        try:
            pytrends.build_payload([keyword], timeframe=timeframe)
            data = pytrends.interest_over_time()
        except:
            return None

        if data.empty:
            return None

        df = data.reset_index()
        df = df.rename(columns={keyword: "google_trend"})
        df = df[["date", "google_trend"]]

        return df

    # ==========================================================
    # MAIN RUN
    # ==========================================================

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

            # Derived metrics
            players_df["players_change_pct"] = players_df["avg_players"].pct_change().fillna(0)
            players_df["rolling_7d_avg_players"] = players_df["avg_players"].rolling(7).mean()
            players_df["rolling_30d_avg_players"] = players_df["avg_players"].rolling(30).mean()

            trends_df = self.get_google_trends(g["name"] + " game")

            if trends_df is not None:
                final_df = players_df.merge(trends_df, on="date", how="left")
                final_df["google_trend"] = final_df["google_trend"].fillna(method="ffill").fillna(0)
            else:
                final_df = players_df
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

        result = pd.concat(all_data, ignore_index=True)

        result.to_csv(self.output_file, index=False)

        print("\n🔥 ADVANCED DATASET READY")
        print("Rows:", len(result))

        return result