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
    def get_steam_store_data(self):
        data_list = []
        for g in self.games:
            appid = g["appid"]
            url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us"
            try:
                r = requests.get(url, timeout=15).json()
            except:
                continue

            if not r.get(str(appid)) or not r[str(appid)]["success"]:
                continue
            data = r[str(appid)]["data"]
            price = 0
            discount = 0
            initial_price = 0
            if "price_overview" in data:
                price = data["price_overview"]["final"] / 100
                discount = data["price_overview"].get("discount_percent", 0)
                initial_price = data["price_overview"].get("initial", 0) / 100
            total_reviews = data.get("recommendations", {}).get("total", 0)
            positive_reviews = int(total_reviews * 0.85)
            negative_reviews = int(total_reviews * 0.15)
            is_free = int(data.get("is_free", False))
            data_list.append({
                "game": g["name"],
                "appid": appid,
                "price": price,
                "discount": discount,
                "initial_price": initial_price,
                "total_reviews": total_reviews,
                "positive_reviews": positive_reviews,
                "negative_reviews": negative_reviews,
                "is_free": is_free
            })
        return pd.DataFrame(data_list)
    def get_steamcharts_data(self):
        all_data = []
        for g in self.games:
            appid = g["appid"]
            url = f"https://steamcharts.com/app/{appid}"
            try:
                r = requests.get(url)
                soup = BeautifulSoup(r.text, "html.parser")
            except:
                continue
            table = soup.find("table", {"class": "common-table"})
            if not table:
                continue
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
            if df.empty:
                continue
            df["date"] = pd.to_datetime(df["month"], format="%B %Y", errors="coerce")
            df = df.dropna(subset=["date"])
            df = df.sort_values("date")
            df = df[df["date"] >= self.start_date]
            df = df.set_index("date")[["avg_players", "peak_players"]]
            df = df.resample("D").mean().interpolate()
            df = df.reset_index()
            df["game"] = g["name"]
            df["appid"] = appid
            all_data.append(df)
        return pd.concat(all_data, ignore_index=True)
    def get_google_trends(self):
        pytrends = TrendReq(hl='en-US', tz=0)
        all_data = []
        timeframe = f"{self.start_date.strftime('%Y-%m-%d')} {self.end_date.strftime('%Y-%m-%d')}"
        for g in self.games:
            keyword = g["name"] + " game"
            try:
                pytrends.build_payload([keyword], timeframe=timeframe)
                data = pytrends.interest_over_time()
            except:
                continue
            if data.empty:
                continue
            df = data.reset_index()
            df = df.rename(columns={keyword: "google_trend"})
            df = df[["date", "google_trend"]]
            df["game"] = g["name"]
            all_data.append(df)
            time.sleep(2)  # avoid blocking
        return pd.concat(all_data, ignore_index=True)
    def run(self):
        steam_api = self.get_steam_store_data()
        steamcharts = self.get_steamcharts_data()
        trends = self.get_google_trends()
        final = steamcharts.merge(trends, on=["date", "game"], how="left")
        final = final.merge(steam_api, on=["game", "appid"], how="left")
        final["google_trend"] = final["google_trend"].fillna(0)
        final.to_csv(self.output_file, index=False)
        return final
