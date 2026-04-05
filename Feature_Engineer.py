import pandas as pd
import numpy as np


class GameFeatureEngineering:

    def __init__(self, df):
        self.df = df.copy()

        # Ensure date format (if exists)
        if "date" in self.df.columns:
            self.df["date"] = pd.to_datetime(self.df["date"], errors="coerce")

    # ------------------------------------------------
    # Normalization (Safe)
    # ------------------------------------------------
    def normalize(self, series):
        if series.max() == series.min():
            return pd.Series(0, index=series.index)
        return (series - series.min()) / (series.max() - series.min())

    # ------------------------------------------------
    # Popularity Level (Robust)
    # ------------------------------------------------
    def add_popularity_level(self):

        required_cols = ["avg_players", "google_trend", "total_reviews"]
        for col in required_cols:
            if col not in self.df.columns:
                raise ValueError(f"Missing column: {col}")

        score = (
            0.5 * self.normalize(self.df["avg_players"]) +
            0.3 * self.normalize(self.df["google_trend"]) +
            0.2 * self.normalize(self.df["total_reviews"])
        )

        self.df["popularity_score"] = score

        q1 = score.quantile(0.33)
        q2 = score.quantile(0.66)

        # Prevent duplicate bin edges issue
        if q1 == q2:
            self.df["popularity_level"] = "Medium"
        else:
            self.df["popularity_level"] = pd.cut(
                score,
                bins=[-0.01, q1, q2, 1.01],
                labels=["Low", "Medium", "High"]
            )

    # ------------------------------------------------
    # Best Game Over Time (Top 20%)
    # ------------------------------------------------
    def add_best_game_feature(self):

        if "avg_players" not in self.df.columns:
            self.df["best_game_over_time"] = False
            return

        threshold = self.df["avg_players"].quantile(0.80)
        self.df["best_game_over_time"] = self.df["avg_players"] > threshold

    # ------------------------------------------------
    # Need Discount (Robust)
    # ------------------------------------------------
    def add_need_discount_feature(self):

        if "discount" not in self.df.columns:
            self.df["need_discount"] = False
            return

        if "date" in self.df.columns:
            self.df = self.df.sort_values("date")

        if "game" in self.df.columns:
            change = self.df.groupby("game")["avg_players"].pct_change()
        else:
            change = self.df["avg_players"].pct_change()

        self.df["need_discount"] = (
            (self.df["discount"] == 0) &
            (change < 0)
        )

    # ------------------------------------------------
    # Engagement Ratio
    # ------------------------------------------------
    def add_engagement_feature(self):

        if "peak_players" not in self.df.columns or "avg_players" not in self.df.columns:
            self.df["engagement_ratio"] = 0
            return

        ratio = self.df["peak_players"] / self.df["avg_players"].replace(0, np.nan)
        self.df["engagement_ratio"] = ratio.fillna(0)

    # ------------------------------------------------
    # Community Sentiment
    # ------------------------------------------------
    def add_sentiment_feature(self):

        if "positive_reviews_est" not in self.df.columns or "negative_reviews_est" not in self.df.columns:
            self.df["community_sentiment"] = 0
            return

        total = self.df["positive_reviews_est"] + self.df["negative_reviews_est"]

        sentiment = self.df["positive_reviews_est"] / total.replace(0, np.nan)
        self.df["community_sentiment"] = sentiment.fillna(0)

    # ------------------------------------------------
    # Player Momentum
    # ------------------------------------------------
    def add_player_momentum(self):

        if "avg_players" not in self.df.columns:
            self.df["player_momentum"] = 0
            return

        if "date" in self.df.columns:
            self.df = self.df.sort_values("date")

        rolling7 = self.df["avg_players"].rolling(7, min_periods=1).mean()
        rolling30 = self.df["avg_players"].rolling(30, min_periods=1).mean()

        self.df["player_momentum"] = rolling7 - rolling30

    # ------------------------------------------------
    # Run All Features
    # ------------------------------------------------
    def build_features(self):

        self.add_popularity_level()
        self.add_best_game_feature()
        self.add_need_discount_feature()
        self.add_engagement_feature()
        self.add_sentiment_feature()
        self.add_player_momentum()

        return self.df
