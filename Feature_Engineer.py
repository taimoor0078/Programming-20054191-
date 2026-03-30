import pandas as pd
import numpy as np


class GameFeatureEngineering:

    def __init__(self, df):
        self.df = df.copy()

    # ------------------------------------------------
    # Popularity Level
    # ------------------------------------------------
    def add_popularity_level(self):

        score = (
            0.5 * self.normalize(self.df["avg_players"]) +
            0.3 * self.normalize(self.df["google_trend"]) +
            0.2 * self.normalize(self.df["total_reviews"])
        )

        self.df["popularity_score"] = score

        self.df["popularity_level"] = pd.cut(
            score,
            bins=[0, 0.4, 0.7, 1],
            labels=["Low", "Medium", "High"]
        )

    # ------------------------------------------------
    # Best Game Over Time
    # ------------------------------------------------
    def add_best_game_feature(self):

        threshold = self.df["avg_players"].quantile(0.80)

        self.df["best_game_over_time"] = self.df["avg_players"] > threshold

    # ------------------------------------------------
    # Need Discount
    # ------------------------------------------------
    def add_need_discount_feature(self):

        condition = (
            (self.df["discount_percent"] == 0) &
            (self.df["avg_players"].pct_change() < 0)
        )

        self.df["need_discount"] = condition

    # ------------------------------------------------
    # Engagement
    # ------------------------------------------------
    def add_engagement_feature(self):

        self.df["engagement_ratio"] = (
            self.df["peak_players"] /
            self.df["avg_players"]
        )

    # ------------------------------------------------
    # Community Sentiment
    # ------------------------------------------------
    def add_sentiment_feature(self):

        self.df["community_sentiment"] = (
            self.df["positive_reviews_est"] /
            (self.df["positive_reviews_est"] + self.df["negative_reviews_est"])
        )

    # ------------------------------------------------
    # Player Momentum
    # ------------------------------------------------
    def add_player_momentum(self):

        rolling7 = self.df["avg_players"].rolling(7).mean()
        rolling30 = self.df["avg_players"].rolling(30).mean()

        self.df["player_momentum"] = rolling7 - rolling30

    # ------------------------------------------------
    # Normalization helper
    # ------------------------------------------------
    def normalize(self, series):

        return (series - series.min()) / (series.max() - series.min())

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