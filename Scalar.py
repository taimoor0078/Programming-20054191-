# preprocessing/scaler.py

from sklearn.preprocessing import StandardScaler, MinMaxScaler


class Scaler:
    def __init__(self, method="standard"):
        if method == "standard":
            self.scaler = StandardScaler()
        elif method == "minmax":
            self.scaler = MinMaxScaler()
        else:
            raise ValueError("method must be 'standard' or 'minmax'")

        self.columns = None

    def fit_transform(self, df, columns):
        df = df.copy()
        self.columns = columns
        df[columns] = self.scaler.fit_transform(df[columns])
        return df

    def transform(self, df):
        df = df.copy()
        df[self.columns] = self.scaler.transform(df[self.columns])
        return df
import numpy as np
import pandas as pd

# sklearn
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# statsmodels (GLM)
import statsmodels.api as sm


class LinearRegressor:
    """
    Linear Regression using:
    1) sklearn
    2) GLM (statsmodels)
    """

    def __init__(self):
        self.sklearn_model = None
        self.glm_model = None

    # =====================================================
    # 1️⃣ SKLEARN LINEAR REGRESSION
    # =====================================================
    def fit_sklearn(self, X, y):
        self.sklearn_model = LinearRegression()
        self.sklearn_model.fit(X, y)
        return self

    def predict_sklearn(self, X):
        if self.sklearn_model is None:
            raise ValueError("Sklearn Linear Regression model not fitted yet.")
        return self.sklearn_model.predict(X)

    def evaluate_sklearn(self, X, y):
        y_pred = self.predict_sklearn(X)
        return {
            "MSE": mean_squared_error(y, y_pred),
            "R2": r2_score(y, y_pred)
        }

    # =====================================================
    # 2️⃣ GLM LINEAR REGRESSION (statsmodels)
    # =====================================================
    def fit_glm(self, X, y):
        """
        GLM with Gaussian family (Linear Regression)
        """
        X_const = sm.add_constant(X)  # add intercept
        self.glm_model = sm.GLM(
            y,
            X_const,
            family=sm.families.Gaussian()
        ).fit()

        return self

    def predict_glm(self, X):
        if self.glm_model is None:
            raise ValueError("GLM model not fitted yet.")
        X_const = sm.add_constant(X)
        return self.glm_model.predict(X_const)

    def summary_glm(self):
        if self.glm_model is None:
            raise ValueError("GLM model not fitted yet.")
        return self.glm_model.summary()
