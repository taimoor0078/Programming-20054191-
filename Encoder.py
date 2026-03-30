import pandas as pd
from sklearn.preprocessing import OneHotEncoder, LabelEncoder


class Encoder:
    def __init__(self):
        self.label_encoders = {}
        self.onehot_encoder = None
        self.onehot_columns = None
        self.nominal_cols = None

    def label_encode(self, df, columns):
        """
        For binary or ordinal categorical variables
        """
        df = df.copy()

        for col in columns:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
            self.label_encoders[col] = le

        return df

    def onehot_encode(self, df, columns, drop_first=True):
        """
        For nominal categorical variables
        """
        df = df.copy()
        self.nominal_cols = columns

        # FIX: sparse -> sparse_output (new sklearn versions)
        self.onehot_encoder = OneHotEncoder(
            sparse_output=False,
            drop="first" if drop_first else None,
            handle_unknown="ignore"
        )

        encoded = self.onehot_encoder.fit_transform(df[columns])

        self.onehot_columns = self.onehot_encoder.get_feature_names_out(columns)

        encoded_df = pd.DataFrame(
            encoded,
            columns=self.onehot_columns,
            index=df.index
        )

        df = df.drop(columns=columns)
        df = pd.concat([df, encoded_df], axis=1)

        return df

    def transform_onehot(self, df):
        """
        Use this on test/unseen data
        """
        df = df.copy()

        encoded = self.onehot_encoder.transform(df[self.nominal_cols])

        encoded_df = pd.DataFrame(
            encoded,
            columns=self.onehot_columns,
            index=df.index
        )

        df = df.drop(columns=self.nominal_cols)
        df = pd.concat([df, encoded_df], axis=1)

        return df
