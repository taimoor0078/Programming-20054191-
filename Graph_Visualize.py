import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


class Visualize_Data:

    def __init__(self, df):
        self.df = df.copy()
        sns.set_theme(style="darkgrid")
        plt.rcParams["figure.figsize"] = (10, 5)

    # -----------------------------
    # 1 Line Plot
    # -----------------------------
    def line_plot(self, x, y, title="Line Plot"):
        sns.lineplot(data=self.df, x=x, y=y)
        plt.title(title)
        plt.show()

    # -----------------------------
    # 2 Bar Plot
    # -----------------------------
    def bar_plot(self, x, y=None, title="Bar Plot"):
        if y:
            sns.barplot(data=self.df, x=x, y=y)
        else:
            sns.countplot(data=self.df, x=x)
        plt.title(title)
        plt.show()

    # -----------------------------
    # 3 Histogram
    # -----------------------------
    def histogram(self, column, title="Histogram"):
        sns.histplot(self.df[column], kde=True)
        plt.title(title)
        plt.show()

    # -----------------------------
    # 4 Boxplot
    # -----------------------------
    def boxplot(self, column, title="Boxplot"):
        sns.boxplot(x=self.df[column])
        plt.title(title)
        plt.show()

    # -----------------------------
    # 5 Scatter Plot
    # -----------------------------
    def scatter_plot(self, x, y, hue=None, size=None, title="Scatter Plot"):
        sns.scatterplot(
            data=self.df,
            x=x,
            y=y,
            hue=hue,
            size=size,
            alpha=0.7
        )
        plt.title(title)
        plt.show()

    # -----------------------------
    # 6 Correlation Heatmap
    # -----------------------------
    def heatmap_corr(self, title="Correlation Heatmap"):
        corr = self.df.corr(numeric_only=True)
        sns.heatmap(corr, cmap="coolwarm", linewidths=0.5)
        plt.title(title)
        plt.show()

    # -----------------------------
    # 7 Custom Heatmap (Pivot)
    # -----------------------------
    def heatmap_pivot(self, index, columns, values, title="Heatmap"):
        pivot = self.df.pivot_table(
            index=index,
            columns=columns,
            values=values,
            aggfunc="mean"
        )
        sns.heatmap(pivot, annot=True, fmt=".0f", cmap="magma")
        plt.title(title)
        plt.show()

    # -----------------------------
    # 8 Pair Plot
    # -----------------------------
    def pair_plot(self, columns=None):
        if columns:
            sns.pairplot(self.df[columns])
        else:
            sns.pairplot(self.df)
        plt.show()

    # -----------------------------
    # 9 Violin Plot
    # -----------------------------
    def violin_plot(self, x, y, title="Violin Plot"):
        sns.violinplot(data=self.df, x=x, y=y)
        plt.title(title)
        plt.show()

    # -----------------------------
    # 10 KDE Plot
    # -----------------------------
    def kde_plot(self, column, title="KDE Plot"):
        sns.kdeplot(self.df[column], fill=True)
        plt.title(title)
        plt.show()

    # -----------------------------
    # 11 Yearly Trend Plot
    # -----------------------------
    def yearly_trend(self, date_col, value_col, title="Yearly Trend"):
        df = self.df.copy()
        df["year"] = pd.to_datetime(df[date_col]).dt.year

        yearly = df.groupby("year")[value_col].mean().reset_index()

        sns.lineplot(data=yearly, x="year", y=value_col, marker="o")
        plt.title(title)
        plt.show()

    # -----------------------------
    # 12 Top N Bar Plot
    # -----------------------------
    def top_n_bar(self, column, value, n=10, title="Top N"):
        top = self.df.groupby(column)[value].mean().nlargest(n)

        top.plot(kind="bar")
        plt.title(title)
        plt.show()