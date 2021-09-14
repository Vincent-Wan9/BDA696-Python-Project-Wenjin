import sys

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def print_heading(title):
    print("*" * 80)
    print(title)
    print("*" * 80)
    return


# Get some simple summary statistics (mean, min, max, quartiles) using numpy
def get_statistics(iris_df):
    print("Mean:\n", iris_df.mean())
    print("\nMinimun Value:\n", iris_df.min())
    print("\nMaximun Value:\n", iris_df.max())
    print("\nQuartiles: ")
    print("sepal length ", np.percentile(iris_df["sepal length"], [25, 50, 75]))
    print("sepal width ", np.percentile(iris_df["sepal width"], [25, 50, 75]))
    print("petal length ", np.percentile(iris_df["petal length"], [25, 50, 75]))
    print("petal width ", np.percentile(iris_df["petal width"], [25, 50, 75]))


# Plot the different classes against one another (try 5 different plots)
def get_plot(iris_df):
    # (1) Scatterplot
    fig = px.scatter_matrix(
        iris_df,
        dimensions=["sepal width", "sepal length", "petal width", "petal length"],
        color="class",
    )
    fig.show()

    # (2) Violin Plot
    fig = go.Figure()
    fig.add_trace(
        go.Violin(
            x=iris_df["class"],
            y=iris_df["sepal length"],
            legendgroup="sepal length",
            scalegroup="sepal length",
            name="sepal length",
            line_color="blue",
        )
    )
    fig.add_trace(
        go.Violin(
            x=iris_df["class"],
            y=iris_df["sepal width"],
            legendgroup="sepal width",
            scalegroup="sepal width",
            name="sepal width",
            line_color="orange",
        )
    )
    fig.update_traces(box_visible=True, meanline_visible=True)
    fig.update_layout(violinmode="group")
    fig.show()

    # (3) Boxplot
    fig = go.Figure()
    fig.add_trace(
        go.Box(
            y=iris_df["petal length"],
            x=iris_df["class"],
            name="petal length",
            marker_color="#3D9970",
        )
    )
    fig.add_trace(
        go.Box(
            y=iris_df["petal width"],
            x=iris_df["class"],
            name="petal width",
            marker_color="#FF4136",
        )
    )
    fig.update_layout(yaxis_title="CM", boxmode="group")
    fig.show()

    # (4) Pie chart
    fig = px.pie(iris_df, values=iris_df["sepal length"], names="class")
    fig.show()

    # (5) Histogram
    df1 = iris_df.groupby(iris_df["class"]).mean()
    df1["class"] = df1.index

    fig = go.Figure(
        data=[
            go.Bar(name="sepal length", x=df1["class"], y=df1["sepal length"]),
            go.Bar(name="sepal width", x=df1["class"], y=df1["sepal width"]),
            go.Bar(name="petal length", x=df1["class"], y=df1["petal length"]),
            go.Bar(name="petal width", x=df1["class"], y=df1["petal width"]),
        ]
    )

    fig.update_layout(barmode="group")
    fig.show()


# Analyze and build models - Use scikit-learn
def build_model(iris_df):
    # Preprocessing the predictors by standardScaler
    x_orig = iris_df.iloc[:, 0:4].values
    scaler = StandardScaler()
    scaler.fit(x_orig)
    x = scaler.transform(x_orig)
    y = iris_df["class"].values

    # Fit the features to a random forest
    random_forest = RandomForestClassifier(random_state=1234)
    random_forest.fit(x, y)
    rf_prediction = random_forest.predict(x)
    rf_probability = random_forest.predict_proba(x)
    print("Random Forest: ")
    print(f"Probability:\n {rf_probability}")
    print(f"Prediction:\n {rf_prediction}")

    # Fit the features to a logistic regression
    logreg = LogisticRegression(C=1e5)
    logreg.fit(x, y)
    lr_prediction = logreg.predict(x)
    lr_probability = logreg.predict_proba(x)
    print("\nLogistic Regression: ")
    print(f"Probability:\n {lr_probability}")
    print(f"Prediction:\n {lr_prediction}")

    # As pipeline
    print("\n")
    print_heading("Model via Pipeline Predictions")
    pipeline1 = Pipeline(
        [
            ("standardScaler", StandardScaler()),
            ("RandomForest", RandomForestClassifier(random_state=1234)),
        ]
    )
    pipeline1.fit(x_orig, y)

    RF_probability = pipeline1.predict_proba(x_orig)
    RF_prediction = pipeline1.predict(x_orig)
    print("Random Forest: ")
    print(f"Probability: {RF_probability}")
    print(f"Predictions: {RF_prediction}")


def main():
    # Load the Iris data into a Pandas DataFrame
    iris_df = pd.read_csv(
        "Data/iris.data",
        names=["sepal length", "sepal width", "petal length", "petal width", "class"],
    )

    print_heading("Get some simple summary statistics")
    get_statistics(iris_df)
    print("\n")

    print_heading("Plot the different classes against one another")
    get_plot(iris_df)
    print("\n")

    print_heading("Analyze and build models")
    build_model(iris_df)


if __name__ == "__main__":
    sys.exit(main())
