import sys

import numpy as np
import pandas as pd
import statsmodels.api
from plotly import express as px
from plotly import figure_factory as ff
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor


def print_heading(title):
    print("*" * 80)
    print(title)
    print("*" * 80)
    return


def check_variable_cat_or_cont(df, cat_name, cont_name):
    feature_type = {}
    for col in df.columns:
        if df[col].dtypes in ["int64", "float64"]:
            feature_type[col] = "Continuous"
        else:
            feature_type[col] = "Categorical"
    for key, value in feature_type.items():
        print(key, ": ", value)
    for key, value in feature_type.items():
        if value == "Categorical":
            cat_name.append(key)
        if value == "Continuous":
            cont_name.append(key)
    return cat_name, cont_name


# determine if response is continuous or boolean
def check_response_cat_or_cont(df_response):
    if len(pd.unique(df_response)) > 2:
        print_heading("Please note that your response variable is continuous. ")
    else:
        print_heading("Please note that your response variable is boolean. ")
    return


# flake8: noqa
def cont_response_cat_predictor(df, df_predictor, df_response):
    for col in df_predictor.columns:
        if df[col].dtypes in ["int64", "float64"]:
            None
        else:
            hist_data = []
            for sub_cat in df_predictor[col].unique():
                x = df.loc[df[col] == sub_cat, df.columns == df_response.name]
                hist_data.append(list(x.iloc[:][df_response.name]))
            group_labels = [f"{i}" for i in df_predictor[col].unique()]

            # Create distribution plot with custom bin_size
            fig = ff.create_distplot(hist_data, group_labels, bin_size=0.2)
            fig.update_layout(
                title=f"Continuous Response ({df_response.name}) by Categorical Predictor ({df_predictor[col]})",
                xaxis_title=f"Continuous Response ({df_response.name})",
                yaxis_title="Distribution",
            )
            fig.write_html(
                file=f"/Users/Vincentwang/PycharmProjects/BDA696-Python-Project-Wenjin/Homework/Data/hw_4_cont_response_cat_predictor_{df_predictor.columns.get_loc(col)}.html",  # noqa
                include_plotlyjs="cdn",
            )
    return


# Make plots for continuous response with p-value if applied
def cont_response_cont_predictor(df, response_value, df_predictor):
    for col in df_predictor.columns:
        if df[col].dtypes in ["int64", "float64"]:  # continuous predictors
            linear_regression_model = statsmodels.api.OLS(response_value, df[col])
            linear_regression_model_fitted = linear_regression_model.fit()

            # Get the stats
            t_value = round(linear_regression_model_fitted.tvalues[0], 6)
            p_value = "{:.6e}".format(linear_regression_model_fitted.pvalues[0])

            # Plot the figure
            fig = px.scatter(x=df[col], y=response_value, trendline="ols")
            fig.update_layout(
                title=f"Variable: {col}: (t-value={t_value}) (p-value={p_value})",
                xaxis_title=f"Variable: {col}",
                yaxis_title="y",
            )
            fig.write_html(
                file=f"/Users/Vincentwang/PycharmProjects/BDA696-Python-Project-Wenjin/Homework/Data/hw_4_cont_response_cont_predictor_{df_predictor.columns.get_loc(col)}.html",  # noqa
                include_plotlyjs="cdn",
            )
    return


# Make plots for categorical response with p-value if applied
def cat_response_cont_predictor(df_predictor, df_response, df):
    for col in df_predictor.columns:
        if df[col].dtypes in ["int64", "float64"]:  # continuous predictors
            Logistic_regression_model = statsmodels.api.Logit(
                df_response, df_predictor[col]
            )
            Logistic_regression_model_fitted = Logistic_regression_model.fit()

            # Get the stats
            t_value = round(Logistic_regression_model_fitted.tvalues[0], 6)
            p_value = "{:.6e}".format(Logistic_regression_model_fitted.pvalues[0])

            # Plot the figure
            fig = px.histogram(x=df_response, y=df_predictor[col], histfunc="count")
            fig.update_layout(
                title=f"Variable: {col}: (t-value={t_value}) (p-value={p_value})",
                xaxis_title=f"Variable: {col}",
                yaxis_title="y",
            )
            fig.write_html(
                file=f"/Users/Vincentwang/PycharmProjects/BDA696-Python-Project-Wenjin/Homework/Data/hw_4_cont_response_cont_predictor_{df_predictor.columns.get_loc(col)}.html",
                include_plotlyjs="cdn",
            )
        else:
            None
    return


# Random Forest Variable importance ranking (continuous predictors only)
def rf_variable_ranking(response_name, df_transformed, df):
    if len(pd.unique(df[response_name])) > 2:  # continuous response
        rf = RandomForestRegressor(n_estimators=100, random_state=42)
        rf.fit(
            df_transformed.loc[:, df.columns != response_name],
            df_transformed[response_name],
        )
        for name, importance in zip(
            df_transformed.loc[:, df.columns != response_name], rf.feature_importances_
        ):
            print(name, "=", importance)
    else:  # categorical response
        rf = RandomForestClassifier(random_state=42)
        rf.fit(
            df_transformed.loc[:, df.columns != response_name],
            df_transformed[response_name],
        )
        for name, importance in zip(
            df_transformed.loc[:, df.columns != response_name], rf.feature_importances_
        ):
            print(name, "=", importance)
    return


# Difference with mean of response
def calculate_difference_cont_predictor(predictor_name, response_name, df):
    bin = pd.cut(df[predictor_name], 10)
    w = bin.value_counts() / sum(bin.value_counts())  # Population Proportion
    w = w.reset_index(name="Population Proportion")
    pop_mean = np.mean(df[response_name])  # population mean
    response_mean = (
        df[response_name]
        .groupby(bin)
        .apply(np.mean)
        .reset_index(name="Mean of response")
    )
    unweigh_diff = np.sum(np.square(response_mean.iloc[:, 1] - pop_mean)) / len(
        response_mean
    )  # unweighted difference with mean
    weigh_diff = np.sum(
        w.iloc[:, 1] * np.square(response_mean.iloc[:, 1] - pop_mean)
    ) / len(
        response_mean
    )  # weighted difference with mean
    print(f"unweighted difference with mean for {predictor_name}: {unweigh_diff}")
    print(f"unweighted difference with mean for {predictor_name}: {weigh_diff}")


def calculate_difference_cat_predictor(predictor_name, response_name, df):
    w = df.groupby(predictor_name).count()[response_name] / sum(
        df.groupby(predictor_name).count()[response_name]
    )  # Population Proportion
    w = w.reset_index(name="Population Proportion")
    pop_mean = np.mean(df[response_name])  # population mean
    bin = df.groupby(predictor_name).apply(np.mean)
    response_mean = bin[response_name].reset_index(name="Mean of response")
    unweigh_diff = np.sum(np.square(response_mean.iloc[:, 1] - pop_mean)) / len(
        response_mean
    )  # unweighted difference with mean
    weigh_diff = np.sum(
        w.iloc[:, 1] * np.square(response_mean.iloc[:, 1] - pop_mean)
    ) / len(
        response_mean
    )  # weighted difference with mean
    print(f"unweighted difference with mean for {predictor_name}: {unweigh_diff}")
    print(f"unweighted difference with mean for {predictor_name}: {weigh_diff}")


def difference_with_mean(df, df_predictor, response_name):
    if len(pd.unique(df[response_name])) > 2:  # continuous response
        for col in df_predictor.columns:
            if df[col].dtypes in ["int64", "float64"]:  # continuous predictor
                calculate_difference_cont_predictor(col, response_name, df)
            else:
                calculate_difference_cat_predictor(col, response_name, df)
    return


def main():
    data = input("Please enter the location of your dataset: ")
    df = pd.read_csv(data)  # load the csv file from your location
    response_name = input("Please enter the response name: ")
    print("\n")
    cat_name = []  # get categorical variable name
    cont_name = []  # get continuous variable name

    print_heading("determine if variable is continuous or categorical")
    check_variable_cat_or_cont(
        df, cat_name, cont_name
    )  # determine if variable is continuous or boolean
    print("\n")

    le = preprocessing.LabelEncoder()  # encode the categorical variables
    df_cat_transformed = df[cat_name].apply(le.fit_transform)
    df_cont = df[np.intersect1d(df.columns, cont_name)]
    df_transformed = pd.concat([df_cat_transformed, df_cont], axis=1)
    df_transformed = df_transformed.dropna()
    if len(pd.unique(df[response_name])) > 2:
        df_response = df[response_name]
    else:
        df_response = df_transformed[response_name]
    df_predictor = df_transformed.loc[:, df.columns != response_name]
    predictor_name = df_predictor.columns

    df_predictor_no_transform = df.loc[:, df.columns != response_name]

    check_response_cat_or_cont(
        df_response
    )  # determine if response is continuous or boolean

    # Make plots and p-value
    try:
        cont_response_cat_predictor(
            df, df_predictor_no_transform, df_response
        )  # distribution plot for cont response and cat predictor
    except:
        print(" ")
    cont_response_cont_predictor(
        df, df_response, df_predictor
    )  # scatter plots for cont response and predictor with p-value
    # cat_response_cont_predictor(df_predictor, df_response, df) # histogram for cat response and cont predictor with p-value
    print_heading("Please not plots have been saved in .../Homework/Data")

    print("\n")
    # Calculate the different ranking algos
    print_heading("Calculate the different ranking algos")
    rf_variable_ranking(
        response_name, df_transformed, df
    )  # Random Forest Variable importance ranking (continuous predictors only)
    difference_with_mean(
        df, df_predictor, response_name
    )  # Difference with mean of response


if __name__ == "__main__":
    sys.exit(main())
