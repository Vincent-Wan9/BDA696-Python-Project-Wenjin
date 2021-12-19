import os
import sys

import numpy as np
import pandas as pd
import statsmodels.api
from plotly import express as px
from plotly import graph_objects as go
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import confusion_matrix


# determine if response is continuous or boolean
def check_response_cat_or_cont(df, response_name):
    if len(pd.unique(df[response_name])) > 2:
        return "Continuous"
    else:
        return "Boolean"


# determine if predictor is cont or cat
def check_predictor_cat_or_cont(df, predictor_name):
    predictor_type = []
    cat_name = []
    cont_name = []
    for col in predictor_name:
        if pd.api.types.is_numeric_dtype(df[col]):
            predictor_type.append("Continuous")
        else:
            predictor_type.append("Categorical")
    for name, type in zip(predictor_name, predictor_type):
        if type == "Categorical":
            cat_name.append(name)
        elif type == "Continuous":
            cont_name.append(name)
    return predictor_type, cat_name, cont_name


# Make scatter plots for cont response and cont predictor
def cont_response_cont_predictor(df, predictor_name, response_name):
    fig = px.scatter(x=df[predictor_name], y=df[response_name], trendline="ols")
    fig.update_layout(
        title="Continuous by Continuous",
        xaxis_title=f"{predictor_name}",
        yaxis_title=f"{response_name}",
    )
    file_path = f"plots/{response_name}-{predictor_name}-plot.html"
    fig.write_html(file=file_path, include_plotlyjs="cdn")
    return file_path


# Make violin plot for cont response and cat predictor
def cont_response_cat_predictor(df, predictor_name, response_name):
    sub_cat_name = []
    sub_cat_value = []
    sub_cat_count = []
    for i in range(df[predictor_name].nunique()):
        sub_cat_name.append(list(df[response_name].groupby(df[predictor_name]))[i][0])
        sub_cat_value.append(list(df[response_name].groupby(df[predictor_name]))[i][1])
        sub_cat_count.append(
            list(df[response_name].groupby(df[predictor_name]))[i][1].count()
        )

    fig = go.Figure()
    for sub_cat_value, sub_cat_name, sub_cat_count in zip(
        sub_cat_value, sub_cat_name, sub_cat_count
    ):
        fig.add_trace(
            go.Violin(
                x=np.repeat(sub_cat_name, sub_cat_count),
                y=sub_cat_value,
                name=sub_cat_name,
                box_visible=True,
                meanline_visible=True,
            )
        )
    fig.update_layout(
        title="Continuous by Categorical",
        xaxis_title=f"Groupings of {predictor_name}",
        yaxis_title=f"{response_name}",
    )
    file_path = f"plots/{response_name}-{predictor_name}-plot.html"
    fig.write_html(file=file_path, include_plotlyjs="cdn")
    return file_path


# Make violin plot for cat response and cont predictor
def cat_response_cont_predictor(df, predictor_name, response_name):
    sub_cat_name = []
    sub_cat_value = []
    sub_cat_count = []
    for i in range(df[response_name].nunique()):
        sub_cat_name.append(list(df[predictor_name].groupby(df[response_name]))[i][0])
        sub_cat_value.append(list(df[predictor_name].groupby(df[response_name]))[i][1])
        sub_cat_count.append(
            list(df[predictor_name].groupby(df[response_name]))[i][1].count()
        )

    fig = go.Figure()
    for sub_cat_value, sub_cat_name, sub_cat_count in zip(
        sub_cat_value, sub_cat_name, sub_cat_count
    ):
        fig.add_trace(
            go.Violin(
                x=np.repeat(sub_cat_name, sub_cat_count),
                y=sub_cat_value,
                name=sub_cat_name,
                box_visible=True,
                meanline_visible=True,
            )
        )
    fig.update_layout(
        title="Continuous by Categorical ",
        xaxis_title=f"Groupings of {response_name}",
        yaxis_title=f"{predictor_name}",
    )
    file_path = f"plots/{response_name}-{predictor_name}-plot.html"
    fig.write_html(file=file_path, include_plotlyjs="cdn")
    return file_path


# Make heatmap for cat response and cat predictor
def cat_response_cat_predictor(df, predictor_name, response_name):
    conf_matrix = confusion_matrix(df[predictor_name], df[response_name])

    fig = go.Figure(data=go.Heatmap(z=conf_matrix, zmin=0, zmax=conf_matrix.max()))
    fig.update_layout(
        title="Categorical by Categorical",
        xaxis_title=f"{predictor_name}",
        yaxis_title=f"{response_name}",
    )
    file_path = f"plots/{response_name}-{predictor_name}-plot.html"
    fig.write_html(file=file_path, include_plotlyjs="cdn")
    return file_path


# Calculate difference ranking of p-value & t-score
def diff_ranking_p_value_t_score(df, predictor_name, response_name):
    le = preprocessing.LabelEncoder()  # encode the categorical variables
    if not pd.api.types.is_numeric_dtype(df[predictor_name]):
        predictor_dummy = le.fit_transform(df[predictor_name])
        predictor = statsmodels.api.add_constant(predictor_dummy)
    else:
        predictor = statsmodels.api.add_constant(df[predictor_name])

    if len(pd.unique(df[response_name])) > 2:
        linear_regression_model = statsmodels.api.OLS(df[response_name], predictor)
        linear_regression_model_fitted = linear_regression_model.fit()
        t_score = round(linear_regression_model_fitted.tvalues[1], 6)
        p_value = "{:.6e}".format(linear_regression_model_fitted.pvalues[1])

    if not len(pd.unique(df[response_name])) > 2:
        response_dummy = le.fit_transform(df[response_name])
        Logistic_regression_model = statsmodels.api.Logit(response_dummy, predictor)
        Logistic_regression_model_fitted = Logistic_regression_model.fit()
        t_score = round(Logistic_regression_model_fitted.tvalues[1], 6)
        p_value = "{:.6e}".format(Logistic_regression_model_fitted.pvalues[1])
    return t_score, p_value


# Difference with mean of response
def diff_mean(df, predictor_name, response_name):
    pop_mean = np.mean(df[response_name])  # population mean
    if pd.api.types.is_numeric_dtype(df[predictor_name]):  # for continuous predictor
        bin = pd.cut(df[predictor_name], 10)
        w = bin.value_counts() / sum(bin.value_counts())  # Population Proportion
        w = w.reset_index(name="Population Proportion")
        response_mean = (
            df[response_name]
            .groupby(bin)
            .apply(np.mean)
            .reset_index(name="Mean of response")
        )
        unweigh_diff = np.sum(np.square(response_mean.iloc[:, 1] - pop_mean)) / len(
            response_mean
        )
        weigh_diff = np.sum(
            w.iloc[:, 1] * np.square(response_mean.iloc[:, 1] - pop_mean)
        )

    else:  # for categorical predictor
        bin = df.groupby(predictor_name).apply(np.mean)
        w = df.groupby(predictor_name).count()[response_name] / sum(
            df.groupby(predictor_name).count()[response_name]
        )  # Population Proportion
        w = w.reset_index(name="Population Proportion")
        response_mean = bin[response_name].reset_index(name="Mean of response")
        unweigh_diff = np.sum(np.square(response_mean.iloc[:, 1] - pop_mean)) / len(
            response_mean
        )
        weigh_diff = np.sum(
            w.iloc[:, 1] * np.square(response_mean.iloc[:, 1] - pop_mean)
        )

    return unweigh_diff, weigh_diff


def final_diff_mean(df, predictor_name, response_name):
    if len(pd.unique(df[response_name])) > 2:
        None
    else:
        le = preprocessing.LabelEncoder()
        df[response_name] = le.fit_transform(df[response_name])
    unweigh_diff, weigh_diff = diff_mean(df, predictor_name, response_name)

    return unweigh_diff, weigh_diff


# Random Forest Variable importance ranking (continuous predictors only)
def rf_variable_ranking(df, response_name, cat_name, cont_name):
    column1 = []
    column2 = []
    column3 = []
    le = preprocessing.LabelEncoder()  # encode the categorical variables
    if len(pd.unique(df[response_name])) > 2:
        df_response = df[response_name]
    else:
        df_response = le.fit_transform(df[response_name])
    df_response = pd.DataFrame(df_response, columns=[response_name])
    df_cat_transformed = df[cat_name].apply(le.fit_transform)
    df_cont = df[np.intersect1d(df.columns, cont_name)]
    df_transformed = pd.concat([df_cat_transformed, df_cont, df_response], axis=1)

    if len(pd.unique(df[response_name])) > 2:  # continuous response
        rf = RandomForestRegressor(n_estimators=100, random_state=42)
        rf.fit(
            df_transformed.loc[:, df.columns != response_name],
            df_transformed[response_name],
        )
        for name, importance in zip(
            df_transformed.loc[:, df.columns != response_name], rf.feature_importances_
        ):
            if name != response_name:
                column1.append(response_name)
                column2.append(name)
                column3.append(importance)
    else:  # categorical response
        rf = RandomForestClassifier(random_state=42)
        predictor = df.columns.tolist()
        predictor.remove(response_name)
        rf.fit(
            df_transformed.loc[:, predictor],
            df_transformed[response_name],
        )
        for name, importance in zip(
            df_transformed.loc[:, predictor], rf.feature_importances_
        ):
            if name != response_name:
                column1.append(response_name)
                column2.append(name)
                column3.append(importance)
    return column1, column2, column3


def group_all_data(df, response_column, predictor_column, rf_important_column):
    response = []
    predictor = []
    p_value = []
    t_score = []
    unwieghed_diff_mean = []
    weighed_diff_mean = []
    rf_important = []
    URL = []
    df_1 = df.copy()

    for i in range(len(predictor_column)):
        if len(pd.unique(df[response_column[i]])) > 2:
            if pd.api.types.is_numeric_dtype(df[predictor_column[i]]):
                URL.append(
                    cont_response_cont_predictor(
                        df, predictor_column[i], response_column[i]
                    )
                )
            else:
                URL.append(
                    cont_response_cat_predictor(
                        df, predictor_column[i], response_column[i]
                    )
                )
        else:
            if pd.api.types.is_numeric_dtype(df[predictor_column[i]]):
                URL.append(
                    cat_response_cont_predictor(
                        df, predictor_column[i], response_column[i]
                    )
                )
            else:
                URL.append(
                    cat_response_cat_predictor(
                        df, predictor_column[i], response_column[i]
                    )
                )

        t_sco, p_val = diff_ranking_p_value_t_score(
            df, predictor_column[i], response_column[i]
        )
        t_score.append(t_sco)
        p_value.append(p_val)
        response.append(response_column[i])
        predictor.append(predictor_column[i])
        rf_important.append(rf_important_column[i])
        unweigh_diff, weigh_diff = final_diff_mean(
            df_1, predictor_column[i], response_column[i]
        )
        unwieghed_diff_mean.append(unweigh_diff)
        weighed_diff_mean.append(weigh_diff)
    return (
        response,
        predictor,
        p_value,
        t_score,
        unwieghed_diff_mean,
        weighed_diff_mean,
        rf_important,
        URL,
    )


# used to "Link" to plots from the table
def make_clickable(val):
    f_url = os.path.basename(val)
    return '<a href="{}">{}</a>'.format(val, f_url)


# Generate "Brute Force" table
def generate_table(
    response,
    predictor,
    p_value,
    t_score,
    unwieghed_diff_mean,
    weighed_diff_mean,
    rf_important,
    URL,
):
    table = pd.DataFrame(
        list(
            zip(
                response,
                predictor,
                p_value,
                t_score,
                unwieghed_diff_mean,
                weighed_diff_mean,
                rf_important,
                URL,
            )
        ),
        columns=[
            "Response",
            "Predictor",
            "p_value",
            "t_score",
            "Unwieghed_diff_mean",
            "Weighed_diff_mean",
            "rf_important",
            "URL",
        ],
    )
    final_table = table.style.format({"URL": make_clickable}, escape="html")
    final_table = final_table.to_html(
        "final_table.html", render_links=True, escape=False
    )
    # html_file = open("final_table.html", "w")
    # html_file.write(final_table)
    # html_file.close()
    return


def main():
    # create folders to store correlation tables, correlation matrices, "Brute Force" plots
    if not os.path.isdir("plots"):
        os.mkdir("plots")

    answer = input("Do you have your own dataset? (Please answer Yes or No): ")
    if answer == "Yes":
        data = input(
            "Please enter the location of your dataset (make sure it is csv file): "
        )
        response_name = input("Please enter the response name: ")
    elif answer == "No":
        print(
            "Please note that a default dataset 'Telco-Customer_Churn.csv' is used to test the code"
        )
        print("The response in this dataset is 'Churn'")
        data = "./Homework/Data/Telco-Customer-Churn.csv"
        response_name = "Churn"

    df = pd.read_csv(data)  # load the csv file from your location
    # response_type = check_response_cat_or_cont(df, response_name)

    predictor_name = df.columns.tolist()
    predictor_name.remove(response_name)
    predictor_type, cat_name, cont_name = check_predictor_cat_or_cont(
        df, predictor_name
    )

    response_column, predictor_column, rf_important_column = rf_variable_ranking(
        df, response_name, cat_name, cont_name
    )
    (
        response,
        predictor,
        p_value,
        t_score,
        unweighed_diff_mean,
        weighed_diff_mean,
        rf_important,
        url,
    ) = group_all_data(df, response_column, predictor_column, rf_important_column)
    generate_table(
        response,
        predictor,
        p_value,
        t_score,
        unweighed_diff_mean,
        weighed_diff_mean,
        rf_important,
        url,
    )


if __name__ == "__main__":
    sys.exit(main())
