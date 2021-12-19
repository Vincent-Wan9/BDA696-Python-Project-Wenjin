# isort: skip_file
import os
import sys

import pandas as pd
import sqlalchemy
from Assignment4 import (
    check_predictor_cat_or_cont,
    generate_table,
    group_all_data,
    rf_variable_ranking,
)
from midterm import finalized_html_file
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB


def print_heading(title):
    print("*" * 80)
    print(title)
    return


def transform_response(df, response_name):
    if len(pd.unique(df[response_name])) > 2:
        pass
    else:
        le = preprocessing.LabelEncoder()  # used to encode the categorical response
        df[response_name] = le.fit_transform(df[response_name])
    return df


def transform_predictor(df, predictor_name):
    le = preprocessing.LabelEncoder()
    for i in range(len(df[predictor_name].columns)):
        if pd.api.types.is_numeric_dtype(df[df[predictor_name].columns[i]]):
            pass
        else:
            df[df[predictor_name].columns[i]] = le.fit_transform(
                df[df[predictor_name].columns[i]]
            )
    return df


def main():
    if not os.path.isdir("plots"):
        os.mkdir("plots")
    if not os.path.isdir("brute-force-plots"):
        os.mkdir("brute-force-plots")

    db_user = "root"
    db_pass = "root"  # pragma: allowlist secret
    db_host = "localhost"
    db_database = "baseball"
    connect_string = (
        f"mariadb+mariadbconnector://{db_user}:{db_pass}@{db_host}/{db_database}"
    )

    sql_engine = sqlalchemy.create_engine(connect_string)

    query = """SELECT * FROM hw5_table"""
    df = pd.read_sql_query(query, sql_engine)
    df["local_date"] = pd.to_datetime(df.local_date)
    df = df.sort_values(by="local_date")

    df = df.drop(["game_id", "home_team_id", "away_team_id", "local_date"], axis=1)
    df_copy = df.copy()

    response_name = "home_team_wins"
    df_copy = transform_response(df_copy, response_name)

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

    # generate finalized html file
    finalized_html_file(df, df_copy, cont_name, cat_name, predictor_name, response_name)

    # Logistic Regression
    df_copy = transform_predictor(df_copy, predictor_name)
    X_train, X_test, y_train, y_test = train_test_split(
        df_copy[predictor_name],
        df_copy[response_name],
        test_size=0.2,
        shuffle=False,
        random_state=42,
    )
    lr = LogisticRegression(max_iter=1000)
    logistic_reg = lr.fit(X_train, y_train)
    lr_score = logistic_reg.score(X_test, y_test)  # accuracy
    print_heading("Logistic Regression: ")
    print(f"The accuracy is {lr_score}")

    # Naive Bayes
    gnb = GaussianNB()
    naive_bayes = gnb.fit(X_train, y_train)
    nb_score = naive_bayes.score(X_test, y_test)  # accuracy
    print_heading("Naive Bayes: ")
    print(f"The accuracy is {nb_score}")

    print_heading("Comparison of two ML models performance: ")
    if lr_score > nb_score:
        print("Logistic Regression is better")
    else:
        print("Naive Bayes is better")


if __name__ == "__main__":
    sys.exit(main())
