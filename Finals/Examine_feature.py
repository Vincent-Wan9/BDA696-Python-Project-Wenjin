# isort: skip_file
# flake8: noqa
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


def main():
    if not os.path.isdir("Finals/plots"):
        os.mkdir("Finals/plots")
    if not os.path.isdir("Finals/brute-force-plots"):
        os.mkdir("Finals/brute-force-plots")

    db_user = "root"
    db_pass = "root"  # pragma: allowlist secret
    db_host = "localhost"
    db_database = "baseball"
    connect_string = (
        f"mariadb+mariadbconnector://{db_user}:{db_pass}@{db_host}/{db_database}"
    )

    sql_engine = sqlalchemy.create_engine(connect_string)

    query = """SELECT * FROM final_features_60_day"""
    df = pd.read_sql_query(query, sql_engine)
    df = df.sort_values(by="local_date")

    df = df.drop(["game_id", "home_team_id", "away_team_id", "local_date"], axis=1)
    df_copy = df.copy()

    response_name = "Home_team_wins"

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


if __name__ == "__main__":
    sys.exit(main())
