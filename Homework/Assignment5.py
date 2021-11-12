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
from midterm import (
    calculate_cat_cat_corr,
    calculate_cat_cat_diff_mean,
    calculate_cat_cont_corr,
    calculate_cat_cont_diff_mean,
    calculate_cont_cont_corr,
    calculate_cont_cont_diff_mean,
    cat_cat_corr_matricies,
    cat_cont_corr_matricies,
    cont_cont_corr_matricies,
    generate_brute_force_table,
    generate_corr_table,
)
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
    if not os.path.isdir("correlation-tables"):
        os.mkdir("correlation-tables")
    if not os.path.isdir("correlation-matrices"):
        os.mkdir("correlation-matrices")
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

    # Generate HTML cont / cont correlation table
    first_column1, second_column1, third_column1 = calculate_cont_cont_corr(
        df, cont_name
    )
    generate_corr_table(first_column1, second_column1, third_column1, "cont", "cont")

    # Generate HTML cat / cont correlation table
    first_column2, second_column2, third_column2 = calculate_cat_cont_corr(
        df, cat_name, cont_name
    )
    generate_corr_table(first_column2, second_column2, third_column2, "cat", "cont")

    # Generate HTML cat / cat correlation table
    first_column3, second_column3, third_column3 = calculate_cat_cat_corr(df, cat_name)
    generate_corr_table(first_column3, second_column3, third_column3, "cat", "cat")

    # Generate HTML correlation matricies
    cont_cont_corr_matricies(df, cont_name)
    cat_cont_corr_matricies(first_column2, second_column2, third_column2)
    cat_cat_corr_matricies(df, cat_name)

    # Generate HTML cont / cont brute-force table linked with plots
    (
        column11,
        column21,
        column31,
        column41,
        cont_cont_plot_path,
    ) = calculate_cont_cont_diff_mean(df_copy, cont_name, response_name)
    generate_brute_force_table(
        column11, column21, column31, column41, cont_cont_plot_path, "cont", "cont"
    )

    # Generate HTML cat / cont brute-force table linked with plots
    (
        column12,
        column22,
        column32,
        column42,
        cat_cont_plot_path,
    ) = calculate_cat_cont_diff_mean(df_copy, predictor_name, response_name)
    generate_brute_force_table(
        column12, column22, column32, column42, cat_cont_plot_path, "cat", "cont"
    )

    # Generate HTML cat / cat brute-force table linked with plots
    (
        column13,
        column23,
        column33,
        column43,
        cat_cat_plot_path,
    ) = calculate_cat_cat_diff_mean(df_copy, cat_name, response_name)
    generate_brute_force_table(
        column13, column23, column33, column43, cat_cat_plot_path, "cat", "cat"
    )

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
