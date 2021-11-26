# isort: skip_file

import os
import sys
from itertools import combinations

import numpy as np
import pandas as pd
from cat_correlation import cat_cont_correlation_ratio, cat_correlation, fill_na
from Assignment4 import (
    cont_response_cont_predictor,
    cont_response_cat_predictor,
    cat_response_cat_predictor,
)
from plotly import figure_factory as ff
from scipy import stats
from sklearn import preprocessing
import webbrowser


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


# check and replace nan value with zero for each continuous variable in the data
def replace_nan_value(df):
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]) and np.isnan(df[col]).any():
            df[col] = fill_na(df[col])
    return df


# Calculate pearson correlation coefficient for Cont / Cont pairs
def calculate_cont_cont_corr(df, cont_name):
    first_column = []
    second_column = []
    third_column = []
    fourth_column = []
    for i in range(len(cont_name)):
        for k in range(len(cont_name)):
            if k > i:
                first_column.append(cont_name[i])
                second_column.append(cont_name[k])
                third_column.append(
                    round(stats.pearsonr(df[cont_name[i]], df[cont_name[k]])[0], 3)
                )
                fourth_column.append(
                    cont_response_cont_predictor(df, cont_name[i], cont_name[k])
                )
    return first_column, second_column, third_column, fourth_column


# Calculate correlation ratio for Cont / Cat pairs
def calculate_cat_cont_corr(df, cat_name, cont_name):
    first_column = []
    second_column = []
    third_column = []
    fourth_column = []
    for i in range(len(cat_name)):
        for k in range(len(cont_name)):
            first_column.append(cat_name[i])
            second_column.append(cont_name[k])
            third_column.append(
                round(cat_cont_correlation_ratio(df[cat_name[i]], df[cont_name[k]]), 3)
            )
            fourth_column.append(
                cont_response_cat_predictor(df, cat_name[i], cont_name[k])
            )
    return first_column, second_column, third_column, fourth_column


# Calculate correlation ratios for Cat / Cat pairs
def calculate_cat_cat_corr(df, cat_name):
    first_column = []
    second_column = []
    third_column = []
    fourth_column = []
    for i in range(len(cat_name)):
        for k in range(len(cat_name)):
            if k > i:
                first_column.append(cat_name[i])
                second_column.append(cat_name[k])
                third_column.append(
                    round(cat_correlation(df[cat_name[i]], df[cat_name[k]]), 3)
                )
                fourth_column.append(
                    cat_response_cat_predictor(df, cat_name[i], cat_name[k])
                )
    return first_column, second_column, third_column, fourth_column


# Generate correlation table
def generate_corr_table(
    first_column, second_column, third_column, fourth_column, var_type1, var_type2
):
    df_corr = pd.DataFrame(
        list(zip(first_column, second_column, third_column, fourth_column)),
        columns=["1st Predictor", "2nd Predictor", "Coefficient Value", "URI"],
    )
    df_corr_sorted = df_corr.sort_values("Coefficient Value", ascending=False)
    corr_table = df_corr_sorted.reset_index(drop=True)
    corr_table = corr_table.style.format({"URI": make_clickable}, escape="html")
    corr_table_html = corr_table.to_html()
    return corr_table_html


# Generate correlation matrices
def corr_matrices(df, cat_name, cont_name, var_type1, var_type2):
    if var_type1 == "cont" and var_type2 == "cont":
        corr = df[cont_name].corr(method="pearson")
        corr = corr[
            corr.columns[::-1]
        ]  # used to align index and column on corr matrices

    if var_type1 == "cat" and var_type2 == "cont":
        (
            first_column,
            second_column,
            third_column,
            fourth_column,
        ) = calculate_cat_cont_corr(df, cat_name, cont_name)
        df_corr = pd.DataFrame(
            list(zip(first_column, second_column, third_column)),
            columns=["1st Predictor", "2nd Predictor", "Coefficient Value"],
        )

        corr = df_corr.pivot(
            index="1st Predictor", columns="2nd Predictor", values="Coefficient Value"
        )

    if var_type1 == "cat" and var_type2 == "cat":
        first_column = []
        second_column = []
        third_column = []
        for i in range(len(cat_name)):
            for k in range(len(cat_name)):
                first_column.append(cat_name[i])
                second_column.append(cat_name[k])
                third_column.append(
                    round(cat_correlation(df[cat_name[i]], df[cat_name[k]]), 3)
                )

        corr_table = pd.DataFrame(
            list(zip(first_column, second_column, third_column)),
            columns=["1st Cat Predictor", "2nd Cat Predictor", "Cramer'V"],
        )

        corr = corr_table.pivot(
            index="1st Cat Predictor", columns="2nd Cat Predictor", values="Cramer'V"
        )
        corr = corr[
            corr.columns[::-1]
        ]  # used to align index and column on corr matrices
    return corr


# Generate correlation matrices plot
def generate_corr_matrices_plot(corr, var_type1, var_type2):
    fig = ff.create_annotated_heatmap(
        z=corr.values,
        x=corr.columns.values.tolist(),
        y=corr.index.values.tolist(),
        zmin=-1,
        zmax=1,
        colorscale="RdBu",
        showscale=True,
        hoverongaps=True,
    )

    fig.update_layout(
        title=f"Correlation Matrices: {var_type1} Predictor by {var_type2} Predictor"
    )
    fig["layout"]["xaxis"]["side"] = "top"
    corr_matrices_html = fig.to_html()
    return corr_matrices_html


# Make plot to see relationship of combined predictors
def generate_brute_force_plot(
    column1_for_plot,
    column2_for_plot,
    column3_for_plot,
    pop_mean,
    pred_name1,
    pred_name2,
):
    df_for_plot_table = pd.DataFrame(
        list(zip(column1_for_plot, column2_for_plot, column3_for_plot)),
        columns=["1st Predictor", "2nd Predictor", "Bin Mean"],
    )

    table = df_for_plot_table.pivot(
        index="1st Predictor", columns="2nd Predictor", values="Bin Mean"
    )

    fig = ff.create_annotated_heatmap(
        z=table.values,
        x=table.columns.values.tolist(),
        y=table.index.values.tolist(),
        zmid=pop_mean,
        colorscale="RdBu",
        showscale=True,
        hoverongaps=True,
    )

    fig.update_layout(
        title=f"{pred_name2} & {pred_name1} - Relationship with Response (Bin Average)",
        xaxis_title=f"{pred_name2}",
        yaxis_title=f"{pred_name1}",
    )
    fig["layout"]["xaxis"]["side"] = "top"
    file_path = f"brute-force-plots/{pred_name2}-{pred_name1}-plot.html"
    fig.write_html(file=file_path, include_plotlyjs="cdn")
    return file_path


# calculate difference with mean of response for cont and cont predictors
def calculate_cont_cont_diff_mean(df, cont_name, response_name):
    column1 = []
    column2 = []
    column3 = []
    column4 = []
    cont_cont_plot_path = []  # used to store plot path
    pop_mean = np.mean(df[response_name])  # population mean
    cont_cont_combined = list(combinations(df[cont_name], 2))
    for i in range(len(cont_cont_combined)):
        column1_for_plot = []  # used to store bin of 1st cont predictor
        column2_for_plot = []  # used to store bin of 2nd cont predictor
        column3_for_plot = []  # used to store bin mean
        mean_diff = []
        mean_diff_weighed = []
        for k in range(2):
            if k == 0:
                bin1 = pd.cut(
                    df[cont_cont_combined[i][k]], 10
                )  # cut the 1st predictor into 10 bins
            if k == 1:
                bin2 = pd.cut(
                    df[cont_cont_combined[i][k]], 10
                )  # cut the 2nd predictor into 10 bins

        for a in range(10):
            for b in range(10):
                bin1_index = list(df[response_name].groupby(bin1))[a][
                    1
                ].index  # the a th bin in bin1
                bin2_index = list(df[response_name].groupby(bin2))[b][
                    1
                ].index  # the b th bin in bin2
                final_bin_index = bin1_index.intersection(
                    bin2_index
                )  # return index value
                w = len(final_bin_index) / sum(
                    bin1.value_counts()
                )  # return population proportion for each final bin
                bin_mean = np.mean(
                    df[response_name].get(final_bin_index)
                )  # final bin mean
                mean_diff.append(np.square(bin_mean - pop_mean))
                mean_diff_weighed.append(w * np.square(bin_mean - pop_mean))
                column1_for_plot.append(
                    list(df[response_name].groupby(bin1))[a][0].mid
                )  # extract center point of each bin
                column2_for_plot.append(list(df[response_name].groupby(bin2))[b][0].mid)
                column3_for_plot.append(bin_mean)

        column1.append(cont_cont_combined[i][0])
        column2.append(cont_cont_combined[i][1])
        column3.append(
            round(
                np.nansum(mean_diff) / len([x for x in mean_diff if str(x) != "nan"]), 3
            )
        )
        column4.append(round(np.nansum(mean_diff_weighed), 3))
        cont_cont_plot_path.append(
            generate_brute_force_plot(
                column1_for_plot,
                column2_for_plot,
                column3_for_plot,
                pop_mean,
                cont_cont_combined[i][0],
                cont_cont_combined[i][1],
            )
        )
    return column1, column2, column3, column4, cont_cont_plot_path


# calculate difference with mean of response for cat and cont predictors
def calculate_cat_cont_diff_mean(df, predictor_name, response_name):
    cat_cont_combined = []
    column1 = []
    column2 = []
    column3 = []
    column4 = []
    cat_cont_plot_path = []  # used to store plot path
    pop_mean = np.mean(df[response_name])  # population mean

    all_combined = list(combinations(df[predictor_name], 2))
    for i in range(len(all_combined)):
        type = []
        for k in range(2):
            type.append(pd.api.types.is_numeric_dtype(df[all_combined[i][k]]))
        if type[0] != type[1]:
            cat_cont_combined.append(all_combined[i])

    for i in range(len(cat_cont_combined)):
        column1_for_plot = []  # used to store bin of cat predictor
        column2_for_plot = []  # used to store bin of cont predictor
        column3_for_plot = []  # used to store bin mean
        mean_diff = []
        mean_diff_weighed = []
        for k in range(2):
            if pd.api.types.is_numeric_dtype(df[cat_cont_combined[i][k]]):
                bin2 = pd.cut(
                    df[cat_cont_combined[i][k]], 10
                )  # cut the cont predictor into 10 bins
            elif not pd.api.types.is_numeric_dtype(df[cat_cont_combined[i][k]]):
                cat_pred_name = cat_cont_combined[i][k]

        for a in range(df[cat_pred_name].nunique()):  # cat predictor
            for b in range(10):  # cont predictor
                bin1_index = list(df[response_name].groupby(df[cat_pred_name]))[a][
                    1
                ].index  # the a th bin in cat pred
                bin2_index = list(df[response_name].groupby(bin2))[b][
                    1
                ].index  # the b th bin in cont predictor
                final_bin_index = bin1_index.intersection(
                    bin2_index
                )  # return index value
                w = len(final_bin_index) / sum(
                    bin2.value_counts()
                )  # return population proportion for each final bin
                bin_mean = np.mean(
                    df[response_name].get(final_bin_index)
                )  # final bin mean
                mean_diff.append(np.square(bin_mean - pop_mean))
                mean_diff_weighed.append(w * np.square(bin_mean - pop_mean))
                column1_for_plot.append(
                    list(df[response_name].groupby(df[cat_pred_name]))[a][0]
                )
                column2_for_plot.append(
                    list(df[response_name].groupby(bin2))[b][0].mid
                )  # extract center point of each bin
                column3_for_plot.append(bin_mean)

        column1.append(cat_cont_combined[i][0])
        column2.append(cat_cont_combined[i][1])
        column3.append(
            round(
                np.nansum(mean_diff) / len([x for x in mean_diff if str(x) != "nan"]), 3
            )
        )
        column4.append(round(np.nansum(mean_diff_weighed), 3))
        cat_cont_plot_path.append(
            generate_brute_force_plot(
                column1_for_plot,
                column2_for_plot,
                column3_for_plot,
                pop_mean,
                cat_cont_combined[i][0],
                cat_cont_combined[i][1],
            )
        )
    return column1, column2, column3, column4, cat_cont_plot_path


# calculate difference with mean of response for cat and cat predictors
def calculate_cat_cat_diff_mean(df, cat_name, response_name):
    column1 = []  # used to store 1st cat predictor name
    column2 = []  # used to store 2nd cat predictor name
    column3 = []  # used to store unweighted difference with mean of response
    column4 = []  # used to store weighted difference with mean of response
    cat_cat_plot_path = []  # used to store plot path
    pop_mean = np.mean(df[response_name])  # population mean

    cat_cat_combined = list(combinations(df[cat_name], 2))
    for i in range(len(cat_cat_combined)):
        column1_for_plot = []  # used to store bin of 1st cat predictor
        column2_for_plot = []  # used to store bin of 2nd cat predictor
        column3_for_plot = []  # used to store bin mean
        mean_diff = []
        mean_diff_weighed = []
        cat_pred_name1 = cat_cat_combined[i][0]
        cat_pred_name2 = cat_cat_combined[i][1]

        for a in range(df[cat_pred_name1].nunique()):  # 1st cat predictor
            for b in range(df[cat_pred_name2].nunique()):  # 2nd cat predictor
                bin1_index = list(df[response_name].groupby(df[cat_pred_name1]))[a][
                    1
                ].index  # the a th bin in cat pred
                bin2_index = list(df[response_name].groupby(df[cat_pred_name2]))[b][
                    1
                ].index  # the b th bin in cat pred
                final_bin_index = bin1_index.intersection(
                    bin2_index
                )  # return index value
                w = len(final_bin_index) / sum(
                    df[response_name].value_counts()
                )  # return population proportion for each final bin
                bin_mean = np.mean(
                    df[response_name].get(final_bin_index)
                )  # final bin mean
                mean_diff.append(np.square(bin_mean - pop_mean))
                mean_diff_weighed.append(w * np.square(bin_mean - pop_mean))
                column1_for_plot.append(
                    list(df[response_name].groupby(df[cat_pred_name1]))[a][0]
                )
                column2_for_plot.append(
                    list(df[response_name].groupby(df[cat_pred_name2]))[b][0]
                )
                column3_for_plot.append(bin_mean)

        column1.append(cat_cat_combined[i][0])
        column2.append(cat_cat_combined[i][1])
        column3.append(
            round(
                np.nansum(mean_diff) / len([x for x in mean_diff if str(x) != "nan"]), 3
            )
        )
        column4.append(round(np.nansum(mean_diff_weighed), 3))
        cat_cat_plot_path.append(
            generate_brute_force_plot(
                column1_for_plot,
                column2_for_plot,
                column3_for_plot,
                pop_mean,
                cat_pred_name1,
                cat_pred_name2,
            )
        )
    return column1, column2, column3, column4, cat_cat_plot_path


# used to "Link" to plots from the table
def make_clickable(val):
    f_url = os.path.basename(val)
    return '<a href="{}">{}</a>'.format(val, f_url)


# Generate "Brute Force" table
def generate_brute_force_table(
    column1, column2, column3, column4, plot_path, var_type1, var_type2
):
    df_brute_force = pd.DataFrame(
        list(zip(column1, column2, column3, column4, plot_path)),
        columns=[
            "1st Predictor",
            "2nd Predictor",
            "Unweighted Difference with Mean",
            "Weighted Difference with Mean",
            "URI",
        ],
    )
    df_brute_force_sorted = df_brute_force.sort_values(
        "Weighted Difference with Mean", ascending=False
    )
    brute_force_table = df_brute_force_sorted.reset_index(drop=True)
    brute_force_table = brute_force_table.style.format(
        {"URI": make_clickable}, escape="html"
    )
    brute_force_table_html = brute_force_table.to_html()
    return brute_force_table_html


# Consolidate all outputs into a single HTML file
def finalized_html_file(
    df, df_copy, cont_name, cat_name, predictor_name, response_name
):
    file = open("midterm_analysis.html", "w")
    header = """<!DOCTYPE html>
        <html>
        <head>
            <link rel="stylesheet" type="text/css" href="style.css">
            <title>Midterm</title>
        </head>
        <body>
        <h1>Predictor Analysis</h1>
        <h2>Continuous / Continuous Predictor Pairs</h2>
        """
    file.write(header)

    # Continuous/Continuous Predictor Pairs
    file.write("<h3>Correlation Table</h3>")
    (
        first_column1,
        second_column1,
        third_column1,
        fourth_column1,
    ) = calculate_cont_cont_corr(df, cont_name)
    file.write(
        generate_corr_table(
            first_column1, second_column1, third_column1, fourth_column1, "cont", "cont"
        )
    )  # add corr table

    file.write("<h3>Correlation Matrices</h3>")
    corr = corr_matrices(df, cat_name, cont_name, "cont", "cont")
    file.write(generate_corr_matrices_plot(corr, "cont", "cont"))  # add corr matrices

    file.write("<h3>'Brute Force' Table</h3>")
    (
        column11,
        column21,
        column31,
        column41,
        cont_cont_plot_path,
    ) = calculate_cont_cont_diff_mean(df_copy, cont_name, response_name)
    file.write(
        generate_brute_force_table(
            column11, column21, column31, column41, cont_cont_plot_path, "cont", "cont"
        )
    )  # add brute-force table

    # Categorical/Continuous Predictor Pairs
    file.write("<h2> Categorical / Continuous Predictor Pairs </h2>")
    file.write("<h3>Correlation Table</h3>")
    (
        first_column2,
        second_column2,
        third_column2,
        fourth_column2,
    ) = calculate_cat_cont_corr(df, cat_name, cont_name)
    file.write(
        generate_corr_table(
            first_column2, second_column2, third_column2, fourth_column2, "cat", "cont"
        )
    )  # add corr table

    file.write("<h3>Correlation Matrices</h3>")
    corr = corr_matrices(df, cat_name, cont_name, "cat", "cont")
    file.write(generate_corr_matrices_plot(corr, "cat", "cont"))  # add corr matrices

    file.write("<h3>'Brute Force' Table</h3>")
    (
        column12,
        column22,
        column32,
        column42,
        cat_cont_plot_path,
    ) = calculate_cat_cont_diff_mean(df_copy, predictor_name, response_name)
    file.write(
        generate_brute_force_table(
            column12, column22, column32, column42, cat_cont_plot_path, "cat", "cont"
        )
    )  # add brute-force table

    # Categorical/Categorical Predictor Pairs
    file.write("<h2> Categorical / Categorical Predictor Pairs </h2>")
    file.write("<h3>Correlation Table</h3>")
    (
        first_column3,
        second_column3,
        third_column3,
        fourth_column3,
    ) = calculate_cat_cat_corr(df, cat_name)
    file.write(
        generate_corr_table(
            first_column3, second_column3, third_column3, fourth_column3, "cat", "cat"
        )
    )  # add corr table

    file.write("<h3>Correlation Matrices</h3>")
    corr = corr_matrices(df, cat_name, cont_name, "cat", "cat")
    file.write(generate_corr_matrices_plot(corr, "cat", "cat"))  # add corr matrices

    file.write("<h3>'Brute Force' Table</h3>")
    (
        column13,
        column23,
        column33,
        column43,
        cat_cat_plot_path,
    ) = calculate_cat_cat_diff_mean(df_copy, cat_name, response_name)
    file.write(
        generate_brute_force_table(
            column13, column23, column33, column43, cat_cat_plot_path, "cat", "cat"
        )
    )  # add corr matrices

    footer = """<p>BDA696 Midterm, Fall 2021, by Wenjin Wang</p>
        </body>
        </html>
        """
    file.write(footer)

    webbrowser.open("assignment5_analysis.html", new=2)
    return


def main():
    # create folder to store "Brute Force" plots
    if not os.path.isdir("brute-force-plots"):
        os.mkdir("brute-force-plots")
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
    df = replace_nan_value(df)
    response_type = check_response_cat_or_cont(df, response_name)
    if response_type == "Boolean":
        le = preprocessing.LabelEncoder()  # used to encode the categorical response
        df[response_name] = le.fit_transform(df[response_name])

    predictor_name = df.columns.tolist()
    predictor_name.remove(response_name)
    predictor_type, cat_name, cont_name = check_predictor_cat_or_cont(
        df, predictor_name
    )

    # generate finalized html file
    finalized_html_file(df, cont_name, cat_name, predictor_name, response_name)


if __name__ == "__main__":
    sys.exit(main())
