# isort: skip_file

import sys
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlalchemy
from sklearn import preprocessing
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

# import seaborn as sns


# ML model fitting
def model_fitting(X_train, y_train):
    models = [
        RandomForestClassifier(),
        LogisticRegression(max_iter=10000),
        GaussianNB(),
        DecisionTreeClassifier(),
        KNeighborsClassifier(),
        SVC(),
    ]
    names = [
        "Random Forest",
        "Logistic Regression",
        "Naive Bayes",
        "Decision Tree",
        "KNN",
        "SVM",
    ]
    accuracy = []
    precision = []
    recall = []
    time_used = []
    for model, name in zip(models, names):
        start = time.time()
        for score in ["accuracy", "precision", "recall"]:
            if score == "accuracy":
                accuracy.append(
                    cross_val_score(model, X_train, y_train, scoring=score, cv=10)
                )
            if score == "precision":
                precision.append(
                    cross_val_score(model, X_train, y_train, scoring=score, cv=10)
                )
            if score == "recall":
                recall.append(
                    cross_val_score(model, X_train, y_train, scoring=score, cv=10)
                )
        time_used.append(time.time() - start)
    return accuracy, precision, recall, time_used


# Performance Comparison Among 6 Models (performance metric: Accuracy)
def performance_compare_on_train_data(accuracy):
    name_lst = ["RF", "LR", "NB", "DT", "KNN", "SVM"]
    accuracy_t = np.transpose(accuracy)
    accuracy_perf_df = pd.DataFrame(data=accuracy_t, columns=name_lst)
    ax = plt.subplot(1, 1, 1)
    ax.boxplot(accuracy_perf_df.values, vert=0)
    ax.set_yticklabels(name_lst)
    ax.set_title("accuracy_fs")
    plt.show()


# Selected Model Performance on test data: SVM, LR, NB
def performance_compare_on_test_data(X_train, y_train, X_test, y_test):
    accuracy_rate = []

    # Support Vector Machine
    SVM = SVC()
    SVM.fit(X_train, y_train)
    SVM_predictions = SVM.predict(X_test)
    accuracy_rate.append(accuracy_score(y_test, SVM_predictions))

    # Logistic Regression
    LR = LogisticRegression(max_iter=10000)
    LR.fit(X_train, y_train)
    LR_predictions = LR.predict(X_test)
    accuracy_rate.append(accuracy_score(y_test, LR_predictions))

    # Naive Bayes
    NB = GaussianNB()
    NB.fit(X_train, y_train)
    NB_predictions = NB.predict(X_test)
    accuracy_rate.append(accuracy_score(y_test, NB_predictions))

    # get best model prediction
    ML_model = ["SVM", "LR", "NB"]
    best_ml_model = ML_model[accuracy_rate.index(max(accuracy_rate))]

    if best_ml_model == "SVM":
        best_ml_predictions = SVM_predictions
    elif best_ml_model == "LR":
        best_ml_predictions = LR_predictions
    elif best_ml_model == "NB":
        best_ml_predictions = NB_predictions

    return best_ml_model, max(accuracy_rate), best_ml_predictions


# final model performance
def final_model_performance(
    best_ml_models, best_accuracy_rates, best_ml_predictions, y_tests
):
    dataset = ["Raw Data", "Feature Selection", "PCA"]

    best_ml_model = best_ml_models[best_accuracy_rates.index(max(best_accuracy_rates))]
    best_accuracy_rate = max(best_accuracy_rates)
    best_ml_prediction = best_ml_predictions[
        best_accuracy_rates.index(max(best_accuracy_rates))
    ]
    y_test = y_tests[best_accuracy_rates.index(max(best_accuracy_rates))]
    best_data = dataset[best_accuracy_rates.index(max(best_accuracy_rates))]

    best_model_accuracy_data = [best_ml_model, best_accuracy_rate, best_data]
    classify_report = classification_report(y_test, best_ml_prediction)

    # Confusion Matrix
    cm = confusion_matrix(y_test, best_ml_prediction)
    fig, ax = plt.subplots(figsize=(7.5, 7.5))
    ax.matshow(cm, cmap=plt.cm.Blues, alpha=0.3)
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            ax.text(x=j, y=i, s=cm[i, j], va="center", ha="center", size="xx-large")
    plt.title("Confusion matrix of the classifier")
    plt.xlabel("Predictions")
    plt.ylabel("Actuals")
    plt.show()
    return best_model_accuracy_data, classify_report


def main():
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

    """
    # Data Pre-processing
    df.head()
    df.info()  # check NA value and data type
    pd.DataFrame(df.nunique()).sort_values(0).rename({0: 'Unique Values'}, axis=1)  # check constant variance

    # Response "Home_team_wins" distribution to see if it is balanced data
    plt.figure(figsize=(15, 5))
    plt.subplot(121)
    sns.histplot(data=df, x='Home_team_wins', stat="percent", discrete=True)
    plt.xticks([0, 1])
    plt.show()
    """

    # The data is time series, sorting it out based on date for later train/test split
    df = df.sort_values(by="local_date")

    # Remove useless features
    df = df.drop(["game_id", "home_team_id", "away_team_id", "local_date"], axis=1)

    """
    # Make feature importance plot based on random forest
    clf = RandomForestClassifier()
    clf.fit(df.drop('Home_team_wins', axis=1), df['Home_team_wins'])

    plt.style.use('seaborn-whitegrid')
    importance = clf.feature_importances_
    importance = pd.DataFrame(importance, index=df.drop('Home_team_wins', axis=1).columns, columns=["Importance"])
    importance.sort_values(by='Importance', ascending=True).plot(kind='barh', figsize=(20, len(importance)/2))
    plt.show()
    """

    # Feature selection: remove less important and highly correlated features
    feature_select_df = df.copy()

    # by random forest
    feature_select_df = feature_select_df.drop(
        ["Triple_Play_diff", "p_Triple_Play_diff"], axis=1
    )
    # by Difference with mean of response
    feature_select_df = feature_select_df.drop(
        ["B_diff", "p_Walk_diff", "S_diff"], axis=1
    )
    # by p-value
    feature_select_df = feature_select_df.drop(
        [
            "HBP_diff",
            "Double_Play_diff",
            "PA_per_SO_diff",
            "p_Double_Play_diff",
            "p_Pop_Out_diff",
            "p_Line_Out_diff",
            "p_day_diff",
        ],
        axis=1,
    )
    # by correlation
    feature_select_df = feature_select_df.drop(
        [
            "RC_diff",
            "GPA_diff",
            "TOB_diff",
            "ISO_diff",
            "BABIP_diff",
            "OBP_diff",
            "TB_diff",
            "p_WHIP_diff",
            "p_pitchesThrown_diff",
            "p_Hit_diff",
            "H_diff",
            "p_Fly_Out_diff",
            "p_atBat_diff",
            "slug_diff",
            "BA_diff",
            "BB_to_K_diff",
            "p_bull_diff",
        ],
        axis=1,
    )

    # Perform PCA and graphing the Variance for each feature
    std_scale = preprocessing.StandardScaler().fit(df.drop("Home_team_wins", axis=1))
    X = std_scale.transform(df.drop("Home_team_wins", axis=1))

    pca = PCA(0.80, whiten=True)  # Keep 80% explained variance

    fit = pca.fit(X)

    # Graphing the variance for each feature
    plt.style.use("seaborn-whitegrid")
    plt.figure(figsize=(25, 7))
    plt.xlabel("PCA Feature")
    plt.ylabel("Variance")
    plt.title("PCA for Whole Dataset")
    plt.bar(range(0, fit.explained_variance_ratio_.size), fit.explained_variance_ratio_)
    # plt.show()

    # Get pca transformed data
    pca_data = pca.transform(X)
    pca_data = np.array(pca_data)

    # Three datasets to try: [1] raw data [2] feature selection [3] PCA

    # Split raw data into 70% training and 30% test
    X_train_raw, X_test_raw, y_train_raw, y_test_raw = train_test_split(
        df.drop("Home_team_wins", axis=1),
        df["Home_team_wins"],
        test_size=0.3,
        shuffle=False,
    )

    # Split feature selection data into 70% training and 30% test
    X_train_fs, X_test_fs, y_train_fs, y_test_fs = train_test_split(
        feature_select_df.drop("Home_team_wins", axis=1),
        feature_select_df["Home_team_wins"],
        test_size=0.3,
        shuffle=False,
    )

    # Split PCA data into 70% training and 30% test
    X_train_pca, X_test_pca, y_train_pca, y_test_pca = train_test_split(
        pca_data,
        df["Home_team_wins"],
        test_size=0.3,
        shuffle=False,
    )

    # output of ML model fitting
    accuracy_raw, precision_raw, recall_raw, time_used_raw = model_fitting(
        X_train_raw, y_train_raw
    )
    accuracy_fs, precision_fs, recall_fs, time_used_fs = model_fitting(
        X_train_fs, y_train_fs
    )
    accuracy_pca, precision_pca, recall_pca, time_used_pca = model_fitting(
        X_train_pca, y_train_pca
    )

    """
    # compare performance by looking at the generated boxplots
    performance_compare_on_train_data(accuracy_raw)
    performance_compare_on_train_data(accuracy_fs)
    performance_compare_on_train_data(accuracy_pca)
    """

    # extract best model and its related info by each dataset
    (
        best_ml_model_raw,
        best_accuracy_rate_raw,
        best_ml_predictions_raw,
    ) = performance_compare_on_test_data(
        X_train_raw, y_train_raw, X_test_raw, y_test_raw
    )
    (
        best_ml_model_fs,
        best_accuracy_rate_fs,
        best_ml_predictions_fs,
    ) = performance_compare_on_test_data(X_train_fs, y_train_fs, X_test_fs, y_test_fs)
    (
        best_ml_model_pca,
        best_accuracy_rate_pca,
        best_ml_predictions_pca,
    ) = performance_compare_on_test_data(
        X_train_pca, y_train_pca, X_test_pca, y_test_pca
    )

    best_ml_models = [best_ml_model_raw, best_ml_model_fs, best_ml_model_pca]
    best_accuracy_rates = [
        best_accuracy_rate_raw,
        best_accuracy_rate_fs,
        best_accuracy_rate_pca,
    ]
    best_ml_predictions = [
        best_ml_predictions_raw,
        best_ml_predictions_fs,
        best_ml_predictions_pca,
    ]

    # retrieve best model performance among three datasets
    y_tests = [y_test_raw, y_test_fs, y_test_pca]
    best_model_accuracy_data, classify_report = final_model_performance(
        best_ml_models, best_accuracy_rates, best_ml_predictions, y_tests
    )
    print("The best model performance is: ", best_model_accuracy_data)
    print(classify_report)


if __name__ == "__main__":
    sys.exit(main())
