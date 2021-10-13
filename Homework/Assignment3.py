import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.window import Window


def main():
    # Setup Spark
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    database = "baseball"
    port = "3306"
    user = "root"
    password = "root"  # pragma: allowlist secret

    # Get game table from MariaDB
    game_df = (
        spark.read.format("jdbc")
        .options(
            url=f"jdbc:mysql://localhost:{port}/{database}?zeroDateTimeBehavior=CONVERT_TO_NULL",
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="game",
            user=user,
            password=password,
        )
        .load()
    )

    game_df.createOrReplaceTempView("game")
    game_df.persist(StorageLevel.DISK_ONLY)

    # Get batter_counts table from MariaDB
    batter_counts_df = (
        spark.read.format("jdbc")
        .options(
            url=f"jdbc:mysql://localhost:{port}/{database}",
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="batter_counts",
            user=user,
            password=password,
        )
        .load()
    )

    batter_counts_df.createOrReplaceTempView("batter_counts")
    batter_counts_df.persist(StorageLevel.DISK_ONLY)

    # Get needed columns from game and batter_counts table
    results = spark.sql(
        """SELECT   CAST(g.local_date AS DATE ) AS local_date, g.game_id, bc.batter, bc.Hit, bc.atBat,
       DATEDIFF( CAST(g.local_date AS DATE ), (SELECT min(CAST(local_date AS DATE )) FROM game)) AS date_offset
       FROM     game g
       JOIN     batter_counts bc ON g.game_id = bc.game_id
       ORDER BY local_date ASC """
    )

    # Calculate rolling average for last 100 days using transformer
    window_spec = (
        Window.partitionBy("batter").orderBy("date_offset").rangeBetween(-100, 0)
    )
    results = results.withColumn(
        "rolling_100_batting_avg",
        func.sum("Hit").over(window_spec) / func.sum("atBat").over(window_spec),
    )

    results.show()


if __name__ == "__main__":
    sys.exit(main())
