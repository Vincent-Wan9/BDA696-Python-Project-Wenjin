use baseball;

-- Create a temporary table to calculate rolling average
DROP TABLE IF EXISTS temp_table;
CREATE TABLE temp_table AS
(SELECT TG.*, BC.batter, BC.Hit, BC.atBat
 FROM (SELECT game_id, CAST(local_date AS DATE ) AS local_date,
              DATEDIFF( CAST(local_date AS DATE ),
              (SELECT min(CAST(local_date AS DATE )) FROM game)) AS date_offset
       FROM game
       GROUP BY game_id) TG
 JOIN batter_counts BC ON  TG.game_id = BC.game_id
) ;


-- Rolling Batting Average
DROP TABLE IF EXISTS rolling_average;
CREATE TABLE rolling_average AS
(SELECT batter AS Player, local_date, game_id,
        SUM(Hit) OVER (ORDER BY date_offset ASC RANGE 100 PRECEDING) /
        SUM(atBat) OVER (ORDER BY date_offset ASC RANGE 100 PRECEDING) AS 100_Days_Rolling_Batting_Average
FROM  temp_table
WHERE game_id = 12560
GROUP BY batter, local_date, game_id
ORDER BY batter, local_date, game_id
);


SELECT Player, 100_Days_Rolling_Batting_Average FROM rolling_average;