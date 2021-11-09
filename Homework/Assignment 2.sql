/******************* Historic Average *******************/

-- Create a Historic Batting Average Table
DROP    TABLE  IF EXISTS historic_average;
CREATE  TABLE  historic_average
(SELECT batter AS Player, SUM(Hit)/NULLIF(SUM(atBat), 0) AS "Historic Batting Average"
 FROM   batter_counts
 GROUP  BY batter
);

-- Show the first 20 rows of Historic Batting Average Table
SELECT *
FROM   historic_average
LIMIT  20;

/******************* Annual Average *******************/

-- Create an Annual Batting Average Table
DROP    TABLE IF EXISTS annual_average;
CREATE  TABLE annual_average
(SELECT BC.batter AS Player, YEAR(G.local_date) AS YEAR,
        SUM(BC.Hit)/ NULLIF(SUM(BC.atBat),0) AS "Annual Batting Average"
 FROM   batter_counts BC
 JOIN   game G ON BC.game_id = G.game_id
 GROUP  BY BC.batter, YEAR(G.local_date)
 ORDER  BY BC.batter, YEAR(G.local_date)
);

-- Show the first 20 rows of Annual Batting Average Table
SELECT *
FROM   annual_average
LIMIT  20;

/******************* Rolling Average *******************/
-- Create a temporary game table with game_id, local_date and date_offset
DROP TABLE IF EXISTS temp_game;
CREATE TEMPORARY TABLE temp_game
(SELECT game_id, CAST(local_date AS DATE ) AS local_date, DATEDIFF( CAST(local_date AS DATE ),
       (SELECT min(CAST(local_date AS DATE )) FROM game)) AS date_offset
 FROM game
 GROUP BY game_id
);

-- Create final temporary table for calculating rolling average
DROP TABLE IF EXISTS temp_table;
CREATE TEMPORARY TABLE temp_table
(SELECT TG.*, BC.batter, BC.Hit, BC.atBat
 FROM temp_game TG
 JOIN batter_counts BC ON  TG.game_id = BC.game_id
) ;

-- Create index to improve the query performance
CREATE UNIQUE INDEX temp_game_batter_date_idx ON temp_table (game_id, batter, local_date);
CREATE UNIQUE INDEX temp_game_batter_idx ON temp_table (game_id, batter);
CREATE INDEX temp_date_idx ON temp_table (local_date);
CREATE INDEX temp_game_idx ON temp_table (game_id);
CREATE INDEX temp_batter_idx ON temp_table (batter);
CREATE INDEX temp_offset_idx ON temp_table (date_offset);

-- Rolling Batting Average
DROP TABLE IF EXISTS 100_days_rolling_average;
CREATE TABLE 100_days_rolling_average ENGINE=MEMORY AS
(SELECT batter AS Player, local_date, game_id,
        SUM(Hit) OVER (ORDER BY date_offset ASC RANGE 100 PRECEDING) /
        SUM(atBat) OVER (ORDER BY date_offset ASC RANGE 100 PRECEDING) AS "100 Days Rolling Batting Average"
FROM  temp_table
GROUP BY batter, local_date, game_id
ORDER BY batter, local_date, game_id
);


-- Show the Rolling Batting Average Table
SELECT *
FROM 100_days_rolling_average
LIMIT 20
;





