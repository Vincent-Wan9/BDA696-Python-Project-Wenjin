/******************* Historic Average *******************/

-- Create a Historic Batting Average Table
DROP TABLE IF EXISTS historic_average;
CREATE TABLE historic_average
(SELECT DISTINCT batter AS Player, SUM(Hit)/NULLIF(SUM(atBat), 0) AS "Historic Batting Average"
 FROM   batter_counts
 GROUP  BY batter);

-- Show the first 20 rows of Historic Batting Average Table
SELECT *
FROM   historic_average
LIMIT  20;

/******************* Annual Average *******************/

-- Create an Annual Batting Average Table
DROP TABLE IF EXISTS annual_average;
CREATE TABLE annual_average
(
    SELECT DISTINCT BC.batter AS Player,
    YEAR(G.local_date) AS YEAR,
    SUM(BC.Hit)/ NULLIF(SUM(BC.atBat),0) AS "Annual Batting Average"
        FROM batter_counts BC
        JOIN game G ON BC.game_id = G.game_id
        GROUP BY BC.batter,
    YEAR(G.local_date)
        ORDER BY BC.batter,
    YEAR(G.local_date)
);

-- Show the first 20 rows of Annual Batting Average Table
SELECT *
FROM   annual_average
LIMIT  20;

/******************* Rolling Average *******************/
-- Create a temporary game table with game_id, local_date and date_offset
DROP TABLE IF EXISTS temp_game;
CREATE TABLE temp_game
(SELECT game_id, CAST(local_date AS DATE ) AS local_date, DATEDIFF( CAST(local_date AS DATE ), (SELECT min(CAST(local_date AS DATE ))
                                                                                                FROM game)) AS date_offset
      FROM game
      GROUP BY game_id);

-- Create Final table for calculating rolling average
DROP TABLE IF EXISTS temp_table;
CREATE TABLE temp_table
(SELECT TG.*, BC.batter, BC.Hit, BC.atBat
FROM temp_game TG
JOIN batter_counts BC ON  TG.game_id = BC.game_id
);

-- Rolling Batting Average For largest game ID
DROP TABLE IF EXISTS rolling_average_20120625;
CREATE TABLE rolling_average_20120625
(SELECT DISTINCT batter AS Player, game_id, local_date,
                SUM(Hit)/SUM(atBat) OVER (ORDER BY date_offset ASC RANGE 100 PRECEDING)
                AS "100 Days Rolling Batting Average"
FROM  temp_table
WHERE game_id = (SELECT MAX(game_id) FROM temp_table)
GROUP BY batter, game_id, local_date)
;

-- Show the Rolling Batting Average Table
SELECT *
FROM rolling_average_20120625
;









