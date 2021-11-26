/******************* Extract Features for HW5 *******************/

-- response variable and 1st features (& away_streak)
CREATE TEMPORARY TABLE temporary1
(SELECT game_id, team_id AS home_team_id, opponent_id AS away_team_id,
case when win_lose = "W" then "Y"
else "N" end AS home_team_wins, away_streak, CAST(local_date AS DATE) AS local_date
FROM team_results
WHERE home_away = "H"
);


-- 2rd feature (days_to_next_game_home_team)
CREATE TEMPORARY TABLE temporary2
(SELECT ts1.game_id, ts1.team_id AS home_team_id,
DATEDIFF( CAST(ts2.local_date AS DATE ),
       CAST(ts1.local_date AS DATE )) AS days_to_next_game_home_team
FROM team_streak ts1
JOIN team_streak ts2 ON ts1.pre_game_id = ts2.game_id
WHERE ts1.home_away = "H"
);


-- 3th feature (days_to_next_game_away_team)
CREATE TEMPORARY TABLE temporary3
(SELECT ts1.game_id, ts1.team_id AS away_team_id,
DATEDIFF( CAST(ts2.local_date AS DATE ),
       CAST(ts1.local_date AS DATE )) AS days_to_next_game_away_team
FROM team_streak ts1
JOIN team_streak ts2 ON ts1.pre_game_id = ts2.game_id
WHERE ts1.home_away = "A"
);


-- 4, 5, 6, 7th feature (atBat, Hit, Batting Average, plateApperance from previous game)
CREATE TEMPORARY TABLE temporary4
(SELECT tbc.game_id, tbc.team_id, tgpn.prior_game_id, tbc.atBat, tbc.Hit, tbc.plateApperance
FROM team_game_prior_next tgpn
JOIN team_batting_counts tbc ON tgpn.team_id = tbc.team_id AND tgpn.game_id = tbc.game_id
WHERE tbc.homeTeam = 1)
;


CREATE TEMPORARY TABLE temporary5
(SELECT t1.game_id, t1.team_id, t2.Hit AS prior_Hit, t2.atBat AS prior_atBat,
       t2.Hit / NULLIF(t2.atBat,0) AS prior_Batting_Average, t2.plateApperance
FROM temporary4 t1
JOIN temporary4 t2 ON t1.prior_game_id = t2.game_id
);


-- 8, 9th feature (league & division)
CREATE TEMPORARY TABLE temporary6
(SELECT team_id, league, division
FROM team
);



-- 10th feature (No of Batter)
CREATE TEMPORARY TABLE temporary7
(SELECT bc.game_id, bc.team_id, CAST(g.local_date AS DATE ) AS local_date, bc.batter
FROM batter_counts bc
JOIN game g ON bc.game_id = g.game_id);

CREATE TEMPORARY TABLE temporary8
(SELECT game_id, team_id, COUNT(DISTINCT batter)  AS no_of_batter
FROM temporary7
GROUP BY game_id, team_id);





-- Make final table for storing all features and response
DROP TABLE IF EXISTS hw5_table;
CREATE TABLE hw5_table
(SELECT DISTINCT t1.game_id, t1.home_team_id, t1.away_team_id, t1.local_date, t1.home_team_wins, t1.away_streak,
        t2.days_to_next_game_home_team,
        t3.days_to_next_game_away_team,
        t5.prior_Hit AS home_prior_Hit, t5.prior_atBat AS home_prior_atBat,
        t5.prior_Batting_Average AS home_prior_Batting_Average, t5.plateApperance AS home_prior_plateApperance,
        t6.league AS home_league, t6.division AS home_division,
        t8.no_of_batter AS home_no_of_batter
FROM      temporary1 t1
JOIN temporary2 t2 ON t1.game_id = t2.game_id AND t1.home_team_id = t2.home_team_id
JOIN temporary3 t3 ON t1.game_id = t3.game_id AND t1.away_team_id = t3.away_team_id
JOIN temporary5 t5 ON t1.game_id = t5.game_id AND t1.home_team_id = t5.team_id
JOIN temporary6 t6 ON t1.home_team_id = t6.team_id
JOIN temporary8 t8 ON t1.game_id = t8.game_id AND t1.home_team_id = t8.team_id
);

-- Show the final Table
SELECT *
FROM hw5_table
LIMIT 20
;




