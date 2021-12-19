USE baseball;

/********************************* 60 day rolling Batting Features *********************************/
-- 60 days rolling sum
DROP TABLE IF EXISTS batting_rolling_60_day;
CREATE TABLE batting_rolling_60_day AS
(SELECT  tbc1.team_id,
		 tbc1.game_id,
		 g1.local_date,
		 count(*) AS cnt,
		 SUM(tbc2.plateApperance) AS plateAppearance,
		 SUM(tbc2.atBat) AS atBat,
		 SUM(tbc2.Hit) AS Hit,
		 SUM(tbc2.caughtStealing2B) AS caughtStealing2B,
		 SUM(tbc2.caughtStealing3B) AS caughtStealing3B,
		 SUM(tbc2.caughtStealingHome) AS caughtStealingHome,
		 SUM(tbc2.stolenBase2B) AS stolenBase2B,
		 SUM(tbc2.stolenBase3B) AS stolenBase3B,
		 SUM(tbc2.stolenBaseHome) AS stolenBaseHome,
		 SUM(tbc2.toBase) AS toBase,
		 SUM(tbc2.Batter_Interference) AS Batter_Interference,
		 SUM(tbc2.Bunt_Ground_Out)+sum(tbc2.Bunt_Groundout) AS Bunt_Ground_Out,
		 SUM(tbc2.Bunt_Pop_Out) AS Bunt_Pop_Out,
		 SUM(tbc2.Catcher_Interference) AS Catcher_Interference,
		 SUM(tbc2.`Double`) AS `Double`,
		 SUM(tbc2.Double_Play) AS Double_Play,
		 SUM(tbc2.Fan_interference) AS Fan_interference,
		 SUM(tbc2.Field_Error) AS Field_Error,
		 SUM(tbc2.Fielders_Choice) AS Fielders_Choice,
		 SUM(tbc2.Fly_Out)+sum(tbc2.Flyout) AS Fly_Out,
		 SUM(tbc2.Force_Out)+SUM(tbc2.Forceout) AS Force_Out,
		 SUM(tbc2.Ground_Out)+SUM(tbc2.Groundout) AS Ground_Out,
		 SUM(tbc2.Grounded_Into_DP) AS Grounded_Into_DP,
		 SUM(tbc2.Hit_By_Pitch) AS Hit_By_Pitch,
		 CASE WHEN SUM(tbc2.Home_Run) = 0 THEN 1 ELSE SUM(tbc2.Home_Run) END AS Home_Run,
		 SUM(tbc2.Intent_Walk) AS Intent_Walk,
		 SUM(tbc2.Line_Out) AS Line_Out,
		 SUM(tbc2.Pop_Out) AS Pop_Out,
		 SUM(tbc2.Runner_Out) AS Runner_Out,
		 SUM(tbc2.Sac_Bunt) AS Sac_Bunt,
		 SUM(tbc2.Sac_Fly) AS Sac_Fly,
		 SUM(tbc2.Sac_Fly_DP) AS Sac_Fly_DP,
		 SUM(tbc2.Sacrifice_Bunt_DP) AS Sacrifice_Bunt_DP,
		 SUM(tbc2.Single) AS Single,
         CASE WHEN SUM(tbc2.Strikeout) = 0 THEN 296 ELSE SUM(tbc2.Strikeout) END AS Strikeout,
		 SUM(tbc2.`Strikeout_-_DP`) AS `Strikeout_-_DP`,
		 SUM(tbc2.`Strikeout_-_TP`) AS `Strikeout_-_TP`,
		 SUM(tbc2.Triple) AS Triple,
		 SUM(tbc2.Triple_Play) AS Triple_Play,
		 SUM(tbc2.Walk) AS Walk
FROM     team_batting_counts tbc1
JOIN     game g1 ON tbc1.game_id = g1.game_id
AND      g1.`type` IN ("R")
JOIN     team_batting_counts tbc2 ON tbc1.team_id = tbc2.team_id
JOIN     game g2 ON tbc2.game_id = g2.game_id
AND      g2.`type` in ("R")
AND      g2.local_date < g1.local_date
AND      g2.local_date >= DATE_ADD(g1.local_date,INTERVAL -60 DAY)
GROUP BY tbc1.team_id,tbc1.game_id,g1.local_date
ORDER BY g1.local_date,tbc1.team_id);

CREATE UNIQUE INDEX team_game ON batting_rolling_60_day(team_id,game_id);

-- difference between home team and away team
DROP TABLE IF EXISTS batting_feature_diff;
CREATE TABLE batting_feature_diff AS (
SELECT g.game_id,
       g.home_team_id,
       g.away_team_id,
       g.local_date,
       CASE WHEN b.away_runs < b.home_runs THEN 1 WHEN b.away_runs > b.home_runs THEN 0 ELSE 0 END AS Home_team_wins,
       (r2dh.plateAppearance - r2da.plateAppearance) AS P_diff, -- plateAppearance
       (r2dh.atBat - r2da.atBat) AS B_diff, -- atBat
       (r2dh.Hit - r2da.Hit) AS H_diff, -- Hit
       (r2dh.Walk - r2da.Walk) AS W_diff, -- Walk
       (r2dh.Strikeout - r2da.Strikeout) AS S_diff, -- Strikeout
       (r2dh.Hit_By_Pitch - r2da.Hit_By_Pitch) AS HBP_diff, -- Hit_By_Pitch
       (r2dh.Double_Play - r2da.Double_Play) AS Double_Play_diff, -- Double_Play
       (r2dh.Triple_Play - r2da.Triple_Play) AS Triple_Play_diff, -- Triple_Play
       (r2dh.Hit/r2dh.atBat) - (r2da.Hit/r2da.atBat) AS BA_diff, -- batting average
       (r2dh.Single + 2*r2dh.`Double` +3*r2dh.Triple + 4*r2dh.Home_Run )/(r2dh.atBat)-(r2da.Single+2*r2da.`Double` +3*r2da.Triple+4*r2da.Home_Run)/(r2da.atBat) AS slug_diff,-- slugging percentage
       ((r2dh.Single + 2*r2dh.`Double` +3*r2dh.Triple + 4*r2dh.Home_Run )- (r2da.Single+2*r2da.`Double` +3*r2da.Triple+4*r2da.Home_Run)) AS TB_diff, -- Total bases
       ((r2dh.Hit+ r2dh.Walk+ r2dh.Hit_By_Pitch)-(r2da.Hit+ r2da.Walk+ r2da.Hit_By_Pitch)) AS TOB_diff, -- Times on base
       (((r2dh.Hit+ r2dh.Walk + r2dh.Hit_By_Pitch)/(r2dh.atBat+ r2dh.Walk+ r2dh.Hit_By_Pitch+ r2dh.Sac_Fly))-
       ((r2da.Hit+ r2da.Walk + r2da.Hit_By_Pitch)/(r2da.atBat+ r2da.Walk+ r2da.Hit_By_Pitch+ r2da.Sac_Fly))) AS OBP_diff, -- On-base percentage
       (((r2dh.Hit+ r2dh.Walk + r2dh.Hit_By_Pitch)/(r2dh.atBat+ r2dh.Walk+ r2dh.Hit_By_Pitch+ r2dh.Sac_Fly))+
       ((r2dh.Single+2*r2dh.`Double`+3*r2dh.Triple+4*r2dh.Home_Run)/(r2dh.atBat)) -
       ((r2da.Hit+ r2da.Walk + r2da.Hit_By_Pitch)/(r2da.atBat+ r2da.Walk+ r2da.Hit_By_Pitch+ r2da.Sac_Fly))+
       ((r2da.Single+2*r2da.`Double`+3*r2da.Triple+4*r2da.Home_Run)/(r2da.atBat))) AS OPS_diff, -- On-base plus slugging
       ((r2dh.plateAppearance/r2dh.Strikeout)-(r2da.plateAppearance/r2da.Strikeout)) AS PA_per_SO_diff, -- Plate appearances per strikeout
       ((r2dh.Walk/r2dh.Strikeout)-(r2da.Walk/r2da.Strikeout)) AS BB_to_K_diff, -- Walk-to-strikeout ratio
       ((r2dh.Home_Run/r2dh.Hit)-(r2da.Home_Run/r2da.Hit)) AS HR_per_H_diff, -- Home runs per hit
       ((r2dh.atBat/r2dh.Home_Run)-(r2da.atBat/r2da.Home_Run)) AS AB_per_HR_diff, -- At bats per home run
       ((((r2dh.Hit+ r2dh.Walk + r2dh.Hit_By_Pitch)/(r2dh.atBat+ r2dh.Walk+ r2dh.Hit_By_Pitch+ r2dh.Sac_Fly))*1.8 +
       ((r2dh.Single + 2*r2dh.`Double` +3*r2dh.Triple + 4*r2dh.Home_Run )/(r2dh.atBat)))/4 -
       (((r2da.Hit+ r2da.Walk + r2da.Hit_By_Pitch)/(r2da.atBat+ r2da.Walk+ r2da.Hit_By_Pitch+ r2da.Sac_Fly))*1.8 +
       ((r2da.Single + 2*r2da.`Double` +3*r2da.Triple + 4*r2da.Home_Run )/(r2da.atBat)))/4) AS GPA_diff, -- Gross production average
       ((r2dh.Hit - r2dh.Home_Run)/(r2dh.atBat - r2dh.Strikeout - r2dh.Home_Run + r2dh.Sac_Fly) -
       (r2da.Hit - r2da.Home_Run)/(r2da.atBat - r2da.Strikeout - r2da.Home_Run + r2da.Sac_Fly)) AS BABIP_diff, -- Batting average on balls in play
       (r2dh.Ground_Out/r2dh.Fly_Out - r2da.Ground_Out/r2da.Fly_Out) AS GO_AO_diff, -- Ground ball fly ball ratio
       ((r2dh.Single+2*r2dh.`Double`+3*r2dh.Triple+4*r2dh.Home_Run)/(r2dh.atBat)-(r2dh.Hit/r2dh.atBat)) -
       ((r2da.Single+2*r2da.`Double`+3*r2da.Triple+4*r2da.Home_Run)/(r2da.atBat)-(r2da.Hit/r2da.atBat)) AS ISO_diff, -- Isolated power
       (((r2dh.Hit + r2dh.Walk)*(r2dh.Single + 2*r2dh.`Double` +3*r2dh.Triple + 4*r2dh.Home_Run))/ (r2dh.atBat + r2dh.Walk) -
       ((r2da.Hit + r2da.Walk)*(r2da.Single + 2*r2da.`Double` +3*r2da.Triple + 4*r2da.Home_Run))/ (r2da.atBat + r2da.Walk)) AS RC_diff -- Runs created
FROM   game g
JOIN   batting_rolling_60_day r2da ON r2da.team_id = g.away_team_id AND g.game_id = r2da.game_id
JOIN   batting_rolling_60_day r2dh ON r2dh.team_id = g.home_team_id AND g.game_id = r2dh.game_id
JOIN   boxscore b                   ON b.game_id = g.game_id );

CREATE UNIQUE INDEX game_home_away ON batting_feature_diff(game_id, home_team_id, away_team_id);

-- diff streak 60 rolling day
DROP TABLE IF EXISTS avg_streak;
CREATE TABLE avg_streak AS
(SELECT  ts1.team_id,
		 ts1.game_id,
		 g1.local_date,
		 ts1.home_away,
         AVG(ts2.streak) AS avg_streak
FROM     team_results ts1
JOIN     game g1 ON ts1.game_id = g1.game_id
AND      g1.`type` IN ("R")
JOIN     team_results ts2 ON ts1.team_id = ts2.team_id
JOIN     game g2 ON ts2.game_id = g2.game_id
AND      g2.`type` IN ("R")
AND      g2.local_date < g1.local_date
AND      g2.local_date >= DATE_ADD(g1.local_date,INTERVAL -60 DAY)
GROUP BY ts1.team_id, ts1.game_id, g1.local_date
ORDER BY g1.local_date, ts1.team_id);

CREATE UNIQUE INDEX team_game2 ON avg_streak(team_id,game_id);

DROP TABLE IF EXISTS diff_streak;
CREATE TABLE diff_streak AS
(SELECT g.game_id,
       g.home_team_id,
       g.away_team_id,
	   (ash.avg_streak - asa.avg_streak) AS diff_streak
FROM   game g
JOIN   avg_streak asa ON asa.team_id = g.away_team_id AND g.game_id = asa.game_id
JOIN   avg_streak ash ON ash.team_id = g.home_team_id AND g.game_id = ash.game_id);

CREATE UNIQUE INDEX game_home_away2 ON diff_streak(game_id, home_team_id, away_team_id);

/********************************* 60 day rolling pitching Features *********************************/
-- 60 days rolling sum
DROP TABLE IF EXISTS pitch_rolling_60_day;
CREATE TABLE pitch_rolling_60_day AS
(SELECT  tpc1.team_id,
		 tpc1.game_id,
		 g1.local_date,
		 count(*) AS cnt,
		 SUM(tpc2.win) AS win,
		 SUM(tpc2.plateApperance) AS plateAppearance,
		 SUM(tpc2.atBat) AS atBat,
		 SUM(tpc2.Hit) AS Hit,
	     SUM(tpc2.bullpenWalk) AS bullpenWalk,
		 SUM(tpc2.toBase) AS toBase,
		 SUM(tpc2.Batter_Interference) AS Batter_Interference,
		 SUM(tpc2.Bunt_Ground_Out)+sum(tpc2.Bunt_Groundout) AS Bunt_Ground_Out,
		 SUM(tpc2.Bunt_Pop_Out) AS Bunt_Pop_Out,
		 SUM(tpc2.Catcher_Interference) AS Catcher_Interference,
		 SUM(tpc2.`Double`) AS `Double`,
		 SUM(tpc2.Double_Play) AS Double_Play,
		 SUM(tpc2.Fan_interference) AS Fan_interference,
		 SUM(tpc2.Field_Error) AS Field_Error,
		 SUM(tpc2.Fielders_Choice) AS Fielders_Choice,
		 SUM(tpc2.Fly_Out)+sum(tpc2.Flyout) AS Fly_Out,
		 SUM(tpc2.Force_Out)+SUM(tpc2.Forceout) AS Force_Out,
		 SUM(tpc2.Ground_Out)+SUM(tpc2.Groundout) AS Ground_Out,
		 SUM(tpc2.Grounded_Into_DP) AS Grounded_Into_DP,
		 SUM(tpc2.Hit_By_Pitch) AS Hit_By_Pitch,
		 SUM(tpc2.Home_Run) AS Home_Run,
		 SUM(tpc2.Intent_Walk) AS Intent_Walk,
		 SUM(tpc2.Line_Out) AS Line_Out,
		 SUM(tpc2.Pop_Out) AS Pop_Out,
		 SUM(tpc2.Runner_Out) AS Runner_Out,
		 SUM(tpc2.Sac_Bunt) AS Sac_Bunt,
		 SUM(tpc2.Sac_Fly) AS Sac_Fly,
		 SUM(tpc2.Sac_Fly_DP) AS Sac_Fly_DP,
		 SUM(tpc2.Sacrifice_Bunt_DP) AS Sacrifice_Bunt_DP,
		 SUM(tpc2.Single) AS Single,
		 SUM(tpc2.Strikeout) AS Strikeout,
		 SUM(tpc2.`Strikeout_-_DP`) AS `Strikeout_-_DP`,
		 SUM(tpc2.`Strikeout_-_TP`) AS `Strikeout_-_TP`,
		 SUM(tpc2.Triple) AS Triple,
		 SUM(tpc2.Triple_Play) AS Triple_Play,
		 SUM(tpc2.Walk) AS Walk,
		 SUM(pc.startingInning) AS startingInning,
		 SUM(pc.endingInning) AS endingInning,
		 SUM(pc.pitchesThrown) AS pitchesThrown,
         AVG(pc.DaysSinceLastPitch) AS avg_DaysSinceLastPitch
FROM     team_pitching_counts tpc1
JOIN     game g1 ON tpc1.game_id = g1.game_id
AND      g1.`type` IN ("R")
JOIN     team_pitching_counts tpc2 ON tpc1.team_id = tpc2.team_id
JOIN     pitcher_counts pc ON pc.team_id = tpc2.team_id
AND      pc.game_id = tpc2.game_id
JOIN     game g2 ON tpc2.game_id = g2.game_id
AND      g2.`type` in ("R")
AND      g2.local_date < g1.local_date
AND      g2.local_date >= DATE_ADD(g1.local_date,INTERVAL -60 DAY)
GROUP BY tpc1.team_id, tpc1.game_id, g1.local_date
ORDER BY g1.local_date, tpc1.team_id);

CREATE UNIQUE INDEX team_game3 ON pitch_rolling_60_day(team_id,game_id);

-- difference between home team and away team
DROP TABLE IF EXISTS pitching_feature_diff;
CREATE TABLE pitching_feature_diff AS (
SELECT g.game_id,
       g.home_team_id,
       g.away_team_id,
       (pr2dh.win - pr2da.win) AS p_win_diff, -- win
       (pr2dh.plateAppearance - pr2da.plateAppearance) AS p_plateAppearance_diff, -- plateAppearance
       (pr2dh.atBat - pr2da.atBat) AS p_atBat_diff, -- atBat
       (pr2dh.Hit - pr2da.Hit) AS p_Hit_diff, -- Hit
       (pr2dh.Strikeout - pr2da.Strikeout) AS p_Strikeout_diff, -- Strikeout
       (pr2dh.Hit_By_Pitch - pr2da.Hit_By_Pitch) AS p_HBP_diff, -- Hit_By_Pitch
       (pr2dh.Double_Play - pr2da.Double_Play) AS p_Double_Play_diff, -- Double_Play
       (pr2dh.Triple_Play - pr2da.Triple_Play) AS p_Triple_Play_diff, -- Triple_Play
	   (pr2dh.bullpenWalk - pr2da.bullpenWalk) AS p_bull_diff, -- bullpenWalk
	   (pr2dh.Fly_Out - pr2da.Fly_Out) AS p_Fly_Out_diff, -- Fly_Out
	   (pr2dh.Field_Error - pr2da.Field_Error) AS p_Field_Error_diff, -- Field_Error
	   (pr2dh.Pop_Out - pr2da.Pop_Out) AS p_Pop_Out_diff, -- Pop_Out
	   (pr2dh.Line_Out - pr2da.Line_Out) AS p_Line_Out_diff, -- Line_Out
	   (pr2dh.Walk - pr2da.Walk) AS p_Walk_diff, -- Walk
       (pr2dh.pitchesThrown - pr2da.pitchesThrown) AS p_pitchesThrown_diff, -- pitchesThrown
       (pr2dh.avg_DaysSinceLastPitch - pr2da.avg_DaysSinceLastPitch) AS p_day_diff, -- diff between avg_DaysSinceLastPitch
	   (pr2dh.Walk + pr2dh.Hit)/(pr2dh.endingInning-pr2dh.startingInning) -
	   (pr2da.Walk + pr2da.Hit)/(pr2da.endingInning-pr2da.startingInning) AS p_WHIP_diff -- Walks plus hits per inning pitched
FROM   game g
JOIN   pitch_rolling_60_day pr2da ON pr2da.team_id = g.away_team_id AND g.game_id = pr2da.game_id
JOIN   pitch_rolling_60_day pr2dh ON pr2dh.team_id = g.home_team_id AND g.game_id = pr2dh.game_id
JOIN   boxscore b                  ON b.game_id = g.game_id );

CREATE UNIQUE INDEX game_home_away3 ON pitching_feature_diff(game_id, home_team_id, away_team_id);


/********************************* 60 day rolling final features *********************************/
-- 40 features in total
DROP TABLE IF EXISTS final_features_60_day;
CREATE TABLE final_features_60_day AS (
SELECT bfd.*,
       ds.diff_streak,
       pfd.p_plateAppearance_diff,
       pfd.p_atBat_diff,
       pfd.p_Hit_diff,
       pfd.p_Strikeout_diff,
       pfd.p_HBP_diff,
       pfd.p_Double_Play_diff,
       pfd.p_Triple_Play_diff,
       pfd.p_bull_diff,
       pfd.p_Fly_Out_diff,
       pfd.p_Field_Error_diff,
       pfd.p_Pop_Out_diff,
       pfd.p_Line_Out_diff,
       pfd.p_Walk_diff,
       pfd.p_pitchesThrown_diff,
       pfd.p_day_diff,
       pfd.p_WHIP_diff
FROM   batting_feature_diff bfd
JOIN   diff_streak          ds  ON bfd.game_id = ds.game_id
AND    bfd.home_team_id = ds.home_team_id
AND    bfd.away_team_id = ds.away_team_id
JOIN   pitching_feature_diff pfd ON bfd.game_id = pfd.game_id
AND    bfd.home_team_id = pfd.home_team_id
AND    bfd.away_team_id = pfd.away_team_id);



DROP TABLE IF EXISTS batting_rolling_60_day;
DROP TABLE IF EXISTS batting_feature_diff;
DROP TABLE IF EXISTS avg_streak;
DROP TABLE IF EXISTS diff_streak;
DROP TABLE IF EXISTS pitch_rolling_60_day;
DROP TABLE IF EXISTS pitching_feature_diff;






/********************************* 200 day rolling Batting Features *********************************/
-- 200 days rolling sum
DROP TABLE IF EXISTS batting_rolling_200_day;
CREATE TABLE batting_rolling_200_day AS
(SELECT  tbc1.team_id,
		 tbc1.game_id,
		 g1.local_date,
		 count(*) AS cnt,
		 SUM(tbc2.plateApperance) AS plateAppearance,
		 SUM(tbc2.atBat) AS atBat,
		 SUM(tbc2.Hit) AS Hit,
		 SUM(tbc2.caughtStealing2B) AS caughtStealing2B,
		 SUM(tbc2.caughtStealing3B) AS caughtStealing3B,
		 SUM(tbc2.caughtStealingHome) AS caughtStealingHome,
		 SUM(tbc2.stolenBase2B) AS stolenBase2B,
		 SUM(tbc2.stolenBase3B) AS stolenBase3B,
		 SUM(tbc2.stolenBaseHome) AS stolenBaseHome,
		 SUM(tbc2.toBase) AS toBase,
		 SUM(tbc2.Batter_Interference) AS Batter_Interference,
		 SUM(tbc2.Bunt_Ground_Out)+sum(tbc2.Bunt_Groundout) AS Bunt_Ground_Out,
		 SUM(tbc2.Bunt_Pop_Out) AS Bunt_Pop_Out,
		 SUM(tbc2.Catcher_Interference) AS Catcher_Interference,
		 SUM(tbc2.`Double`) AS `Double`,
		 SUM(tbc2.Double_Play) AS Double_Play,
		 SUM(tbc2.Fan_interference) AS Fan_interference,
		 SUM(tbc2.Field_Error) AS Field_Error,
		 SUM(tbc2.Fielders_Choice) AS Fielders_Choice,
		 SUM(tbc2.Fly_Out)+sum(tbc2.Flyout) AS Fly_Out,
		 SUM(tbc2.Force_Out)+SUM(tbc2.Forceout) AS Force_Out,
		 SUM(tbc2.Ground_Out)+SUM(tbc2.Groundout) AS Ground_Out,
		 SUM(tbc2.Grounded_Into_DP) AS Grounded_Into_DP,
		 SUM(tbc2.Hit_By_Pitch) AS Hit_By_Pitch,
		 CASE WHEN SUM(tbc2.Home_Run) = 0 THEN 1 ELSE SUM(tbc2.Home_Run) END AS Home_Run,
		 SUM(tbc2.Intent_Walk) AS Intent_Walk,
		 SUM(tbc2.Line_Out) AS Line_Out,
		 SUM(tbc2.Pop_Out) AS Pop_Out,
		 SUM(tbc2.Runner_Out) AS Runner_Out,
		 SUM(tbc2.Sac_Bunt) AS Sac_Bunt,
		 SUM(tbc2.Sac_Fly) AS Sac_Fly,
		 SUM(tbc2.Sac_Fly_DP) AS Sac_Fly_DP,
		 SUM(tbc2.Sacrifice_Bunt_DP) AS Sacrifice_Bunt_DP,
		 SUM(tbc2.Single) AS Single,
		 SUM(tbc2.Strikeout) AS Strikeout,
		 SUM(tbc2.`Strikeout_-_DP`) AS `Strikeout_-_DP`,
		 SUM(tbc2.`Strikeout_-_TP`) AS `Strikeout_-_TP`,
		 SUM(tbc2.Triple) AS Triple,
		 SUM(tbc2.Triple_Play) AS Triple_Play,
		 SUM(tbc2.Walk) AS Walk
FROM     team_batting_counts tbc1
JOIN     game g1 ON tbc1.game_id = g1.game_id
AND      g1.`type` IN ("R")
JOIN     team_batting_counts tbc2 ON tbc1.team_id = tbc2.team_id
JOIN     game g2 ON tbc2.game_id = g2.game_id
AND      g2.`type` in ("R")
AND      g2.local_date < g1.local_date
AND      g2.local_date >= DATE_ADD(g1.local_date,INTERVAL -200 DAY)
GROUP BY tbc1.team_id,tbc1.game_id,g1.local_date
ORDER BY g1.local_date,tbc1.team_id);

CREATE UNIQUE INDEX team_game ON batting_rolling_200_day(team_id,game_id);

-- difference between home team and away team
DROP TABLE IF EXISTS batting_feature_diff;
CREATE TABLE batting_feature_diff AS (
SELECT g.game_id,
       g.home_team_id,
       g.away_team_id,
       g.local_date,
       CASE WHEN b.away_runs < b.home_runs THEN 1 WHEN b.away_runs > b.home_runs THEN 0 ELSE 0 END AS Home_team_wins,
       (r2dh.plateAppearance - r2da.plateAppearance) AS P_diff, -- plateAppearance
       (r2dh.atBat - r2da.atBat) AS B_diff, -- atBat
       (r2dh.Hit - r2da.Hit) AS H_diff, -- Hit
       (r2dh.Walk - r2da.Walk) AS W_diff, -- Walk
       (r2dh.Strikeout - r2da.Strikeout) AS S_diff, -- Strikeout
       (r2dh.Hit_By_Pitch - r2da.Hit_By_Pitch) AS HBP_diff, -- Hit_By_Pitch
       (r2dh.Double_Play - r2da.Double_Play) AS Double_Play_diff, -- Double_Play
       (r2dh.Triple_Play - r2da.Triple_Play) AS Triple_Play_diff, -- Triple_Play
       (r2dh.Hit/r2dh.atBat) - (r2da.Hit/r2da.atBat) AS BA_diff, -- batting average
       (r2dh.Single + 2*r2dh.`Double` +3*r2dh.Triple + 4*r2dh.Home_Run )/(r2dh.atBat)-(r2da.Single+2*r2da.`Double` +3*r2da.Triple+4*r2da.Home_Run)/(r2da.atBat) AS slug_diff,-- slugging percentage
       ((r2dh.Single + 2*r2dh.`Double` +3*r2dh.Triple + 4*r2dh.Home_Run )- (r2da.Single+2*r2da.`Double` +3*r2da.Triple+4*r2da.Home_Run)) AS TB_diff, -- Total bases
       ((r2dh.Hit+ r2dh.Walk+ r2dh.Hit_By_Pitch)-(r2da.Hit+ r2da.Walk+ r2da.Hit_By_Pitch)) AS TOB_diff, -- Times on base
       (((r2dh.Hit+ r2dh.Walk + r2dh.Hit_By_Pitch)/(r2dh.atBat+ r2dh.Walk+ r2dh.Hit_By_Pitch+ r2dh.Sac_Fly))-
       ((r2da.Hit+ r2da.Walk + r2da.Hit_By_Pitch)/(r2da.atBat+ r2da.Walk+ r2da.Hit_By_Pitch+ r2da.Sac_Fly))) AS OBP_diff, -- On-base percentage
       (((r2dh.Hit+ r2dh.Walk + r2dh.Hit_By_Pitch)/(r2dh.atBat+ r2dh.Walk+ r2dh.Hit_By_Pitch+ r2dh.Sac_Fly))+
       ((r2dh.Single+2*r2dh.`Double`+3*r2dh.Triple+4*r2dh.Home_Run)/(r2dh.atBat)) -
       ((r2da.Hit+ r2da.Walk + r2da.Hit_By_Pitch)/(r2da.atBat+ r2da.Walk+ r2da.Hit_By_Pitch+ r2da.Sac_Fly))+
       ((r2da.Single+2*r2da.`Double`+3*r2da.Triple+4*r2da.Home_Run)/(r2da.atBat))) AS OPS_diff, -- On-base plus slugging
       ((r2dh.plateAppearance/r2dh.Strikeout)-(r2da.plateAppearance/r2da.Strikeout)) AS PA_per_SO_diff, -- Plate appearances per strikeout
       ((r2dh.Walk/r2dh.Strikeout)-(r2da.Walk/r2da.Strikeout)) AS BB_to_K_diff, -- Walk-to-strikeout ratio
       ((r2dh.Home_Run/r2dh.Hit)-(r2da.Home_Run/r2da.Hit)) AS HR_per_H_diff, -- Home runs per hit
       ((r2dh.atBat/r2dh.Home_Run)-(r2da.atBat/r2da.Home_Run)) AS AB_per_HR_diff, -- At bats per home run
       ((((r2dh.Hit+ r2dh.Walk + r2dh.Hit_By_Pitch)/(r2dh.atBat+ r2dh.Walk+ r2dh.Hit_By_Pitch+ r2dh.Sac_Fly))*1.8 +
       ((r2dh.Single + 2*r2dh.`Double` +3*r2dh.Triple + 4*r2dh.Home_Run )/(r2dh.atBat)))/4 -
       (((r2da.Hit+ r2da.Walk + r2da.Hit_By_Pitch)/(r2da.atBat+ r2da.Walk+ r2da.Hit_By_Pitch+ r2da.Sac_Fly))*1.8 +
       ((r2da.Single + 2*r2da.`Double` +3*r2da.Triple + 4*r2da.Home_Run )/(r2da.atBat)))/4) AS GPA_diff, -- Gross production average
       ((r2dh.Hit - r2dh.Home_Run)/(r2dh.atBat - r2dh.Strikeout - r2dh.Home_Run + r2dh.Sac_Fly) -
       (r2da.Hit - r2da.Home_Run)/(r2da.atBat - r2da.Strikeout - r2da.Home_Run + r2da.Sac_Fly)) AS BABIP_diff, -- Batting average on balls in play
       (r2dh.Ground_Out/r2dh.Fly_Out - r2da.Ground_Out/r2da.Fly_Out) AS GO_AO_diff, -- Ground ball fly ball ratio
       ((r2dh.Single+2*r2dh.`Double`+3*r2dh.Triple+4*r2dh.Home_Run)/(r2dh.atBat)-(r2dh.Hit/r2dh.atBat)) -
       ((r2da.Single+2*r2da.`Double`+3*r2da.Triple+4*r2da.Home_Run)/(r2da.atBat)-(r2da.Hit/r2da.atBat)) AS ISO_diff, -- Isolated power
       (((r2dh.Hit + r2dh.Walk)*(r2dh.Single + 2*r2dh.`Double` +3*r2dh.Triple + 4*r2dh.Home_Run))/ (r2dh.atBat + r2dh.Walk) -
       ((r2da.Hit + r2da.Walk)*(r2da.Single + 2*r2da.`Double` +3*r2da.Triple + 4*r2da.Home_Run))/ (r2da.atBat + r2da.Walk)) AS RC_diff -- Runs created
FROM   game g
JOIN   batting_rolling_200_day r2da ON r2da.team_id = g.away_team_id AND g.game_id = r2da.game_id
JOIN   batting_rolling_200_day r2dh ON r2dh.team_id = g.home_team_id AND g.game_id = r2dh.game_id
JOIN   boxscore b                   ON b.game_id = g.game_id );

CREATE UNIQUE INDEX game_home_away ON batting_feature_diff(game_id, home_team_id, away_team_id);

-- diff streak 200 rolling day
DROP TABLE IF EXISTS avg_streak;
CREATE TABLE avg_streak AS
(SELECT  ts1.team_id,
		 ts1.game_id,
		 g1.local_date,
		 ts1.home_away,
         AVG(ts2.streak) AS avg_streak
FROM     team_results ts1
JOIN     game g1 ON ts1.game_id = g1.game_id
AND      g1.`type` IN ("R")
JOIN     team_results ts2 ON ts1.team_id = ts2.team_id
JOIN     game g2 ON ts2.game_id = g2.game_id
AND      g2.`type` IN ("R")
AND      g2.local_date < g1.local_date
AND      g2.local_date >= DATE_ADD(g1.local_date,INTERVAL -200 DAY)
GROUP BY ts1.team_id, ts1.game_id, g1.local_date
ORDER BY g1.local_date, ts1.team_id);

CREATE UNIQUE INDEX team_game2 ON avg_streak(team_id,game_id);

DROP TABLE IF EXISTS diff_streak;
CREATE TABLE diff_streak AS
(SELECT g.game_id,
       g.home_team_id,
       g.away_team_id,
	   (ash.avg_streak - asa.avg_streak) AS diff_streak
FROM   game g
JOIN   avg_streak asa ON asa.team_id = g.away_team_id AND g.game_id = asa.game_id
JOIN   avg_streak ash ON ash.team_id = g.home_team_id AND g.game_id = ash.game_id);

CREATE UNIQUE INDEX game_home_away2 ON diff_streak(game_id, home_team_id, away_team_id);

/********************************* 200 day rolling pitching Features *********************************/
-- 200 days rolling sum
DROP TABLE IF EXISTS pitch_rolling_200_day;
CREATE TABLE pitch_rolling_200_day AS
(SELECT  tpc1.team_id,
		 tpc1.game_id,
		 g1.local_date,
		 count(*) AS cnt,
		 SUM(tpc2.win) AS win,
		 SUM(tpc2.plateApperance) AS plateAppearance,
		 SUM(tpc2.atBat) AS atBat,
		 SUM(tpc2.Hit) AS Hit,
	     SUM(tpc2.bullpenWalk) AS bullpenWalk,
		 SUM(tpc2.toBase) AS toBase,
		 SUM(tpc2.Batter_Interference) AS Batter_Interference,
		 SUM(tpc2.Bunt_Ground_Out)+sum(tpc2.Bunt_Groundout) AS Bunt_Ground_Out,
		 SUM(tpc2.Bunt_Pop_Out) AS Bunt_Pop_Out,
		 SUM(tpc2.Catcher_Interference) AS Catcher_Interference,
		 SUM(tpc2.`Double`) AS `Double`,
		 SUM(tpc2.Double_Play) AS Double_Play,
		 SUM(tpc2.Fan_interference) AS Fan_interference,
		 SUM(tpc2.Field_Error) AS Field_Error,
		 SUM(tpc2.Fielders_Choice) AS Fielders_Choice,
		 SUM(tpc2.Fly_Out)+sum(tpc2.Flyout) AS Fly_Out,
		 SUM(tpc2.Force_Out)+SUM(tpc2.Forceout) AS Force_Out,
		 SUM(tpc2.Ground_Out)+SUM(tpc2.Groundout) AS Ground_Out,
		 SUM(tpc2.Grounded_Into_DP) AS Grounded_Into_DP,
		 SUM(tpc2.Hit_By_Pitch) AS Hit_By_Pitch,
		 SUM(tpc2.Home_Run) AS Home_Run,
		 SUM(tpc2.Intent_Walk) AS Intent_Walk,
		 SUM(tpc2.Line_Out) AS Line_Out,
		 SUM(tpc2.Pop_Out) AS Pop_Out,
		 SUM(tpc2.Runner_Out) AS Runner_Out,
		 SUM(tpc2.Sac_Bunt) AS Sac_Bunt,
		 SUM(tpc2.Sac_Fly) AS Sac_Fly,
		 SUM(tpc2.Sac_Fly_DP) AS Sac_Fly_DP,
		 SUM(tpc2.Sacrifice_Bunt_DP) AS Sacrifice_Bunt_DP,
		 SUM(tpc2.Single) AS Single,
		 SUM(tpc2.Strikeout) AS Strikeout,
		 SUM(tpc2.`Strikeout_-_DP`) AS `Strikeout_-_DP`,
		 SUM(tpc2.`Strikeout_-_TP`) AS `Strikeout_-_TP`,
		 SUM(tpc2.Triple) AS Triple,
		 SUM(tpc2.Triple_Play) AS Triple_Play,
		 SUM(tpc2.Walk) AS Walk,
		 SUM(pc.startingInning) AS startingInning,
		 SUM(pc.endingInning) AS endingInning,
		 SUM(pc.pitchesThrown) AS pitchesThrown,
         AVG(pc.DaysSinceLastPitch) AS avg_DaysSinceLastPitch
FROM     team_pitching_counts tpc1
JOIN     game g1 ON tpc1.game_id = g1.game_id
AND      g1.`type` IN ("R")
JOIN     team_pitching_counts tpc2 ON tpc1.team_id = tpc2.team_id
JOIN     pitcher_counts pc ON pc.team_id = tpc2.team_id
AND      pc.game_id = tpc2.game_id
JOIN     game g2 ON tpc2.game_id = g2.game_id
AND      g2.`type` in ("R")
AND      g2.local_date < g1.local_date
AND      g2.local_date >= DATE_ADD(g1.local_date,INTERVAL -200 DAY)
GROUP BY tpc1.team_id, tpc1.game_id, g1.local_date
ORDER BY g1.local_date, tpc1.team_id);

CREATE UNIQUE INDEX team_game3 ON pitch_rolling_200_day(team_id,game_id);

-- difference between home team and away team
DROP TABLE IF EXISTS pitching_feature_diff;
CREATE TABLE pitching_feature_diff AS (
SELECT g.game_id,
       g.home_team_id,
       g.away_team_id,
       (pr2dh.win - pr2da.win) AS p_win_diff, -- win
       (pr2dh.plateAppearance - pr2da.plateAppearance) AS p_plateAppearance_diff, -- plateAppearance
       (pr2dh.atBat - pr2da.atBat) AS p_atBat_diff, -- atBat
       (pr2dh.Hit - pr2da.Hit) AS p_Hit_diff, -- Hit
       (pr2dh.Strikeout - pr2da.Strikeout) AS p_Strikeout_diff, -- Strikeout
       (pr2dh.Hit_By_Pitch - pr2da.Hit_By_Pitch) AS p_HBP_diff, -- Hit_By_Pitch
       (pr2dh.Double_Play - pr2da.Double_Play) AS p_Double_Play_diff, -- Double_Play
       (pr2dh.Triple_Play - pr2da.Triple_Play) AS p_Triple_Play_diff, -- Triple_Play
	   (pr2dh.bullpenWalk - pr2da.bullpenWalk) AS p_bull_diff, -- bullpenWalk
	   (pr2dh.Fly_Out - pr2da.Fly_Out) AS p_Fly_Out_diff, -- Fly_Out
	   (pr2dh.Field_Error - pr2da.Field_Error) AS p_Field_Error_diff, -- Field_Error
	   (pr2dh.Pop_Out - pr2da.Pop_Out) AS p_Pop_Out_diff, -- Pop_Out
	   (pr2dh.Line_Out - pr2da.Line_Out) AS p_Line_Out_diff, -- Line_Out
	   (pr2dh.Walk - pr2da.Walk) AS p_Walk_diff, -- Walk
       (pr2dh.pitchesThrown - pr2da.pitchesThrown) AS p_pitchesThrown_diff, -- pitchesThrown
       (pr2dh.avg_DaysSinceLastPitch - pr2da.avg_DaysSinceLastPitch) AS p_day_diff, -- diff between avg_DaysSinceLastPitch
	   (pr2dh.Walk + pr2dh.Hit)/(pr2dh.endingInning-pr2dh.startingInning) -
	   (pr2da.Walk + pr2da.Hit)/(pr2da.endingInning-pr2da.startingInning) AS p_WHIP_diff -- Walks plus hits per inning pitched
FROM   game g
JOIN   pitch_rolling_200_day pr2da ON pr2da.team_id = g.away_team_id AND g.game_id = pr2da.game_id
JOIN   pitch_rolling_200_day pr2dh ON pr2dh.team_id = g.home_team_id AND g.game_id = pr2dh.game_id
JOIN   boxscore b                  ON b.game_id = g.game_id );

CREATE UNIQUE INDEX game_home_away3 ON pitching_feature_diff(game_id, home_team_id, away_team_id);


/********************************* 200 day final features *********************************/
-- 40 features in total
DROP TABLE IF EXISTS final_features_200_day;
CREATE TABLE final_features_200_day AS (
SELECT bfd.*,
       ds.diff_streak,
       pfd.p_plateAppearance_diff,
       pfd.p_atBat_diff,
       pfd.p_Hit_diff,
       pfd.p_Strikeout_diff,
       pfd.p_HBP_diff,
       pfd.p_Double_Play_diff,
       pfd.p_Triple_Play_diff,
       pfd.p_bull_diff,
       pfd.p_Fly_Out_diff,
       pfd.p_Field_Error_diff,
       pfd.p_Pop_Out_diff,
       pfd.p_Line_Out_diff,
       pfd.p_Walk_diff,
       pfd.p_pitchesThrown_diff,
       pfd.p_day_diff,
       pfd.p_WHIP_diff
FROM   batting_feature_diff bfd
JOIN   diff_streak          ds  ON bfd.game_id = ds.game_id
AND    bfd.home_team_id = ds.home_team_id
AND    bfd.away_team_id = ds.away_team_id
JOIN   pitching_feature_diff pfd ON bfd.game_id = pfd.game_id
AND    bfd.home_team_id = pfd.home_team_id
AND    bfd.away_team_id = pfd.away_team_id);


DROP TABLE IF EXISTS batting_rolling_200_day;
DROP TABLE IF EXISTS batting_feature_diff;
DROP TABLE IF EXISTS avg_streak;
DROP TABLE IF EXISTS diff_streak;
DROP TABLE IF EXISTS pitch_rolling_200_day;
DROP TABLE IF EXISTS pitching_feature_diff;


