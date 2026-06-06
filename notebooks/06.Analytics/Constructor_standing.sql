-- Databricks notebook source
CREATE OR REPLACE VIEW formula1_incremental.gold.vw_constructor_standing AS
WITH driver_session_summary AS 
(
  SELECT 
    r.season,
    c.constructor_name,
    c.nationality,
    COUNT(*) AS race_starts,
    SUM(r.points) AS total_points,
    COUNT_IF(is_win) AS number_of_wins,
    SUM(CASE WHEN r.is_podium = true THEN 1 ELSE 0 END) AS number_of_podiums
  FROM formula_1.gold.fact_results r
  JOIN formula_1.gold.dim_constructors c
    ON r.constructor_id = c.constructor_id
  WHERE
    r.session_type = 'RACE'
  GROUP BY   r.season, c.constructor_name, c.nationality
)

SELECT
  *,
  DENSE_RANK() OVER(PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing_position
FROM driver_session_summary
ORDER BY season DESC, total_points DESC


-- COMMAND ----------

WITH constructors_metrics AS
(
SELECT
  constructor_name,
  SUM(race_starts) AS total_races,
  SUM(number_of_wins) AS total_wins,
  SUM(number_of_podiums) AS total_podiums,
  SUM(CASE
        WHEN standing_position = 1 THEN 1 ELSE 0
      END) AS total_championships
FROM formula_1.gold.vw_constructor_standing
GROUP BY constructor_name
HAVING total_championships>0
)

SELECT 
  *,
  round((total_championships*100 + (total_wins/total_races*100)*30 + total_podiums*10),2) AS total_score
FROM constructors_metrics
ORDER BY total_score DESC
