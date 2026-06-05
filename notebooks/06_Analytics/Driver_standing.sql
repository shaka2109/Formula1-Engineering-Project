-- Databricks notebook source
CREATE OR REPLACE VIEW formula_1.gold.vw_driver_standing AS
WITH driver_session_summary AS 
(
  SELECT 
    r.season,
    d.driver_name,
    d.nationality,
    COUNT(*) AS race_starts,
    SUM(r.points) AS total_points,
    COUNT_IF(is_win) AS number_of_wins,
    SUM(CASE WHEN r.is_podium = true THEN 1 ELSE 0 END) AS number_of_podiums
  FROM formula_1.gold.fact_results r
  JOIN formula_1.gold.dim_drivers d
    ON r.driver_id = d.driver_id
  WHERE
    r.session_type = 'RACE'
  GROUP BY   r.season, d.driver_name, d.nationality
)

SELECT
  *,
  DENSE_RANK() OVER(PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing_position
FROM driver_session_summary
ORDER BY season DESC, total_points DESC
