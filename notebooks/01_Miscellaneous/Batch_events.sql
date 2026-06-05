-- Databricks notebook source
-- Creacion de tabla para el uso del trigger

CREATE TABLE IF NOT EXISTS formula_1.default.batch_events
(
    batch_id INT,
    event_timestamp TIMESTAMP
)

-- COMMAND ----------

-- Activar el trigger insertando una fila en la tabla

INSERT INTO formula_1.default.batch_events
VALUES (1, current_timestamp());

-- COMMAND ----------

-- Activar el trigger por segunda vez

INSERT INTO formula_1.default.batch_events
VALUES (2, current_timestamp());

-- COMMAND ----------

-- Revisar la tabla

SELECT * FROM formula_1.default.batch_events;
