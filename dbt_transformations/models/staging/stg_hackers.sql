{{ config(materialized='view') }}

SELECT 
  CAST(column1 AS INT) AS hacker_id,
  CAST(column2 AS STRING) AS hacker_name
FROM {{ source('dataset_challenge_ipnet', 'hackers') }}