{{ config(materialized='table') }}

WITH submissions AS (
    SELECT
        s.submissions_id,
        s.hacker_id,
        h.hacker_name,
        s.challenge_id,
        c.difficulty_level,
        d.difficulty_score,
        s.score
    FROM {{ ref('stg_submissions') }} s
    JOIN {{ ref('stg_hackers') }} h ON s.hacker_id = h.hacker_id
    JOIN {{ ref('stg_challenges') }} c ON s.challenge_id = c.challenge_id
    JOIN {{ ref('stg_difficulty') }} d ON c.difficulty_level = d.level_id
)

SELECT
    submissions_id,
    hacker_id,
    hacker_name,
    challenge_id,
    difficulty_level,
    difficulty_score,
    score,
    score * difficulty_score AS weighted_score
FROM submissions
