-- Jinja2 Template: waterfall.sql.j2
-- Purpose: Unified waterfall report with base/channel BA segments and non-BA bucket segments, including regain metric.
-- Records claimed for segA
SELECT
  'segA' AS section,
  'Records Claimed' AS stat_name,
  NULL AS check_name,
  COUNT(*) AS cntr
FROM elig c
WHERE base_filter

UNION ALL

-- Detailed breakdown for segA

WITH current_segment_population AS (
    SELECT *
    FROM elig c
    WHERE base_filter
),
flags AS (
    SELECT
      id, 
      s1, 
      s2
    FROM current_segment_population
)
SELECT
  'segA' AS section,
  stat_name,
  check_name,
  cntr
FROM (
    SELECT
      'unique_drops' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's1' AS check_name,SUM(CASE WHEN s1 = 0 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      UNION ALL
      
      SELECT
        's2' AS check_name,SUM(CASE WHEN s2 = 0 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) qUNION ALL
    SELECT
      'incremental_drops' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's1' AS check_name,SUM(CASE WHEN s1 = 0 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      UNION ALL
      
      SELECT
        's2' AS check_name,SUM(CASE WHEN s2 = 0 AND s1 = 1 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) qUNION ALL
    SELECT
      'remaining' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's1' AS check_name,SUM(CASE WHEN s1 = 1 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      UNION ALL
      
      SELECT
        's2' AS check_name,SUM(CASE WHEN s2 = 1 AND s1 = 1 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) qUNION ALL
    SELECT
      'cumulative_drops' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's1' AS check_name,COUNT(*) - SUM(CASE WHEN s1 = 1 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      UNION ALL
      
      SELECT
        's2' AS check_name,COUNT(*) - SUM(CASE WHEN s2 = 1 AND s1 = 1 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) q

    UNION ALL

    -- Regain metric for the segment
    SELECT
      'regain' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's1' AS check_name,
        SUM(CASE WHEN s1 = 0 AND s2 = 1
            AND NOT ( s3 = 1 )
        THEN 1 ELSE 0 END) AS cntr
      FROM flags
      UNION ALL
      
      SELECT
        's2' AS check_name,
        SUM(CASE WHEN s2 = 0 AND s1 = 1
            AND NOT ( s3 = 1 )
        THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) r
  ) metrics
  UNION ALL
-- Records claimed for segB
SELECT
  'segB' AS section,
  'Records Claimed' AS stat_name,
  NULL AS check_name,
  COUNT(*) AS cntr
FROM elig c
WHERE base_filter
  AND NOT ( c.s1 = 1 OR c.s2 = 1 )

UNION ALL

-- Detailed breakdown for segB

WITH current_segment_population AS (
    SELECT *
    FROM elig c
    WHERE base_filter
      AND NOT ( c.s1 = 1 OR c.s2 = 1 )
),
flags AS (
    SELECT
      id, 
      s3
    FROM current_segment_population
)
SELECT
  'segB' AS section,
  stat_name,
  check_name,
  cntr
FROM (
    SELECT
      'unique_drops' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's3' AS check_name,SUM(CASE WHEN s3 = 0 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) qUNION ALL
    SELECT
      'incremental_drops' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's3' AS check_name,SUM(CASE WHEN s3 = 0 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) qUNION ALL
    SELECT
      'remaining' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's3' AS check_name,SUM(CASE WHEN s3 = 1 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) qUNION ALL
    SELECT
      'cumulative_drops' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's3' AS check_name,COUNT(*) - SUM(CASE WHEN s3 = 1 THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) q

    UNION ALL

    -- Regain metric for the segment
    SELECT
      'regain' AS stat_name,
      check_name,
      cntr
    FROM (
      SELECT
        's3' AS check_name,
        SUM(CASE WHEN s3 = 0
        THEN 1 ELSE 0 END) AS cntr
      FROM flags
      
      
    ) r
  ) metrics
  ;
