-- Add hive-kafka handler
ADD JAR /usr/hdp/3.0.1.0-187/hive/lib/kafka-handler-3.1.0.3.1.0.6-1.jar;

-- Create result table
CREATE TABLE IF NOT EXISTS result_busy_hotels (
    year  INTEGER,
    month INTEGER,
    rank  BIGINT,
    hotel_id BIGINT,
    visits_per_month BIGINT)
STORED AS ORC;


-- Query and write results
INSERT OVERWRITE TABLE result_busy_hotels
    SELECT mon_vis.year AS year,
           mon_vis.month AS month,
           mon_vis.rank AS rank,
           mon_vis.hotel_id AS hotel_id,
           mon_vis.visits AS visits_per_month
    FROM (SELECT YEAR(expl.month_date) AS year,
                 MONTH(expl.month_date) AS month,
                 ROW_NUMBER() OVER (
                     PARTITION BY YEAR(expl.month_date), MONTH(expl.month_date)
                     ORDER BY (SUM(expl.srch_adults_cnt) + SUM(expl.srch_children_cnt)) DESC) AS rank,
                 expl.hotel_id,
                 (SUM(expl.srch_adults_cnt) + SUM(expl.srch_children_cnt)) AS visits
            FROM (SELECT e.hotel_id,
                         e.srch_adults_cnt,
                         e.srch_children_cnt,
                         ADD_MONTHS(e.srch_ci, pe.i) as month_date
                    FROM expedia e
                LATERAL VIEW
                    POSEXPLODE(SPLIT(SPACE(CAST(MONTHS_BETWEEN(e.srch_co, e.srch_ci) AS INT)),' ')) pe as i,x) expl
        GROUP BY YEAR(expl.month_date), MONTH(expl.month_date), expl.hotel_id) mon_vis
    WHERE mon_vis.rank <= 10
    ORDER BY year DESC, month DESC, rank;
