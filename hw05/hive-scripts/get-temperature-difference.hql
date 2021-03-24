-- Add hive-kafka handler
ADD JAR /usr/hdp/3.0.1.0-187/hive/lib/kafka-handler-3.1.0.3.1.0.6-1.jar;

-- Create result table
CREATE TABLE IF NOT EXISTS result_tmpr_diff (
    rank  BIGINT,
    hotel_id BIGINT,
    hotel_name STRING,
    max_tmpr_c_diff DOUBLE)
STORED AS ORC;


-- Query and write results
INSERT OVERWRITE TABLE result_tmpr_diff
    SELECT max_difs.rank AS rank,
           max_difs.hotel_id AS hotel_id,
           max_difs.hotel_name AS hotel_name,
           max_difs.max_tmpr_c_diff AS max_tmpr_c_diff
    FROM (SELECT RANK() over (ORDER BY MAX(difs.difference) DESC) AS rank,
                 difs.hotel_id,
                 difs.hotel_name,
                 MAX(difs.difference) AS max_tmpr_c_diff
            FROM (SELECT id AS hotel_id,
                         name AS hotel_name,
                         ABS(MAX(avg_tmpr_c) - MIN(avg_tmpr_c)) AS difference
                    FROM hotels_weather
                GROUP BY id, name, YEAR(wthr_date), MONTH(wthr_date)) difs
        GROUP BY hotel_id, hotel_name) max_difs
    WHERE max_difs.rank <= 10;
