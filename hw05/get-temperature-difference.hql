-- Add hive-kafka handler
ADD JAR /usr/hdp/3.0.1.0-187/hive/lib/kafka-handler-3.1.0.3.1.0.6-1.jar;

-- Query
SELECT max_difs.rank AS rank,
       max_difs.hotel_id AS hotel_id,
       max_difs.hotel_name AS hotel_name,
       max_difs.max_tmpr_c_diff AS max_tmpr_c_diff
FROM (SELECT rank() over (ORDER BY MAX(difs.difference) DESC) AS rank,
             difs.hotel_id,
             difs.hotel_name,
             MAX(difs.difference) AS max_tmpr_c_diff
        FROM (SELECT id AS hotel_id,
                     name AS hotel_name,
                     ABS(MAX(avg_tmpr_c) - MIN(avg_tmpr_c)) AS difference
                FROM hotels_weather
            GROUP BY id, name, MONTH(wthr_date)) difs
    GROUP BY hotel_id, hotel_name) max_difs
WHERE max_difs.rank <= 10;
