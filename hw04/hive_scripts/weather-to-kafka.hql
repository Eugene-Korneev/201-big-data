-- Add hive-kafka handler
ADD JAR /usr/hdp/3.0.1.0-187/hive/lib/kafka-handler-3.1.0.3.1.0.6-1.jar;

-- Map weather parquet data as hive table
CREATE EXTERNAL TABLE IF NOT EXISTS weather_parquet (
        lng DOUBLE,
        lat DOUBLE,
        avg_tmpr_f DOUBLE,
        avg_tmpr_c DOUBLE,
        wthr_date STRING
    )
    STORED AS PARQUET
    LOCATION '/201_hw_dataset/weather';

-- Map weather kafka stream as hive table
CREATE EXTERNAL TABLE IF NOT EXISTS weather (
        lng DOUBLE,
        lat DOUBLE,
        avg_tmpr_f DOUBLE,
        avg_tmpr_c DOUBLE,
        wthr_date STRING
    )
    STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
    TBLPROPERTIES (
        "kafka.topic" = "weather",
        "kafka.bootstrap.servers" = "sandbox-hdp:6667"
    );

-- write from parquet to kafka
INSERT INTO TABLE weather
    SELECT lng, lat, avg_tmpr_f, avg_tmpr_c, wthr_date,
        NULL AS `__key`, NULL AS `__partition`, -1 AS `__offset`, NULL AS `__timestamp`
    FROM weather_parquet;
