-- Add hive-kafka handler
ADD JAR /usr/hdp/3.0.1.0-187/hive/lib/kafka-handler-3.1.0.3.1.0.6-1.jar;

-- Map expedia ORC data as hive table
CREATE EXTERNAL TABLE IF NOT EXISTS expedia (
    id  BIGINT,
    date_time STRING,
    site_name INTEGER,
    posa_continent INTEGER,
    user_location_country INTEGER,
    user_location_region INTEGER,
    user_location_city INTEGER,
    orig_destination_distance DOUBLE,
    user_id INTEGER,
    is_mobile INTEGER,
    is_package INTEGER,
    channel INTEGER,
    srch_ci STRING,
    srch_co STRING,
    srch_adults_cnt INTEGER,
    srch_children_cnt INTEGER,
    srch_rm_cnt INTEGER,
    srch_destination_id INTEGER,
    srch_destination_type_id INTEGER,
    hotel_id BIGINT)
STORED AS AVRO
LOCATION '/201_hw_dataset/expedia';

-- Map hotels weather kafka stream
CREATE EXTERNAL TABLE IF NOT EXISTS hotels_weather (
    id BIGINT,
    name STRING,
    country STRING,
    city STRING,
    address STRING,
    lat DOUBLE,
    lng DOUBLE,
    geohash STRING,
    wthr_date STRING,
    avg_tmpr_f DOUBLE,
    avg_tmpr_c DOUBLE)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES (
    "kafka.topic" = "hotels-weather",
    "kafka.bootstrap.servers" = "sandbox-hdp:6667");
