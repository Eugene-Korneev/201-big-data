-- Add hive-kafka handler
ADD JAR /usr/hdp/3.0.1.0-187/hive/lib/kafka-handler-3.1.0.3.1.0.6-1.jar;

-- Add geohash UDF
CREATE TEMPORARY FUNCTION geohashUDF
    AS 'com.hive.udf.GeohashUDF'
USING JAR '/home/app/hive-kafka-udf-1.0-SNAPSHOT.jar';

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


-- Map hotels kafka stream as hive table
CREATE EXTERNAL TABLE IF NOT EXISTS hotels (
        Id BIGINT,
        Name STRING,
        Country STRING,
        City STRING,
        Address STRING,
        Latitude DOUBLE,
        Longitude DOUBLE,
        GeoHash STRING
    )
    STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
    TBLPROPERTIES (
        "kafka.topic" = "hotels",
        "kafka.bootstrap.servers" = "sandbox-hdp:6667"
    );

-- Create temporary parquet table for results
CREATE TABLE IF NOT EXISTS hotels_weather_parquet (
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
        avg_tmpr_c DOUBLE
    )
    STORED AS PARQUET;

-- query and write enriched data to parquet
INSERT OVERWRITE TABLE hotels_weather_parquet
    SELECT h.Id AS id,
           h.Name AS name,
           h.Country AS country,
           h.City AS city,
           h.Address AS address,
           h.Latitude AS lat,
           h.Longitude AS lng,
           h.Geohash AS geohash,
           h.wthr_date AS wthr_date,
           AVG(w.avg_tmpr_f) AS avg_tmpr_f,
           AVG(w.avg_tmpr_c) AS avg_tmpr_c
    FROM (
        SELECT Id, Name, Country, City, Address, Latitude, Longitude, GeoHash, wthr_date
        FROM hotels
        CROSS JOIN (
            SELECT DISTINCT wthr_date
            FROM weather_parquet
        ) AS wd
    ) AS h
    LEFT JOIN (
        SELECT lng, lat, avg_tmpr_f, avg_tmpr_c, wthr_date, geohashUDF(lat, lng, 4) AS geohash
        FROM weather_parquet
    ) AS w
    ON (h.wthr_date = w.wthr_date AND h.GeoHash = w.geohash)
    GROUP BY h.Id, h.Name, h.Country, h.City, h.Address, h.Latitude, h.Longitude, h.Geohash,
             h.wthr_date;

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
        avg_tmpr_c DOUBLE
    )
    STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
    TBLPROPERTIES (
        "kafka.topic" = "hotels-weather",
        "kafka.bootstrap.servers" = "sandbox-hdp:6667"
    );

-- write enriched data to kafka
INSERT INTO TABLE hotels_weather
    SELECT id,
           name,
           country,
           city,
           address,
           lat,
           lng,
           geohash,
           wthr_date,
           avg_tmpr_f,
           avg_tmpr_c,
           NULL AS `__key`,
           NULL AS `__partition`,
           -1 AS `__offset`,
           NULL AS `__timestamp`
    FROM hotels_weather_parquet;

-- write enriched data to hdfs as parquet
--INSERT OVERWRITE DIRECTORY '/201_hw_dataset/hotels_weather' STORED AS PARQUET
--    SELECT * FROM hotels_weather_parquet;

CREATE EXTERNAL TABLE IF NOT EXISTS hotels_and_weather (
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
        avg_tmpr_c DOUBLE
    )
    STORED AS PARQUET
    LOCATION '/201_hw_dataset/hotels_weather';

INSERT OVERWRITE TABLE hotels_and_weather
    SELECT id,
           name,
           country,
           city,
           address,
           lat,
           lng,
           geohash,
           wthr_date,
           avg_tmpr_f,
           avg_tmpr_c
    FROM hotels_weather_parquet;
