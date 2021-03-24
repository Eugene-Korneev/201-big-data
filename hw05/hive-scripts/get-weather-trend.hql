-- Add hive-kafka handler
ADD JAR /usr/hdp/3.0.1.0-187/hive/lib/kafka-handler-3.1.0.3.1.0.6-1.jar;

-- Create result table
CREATE TABLE IF NOT EXISTS result_wthr_trend (
    visit_id BIGINT,
    checkin STRING,
    checkout STRING,
    days_of_stay INTEGER,
    checkin_tmpr DOUBLE,
    checkout_tmpr DOUBLE,
    weather_trend DOUBLE,
    avg_tmpr_during_stay DOUBLE)
STORED AS ORC;


-- Query and write results
INSERT OVERWRITE TABLE result_wthr_trend
    SELECT e.id AS visit_id,
           e.srch_ci AS checkin,
           e.srch_co AS checkout,
           datediff(e.srch_co, e.srch_ci) AS days_of_stay,
           hw1.avg_tmpr_c AS checkin_tmpr,
           hw2.avg_tmpr_c AS checkout_tmpr,
           hw2.avg_tmpr_c - hw1.avg_tmpr_c AS weather_trend,
           tat.avg_tmpr_during_stay AS avg_tmpr_during_stay
    FROM expedia e
    LEFT JOIN hotels_weather hw1
        ON e.srch_ci = hw1.wthr_date AND e.hotel_id = hw1.id
    LEFT JOIN hotels_weather hw2
        ON e.srch_co = hw2.wthr_date AND e.hotel_id = hw2.id
    LEFT JOIN (SELECT exp_hw.visit_id,
                      AVG(exp_hw.avg_tmpr_c) AS avg_tmpr_during_stay
                FROM (
                    SELECT exp_pe.visit_id,
                           hw.avg_tmpr_c
                    FROM (
                        SELECT exp.visit_id,
                               exp.hotel_id,
                               date_add(exp.checkin, pe.i) AS visit_date
                        FROM (SELECT e.id AS visit_id,
                                     e.srch_ci AS checkin,
                                     e.srch_co AS checkout,
                                     datediff(e.srch_co, e.srch_ci) AS days_of_stay,
                                     e.hotel_id
                              FROM expedia e
                              WHERE datediff(e.srch_co, e.srch_ci) > 7) exp
                        LATERAL VIEW
                            POSEXPLODE(SPLIT(SPACE(datediff(exp.checkout, exp.checkin)),' ')) pe as i,x) exp_pe
                    LEFT JOIN hotels_weather hw
                        ON exp_pe.visit_date = hw.wthr_date AND exp_pe.hotel_id = hw.id) exp_hw
                GROUP BY exp_hw.visit_id) tat
        ON e.id = tat.visit_id
    WHERE datediff(e.srch_co, e.srch_ci) > 7;
