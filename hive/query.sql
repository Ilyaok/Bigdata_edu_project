ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;

USE goldovskijil;

DROP TABLE IF EXISTS user_logs;
CREATE EXTERNAL TABLE user_logs (
    ip STRING,
    request_time STRING,
    request STRING,
    page_size INT,
    http_status_code INT,
    app_info STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S*)[\\s*]+(\\d*)[\\s*]+(\\S*)[\\s*]+(\\d*)[\\s*]+(\\d*)[\\s*]+(.*)$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=10000;

DROP TABLE IF EXISTS logs;
CREATE EXTERNAL TABLE logs (
    ip STRING,
    request STRING,
    page_size INT,
    http_status_code INT,
    app_info STRING
)
PARTITIONED BY (request_time INT)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE logs PARTITION (request_time)
SELECT
    ip,
    request,
    page_size,
    http_status_code,
    app_info,
    cast(substr(request_time, 1, 8) as int)
FROM user_logs;

SELECT * FROM logs LIMIT 10;

DROP TABLE IF EXISTS users;
CREATE EXTERNAL TABLE users (
    ip STRING,
    browser STRING,
    sex STRING,
    age INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S*)[\\s*]+(\\S*)[\\s*]+(\\S*)[\\s*]+(\\d*)$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M';

SELECT * FROM users LIMIT 10;

DROP TABLE IF EXISTS IPRegions;
CREATE EXTERNAL TABLE IPRegions (
    ip STRING,
    region STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S*)[\\s*]+(.*)$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';

SELECT * FROM IPRegions LIMIT 10;

DROP TABLE IF EXISTS subnets;
CREATE EXTERNAL TABLE subnets (
    ip STRING,
    mask STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = '^(\\S*)[\\s*]+(.*)$'
)
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';

SELECT * FROM subnets LIMIT 10;
