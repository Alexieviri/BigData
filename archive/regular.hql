ADD JAR /usr/local/hive/lib/hive-serde.jar;

USE acn2021q3_stadnikov;

DROP TABLE IF EXISTS ser_de_example;

CREATE EXTERNAL TABLE ser_de_example (
    ip STRING,
    `date` STRING,
    request STRING,
    response_code INT
)
ROW FORMAT
    serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
    with serdeproperties (
        "input.regex" = "^(\\S+)\\t*(\\S+)\\t*(\\S+)\\t*(\\d+)\\t.*"
    )
STORED AS textfile
LOCATION '/data/user_logs/user_logs_M';

SELECT * FROM ser_de_example LIMIT 10;