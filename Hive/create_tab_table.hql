use acn2021q3_stadnikov;

drop table if exists tab_dataset;

create external table tab_dataset (
    first_column string,
    second_column string,
    value int
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
location '/user/acn2021q3_stadnikov/hive_practice_data/';