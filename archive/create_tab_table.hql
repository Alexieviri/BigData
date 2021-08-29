use acn2021q3_stadnikov;
drop table if exists tab_dataset;
create table tab_dataset (
first_column string,
second_column string,
value int
)
location '/user/acn2021q3_stadnikov/hive_practice_data/';