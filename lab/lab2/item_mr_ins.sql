CREATE DATABASE IF NOT EXISTS dblab2;
CREATE TABLE IF NOT EXISTS dblab2.items_gym
  (itemid INT,item_category STRING) 
COMMENT 'subset of items_table' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE dblab2.items_gym 
select * from default.items where itemid in 
(select itemid from default.items where category like '%Gyms%');
