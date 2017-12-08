CREATE DATABASE IF NOT EXISTS dblab2;
CREATE TABLE IF NOT EXISTS dblab2.user_log_good
(seq INT, user_id INT,item_id INT,cat_id INT,merchant_id INT,
brand_id INT,month STRING,day STRING,action INT,age_range INT, gender INT,province STRING) 
COMMENT 'loyal user log for taubau' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

use dblab2;

INSERT OVERWRITE TABLE user_log_good
select * from user_log where user_id in 
(select user_id from user_log where action='2' group by user_id having count(action='2')>2); 