-- comment 
create database CAT_DB;

use database CAT_DB;

create or replace table yelp_training (val variant);
create or replace table yelp_testing (val variant);


copy into yelp_training from @CAT_STAGE/train-00000-of-00001.parquet FIlE_FORMAT =training_db.TPCH_SF1.MYPARQUETFORMAT;


copy into yelp_testing from @CAT_STAGE/test-00000-of-00001.parquet FIlE_FORMAT =training_db.TPCH_SF1.MYPARQUETFORMAT;

-- val:<stuff>
select * from yelp_testing WHERE val:label = 0 OR val:label = 4;;
select * from yelp_training;

