CREATE EXTERNAL IF NOT EXISTS TABLE `customer_landing`(
  `customername` string, 
  `email` string, 
  `phone` string, 
  `birthday` string, 
  `serialnumber` string, 
  `registrationdate` bigint, 
  `lasupdatedate` bigint, 
  `sharewithresearchasofdate` bigint, 
  `sharewithfriendsasofdate` bigint, 
  `sharewithpublicasofdate` bigint )
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://customerrecords2023/customer/landing'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1685645449')