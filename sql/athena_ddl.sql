CREATE EXTERNAL TABLE `airflow`(
  `id` string, 
  `nome` string, 
  `telefone` string, 
  `dtnascimento` string, 
  `endereço` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://airflowbucketparquet/parquet/2023-04-21'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='none', 
  'typeOfData'='file')

