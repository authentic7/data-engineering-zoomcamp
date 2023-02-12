-- Create an external table using the fhv 2019 data. 
CREATE OR REPLACE EXTERNAL TABLE `sage-reach-375315.trips_data_all.fhv-2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_sage-reach-375315/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). 
CREATE OR REPLACE TABLE `sage-reach-375315.trips_data_all.fhv-2019-bq`
AS SELECT * 
FROM `sage-reach-375315.trips_data_all.fhv-2019`;

-- Question 1: What is count for fhv vehicle records for year 2019? *  
select count(*) from `trips_data_all.fhv-2019`
-- 43244696
-- Bytes processed: 2.52 GB

-- Question 3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset? 
select count(*) from `trips_data_all.fhv-2019` where PUlocationID is null and DOlocationID is null;
-- 717,748

-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always filter by pickup_datetime and order by affiliated_base_number?
CREATE OR REPLACE TABLE `sage-reach-375315.trips_data_all.fhv-2019_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `sage-reach-375315.trips_data_all.fhv-2019`;

-- Question 5:  
-- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 
-- 03/01/2019 and 03/31/2019 (inclusive)

select distinct Affiliated_base_number 
from `trips_data_all.fhv-2019-bq`
where pickup_datetime >= '2019-03-01' 
and pickup_datetime <= '2019-03-31';
-- 647.87 MB for non-partitioned table

SELECT distinct Affiliated_base_number 
from `trips_data_all.fhv-2019_partitioned_clustered` 
where pickup_datetime >= '2019-03-01' 
and pickup_datetime <= '2019-03-31';
-- 23.06 MB for the partitioned table