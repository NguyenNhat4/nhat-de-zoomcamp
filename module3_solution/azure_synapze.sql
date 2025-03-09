-- Query public available table (equivalent - you would reference the appropriate table in your Azure environment)
SELECT station_id, name FROM
    [your_database].[your_schema].[citibike_stations]
WHERE 1=1
LIMIT 100;

-- Creating external table referring to azure storage path
CREATE EXTERNAL TABLE [nytaxi].[external_yellow_tripdata]
(
    -- Include all columns from the yellow_tripdata schema here
    -- For example:
    VendorID INT,
    tpep_pickup_datetime DATETIME2,
    tpep_dropoff_datetime DATETIME2,
    passenger_count INT,
    trip_distance FLOAT,
    -- Continue with all columns needed
    ...
)
WITH
(
    LOCATION = '/trip data/yellow_tripdata_*.csv',
    DATA_SOURCE = [nyc_taxi_data_source],
    FILE_FORMAT = [CSV_FILE_FORMAT]
);

-- Note: You need to create the data source and file format first with commands like:
CREATE EXTERNAL DATA SOURCE [nyc_taxi_data_source]
WITH
(
    TYPE = HADOOP,
    LOCATION = 'abfs://nyc-tl-data@yourstorageaccount.dfs.core.windows.net'
);

CREATE EXTERNAL FILE FORMAT [CSV_FILE_FORMAT]
WITH
(
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2 -- Assuming first row has headers
    )
);

-- Check yellow trip data
SELECT TOP 10 * FROM [nytaxi].[external_yellow_tripdata];

-- Create a non partitioned table from external table
CREATE TABLE [nytaxi].[yellow_tripdata_non_partitioned]
WITH
(
    DISTRIBUTION = ROUND_ROBIN
)
AS
SELECT * FROM [nytaxi].[external_yellow_tripdata];

-- Create a partitioned table (using hash distribution on pickup datetime)
CREATE TABLE [nytaxi].[yellow_tripdata_partitioned]
WITH
(
    DISTRIBUTION = HASH(tpep_pickup_datetime),
    PARTITION (
        tpep_pickup_datetime_pt RANGE RIGHT FOR VALUES (
            '2019-01-01', '2019-02-01', '2019-03-01', -- Add all months
            '2019-04-01', '2019-05-01', '2019-06-01',
            '2019-07-01', '2019-08-01', '2019-09-01',
            '2019-10-01', '2019-11-01', '2019-12-01',
            '2020-01-01', '2020-02-01', '2020-03-01',
            '2020-04-01', '2020-05-01', '2020-06-01',
            '2020-07-01', '2020-08-01', '2020-09-01',
            '2020-10-01', '2020-11-01', '2020-12-01',
            '2021-01-01'
        )
    )
)
AS
SELECT *, CAST(tpep_pickup_datetime AS DATE) AS tpep_pickup_datetime_pt
FROM [nytaxi].[external_yellow_tripdata];

-- Impact of partition - Query for non-partitioned table
SELECT DISTINCT VendorID
FROM [nytaxi].[yellow_tripdata_non_partitioned]
WHERE CAST(tpep_pickup_datetime AS DATE) BETWEEN '2019-06-01' AND '2019-06-30';

-- Query for partitioned table
SELECT DISTINCT VendorID
FROM [nytaxi].[yellow_tripdata_partitioned]
WHERE CAST(tpep_pickup_datetime AS DATE) BETWEEN '2019-06-01' AND '2019-06-30';

-- Get information about partitions (Azure Synapse equivalent)
SELECT 
    OBJECT_NAME(object_id) AS table_name,
    partition_number,
    rows
FROM sys.partitions
WHERE OBJECT_NAME(object_id) = 'yellow_tripdata_partitioned'
ORDER BY rows DESC;

-- Creating a clustered columnstore index table with partitioning
CREATE TABLE [nytaxi].[yellow_tripdata_partitioned_clustered]
WITH
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH(VendorID),
    PARTITION (
        tpep_pickup_datetime_pt RANGE RIGHT FOR VALUES (
            '2019-01-01', '2019-02-01', '2019-03-01', -- Add all months
            '2019-04-01', '2019-05-01', '2019-06-01',
            '2019-07-01', '2019-08-01', '2019-09-01',
            '2019-10-01', '2019-11-01', '2019-12-01',
            '2020-01-01', '2020-02-01', '2020-03-01',
            '2020-04-01', '2020-05-01', '2020-06-01',
            '2020-07-01', '2020-08-01', '2020-09-01',
            '2020-10-01', '2020-11-01', '2020-12-01',
            '2021-01-01'
        )
    )
)
AS
SELECT *, CAST(tpep_pickup_datetime AS DATE) AS tpep_pickup_datetime_pt
FROM [nytaxi].[external_yellow_tripdata];

-- Performance comparison query 1
SELECT COUNT(*) AS trips
FROM [nytaxi].[yellow_tripdata_partitioned]
WHERE CAST(tpep_pickup_datetime AS DATE) BETWEEN '2019-06-01' AND '2020-12-31'
AND VendorID = 1;

-- Performance comparison query 2
SELECT COUNT(*) AS trips
FROM [nytaxi].[yellow_tripdata_partitioned_clustered]
WHERE CAST(tpep_pickup_datetime AS DATE) BETWEEN '2019-06-01' AND '2020-12-31'
AND VendorID = 1;