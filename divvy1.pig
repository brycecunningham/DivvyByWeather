-- Import the CSVLoader plugin (available from the 3rd party piggybank modules collection)
REGISTER /usr/hdp/2.2.8.0-3150/pig/lib/piggybank.jar;

-- Load divvy csv data from hdfs
A = LOAD '/mnt/scratch/bcunningham/input/Divvy_Trips_2014*' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER'); 

--Take selected columns
B = FOREACH A GENERATE $0 AS trip_id, $1 AS starttime, $2 AS stoptime, $4 AS tripduration, $10 AS gender, $11 AS birthyear;
    
-- Filter out transactions that are missing the fields we are interested in
C = FILTER B BY (trip_id neq '') AND (starttime neq '') AND (stoptime neq '') AND (tripduration neq '') AND (gender neq '') AND (birthyear neq '');

-- use regex_extract to pull out month, day and year from starttime
divvy_rides = FOREACH C GENERATE trip_id, REGEX_EXTRACT(starttime,'(.*) (.*)',1) AS date, REGEX_EXTRACT(starttime,'(.*)/(.*)/(.*) (.*)',1) AS month, REGEX_EXTRACT(starttime,'(.*)/(.*)/(.*) (.*)',2) AS day, REGEX_EXTRACT(starttime,'(.*)/(.*)/(.*) (.*)',3) AS year, tripduration, gender, birthyear;



-- Store the resulting data into HDFS as a CSV file

STORE divvy_rides INTO '/mnt/scratch/bcunningham/divvy' USING PigStorage(',');
