divvy_and_weather = LOAD '/mnt/scratch/bcunningham/divvy_and_weather' USING PigStorage(',')
      as (year:int, month:int, day:int, avgtrip:int, numtrips:int, avgage:int, temp:double, visibility:double,
      windspeed:double, fog:int, rain:int, snow:int, hail:int, thunder:int, tornado:int, clear:int); 

test = FOREACH divvy_and_weather GENERATE year, month, day, avgtrip, numtrips, (avgtrip * numtrips) AS totaltriptime, avgage, (avgage * numtrips) AS totalageyears, temp, visibility, windspeed;

grouped_by_month = GROUP test by month;

divvy_by_month_raw = FOREACH grouped_by_month GENERATE group AS key,
      SUM($1.totaltriptime)  AS totaltriptime,
      SUM($1.numtrips) AS numtrips,
      SUM($1.totalageyears)  AS totalageyears,
      AVG($1.temp) AS temp,
      AVG($1.visibility) AS visibility,
      AVG($1.windspeed) AS windspeed;

divvy_by_month = FOREACH divvy_by_month_raw GENERATE key, 
      (CASE key
          WHEN 1 THEN 'January'
          WHEN 2 THEN 'February'
          WHEN 3 THEN 'March'
          WHEN 4 THEN 'April'
          WHEN 5 THEN 'May'
          WHEN 6 THEN 'June'
          WHEN 7 THEN 'July'
          WHEN 8 THEN 'August'
          WHEN 9 THEN 'September'
          WHEN 10 THEN 'October'
          WHEN 11 THEN 'November'
          WHEN 12 THEN 'December'
      END) AS month_word,
      (totaltriptime / numtrips) AS avgtrip, numtrips, (totalageyears / numtrips) AS avgage, temp, visibility, windspeed;

STORE divvy_by_month INTO 'hbase://bc_divvy_by_month' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
      'divvyinfo:month, divvyinfo:avgtrip, divvyinfo:numtrips, divvyinfo:avgage, divvyinfo:temp, divvyinfo:visibility, 
      divvyinfo:windspeed');
