REGISTER /usr/local/elephant-bird/elephant-bird-core-4.10.jar;
REGISTER /usr/local/elephant-bird/elephant-bird-pig-4.10.jar;
REGISTER /usr/local/elephant-bird/elephant-bird-hadoop-compat-4.10.jar;
REGISTER /usr/hdp/2.2.8.0-3150/pig/lib/piggybank.jar;
REGISTER /mnt/scratch/bcunningham/uber-bc-project-ingest-weather-0.0.1-SNAPSHOT.jar;

DEFINE WSThriftBytesToTuple com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('edu.uchicago.mpcs53013.weatherSummary.WeatherSummary');

-- Load data
raw_data = LOAD '/mnt/scratch/bcunningham/input/thriftWeather/weather-*' USING org.apache.pig.piggybank.storage.SequenceFileLoader() as (key:long, value: bytearray);

divvy_rides = LOAD '/mnt/scratch/bcunningham/divvy/part*' USING PigStorage(',')
  as (trip_id, date, month:int, day:int, year:int, tripduration:int, gender, birthyear:int);
 
weather_summary = FOREACH raw_data GENERATE FLATTEN(WSThriftBytesToTuple(value));

-- Filter for Chicago (Divvy's only in Chicago)
weather_summary_chi = FILTER weather_summary by (WeatherSummary::station==725340);

grouped_weather_raw = GROUP weather_summary_chi by (WeatherSummary::year, WeatherSummary::month, WeatherSummary::day);

grouped_weather = FOREACH grouped_weather_raw GENERATE group.year AS year, group.month AS month, group.day AS day, 
  
      AVG($1.meanTemperature) AS temp,
      AVG($1.meanVisibility) AS visibility,
      AVG($1.meanWindSpeed) AS windspeed,
      ROUND(AVG($1.fog)) AS fog,
      ROUND(AVG($1.rain)) AS rain,
      ROUND(AVG($1.snow)) AS snow,
      ROUND(AVG($1.hail)) AS hail,
      ROUND(AVG($1.thunder)) AS thunder,
      ROUND(AVG($1.tornado)) AS tornado,
      ((AVG($1.fog)==0 AND AVG($1.rain)==0 AND AVG($1.snow)==0 AND AVG($1.hail)==0 AND AVG($1.thunder)==0 AND AVG($1.tornado)==0) ? 1 : 0) AS clear;

grouped_divvy_raw = GROUP divvy_rides by (year, month, day);

grouped_divvy = FOREACH grouped_divvy_raw GENERATE group.year AS dyear, group.month AS dmonth, group.day AS dday,

      ROUND(AVG($1.tripduration)) AS avgtrip,
      COUNT($1.tripduration) AS numtrips,
      (2015 - ROUND(AVG($1.birthyear))) AS avgage;

joined_divvy_and_weather = JOIN grouped_divvy by (dyear, dmonth, dday), grouped_weather by (year, month, day);

divvy_and_weather = FOREACH joined_divvy_and_weather GENERATE year, month, day, avgtrip, numtrips, avgage, temp, visibility, windspeed, fog, rain, snow, hail, thunder, tornado, clear;

STORE divvy_and_weather into '/mnt/scratch/bcunningham/divvy_and_weather' USING PigStorage(',');
