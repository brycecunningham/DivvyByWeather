divvy_and_weather = LOAD '/mnt/scratch/bcunningham/divvy_and_weather' USING PigStorage(',')
      as (year:int, month:int, day:int, avgtrip:int, numtrips:int, avgage:int, temp:double, visibility:double,
      windspeed:double, fog:int, rain:int, snow:int, hail:int, thunder:int, tornado:int, clear:int); 


A = FOREACH divvy_and_weather GENERATE year, month, day, numtrips, (avgtrip * numtrips) AS totaltriptime, fog, rain, snow, hail, thunder, tornado, clear;

B = FOREACH A GENERATE year, month, day,
      (fog==1 ? totaltriptime : 0) AS fogtriptime,
      (rain==1 ? totaltriptime : 0) AS raintriptime,
      (snow==1 ? totaltriptime : 0) AS snowtriptime,
      (hail==1 ? totaltriptime : 0) AS hailtriptime,
      (thunder==1 ? totaltriptime : 0) AS thundertriptime,
      (tornado==1 ? totaltriptime : 0) AS tornadotriptime,
      (clear==1 ? totaltriptime : 0) AS cleartriptime,

      (fog==1 ? numtrips : 0) AS fogtrips,
      (rain==1 ? numtrips : 0) AS raintrips,
      (snow==1 ? numtrips : 0) AS snowtrips,
      (hail==1 ? numtrips : 0) AS hailtrips,
      (thunder==1 ? numtrips : 0) AS thundertrips,
      (tornado==1 ? numtrips : 0) AS tornadotrips,
      (clear==1 ? numtrips : 0) AS cleartrips;

C = GROUP B by year;

D = FOREACH C GENERATE SUM($1.fogtriptime) AS totalftt, SUM($1.raintriptime) AS totalrtt, SUM($1.snowtriptime) AS totalstt, SUM($1.hailtriptime) AS totalhtt, SUM($1.thundertriptime) AS totalthtt, SUM($1.tornadotriptime) AS totaltott, SUM($1.cleartriptime) AS totalctt, SUM($1.fogtrips) AS totalfog, SUM($1.raintrips) AS totalrain, SUM($1.snowtrips) AS totalsnow, SUM($1.hailtrips) AS totalhail, SUM($1.thundertrips) AS totalthunder, SUM($1.tornadotrips) AS totaltornado, SUM($1.cleartrips) AS totalclear;

divvy_by_weather = FOREACH D GENERATE 1 AS key, (totalftt/totalfog) AS avgfog, totalfog, (totalrtt/totalrain) AS avgrain, totalrain, (totalstt/totalsnow) AS avgsnow, totalsnow, (totalhtt/totalhail) AS avghail, totalhail, (totalthtt/totalthunder) AS avgthunder, totalthunder, (totaltott/totaltornado) AS avgtornado, totaltornado, (totalctt/totalclear) AS avgclear, totalclear;

STORE divvy_by_weather INTO 'hbase://bc_divvy_by_weather' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
      'divvyinfo:avgfog, divvyinfo:totalfog, divvyinfo:avgrain, divvyinfo:totalrain, divvyinfo:avgsnow, divvyinfo:totalsnow, divvyinfo:avghail, divvyinfo:totalhail, divvyinfo:avgthunder, divvyinfo:totalthunder, divvyinfo:avgtornado, divvyinfo:totaltornado, divvyinfo:avgclear, divvyinfo:totalclear');
