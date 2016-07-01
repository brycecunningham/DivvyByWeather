My project looks at Divvy bike ride info from 2014 and combines it with the weather data we used in class.


A few notes:

I didn't submit a .sh file for getting or ingesting the divvy data to hdfs. The csv files I downloaded from
the Divvy website weren't named consistently so I manually renamed a couple of them before putting them
directly into hdfs (like we did with the flight data; no thrift).

(I submitted the divvy csv files to phoenixforge but not the weather data since it's the same .gz files we used
in the class weather/flight app)

I then ran pig scripts to combine the divvy and weather data to get trip times, number of trips, average
rider age, average temperature, fog, rain, et. al. by each month and day.

My perl and html files have a form for the user to select a month and then certain average divvy info
for that month is shown in the first table. In the second table, avg divvy ride time and number of rides
by fog, rain, snow, etc. are shown for 2014.

I submitted weather and divvy topologies though they were still in progress.


