#!/usr/bin/perl -w
# Creates an html table of divvy info by month and then by weather

# Needed includes
use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;

# Read the selected month from the form
my $month_num = param('month_num');



# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server
my $hbase = HBase::JSONRest->new(host => "hadoop-m:2056");

# This function takes a row and gives you the value of the given column
# E.g., cellValue($row, 'delay:rain_delay') gives the value of the
# rain_delay column in the delay family.
# It uses somewhat tricky perl, so you can treat it as a black box
sub cellValue {
    my $row = $_[0]; 
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
        if ($$cell{'name'} eq $field_name) {
            return $$cell{'value'};
        }
    }
    return 'missing';
}



# Query hbase for the divvy info by month
my $records = $hbase->get({
  table => 'bc_divvy_by_month',
  where => {
    key_equals => $month_num
  },
});

print header, start_html(-title=>'Divvy Info',-head=>Link({-rel=>'stylesheet',-href=>'/bcunningham/flights/table.css',-type=>'text/css'}));

my($month_word, $avgtrip, $numtrips, $avgage, $temp, $visibility,
    $windspeed);



foreach my $row (@$records) {

    $month_word = cellValue($row, 'divvyinfo:month');
    $avgtrip = cellValue($row, 'divvyinfo:avgtrip');
    $numtrips = cellValue($row, 'divvyinfo:numtrips');
    $avgage = cellValue($row, 'divvyinfo:avgage');
    $temp = cellValue($row, 'divvyinfo:temp');
    $visibility = cellValue($row, 'divvyinfo:visibility');
    $windspeed = cellValue($row, 'divvyinfo:windspeed');
}



print div({-style=>'margin-left:auto;text-align:center;width:40%;margin-right:auto;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Divvy Info for ' . $month_word . ' 2014&nbsp;');
print     p({-style=>"bottom-margin:10px"});
print table({-class=>'CSS_Table_Example', -style=>'width:60%;margin:auto;'},
        Tr([td(['Month', 'Avg Trip Duration', 'Number of Trips', 'Avg Rider Age', 'Avg Temperature', 'Avg Visibility', 'Avg Wind Speed']),
                td([$month_word,$avgtrip,$numtrips,$avgage,$temp,$visibility,$windspeed])])),
    p({-style=>"bottom-margin:10px"});


# Change the hbase table to get divvy info by weather
my $records = $hbase->get({
  table => 'bc_divvy_by_weather',
  where => {
    key_equals => 1
  },
});

my($avgfog, $totalfog, $avgrain, $totalrain, $avgsnow, $totalsnow, $avghail, $totalhail, $avgthunder, $totalthunder,
    $avgtornado, $totaltornado, $avgclear, $totalclear);
      'divvyinfo:avgfog, divvyinfo:totalfog, divvyinfo:avgrain, divvyinfo:totalrain, divvyinfo:avgsnow, divvyinfo:totalsnow, divvyinfo:avghail, divvyinfo:totalhail, divvyinfo:avgthunder, divvyinfo:totalthunder, divvyinfo:avgtornado, divvyinfo:totaltornado, divvyinfo:avgclear, divvyinfo:totalclear');

foreach my $row (@$records) {

    $avgfog = cellValue($row, 'divvyinfo:avgfog');
    $totalfog = cellValue($row, 'divvyinfo:totalfog');
    $avgrain = cellValue($row, 'divvyinfo:avgrain');
    $totalrain = cellValue($row, 'divvyinfo:totalrain');
    $avgsnow = cellValue($row, 'divvyinfo:avgsnow');
    $totalsnow = cellValue($row, 'divvyinfo:totalsnow');
    $avghail = cellValue($row, 'divvyinfo:avghail');
    $totalhail = cellValue($row, 'divvyinfo:totalhail');
    $avgthunder = cellValue($row, 'divvyinfo:avgthunder');
    $totalthunder = cellValue($row, 'divvyinfo:totalthunder');
    $avgtornado = cellValue($row, 'divvyinfo:avgtornado');
    $totaltornado = cellValue($row, 'divvyinfo:totaltornado');
    $avgclear = cellValue($row, 'divvyinfo:avgclear');
    $totalclear = cellValue($row, 'divvyinfo:totalclear');

}

print div({-style=>'margin-left:auto;text-align:center;width:40%;margin-right:auto;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Average Divvy Trip Times and Number of Trips by Weather in 2014&nbsp;');
print     p({-style=>"bottom-margin:10px"});
print table({-class=>'CSS_Table_Example', -style=>'width:60%;margin:auto;'},
        Tr([td(['Fog', 'Trips', 'Rain', 'Trips', 'Snow', 'Trips', 'Hail', 'Trips', 'Thunder', 'Trips', 'Tornado', 'Trips', 'Clear', 'Trips']),
                td([$avgfog,$totalfog,$avgrain,$totalrain,$avgsnow,$totalsnow,$avghail,$totalhail,$avgthunder,$totalthunder,$avgtornado,$totaltornado,$avgclear,$totalclear])])),
    p({-style=>"bottom-margin:10px"});


print end_html;
