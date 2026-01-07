# Coding exercise

## Run locally
Run:
```bash
pip-compile-multi
docker build . -t da-spark-image
```
This will build image with spark that we will use to run the transformations on.  
Next step is to run docker compose:
```bash
docker compose up
```
If you want, you can add `-d` flag to run containers as a daemon, but I suggest to run it this way to see the logs. If you do so, run the next command in separate terminal.  
To see anything you do need to provide the solution with data. You can find the link below. Please save `knmi.parquet` in `data` folder  
To actually run the solution:
```bash
docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/main.py
```
To run tests:
```bash
docker exec da-spark-master python3 -m pytest --log-cli-level info -p no:warnings -v /opt/spark/apps/tests
```

## Exercise
For this exercise you will calculate heatwaves in the Netherlands for the past 20 years. The
Royal Netherlands Meteorological Institute defines a heat wave as a period of at least 5
consecutive days in which the maximum temperature in De Bilt reaches or surpasses 25°C.
Additionally, during this 5 day period, the maximum temperature in De Bilt should reach or
surpass 30°C for at least 3 days.
Your goal is to calculate heatwaves for The Netherlands starting from 2003. The output
should contain the following columns:
1. From date
2. To date (inc.)
3. Duration (in days)
4. Number of tropical days
5. Max temperature


If you have some time to spare, or want to show off your skills you could extend the
assignment calculating coldwaves. A coldwave is a period of excessively cold weather with a
minimum of five consecutive days below freezing (max temperature below 0.0 °C) and at
least three days with high frost (min temperature is lower than -10.0 °C).
Note that some columns in the data might be empty. Empty values should be excluded from
calculations.
The data is described [here](https://dataplatform.knmi.nl/dataset/vochtigheid-en-temperatuur-1-0), a relevant subset can be downloaded [here](https://drive.google.com/file/d/1SeSC7VR0zvLAGIyqFQzDTfYj0y7mnhaW/view?usp=drive_link). The data contains
the following columns:

|Field | Description                                                         |
|---|---------------------------------------------------------------------|
|DTG | date of measurement                                                 |
|LOCATION | location of the meteorological station                              |
|NAME | name of the meteorological station                                  |
|LATITUDE | in degrees (WGS84)                                                  |
|LONGITUDE | in degrees (WGS84)                                                  |
|ALTITUDE | in 0.1 m relative to Mean Sea Level (MSL)                           |
|U_BOOL_10 | air humidity code boolean 10' unit                                  |
|T_DRYB_10 | air temperature 10' unit Celsius degrees Celsius degrees            |
|TN_10CM_PAST_6H_10 | air temperature minimum 0.1m 10' unit                               |
|T_DEWP_10 | air temperature derived dewpoint - 10' unit Celsius degrees         |
|T_DEWP_SEA_10 | air temperature derived dewpoint- sea 10' unit Celsius degrees      |
|T_DRYB_SEA_10 | air temperature height oil platform 10 minutes unit Celsius degrees |
|TN_DRYB_10 | air temperature minimum 10' unit Celsius degrees                    |
|T_WETB_10 | air temperature derived wet bulb- 10' unit Celsius degrees          |
|TX_DRYB_10 | air temperature maximum 10' unit Celsius degrees                    |
|U_10 | relative air humidity 10' unit %                                    |
|U_SEA_10 | is relative sea air humidity 10' unit %                             |


## Questions to the business before starting the exercise?
 - Do you want to have a possibility to adjust the determining values?
 - Is it part of larger dataset? Will this be somehow expanded?
 - Null values, where are they acceptable? Right now, nulls on temperature (`T_DRYB_10`) are discarded, as well as nulls in `Name` column will exclude that record from extreme temperature check (but not from qualifying)
 - Should we aggregate the data based on location? If the dataset will start to include more places, the results might stop making sense. It would be useful to set some boundries.

## Possible next moves
Ideas:
 - Parametrize Localization for extreme weather
 - Extract config to an external file (json), or database, so it's easier for business to do a change if needed.
 - Comparison parameter could be an enum/string instead of operator function, get translated to function.
 - Extreme conditions could be optional if needed
 - Incremental load with merge
 - Set medallion architecture