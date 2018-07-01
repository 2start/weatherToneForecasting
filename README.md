# GDELT events & weather stream analysis using Apache Flink


__Components:__

- AvgNumMentions: analyze average number of event mentions depending on precipitation levels
- AvgTone: analyze average event report tone for each day depending on temperature levels

If only riots are interesting, you can uncomment the filter `event.eventRootCode.equals("14")` in both classes.


__Data Sets:__

- [GDELT 1.0 Event Database](https://www.gdeltproject.org/data.html#documentation)
- [NCDC NOAA Weather Data](ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt): [`ghcnd-stations.txt`](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt) and `2017.csv` from `by_year` subdirectory ([link](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/)).

No preprocessing of data sets, so they could be coming in as a real stream.
