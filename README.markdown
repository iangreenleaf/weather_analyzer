# Weather Analyzer #

(For lack of a better name)

## Data import ##

I've been using mostly the [NOAA climate normals](http://www.ncdc.noaa.gov/cdo-web/datasets).

Put em in `data/normals`, and run:

    padrino runner data/normals.rb data/development

Or once you've FTP'd an entire text dump from NOAA, pass the prod dir instead:

    padrino runner data/normals.rb data/noaa_data

## TODO ##

Some data sources to maybe consider:

* http://www.ncdc.noaa.gov/oa/climate/research/ushcn/
* http://www.ncdc.noaa.gov/data-access/quick-links#storm-d
