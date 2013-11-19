# 30-year averages supplied by NOAA.
#
# This script imports the raw data (located in ./normals) into Couch.

require 'csv'

# Handles the messy business of parsing values out of NOAA's provided
# plain-text data files.
class ::NoaaDataFile < CouchRest::Model::Base
  property :md5, String
  property :defn, HashWithIndifferentAccess
  timestamps!
  attr_accessor :stale

  # The usual find or create logic.
  #
  # Also does some stuff with checking if the saved record reflects
  # the latest physical input file, which maybe doesn't belong here.
  def self.find_or_create filename, defn
    m = self.find filename

    if m.nil?
      m = self.new filename: filename
    end

    m.defn = defn.as_json.with_indifferent_access

    m
  end

  def filename
    self['_id']
  end

  def filename= val
    self['_id'] = val
  end

  def each_row
    self.md5 = Digest::MD5.file(filename).to_s
    unless md5_changed? or defn_changed?
      logger.info "#{filename} has already been parsed, skipping."
      return
    end
    logger.info "Importing data from #{filename}"
    total_lines = %x{wc -l #{filename}}.split.first.to_i
    progress = ProgressBar.create total: total_lines, format: "%a|%B%c/%C|%e", smoothing: 0.4

    IO.foreach(filename) do |row|
      result = HashWithIndifferentAccess.new
      defn.each do |key, schema|
        if schema[:repeat]
          result[key] = schema[:repeat].times.map do |i|
            start = schema[:start] - 1 + i * schema[:length]
            normalize row[start, schema[:length]], schema
          end
        else
          result[key] = normalize row[(schema[:cols].min-1)..(schema[:cols].max-1)], schema
        end
      end
      yield result
      progress.increment
    end

    save!
  end

  protected
  # May return either a Numeric, or a String. Don't count on one in particular.
  def normalize val, schema
    val = parse_num val if schema[:parse]
    val = val.try :strip
    val = val.to_f / schema[:divisor] if schema[:divisor].present?
    val
  end

  def parse_num val
    # Handle NOAA completeness flags
    unless (val =~ /\A\s*(-?\d+)[CSRPQ]\s*\Z/).nil?
      val = $1
    end
    # Handle special values for missing/bad data
    if %w[-9999 -8888 -7777 -6666 -5555].include? val
      val = nil
    end

    val
  end
end

#  station-inventories/allstations.txt: Links a station id to name & location.
#
#      The variables in each record include the following:
#      ------------------------------
#      Variable   Columns   Type
#      ------------------------------
#      ID            1-11   Character
#      LATITUDE     13-20   Real
#      LONGITUDE    22-30   Real
#      ELEVATION    32-37   Real
#      STATE        39-40   Character
#      NAME         42-71   Character
#      GSNFLAG      73-75   Character
#      HCNFLAG      77-79   Character
#      WMOID        81-85   Character
#      METHOD*      87-99   Character
#      ------------------------------

#      These variables have the following definitions:

#      ID         is the station identification code.  Note that the first two
#                 characters denote the FIPS country code, the third character
#                 is a network code that identifies the station numbering system
#                 used, and the remaining eight characters contain the actual
#                 station ID.
#      LATITUDE   is latitude of the station (in decimal degrees).
#      LONGITUDE  is the longitude of the station (in decimal degrees).
#      ELEVATION  is the elevation of the station (in meters, missing = -999.9).
#      STATE      is the U.S. postal code for the state (for U.S. stations only).
#      NAME       is the name of the station.
#      GSNFLAG    is a flag that indicates whether the station is part of the GCOS
#                 Surface Network (GSN). The flag is assigned by cross-referencing
#                 the number in the WMOID field with the official list of GSN
#                 stations. There are two possible values:

#                 Blank = non-GSN station or WMO Station number not available
#                 GSN   = GSN station

#      HCNFLAG    is a flag that indicates whether the station is part of the U.S.
#                 Historical Climatology Network (HCN).  There are two possible
#                 values:

#                 Blank = non-HCN station
#                 HCN   = HCN station

#      WMOID      is the World Meteorological Organization (WMO) number for the
#                 station. If the station has no WMO number, then the field is blank.
#      METHOD*    is an indication of whether a "traditional" or a "pseudonormals"
#                 approach was utilized for temperature or precipitation. This field
#                 in only found in prcp-inventory.txt and temp-inventory.txt
stations_input = NoaaDataFile.find_or_create(
  "data/normals/station-inventories/allstations.txt",
  {
    id: { cols: 1..11 },
    latitude: { cols: 13..20 },
    longitude: { cols: 22..30 },
    elevation: { cols: 32..37 },
    state: { cols: 39..40 },
    name: { cols: 42..71 },
  }
)
stations_input.each_row do |row|
  Station.find(row[:id]).try :destroy
  station = Station.new row
  station.save!
end

# products/precipitation/mly-snow-normal.txt: Monthly snowfall normals.
snowfall_input = NoaaDataFile.find_or_create(
  "data/normals/products/precipitation/mly-snow-normal.txt",
  {
    id: { cols: 1..11 },
    months: { repeat: 12, length: 7, start: 19, parse: true, divisor: 10 },
  }
)
snowfall_input.each_row do |row|
  station = Station.find row[:id]
  station.monthly.each do |month|
    month.snowfall = row[:months].shift
  end
  station.save!
end

# products/precipitation/mly-prcp-normal.txt: Monthly precipitation normals.
precip_input = NoaaDataFile.find_or_create(
  "data/normals/products/precipitation/mly-prcp-normal.txt",
  {
    id: { cols: 1..11 },
    months: { repeat: 12, length: 7, start: 19, parse: true, divisor: 100 },
  }
)
precip_input.each_row do |row|
  station = Station.find row[:id]
  station.monthly.each do |month|
    month.precipitation = row[:months].shift
  end
  station.save!
end

# products/precipitation/mly-prcp-avgnds-ge010hi.txt: Days per month with
# precipitation of 0.1" or greater. Using this for number of "rainy days".
rainy_days_input = NoaaDataFile.find_or_create(
  "data/normals/products/precipitation/mly-prcp-avgnds-ge010hi.txt",
  {
    id: { cols: 1..11 },
    months: { repeat: 12, length: 7, start: 19, parse: true, divisor: 10 },
  }
)
rainy_days_input.each_row do |row|
  station = Station.find row[:id]
  station.monthly.each do |month|
    month.rainy_days = row[:months].shift
  end
  station.save!
end

# products/hourly/hly-clod-pctovc.txt: Daily percentages of overcast cloud cover.
# This isn't really in the format we want. Maybe we'll use a Couch view.
cloud_input = NoaaDataFile.find_or_create(
  "data/normals/products/hourly/hly-clod-pctovc.txt",
  {
    id: { cols: 1..11 },
    month: { cols: 13..14 },
    day: { cols: 16..17 },
    hours: { repeat: 24, length: 7, start: 19, parse: true, divisor: 10 },
  }
)
cloud_unsaved = {}
cloud_input.each_row do |row|
  station = (cloud_unsaved[row[:id]] ||= Station.find row[:id])
  # Year doesn't matter, just need non-leap year
  day_num = Date.new(2001, row[:month].to_i, row[:day].to_i).yday
  station.daily[day_num-1].overcast_percentages = row[:hours]
end
unless cloud_unsaved.blank?
  progress = ProgressBar.create total: cloud_unsaved.count, format: "%a|%B%c/%C|%e", smoothing: 0.4
  cloud_unsaved.each do |_, station|
    station.save!
    progress.increment
  end
end
