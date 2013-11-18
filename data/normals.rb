require 'csv'
# 30-year averages supplied by NOAA.
#
# This imports the raw data (located in ./normals) into Couch.
#
# Files in which we are interested:
#
#  * station-inventories/allstations.txt: Links a station id to name & location.
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
def parse_columns row, defn
  result = {}
  defn.each do |key, bounds|
    result[key] = row[(bounds.begin-1)..(bounds.end-1)].strip
  end
  result
end
IO.foreach("data/normals/station-inventories/allstations.txt") do |row|
  defn = {
    id: 1..11,
    latitude: 13..20,
    longitude: 22..30,
    elevation: 32..37,
    state: 39..40,
    name: 42..71,
  }
  station = Station.new parse_columns(row, defn)
  station.save!
end
