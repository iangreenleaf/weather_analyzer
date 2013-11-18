class Station < CouchRest::Model::Base
  # We use the NOAA-assigned id for this!
  unique_id :id
  property :name, String
  property :latitude, Float
  property :longitude, Float
  property :state, String
  property :elevation, Float

  property :monthly, array: true, default: (12.times.map {{}}) do |month|
    month.property :snowfall, Float
  end
end
