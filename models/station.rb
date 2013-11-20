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
    month.property :precipitation, Float
    month.property :rainy_days, Integer
  end

  property :daily, array: true, default: (365.times.map {{}}) do |month|
    month.property :overcast_percentages, Array
  end

  design do
    view :overcast_m_h,
      map:
        'function(doc) {
          if (doc.daily) {
            doc.daily.forEach(function(day, dayNum) {
              var month = (new Date(2001, 0, dayNum)).getMonth() + 1;
              if (day.overcast_percentages) {
                day.overcast_percentages.forEach(function(pct, hour) {
                  var period = hour < 8 || hour >= 19 ? "night" : "day";
                  emit([doc._id, period, month, hour], [pct, 1]);
                });
              }
            });
          }
        }',
      reduce:
        'function(keys, values, rereduce) {
          var sum = 0.0;
          var hours = 0.0;
          values.forEach(function(arr) {
            sum += arr[0];
            hours += arr[1];
          });
          return [sum, hours];
        }'
  end
end
