case Padrino.env
  when :development then db_name = 'weather_analyzer_development'
  when :production  then db_name = 'weather_analyzer_production'
  when :test        then db_name = 'weather_analyzer_test'
end

CouchRest::Model::Base.configure do |conf|
  conf.model_type_key = 'type' # compatibility with CouchModel 1.1
  conf.database = CouchRest.database!(db_name)
  conf.environment = Padrino.env
  # conf.connection = {
  #   :protocol => 'http',
  #   :host     => 'localhost',
  #   :port     => '5984',
  #   :prefix   => 'padrino',
  #   :suffix   => nil,
  #   :join     => '_',
  #   :username => nil,
  #   :password => nil
  # }
end
