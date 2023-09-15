create table if not exists hourly_weather(
  reference_date date not null
  , forecast_date date not null
  , hour int not null
  , time_horizon int not null
  , apparent_temp float not null
  , temp_2m float not null
  , precip_prob int not null
  , dewpoint_2m float not null
  , windspeed_10m float not null
  , cloudcover int not null
  , primary key (reference_date , forecast_date , hour))
