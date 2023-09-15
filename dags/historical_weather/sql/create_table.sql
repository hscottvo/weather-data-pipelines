create table if not exists weather_historical(
    date date not null
    , hour int not null
    , apparent_temp float not null
    , temp_2m float not null
    , precip float not null
    , dewpoint_2m float not null
    , windspeed_10m float not null
    , cloudcover int not null
    , primary key (date, hour)
)
