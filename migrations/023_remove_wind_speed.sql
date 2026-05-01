-- remove historical wind_speed forecast and weight rows
delete from forecasts where variable = 'wind_speed';
delete from weights where variable = 'wind_speed';
