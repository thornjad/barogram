-- remove all precip_prob forecast history and the airmass_precip model
delete from forecasts where variable = 'precip_prob';
delete from weights where variable = 'precip_prob';
delete from members where model_id = 11;
delete from models where id = 11;
