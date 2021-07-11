import configparser
import logging
import os

def setup(cfg_name, key_params_dict, skip_log=False):
	if not skip_log:
		_initialize_log()
	return _parse_config_params(cfg_name, key_params_dict)

def _parse_config_params(cfg_name, key_params_dict):
	""" Parse env variables to find program config params

	Function that search and parse program configuration parameters in the
	program environment variables. If at least one of the config parameters
	is not found a KeyError exception is thrown. If a parameter could not
	be parsed, a ValueError is thrown. If parsing succeeded, the function
	returns a map with the env variables
	"""

	config = configparser.ConfigParser()		
	config.read(cfg_name)
	ini_config = config['DEV']

	config_params = {}

	for k, v in key_params_dict.items():
		try:
			config_params[k] = _get_config_key(k, ini_config)

			is_int = v

			if is_int:
				config_params[k] = int(config_params[k])
		except ValueError as e:
			raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

	return config_params

def _get_config_key(name, config_fallback):
	value = os.getenv(name)

	if not value:
		try:
			value = config_fallback[name]
		except KeyError as e:
			raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
	
	return value

def _initialize_log(p_id=None):
	"""
	Python custom logging initialization

	Current timestamp is added to be able to identify in docker
	compose logs the date when the log has arrived
	"""
	if p_id is not None:
		logging.basicConfig(
			format=f'%(asctime)s %(levelname)-8s #{p_id} %(message)s',
			level=logging.INFO,
			datefmt='%Y-%m-%d %H:%M:%S',
		)
	else:
		logging.basicConfig(
			format='%(asctime)s %(levelname)-8s %(message)s',
			level=logging.INFO,
			datefmt='%Y-%m-%d %H:%M:%S',
		)