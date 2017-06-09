import configparser
import os

def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

config = load_config(config_file = os.path.join(os.path.dirname(__file__), 'config.ini'))

FP_ENV = config["general"]["ENVIRONMENT"]
LOG_FOLDER = config["general." + FP_ENV]["LOG_FOLDER"]
DOWNLOAD_FOLDER = config["general." + FP_ENV]["DOWNLOAD_FOLDER"]

