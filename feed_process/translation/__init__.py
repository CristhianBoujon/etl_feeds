import configparser
import os
from feed_process import FP_ENV



config_file = os.path.join(os.path.dirname(__file__), 'config.ini')

config = configparser.ConfigParser()
config.read(config_file)

DATABASE_CONFIG = config["database." + FP_ENV]

from feed_process.translation.translation import Translator