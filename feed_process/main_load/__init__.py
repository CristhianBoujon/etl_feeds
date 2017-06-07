import os
from feed_process import load_config, FP_ENV

file_name = os.path.join(os.path.dirname(__file__), "config.ini")
config = load_config(file_name)

def get_loader_data_connection(loader_name):
    return config[loader_name + "." + FP_ENV]




from feed_process.main_load.feeds_load import run