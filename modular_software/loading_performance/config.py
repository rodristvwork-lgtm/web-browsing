import logging
import os
import json

from datetime import datetime

# how see ig logging of python is installed
#variable used by various modules to read app settings
settings: dict

def _init_logging() -> None:
    """initialize logging system"""

    logging.basicConfig(filename=settings["log_file_path"], filemode="w", format='%(asctime)s - %(levelname)s:%(message)s', level=logging.DEBUG)
    logging.info("logging system initialized")

def _read_settings() -> None:
    """read and stores settings to global variable"""

    global settings
    with open(f"{os.path.dirname(__file__)}/settings.json") as json_file:
        settings = json.load(json_file) 

def _init() -> None:
    """initialize config"""

    _read_settings()
    _init_logging()
    #sets result file name for current execution
    settings["results_file_name"] = settings["results_file_name"].format(
        hostname = get_nodename(),
        ts=datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    )

def get_nodename() -> str:
    """returns host name"""

    name: str
    try:
        name = "TP1_A42"
    except Exception:
        name = settings["host"]["default_node_name"]
        logging.error(f"failed to get host name, default: {name}")
    return name

#set config when imported first time
# config.py

def _init():
    _read_settings()
    _init_logging()
    settings["results_file_name"] = settings["results_file_name"].format(
        hostname=get_nodename(),
        ts=datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    )

# run automatically when imported
_init()

# optional: run again when executed directly
if __name__ == "__main__":
    print("Config initialized")
