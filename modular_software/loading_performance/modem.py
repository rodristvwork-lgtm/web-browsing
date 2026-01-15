"""module used to interact with modem"""
import requests
import logging

import config

from functools import lru_cache

def get_ipgw() -> str:
    """returns modem's ipgw"""

    return _get_association().get("ipgw_id", "NA")

def get_hw_type() -> str:
    """returns modem's ipgw"""

    return _get_terminal_info().get("board_type", "NA")

def get_sw_type() -> str:
    """returns modem's ipgw"""

    return _get_terminal_info().get("version", "NA")

def get_esn() -> str:
    """returns modem's esn"""

    return str(_get_terminal_info().get("esn", "NA"))

def get_siteid() -> str:
    """returns modem's siteid"""

    return str(_get_terminal_info().get("san", "NA"))

def get_beam() -> str:
    """returns modem's beam"""

    return str(_get_scan_progress().get("curBeamID", "NA"))

def get_outroute_id() -> str:
    """returns modem's outroute freq"""

    return str(_get_cur_tune_status().get("outroute_num", "NA"))

@lru_cache(maxsize=None)   
def _get_terminal_info() -> dict:
    """returns terminal info"""

    try:
        response = requests.get(config.settings["modem"]["terminal_info"], timeout=2)
        return response.json()
    except Exception:
        logging.error("failed to retrieve terminal info")
        return {}

@lru_cache(maxsize=None)  
def _get_association() -> dict:
    """returns association info"""

    try:
        response = requests.get(config.settings["modem"]["association"], timeout=2)
        return response.json()
    except Exception as e:
        logging.error("failed to retrieve association info")
        logging.exception(e)
        return {}

@lru_cache(maxsize=None)  
def _get_cur_tune_status() -> dict:
    """returns cur tune status info"""

    try:
        response = requests.get(config.settings["modem"]["cur_tune_status"])
        return response.json()
    except Exception as e:
        logging.error("failed to retrieve cur tune status info")
        logging.exception(e)
        return {}

@lru_cache(maxsize=None)  
def _get_scan_progress() -> dict:
    """returns cur tune status info"""

    try:
        response = requests.get(config.settings["modem"]["scan_progress"])
        return response.json()
    except Exception as e:
        logging.error("failed to retrieve scan progress info")
        logging.exception(e)
        return {}
