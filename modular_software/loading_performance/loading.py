"""Version 1.4"""
import os
import json
import pathlib
import argparse
import time
import csv
import logging
import config
import modem
from subprocess import PIPE, Popen
from platform import system

from typing import Dict, List
from selenium.webdriver import Firefox
from datetime import datetime
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options


def _test_single_metric(driver, metrics):
    to_ret = ""
    try:
        to_ret = driver.execute_script(config.settings[metrics])
    except Exception as e:
        logging.exception(e)
        to_ret = config.settings["failed_test_value"]
    return to_ret

def _traceroute_website(to_ret, url):
    to_ret["total_time"] = -1
    to_ret["hop_number"] = -1
    to_ret["trace_dict"] = {}
    total_time, ast_count = 0, 1
    if system() != 'Windows':
        resp = make_cmd("sudo -S traceroute -n -q 1 " + url.split("://")[1]+" -m 50 -I")
        if resp['cmd_err'] == "":
            lines = resp['cmd_out'].split("\n")
            for lin in lines[1:-1]:
                ip = lin.split("  ")[1]
                if ip != "*":
                    time_hop = float(lin.split("  ")[2].split(" ")[0])
                    #total_time += time_hop
                    to_ret["trace_dict"][ip] = time_hop
                else:
                    to_ret["trace_dict"]['*' + str(ast_count)] = '*'
                    ast_count += 1
            if lines[-1] == "":
                num_hops = lines[-2].split(" ")[0]
                delay = lines[-2].split(" ")[-2]
            else:
                num_hops =lines[-1].split(" ")[0]
                delay = lines[-1].split(" ")[-2] 
            to_ret["total_time"] = delay
            to_ret["hop_number"] = num_hops
    else:
        resp = make_cmd("tracert -d -4 " + url.split("://")[1])
        if resp['cmd_err'] == "":
            lines = resp['cmd_out'].split("\n")
            for lin in lines[4:-3]:
                line_split = lin.split("  ")
                ip = line_split[len(line_split) - 1][:-2]
                if ip != " Request timed out":
                    i = 0
                    for l in line_split:
                        if l.find("ms") != -1:
                            break
                        i += 1
                    time_hop = line_split[i].split("ms")[0]
                    if time_hop.find("<") != -1:
                        time_hop = time_hop[1:]
                    time_hop = float(time_hop)
                    total_time += time_hop
                    to_ret["trace_dict"][ip] = time_hop
                else:
                    to_ret["trace_dict"]['*' + str(ast_count)] = '*'
                    ast_count += 1
            to_ret["total_time"] = lines[-4].split("ms")[0].split(" ")[-2]
            to_ret["hop_number"] = len(lines[4:-3])
    return to_ret

def _get_modem_info(to_ret):
    ipgw = modem.get_ipgw()
    if ipgw != 'NA':
        to_ret["ipgw"] = ipgw
        to_ret["esn"] = modem.get_esn()
        to_ret["siteid"] = modem.get_siteid()
        to_ret["beam"] = modem.get_beam()
        to_ret["outroute_freq"] = modem.get_outroute_id()
    return to_ret

def _load_single_website(url: str) -> dict:
    """returns loading time of single website"""
    to_ret = {"website": url, "host_name": config.get_nodename()}
    driver = None # <-- critical fix    
    try:
        to_ret["ipgw"], to_ret["esn"], to_ret["siteid"], to_ret["beam"], to_ret["outroute_freq"] = 'NA', 'NA', 'NA', 'NA', 'NA'
        to_ret = _get_modem_info(to_ret)
        logging.info(f"loading {url}")
        
        #### Added for Firefox new setup
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        service = Service(
            executable_path=f"{os.getcwd()}/{config.settings['geckodriver_path']}"
        )

        driver = Firefox(
            service=service,
            options=options
        )

        driver.set_page_load_timeout(config.settings["website_loading_timeout"])
        driver.implicitly_wait(config.settings["website_loading_timeout"])
     
        ####     end added for Firefox new setup    
        time.sleep(config.settings["sleep_before_load"])
        driver.get(url)
        to_ret["first_contentful_paint"] = _test_single_metric(driver, "first_contentful_paint")
        to_ret["total_blocking_time"] = _test_single_metric(driver, "total_blocking_time")
        navigation_start = _test_single_metric(driver, "start_time_cmd")
        load_complete = _test_single_metric(driver, "end_time_cmd")
        dom_complete = _test_single_metric(driver, "dom_complete_cmd")
        domInteractive = _test_single_metric(driver, "domInteractive_cmd")
        response_start = _test_single_metric(driver, "response_start_cmd")
        # prova1 = driver.execute_script(config.settings["prova"])    
        to_ret["load_time"] = load_complete - navigation_start
        to_ret["first_byte_time"] = response_start - navigation_start
        to_ret["dom_content_loaded"] = dom_complete - navigation_start
        to_ret["dom_Interactive_time"] = domInteractive - navigation_start
        logging.info("start traceroute")
        to_ret = _traceroute_website(to_ret, url)
        to_ret["Hardware_type"] = modem.get_hw_type()
        to_ret["Software_version"] = modem.get_sw_type()
        to_ret["Environment"] = "prod"
        logging.info("test done")
        
    except Exception as e:
        logging.error(f"an exception occured while testing {url}")
        logging.exception(e)

    finally:
        
        if driver is not None:
            try:
                driver.quit()   # safer than close()
            except Exception:
                pass

        to_ret["timestamp"] = datetime.strptime(
            datetime.now().strftime('%Y-%m-%d %H:%M'),
            '%Y-%m-%d %H:%M'
        )
    return to_ret

def make_cmd(cmd, sys=False):
    '''Execute os cmd'''
    response = {}
    try:
        logging.info(f"Eseguo comando: {cmd}")
        if sys is False:
            cmd_exec = Popen(args=cmd, stdout=PIPE, stderr=PIPE, shell=True)
            out_err = cmd_exec.communicate()
            cmd_out = str(out_err[0])[2:-1].replace("\\t", "\t").replace("\\n", "\n").replace("\\r", "\r")
            cmd_err = str(out_err[1])[2:-1].replace("\\t", "\t").replace("\\n", "\n").replace("\\r", "\r")
            logging.info(f"Return Code: {cmd_exec.returncode}")
            # logging.info(f"Output: {cmd_out}")
            # logging.info(f"Error: {cmd_err}")
            if cmd_err == "" and cmd_exec.returncode != 0:
                cmd_err = cmd_out
            if cmd_exec.returncode == 0:
                cmd_err = ""
            response = {'return_code': cmd_exec.returncode, 'cmd_out': cmd_out, 'cmd_err': cmd_err}
        else:
            os.system(cmd)
    except Exception as e:
        logging.exception(e)
        response = {'return_code': -1, 'cmd_out': '', 'cmd_err': str(e)}
    return response

def _write_results(results: List[dict]) -> None:
    """writes results to file"""

    #if file doesnt exist, then write_headers = true
    write_headers: bool = not os.path.isfile(config.settings["results_file_name"])
    with open(config.settings["results_file_name"], mode='a') as csv_file:
        result: dict
        writer = csv.DictWriter(csv_file, fieldnames=results[0].keys())
        if write_headers:
            writer.writeheader()
        #write rows
        for result in results:
            writer.writerow(result)
        logging.info(f"{len(results)} tests written to results file")

def _read_input_parameters() -> List[str]:
    """returns list representing input params
    [google.it, facebook.com]"""

    #read input file
    websites: List[str]
    try:
        with open(config.settings['input_file_path']) as json_file:
            websites = json.load(json_file).get("websites")
    except Exception:
        logging.error("failed to read input file")
        raise Exception("failed to read input file")
    logging.info(f"found {len(websites)} websites from input file")
    return websites

def test_websites(sites: List[str]) -> None:
    """test list of websites from input and stores results"""

    results: List[dict] = []
    site: str
    for site in sites:
        results.append(_load_single_website(site))
    _write_results(results)

if __name__ == "__main__":
    sites: List[str] = _read_input_parameters()
    test_websites(sites)

