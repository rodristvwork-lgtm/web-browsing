from ast import Global, arg
from email.policy import default
from multiprocessing.connection import wait
from scp import SCPClient
import argparse
import os
import inspect
import pandas as pd
import numpy as np
import shutil 
import glob
from collections import defaultdict
import argparse
import time

# private
from python_connectors.Helper.Config import config
from python_connectors.Helper.Logger_P36 import SKLlogging
from python_connectors.Helper.Kafka_Handle_P36 import Kafka_Producer
from python_connectors.SKl_Connector_P37.ContainerAPI import Readers

class crontab():
    def __init__(self):
        try:
            self.LError = []
            self.logf = None
            # absolute path
            self.abspath = os.path.dirname(os.path.abspath(__file__))
            self.fname = os.path.basename(__file__)

            conf = config(self.abspath, 'settings.json')
            params = conf.ReadScriptData('update_data')

            #------------------Args----------------------------------
            parser = argparse.ArgumentParser(description='This script retrieves VSAT Stats via API method')
            parser.add_argument('-ip', '--ip',help='Specify the modem IP to write the crontab on', required=True,  type=str)
            parser.add_argument('-t', '--time',help="Specify the time of the crontab", required=True,  type=str)
            parser.add_argument('-m', '--mode',help="Specify the time of the crontab", required=True,  type=str)


            self.args = parser.parse_args()

            # ----------------- Paramiko -----------------------------
            self.prmk_user = params['terminal']['user_term']
            self.prmk_psw = params['terminal']['psw_term']
            self.timeout = 60

            # ---------------- Repository --------------------------------------------------------------------

            self.homeRepository = params['repository']

            # if no path has been defined, the local path is used
            if('/' not in self.homeRepository[-1]):
                self.homeRepository = self.abspath + self.homeRepository

            # ---------------- Logs --------------------------------------------------------------------

            self.homeLogs = params['log']

            # if no path has been defined, the local path is used
            if('/' not in self.homeLogs[-1]):
                self.homeLogs = self.abspath + self.homeLogs

            # shares the logs folder --> used by Error.py
            os.environ["logs"] = self.homeLogs 

            # Log
            # self.logf = SKLlogging('update_data', 'BSO_0000', 'I' if(verbose is False) else 'D', self.homeLogs, False)
            self.logf = SKLlogging('crontab', 'BSO_0000', 'I', self.homeLogs)
            self.path_hm_source = './Software/modular_software/loading_performance/results/'
            self.path_hm_source_launcher= 'Software/launcher.sh'
            self.path_hm_source_results = './Software/modular_software/loading_performance/results/'
            #destinazione file scaricati dalle hm
            self.path_hm_local = self.homeRepository + '/' + 'crontab' + '_in/'
            #destinazione file caricati su kafka
            self.path_hm_local_done =  self.homeRepository + '/' + 'crontab' + '_out/'
            self.path_hm_dest = '/already_on_noc/'

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
            print(err)
    
    def main(self):
        try:
            counter = 0
            for ip in self.args.ip.split(","):
                cron_time = int(self.args.time.split(",")[counter])
                counter += 1
                self.logf.info(f"Connecting to ({ip})")
                self._connectionHM(ip, cron_time)

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))

    def _connectionHM(self, ip:str, cron_time:int):
        try:
            if self.args.mode == "add":
                cron= f"crontab -l | {{ cat; echo '{cron_time} * * * * DISPLAY=:0 /home/administrator/Software/launcher.sh'; }} | crontab -"
            else:
                cron = "crontab -r"

            # connection at the terminal
            srv_conn = self._hostConnectionPrmk(ip)
            cmd_execute = srv_conn.ExecuteCmd(cron)
        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
        finally:
            if (srv_conn is not None):
                srv_conn.Close()

    def _hostConnectionPrmk(self, host: str):
        """ Host connection
        Args:
            name (str): host name
        Returns:
            [object]: session pointer
        """ 
        try:
            prmk = Readers.CallParamiko271()
            prmk.Setvalues({"host": host, "user": self.prmk_user, "psw": self.prmk_psw, 'timeout': self.timeout})
            prmk.Connect()
            return(prmk)
        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err)) 

    def _print_err(self, module: str = None, err: str = None):
        """ Load the error list to returned and writes to the log files
            Args:
                module (str, optional): the name of the module where the error occurred. Defaults to None.
                err (str, optional): the error. Defaults to None.
        """ 
        self.err = True
        if(module is None): 
            strerr = f'{err}'
        else: 
            strerr = f'{module} - {err}'            
        self.LError.append(strerr)
        # if log is defined
        if(self.logf is not None):
            self.logf.error(f'Error: {strerr}')

if __name__ == "__main__":
    """ If run from the command line 
    """  
    cls = crontab()
    if(cls.LError == []):
        cls.main()
        print("Crontab Complete")
    else:
        print(*cls.LError, sep="\n")
