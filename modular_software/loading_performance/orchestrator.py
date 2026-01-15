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
from tabulate import tabulate
import sys

from get_baseline import get_baseline
from update_data import update_data

# private
from python_connectors.Helper.Config import config
from python_connectors.Helper.Logger_P36 import SKLlogging
from python_connectors.Helper.Kafka_Handle_P36 import Kafka_Producer
from python_connectors.SKl_Connector_P37.ContainerAPI import Readers



class orchestrator():

    def __init__(self):
        """ Environment initialization

        Args:
            env (str): environment: lab / prod
        """
        try:
            self.LError = []
            self.logf = None
            # absolute path
            self.abspath = os.path.dirname(os.path.abspath(__file__))
            self.fname = os.path.basename(__file__)

            conf = config(self.abspath, 'settings.json')


            params = conf.ReadScriptData('update_data')
            
            self.broker = params['kafka']['broker']
            self.topic = params['kafka']['topic']
            
            #------------------Args----------------------------------
            parser = argparse.ArgumentParser(description='This script retrieves VSAT Stats via API method')
            parser.add_argument('-ip', '--ip',help='Specify the modem IP to run the test', required=True,  type=str)
            parser.add_argument('-b', '--beam',help="Specify the modem's beam", required=True,  type=int)
            parser.add_argument('-d', '--device',help="Specify if the modem is an ota or an hm", required=True,  type=str, default= "ota")
            parser.add_argument('-E', '--env',help="Specify if the modem is in prod or lab", required=True,  type=str, default= "prod")
            parser.add_argument('-c', '--compare',help="Specify if you want compare results with a baseline", required=True,  type=str, default="False")
            parser.add_argument('-m', '--mode',help="Specify if the date range for the baseline will be decided automatically(last two weeks) or insert the range manually", required=True,  type=str, default="A")
            parser.add_argument('-i', '--initial_date',help="If mode = M initial date format %Y-%m-%d", required=False,  type=str)
            parser.add_argument('-f', '--final_date',help="If mode = M Final date format %Y-%m-%d", required=False,  type=str)
            parser.add_argument('-l', '--label',help="Label identificativo della baseline", required=False,  type=str)
            parser.add_argument('-bc', '--beam_compare',help="Specify the baseline's beam you want compare with results test", required=True,  type=int)



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
            self.logf = SKLlogging('orchestrator', 'BSO_0000', 'I', self.homeLogs)
            self.path_hm_source = './Software/modular_software/loading_performance/results/'
            self.path_hm_source_launcher= 'Software/launcher.sh'
            self.path_hm_source_results = './Software/modular_software/loading_performance/results/'
            #destinazione file scaricati dalle hm
            self.path_hm_local = self.homeRepository + '/' + 'orchestrator' + '_in/'
            #destinazione file caricati su kafka
            self.path_hm_local_done =  self.homeRepository + '/' + 'orchestrator' + '_out/'
            self.path_hm_dest = '/already_on_noc/'

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
            print(err)


    def main(self):
        #ip='172.30.10.66'
        ip = self.args.ip
        try:
            self.logf.info(f"Connecting to ({ip})")
            return_con = self._connectionHM(ip)
            if return_con != -1 and return_con != -2:
                self.logf.info(f"Processing the result file")
                ##Calling the comparison function

                if self.args.compare == "True":
                    if self.args.mode == "A":
                        self.args.label = "Automatic"
                        self.args.initial_date = "not important"
                        self.args.final_date = "not important"
                        
                    cls1 = get_baseline(self.args.device, self.args.beam, self.args.env, self.args.mode, self.args.initial_date, self.args.final_date, self.args.label, self.args.beam_compare)
                    if(cls1.LError == []):
                        cls1.main()
                    else:
                        print(*cls1.LError, sep="\n")
                else:
                    path = os.path.dirname(os.path.abspath(__file__))
                    path = path.split("modular_software")[0] + "repository/orchestrator_in/*csv"
                    list_files = glob.glob(path) 
                    if len(list_files) > 1:
                        beam_files_list = []
                        for el in list_files:
                            if "B" + str(self.args.beam) in el:
                                beam_files_list.append(el)
                        file = max(beam_files_list, key=os.path.getctime)
                    else:
                        file = list_files[0] 
                    #last_file = max(list_files, key=os.path.getctime)  #find last file created
                    df_test = pd.read_csv(file)
                    
                    pd.set_option('max_columns', None)
                    df_test.drop(["trace_dict", "host_name", "ipgw", "esn", "siteid", "beam", "outroute_freq", "first_contentful_paint", "total_blocking_time", "Hardware_type", "Software_version", "Environment", "timestamp"], axis=1, inplace=True)
                    pdtabulate=lambda result:tabulate(result,headers='keys',tablefmt='psql')
                    print(pdtabulate(df_test))

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))

        finally:
            if return_con != -1 and return_con != -2:
                list_files = glob.glob(f"{self.path_hm_local}*.csv")

                if len(list_files) > 1:
                    beam_files_list = []
                    for el in list_files:
                        if "B" + str(self.args.beam) in el:
                            beam_files_list.append(el)
                    file = max(beam_files_list, key=os.path.getctime)
                else:
                    file = list_files[0] 

                #last_file = max(list_files, key=os.path.getctime)  #find last file created
                file_name = file.split("/")[-1]
                ###moving file to db
                data = pd.read_csv(file, sep=",", dtype=str)
                data = data.replace({np.nan: None})
                ret = data.to_dict(orient='records')
                try:
                    Kafka = Kafka_Producer(self.topic, self.broker)
                    
                    self.logf.info(f"Writing result on DB") 

                    for data in ret:
                        kafka_key_id = 3
                        kafka_field = [item for item in data.keys()]
                        kafka_values = [item for item in data.values()]
                        kafka_types = [type(item) for item in data.values()]
                        kafka_values = [str(item) for item in data.values()]
                        kafka_values[15] = kafka_values[15].replace("'", "")
                        message = f"{kafka_key_id},{kafka_field},{kafka_values},{kafka_types}"          
                        ret = Kafka.Producer(message)
                        if ret[0] == 0:
                            self.logf.critical("File to send line ")
                            self.logf.critical(data)
                            self.logf.critical(ret)

                except Exception as err:
                    self._print_err(inspect.stack()[0][3], str(err))
                finally:
                    shutil.move(file, self.path_hm_local_done + file_name)

    def _connectionHM(self, terminalIp: str):
        """ Connect to hm and download file in local, on remote move file to already_on_noc

            Args:
                ip (str): Terminal IP address
        """
        cmd_execute1 = f'ps aux | egrep python'
        cmd_execute = f'DISPLAY=:0 sh {self.path_hm_source_launcher}'
        lscmd = f'ls {self.path_hm_source} | egrep *.csv'

        ping_cmd = "ping -c 2 8.8.8.8"

        # connection at the terminal
        srv_conn = self._hostConnectionPrmk(terminalIp)
        if srv_conn == -1:
            self.logf.error("Aborting")
            return -2
        try:
            cmd_response = srv_conn.ExecuteCmd(ping_cmd)
            try:
                if cmd_response.split("\n")[-1] == "":
                    loss = int(cmd_response.split('\n')[-3].split("packet loss")[0].split(",")[-1].split("%")[0])
                else:
                    loss = int(cmd_response.split('\n')[-2].split("packet loss")[0].split(",")[-1].split("%")[0])
            except:
                self.logf.error("Error with the ping command")
            
            if loss == 0:
                cmd_response = srv_conn.ExecuteCmd(cmd_execute1)
                if "browsertime.py" in cmd_response:
                    proc = 1
                elif "loading.py" in cmd_response:
                    proc = 2
                else:
                    proc = 3
                if proc == 2:
                    self.logf.info("Browsing tests already running, waiting for the result")
                    wait_lp = 1
                    while wait_lp == 1:
                        cmd_response = srv_conn.ExecuteCmd(cmd_execute1)
                        if "loading.py" not in cmd_response:
                            wait_lp = 0
                        time.sleep(60)
                else:
                    self.logf.info("Launching browsing tests, can take up to 20 minutes")
                    cmd_execute = srv_conn.ExecuteCmd(cmd_execute)
                
                listfile = srv_conn.ExecuteCmd(lscmd)
                if listfile == "":
                    self.logf.error("No result file found")
                    return -1
                listfile = listfile.split('\n')
                last_file = listfile[-2] 
                self.logf.info("Finished with the browsing test, copying files locally")
                # Copy files locally
                scp = SCPClient(srv_conn.GetTransport())
                scp.get(f"{self.path_hm_source}{last_file}", f"{self.path_hm_local}")
                srv_conn.ExecuteCmd(f"mv {self.path_hm_source}{last_file} {self.path_hm_source}{self.path_hm_dest}{last_file}")

                self.logf.info("Copying result file from modem to the folder orchestrator_in")  
                scp.close()
                return 0
            else:
                self.logf.error(f"VM internet not working correctly, ping pakcet loss: {loss}%")
                return -1
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
            if host == "172.30.34.72" or host == "172.30.34.71":
                self.prmk_user = "Administrator"
            prmk.Setvalues({"host": host, "user": self.prmk_user, "psw": self.prmk_psw, 'timeout': self.timeout})
            prmk.Connect()
        
            return(prmk)

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
            return -1 

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
    #parser = argparse.ArgumentParser(description='This script collect all data from VM and send to dB')
    #parser.add_argument("-w", "--write", help="Write all file on dB", type=bool,default=False)
    #parser.add_argument('-Type','-t','-T', dest='tipo', help='Specify the type of test lp/site(loading_performance/sitespeed)', required=True,  type=str)
    #global args 
    #args = parser.parse_args() 
    cls = orchestrator()
    if(cls.LError == []):
        cls.main()
        print("INFO orchestrator Complete")
    else:
        print(*cls.LError, sep="\n")
