from ast import Global, arg
from logging import raiseExceptions
from time import sleep
from scp import SCPClient
import argparse
import os
import inspect
import pandas as pd
import numpy as np
import shutil
import sys
import imp
# private
from python_connectors.Helper.Config import config
from python_connectors.Helper.Logger_P36 import SKLlogging
from python_connectors.Helper.Kafka_Handle_P36 import Kafka_Producer
from python_connectors.SKl_Connector_P37.ContainerAPI import Readers

db_lib = imp.load_source('DB_Handle_P35','/opt/scripts/lib/python-library/DataBase/DB_Handle_P35.py')

class update_data():

    def __init__(self):
        """ Environment initialization

        Args:
            env (str): environment: lab / prod
        """

        try:
            # initialize the data buffers

            self.LData = []
            self.LError = []
            self.logf = None
            self.filseparator = ','

            # absolute path
            self.abspath = os.path.dirname(os.path.abspath(__file__))
            self.fname = os.path.basename(__file__)

            conf = config(self.abspath, 'settings.json')

            params = conf.ReadScriptData('update_data')
            # params = conf.ReadScriptData('kafka')
            self.broker = params['kafka']['broker']
            self.topic = params['kafka']['topic']
            
            #query params
            self.username = params["sql"]["username"]
            self.pwd = params["sql"]["pwd"]
            self.server = params["sql"]["server"]

            self.lips = params['static_ip']

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
            self.logf = SKLlogging('update_data', 'BSO_0000', 'I', self.homeLogs)

            # logging.basicConfig(filename='./Import.log', filemode="w", format='%(asctime)s - %(levelname)s:%(message)s',level=logging.INFO)
            print (args.tipo)
            # cartella srogente dei risultati
            if 'site'  in args.tipo:
                self.path_hm_source= './Software/modular_software/loading_performance/Sitespeed/Results/'
                self.kafka_key_id = params['kafka']['type_id_sitespeed']#3
                #destinazione file scaricati dalle hm
                self.path_hm_local = self.homeRepository + '/' + 'sitespeed' + '_in/'
                #destinazione file caricati su kafka
                self.path_hm_local_done =  self.homeRepository + '/' + 'sitespeed' + '_out/'
            else:
                self.path_hm_source = './Software/modular_software/loading_performance/results/'
                self.kafka_key_id = params['kafka']['type_id_loading']#4
                #destinazione file scaricati dalle hm
                self.path_hm_local = self.homeRepository + '/' + 'loading_performance' + '_in/'
                #destinazione file caricati su kafka
                self.path_hm_local_done =  self.homeRepository + '/' + 'loading_performance' + '_out/'
            self.path_hm_dest = '/already_on_noc/'

            self.logf.info("logging system initialized")

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
            print(err)

    def main(self, write: str):
        try:
            if write == "False":
                self.logf.info("Retrieving csv files from the VMs")
                for ip in self.lips:
                    self.logf.info(f"connectionHM({ip})")
                    self._connectionHM(ip)
            self.logf.info(f"Iterating over the csv files")
            self._iterate_over_file()

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))


    def _connectionHM(self, terminalIp: str):
        """ Connect to hm and download file in local, on remote move file to already_on_noc

            Args:
                ip (str): Terminal IP address
        """

        try: 
            lscmd = f'ls {self.path_hm_source} | egrep *.csv'
            # connection at the terminal
            srv_conn = self._hostConnectionPrmk(terminalIp)
            listfile = srv_conn.ExecuteCmd(lscmd)
            listfile = listfile.split('\n')
            # Copy files locally
            scp = SCPClient(srv_conn.GetTransport())
            
            for file in listfile[:-1]:
                scp.get(f"{self.path_hm_source}{file}", f"{self.path_hm_local}")
                srv_conn.ExecuteCmd(f"mv {self.path_hm_source}{file} {self.path_hm_source}{self.path_hm_dest}{file}")

            self.logf.info("Copy ended")  
            scp.close()

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
        finally:
            if (srv_conn is not None):
                srv_conn.Close()

    def _iterate_over_file(self):
        """ Iterate over csv files to read data to send to kafka 

        """

        # list of files
        lfiles = os.listdir(self.path_hm_local)
        self.logf.info("Creating the dictionary")       
        for file in lfiles:
            nome_file, ext = os.path.splitext(file)
            if ext == ".csv":
                try:
                    self._build_dict(self.path_hm_local + file)
                    shutil.move(self.path_hm_local +file, self.path_hm_local_done + file)
                    print(f"INFO: move {file}")
                except Exception as err:
                    self._print_err(inspect.stack()[0][3], str(err))
                    self.logf.error(f"ERROR : move {self.path_hm_local +file}")
        self.logf.info("Finished")

    def _build_dict(self, file) -> None:
        """ Read the csv file and send data to Kafka
        Args:
            filename (str): file to be processed
        """     
        data = self._readCSV(file)
        self._kafka_recording(data)

    def _readCSV(self, filename):
        """ Read the csv file
        Args:
            filename (str): file to read
        """        

        data = pd.read_csv(filename, sep=self.filseparator, dtype=str)
        # data = data.fillna(0)       # NaN --> 0.0
        data = data.replace({np.nan: None})
        ret = data.to_dict(orient='records')

        return(ret)

    def _kafka_recording(self, LData: dict):
        """ Send a kafka message 
        Args:
            Data (str): data to send to kafka
        Raises:
            Exception: [description]
        """

        try:
            Kafka = Kafka_Producer(self.topic, self.broker)
            
            username: str = self.username
            pwd: str = self.pwd
            server: str = self.server
            
            if 'site'  not in args.tipo:
                test_type_id = 3
            else:
                test_type_id = 4

            query = f"SELECT COUNT(*) FROM kn_core.testinfo_noc2ch_tab WHERE test_type_id = {self.kafka_key_id}"

            if args.check == "yes" or args.check == "y":
                query_len_before,res=db_lib.mysql_query(query, username=username, pwd=pwd, server=server)

            for data in LData:

                kafka_key_id = self.kafka_key_id
                kafka_field = [item for item in data.keys()]
                kafka_values = [item for item in data.values()]
                kafka_types = [type(item) for item in data.values()]
                kafka_values = [str(item) for item in data.values()]
                if 'site'  not in args.tipo:
                    kafka_values[15] = kafka_values[15].replace("'", "")
                message = f"{kafka_key_id},{kafka_field},{kafka_values},{kafka_types}"

                #ret = Kafka.Producer(message)

                #if ret[0] == 0:
                #        self.logf.critical("File to send line ")
                #        self.logf.critical(data)
                #        self.logf.critical(ret)

            if args.check == "yes" or args.check == "y":
                sleep(1)
                query_len_after,res=db_lib.mysql_query(query, username=username, pwd=pwd, server=server)
                if query_len_before[0]["count()"] == query_len_after[0]["count()"]:
                    self.logf.critical("Message was not written in DB")
                    sys.exit()

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))

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
    parser = argparse.ArgumentParser(description='This script collect all data from VM and send to dB')
    parser.add_argument("-w", "--write", help="Write all file on dB", type=str,default="False")
    parser.add_argument('-Type','-t','-T', dest='tipo', help='Specify the type of test lp/site(loading_performance/sitespeed)', required=True,  type=str)
    parser.add_argument("-check", "-c", help="Check if the results are inserted,takes longer time", type=str,default="yes")
 
    global args 
    args = parser.parse_args() 
    cls = update_data()
    if(cls.LError == []):
        cls.main(args.write)
        print("INFO update_data Complete")
    else:
        print(*cls.LError, sep="\n")
