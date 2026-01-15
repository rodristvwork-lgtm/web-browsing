import imp
import argparse
import inspect
import os
import pandas as pd
# private
from python_connectors.Helper.Config import config
from python_connectors.Helper.Logger_P36 import SKLlogging
from python_connectors.Helper.Kafka_Handle_P36 import Kafka_Producer
from python_connectors.SKl_Connector_P37.ContainerAPI import Readers

db_lib = imp.load_source('DB_Handle_P35','/opt/scripts/lib/python-library/DataBase/DB_Handle_P35.py')

class baseline_list():

    def __init__(self) -> None:
        try:
            print("Initializing configuration")
            self.logf = None
            self.LError = []
            # absolute path
            
            self.abspath = os.path.dirname(os.path.abspath(__file__))
            self.fname = os.path.basename(__file__)

            #------------------Args----------------------------------
            parser = argparse.ArgumentParser(description='This script retrieves a list of baseline versions')
            parser.add_argument('-d', '--device',help="Specify device name ota,hm or ht", required=False,  type=str)
            parser.add_argument('-b', '--beam',help="Specify the beam", required=False,  type=int)
            parser.add_argument('-E', '--env',help="Specify if the environment is prod or lab", required=True,  type=str)
            parser.add_argument('-n', '--num',help="Number of last baselines shown", required=False,  type=int, default= -1)
            parser.add_argument('-a', '--all',help="If True will be shown baselines from all devices belonging to choosen env", required=False,  type=str, default="False")            
            parser.add_argument('-tsk', '--timestamp_k',help="it will be taken only baselines published on db after timestamp indicated", required=False,  type=str, default="2022-02-18 13:10:00")                       
            self.args = parser.parse_args()
            
            conf = config(self.abspath, 'settings.json')
            params = conf.ReadScriptData('create_baseline')
            self.username = params["sql"]["username"]
            self.pwd = params["sql"]["pwd"]
            self.server = params["sql"]["server"]
            self.devices = params["sql"]["env"][self.args.env]["devices"]
            self.broker = params['kafka']['broker_lab']
            self.topic = params['kafka']['topic_lab']




            # ---------------- Logs --------------------------------------------------------------------
            self.homeLogs = "/../../logs"
            if('/' not in self.homeLogs[-1]):
                self.homeLogs = self.abspath + self.homeLogs

            self.logf = SKLlogging('baseline_list', 'BSO_0000', 'I', self.homeLogs)

            self.logf.info("logging system initialized")


#            self.logf.info(f"Baseline will be created using the data from {self.initial_date}  to  {self.final_date}")

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
            self.LError.append(str(err))
            print(err)

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

    def main(self):
        if self.args.all == "False":
            self.devices = [[self.args.device, self.args.beam]]
        for d in self.devices:
            flag_bp = True
            flag_v = True
            device = d[0]
            beam = d[1]
            try:
                if self.args.env == "prod":
                    if device == "ota":
                        test_type_id = int(beam) + 1000
                    else:
                        test_type_id = int(beam)
                else:
                    if device == "hm":
                        test_type_id = int(beam) + 2000
                    else:
                        test_type_id = int(beam) + 3000

                #browsing and SAT test query
                query = f"SELECT DISTINCT a_val[12], a_val[15] FROM kn_core.testinfo_noc2ch_tab WHERE test_type_id = {test_type_id} AND timestamp_k > '{self.args.timestamp_k}' ORDER BY timestamp_k DESC"
#                self.logf.info("Starting DB query")

                username: str = self.username
                pwd: str = self.pwd
                server: str = self.server
                SS,res=db_lib.mysql_query(query, username=username, pwd=pwd, server=server)
                if SS == 0:
                    flag_bp = False
                list_initial_automatic = []
                list_final_automatic = []
                list_label_automatic = []
                list_initial_manual = []
                list_final_manual = []
                list_label_manual = []
                if flag_bp == True:
                    for dict in SS:
                        if dict["arrayElement(a_val, 12)"] == "" or dict["arrayElement(a_val, 15)"] == "":
                            continue
                        if  "Automatic" in dict["arrayElement(a_val, 15)"] and "-" in dict["arrayElement(a_val, 12)"]:
                            list_initial_automatic.append(dict["arrayElement(a_val, 12)"].split("-")[0])
                            list_final_automatic.append(dict["arrayElement(a_val, 12)"].split("-")[1])
                            list_label_automatic.append(dict["arrayElement(a_val, 15)"])
                        elif "-" in dict["arrayElement(a_val, 12)"]:
                            list_initial_manual.append(dict["arrayElement(a_val, 12)"].split("-")[0])
                            list_final_manual.append(dict["arrayElement(a_val, 12)"].split("-")[1])
                            list_label_manual.append(dict["arrayElement(a_val, 15)"])

                #SIP test query
                query = f"SELECT DISTINCT a_val[11], a_val[14] FROM kn_core.testinfo_noc2ch_tab WHERE test_type_id = {test_type_id} AND a_val[14] LIKE '%SIP%' AND timestamp_k > '{self.args.timestamp_k}' ORDER BY timestamp_k DESC"
#                self.logf.info("Starting DB query")

                username: str = self.username
                pwd: str = self.pwd
                server: str = self.server
                SS,res=db_lib.mysql_query(query, username=username, pwd=pwd, server=server)
                if SS == 0:
                    flag_v = False
                if flag_v == False and flag_bp == False:
                    print(f"{device} in beam: {beam} has not any baseline written after {self.args.timestamp_k}")
                    continue #next device and beam
                if flag_v == True:
                    for dict in SS:
                        if dict["arrayElement(a_val, 11)"] == "" or dict["arrayElement(a_val, 14)"] == "":
                            continue
                        if  "Automatic" in dict["arrayElement(a_val, 14)"] and "-" in dict["arrayElement(a_val, 11)"]:
                            list_initial_automatic.append(dict["arrayElement(a_val, 11)"].split("-")[0])
                            list_final_automatic.append(dict["arrayElement(a_val, 11)"].split("-")[1])
                            list_label_automatic.append(dict["arrayElement(a_val, 14)"])
                        elif "-" in dict["arrayElement(a_val, 11)"]:
                            list_initial_manual.append(dict["arrayElement(a_val, 11)"].split("-")[0])
                            list_final_manual.append(dict["arrayElement(a_val, 11)"].split("-")[1])
                            list_label_manual.append(dict["arrayElement(a_val, 14)"])
                df_a = pd.DataFrame(list(zip(list_initial_automatic, list_final_automatic, list_label_automatic)),
                    columns =['initial date', 'final date', 'label'])
                df_m = pd.DataFrame(list(zip(list_initial_manual, list_final_manual, list_label_manual)),
                    columns =['initial date', 'final date', 'label'])
                #df_a = df_a.head(self.args.num)
                if self.args.num == -1:
                    if len(df_m) != 0:
                        print(f"Manual baselines for device type: {device} and beam: {beam}")
                        print(df_m) 
                    else:
                         print(f"No Manual baselines for device type: {device} and beam: {beam}")
                    if len(df_a) != 0:                 
                        print(f"\nAutomatic baselines for device type: {device} and beam: {beam}\n")
                        print(df_a)
                    else:
                         print(f"\nNo Automatic baselines for device type: {device} and beam: {beam}\n")
                else:
                    df_a = df_a.head(self.args.num)
                    df_m = df_m.head(self.args.num)
                    if len(df_m) != 0:
                        print(f"Last {self.args.num} manual baselines for device type: {device} and beam: {beam}")
                        print(df_m)
                    else:
                         print(f"No Manual baselines for device type: {device} and beam: {beam}")
                    if len(df_a) != 0:
                        print(f"\nLast {self.args.num} automatic baselines for device type: {device} and beam: {beam}\n")
                        print(df_a)
                    else:
                         print(f"\nNo Automatic baselines for device type: {device} and beam: {beam}\n")


            except Exception as err:
                self._print_err(inspect.stack()[0][3], str(err))
                self.LError.append(str(err))
                print(err)




if __name__ == "__main__":
    cls = baseline_list()
    if(cls.LError == []):
        cls.main()
    else:
        print(*cls.LError, sep="\n")
