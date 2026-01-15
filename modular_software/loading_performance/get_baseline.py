from collections import defaultdict
from operator import index
import pandas as pd
import inspect
import os
import imp
import datetime
import glob
import math
from datetime import timedelta
from collections import defaultdict
from tabulate import tabulate

# private
from python_connectors.Helper.Config import config
from python_connectors.Helper.Logger_P36 import SKLlogging
from python_connectors.Helper.Kafka_Handle_P36 import Kafka_Producer
from python_connectors.SKl_Connector_P37.ContainerAPI import Readers

db_lib = imp.load_source('DB_Handle_P35','/opt/scripts/lib/python-library/DataBase/DB_Handle_P35.py')


def get_baseline_from_db(self):

    if self.env == "prod":
        if self.device == "hm":
            test_type_id = self.beam_compare
        else:
            test_type_id = self.beam_compare + 1000
    else:
        if self.device == "hm":
            test_type_id = self.beam_compare + 2000
        else:
            test_type_id = self.beam_compare + 3000

    
    username: str = self.username
    pwd: str = self.pwd
    server: str = self.server

    self.logf.info("Starting DB query")


    if self.mode != "A":
            version = self.initial_date.split(" ")[0].replace("-", "/") + "-" + self.final_date.split(" ")[0].replace("-", "/")

            query = f"SELECT * FROM kn_core.testinfo_noc2ch_tab WHERE test_type_id = {test_type_id} AND a_val[12] = '{version}' AND a_val[15] = '{self.label}'"

            #SS,res=db_lib.mysql_query(query, username=username, pwd=pwd, server=server)
            clickhouse = Readers.CallPymysql102()
            clickhouse.Setvalues({
                "dbhost": server,
                "user": username,
                "psw": pwd
            })

            SS = clickhouse.GetDict(query)
            try:
                if len(SS) != 0:
                    self.logf.info(f"Baseline found with label {self.label} in the range {self.initial_date} to {self.final_date}")
            except:
                self.logf.critical(f"No baseline found with label {self.label} in the range {self.initial_date} to {self.final_date}")
                exit = 1

    else:
        for ii in range(5):
            
            self.initial_date = str((datetime.datetime.now() - timedelta(days=14+ii)).strftime('%Y-%m-%d')) + " 00:00:00"
            self.final_date = str((datetime.datetime.now() - timedelta(days=ii+1)).strftime('%Y-%m-%d')) + " 23:59:59"

            version = self.initial_date.split(" ")[0].replace("-", "/") + "-" + self.final_date.split(" ")[0].replace("-", "/")
            
            query = f"SELECT * FROM kn_core.testinfo_noc2ch_tab WHERE test_type_id = {test_type_id} AND a_val[12] = '{version}'"

            #print("Starting DB query")

            #SS,res=db_lib.mysql_query(query, username=username, pwd=pwd, server=server)
            clickhouse = Readers.CallPymysql102()
            clickhouse.Setvalues({
            "dbhost": config.settings["sql1"]["server"],
            "user": config.settings["sql1"]["username"],
            "psw": config.settings["sql1"]["pwd"]
            })
            SS = clickhouse.GetDict(query)
            
            try:
                if len(SS) != 0:
                    break
            except:
                if ii == 4:
                    self.logf.warning(f"No baseline found in the last 5 days")
                    exit = 1
                else:
                    self.logf.critical(f"No baseline found for version: {version}")
                continue

    data_rows = len(SS)
    
    #print(f"Query completed, number of data rows to be analysed: {data_rows}")
    self.logf.info(f"Query completed, number of data rows to be analysed: {data_rows}")
    
    keys = SS[0]["a_key"].split("[")[1].split("]")[0].replace("'","").split(",")

    df = pd.DataFrame(columns = SS[0]["a_key"].split("[")[1].split("]")[0].replace("'","").split(","))
    
    #print_counter = 1

    #print("Creating dictionary")
    self.logf.info("Creating dictionary")
    try:
        for elem in range(len(SS)):
            # if elem == print_counter * 1000:
            #     print_counter = print_counter + 1
            #     print(f"Done with row: {elem}")
            
            values = SS[elem]["a_val"].split("[")[1].split("]")[0].replace("'","").split(",")
            df_lenght = len(df)
            df.loc[df_lenght] = values
          
        #print("Dictionary created successfully")
        self.logf.info("Dictionary created successfully")

    except Exception as err:
        print(elem)
        df.to_csv("/home/export/ggrillo/GIT/modular_software/loading_performance/baseline_test.csv", na_rep='NA', index= False, sep = ";")
        self._print_err(inspect.stack()[0][3], str(err))
    
    return(df)

class get_baseline:

    def __init__(self,device, beam, env, mode, initial_date, final_date, label, beam_compare) -> None:
        try:

            #print("Initializing configuration")
            self.logf = None
            self.LError = []
            # absolute path
            
            self.abspath = os.path.dirname(os.path.abspath(__file__))
            self.fname = os.path.basename(__file__)
            
            conf = config(self.abspath, 'settings.json')
            params = conf.ReadScriptData('get_baseline')
            self.username = params["sql"]["username"]
            self.pwd = params["sql"]["pwd"]
            self.server = params["sql"]["server"]
            #self.device = params["sql"]["device"]
            #self.beam = params["sql"]["beam"]
            self.device = device
            self.beam = beam
            self.env = env
            self.broker = params['kafka']['broker_lab']
            self.topic = params['kafka']['topic_lab']
            self.mode = mode
            self.initial_date = initial_date
            self.final_date = final_date
            self.label = label
            self.beam_compare = beam_compare

            # ---------------- Logs --------------------------------------------------------------------
            self.homeLogs = "/../../logs"
            if('/' not in self.homeLogs[-1]):
                self.homeLogs = self.abspath + self.homeLogs

            self.logf = SKLlogging('get_baseline', 'BSO_0000', 'I', self.homeLogs)

            self.logf.info("logging system initialized")

            #print(f"Baseline will be created using the data from {self.initial_date}  to  {self.final_date}")
            self.logf.info(f"Retreiving Baseline for {self.device} and beam {self.beam}")

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
        try:
            df = get_baseline_from_db(self)
            compare_run_with_baseline(self, df)

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
            self.LError.append(str(err))
            print(err)


def check_test_time(self,df):
    '''exctract test hour from timestamp's last test'''
    if self.device == "ota" and int(self.beam) == 217:
        phase = 0
    else:
        if self.initial_date == "2022-06-28" and self.final_date == "2022-07-04":
            phase = 6
        else:
            phase = 4
    if df.at[0,"timestamp"][-8] == "0":
        hour = df.at[0, "timestamp"][-7]  #one digit number, without 0
    else:
        hour = df.at[0, "timestamp"][-8:-6] #two digit number

    hour = int(hour) + phase
    if hour == 24:
        hour = 0
    elif hour == 25:
        hour = 1
    elif hour == 26:
        hour = 2
    elif hour== 27:
        hour = 3
    elif hour == 28:
        hour = 4
    elif hour == 29:
        hour = 5
    elif hour == 30:
        hour = 6


    return hour


def compare_run_with_baseline(self, df) :
    path = os.path.dirname(os.path.abspath(__file__))
    path = path.split("modular_software")[0] + "repository/orchestrator_in/*csv"
    list_files = glob.glob(path) 
    if len(list_files) > 1:
        beam_files_list = []
        for el in list_files:
            if "B" + str(self.beam) in el:
                beam_files_list.append(el)
        file = max(beam_files_list, key=os.path.getctime)
    else:
        file = list_files[0] 
    #last_file = max(list_files, key=os.path.getctime)  #find last file created
    df_test = pd.read_csv(file)
#    df_baseline = pd.read_csv(r"C:\Users\michele.mandosso\Desktop\prova\baseline (1).csv")
    df_baseline = df
#    print(df_baseline)
    hour = check_test_time(self,df_test)
    #metric = "load_time"

    features = ["load_time" , "first_byte_time", \
            "dom_content_loaded", "dom_Interactive_time", "total_time", "hop_number"] 
    failed_test = 0
    count_row = df_test.shape[0]  # Gives number of rows
    total_tests = 0

    result = defaultdict(dict)
    dict_per1 = defaultdict(dict)
    dict_per2 = defaultdict(dict)
    
    websites = df_test["website"].unique()
    for sites in websites:
        dict_per1[sites] = defaultdict(dict)
        dict_per2[sites] = defaultdict(dict)
    for metric in features:
        result[metric] = defaultdict(dict)
        for row in range(0, count_row):
            res = {}
            res["website"] = df_test.at[row, "website"]
            res["data"] = df_test.at[row, metric]
            site = df_test.at[row, "website"]
            #t = df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]
            #result[metric][res["website"]] = defaultdict(dict)
            


            if math.isnan(float(df_test.at[row, metric])) == True or df_test.at[row, metric] == -1: #don't consider if NaN or -1
                res["result"] = "test result NaN"
                #print(res)
                continue      

            if metric ==  "load_time":
                name_per_dict = "lt"
                option= 1
            elif metric == "first_byte_time":
                name_per_dict = "fbt"
                option= 1
            elif metric == "dom_content_loaded":
                name_per_dict = "dcl"
                option= 1
            elif metric == "dom_Interactive_time":
                name_per_dict = "dit"
                option= 2
            elif metric == "total_time":
                name_per_dict = "tt"
                option= 2
            elif metric == "hop_number":
                name_per_dict = "hn"
                option= 2   
            try:
                total_tests += 1
                if option == 1:
                    dict_per1[site][name_per_dict+str(25)] = res["25 percentile"] = float(df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]["per25"].item())
                    dict_per1[site][name_per_dict+str(50)] = res["avg"] = float(df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]["mediana"].item())
                    dict_per1[site][name_per_dict+str(75)] = res["75 percentile"] = float(df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]["per75"].item())
                    dict_per1[site][name_per_dict+str(90)] = res["90 percentile"] = float(df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]["per90"].item())  #find per90 value in df_baseline given website, hour and metric
                else:
                    dict_per2[site][name_per_dict+str(25)] = res["25 percentile"] = float(df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]["per25"].item())
                    dict_per2[site][name_per_dict+str(50)] = res["avg"] = float(df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]["mediana"].item())
                    dict_per2[site][name_per_dict+str(75)] = res["75 percentile"] = float(df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]["per75"].item())
                    dict_per2[site][name_per_dict+str(90)] = res["90 percentile"] = float(df_baseline.loc[(df_baseline["Site"] == res["website"]) & (df_baseline["Hour"] == str(hour)) & (df_baseline["Feature"] == metric)]["per90"].item())  #find per90 value in df_baseline given website, hour and metric
            except:
                res["result"] = "no data in baseline"
                print(res)       
                continue         

            if metric == "hop_number":
                metrica = ""
            else:
                metrica = "ms"

            space = ""
            if len(str(int(res["data"]))) == 3:
                space = "  " 
            if len(str(res["data"])) == 4:
                space = " " 

            if res["data"] > res["90 percentile"]: 
                failed_test += 1
                res["result"] = f"{int(res['data'])} {metrica} {space}(90-100)"
            else:
                if  res["data"] < res["25 percentile"]:
                    res["result"] = f"{int(res['data'])} {metrica} {space}(0-25)"
                elif res["data"] < res["avg"]:
                    res["result"] = f"{int(res['data'])} {metrica} {space}(25-50)"
                elif res["data"] < res["75 percentile"]:
                    res["result"] = f"{int(res['data'])} {metrica} {space}(50-75)"
                else:
                    res["result"] = f"{int(res['data'])} {metrica} {space}(75-90)"
            
            result[metric][res["website"]] = res["result"]
    result = pd.DataFrame.from_dict(result)
    res_perc1 = pd.DataFrame(dict_per1).T
    res_perc2 = pd.DataFrame(dict_per2).T

    pdtabulate=lambda result:tabulate(result,headers='keys',tablefmt='psql')
    print(pdtabulate(result))
    print("\n")
    pdtabulate=lambda result:tabulate(res_perc1,headers='keys',tablefmt='psql')
    if self.initial_date == "2022-06-28" and self.final_date == "2022-07-04":
        print(f"baseline from beam {self.beam_compare} at hour {hour-2} UTC")
    else:
        print(f"baseline from beam {self.beam_compare} at hour {hour} UTC")
    print("\n")
    print(pdtabulate(res_perc1))
    print("\n")
    pdtabulate=lambda result:tabulate(res_perc2,headers='keys',tablefmt='psql')
    print(pdtabulate(res_perc2))
    print("\n")

    #print(tabulate(result,headers='firstrow'))

#    failed_ratio = (failed_test/total_tests) * 100
#    if failed_ratio >= 15:
#        print(f"TEST FAILED, {failed_ratio}% of the metrics were above the 90 percentile of the Baseline")
#    else:
#        print(f"TEST PASSED, {100-failed_ratio}% of the metrics were below the 90 percentile of the Baseline")




# if __name__ == "__main__":
#     cls = get_baseline()
#     if(cls.LError == []):
#         cls.main()
#     else:
#         print(*cls.LError, sep="\n")
