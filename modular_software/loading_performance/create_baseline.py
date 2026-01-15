import argparse
import pickle
import numpy as np
from collections import defaultdict
from operator import index
from datetime import datetime,timedelta
import pandas as pd
import inspect
import os
import imp
import datetime
import string
import json

# private
from python_connectors.Helper.Config import config
from python_connectors.Helper.Logger_P36 import SKLlogging
from python_connectors.Helper.Kafka_Handle_P36 import Kafka_Producer
from python_connectors.SKl_Connector_P37.ContainerAPI import Readers


#load wrapper for db query (present on nocscript.lan)
db_lib = imp.load_source('DB_Handle_P35','/opt/scripts/lib/python-library/DataBase/DB_Handle_P35.py')


class create_baseline():

    def __init__(self) -> None:
        try:

            print("Initializing configuration")
            self.logf = None
            self.LError = []
            # absolute path
            
            self.abspath = os.path.dirname(os.path.abspath(__file__))
            self.fname = os.path.basename(__file__)

            #------------------Args----------------------------------
            parser = argparse.ArgumentParser(description='This script retrieves VSAT Stats via API method')
            parser.add_argument('-E', '--env',help="Specify if the environment is prod or lab", required=True,  type=str)
            parser.add_argument('-m', '--mode',help="Specify if the date range for the baseline will be decided automatically(last two weeks) or insert the range manually", required=True,  type=str, default="A")
            parser.add_argument('-i', '--initial_date',help="If mode = M initial date format %Y-%m-%d", required=False,  type=str)
            parser.add_argument('-f', '--final_date',help="If mode = M Final date format %Y-%m-%d", required=False,  type=str)
            parser.add_argument('-l', '--label',help="Comment of the baseline", required=False,  type=str)
            parser.add_argument('-d', '--device',help="In manual mode specify device name ota,hm or ht", required=False,  type=str)
            parser.add_argument('-b', '--beam',help="In manual mode specify the beam", required=False,  type=int)
            parser.add_argument('-a', '--aggregate',help="In manual mode specify if yuo want aggregate results of two consecutives hours ", required=False,  type=str, default="no")


            self.args = parser.parse_args()
            
            conf = config(self.abspath, 'settings.json')
            params = conf.ReadScriptData('create_baseline')
            #self.days_delay = params["sql"]["days_delay"]
            self.username = params["sql"]["username"]
            self.pwd = params["sql"]["pwd"]
            self.server = params["sql"]["server"]
            self.devices = params["sql"]["devices"]
            self.broker = params['kafka']['broker_lab']
            self.topic = params['kafka']['topic_lab']
            
            if self.args.mode.upper() == "A":
                self.initial_date = str((datetime.datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')) + " 00:00:00"
                self.final_date = str((datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')) + " 23:59:59"
            else:
                self.initial_date = self.args.initial_date + " 00:00:00"
                self.final_date  = self.args.final_date + " 23:59:59"

            # ---------------- Logs --------------------------------------------------------------------
            self.homeLogs = "/../../logs"
            if('/' not in self.homeLogs[-1]):
                self.homeLogs = self.abspath + self.homeLogs

            self.logf = SKLlogging('create_baseline', 'BSO_0000', 'I', self.homeLogs)

            self.logf.info("logging system initialized")

            
            self.logf.info(f"Baseline will be created using the data from {self.initial_date}  to  {self.final_date}")

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

        ############### Manual Mode ######################
        if self.args.mode != "A":
            try:
                device_type = self.args.device
                beam_number = self.args.beam
                if device_type != "ota" and device_type != "hm" and device_type != "ht":
                    self.logf.info("Parameter device is not set correctly on settings.json")
                else:
                    self.logf.info(f"Creating baseline for device: {device_type} and beam: {beam_number}") 

                    db = get_data_fromDB(self, device_type, beam_number)

                    if len(db) > 1:
                        df = baseline(db,self)
                        df = df.to_dict()
                        if self.short_samples < 600:
                            _kafka_recording(self, df, device_type, beam_number)
                        else:
                            self.logf.critical("Not enough samples to create the baseline")
                    else:
                        self.logf.info(f"Data for device {device_type} and beam {beam_number} not found in DB in the date range: {self.initial_date}  to  {self.final_date}")
            except Exception as err:
                    self._print_err(inspect.stack()[0][3], str(err))


    ################## Automatic mode#####################################
        else:
            for el in self.devices:
                device_type = el[0]
                beam_number = el[1]
                self.args.env = el[2]
                try:
                    if device_type != "ota" and device_type != "hm" and device_type != "ht":
                        self.logf.info("Parameter device is not set correctly on settings.json")
                    else:
                        self.logf.info(f"Creating baseline for device: {device_type} and beam: {beam_number}") 

                        db = get_data_fromDB(self, device_type, beam_number)
                        ##TESING

                        
                        if len(db) > 1:
                            df = baseline(db,self)
                            df = df.to_dict()
                            if self.short_samples < 600:
                                _kafka_recording(self, df, device_type, beam_number)
                            else:
                                self.logf.critical("Not enough samples to create the baseline")
                        else:
                            self.logf.info(f"Data for device {device_type} and beam {beam_number} not found in DB in the date range: {self.initial_date}  to  {self.final_date}")

                except Exception as err:
                    self._print_err(inspect.stack()[0][3], str(err))



def _kafka_recording(self, LData: dict, device_type: str, beam_number: int):
        """ Send a kafka message 
        Args:
            Data (str): data to send to kafka
        Raises:
            Exception: [description]
        """

        try:
            Kafka = Kafka_Producer(self.topic, self.broker)
            
            counter = 0
            
            ### If the modem is an HM the Baseline will be saved on DB with test_type_id equal to the beam, if its an ota it will be saved with id 1000+beam
            if self.args.env == "prod":
                if device_type == "hm":
                    kafka_key_id = beam_number
                else:
                    kafka_key_id = beam_number + 1000
            else:
                if device_type == "hm":
                    kafka_key_id = beam_number + 2000
                else:
                    kafka_key_id = beam_number + 3000

            kafka_field = list(LData.keys())

            self.logf.info(f"Writing on DB with kafka_key_id = {kafka_key_id}")   

            for el in range(len(LData[kafka_field[0]])):
                kafka_values = []
                for field in kafka_field:
                    kafka_values.append(str(LData[field][el]))
                kafka_types = [type(item) for item in kafka_values]
                message = f"{kafka_key_id},{kafka_field},{kafka_values},{kafka_types}"
                
                ret = Kafka.Producer(message)

                if ret[0] == 0:
                    self.logf.critical("File to send line ")
                    self.logf.critical(el)
                    self.logf.critical(ret)

            self.logf.info("Baseline created succesfully")

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))

def get_data_fromDB(self, device_type, beam_number):



    query = f"SELECT  DISTINCT a_val, a_key FROM kn_core.testinfo_noc2ch_tab WHERE test_type_id = 3 AND a_val[-1] BETWEEN '{self.initial_date}' AND '{self.final_date}' AND a_val[6] = '{beam_number}'"
    query= query.replace("/", "-")

    self.logf.info("Starting DB query")

    username: str = self.username
    pwd: str = self.pwd
    server: str = self.server
#    SS,res=db_lib.mysql_query(query, username=username, pwd=pwd, server=server)
    clickhouse = Readers.CallPymysql102()
    clickhouse.Setvalues({
    "dbhost": server,
    "user": username,
    "psw": pwd
    })
    SS = clickhouse.GetDict(query) 
    
    try:
        data_rows = len(SS)
    except:
        self.logf.critical(f"Error with the db query")
        return -1
    self.logf.info(f"Query completed, number of data rows to be analysed: {data_rows}")
    

    ## Creating the dataframe with all possible columns

    db = pd.DataFrame(columns =["website", "host_name", "ipgw" ,"esn","siteid","beam", "outroute_freq", "first_contentful_paint", "total_blocking_time", "load_time", "first_byte_time",\
         "dom_content_loaded", "dom_Interactive_time", "total_time", "hop_number", "Hardware_type", "Software_version", "Environment", "timestamp", "iperf_speed"])

    skip_counter = 0
    print_counter = 1
    
    self.logf.info("Creating dictionary")

    ## Initial loop over the sql query response
    
    try:
        for elem in range(len(SS)):
            if elem == print_counter * 5000:
                print_counter = print_counter + 1
                self.logf.info(f"Done analysing row: {elem}")

            keys = SS[elem]["a_key"].split("[")[1].split("]")[0].replace("'","").split(",")

            #find position of trace_dict
            for el in range(len(keys)):
                if keys[el] == "trace_dict":
                    trace = el      

            site = SS[elem]["a_val"].split("[")[1].split("]")[0].replace("'","").split("{")[0].split(",")[:-1][0]
            modem = SS[elem]["a_val"].split("[")[1].split("]")[0].replace("'","").split("{")[0].split(",")[:-1][1]
            beam = SS[elem]["a_val"].split("[")[1].split("]")[0].replace("'","").split("{")[0].split(",")[5]
            site_id = SS[elem]["a_val"].split("[")[1].split("]")[0].replace("'","").split("{")[0].split(",")[4]



            if "Environment" not in keys:
                if modem == "HM10_B117" or modem == "HT6_B117":
                    env = "lab"
                else:
                    env = "prod"
            else:
                env = SS[elem]["a_val"].split("[")[1].split("]")[0].replace("'","").split("}")[-1].split(",")[3]
            
            if "ota" in modem:
                device = "ota"
            elif "GW" in modem or "HM" in modem:
                device = "hm"
            elif "HT" in modem:
                device = "ht"
                
                ## skip the sites that give no info and the modems we are not interested in
            try:
                if self.args.env != env:
                    skip_counter = skip_counter + 1
                    continue
                if  site == 'https://www.tragesser.de' or site == "https://www.beeld.com"  or modem == "starlink" or modem == "fixed_skl_net" or modem == "Huges":
                    skip_counter = skip_counter + 1 
                    continue
                if site_id == 'Not_Commissioned':
                   skip_counter = skip_counter + 1 
                   continue                 
                if beam_number != int(beam) or device_type != device:
                    skip_counter = skip_counter + 1 
                    continue
            except:
                skip_counter = skip_counter + 1
                continue  
            
            #Recuperating all of the metrics from str to dataframe except tracedict 
            try:
                if SS[elem]["a_val"].count("None") > 3: ## skip files with too many None values
                    continue
                values = SS[elem]["a_val"].split("[")[1].split("]")[0].replace("'","").split("{")[0].split(",")[:-1]
                values1 = SS[elem]["a_val"].split("[")[1].split("]")[0].replace("'","").split("}")[-1].split(",")[1:]
                a_series = pd.Series(values, index = keys[:trace])
                a_series1 = pd.Series(values1, index = keys[trace+1:])
                a_series = a_series. append(a_series1)
                db = db.append(a_series, ignore_index=True)
            except:
                print(f"Error with data in line {elem}")
                continue

        self.logf.info("DataFrame created successfully")

        ## Setting all of the timestamps to UTC
        for ii in range(len(db)):
            if db["host_name"][ii] == "ota3":
                continue
            else:
                old_date = datetime.datetime.strptime(db["timestamp"][ii], '%Y-%m-%d %H:%M:%S')
                hours_added  = datetime.timedelta(hours = 4)
                ok_date = old_date + hours_added
                db["timestamp"][ii] = ok_date.strftime('%Y-%m-%d %H:%M:%S')

    except Exception as err:
        print(elem)
        db.to_csv("/home/export/ggrillo/GIT/modular_software/loading_performance/baseline_test.csv", na_rep='NA', index= False, sep = ";")
        self._print_err(inspect.stack()[0][3], str(err))
    rows_db = len(db)    
    self.logf.info(f"Number of data rows in db: {rows_db}")
    return(db)


def baseline(db,self):
    #change names of the hosts to group if necessary
    self.logf.info("Creating dataframe")

    modems = db.host_name.unique()
    self.logf.info(f"Modems: {modems}")
    db = db.fillna("NaN")
    ## Features of interest for the baseline
    features = ["load_time" , "first_byte_time", \
            "dom_content_loaded", "dom_Interactive_time", "total_time", "hop_number"]



    for feature in features:
        try:
            db[feature] = db[feature].replace("None", -1)
            db[feature] = db[feature].replace("NaN", np.nan)
            db[feature] = db[feature].astype(int)
        except Exception as err:
            db[feature] = db[feature].astype(float)
            db[feature] = db[feature].astype(int)
    try:
        software_list = str(db["Software_version"].value_counts().index.tolist())[2:-2].replace(",", "-").replace("'", "").replace(" ","")
        hardware_list = str(db["Hardware_type"].value_counts().index.tolist())[2:-2].replace(",", "-").replace("'", "").replace(" ","")

    except:
        db.software_version = "NA"
        db.hardware_type = "NA"
        db2 = db


    db2 = db
    
    
    
    self.short_samples = 0



    websites = db2.website.unique()    

    stats = defaultdict(dict)
    

    if "ota" in modems[0]:
        device = "ota"
    elif "GW" in modems[0] or "HM" in modems[0]:
        device = "hm"
    elif "HT" in modems[0]: 
        device = "ht"

    beam = db["beam"][0]
    stats[device] = defaultdict(dict)    
    stats[device][beam] = defaultdict(dict)

    
    for site in websites:
        stats[device][beam][site] = defaultdict(dict) 
        db3 = db2[db2.website == site] 
        hours = []
        count = 0
        for time in db3.timestamp:
            hours.append(int(time.split(" ")[-1].split(":")[0]))

                    
        db3.loc[:,"hour"] = hours
        
        for hour in range(24):
            db4 = db3[db3.hour == hour]
            if self.args.aggregate == "yes":
                if hour == 23:
                    db4 = db3[(db3.hour == hour) | (db3.hour == 0)]
                else:
                    db4 = db3[(db3.hour == hour) | (db3.hour == hour + 1)]
                if len(db4) < 20:
                    self.logf.warning(f"{site} has only {len(db4)} samples for hours : {hour} and {hour+1}")
                    self.short_samples += 1
            else:
                if len(db4) < 10:
                    self.logf.warning(f"{site} has only {len(db4)} samples for hour : {hour}")
                    self.short_samples += 1
                
            stats[device][beam][site][hour] = defaultdict(dict) 

            for feature in features:
                stats[device][beam][site][hour][feature]["avg"] = db4[feature].mean()
                stats[device][beam][site][hour][feature]["mediana"] = db4[feature].median()
                stats[device][beam][site][hour][feature]["sd"] = db4[feature].std()
                stats[device][beam][site][hour][feature]["per25"] = db4[feature].quantile(.25)
                stats[device][beam][site][hour][feature]["per75"] = db4[feature].quantile(.75)
                stats[device][beam][site][hour][feature]["per90"] = db4[feature].quantile(.90)  

    df = pd.DataFrame(columns= ["Device", "Beam", "Site", "Hour", "Feature", "avg", "mediana","sd", "per25","per75","per90"])

    for device in stats:
        for beam in stats[device]:
            for site in stats[device][beam]:
                for hour in stats[device][beam][site]:
                    for feature in stats[device][beam][site][hour]:
                        keys_list = ["device", "beam", "site", "hour", "feature" ,"avg", "mediana","sd", "per25","per75","per90"]
                        values_list = [device, beam, site, hour, feature]
                        for statistic in stats[device][beam][site][hour][feature]:
                            values_list.append(stats[device][beam][site][hour][feature][statistic])
                        df_length = len(df)
                        df.loc[df_length] = values_list

    df["version"] = self.initial_date.split(" ")[0].replace("-", "/") + "-" + self.final_date.split(" ")[0].replace("-", "/")
    df["software_version"] = software_list
    df["hardware_type"] = hardware_list
    if self.args.mode == "A":
        if self.args.aggregate == "yes":
            df["label"] = "Automatic-browsing test aggregation 2 hours"
        else:
            df["label"] = "Automatic-browsing test"
    else:
        if self.args.aggregate == "yes":
            df["label"] = self.args.label + "-browsing test aggregation 2 hours"
        else:
            df["label"] = self.args.label + "-browsing test"

    ##Change Baseline version in settings
    with open(f"{os.path.dirname(__file__)}/settings.json") as json_file:
        settings = json.load(json_file)


    with open(f"{os.path.dirname(__file__)}/settings.json", 'w') as json_file:
        json.dump(settings,json_file, indent=2)

    file_name = os.path.dirname(os.path.abspath(__file__)) + "/baseline.csv"
    df.to_csv(file_name,index = False)
    self.logf.info(f"Baseline created succesfully on the following path: {file_name}")
    return(df)

    



if __name__ == "__main__":
    cls = create_baseline()
    if(cls.LError == []):
        cls.main()
    else:
        print(*cls.LError, sep="\n")
