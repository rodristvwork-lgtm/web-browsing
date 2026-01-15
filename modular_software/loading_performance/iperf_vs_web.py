import os
import argparse
import inspect
from scp import SCPClient
import pandas as pd
import numpy as np
import shutil
import sys
import glob
import time
from subprocess import PIPE, Popen
# private
from python_connectors.Helper.Config import config
from python_connectors.Helper.Logger_P36 import SKLlogging
from python_connectors.Helper.Kafka_Handle_P36 import Kafka_Producer
from python_connectors.SKl_Connector_P37.ContainerAPI import Readers

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Iperf3Test_FreeTest import IperfTest
#iperf = imp.load_source('Iperf3Test_FreeTest','/opt/scripts/venv/noc_microservices_master/modular_software/Iperf3Test_FreeTest.py')

class iperf_vs_web():
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

            env = 'lab'

            params = conf.ReadScriptData('update_data')
            self.broker = params['kafka'][f'broker']
            self.topic = params['kafka'][f'topic']
            # ----------------- Paramiko -----------------------------
            self.prmk_user = params['terminal']['user_term']
            self.prmk_psw = params['terminal']['psw_term']
            self.timeout = 60
            # ---------------- Logs --------------------------------------------------------------------
            self.homeLogs = params['log']
            # if no path has been defined, the local path is used
            if('/' not in self.homeLogs[-1]):
                self.homeLogs = self.abspath + self.homeLogs
            # shares the logs folder --> used by Error.py
            os.environ["logs"] = self.homeLogs 
            # Log
            # self.logf = SKLlogging('update_data', 'BSO_0000', 'I' if(verbose is False) else 'D', self.homeLogs, False)
            self.logf = SKLlogging('iperf_vs_web', 'BSO_0000', 'I', self.homeLogs)
            # ---------------- Repository --------------------------------------------------------------------

            self.homeRepository = params['repository']

            # if no path has been defined, the local path is used
            if('/' not in self.homeRepository[-1]):
                self.homeRepository = self.abspath + self.homeRepository
            # path hm/Noc
            self.Sitespeed_remote= 'Software/modular_software/loading_performance/Sitespeed/Results/'
            #destinazione file scaricati dalle hm
            self.path_hm_local = self.homeRepository + '/' + 'iperf' + '_in/'
            #destinazione file caricati su kafka
            self.path_hm_local_done =  self.homeRepository + '/' + 'iperf' + '_out/'
            self.loading_performance_remote = 'Software/modular_software/loading_performance/results/'
            #destinazione file caricati su kafka
            self.path_hm_dest = '/already_on_noc/'

            #Launcher Single_test
            self.path_hm_source_launcher_single_test= 'Software/single_site.sh'
            #Launcher Single_test
            self.path_hm_source_launcher= 'Software/site_lp.sh'


#           Args
            p = argparse.ArgumentParser()
            p.add_argument("-v", "--environment", help="Set environment: 'lab','prod'", type=str, required=False)
            p.add_argument('-sv', '--sandvine', help="Sandvine: 2='PRE' (default), 2='POST'", default=2, type=int, required=False)
            p.add_argument("-e", "--esn", help="esn list", nargs='+', type=int, required=False)
            p.add_argument("-st", "--siteId", help="SiteID list", nargs='+', type=str, required=False)
            p.add_argument("-vn", "--vno", help="Vno list to accept", nargs='+', type=str, required=False)
            p.add_argument('-nsd', '--nominalSpeedDownRateKbps', help='nominalSpeedDownRateKbps list', nargs='+', type=int , required=False)
            p.add_argument('-nsu', '--nominalSpeedUpRateKbps', help='nominalSpeedUpRateKbps list', nargs='+', type=int, required=False)
            p.add_argument("-csv", "--filecsv", help="full path file csv", type=str, required=False)
            p.add_argument('-dr', '--duration', help='iperf3 duration', type=int, required=False)
            p.add_argument('-fp', '--fullprint', help="Print all data recived from Server", action='store_true')
            p.add_argument("-lt", "--listtest", help="List of test: DTCP, DUDP, UTCP, UUDP (default All)", nargs='+', type=str, required=False)
            p.add_argument('-mt', '--maxtime', help='maximum execution time', type=int, required=False)
            p.add_argument("-pt", "--port", help="starting value for the ports used in the tests (range from 5052 to 5101)", type=int, required=False)
            p.add_argument("-rt", "--rate", help="value used in TCP tests (Mbps)", type=int, required=False)
            p.add_argument('-ip', '--ip',help='Specify the modem IP to run the test', required=True,  type=str)
            self.args = p.parse_args()
            self.Last_file_loading= ""


        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
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
        self.logf.info("Esecuzione test Iperf\n")
        cls_iperf = IperfTest(self.args.sandvine, self.args.environment, self.args.fullprint, self.args.maxtime, self.args.listtest, self.args.rate)
        cls_iperf.main(self.args.esn, self.args.siteId, self.args.vno, self.args.filecsv, self.args.nominalSpeedDownRateKbps, self.args.nominalSpeedUpRateKbps, self.args.duration, self.args.port)
        self.logf.info("Salvataggio risultati iperf")
        cmd=f"cd {os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))}/logs && ls | grep TestFreeIperf3_IPER"
        resp = _make_cmd(cmd)
        if resp['cmd_err'] == "":
            list_file_iper = resp['cmd_out'].split("\n")
            tail_cmd =f"tail -n 5 {os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))}/logs/{list_file_iper[-2]}"
            tail_exe = _make_cmd(tail_cmd)
            if tail_exe['cmd_err'] == "":
                iper_file = tail_exe['cmd_out'].split("\n")
        if ("Error errorIndication: No SNMP response received before timeout"  not in iper_file[2]):
            timestamp = iper_file[0][:19]
            result_Speed = int (iper_file[0].split(" ")[-1])
        else :
            timestamp = iper_file[0][:19]
            result_Speed = "NA"
        print(f"timestamp:{timestamp} result_Speed:{result_Speed}")
        #Exucute single test
        path_job=self.path_hm_source_launcher_single_test
        ip=self.args.ip
        _launch_job(self,path_job,ip)
        ret = make_dict(self, timestamp, result_Speed)
        try:
                Kafka = Kafka_Producer(self.topic, self.broker)

                self.logf.info("Inizio scrittura su kafka ")
                kafka_key_id = 8
                kafka_field = [item for item in ret.keys()]
                kafka_values = [item for item in ret.values()]
                kafka_types = [type(item) for item in ret.values()]
                kafka_values = [str(item) for item in ret.values()]
                kafka_values[30] = kafka_values[30].replace("'", "")
                message = f"{kafka_key_id},{kafka_field},{kafka_values},{kafka_types}"
                print(message)
                ret_kafka = Kafka.Producer(message)
                if ret_kafka[0] == 0:
                    self.logf.critical("File to send line ")
                    self.logf.critical(ret)
                    self.logf.critical(ret_kafka)

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



def _launch_job(self,path_job:str,terminalIp: str):
    """ Connect to hm and launch job.sh
        Args:
            ip (str): Terminal IP address
    """       
    self.logf.info("Esecuzione test singolo per loading/site")
    try:
        cmd_execute = f'DISPLAY=:0 sh {path_job}'
        lscmd_site = f'cd  {self.Sitespeed_remote} && ls | grep .csv*'
        lscmd_loading = f'cd {self.loading_performance_remote} && ls | grep .csv*'
        # connection at the terminal
        srv_conn = self._hostConnectionPrmk(terminalIp)
        cmd_execute = srv_conn.ExecuteCmd(cmd_execute)
        time.sleep(3)
        #last_file site lscmd_site
        listfile_site = srv_conn.ExecuteCmd(lscmd_site)
        listfile_site = listfile_site.split('\n')
        listfile_site = listfile_site[-2]
        #last_file loading
        listfile_loading = srv_conn.ExecuteCmd(lscmd_loading)
        listfile_loading = listfile_loading.split('\n')
        listfile_loading = listfile_loading[-2]
        self.logf.info("Esecuzione test terminati")
        self.logf.info("Download file Results in locale")
        scp = SCPClient(srv_conn.GetTransport())
        scp.get(f"{self.Sitespeed_remote}{listfile_site}", f"{self.path_hm_local}")
        scp.get(f"{self.loading_performance_remote}{listfile_loading}", f"{self.path_hm_local}")
        # Copy files locally
        #self.logf.info("Copying result file from modem to the folder orchestrator_in")
    except Exception as err:
        self._print_err(inspect.stack()[0][3], str(err))
    finally:
        if (srv_conn is not None):
            srv_conn.Close()

def make_dict(self, timestamp, speed):
    self.logf.info("Creazione del dizionario")
    res = {}
    two_latest_file_cmd = f"cd {self.path_hm_local} && ls -Art | tail -n 2"
    response = _make_cmd(two_latest_file_cmd)
    list_file_repository = response['cmd_out'].split("\n")[:-1]
    for file in list_file_repository:
        with open (self.path_hm_local + file) as f:
            df = pd.read_csv(f)
            columns_name = df.columns.values.tolist()
            if columns_name[0] == "Browser":
                for name in columns_name:
                    if name != "ipgw" and name != "esn" and name != "siteid" and name != "beam" and name != "outroute_freq" and name!= "Host_Name":
                        new_name = name + "_s"
                        res[new_name] = df.at[0, name]
                    else:
                        new_name = name
                        res[new_name] = df.at[0, name]
            else:
                for name in columns_name:
                    if name != "ipgw" and name != "esn" and name != "siteid" and name != "beam" and name != "outroute_freq" and name != "host_name":
                        new_name = name + "_l"
                        res[new_name] = df.at[0, name]
            file_name = file.split("/")[-1]
            shutil.move(f.name, self.path_hm_local_done)
    res["result_speed_iperf"] = speed
    res["timestamp_iperf"] = timestamp
    print(res)
    return(res)




def _make_cmd(cmd, sys=False):
    '''Execute os cmd'''
    response = {}
    try:
        print(f"Eseguo comando: {cmd}")
        if sys is False:
            cmd_exec = Popen(args=cmd, stdout=PIPE, stderr=PIPE, shell=True)
            out_err = cmd_exec.communicate()
            cmd_out = str(out_err[0])[2:-1].replace("\\t", "\t").replace("\\n", "\n").replace("\\r", "\r")
            cmd_err = str(out_err[1])[2:-1].replace("\\t", "\t").replace("\\n", "\n").replace("\\r", "\r")
            print(f"Return Code: {cmd_exec.returncode}")
            if cmd_err == "" and cmd_exec.returncode != 0:
                cmd_err = cmd_out
            if cmd_exec.returncode == 0:
                cmd_err = ""
            response = {'return_code': cmd_exec.returncode, 'cmd_out': cmd_out, 'cmd_err': cmd_err}
        else:
            os.system(cmd)
    except Exception as e:
        print(f"make_cmd :{e}")
        response = {'return_code': -1, 'cmd_out': '', 'cmd_err': str(e)}
    return response


if __name__ == "__main__":
    """ Questo modulo si occupa di lanciare un test iperf salvando i dati 
        successivamente si collega alle macchine tramite  ip di mgmt lancia il job 
        per eseguire il test singolo  sia site/loading scaricnado i dati 
        nell'ultime fase unisce i dizionari delle esecuzioningole con i dati dell'iper
        caricando sul dB
    """
    cls = iperf_vs_web()

    if(cls.LError == []):
        cls.main()
        print("INFO iperf_vs_web Complete")
    else:
        print(*cls.LError, sep="\n")
