import argparse
import inspect
import os
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
from Iperf3Test_FreeTest import IperfTest
from distutils.util import strtobool

# private
from python_connectors.Helper.Config import config
from python_connectors.Helper.Logger_P36 import SKLlogging
from python_connectors.SKl_Connector_P37.ContainerAPI import Readers


class api_iperf_test():
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
            # ---------------- Logs --------------------------------------------------------------------
            self.homeLogs = params['log']
            # if no path has been defined, the local path is used
            if('/' not in self.homeLogs[-1]):
                self.homeLogs = self.abspath + self.homeLogs
            # shares the logs folder --> used by Error.py
            os.environ["logs"] = self.homeLogs 
            # Log
            self.logf = SKLlogging('api_iperf_test', 'BSO_0000', 'I', self.homeLogs)
            # ---------------- Repository --------------------------------------------------------------------

            self.homeRepository = params['repository']

            # if no path has been defined, the local path is used
            if('/' not in self.homeRepository[-1]):
                self.homeRepository = self.abspath + self.homeRepository + "/plots/iperf"
            
            # inject the Connector Service Class
            self.resr = Readers.callResources2230()
            self.resr._client.flgHost = False

            #args
            p = argparse.ArgumentParser()
            p.add_argument("-v", "--environment", help="Set environment: 'lab','prod'", type=str, required=False)
            p.add_argument('-sv', '--sandvine', help="Sandvine: 2='PRE' (default), 2='POST'", default=2, type=int, required=False)
            p.add_argument("-e", "--esn", help="esn list", nargs='+', type=int, required=False)
            p.add_argument("-st", "--siteId", help="SiteID list", nargs='+', type=str, required=False)
            p.add_argument("-vn", "--vno", help="Vno list to accept", nargs='+', type=str, required=False)
            p.add_argument('-nsd', '--nominalSpeedDownRateKbps', help='nominalSpeedDownRateKbps list', nargs='+', type=int, required=False)
            p.add_argument('-nsu', '--nominalSpeedUpRateKbps', help='nominalSpeedUpRateKbps list', nargs='+', type=int, required=False)
            p.add_argument("-csv", "--filecsv", help="full path file csv", type=str, required=False)
            p.add_argument('-dr', '--duration', help='iperf3 duration', type=int, required=False)
            p.add_argument('-fp', '--fullprint', help="Print all data recived from Server", action='store_true')
            #in order to have correct charts listtest must have only one element
            p.add_argument("-lt", "--listtest", help="List of test: DTCP, DUDP, UTCP, UUDP (default All)", nargs='+', type=str, required=False)
            p.add_argument('-mt', '--maxtime', help='maximum execution time', type=int, required=False)
            p.add_argument("-pt", "--port", help="starting value for the ports used in the tests (range from 5052 to 5101)", type=int, required=False)
            p.add_argument("-rt", "--rate", help="value used in TCP tests (Mbps)", type=int, required=False)
            p.add_argument("-nv", "--newversion", help="New software version", type=lambda x: bool(strtobool(x)), nargs='+')
            p.add_argument("-la", "--label", help="chart title, if empty there will be a default name", type=str, required=False)
            self.args = p.parse_args()



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
        try:
            for test in self.args.listtest:
                self.logf.info(f"Iperf test execution: {test}")
                cls_iperf = IperfTest(self.args.sandvine, self.args.environment, self.args.fullprint, self.args.maxtime, [test], self.args.rate)
                cls_iperf.main(self.args.esn, self.args.siteId, self.args.vno, self.args.filecsv, self.args.nominalSpeedDownRateKbps, self.args.nominalSpeedUpRateKbps, self.args.duration, self.args.newversion,self.args.port)
                self.logf.info("Iperf test done")
                if self.args.filecsv: #retrives siteID and check if sw version is new
                    df = pd.read_csv(self.args.filecsv)
                    site_id_list = df["siteId"]
                    new_version_list = df["newversion"]
                else:
                    site_id_list = self.args.siteId
                    new_version_list = self.args.newversion
                i = 0
                res_list = []
                date_list = []
                for siteID in site_id_list:
                    if new_version_list[i] == True:
                        res, date = get_test_results(self, test, siteID)
                        res_list.append(res)
                        date_list.append(date)
                        i +=1
                make_charts(self,res_list, date_list, site_id_list, test)

        except Exception as err:
            self._print_err(inspect.stack()[0][3], str(err))
            print(err)


def check_results(self, siteID, date):
    "check if last test results are already existing"
    with open(self.homeRepository + "/test_already_done.txt", "r") as f:
        if siteID + " " + date + "\n" in f.read():
            return False
        else:
            return True


def get_data_from_output_text(self, res):
    try:
        rate_list = []
        values = res["result"]["server_output_text"].splitlines() 
        for val in values[:-2]:
            if ('sec' not in val) or ('omitted' in val):
                continue 
            rate_value = val.split(" ")
            if "Mbits/sec" in rate_value:
                num = rate_value.index("Mbits/sec")
                rate_list.append(float(rate_value[num-1]))
            elif "Kbits/sec" in rate_value:
                num = rate_value.index("Kbits/sec")
                rate_list.append(float(rate_value[num-1])/1000)
        
        return rate_list
    except Exception as err:
        self._print_err(inspect.stack()[0][3], str(err))
        print(err)


def get_test_results(self, test_type, siteID):
    try:
        ut_cmd_ret = f"http://{siteID}.terminal.jupiter.konnect.net/api/network/iperf3?cmd=result"
        proxies = {'proxies': {'http': 'http://proxy2.lan:8080'}}
        res = self.resr.GetResource(call=ut_cmd_ret, params=None, args=proxies)
        #extract timestamp from dict and process it
        if "error" in res["result"]:
            err = res["result"]["error"]
            self.logf.error(f"siteId: {siteID}, {err}")
        if res["result"]["intervals"] != []:
            date = datetime.strptime(res["result"]["start"]["timestamp"]["time"].split(", ")[1].split(" GMT")[0], '%d %b %Y %H:%M:%S')
            date = str(date).replace(" ","_")
            flag = check_results(self,siteID,date) #check is last test is started 
            if flag == False:
                self.logf.error(f"chart already saved, probably test in {siteID} is not started")
            else:

                if test_type == "UUDP" or test_type == "UTCP":  #get data from server output text
                    rate = get_data_from_output_text(self,res)
                else:
                    data = res["result"]["intervals"][8:] #first 8 values are omitted
                    rate = []
                    for el in data:
                        rate.append(el["streams"][0]["bits_per_second"]/1000000)
                mean = sum(rate) / len(rate)
                print("media: " + str(mean))
                #save siteID and timestamp in a txt file
                with open(self.homeRepository + "/test_already_done.txt", "a") as f:
                    siteID_timestamp = siteID + " " + date + "\n"
                    f.write(siteID_timestamp)
                return rate,date

    except Exception as err:
        self._print_err(inspect.stack()[0][3], str(err))
        print(err)

"""
                x=range(len(rate))
                fig1  = plt.figure(dpi=100)
                plt.plot(x,rate)
                plt.title(name_fig)
                plt.ylabel("Mbps")
                plt.xlabel("Time (s)")
                plt.grid()
        #        plt.show()
                plt.savefig(path_img)
                self.logf.info(f"chart saved correctly in repository with filename: {name_fig}")
                return rate
"""
def make_charts(self, res_list, date_list, site_id_list, test):
    try:
        if self.args.label == "":
            name_fig= f"api" + "_" + date_list[0] + "_" + test #name pdf file
        else:
            name_fig = self.args.label + "_" + date_list[0] + "_" + test
        y_min = 9/10 * min(map(min, res_list))
        y_max = 11/10 * max(map(max, res_list))
        if len(res_list) == 1:
            x=range(len(res_list[0]))
            plt.figure(dpi=200)
            plt.plot(x,res_list[0])
            plt.title(name_fig + "_" + site_id_list[0])
            plt.ylabel("Rate Mbps", fontsize=22)
            plt.xlabel("Time(s)", fontsize=22)
            plt.grid()
            plt.tick_params(axis='both', labelsize = 16)
            plt.ylim((y_min, y_max))
        else: 
            fig1, axs = plt.subplots(len(res_list), sharex=True, figsize=(20, 20) , dpi=200)
            plt.suptitle(name_fig, fontsize=30)
            plt.subplots_adjust(top=0.88)
            count = 0
            for el in res_list:
                axs[count].plot(el, label=f"{el}")
                axs[count].grid()
                axs[count].set_title(site_id_list[count], fontsize=22)
                axs[count].tick_params(axis ='both', labelsize = 16)
                axs[count].set_ylabel('Rate Mbps', fontsize=22)
                axs[count].set_xlabel('Time(s)', fontsize=22)
                axs[count].set_ylim([y_min, y_max])
                count +=1
#        axs[1].set_ylabel('Rate Mbps', fontsize=22)
#        plt.xlabel("Time(s)", fontsize=22)
        plt.tight_layout()
        plt.savefig(self.homeRepository + f"/{name_fig}.pdf")
        self.logf.info(f"charts saved in repository, file name: {name_fig}")

    except Exception as err:
        self._print_err(inspect.stack()[0][3], str(err))
        print(err)





if __name__ == "__main__":
    """ This script launches an Iperf test and makes a linechart with results
    """
    cls = api_iperf_test()

    if(cls.LError == []):
        cls.main()
        print("INFO iperf_vs_web Complete")
    else:
        print(*cls.LError, sep="\n")



