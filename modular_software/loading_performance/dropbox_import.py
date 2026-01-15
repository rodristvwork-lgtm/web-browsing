"""Import file and write to kafka"""
import dropbox
import config
import csv
import logging

def _connection_dropbox_via_proxy() -> dropbox.Dropbox:
    """ Static method to get proxy
        Connection to Dropbox
    """
    try:
        proxy = 'proxy.lan:3128'
        http_proxy = "http://" + proxy
        https_proxy = "https://" + proxy
        #ftp_proxy = "ftp://" + proxy

        proxyDict = {
            "http": http_proxy,
            "https": http_proxy
            #"ftp": ftp_proxy
        }
        access_token = config.settings["token_dropbox"]
        mysesh = dropbox.create_session(1,proxyDict)
        dbx = dropbox.Dropbox(access_token,session=mysesh)
        
        return dbx
    except Exception:
        logging.error("connection_dropbox failed")


# Riferimento Kafka
def _write_kafka(data: dict) -> None:
    """
    kafka_key_id = 0
    kafka_field = [item for item in d1.keys()]
    kafka_values = [item for item in d1.values()]
    kafka_types = [type(item) for item in d1.values()]
    kafka_values = [str(item) for item in d1.values()]
    #kafka_=l
    message='%s,%s,%s,%s' %(kafka_key_id,kafka_field,kafka_values,kafka_types)
    topic: str = config.settings["kafka"]["topic"]
    server_list: list = config.settings["kafka"]["server"]
    config.log.info(f"writing kafka message to topic {topic}")
    kafka_lib.kafka_producer(message, topic, server_list)
    config.log.info(f"message written successfully for step {step_key}")
    """
    return None

def _create_dictionary_from_dbx(dbx :dropbox.Dropbox) -> None:
    """Create the dictionary with all the data on dropbox """
 
    entries = dbx.files_list_folder('/loading_performance').entries
    for entri in entries:
        print(entri.path_display)
        metadata, res = dbx.files_download(path=entri.path_display)
        data = res.content
        testdata = data.decode('utf-8').splitlines()
        reader = csv.DictReader(testdata)
        for row in reader:
            pass
            #passare il dizionario alla funzione _write_kafka()
            #print(row['website'])

def _delete_file_from_dbx(file_to_delete:str,dbx :dropbox.Dropbox)-> None:
       dbx.files_delete_v2(path=file_to_delete)


def init() -> None:
    dbx = _connection_dropbox_via_proxy()
    #_create_dictionary_from_dbx(dbx)
    _delete_file_from_dbx("/loading_performance/noleggioConsultaDettaglio_Mule4.json",dbx)
    

if __name__ == "__main__":
    init()
