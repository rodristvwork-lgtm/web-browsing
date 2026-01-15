import os
import dropbox
from dropbox.files import WriteMode
import config
from typing import Dict, List
import shutil
import logging

def export_data() -> None:
    """upload the data in dropbox"""
    try:
        file_to_upload: List[str]
        file_to_upload_firstime: List[str]
        list_file_drop: List[str] = []
        dbx = dropbox.Dropbox(config.settings["token_dropbox"])
        path_results = config.settings['results_file_name']
        file_name_to_send =config.settings['results_file_name'].split('/')[-1]
        with open(path_results, "rb") as file_results:
            testo = file_results.read()
            dbx.files_upload(
            testo, 
            f"/loading_performance/{file_name_to_send}")
        shutil.move(path_results,f"{config.settings['folder_result_done']}/{file_name_to_send}")
        logging.info("export done")
    except Exception:
            logging.error(f"an exception occured while export on dropbox file : {file_name_to_send}")



if __name__ == "__main__":
    export_data()
