import luigi 
from datetime import datetime 
import logging 
import time 
import pandas as pd 
import os 
from dotenv import load_dotenv

load_dotenv()

# Define Directory 
DIR_ROOT_PROJECT =os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_EXTRACT_QUERY = os.getenv("DIR_EXTRACT_QUERY")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOG = os.getenv("DIR_LOG")
DIR_SOURCE_DATA = os.getenv('DIR_SOURCE_DATA')

class Extract(luigi.Task):
    rename_map = {
        "LeagueofLegends.csv": "raw.match_data.csv",
        "banValues.csv": "raw.ban_data.csv",
        "deathValues.csv": "raw.death_data.csv",
        "namafile_utf8.csv":"raw.champ.csv",
        'role.csv':'raw.role.csv'
    }
    def requires(self):
        pass 

    def run(self):
        try:
            # config logging
            logging.basicConfig(filename=f'{DIR_TEMP_LOG}/logs.log',
                                level= logging.INFO,
                                format= '%(asctime)s - %(levelname)s - %(message)s') 
            # define db_conn engine
            start_time = time.time()
            logging.info("EXTRACT DATA START:")
            for src_name, dest_name in self.rename_map.items():
                try:
                    src_path = os.path.join(DIR_SOURCE_DATA,src_name)
                    dest_path = os.path.join(DIR_TEMP_DATA,dest_name )
                    if not os.path.exists(src_path):
                        logging.error(f"File {src_path} not found.")
                        raise FileNotFoundError(src_path)
                    
                    df = pd.read_csv(src_path, sep=",", encoding='utf-8')
                    df.to_csv(dest_path, index=False)


                
                except Exception as e:
                    logging.error(f"Extract FAILED: {e}")
                    raise e 
            end_time = time.time()
            elapsed = round(end_time - start_time, 2)
            logging.info(f"Local Extract SUCCESS in {elapsed}s")



        except Exception as e:
            print(e)

    def output(self):
        return[luigi.LocalTarget(os.path.join(DIR_TEMP_DATA, new_name))
                for new_name in self.rename_map.values()]