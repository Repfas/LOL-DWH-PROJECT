import luigi 
import logging 
import pandas as pd 
import time 
import sqlalchemy
from datetime import datetime 
from sqlalchemy.orm import sessionmaker 
import os 
from pipeline.extract import Extract
from pipeline.utils.conn_db import db_connection 
from pipeline.utils.read_sql_file import read_sql_file
from dotenv import load_dotenv

load_dotenv()

DIR_ROOT_PROJECT =os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_EXTRACT_QUERY = os.getenv("DIR_EXTRACT_QUERY")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOG = os.getenv("DIR_LOG")
DIR_SOURCE_DATA = os.getenv('DIR_SOURCE_DATA')
DIR_LOAD_QUERY = os.getenv('DIR_LOAD_QUERY')

class Load(luigi.Task):
    def requires(self):
        return Extract()
    
    def run(self):
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')

        try:
            # Read query to truncate dvdrental schema in dwh
            truncate_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/lol-truncate-table.sql'
            )
            
            
            logging.info("Read Load Query - SUCCESS")
            
        except Exception:
            logging.error("Read Load Query - FAILED")
            raise Exception("Failed to read Load Query")
        
        # read data to be load 
        try:
            # ✅ PERBAIKAN TOTAL - Sesuai rename_map
            match_data = pd.read_csv(self.input()[0].path)  
            ban_data = pd.read_csv(self.input()[1].path)  
            death_data = pd.read_csv(self.input()[2].path)    
            champ = pd.read_csv(self.input()[3].path)       
            role = pd.read_csv(self.input()[4].path)        
            logging.info(f"Read Extracted Data - SUCCESS")

        except Exception as e:
            logging.info(f'Read Extracted Data  - FAILED')
            raise Exception("Failed to Read Extracted Data")
        
        # connect to dwh
        try:
            dwh_engine = db_connection()
            logging.info(f'Success connect to dwh')
        
        except:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        # Truncate all tables before load
        # This puropose to avoid errors because duplicate key value violates unique constraint
        try:
            truncate_query = truncate_query.split(';')
            # create session 
            truncate_query = [query.strip() for query in truncate_query if query.strip()]
            Session = sessionmaker(bind = dwh_engine)
            session = Session()
            # excecute query
            for query in truncate_query:
                query = sqlalchemy.text(query)
                session.execute(query)

            session.commit()

            session.close()
        except Exception:
            logging.error(f"Truncate raw Schema in DWH - FAILED")
            
            raise Exception("Failed to Truncate lol Schema in DWH")
        
        #  Record start time for loading tables
        start_time = time.time()  
        logging.info('lOAD DATA START:')

        try:
            
            try:
                # Load match_data tables    
                match_data.to_sql('match_data', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'raw')
                logging.info(f"LOAD 'raw.match_data' - SUCCESS")
                
                # Load champ tables
                champ.to_sql('champ', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'raw')
                logging.info(f"LOAD 'raw.champ' - SUCCESS")
                
                # Load death_data tables
                death_data.to_sql('death_data', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'raw')
                logging.info(f"LOAD 'raw.death_data' - SUCCESS")
                
                
                # Load ban_data tables
                ban_data.to_sql('ban_data', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'raw')
                logging.info(f"LOAD 'raw.ban_data' - SUCCESS")

                role.to_sql('role', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'raw')
                logging.info(f"LOAD 'raw.role' - SUCCESS")
                
                
                logging.info(f"LOAD All Tables To DWH-raw - SUCCESS")
                
            except Exception:
                logging.error(f"LOAD All Tables To DWH-raw - FAILED")
                raise Exception('Failed Load Tables To DWH-raw')        
        
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
        except Exception:
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
            logging.error("LOAD All Tables To DWH - FAILED")
            raise Exception('Failed Load Tables To DWH')  
        
        logging.info('END LOAD') 
    

    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/load-summary.csv')]