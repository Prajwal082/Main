import logging
import json
import pandas as pd
from sqlalchemy import create_engine

class Utils():

    def __init__(self) -> None:

        self.Log_format='[%(asctime)s] : [%(lineno)s] : [%(levelname)s] : %(message)s'
        self.level = logging.INFO
        logging.basicConfig(level=self.level,format=self.Log_format)

        self.Logger = logging.getLogger("py4j")
        self.Logger.info("Logging setup done....!")

        self.initconif()


    def initconif(self) -> dict:
        f = open("D:\Databricks\Vsprojects\library\constants.json")
        self.config_data = json.load(f)

        return self.config_data


    def get_leafconfig(self,json:dict, code:str ) -> str:
        '''
            This Function will recursively fetch the value 
            by passing the appropriate key for the given input JSON/dict 
        '''
        code_list = code.split('.')
        
        if code_list[0] in json.keys():
            json = json[f'{code_list[0]}']
        else:
            raise KeyError("Invalid Key...!")

        if len(code_list)==1:
            return json
        
        code_list = '.'.join(code_list[1:])

        return self.get_leafconfig(json,code_list)
    
    def truncate_table_sql_server(self,table_name:str,pyodbc:str) ->None:

        # Replace with your SQL Server connection details
        engine = create_engine(pyodbc)

        # Connect to the database
        connection = engine.raw_connection()

        cursor = connection.cursor()
        

        try:
            # Execute the stored procedure to truncate the table
            cursor.execute(f"EXEC spTruncateTable  @tableName= {table_name}")
            connection.commit()

        finally:
            # Close the database connection
            connection.close()