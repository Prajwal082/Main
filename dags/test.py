"""
test
DAG auto-generated by Astro Cloud IDE.
"""

from airflow.decorators import dag
from astro import sql as aql
import pandas as pd
import pendulum


@aql.dataframe(task_id="python_1")
def python_1_func():
    import pandas as pd
    import requests,pprint
    from datetime import date,timedelta
    
    pd.set_option("display.width",5000)
    pd.set_option("display.max_rows",100)
    pd.set_option("display.max_columns",1000)
    
    class Nse():
    
        def __init__(self) -> None:
            self.__headers = {'User-Agent' :'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'}
            self.__session = requests.session()
            self.__session.get("https://www.nseindia.com",headers = self.__headers)
            self.read_PramFile()
    
            old_date = date.today() - timedelta(days=90)
            self.old_date = old_date.strftime("%d-%m-%Y")
            self.today_date = date.today().strftime('%d-%m-%Y')
    
        def read_PramFile(self) -> dict:
    
            sheet_name = "1yoQZNPwdYoQte13GKAsEzQ4WkABx77usQiSW2O-Hj-I"
            df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_name}/export?format=csv")
    
            self.dict_conxt = {}
    
            for script, is_Active in zip(list(df['Script_Name']),list(df['Is_Active'])) : 
                self.dict_conxt[f'{script}'] = is_Active
    
            return self.dict_conxt 
    
        def get_DeliveryData(self):
    
            for key, val in self.dict_conxt.items():
                if val==1:
                    
                    URL=f"https://www.nseindia.com/api/historical/securityArchives?from={self.old_date}&to={self.today_date}&symbol={key}&dataType=priceVolumeDeliverable&series=ALL"
    
                    response = self.__session.get(URL, headers = self.__headers)
    
                    if response.status_code == 200:
                        print("Acceped..!")
    
                        df = pd.DataFrame(response.json()["data"])
                        dt = df.drop(columns=['_id','CH_SERIES','CH_MARKET_TYPE','CH_ISIN','TIMESTAMP','createdAt','updatedAt','__v','VWAP','mTIMESTAMP'])
    
                        cols = dt.columns
    
                        if 'CA' in cols:
                            dt = dt.set_index('CA',drop=True)
    
                        file_name = response.json()["data"][0]['CH_SYMBOL']
                        print(f"File Created for: {file_name}")
    
                        # dt.to_csv(f"/opt/airflow/src/{file_name}.csv",index=False)
                        dt.to_csv(f"D:\Databricks\Vsprojects\gitclone\SparkingFlow\src\{file_name}.csv",index=False)
                    else:
                        print(f"Response returned a status code {response.status_code}")
    
                        raise ConnectionRefusedError
                    
    
    if __name__ == '__main__':
    
        nse = Nse()
    
        nse.get_DeliveryData()

default_args={
    "owner": "stevedz082@gmail.com,Open in Cloud IDE",
}

@dag(
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=pendulum.from_format("2024-02-25", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "stevedz082@gmail.com": "mailto:stevedz082@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/clt16dtux4dtn01n736enk148/cloud-ide/clt18z3bn4due01n7bp4xcly5/clt1f8hea4d7h01m19wocjs5o",
    },
)
def test():
    python_1 = python_1_func()

dag_obj = test()