import pandas as pd
import requests,pprint
pd.set_option("display.width",5000)
pd.set_option("display.max_rows",100)
pd.set_option("display.max_columns",1000)

class Nse():

    def __init__(self) -> None:
        self.__headers = {'User-Agent' :'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'}
        self.__session = requests.session()
        self.__session.get("https://www.nseindia.com",headers = self.__headers)
        self.read_PramFile()

    def read_PramFile(self) -> dict:

        sheet_name = "1yoQZNPwdYoQte13GKAsEzQ4WkABx77usQiSW2O-Hj-I"
        df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_name}/export?format=csv")

        self.dict_conxt = {}

        for script, is_Active in zip(list(df['Script_Name']),list(df['Is_Active'])) : self.dict_conxt[f'{script}'] = is_Active

        return self.dict_conxt 

    def get_DeliveryData(self):

        for key, val in self.dict_conxt.items():
            if val==1:
                
                URL=f"https://www.nseindia.com/api/historical/securityArchives?from=30-06-2023&to=30-12-2023&symbol={key}&dataType=priceVolumeDeliverable&series=ALL"

                response = self.__session.get(URL, headers = self.__headers)

                print("Acceped..!") if response.status_code == 200 else print(f"Response returned a status code {response.status_code}")

                if response.status_code == 200:

                    df = pd.DataFrame(response.json()["data"])
                    dt = df.drop(columns=['_id','CH_SERIES','CH_MARKET_TYPE','CH_ISIN','TIMESTAMP','createdAt','updatedAt','__v','VWAP','mTIMESTAMP'])

                    cols = dt.columns

                    if 'CA' in cols:
                        dt = dt.set_index('CA',drop=True)

                    file_name = response.json()["data"][0]['CH_SYMBOL']
                    print(f"File Created for: {file_name}")

                    dt.to_csv(f"D:\Databricks/stock_csv/{file_name}.csv",index=False)

if __name__ == '__main__':

    nse = Nse()

    nse.get_DeliveryData()