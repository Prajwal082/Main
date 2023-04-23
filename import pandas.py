class Stockmarket:

    def __init__(self, api_key,client_ID,password,totp) -> None:
        from smartapi import SmartConnect
        import pandas as pd
        import pyotp
        

        self.__api_key=api_key
        self.client_ID=client_ID
        self.password=password
        self.totp=totp

        obj =SmartConnect(api_key=self.__api_key)
        data=obj.generateSession(self.client_ID,self.password,pyotp.TOTP(self.totp).now())
        refreshToken= data['data']['refreshToken']
        feedToken=obj.getfeedToken()
        print(feedToken)
        # self.getdata()

        try:
            historicParam={
            "exchange": "NSE",
            "symboltoken": "3045",
            "interval": "ONE_DAY",
            "fromdate": "2023-02-08 00:00", 
            "todate": "2023-04-21 00:00"
            }
            srcdata = obj.getCandleData(historicParam)
            columns = ['TimeStamp','Open','High','Low','Close','Volume']
            df=pd.DataFrame(srcdata['data'],columns=columns)
            self.df=df
            self.transform()
        except Exception as e:
            print("Historic Api failed: {}".format(e))

    def transform(self):
        self.Stock='SBI'
        self.df['Stock'] = self.Stock
        self.df=self.df[['Stock','Open','High','Low','Close','Volume','TimeStamp']]
        # print(self.df)
        for index,row in self.df:
            print(index)

    def sqlsetup(self):
        import pyodbc
        cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};"
                      "Server=DESKTOP-0A2HT13;"
                      "Database=Databricks;"
                      "UID=prajwal;"
                      "PWD=Prajwal082;"
                      "Trusted_Connection=yes;")
        cursor = cnxn.cursor()
        # for index,row in self.df:
        #     print(index,row)



stock=Stockmarket('tEUW4a6X','P381661','4812','QZYS2YLQQLUBXTXVS3S5WCBQBA')


