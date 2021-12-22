import pandas as pd
import re
class TableMeteoExtractor():
    def __init__(self,url16Nov,url15Nov):
        self.data16Nov2021=None
        self.data15Nov2021=None
        self.url16Nov=url16Nov
        self.url15Nov=url15Nov
        self.columns=["hour","temperature","humidity"]
    
    def __setColumsData__(self,row):
        _humidity=row["humidity"]
        _temperature=row["temperature"]
        _hour=row["hour"]
        _rHour=re.findall("(\d+) h",str(_hour))
        _rTemperatur=re.findall("(\d*\.?\d*) °C",str(_temperature))
        _rHumidity=re.findall("(\d*\.?\d*)%",str(_humidity))
        row["humidity"]=_rHumidity[0]
        row["temperature"]=_rTemperatur[0]
        row["hour"]=_rHour[0]
        return row 

    def __setDataValues__(self,meteoDay, url):
        dfs = pd.read_html(url)
        df = dfs[4]
        _masque=df[0].str.contains('^\d+ h', na=False)
        df=df[_masque]
        df=df[[0,4,5]]
        df.columns=self.columns
        df = df.apply(lambda row: self.__setColumsData__(row), axis=1)
        df.to_csv("./{}.csv".format(meteoDay),header= self.columns , index=False)
        return df

    def run(self,meteo15Nov="outside_meteo_nov_15_2021",meteo16Nov="outside_meteo_nov_16_2021"):
        self.data15Nov2021= self.__setDataValues__(meteoDay=meteo15Nov, url=self.url15Nov)
        self.data16Nov2021= self.__setDataValues__(meteoDay=meteo16Nov ,url=self.url16Nov )
            
       
        
                