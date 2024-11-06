import sys
import os
import time
import numpy as np
import pandas as pd
from als_import import webimport
from sensorREST import sensorREST
import random
from icecream import ic
import selenium_import2
from prometheus_client import Gauge, start_http_server
from datetime import datetime
from datetime import date
import pytz
from dotenv import load_dotenv, main
import os
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

timezone = pytz.timezone('Europe/Vienna')
'''#from systemd.journal import JournalHandler

# Setup logging to the Systemd Journal
#log = logging.getLogger('A370A')
#log.addHandler(journal.JournaldLogHandler())
#log.addHandler(JournalHandler())
#log.setLevel(logging.INFO)

# The time in seconds between sensor reads
READ_INTERVAL = 60.0
# Create Prometheus gauges for humidity and temperature in
# Celsius and Fahrenheit
g_pmax = Gauge('maximaler_Spritzdruck',
           'maximaler_Spritzdruck')

g_cyc = Gauge('Zyklen',
           'Zyklen')
#gt = Gauge('temperature',
#           'temperature', ['scale'])'''

def readvalue(i,ZyklenAlt,dfact,urldb,token,org,bucket):
    try:
        df = pd.DataFrame()

        program = ""

        #urls = ['http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1695205705851135']
        urls = {
            "EN01-650": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1513755320190225",
            "EN02-120": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1513755359594227",
            "A01-175": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1510669191629004",
            "A02-420C": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1510669408795006",
            "A03-470C": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1510668999539002",
            "A04-370S": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1510669798061008",
            "A05-520S": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1517298167077055",
            "A06-720S": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1517298229450057",
            "A07-375V": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1559632182014003",
            "A08-470S": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1585570262001631",
            "A09-370A": "http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1695205705851135"
            }

        today = date.today()
        day1 = today.strftime("%Y%m%d")
        day2 = day1
        
        #for url in urls:
        url = urls.get(bucket)
        url = url.replace('day1',day1)
        url = url.replace('day2',day2)
        print(url)
        df = pd.concat([df, selenium_import2.get_data_from_url(url,program)])

        Zyklen = df.iat[-1,3]
        pmax = df.iat[-1,12]
        print("Zyklen: ", Zyklen," pmax: ",pmax)

        for ind in df.index:
            if ind > 1:
                if df.loc[ind, 'datetime'] == df.loc[ind - 1, 'datetime']:
                    df.loc[ind, 'datetime'] = df.loc[ind, 'datetime'] + pd.Timedelta(df.loc[ind, 'Zykluszeit'], "seconds")
                elif df.loc[ind, 'datetime'] < df.loc[ind - 1, 'datetime']:
                    df.loc[ind, 'datetime'] = df.loc[ind - 1, 'datetime'] + pd.Timedelta(df.loc[ind, 'Zykluszeit'], "seconds")
                if ind == df.index[-1]:
                    break

        #print(df.datetime)
        if not dfact.empty:
            #Vergleich des neuen mit dem vorherigen df, löschen aller vorherigen Zeilen
            dfnew1 = pd.concat([df,dfact])
            dfnew2 = dfnew1.drop_duplicates(keep=False)

            #Definition eines neuen Index basierend auf Datum und Zeit
            format = '%Y-%m-%d %H:%M:%S.%f'
            dfnew2['datetime'] = dfnew2['datetime'].astype(str)
            #Formatierung um einen Index aus datetime bilden zu können
            dfnew2['Datetime'] = pd.to_datetime(dfnew2["datetime"], format=format)
            dfnew2 = dfnew2.set_index(pd.DatetimeIndex(dfnew2["Datetime"]))
            #Lokalisierung auf AT, da InfluxDB alles als UTC Zeit abspeichert
            dfnew2.index = dfnew2.index.tz_localize('Europe/Vienna')
            dfnew2.index = dfnew2.index.tz_convert('UTC')
            dfnew2 = dfnew2.drop(["datetime"], axis=1)
            
            #Schreibt den df in InfluxDB
            with InfluxDBClient(url=urldb, token=token, org=org) as client:
                with client.write_api(write_options=SYNCHRONOUS) as write_api:
                    write_api.write(bucket=bucket, record=dfnew2, data_frame_measurement_name='demo')

            print(dfnew2)
        else:
            print(df)

        return df

    except RuntimeError as e:
        print(e)
    
if __name__ == "__main__":

    #Liest den Bucketnamen aus dem Input beim Scriptstart ein über ALSplot.py Input
    bucket = str(sys.argv[1])
    
    load_dotenv()
    # You can generate a Token from the "Tokens Tab" in the UI

    token = os.getenv('TOKEN')
    org = os.getenv('ORG')
    #bucket = os.getenv('BUCKET')
    urldb = "http://172.21.135.18:8086"
   
    i = 0
    ZyklenAlt = 0
    dfact = pd.DataFrame()

    while True:
        dfact = readvalue(i,ZyklenAlt,dfact,urldb,token,org,bucket)
        #print(dfact)
        time.sleep(30)

    time.sleep(1)