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
from dotenv import load_dotenv, main
import os
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
#from systemd.journal import JournalHandler

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
#           'temperature', ['scale'])

class InfluxClient:
    def __init__(self,token,org,bucket,url): 
        self._org=org 
        self._bucket = bucket
        self._client = InfluxDBClient(url=url, token=token)

def write_data(self,data,write_option=SYNCHRONOUS):
    write_api = self._client.write_api(write_option)
    write_api.write(self._bucket, self._org , data,write_precision='s')

def readvalue(i,ZyklenAlt,client,dfact):
    try:
        df = pd.DataFrame()

        program = ""

        urls = ['http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=day1&to=day2&maid=1695205705851135']

        today = date.today()
        day1 = today.strftime("%Y%m%d")
        day2 = day1
        
        for url in urls:
            url = url.replace('day1',day1)
            url = url.replace('day2',day2)
            #print(url)
            #df = df.append(selenium_import2.get_data_from_url(url,program))
            df = pd.concat([df, selenium_import2.get_data_from_url(url,program)])
        #print(df)

        #dftemp = sensorREST(df)
        #df = dftemp
        Zyklen = df.iat[-1,3]
        pmax = df.iat[-1,12]
        print("Zyklen: ", Zyklen," pmax: ",pmax)
        #idbstring = "test,Zyklen="+str(Zyklen)+" Pmax="+str(pmax)
        '''data = []
        data.append("{measurement},Cycles={Zyklen},Pmax={pmax}"
                .format(measurement="test",
                        Zyklen=df.iat[-1,3],
                        pmax=df.iat[-1,12]))'''

        '''#if Zyklen > ZyklenAlt:
            #g_cyc.set(Zyklen)
            #g_pmax.set(pmax)

            #client.write_data([idbstring])'''
        
        #print(df.columns)

        #print(df.datetime)
        #print(df.index_y)

        #for ind in df.index:
            #if df['minute'][ind] == df['minute'][ind+1]:
            #print(df['datetime'][ind])
                #df['datetime'][ind+3] = df['datetime'][ind+3] + df['Zykluszeit'][ind]
            #if ind+3 == len(df):
             #   break

        #print(df.datetime)
        if not dfact.empty:
            dfnew1 = pd.concat([df,dfact])
            dfnew2 = dfnew1.drop_duplicates(keep=False)
            dfnew2.set_index("datetime")
            client.write_data(dfnew2)
            print(dfnew2)
        else:
            print(df)

        #print(df)

        return df

    except RuntimeError as e:
        # GPIO access may require sudo permissions
        # Other RuntimeError exceptions may occur, but
        # are common.  Just try again.
        #log.error("RuntimeError: {}".format(e))
        print(e)
    
if __name__ == "__main__":
    # Expose metrics
    metrics_port = 8001
    start_http_server(metrics_port)
    print("Serving sensor metrics on :{}".format(metrics_port))
    #log.info("Serving sensor metrics on :{}".format(metrics_port))

    load_dotenv()
    # You can generate a Token from the "Tokens Tab" in the UI

    token = os.getenv('TOKEN')
    org = os.getenv('ORG')
    bucket = os.getenv('BUCKET')
    url = "http://172.21.135.18:8086"

    #IC = InfluxClient(token,org,bucket,url)

    client = InfluxDBClient(url=url, token=token)
    
    i = 0
    ZyklenAlt = 0
    dfact = pd.DataFrame()

    while True:
        dfact = readvalue(i,ZyklenAlt,client,dfact)
        #print(dfact)
        time.sleep(15)

    time.sleep(1)