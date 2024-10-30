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

def readvalue(i,ZyklenAlt):
    try:
        df = pd.DataFrame()

        program = ""

        urls = ['http://172.21.131.9/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=20241030&to=20241030&maid=1695205705851135']

        for url in urls:
            #df = df.append(selenium_import2.get_data_from_url(url,program))
            df = pd.concat([df, selenium_import2.get_data_from_url(url,program)])
        #print(df)

        dftemp = sensorREST(df)
        df = dftemp
        Zyklen = df.iat[-1,3]
        print("Zyklen: ", Zyklen)

        if Zyklen > ZyklenAlt:
            g_cyc.set(Zyklen)
            g_pmax.set(df.iat[-1,12])


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
    
    i = 0
    ZyklenAlt = 0

    while True:
        readvalue(i,ZyklenAlt)
        time.sleep(15)

    time.sleep(1)