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
from systemd.journal import JournaldLogHandler

# Setup logging to the Systemd Journal
log = logging.getLogger('SHTC3_sensor')
#log.addHandler(journal.JournaldLogHandler())
log.addHandler(JournaldLogHandler())
log.setLevel(logging.INFO)

# The time in seconds between sensor reads
READ_INTERVAL = 60.0
# Create Prometheus gauges for humidity and temperature in
# Celsius and Fahrenheit
g_pmax = Gauge('maximaler Spritzdruck',
           'maximaler Spritzdruck')
#gt = Gauge('temperature',
#           'temperature', ['scale'])

def readvalue(i,ZyklenAlt):
    try:
        df = pd.DataFrame()

        program = ""

        urls = ['http://als001/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=20241029&to=20241029&maid=1695205705851135']

        for url in urls:
            #df = df.append(selenium_import2.get_data_from_url(url,program))
            df = pd.concat([df, selenium_import2.get_data_from_url(url,program)])
        #print(df)

        dftemp = sensorREST(df)
        df = dftemp
        Zyklen = df.iat[-1,3]

        #if Zyklen > ZyklenAlt:






    
if __name__ == "__main__":
    # Expose metrics
    metrics_port = 8000
    start_http_server(metrics_port)
    print("Serving sensor metrics on :{}".format(metrics_port))
    log.info("Serving sensor metrics on :{}".format(metrics_port))
    
    i = 0
    ZyklenAlt = 0

    readvalue(i,ZyklenAlt)


    time.sleep(1)