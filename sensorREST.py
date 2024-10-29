# reads the temperature and humidity values from the sensors in the production facility

import csv
import numpy as np
import pandas as pd
import datetime as dt
import pytz
from datetime import datetime, timedelta
from icecream import ic
from prometheus_api_client import PrometheusConnect,  MetricSnapshotDataFrame, MetricRangeDataFrame


def sensorREST(df):
    dfm = pd.DataFrame()
    
    #Tstart = df.iloc[0]["datetime"].to_pydatetime()
    #Tend = df.iloc[-1]["datetime"].to_pydatetime()
    Tstart=df['datetime'].iat[0].to_pydatetime()
    Tend=df['datetime'].iat[-1].to_pydatetime()

    ic(Tstart,Tend)
    ic(type(Tstart))

    now=dt.datetime.now()

    #vienna = pytz.timezone('Europe/Vienna')
    #now = now.replace(tzinfo=pytz.utc).astimezone(vienna)
    shift=dt.timedelta(hours=2)

    Tstartdelta = now - Tstart - shift
    Tenddelta = now - Tend - shift

    #ic(Tstartdelta)

    prom = PrometheusConnect(url ="http://172.21.135.100:9090", disable_ssl=True)

    # metric values for a range of timestamps
    metric_data = prom.get_metric_range_data(
        metric_name='temperature',
        start_time = now - Tstartdelta,
        end_time = now - Tenddelta
    )

    metric_temp = MetricRangeDataFrame(metric_data)
    ic(metric_temp.iloc[0])

    df_temp = metric_temp[metric_temp["hostname"]=="SG_Monitor"]
    
    dfm["minute"] = pd.DataFrame(metric_temp.index.to_pydatetime()).iloc[:,0].dt.floor('T')

    df1 = df_temp.reset_index(drop=True)

    dfm["temperature"] = df1["value"]


    metric_data = prom.get_metric_range_data(
        metric_name='humidity_percent',
        start_time = now - Tstartdelta,
        end_time = now - Tenddelta
    )

    metric_hum = MetricRangeDataFrame(metric_data)

    df_hum = metric_hum[metric_hum["hostname"]=="SG_Monitor"]

    df1 = df_hum.reset_index(drop=True)

    dfm["humidity"] = df1["value"]

    df["minute"] = df["datetime"].dt.floor('T')

    #df = df.set_index('minute').join(dfm.set_index('minute'))   
    df = df.reset_index().merge(dfm.reset_index(), on='minute', how='inner')  # Example using merge with inner join
    df = df.dropna(subset=["temperature"])


    return df