#reads from ALS Webviewer

import pandas as pd
import numpy as np
from unicodedata import normalize
from icecream import ic

def webimport(start,end):

    #link = "http://als001/servlet?command=prodprotocol&hierarchy=mop&language=DE&from=20220725&to=20220726&maid=1513755320190225"
    #link2 ="http://als001/servlet?command=loadprotocolfile&protocol=prod&file=\\\\ALS001\\ACTVAL\\1513755320190225_EN01-650\\P221195%2B11960010\\1124.001.1390_19_5\\daystr.HTM"
    link2 = "http://als001/servlet?command=loadprotocolfile&protocol=prod&file=\\\\ALS001\ACTVAL\\1513755320190225_EN01-650\\P221138%2B11390010\\1124.001.1390_19_5\\daystr.HTM"

    dates = list(range(start,end,1))
    ic(dates)

    df = pd.DataFrame()

    for count, day in enumerate(dates):
        url = link2.replace("daystr",str(day))

        if count == 0:
            ic(day)
            df1 = pd.read_html(url, attrs={"bordercolor": "#FFFFFF"}, header=[0])#, header=[0,1])
            df = df.append(df1)
            
        else:
            #ic(day)
            # try, except Exception as ex überspringt Tage, an denen keine Daten vorhanden sind
            try:
                df = df.append(pd.read_html(url, attrs={"bordercolor": "#FFFFFF"}, header=[0]))
            except Exception as ex:
                pass
            #df = df.append(df1)

        print(day)
    
    #Löschen der Zeilen mit Toleranzen usw. da nicht ben�tigt und meistens ohne Inhalt
    df = df.drop([0,1,2])

    #Finden der Uhrzeit
    i=1
    while True:
        if ':' in str(df.iat[0,i]):
            timeCol = i
            break

        if i == len(df.columns):
            break

        i+=1

    #datetime Spalte hinzufügen
    if timeCol > 0:
        df["DATE"] = df.DATE.astype(int)
        df["datestr"] = df.DATE.astype(str)
        df["timestr"] = df.TIME.astype(str)
        df["datetime"] = df.datestr + " " + df.timestr
        df["datetime"] = pd.to_datetime(df.datetime, format="%Y%m%d %H:%M:%S", errors='coerce')
        df = df.drop(columns=["DATE", "TIME"])

    #ändern des Datenformats von Object auf numerische Formate
    for column in df:
        if column != "datetime" and column != "TIME" and column != "COUNT":
            df[column]=pd.to_numeric(df[column], errors='coerce')

    return df