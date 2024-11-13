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
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import shutil
import time
import re
import pandas as pd
from selenium.common.exceptions import TimeoutException
import numpy as np
from io import StringIO

timezone = pytz.timezone('Europe/Vienna')

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
        df = pd.concat([df, get_data_from_url(url,program)])

        #Zyklen = df.iat[-1,3]
        #pmax = df.iat[-1,12]
        #print("Zyklen: ", Zyklen," pmax: ",pmax)

        # Check if the column is of type str
        if not df.empty:
            if df['Zykluszeit'].dtype == 'object' and df['Zykluszeit'].apply(lambda x: isinstance(x, str)).all():
                # Convert the column to float
                df['Zykluszeit'] = df['Zykluszeit'].astype(float)


        if not df.empty:
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

            '''#Definition eines neuen Index basierend auf Datum und Zeit
            format = '%Y-%m-%d %H:%M:%S.%f'
            #dfnew2['datetime'] = dfnew2['datetime'].astype(str)
            dfnew2.loc[:,'datetime'] = dfnew2['datetime'].astype(str)
            #Formatierung um einen Index aus datetime bilden zu können
            dfnew2['Datetime'] = pd.to_datetime(dfnew2["datetime"], format=format)
            dfnew2 = dfnew2.set_index(pd.DatetimeIndex(dfnew2["Datetime"]))
            #Lokalisierung auf AT, da InfluxDB alles als UTC Zeit abspeichert
            dfnew2.index = dfnew2.index.tz_localize('Europe/Vienna')
            dfnew2.index = dfnew2.index.tz_convert('UTC')
            dfnew2 = dfnew2.drop(["datetime"], axis=1)'''
            
            # Define the expected datetime format
            format = '%Y-%m-%d %H:%M:%S.%f'

            # Convert datetime column to string to handle cases without milliseconds
            dfnew2['datetime'] = dfnew2['datetime'].astype(str)

            # Check if milliseconds are missing based on string length and add ".000" if needed
            dfnew2['datetime'] = dfnew2['datetime'].apply(lambda x: x if len(x) > 19 else x + '.000')

            # Parse datetime column with milliseconds added where necessary
            dfnew2['Datetime'] = pd.to_datetime(dfnew2['datetime'], format=format)

            # Set the datetime column as the index
            dfnew2 = dfnew2.set_index(pd.DatetimeIndex(dfnew2["Datetime"]))

            # Localize and convert to UTC
            dfnew2.index = dfnew2.index.tz_localize('Europe/Vienna')
            dfnew2.index = dfnew2.index.tz_convert('UTC')

            # Drop the original datetime column if it's no longer needed
            dfnew2 = dfnew2.drop(["datetime"], axis=1)

            # List of columns to ignore during the conversion check
            ignore_columns = ['Ausschuss', 'Auftrag', 'Programm', 'Werkzeug', 'Datetime']

            # Iterate over each column in the dataframe
            for column in dfnew2.columns:
                # Only process columns that are not in the ignore list
                if column not in ignore_columns:
                    # Check if the column is not in numerical form
                    if not pd.api.types.is_numeric_dtype(dfnew2[column]):
                        # Check if the column is a Series before attempting conversion
                        if isinstance(dfnew2[column], (pd.Series, list, tuple, np.ndarray)):
                            # Convert the column to float
                            dfnew2[column] = pd.to_numeric(dfnew2[column], errors='coerce')
                        else:
                            print(f"Column {column} is not a 1-dimensional Series, list, or array. Skipping conversion.")

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

def setup_driver():
    """Set up Selenium WebDriver."""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    chromedriver_path = shutil.which("chromedriver")
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)
    return driver
    return driver

def extract_tables_from_frame(driver, program):
    """Extracts tables from the current frame, adjusts decimal points, and returns as DataFrame."""
    try:
        html_content = StringIO(driver.page_source)
        tables = pd.read_html(html_content, thousands=None, decimal=",")
        heading = tables[0]
        empty_data = []
        empty_df = pd.DataFrame(empty_data)
        data_table = empty_df

        if len(tables) >= 3:

            i = 2
            n = 1

            while i+1 <= len(tables):
                #print(i)
                data_table = pd.concat([data_table, tables[i].iloc[1:]])
                #print(data_table)
                if len(tables) > 3:
                    # Kontrolliert, ob der Datensatz vom ALS konsistent ist
                    # oder ob weitere Tabellen mit inkonsistenten Paramtern
                    # vorhanden sind                    
                    if tables[i+2].columns[0] == "Parameter":
                        if tables[i+2].columns[1].find('?') > -1:
                            print("Header komisch i+2")
                            i = len(tables)+1
                        else:
                            i = i+1

                    if i <= len(tables):
                        if tables[i+3].columns[0] == "Parameter":
                            if tables[i+3].columns[1].find('?') > -1:
                                print("Header komisch i+3")
                                i = len(tables)+1
                            else:
                                i = i+2
                i = i+1
         
            filename_table = tables[1]
            # Convert comma decimals to float in the data_table
            #data_table = convert_comma_to_dot(data_table)
            rowAuftr, col = np.where(filename_table == "Auftrag")
            rowProg, col = np.where(filename_table == "Programm")
            rowWZG, col = np.where(filename_table == "Werkzeug")
            rowDATE, col = np.where(filename_table == "Zeitpunkt")

            data_table['Auftrag'] = filename_table.iloc[rowAuftr][1].values[0]
            data_table['Programm'] = filename_table.iloc[rowProg][1].values[0]
            data_table['Werkzeug'] = filename_table.iloc[rowWZG][1].values[0]
            
            data_table['DATE'] = filename_table.iloc[rowDATE][1].values[0][:10]
            data_program = filename_table.iloc[rowProg][1][1]

            colZusatz = pd.Series({
                0: filename_table.iloc[0][0],
                1: filename_table.iloc[1][0],
                2: filename_table.iloc[2][0],
                3: "DATE"})

            if heading[0][0].find("A0") > 0:
                pos = len(data_table.columns)-5
                colnames = pd.Series({
                    0: "Ausschuss"})
                #colnames = colnames.append(filename_table.iloc[-pos:][1])
                colnames = pd.concat([colnames, filename_table.iloc[-pos:][1]], ignore_index=True)
                colnames = colnames.str.replace(', Istwert','')
                #colnames = colnames.append(colZusatz)
                colnames = pd.concat([colnames,colZusatz], ignore_index=True)
                data_table.columns = colnames
                data_table = data_table.rename(columns={"Uhrzeit" : "TIME"})

            #delete all NaN columns
            data_table = data_table.loc[:,data_table.columns.dropna()]

            if not program:
                return data_table
            else:
                if data_program == program:
                    return data_table
                elif data_program == "":
                    return data_table
                else:
                    empty_data = []
                    empty_df = pd.DataFrame(empty_data)
                    return empty_df
            
    except Exception as e:
        print(f"Failed to extract or parse tables: {e}")
    return pd.DataFrame()

def convert_comma_to_dot(df):
    """Converts strings with commas as decimals to floats."""
    cols = df.select_dtypes(include=['object']).columns  # Select only columns with object type
    df[cols] = df[cols].apply(lambda x: x.str.replace(',', '.'))
    df[cols] = df[cols].apply(pd.to_numeric, errors='ignore')  # Convert to numeric if possible
    return df

def extract_and_save_tables(driver, program):
    """Navigates frames and extracts tables, returns a single DataFrame."""
    frames = driver.find_elements(By.TAG_NAME, "iframe")
    all_frames = frames + driver.find_elements(By.TAG_NAME, "frame")
    all_dataframes = pd.DataFrame()

    if not all_frames:
        df = extract_tables_from_frame(driver, program)
        if not df.empty:
            all_dataframes = pd.concat([all_dataframes, df], ignore_index=True)
    else:
        for frame in all_frames:
            try:
                driver.switch_to.frame(frame)
                df = extract_tables_from_frame(driver, program)
                if not df.empty:
                    all_dataframes = pd.concat([all_dataframes, df], ignore_index=True)

                driver.switch_to.default_content()
            except Exception as e:
                print(f"Error processing frame: {e}")
                driver.switch_to.default_content()
    return all_dataframes

def find_and_process_links(driver, url, program):
    """Finds links containing dates and processes them."""
    driver.get(url)
    time.sleep(2)  # Allow time for the page to load
    links = driver.find_elements(By.TAG_NAME, "a")
    date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')  # Regex to match dates
    all_dataframes = pd.DataFrame()

    for link in links:
        if date_pattern.search(link.text):
            #print(f"Processing link with date: {link.text}")
            ActionChains(driver).move_to_element(link).click(link).perform()
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            df = extract_and_save_tables(driver, program)
            #print(df)
            if not df.empty:
                all_dataframes = pd.concat([all_dataframes, df], ignore_index=True)

    return all_dataframes

def time_to_timedelta(time_str):
    hours, minutes = map(int, time_str.split(':'))
    return pd.Timedelta(hours=hours, minutes=minutes)

def add_datetime(df):
    i=1
    if not df.empty:
        while True:
            if ':' in str(df.iat[0,i]):
                timeCol = i
                break

            if i == len(df.columns):
                break

            i+=1

        #datetime Spalte hinzufügen
        if timeCol > 0:
            
            #Formatierung für Engel Maschinen --> DATE Format als YYYYMMDD
            if all(isinstance(x, int) for x in df["DATE"]):
                df["DATE"] = df.DATE.astype(int)
                df["datestr"] = df.DATE.astype(str)
                df["timestr"] = df.TIME.astype(str)
                df["datetime"] = df.datestr + " " + df.timestr
                df["datetime"] = pd.to_datetime(df.datetime, format="%Y%m%d %H:%M:%S", errors='coerce')
                df = df.drop(columns=["DATE", "TIME", "datestr", "timestr"])

            #Formatierung für Arburg Maschinen --> DATE Format DD.MM.YYYY
            if all(isinstance(x, str) for x in df["DATE"]):
                df["DATE"] = pd.to_datetime(df["DATE"], format='mixed')
                df['time_delta'] = df['TIME'].apply(time_to_timedelta)
                df['datetime'] = df['DATE'] + df['time_delta']
                df = df.drop(columns=["DATE", "TIME","time_delta"])

            #df = df.drop(columns=["DATE", "TIME", "datestr", "timestr","time_delta"])

    return df

def delete_statistics(df):
    # Löscht die statistische Auswertung am Schluss der Tabelle, wenn vorhanden
    if not df.empty:
        if 'Minwert' in str(df.iat[-1,0]):
            df = df.iloc[:-4]

    return df

def get_data_from_url(url,program):
    driver = setup_driver()
    try:
        final_df = find_and_process_links(driver, url, program)
        final_df = delete_statistics(final_df)
        final_df = add_datetime(final_df)
        if isinstance(final_df.columns, pd.MultiIndex):
            final_df.columns = [' '.join(col).strip() for col in final_df.columns]
        final_df.columns = [col.split(' ')[0] for col in final_df.columns]
        final_df = final_df.iloc[3:]
        final_df.reset_index(drop=True, inplace=True)
        #print("Data collection complete. Data shape:", final_df.shape)
        return final_df
    finally:
        driver.quit()
    
if __name__ == "__main__":

    #Liest den Bucketnamen aus dem Input beim Scriptstart ein über ALSplot.py Input
    #bucket = str(sys.argv[1])
    #bucket = "A04-370S"
    bucket = "A06-720S"
    #bucket = "A09-370A"
    #bucket = "EN01-650"
    #bucket = "EN02-120"
    #bucket = "A02-420C"
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
        time.sleep(60)

    time.sleep(1)