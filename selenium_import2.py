from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
import time
import re
import pandas as pd
from selenium.common.exceptions import TimeoutException
import numpy as np

def setup_driver():
    """Set up Selenium WebDriver."""
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    return driver

def extract_tables_from_frame(driver, program):
    """Extracts tables from the current frame, adjusts decimal points, and returns as DataFrame."""
    try:
        tables = pd.read_html(driver.page_source, thousands=None, decimal=",")  # Read tables from the HTML page source
        heading = tables[0]
        empty_data = []
        empty_df = pd.DataFrame(empty_data)
        data_table = empty_df
        if len(tables) >= 3:

            for table in tables:
                if table[0][0][0] == '+':
                    data_table = pd.concat([data_table, table.iloc[1:]])

            print(data_table)

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
                '''colnames = pd.Series({
                    0: filename_table.iloc[0][0],
                    1: filename_table.iloc[1][0],
                    2: filename_table.iloc[2][0],
                    3: filename_table.iloc[3][0],
                    4: "Ausschuss"})'''
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

            print(data_table)

            '''if heading[0][0].find("A0") > 0:
                #data_table = tables[4]
                print(data_table)
                filename_table = tables[1]
                # Convert comma decimals to float in the data_table
                #data_table = convert_comma_to_dot(data_table)
                data_table['Auftrag'] = filename_table.iloc[0][1]
                data_table['Programm'] = filename_table.iloc[1][1]
                data_table['Zeitpunkt'] = filename_table.iloc[3][1][:10]
                data_program = filename_table.iloc[1][1]
                for table in tables:
                    if table[0][0][0] == '+':
                        data_table = data_table.append[table]

            elif heading[0][0].find("EN") > 0:
                #data_table = tables[3]
                print(data_table)
                filename_table = tables[1]
                # Convert comma decimals to float in the data_table
                #data_table = convert_comma_to_dot(data_table)
                data_table['Auftrag'] = filename_table.iloc[0][1]
                data_table['Programm'] = filename_table.iloc[1][1]
                data_table['Zeitpunkt'] = filename_table.iloc[3][1][:10]
                data_program = filename_table.iloc[1][1]
                for table in tables:
                    if table[0][0][0] == '+':
                        data_table = data_table.append[table]
            else:
                empty_data = []
                empty_df = pd.DataFrame(empty_data)
                data_table = empty_df'''

            if not program:
                print("leer")
            else:
                print(program)

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
    time.sleep(5)  # Allow time for the page to load
    links = driver.find_elements(By.TAG_NAME, "a")
    date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')  # Regex to match dates
    all_dataframes = pd.DataFrame()

    for link in links:
        if date_pattern.search(link.text):
            print(f"Processing link with date: {link.text}")
            ActionChains(driver).move_to_element(link).click(link).perform()
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            df = extract_and_save_tables(driver, program)
            print(df)
            if not df.empty:
                all_dataframes = pd.concat([all_dataframes, df], ignore_index=True)

    return all_dataframes

def time_to_timedelta(time_str):
    hours, minutes = map(int, time_str.split(':'))
    return pd.Timedelta(hours=hours, minutes=minutes)

def add_datetime(df):
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

def get_data_from_url(url,program):
    driver = setup_driver()
    try:
        final_df = find_and_process_links(driver, url, program)
        final_df = add_datetime(final_df)
        if isinstance(final_df.columns, pd.MultiIndex):
            final_df.columns = [' '.join(col).strip() for col in final_df.columns]
        final_df.columns = [col.split(' ')[0] for col in final_df.columns]
        final_df = final_df.iloc[3:]
        final_df.reset_index(drop=True, inplace=True)
        print("Data collection complete. Data shape:", final_df.shape)
        return final_df
    finally:
        driver.quit()