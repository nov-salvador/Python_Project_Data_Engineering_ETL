import requests
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
import lxml

data_url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attr_xtrc = ["Name", "MC_USD_Billion"]
table_attr_final = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
csv_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"
rate_csv = "exchange_rate.csv"
log_file = "./code_log.txt"

def log_progress(message):
  timestamp_format = "%Y-%h-%d-%H:%M:%S"
  now = datetime.now()
  timestamp = now.strftime(timestamp_format)
  with open(f'{log_file}', 'a') as f:
    f.write(timestamp + ":" + message + "\n")

def extract(data_url, table_attr):
  html_data = requests.get(data_url).text
  data = BeautifulSoup(html_data, "html.parser")
  tables = data.find_all('tbody')
  rows = tables[0].find_all('tr')
  df = pd.DataFrame({}, columns=table_attr)
  for row in rows[1:]:
    col = row.find_all("td")
    a = col[1].find_all('a')
    name = a[1].contents[0]
    mc = col[2].contents[0]
    cleaned_mc = float(mc.strip())
    data_dict = {
      "Name": name,
      "MC_USD_Billion": cleaned_mc
    }
    df1 = pd.DataFrame(data_dict, index=[0])
    df = pd.concat([df, df1], ignore_index=True)
  print(df)
  return df
  
def transform(df=None,extract_attr=None,final_attr=None, rate_csv=None):
  rate_df = pd.read_csv(rate_csv)
  rate_dict = rate_df.set_index('Currency')['Rate'].to_dict()
  df[f'{extract_attr[1]}'] = pd.to_numeric(df[f'{extract_attr[1]}'], errors='coerce')
  GBP_rate = float(rate_dict['GBP'])
  EUR_rate = float(rate_dict['EUR'])
  INR_rate = float(rate_dict['INR'])
  df[f'{final_attr[2]}'] = [np.round((GBP_rate * n), 2) for n in df[f'{extract_attr[1]}']] #column for GBP rates
  df[f'{final_attr[3]}'] = [np.round((EUR_rate * n), 2) for n in df[f'{extract_attr[1]}']] #column for EUR rates
  df[f'{final_attr[4]}'] = [np.round((INR_rate * n), 2) for n in df[f'{extract_attr[1]}']] #column for INR rates
  print (df)
  return df

def load_to_csv(df,path):
  df.to_csv(path)

def load_to_db(df, connection, table_name):
  df.to_sql(table_name, connection, if_exists='replace',index=False)

def run_queries(statements, connection):
  for statement in statements:
    output = pd.read_sql(statement,connection)
    print ('Your query statement : ' + statement)
    print("The output \n" , output)

log_progress('Preliminaries complete. Initiating ETL process')
df_extracted = extract(data_url, table_attr_xtrc)
log_progress('Data extraction complete. Initiating Transformation process')

df_transformed=transform(df=df_extracted, rate_csv=rate_csv, final_attr=table_attr_final, extract_attr=table_attr_xtrc)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df_transformed, csv_path)
log_progress('Data saved to CSV file')

log_progress('SQL Connection initiated')
sql_connection = sqlite3.connect(db_name) #connection for db#

load_to_db(df_transformed,sql_connection,table_name)
log_progress('Data loaded to Database as a table, Executing queries')

q1 = 'SELECT * FROM Largest_banks'
q2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
q3 = 'SELECT Name from Largest_banks LIMIT 5'
queries= [q1, q2, q3]
run_queries(queries, sql_connection)
log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection closed')