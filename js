## Buitre
import os
import time
import shutil

StartT = time.time()

filepath = "C:/Users/IN10011418/Desktop/FTP/GFE_TestFiles"
path = "C:/Users/IN10011418/Desktop/FTP/FTP_Locations/"


global Files_Dropped
Files_Dropped = 0

files = []

for file in os.listdir(filepath):
    files.append(file)

for folder in os.listdir(path):
    if os.path.isdir(os.path.join(path, folder)):

        for subfolder in os.listdir(path+folder):
            subfolder_path = os.path.join(path, folder, subfolder)

            if os.path.isdir(subfolder_path):

                for file in files:
                    file_path = os.path.join(filepath, file)

                    if os.path.isfile(file_path):

                        f = os.path.basename(file_path)
                        if subfolder in f:
                            # print(file_path, subfolder_path)
                            # shutil.copy2(file_path, subfolder_path)
                            
                            Files_Dropped += 1

EndT = time.time()
TTime = EndT - StartT

print("Total Files Placed ::", Files_Dropped)
print("Time Taken ::", round(TTime,2))



import os
import random
import string

# Set the base directory
base_dir = "C:/Users/IN10011418/Desktop/FTP/FTP_Locations"

# Set the names of the subfolders
subfolder_names = ['FFS01D', 'CPIC01D', 'CPIF01D', 'CPGC01D', 'CPGF01D']

# Create the base directory if it doesn't already exist
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

# Create 500 top-level folders
for i in range(1, 501):

    folder_name = ''.join(random.sample(string.ascii_letters, 6))

    # Create the top-level folder
    top_folder = os.path.join(base_dir, folder_name)
    os.makedirs(top_folder)
    
    # Create the subfolders
    for subfolder in subfolder_names:
        os.makedirs(os.path.join(top_folder, subfolder))

print("Folders created successfully!")


import time
import os
import subprocess
import multiprocessing
from functools import partial
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor

def Main(FileProcess, event_queue):

    class EventHandler(FileSystemEventHandler):
        def on_created(self, event):
            event_queue.put(event)
            # FileProcess(os.path.basename(event.src_path))
            
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    while True:
        try:
            with ThreadPoolExecutor(max_workers=50) as executor:
                while True:
                    '''
                    Get the next file from the queue.
                    '''
                    event = event_queue.get()
                    # print(event.src_path)
                    executor.submit(FileProcess, os.path.basename(event.src_path))
            time.sleep(1)

        except KeyboardInterrupt:
            observer.stop()
        observer.join()

def FileProcess(__parm):

    print("File detected :: ", __parm ,"\nRunning batch file...\n")

    ''' 
     The subprocess module allows you to run commands in a non-blocking way,
     so that the process can continue to watch the directory for new files
    '''
    
    try:
        BatchProcess = subprocess.Popen(["C:/Users/IN10011418/Desktop/FTP/Sample_batfile.bat"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = BatchProcess.communicate()

        if BatchProcess.returncode != 0:
            print(f"There was some error while running the batch file :: {stderr.decode()} ")
        else:
            print("Batch file completed.") #{stdout.decode()}
    except Exception as err:
        print(err)


path = "C:/Users/IN10011418/Desktop/FTP/FTP_Locations"

if __name__ == '__main__':
    ''' 
    multiprocessing.Queue() is a multi-producer, multi-consumer queue.
    that is thread-safe and can be used to pass data between processes.
    '''
    event_queue = multiprocessing.Queue()
    watch_process = multiprocessing.Process(target=Main, args=(partial(FileProcess), event_queue))
    watch_process.start()
    watch_process.join()
         

from sqlalchemy import create_engine
from datetime import datetime

Today = datetime.now()
LogTime = Today.strftime("%m/%d/%Y, %H:%M:%S")

try:
    _ServerName = 'DEVRHUBIWFEX01.R1RCM.TECH'
    _Database = 'TRANKSLLNJ'
    driver = 'ODBC Driver 17 for SQL Server'
    
    ENGINE = create_engine(f'mssql+pyodbc://{_ServerName}/{_Database}?driver={driver}',fast_executemany=True)
    CONN = ENGINE.connect()

except Exception as err:
    print(err)


###################################################################################################################################

##Sandbox

#Compare 2 DF

from sqlalchemy import create_engine
import pandas as pd 
import datacompy

try:
    _ServerName = 'AHS-TRAN22.accretivehealth.local'
    _Database = 'TranBLPA'
    # driver = 'ODBC Driver 17 for SQL Server'
    driver = 'SQL+Server'
    
    ENGINE = create_engine(f'mssql+pyodbc://{_ServerName}/{_Database}?driver={driver}',fast_executemany=True)
    CONN = ENGINE.connect()

except Exception as err:
    print(err)


DF1 = pd.read_sql("select * from tmpretro with(nolock) where patientaccountnbr = '010045523973'", CONN)
DF2 = pd.read_sql("select * from tmpretro with(nolock) where patientaccountnbr = '010045523973'", CONN)

diff = datacompy.Compare(DF1, DF2, join_columns='patientaccountnbr')
output = diff.report(sample_count=100, column_count=100)

file = open("Mismatch.txt", "w")

for line in output:
    file.writelines(line)

file.close()

#Create Insert into script

import pandas as pd

# Read the CSV file using Pandas

df = pd.read_csv('C:/Users/IN10011418/OneDrive - R1/Desktop/Metadata_Table_data.csv')

# Initialize an empty string to store the SQL script
sql_script = ''

# Extract the column names and values for each row
for index, row in df.iterrows():
    columns = ', '.join(df.columns)
    values = ''
    for value in row:
        if isinstance(value, str):
            values += f"'{value}', "
        else:
            values += f"{value}, "
    values = values[:-2]

    # Generate the SQL script for the row and append it to the main SQL script
    sql_script += f"INSERT INTO table_name ({columns}) VALUES ({values});\n"

# Write the SQL script to a file
with open('output_script.sql', 'w') as f:
    f.write(sql_script)
	
#Dask read sql

import dask.dataframe as dd
import pyodbc


cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      'SERVER=DEVRHUBIWFEX01.R1RCM.TECH;'
                      'DATABASE=TRANKSLLNJ;'
                      )


query = 'SELECT col1, col2 FROM table_name WHERE col1 > 5'

df = dd.read_sql_query(query, cnxn, index_col='index_col_name', npartitions=8)

df.to_csv('output_file.csv',compression='blosc')


#Dask write SQL

import dask.dataframe as dd
from dask.diagnostics import ProgressBar

SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
DATABASE ='Srdial'
DRIVER = 'SQL+Server'
TABLE_NAME = 'MFS_Export_GenesysRaw'
FILE_PATH = r'C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv'

ENGINE = f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}'

dask_optimizations = {
    'assume_missing': True,  # Assume missing values
    'low_memory': True,  # Optimize memory usage
    'dtype': str,  # Use string type for all columns
}

# Create a Dask DataFrame from the CSV file
df = dd.read_csv(FILE_PATH, sep=',', **dask_optimizations)
df = df.astype(str)

# Remove hyphens from column names
df = df.rename(columns=lambda x: x.replace('-', ''))

# Convert NaN values to None to match SQL Server NULL semantics
df = df.where(df.notnull(), None)

# Write the Dask DataFrame to SQL Server
with ProgressBar():
    df.to_sql(TABLE_NAME, ENGINE, if_exists='append', index=False)


#Data Clean Split

import pandas as pd

DF = pd.read_csv("C:/Users/IN10011418/OneDrive - R1/Desktop/167554_MISC_RTFILE_152253.20221114.LPNT.PTSMTRP02D.txt", sep="|")

def split_dataframe_rows(df,column_selectors, row_delimiter):
    
    # we need to keep track of the ordering of the columns
    def _split_list_to_rows(row,row_accumulator,column_selector,row_delimiter):
        split_rows = {}
        max_split = 0

        for column_selector in column_selectors:
            split_row = row[column_selector].split(row_delimiter)
            split_rows[column_selector] = split_row
            if len(split_row) > max_split:
                max_split = len(split_row)
            
        for i in range(max_split):
            new_row = row.to_dict()
            for column_selector in column_selectors:
                try:
                    new_row[column_selector] = split_rows[column_selector].pop(0)
                except IndexError:
                    new_row[column_selector] = ''
            row_accumulator.append(new_row)

    new_rows = []

    df.apply(_split_list_to_rows,axis=1,args = (new_rows,column_selectors,row_delimiter))
    new_df = pd.DataFrame(new_rows, columns=df.columns)

    return new_df


Output = split_dataframe_rows(DF, ['AccountNumber','StatementAmount','StatementCode'], ',')

Output.to_csv('167554_MISC_RTFILE_152253.20221114.LPNT.PTSMTRP02D.csv', index=False)

#GUI DB search

import tkinter as tk
import pyodbc

def get_databases():
    server_name = server_entry.get()
    connection = pyodbc.connect('DRIVER={SQL Server};SERVER='+server_name+';Trusted_Connection=yes;')
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sys.databases")
    global databases
    databases = cursor.fetchall()
    listbox.delete(0, tk.END)
    for database in databases:
        listbox.insert(tk.END, database[0])
        
def search_databases(event):
    global databases
    search_text = search_entry.get().lower()
    listbox.delete(0, tk.END)
    for database in databases:
        if database[0].lower().find(search_text) != -1:
            listbox.insert(tk.END, database[0])


root = tk.Tk()
root.title("SQL Server Database Explorer")

# Get the screen width and height
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# Get the window width and height
window_width = 500
window_height = 350

# Calculate the coordinates for centering the window
x_coord = (screen_width/2) - (window_width/2)
y_coord = (screen_height/2) - (window_height/2)

# Set the window geometry
root.geometry("%dx%d+%d+%d" % (window_width, window_height, x_coord, y_coord))

# configure the grid layout
root.grid_columnconfigure(0, weight=1)
root.grid_rowconfigure(3, weight=1)


server_label = tk.Label(root, text="Server Name:")
server_label.grid(row=0, column=0, sticky="W", padx=10, pady=10)

server_entry = tk.Entry(root)
server_entry.grid(row=0, column=1)

get_databases_button = tk.Button(root, text="Get Databases", command=get_databases)
get_databases_button.grid(row=0, column=2, pady=5)

# create the search entry
search_label = tk.Label(root, text="Search:")
search_label.grid(row=1, column=0, sticky="W", padx=10, pady=10)
search_entry = tk.Entry(root)
search_entry.grid(row=1, column=1, sticky="E", padx=10, pady=10)
search_entry.bind("<KeyRelease>", search_databases)

# create the listbox and scrollbar
listbox = tk.Listbox(root)
listbox.grid(row=3, column=0, columnspan=3, sticky="NSEW", padx=10, pady=10)

scrollbar = tk.Scrollbar(root, orient="vertical", command=listbox.yview)
scrollbar.grid(row=3, column=3, sticky="NS")
listbox.configure(yscrollcommand=scrollbar.set)

root.mainloop()

#File watch Process

import time
import os
import multiprocessing
from functools import partial
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

def Main(FileProcess, event_queue):

    class EventHandler(FileSystemEventHandler):
        def on_created(self, event):
            event_queue.put(event)
            FileProcess(os.path.basename(event.src_path))
            
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

def FileProcess(__parm):

    print("File detected :: ", __parm ,"\nRunning batch file...\n")
    os.system("C:/Users/IN10011418/Desktop/FTP/Sample_batfile.bat")

path = "C:/Users/IN10011418/Desktop/FTP"

if __name__ == '__main__':
    ''' 
    multiprocessing.Queue() is a multi-producer, multi-consumer queue.
    that is thread-safe and can be used to pass data between processes.
    '''
    event_queue = multiprocessing.Queue()
    watch_process = multiprocessing.Process(target=Main, args=(partial(FileProcess), event_queue))
    watch_process.start()
    watch_process.join()
         
#Loop start end date

import datetime
import calendar

today = datetime.datetime(2022,12,1, 1,1,1,1) #Start Date 
month_end = datetime.datetime(2022,12,1, 1,1,1,1)#End Date

# today = datetime.datetime.now()
# month_end = today.replace(day=calendar.monthrange(today.year, today.month)[1])

for i in range((month_end - today).days + 1):
    D = today + datetime.timedelta(i)
    date_object = datetime.datetime.strptime(str(D), "%Y-%m-%d %H:%M:%S.%f")
    DATE = date_object.strftime("%Y-%m-%d")
    print(DATE)


#Notepad GUI

import tkinter
import os
from tkinter import *
from tkinter.messagebox import *
from tkinter.filedialog import *

class Notepad():
    
    __root = Tk()
    __thisWidth = 300
    __thisHeight = 300
    __thisTextArea = Text(__root)
    __thisMenuBar = Menu(__root)
    __thisFileMenu = Menu(__thisMenuBar, tearoff=0)
    # __thisEditMenu = Menu(__thisMenuBar, tearoff=0)
    __thisHelpMenu = Menu(__thisMenuBar, tearoff=0)
    __thisScrollBar = Scrollbar(__thisTextArea)
    __file = None

    def __init__(self, **Kwargs):

        try:
            self.__root.wm_iconbitmap("C:/Users/IN10011418/OneDrive - R1/Scripts/PYTHON/Sandbox/logo.ico") #to add icon 
        except Exception as e:
            pass
            
        try:
            self.__thisWidth = Kwargs['width']
        except Exception as e:
            pass

        try:
            self.__thisHeight = Kwargs['height']
        except Exception as e:
            pass

        self.__root.title("R1 - Notepad")
        screenWidth = self.__root.winfo_screenwidth()
        screenHeight = self.__root.winfo_screenheight()

        left = (screenWidth / 2) - (self.__thisWidth / 2)
        top = (screenHeight / 2) - (self.__thisHeight / 2)

        self.__root.geometry('%dx%d+%d+%d' % (self.__thisWidth, self.__thisHeight, left, top))
        self.__root.grid_rowconfigure(0, weight=1)
        self.__root.grid_columnconfigure(0, weight=1)

        self.__thisTextArea.grid(sticky=N + E + S + W)

        self.__thisFileMenu.add_command(label="New", command=self.__newFile)
        self.__thisFileMenu.add_command(label="Open", command=self.__openFile)
        self.__thisFileMenu.add_command(label="Save", command=self.__saveFile)
        
        self.__thisFileMenu.add_separator()
        self.__thisFileMenu.add_command(label="Exit", command=self.__quitApplication)
        self.__thisMenuBar.add_cascade(label="File", menu=self.__thisFileMenu)

        self.__thisMenuBar.add_command(label="Cut", command=self.__cut)
        self.__thisMenuBar.add_command(label="Copy", command=self.__copy)
        self.__thisMenuBar.add_command(label="Paste", command=self.__paste)
        
        # self.__thisMenuBar.add_cascade(label="Edit", menu=self.__thisEditMenu)

        self.__thisHelpMenu.add_command(label="About R1 Notepad", command=self.__showAbout)
        self.__thisMenuBar.add_cascade(label="Help", menu=self.__thisHelpMenu)

        self.__root.config(menu=self.__thisMenuBar)
        self.__thisScrollBar.pack(side=RIGHT, fill=Y)
        self.__thisScrollBar.config(command=self.__thisTextArea.yview)
        self.__thisTextArea.config(yscrollcommand=self.__thisScrollBar.set)


    def __quitApplication(self):
            self.__root.destroy()

    def __showAbout(self):
        showinfo("R1 Notepad", "This is python based notepad.")

    def __openFile(self):
        self.__file = askopenfilename(defaultextension=".txt", filetypes=[("All Files", ".*"), ("Text Documents", "*.txt")])

        if self.__file == "":
            self.__file = None
        else:
            self.__root.title(os.path.basename(self.__file ) + "- R1 Notepad")
            self.__thisTextArea.delete(1.0, END)
            file = open(self.__file, "r")            
            self.__thisTextArea.insert(1.0, file.read())
            file.close()

    def __newFile(self):
        self.__root.title("Untitled - R1 Notepad")
        self.__file = None
        self.__thisTextArea.delete(1.0, END)
    
    def __saveFile(self):
        if self.__file == None:
            self.__file = asksaveasfilename(initialfile='Untitled.txt', defaultextension=".txt", filetypes=[("All Files", ".*"), ("Text Documents", "*.txt")])

            if self.__file == "":
                self.__file = None
            else:
                file = open(self.__file, "w")
                file.write(self.__thisTextArea.get(1.0, END))
                file.close()

                self.__root.title(os.path.basename(self.__file) + "- R1 Notepad")
        else:
            file = open(self.__file, "w")
            file.write(self.__thisTextArea.get(1.0, END))
            file.close()
    
    def __cut(self):
        self.__thisTextArea.event_generate("<<Cut>>")
    
    def __copy(self):
        self.__thisTextArea.event_generate("<<Copy>>")
    
    def __paste(self):
        self.__thisTextArea.event_generate("<<Paste>>") 

    def run(self):
        self.__root.mainloop()

Notepad = Notepad(width=680, height=400)
Notepad.run()

#Outlook automation

import win32com.client

#Opening Outlook application using Python Windows API
outlook = win32com.client.Dispatch('outlook.application')

#Creating Email Object
mail = outlook.CreateItem(0)

mail.To = 'asingh937@r1rcm.com'
mail.Subject = 'TEST MAIL'
mail.Body = '''FYI : Please find file attached.\nThis Mail is sent automated using Python..
               
               Thanks & Regards,
               Avineshwar
               '''

# We can add multiple attachments by calling Attachemnts.Add
# Attachment file needs to be string not path object.

mail.Attachments.Add('C:\\Users\IN10011418\Desktop\locked.zip')

mail.Send()


#Pandas to_sql thread process

import os
import datetime
import time
import pandas as pd
from sqlalchemy import create_engine,event
import concurrent.futures
import gc
gc.enable()

startT = time.time()

# SERVER_NAME = 'DEVCONTWCOR01.r1rcm.tech'
# DATABASE ='Srdial'
# DRIVER = 'ODBC Driver 11 for SQL Server' #'SQL Server Native Client 11.0' #ODBC Driver 13 for SQL Server #ODBC Driver 17 for SQL Server
# TABLE_NAME = 'MFS_Export_GenesysRaw'
# FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-TestData.csv'
# CHUNK_SIZE = 10000
# MAX_THREADS = 10

SERVER_NAME =  'DAL-CICDV1DB01.accretivehealth.local' #'DEVRHUBIWFEX01.R1RCM.TECH'
DATABASE = 'I3IMH01'
DRIVER = 'ODBC Driver 11 for SQL Server'
TABLE_NAME = 'Genesys_Export_Raw_IMH'
FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/IMH-Test.csv'
CHUNK_SIZE = 10000
MAX_THREADS = 10

insert_records_failure_flag_counter = 0
rows_inserted = 0
insertion_err = ''
insert_records_failure_flag = True

try:

    print(f"Connecting to Server to insert data into table :: {TABLE_NAME}.")
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

except Exception as e:

    print(f"Unable to connect to server :: {SERVER_NAME} err_msg :: {e}.")


def get_matching_file(FTP):

    try:
        print(f"Looking for file in path :: {FTP}")

        current_date = datetime.datetime.now().strftime('%Y%m%d')

        for file_name in os.listdir(FTP):
            if file_name.endswith(current_date+'.csv'):
                print(f"File found :: {file_name}")
                return os.path.join(FTP, file_name)
        
        print(f"No matching file found for system current date :: {current_date} in folder :: {FTP}.")
        return None
    
    except Exception as e:
        print(f"Error occured while looking for file in folder :: {FTP}. err_msg :: {e}")

def insert_records(chunk):

    try:
        global rows_inserted, insert_records_failure_flag,insertion_err,insert_records_failure_flag_counter

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        with ENGINE.connect() as CONN:
            chunk.to_sql(TABLE_NAME, CONN, index=False, if_exists="append", schema="dbo")
            CONN.commit()
        
        CONN.close()
        rows_inserted += len(chunk)

    except Exception as e:

        CONN.rollback()
        insertion_err += str(e)

        insert_records_failure_flag_counter += 1

        print(f"Unable to insert data in table :: {TABLE_NAME}. err_msg :: {insertion_err}")


def create_chunk(df):

    global insertion_err

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        @event.listens_for(ENGINE, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        futures = []

        print(f"Inserting data into table :: {TABLE_NAME}.")

        for chunk in chunks:
            future = executor.submit(insert_records,chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

    
    print(f"Data inserted successfully into table :: {TABLE_NAME}.")
    print(f"Total number of rows inserted :: {rows_inserted}.")
    print("Process completed successfully.")

    Success_percentage = (rows_inserted/len(df)) * 100
    Success_Per = "Success Percentage :: {:.2f}%".format(Success_percentage)

    print(Success_Per)

    endT = time.time()
    TotalT = endT - startT
    hours, remainder = divmod(TotalT, 3600)
    minutes, seconds = divmod(remainder, 60)

    Script_Time = "Total time taken :: {:.0f} hr {:.0f} min {:f} sec".format(hours, minutes, seconds)

    print(Script_Time)

if __name__ == '__main__':

    matching_file = FTP

    if matching_file:
        df = pd.read_csv(matching_file,sep=',',low_memory=False)
        create_chunk(df)

    else:
        print("No file found. Sys exit.")
        
#pandas to sql

import pandas as pd
from sqlalchemy import create_engine
import time

try:

    #Connection Para..
    server = 'DEVCONTWCOR01.r1rcm.tech'
    db = 'Srdial'
    driver = 'SQL+Server'

    ENGINE = create_engine('mssql+pyodbc://{}/{}?driver={}'.format(server, db, driver))
    CONN = ENGINE.connect()

except Exception as e:
    print(str(e))


try:
    # DF = pd.DataFrame()

    DF = pd.read_csv(r"C:\Users\IN10011418\OneDrive - R1\Desktop\MFS-TestData.csv",sep=',')
    DF = DF.astype(str)
    print(DF)

    # time.sleep(60)

    try:

        '''
        NOTE : if_exists default fail

        fail: Raise a ValueError.
        replace: Drop the table before inserting new values.
        append: Insert new values to the existing table.
        '''


        TableN = "MFS_Export_GenesysRaw"
        SchemaN = "dbo"

        RowsInserted = DF.to_sql(name=TableN, con=CONN, schema=SchemaN, index=False, method='multi', if_exists='replace')

        print(RowsInserted ," rows inserted into " , TableN)

        CONN.close()

    except Exception as e:
        print(str(e))

except Exception as e:
    print(str(e))


#parse logging file

import re

error_pattern = re.compile(r'\bINFO\b')
warning_pattern = re.compile(r'\bWARNING\b')

with open('ContactList_UploadDataLog_6e9ee166-663f-49b5-87bb-ca2c0628a574.log', 'r') as file:

    log_contents = file.read()

    error_messages = error_pattern.findall(log_contents)
    warning_messages = warning_pattern.findall(log_contents)

    num_errors = len(error_messages)
    num_warnings = len(warning_messages)

    print(f'Error messages ({num_errors})')
    print(f'Warning messages ({num_warnings})')

#sql templete


import re
from sqlparse import parse, format

def format_sql_code(sql_code):
    keywords = ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET']
    try:
        parsed = parse(sql_code)[0]
    except Exception as e:
        return e
    for token in parsed.tokens:
        if token.ttype is None:
            # Capitalize keywords
            for keyword in keywords:
                token.value = re.sub(keyword, keyword.upper(), token.value)
            # Convert snake_case to camelCase
            token.value = re.sub('(?<!^)(_)([a-zA-Z])', lambda x: x.group(2).upper(), token.value)
    # Use the sqlparse.format() function to format the indentation
    formatted_sql = format(str(parsed), reindent=True)
    print(formatted_sql)


sql = """

"""
format_sql_code(sql)


#SQL server connection

import pyodbc
from win10toast import ToastNotifier

try:
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                         'Server=DEVRHUBIWFEX01.R1RCM.TECH;'
                        'Trusted_Connection=yes;')
    '''
    SERVER = 'DEVRHUBIWFEX01.R1RCM.TECH;'
    conn = pyodbc.connect('Driver={{ODBC Driver 17 for SQL Server}};'
                      'Server={}'
                      'Trusted_Connection=yes;'.format(SERVER))
    '''
    
    print(conn)
    
        
except Exception as err:
    _ = ToastNotifier()        
    _.show_toast('Connection Error\n' , str(err) , duration = 10)
    
    #Error in DB connection 
    
#SQL to pandas

import pandas as pd
from sqlalchemy import create_engine
import time

try:

    #Connection Para..
    server = 'CCCCORSQL.accretivehealth.local'
    db = 'Srdial'
    driver = 'SQL+Server'

    ENGINE = create_engine('mssql+pyodbc://{}/{}?driver={}'.format(server, db, driver))
    CONN = ENGINE.connect()

except Exception as e:
    print(str(e))


try:
    # DF = pd.DataFrame()

    DF = pd.read_sql("""Select Top 1 * From CALLQUEUE WITH(NOLOCK);""", CONN)

    # time.sleep(60)

    try:

        DF.to_csv('SrDial_Template.csv', index=False)

        CONN.close()

    except Exception as e:
        print(str(e))

except Exception as e:
    print(str(e))


#sys awake

import pyautogui
import time

while True:


    pyautogui.press('volumedown')
    time.sleep(10)
    pyautogui.press('volumeup')
    time.sleep(10)

# # # #python -m pip install pyautogui

#Tkinter py

import tkinter as tk
from tkinter import messagebox, scrolledtext
from sqlalchemy import create_engine


class DB_Scan():
    
    def __init__(self):

        self.loginwin = tk.Tk()
        self.loginwin.wm_iconbitmap("C:/Users/IN10011418/OneDrive - R1/Scripts/PYTHON/Sandbox/logo.ico")
        self.loginwin.geometry('450x240')
        self.loginwin.title('Login')
        self.loginwin.configure(bg='grey70')
        self.loginwin.resizable(height=False, width=False)

        self.ServernameLabel = tk.Label(text='Server Name', height=1, width=10)
        self.ServernameLabel.place(x=182,y=20)

        self.ServernameHolder = tk.Entry(width=52)
        self.ServernameHolder.place(x=68, y=45)
        self.ServernameHolder.focus_force()

        self.DbLabel = tk.Label(text='Database Name', height=1, width=12)
        self.DbLabel.place(x=175,y=70)

        self.DbHolder = tk.Entry(width=52)
        self.DbHolder.place(x=68, y=95)

        self.ConnectServer = tk.Button(text = 'Connect', width=10, bg='greenyellow', command=self.__ConnectServer)
        self.ConnectServer.place(x=115, y=125)

        self.DisconnectServer = tk.Button(text = 'Disconnect', width=10, bg='red2', command=self.__DisconnectServer)
        self.DisconnectServer.place(x=230, y=125)
        self.DisconnectServer['state'] = 'disable'

        self.Nextbutton = tk.Button(text='>>>', width=5, bg='blue2')
        self.Nextbutton.place(x=380,y=125)
        self.Nextbutton['state'] = 'disable'

        self.Displayinfo = scrolledtext.ScrolledText(height=1,width=38)
        self.Displayinfo.place(x=50,y=160)        
        self.Displayinfo.insert(tk.END,'Enter Server & Database name and click connect.')        

        self.loginwin.mainloop()

    def __ConnectServer(self):

        try:
            
            server = self.ServernameHolder.get()
            db = self.DbHolder.get()
            driver = 'ODBC Driver 17 for SQL Server'

            if server != '' and db != '':
            
                ENGINE = create_engine('mssql+pyodbc://{}/{}?driver={}'.format(server, db, driver))
                self.conn = ENGINE.connect()

                self.Displayinfo.delete("1.0",tk.END)
                self.Displayinfo.insert(tk.END,'Connected..')

                self.ConnectServer['state'] = 'disable'
                self.DisconnectServer['state'] = 'normal'
                self.Nextbutton['state'] = 'normal'

            else:
                self.Displayinfo.delete("1.0",tk.END)
                self.Displayinfo.insert(tk.END,'Please enter Server & Datbase name and try again..')

        except Exception as e:
            self.err = str(e)
            messagebox.showerror(title='Unable to Connect', message=self.err)

    def __DisconnectServer(self):

        try:
            
            self.conn.close()
            
            self.Displayinfo.delete("1.0",tk.END)
            self.Displayinfo.insert(tk.END,'Connection Closed..')

            self.ConnectServer['state'] = 'normal'
            self.DisconnectServer['state'] = 'disable'
            self.Nextbutton['state'] = 'disable'

        except Exception as e:
            self.err = str(e)
            messagebox.showinfo(title='Disconnected', message=self.err)
            



GUI = DB_Scan()


#Uninstall Pkgs

import subprocess
import pkg_resources
pkgs = [pkg.key for pkg in pkg_resources.working_set]

for pkg in pkgs:
    subprocess.call(['pip','uninstall', '-y',pkg])
	
#Unzip 

import zipfile, sys 

ZIP_LOC = "C:\\Users\IN10011418\Desktop\locked.zip"
UNZIP_LOC = "C:\\Users\IN10011418\Desktop"

def Unzipping(ZIP_LOC, UNZIP_LOC):
    '''
    ZIP_LOC Path where ZIP folder is present.
    UnZIP_LOC Path where folder will be extracted.
    
    ** Example **
    ZIP_LOC = "C:\\Users\IN10011418\Desktop\locked.zip"
    UNZIP_LOC = "C:\\Users\IN10011418\Desktop"    
    '''
    
    try:

        with zipfile.ZipFile(ZIP_LOC) as ZIP:
            ZIP.extractall(UNZIP_LOC)

        print("Unzipping Done...")

    except Exception as err:
        print(err)
        sys.exit(1)

Unzipping(ZIP_LOC,UNZIP_LOC)

#Watchdog

import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class EventHandler(FileSystemEventHandler):

    def on_created(self, event):
        # print(event)
        print(event.src_path)

event_handler = EventHandler()
observer = Observer()
observer.schedule(event_handler, 'C:/Users/IN10011418/Desktop/FTP', recursive=True)
observer.start()

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


#windows service

import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import subprocess
import sys

class MyService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'Pubsub_Service'
    _svc_display_name_ = 'Pubsub_Service_Python'

    def __init__(self, args):
        super().__init__(args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_running = True

    def SvcStop(self):
        """
        Stop the service.
        This function stops the service by reporting its status as "SERVICE_STOP_PENDING",
        setting an event to indicate that the service should stop, and updating the
        `is_running` flag to False.
        """
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_running = False

    def SvcDoRun(self):
        """
        Run the main logic of the service.
        This function is the entry point of the service and is responsible for executing the main logic of the service.
        It calls the `main()` function, which contains the main logic of the service.
        If any exception occurs during the execution of the main logic, it is caught and logged using the `log_error()` function.
        The exception details are also logged using the `traceback.format_exc()` function.
        """
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        try:
            self.main()
        except Exception as e:
            self.log_error(f"Error occurred: {str(e)}")

    def main(self):
        """
        Runs the main function.
        This function executes the main logic of the program. It runs a Python script called 'Pubsub_V1.py' using the `subprocess.run()` method. 
        """
        pubsub_script = 'C:/Users/IN10011418/OneDrive - R1/Scripts/PYTHON/Win_Service/Pubsub_V1.py'
        subprocess.run(['python', pubsub_script])

def run_service():
    """
    Run the service.
    This function checks if any command-line arguments are passed to the script. If no arguments are provided, 
    it initializes the service manager, prepares to host a single service (MyService), and starts the service control dispatcher. 
    If arguments are provided, it handles the command line using the win32serviceutil module.
    """
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MyService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MyService)

if __name__ == '__main__':
    run_service()


#file watch process

import time
import os
import subprocess
import multiprocessing
from functools import partial
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor

def Main(FileProcess, event_queue):

    class EventHandler(FileSystemEventHandler):
        def on_created(self, event):
            event_queue.put(event)
            # FileProcess(os.path.basename(event.src_path))
            
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    while True:
        try:
            with ThreadPoolExecutor(max_workers=50) as executor:
                while True:
                    '''
                    Get the next file from the queue.
                    '''
                    event = event_queue.get()
                    # print(event.src_path)
                    executor.submit(FileProcess, os.path.basename(event.src_path))
            time.sleep(1)

        except KeyboardInterrupt:
            observer.stop()
        observer.join()

def FileProcess(__parm):

    print("File detected :: ", __parm ,"\nRunning batch file...\n")

    ''' 
     The subprocess module allows you to run commands in a non-blocking way,
     so that the process can continue to watch the directory for new files
    '''
    
    try:
        BatchProcess = subprocess.Popen(["C:/Users/IN10011418/Desktop/FTP/Sample_batfile.bat"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = BatchProcess.communicate()

        if BatchProcess.returncode != 0:
            print(f"There was some error while running the batch file :: {stderr.decode()} ")
        else:
            print("Batch file completed.") #{stdout.decode()}
    except Exception as err:
        print(err)


path = "C:/Users/IN10011418/Desktop/FTP/FTP_Locations"

if __name__ == '__main__':
    ''' 
    multiprocessing.Queue() is a multi-producer, multi-consumer queue.
    that is thread-safe and can be used to pass data between processes.
    '''
    event_queue = multiprocessing.Queue()
    watch_process = multiprocessing.Process(target=Main, args=(partial(FileProcess), event_queue))
    watch_process.start()
    watch_process.join()
         

#################################################################################################################################################################

#Sample 

# import os
# import zipfile
# import cryptography.hazmat.primitives.ciphers as ciphers
# import cryptography.hazmat.primitives.ciphers.algorithms as algorithms
# import cryptography.hazmat.primitives.ciphers.modes as modes
# import cryptography.hazmat.backends as backends

# # AES decryption function
# def decrypt_aes(key, iv, encrypted_file):
#     backend = backends.default_backend()
#     cipher = ciphers.Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)
#     decryptor = cipher.decryptor()
#     decrypted_file = decryptor.update(encrypted_file) + decryptor.finalize()
#     return decrypted_file

# # Load encrypted file and key
# encrypted_file = open("txt.zip.aes.tag", "rb").read()
# key = b"your_decryption_key"
# iv = b"your_initialization_vector"

# # Decrypt the AES-encrypted file
# decrypted_file = decrypt_aes(key, iv, encrypted_file)

# # Write the decrypted file to disk
# open("txt.zip", "wb").write(decrypted_file)

# # Extract the contents of the zip file
# zip_file = zipfile.ZipFile("txt.zip")
# zip_file.extractall()

# ---------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------


# import subprocess

# url = "https://api.usw2.pure.cloud/api/v2/downloads/4363d8cd2840e380"

# subprocess.run(['start', url,], shell=True)

# ---------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------

# from datetime import datetime

# Today = datetime.now()
# LogTime = Today.strftime("%m-%d-%Y")

# with open(f"{str(LogTime)}.log", "w") as f:
#     f.close()

# ---------------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------------


# import smtplib
# from email.message import EmailMessage
# import time
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText


# _Host = 'uatsmtp.accretivehealth.local' 
# _Port = 25
# _From = 'NoReply@accretivehealth.com'
# Success_To = 'asingh937@r1rcm.com'


# def __SuccessEmailAlert(MSG, num_retries = 2, retry_delay = 30):
#     '''
#     This will send success alert mails.
#     '''
#     retry_count = 0
#     while retry_count < num_retries:
         
#         try:
#             print("Sending email notification.")

#             msg = EmailMessage()
#             msg['From'] = _From
#             msg['To'] = Success_To
#             msg['Subject'] = f"SUCCESS ALERT"
#             msg.set_content( f"""{MSG}/n/n""")

#             with smtplib.SMTP(_Host,_Port) as server:
#                 server.send_message(msg)
#                 server.quit()
            
#             print("Mail sent successfully.")

#             break

#         except Exception as err:
#             retry_count += 1

#             if retry_count < num_retries:
#                 time.sleep(retry_delay)
#             else:
#                 print(f"Unable to send email. {str(err)}")

# MSG = """
# Data loaded successfully into ETL_SCI_Indiana contact list.

# ===============================================================
#  Total rows to be loaded :: 340.                             
#  Total rows loaded :: 340.                                   
#  Success Percentage :: 100.00%                               
# ===============================================================

# Total time taken :: 0 hr 0 min 18.873045 sec
# """

# __SuccessEmailAlert(MSG)



# url = 'https://api.usw2.pure.cloud/api/v2/downloads/27cabeaf211c63b2'
# username = 'asingh937@r1rcm.com'
# password = 'Genesys@2023'


# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.service import Service as ChromeService 
# from webdriver_manager.chrome import ChromeDriverManager 
# from selenium.webdriver.chrome.options import Options

# import time

# chrome_options = Options()
# chrome_options.add_argument('--headless')
# chrome_options.add_argument('--disable-gpu')
# # Initialize the Chrome browser
# browser = webdriver.Chrome(chrome_options=chrome_options)


# browser.get(url)

# time.sleep(5)


# username_field = browser.find_element(By.XPATH,'//*[@id="email"]')
# username_field.send_keys(username)

# password_input = browser.find_element(By.XPATH,'//*[@id="password"]')
# password_input.send_keys(password)

# login_button = browser.find_element(By.XPATH, '//button[text()="Log In"]')
# login_button.click()


# import os
# import datetime

# def get_matching_file(folder_path):
#     current_date = datetime.datetime.now().strftime('%Y%m%d')
#     for file_name in os.listdir(folder_path):
#         if file_name.endswith(current_date+'.csv'):
#             return os.path.join(folder_path, file_name)
#     return None


# folder_path = 'C:/Users/IN10011418/OneDrive - R1/Desktop/FTP/'
# matching_file = get_matching_file(folder_path)

# if matching_file:
#     print(f"Found matching file: {matching_file}")
# else:
#     print("No matching file found")

# import pandas as pd

# df = pd.read_csv('C:/Users/IN10011418/OneDrive - R1/Desktop/FTP/74f903a5-d09d-4435-8563-73637c944cd2-ETL_SCI_FLPEN_20230511.csv')
# from sqlalchemy import create_engine

# TABLE_NAME = 'Genesys_Export_Raw'
# SERVER_NAME = 'DEVRHUBIWFEX01.R1RCM.TECH' #'CCCCORSQL.accretivehealth.local'
# DATABASE = 'FileExchange'
# DRIVER = 'ODBC Driver 11 for SQL Server'


# ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

# cnx = ENGINE.connect()

# df = df.astype(dtype='object')

# df.fillna('NULL',inplace=True)


# # row = df.to_sql(name=TABLE_NAME, con=cnx, schema='dbo', index=False, method='multi', if_exists='append')

# # print(row)


# pattern = r'[^\w\s]'

# # Loop through each column of the DataFrame
# for col in df.columns:
#     # Use the str.contains method to find all special characters in the column
#     special_chars = df[col].str.contains(pattern)
    
#     # Print the column name and the special characters found
#     print(col, list(df[col][special_chars]))





# from sqlalchemy import create_engine,text

# SERVER_NAME= 'DEVRHUBIWFEX01.R1RCM.TECH'
# DATABASE= 'TranBAOK'
# DRIVER = 'SQL Server Native Client 11.0'
# facilityCodeList={'BAOK'}

# ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
# cnx = ENGINE.connect()
# print("Ram")

# for item in facilityCodeList:

#     Update_records =f''' Update Registrations Set IsCancelled=1 Where FacilityCode<>'BAOK' and IsCancelled<>1'''

#     with cnx.begin() as transaction:
#         print("Ram1")
#         cnx.execute(text(Update_records))
#         print("Ram2")
#         transaction.commit()
#         print(Update_records)

# cnx.close()

# import PureCloudPlatformClientV2
# import re

# CLIENT_ID = '667becd3-4cef-434d-8cb8-608afe2fac06'
# CLIENT_SECRET = '2hleuwbtQEm8oWyFVg5o-dyeDfzMU78L-SAWmL7BGIc'
# ORG_REGION = 'us_west_2'
# CAMPAIGN_ID = '81cd367c-a67c-43fe-9133-81b281d0ce96' 
# # CAMPAIGN_ID = '5206fa37-68ac-4f5a-918f-6ee7420cc4c2'

## region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]

# # print(region)

# PureCloudPlatformClientV2.configuration.host = region.get_api_host()

# api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)

# # print(api_client)

# PureCloudPlatformClientV2.configuration.access_token = api_client.access_token

# outbound_api = PureCloudPlatformClientV2.OutboundApi()

# # print(outbound_api)

# try:
#     # Get campaign progress
#     api_response = outbound_api.get_outbound_campaign_progress(CAMPAIGN_ID)
#     print(api_response)
    

# except Exception as e:
#     print("Exception when calling OutboundApi->get_outbound_campaign_progress: %s\n" % e)

# response = str(api_response)
# regex = r"'percentage'\s*:\s*(\d+)"
# match = re.search(regex, response)
# percentage_value = match.group(1)

# print(percentage_value)


# regex = r"'percentage'\s*:\s*(\d+)"
# match = re.search(regex, response)
# percentage_field_with_value = match.group(0)

# print(percentage_field_with_value)


# regex = r"'percentage'\s*:\s*(\d+)\b"
# match = re.search(regex, response)

# if match:
#     percentage_field_with_value = match.group(0)
#     print(percentage_field_with_value)

# else:
#     print("No percentage field found.")


# import pyodbc

# conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
#                         'Server=ALPTRAN27.accretivehealth.local;'
#                         'Database=tranPHTX;'
#                         'Trusted_Connection=yes;')

# print(conn)

# conn.close()

# print("\n\nChecking Available SQL Server Drivers.")

# import pyodbc

# drivers = pyodbc.drivers()

# print("\nAvailable Drivers...\n")

# for driver in drivers:

#     print(driver) 



# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# import time

# username = 'asingh937@r1rcm.com'
# password = 'Genesys@2023'

# options = webdriver.EdgeOptions()
# options.add_experimental_option('excludeSwitches', ['enable-automation'])
# browser = webdriver.Chrome(options=options)


# browser.get("https://api.usw2.pure.cloud/api/v2/downloads/6f1f03bade75ad0e")

# wait = WebDriverWait(browser, 30)
# email_field = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="email"]')))

# username_field = browser.find_element(By.XPATH,'//*[@id="email"]')
# username_field.send_keys(username)

# wait = WebDriverWait(browser, 30)
# pass_field = wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="password"]')))

# password_input = browser.find_element(By.XPATH,'//*[@id="password"]')
# password_input.send_keys(password)

# wait = WebDriverWait(browser, 30)
# login_field = wait.until(EC.visibility_of_element_located((By.XPATH, '//button[text()="Log In"]')))

# login_button = browser.find_element(By.XPATH, '//button[text()="Log In"]')
# login_button.click()

# time.sleep(300)

# browser.close()



# import PureCloudPlatformClientV2
# from PureCloudPlatformClientV2.rest import ApiException
# from pprint import pprint


# CLIENT_ID = '667becd3-4cef-434d-8cb8-608afe2fac06'
# CLIENT_SECRET = '2hleuwbtQEm8oWyFVg5o-dyeDfzMU78L-SAWmL7BGIc'
# ORG_REGION = 'us_west_2'

# #region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
# PureCloudPlatformClientV2.configuration.host = region.get_api_host()

# api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)

# PureCloudPlatformClientV2.configuration.access_token = api_client.access_token

# # create an instance of the API class
# api_instance = PureCloudPlatformClientV2.DownloadsApi()
# download_id = '6f1f03bade75ad0e' # str | Download ID

# issue_redirect = False # bool |  (optional) (default to True)
# redirect_to_auth = False # bool |  (optional) (default to True)

# try:
#     # Issues a redirect to a signed secure download URL for specified download
#     api_response = api_instance.get_download(download_id, issue_redirect=issue_redirect, redirect_to_auth=redirect_to_auth)
#     pprint(api_response)
# except ApiException as e:
#     print("Exception when calling DownloadsApi->get_download: %s\n" % e)

# url = "'url': 'https://prod-usw2-dialer.s3.us-west-2.amazonaws.com/contact-lists/exports/ca68a571-82f0-4ff7-ad97-a6156f12a92d-ETL_IMH_GDialer.csv?response-content-disposition=attachment%3Bfilename%3D%22ca68a571-82f0-4ff7-ad97-a6156f12a92d-ETL_IMH_GDialer.csv%22&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLXdlc3QtMiJHMEUCIEPMcMM2i2bX43VI4fRKPu9SyDQ1GeC3zd0%2Fnls0FpoNAiEA5O5kDExLdXL0dAIwgao1o0G9orGdwEzgnnyG0GlBo6MqsgUIQhAAGgw3NjU2Mjg5ODU0NzEiDAwgc83CUC1%2BVv875CqPBcIlsdxo3Gn78u7XocMM3ymQzEsUE5C4QsgARge8HDeOOZsJk0SnP9PzAt7o7FTxlIFGk%2FHSvWUaFkyojWQ7ZJ2atdz%2BR70WbwFKv6qiGotGIBCzujVQ2jGlxq7Qcs1eHkFBKNPhjvBosmfM5K24ONh9%2FgbyF6uJM0kMspTd%2B29KtK%2BD9sf1iOAoRFo9UDK6RVDKB4Wif5KwLAFTOf1FIjuViAGmoN%2BS%2BHzqBfGaZBTfBaNQTnL%2BdSZO0pDYo9143NT4xZ5XelIAVrMquZO2iDFxrA7MpWMw6yL9IKzLx%2BzGMmUjlE5SED2kP5fNSlBE0GbK2B3SnBXCfSJXdWlVliFKogWPqoZrlKuG9aaUfJtpMC4NzFRMtV4M0wVmd5sprmErc1NXfWG19JVnjx%2BNRiX8afyphELY8ELoXHLbesd3IyJuivFSgI1JiccOZprJmGsqo9PkZVcKlEF8fSZADGinKWNWq5NQvAaxlBf%2FOfMjcP1NmWfvRLfkhZ1oC6%2FBxPwlHaDDA49g8lt%2FQlQ6wQZlZiJFzUlDYcdpc1dxZps192sKoASN%2B9pIJC%2BwhC44kMW71IkR9PdA3JenWzVSja2xbrdYzZ4sthGtLe5dYmPxXaosKcgtj4jQUtNr3O2pPjwHxXh%2BsvZIKZdxSb4PTHrdYLsqRQCByqzcd5b7m0CxDf4nDOcgQP3hxdpK1FlwH%2FPDDRLUVX9eBTdIVdMRYHOcvjsA4jf9fZMfbzQp6%2B9w0IXp7m794QZEezwouCPKroYnGx0WLdiV946yHK4bIOblWGkfM6P8pK0AqX%2Fv1jeOKOSX8Dpa72096DAxrDETyYTS32wRngFrqwAb1Uhk0xFzq3vlrfVtzymSGPzjrx0wz7PlpAY6sQHiZnqnXy87HSXDlGseGXqj74IJOENYKi0v45tuseIDnlyJVwgzFj2GdKDwMHc4Egez8I5gmMKKNdKN8tqiIG5z%2BFZXx7EW0s1CIWV7ghaTkBv87mTn81Y0rTCMlp%2FxJY4CA%2F3dqA9QN6bdpl25UGAJB7sTPcnAbDoZYFHG0mlUuHrAC7pSdJm9GSbdBbLEj%2BwAX9Hfj7oLIP%2BTc1t6OmMex7hzQBmWOSYaelwNBHGagVI%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230626T102915Z&X-Amz-SignedHeaders=host&X-Amz-Expires=86400&X-Amz-Credential=ASIA3EQYLGB7SUDS7YIO%2F20230626%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=945d3c5c761c053195325abcb44bc149ecbf323221606b1ed9ee50058d9d1d7c'"

# import re


# regex = r"url': '(.*?)'"
# match = re.search(regex, str(url))
# uri = match.group(1)

# print(uri)

# import re

# url = "https://api.usw2.pure.cloud/api/v2/downloads/6f1f0de75ad0e"
# match = re.search(r'/([a-fA-F\d]+)$', url)
# if match:
#     print(match.group(1))


# import webbrowser

# webbrowser.open("https://prod-usw2-dialer.s3.us-west-2.amazonaws.com/contact-lists/exports/14cccf52-840c-46f3-a7b0-8c99a0003620-ETL-Titans_Contact_DEV.csv?response-content-disposition=attachment%3Bfilename%3D%2214cccf52-840c-46f3-a7b0-8c99a0003620-ETL-Titans_Contact_DEV.csv%22&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLXdlc3QtMiJIMEYCIQC14c3v7loRy0%2B1wCoS00lfTs4vmKuD%2BDqtpxW8mqGSZQIhANWAUGN7x%2Fm50CEuvsGYFTSMPwZbw4oWQkHWpscovzBiKrIFCEMQABoMNzY1NjI4OTg1NDcxIgzTR8haOMwn8YKJrqEqjwXeNhvQotws1vcnFmbXMatMkp43i8qX140BbSjhHCo80SYdeomwwM3%2FNhFKyl1%2Bl%2F19rpgbMZ666cYMOPN0fFqkQKPAJBvTSORFd9DW%2FKylmbKWmXyfxwDbiJ5CabgPKPrpn9GZGhL6CMD9g1MGPVJzQYEMbK7V1CHdGzoaYT0k8JezLV7hMCBmThCmxxKE7J5x%2BTQElAH1JmvG3K2p6LoYJxTTozw%2FfY6jlvnYN%2B1iT8Vu1D1nr5kigghQR4sgfQc0rIgOFKeSIkACRTZt09cTvTtTT8hqYZmwH%2F9LkXgQv5IqI1t3IcOiPZDx3b6kFuOjnDn5VENI6Lrkxr9vdeIAqkR3ycfUK4ikiEsciPFqdLxmGNpmtahGeHijDPIwCqYKeBj%2BRboToZS6g5PFMRFz7%2BBTGbejxCw4%2Flp1Q2MisK%2FWkO5ULBjNI%2BAY1ls2%2BcMW2VWZX4lgBKh9Y6aG4KdlA91QLsvmJatqF93CTb%2Fw39BL%2FOXSbcurv8gqtqGDL%2FB1UekZh0BowKM1kq2%2FStuXxiSXCK%2Fr7hmIH%2Be0ZSYr%2B1W8g6SmCS38aMs3ZsIqJ1TEdbFhCklFXtMsw70LNLfveqX6sqniq2fyW5VxBXpIuuE9g7jFWwGpTtg5vyETujnm7qSPOaMcEi3FiO75DC1jmDC6bszqWHbVEmyVGY7K%2BWaGvReZhjh0I%2F%2Begj%2FnDlaw3Xdky%2F1UBQhXJwePU1usy8s4NP5CQtDor42XClFw8%2BZU4rH6egOc0dCDo8md6G7yvfxWJDwxN1EelBdNz%2Fry6iAyrWKl28bxDIvYhlMeCvq0pD%2BYC5LkS04NvS1L5kYYf4PORuOQrH6B4o2h58ULrZU7d%2B0%2FKVlW8gi9b5MxMJXO5aQGOrABr8O0AzgPOqUygpDWiWQ0tx%2BLMdgBbq7ooU0axonGRsvziMV%2FqhDBz7c%2BBvEOYnDj1FGPV5WA8b5BhE1faHpoDdktQYDexCi6NRBppNS1Xnu2WSyuKZzW3lOQfGTSdboxumPBWWmKHO%2FxCQpIHQg%2Bq12O8p7h4qMXkoK6Muon3kwy69p8rTeQ9Z72QU%2BbQez%2BKM15SBtTcvwVFUsJQBRaDg5WSAQBvdGv4WrpfRr5Vqg%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230626T104939Z&X-Amz-SignedHeaders=host&X-Amz-Expires=86400&X-Amz-Credential=ASIA3EQYLGB75HZUGDMK%2F20230626%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=004c302cd894befa313c8f195da3e41fb20b108060d55a677cb7a53953dbcd61")

# import requests

# url = "https://prod-usw2-dialer.s3.us-west-2.amazonaws.com/contact-lists/exports/ca68a571-82f0-4ff7-ad97-a6156f12a92d-ETL_IMH_GDialer.csv?response-content-disposition=attachment%3Bfilename%3D%22ca68a571-82f0-4ff7-ad97-a6156f12a92d-ETL_IMH_GDialer.csv%22&X-Amz-Security-Token=IQoJb3JpZ2luX2VjENz%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLXdlc3QtMiJHMEUCIH4lOMg2ncIJBp7IHhHQMoGcwneX89K0NzcPwGv3ozuhAiEA18BG%2BudhavB9ig5005lxg3brgYjaZHuB7AQQJK%2BLfeIqsgUIRRAAGgw3NjU2Mjg5ODU0NzEiDKdrCLvEJMrIPgHikiqPBV%2BYUAcTKbc3gDeE5c7%2FppWODA7TfEQ%2Ffi7A64YHzREG7CbpgxzIfsN%2FKv7AzH1nc%2Bj5NPeuNJeZO4yjF0J%2BzU4bjOkn7%2F8%2B%2FPVAujRc0bHJ%2Bgn5XcvEKDXM2Yfcsoc4te7GzRuEaTaWqU4XMXNA8wbiII0E2AKsDkPyH1A%2BKkKhhRYdQvaFdlkm6BpPgnlubtZixGvncaCyLKHgYtFEM2%2FY6m09YpOTbqWIheVOqGd7tGOH6%2FoIiae1Hl5D7%2BMfRRL8R8tUVcMzh2%2FZY18Hn3DZqBnt3eTudjEDxLiqFiYh9lXp6GXZpbOcTygGraaTCwZ7tSk4mET%2BkEnrY%2FfQsZ8zM6IAyElTOiIc76yHgs41BhSNA9oxlZmjr63caHcxF8da77z4Z%2FjbFHHTNRl670HOLx12%2BxI%2BWdglk7auvnUH85KRZ6j9TiePF3pUDqRCFVbwhh8O7hd7Y90c%2F5X9pbZZDHX1OpiOs8wBFb2%2FlC5h4%2BeOS84OS%2FcEiSRgKZTHG3NGIfVJY7Q81KTVV7w496qRvzvWiUVSAab78GWNXJRv4BaD8HQN0ajVQzIA444Om7mtmZYVfN%2FeDm8Tr5uch92gXkzqZAuP3a2mtXtW2Lc2qC1uRB2vSpbuRTtFm2slEwVPieHaPPsXZ18vOxrmE44MQ%2FVrzygcccz%2B1iSKpgPa6Q8hYCW7c7aDnisX11FwFeL9oFGcPuFMRUrmW5RCzYH2K6MV2EZWDa04DgfmYIo5YUPbZEN0iPnDqPRRRl7CCFvZdW73u9AtZpM2n%2F2SpjK1mbUaAXv9xouU7pvZgca%2BzmyrY5xiy2awXYtWuUrrcIn3o1xonkX1cyzSiEZvtSKCO5402%2BXAhFDmwQ8qyrYw%2BIDmpAY6sQF%2BiHrlM8h2Bg%2FikQedjR2Awn2fTbMTweztVuQZW%2BXn6BMVxpHfh02oL%2F9vvCgEz%2FUSxQ%2BaobXv4BjjIphE68UCe7rKex7QReQP7X%2B5G3ycBO3X3eCdwJIhGJ6YL6QTdv%2Fk2gonBiLAt7%2FbxxP3hX5zhFLhyXwfeRIQ07MbvVuop99piDuv8oGRTEWEMgMCPD25VZ938hHPJ2iagtXtd0bDdneeqgIpKiF16hHXtHhHO8E%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20230626T132211Z&X-Amz-SignedHeaders=host&X-Amz-Expires=86400&X-Amz-Credential=ASIA3EQYLGB7XOQ6I7D4%2F20230626%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=8f5833f585226aa1a8ea503e5fc2e25c4d09d89f503340156d7410aa8104ece8"

# res = requests.get(url=url)

# # print(res.content)
# import urllib.request  

# urllib.request.urlretrieve(url=url, filename='Output.csv')



# import PureCloudPlatformClientV2
# from PureCloudPlatformClientV2.rest import ApiException
# from pprint import pprint

# CLIENT_ID = '667becd3-4cef-434d-8cb8-608afe2fac06'
# CLIENT_SECRET = '2hleuwbtQEm8oWyFVg5o-dyeDfzMU78L-SAWmL7BGIc'
# ORG_REGION = 'us_west_2'

# #region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
# PureCloudPlatformClientV2.configuration.host = region.get_api_host()


# api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)
# outbound_api = PureCloudPlatformClientV2.OutboundApi(api_client)

# # Configure OAuth2 access token for authorization: PureCloud OAuth
# PureCloudPlatformClientV2.configuration.access_token = api_client.access_token
# # or use get_client_credentials_token(...), get_saml2bearer_token(...) or get_code_authorization_token(...)

# # create an instance of the API class
# api_instance = PureCloudPlatformClientV2.UtilitiesApi()

# try:
#     # Get public ip address ranges for Genesys Cloud
#     api_response = api_instance.get_ipranges()
#     pprint(api_response)
# except ApiException as e:
#     print("Exception when calling UtilitiesApi->get_ipranges: %s\n" % e)


# from sqlalchemy import create_engine, event
# import pandas as pd


# SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
# DATABASE ='Srdial'
# DRIVER = 'SQL+Server'
# TABLE_NAME = 'MFS_Export_GenesysRaw'
# FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-Test.csv'


# engine = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver=ODBC Driver 17 for SQL Server',fast_executemany=True)

# # Path to your CSV file
# csv_file_path = FTP

# # Read the CSV file into a DataFrame
# df = pd.read_csv(csv_file_path,sep=',',low_memory=True)
# df = df.astype(str)

# # Table name where you want to insert the data
# tbl = TABLE_NAME

# # Event listener function for fast_executemany
# @event.listens_for(engine, "before_cursor_execute")
# def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
#     if executemany:
#         cursor.fast_executemany = True

# # Bulk insert operation
# df.to_sql(tbl, engine, index=False, if_exists="append", schema="dbo")


# import os
# import datetime
# import time
# import pandas as pd
# from sqlalchemy import create_engine,event
# import concurrent.futures
# import gc
# gc.enable()

# startT = time.time()

# SERVER_NAME ='DEVCONTWCOR01.r1rcm.tech'
# DATABASE ='Srdial'
# DRIVER = 'ODBC Driver 11 for SQL Server' #'SQL Server Native Client 11.0' #ODBC Driver 13 for SQL Server #ODBC Driver 17 for SQL Server
# TABLE_NAME = 'MFS_Export_GenesysRaw'
# FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/MFS-Test.csv'
# CHUNK_SIZE = 10000
# MAX_THREADS = 10

# SERVER_NAME = 'DEVRHUBIWFEX01.R1RCM.TECH'
# DATABASE = 'I3IMH01'
# DRIVER = 'ODBC Driver 11 for SQL Server'
# TABLE_NAME = 'Genesys_Export_Raw_IMH'
# FTP = 'C:/Users/IN10011418/OneDrive - R1/Desktop/IMH-Test.csv'
# CHUNK_SIZE = 10000
# MAX_THREADS = 10

# insert_records_failure_flag_counter = 0
# rows_inserted = 0
# insertion_err = ''
# insert_records_failure_flag = True

# try:

#     print(f"Connecting to Server to insert data into table :: {TABLE_NAME}.")
#     ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

# except Exception as e:

#     print(f"Unable to connect to server :: {SERVER_NAME} err_msg :: {e}.")


# def get_matching_file(FTP):

#     try:
#         print(f"Looking for file in path :: {FTP}")

#         current_date = datetime.datetime.now().strftime('%Y%m%d')

#         for file_name in os.listdir(FTP):
#             if file_name.endswith(current_date+'.csv'):
#                 print(f"File found :: {file_name}")
#                 return os.path.join(FTP, file_name)
        
#         print(f"No matching file found for system current date :: {current_date} in folder :: {FTP}.")
#         return None
    
#     except Exception as e:
#         print(f"Error occured while looking for file in folder :: {FTP}. err_msg :: {e}")

# def insert_records(chunk):

#     try:
#         global rows_inserted, insert_records_failure_flag,insertion_err,insert_records_failure_flag_counter

#         chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
#         chunk.fillna('NULL', inplace=True)

#         with ENGINE.connect() as CONN:
#             chunk.to_sql(TABLE_NAME, CONN, index=False, if_exists="append", schema="dbo")
#             CONN.commit()
        
#         CONN.close()
#         rows_inserted += len(chunk)

#     except Exception as e:

#         CONN.rollback()
#         insertion_err += str(e)

#         insert_records_failure_flag_counter += 1

#         print(f"Unable to insert data in table :: {TABLE_NAME}. err_msg :: {insertion_err}")


# def create_chunk(df):

#     global insertion_err

#     chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

#     with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

#         @event.listens_for(ENGINE, "before_cursor_execute")
#         def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
#             if executemany:
#                 cursor.fast_executemany = True

#         futures = []

#         print(f"Inserting data into table :: {TABLE_NAME}.")

#         for chunk in chunks:
#             future = executor.submit(insert_records,chunk)
#             futures.append(future)

#         for future in concurrent.futures.as_completed(futures):
#             print(future)

    
#     print(f"Data inserted successfully into table :: {TABLE_NAME}.")
#     print(f"Total number of rows inserted :: {rows_inserted}.")
#     print("Process completed successfully.")

#     Success_percentage = (rows_inserted/len(df)) * 100
#     Success_Per = "Success Percentage :: {:.2f}%".format(Success_percentage)

#     print(Success_Per)

#     endT = time.time()
#     TotalT = endT - startT
#     hours, remainder = divmod(TotalT, 3600)
#     minutes, seconds = divmod(remainder, 60)

#     Script_Time = "Total time taken :: {:.0f} hr {:.0f} min {:f} sec".format(hours, minutes, seconds)

#     print(Script_Time)

# if __name__ == '__main__':

#     matching_file = FTP

#     if matching_file:
#         df = pd.read_csv(matching_file,sep=',',low_memory=False)
#         create_chunk(df)

#     else:
#         print("No file found. Sys exit.")


# from sqlalchemy import create_engine,text

# SERVER_NAME1 = 'ALPFEX03.accretivehealth.local'
# SERVER_NAME2 = 'AHS-STAGE01.accretivehealth.local'
# SERVER_NAME3 = 'AHS-A2AFEX01.EXTAPP.LOCAL'
# DATABASE = 'FileExchange'
# DRIVER = 'ODBC Driver 11 for SQL Server'

# SQL = """
# Use FileExchange;
# Update FileExchange.dbo.Genesys_DataLoad_Metadata
# Set SQL_Query = 'Select cqCampName,cqCampId,cqOrderId,cqCampOrder,cqSourceDB,cqAppName,cqAppRecordId,cqFacility,cqPersonId,cqAccountNum,cqFirstName,cqMiddleName,cqLastName,cqStateCode,cqZipCode,cqLanguage,cqPhoneHome,cqPhoneWork,cqPhoneMobile,cqWorklist,cqWlstatus,cqOraCreatetime,cqOFModality,cqSmdepartmentname,cqRunDate,cqFlag,cqNotes,cqDestination,ZONE,STATUS,I3_IDENTITY,I3_UPLOAD_ID,WeightScore,cqPriority,FileDate,FileLogId,''+17022137266'' AS I3_CAMPAIGNOWNERID,I3_CAMPAIGNOWNERNAME,I3_CAMPAIGNOWNEREXPIRATION From i3_SCI_QHCMD.dbo.CallQueue With(NoLock) Where cqNotes <> ''Loaded To Genesys'';'
# Where [Database] = 'i3_SCI_QHCMD';
# """

# ENGINE1 = create_engine(f'mssql+pyodbc://{SERVER_NAME1}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
# ENGINE2 = create_engine(f'mssql+pyodbc://{SERVER_NAME2}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
# ENGINE3 = create_engine(f'mssql+pyodbc://{SERVER_NAME3}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

# cnx1 = ENGINE1.connect()

# with cnx1.begin() as transaction:

#     cnx1.execute(text(SQL))
#     transaction.commit()

# cnx1.close()

# cnx2 = ENGINE2.connect()

# with cnx2.begin() as transaction:

#     cnx2.execute(text(SQL))
#     transaction.commit()

# cnx2.close()

# cnx3 = ENGINE3.connect()

# with cnx3.begin() as transaction:

#     cnx3.execute(text(SQL))
#     transaction.commit()

# cnx3.close()

# import dash
# import dash_core_components as dcc
# import dash_html_components as html
# import pandas as pd

# # Sample data
# data = {
#     'Column1': [1, 2, 3, 4, 5],
#     'Column2': [10, 20, 30, 40000, 50],
#     'Column3': [100, 200, 300, 400, 500],
#     'Column4': [1000, 2000, 3000, 4000, 5000],
#     'Column5': [10000, 20000, 30000, 40000, 50000]
# }

# df = pd.DataFrame(data)

# # Create the app
# app = dash.Dash(__name__)

# # Define the layout
# app.layout = html.Div(
#     children=[
#         html.H1(children='My Dashboard'),
#         dcc.Graph(
#             id='graph',
#             figure={
#                 'data': [
#                     {'x': df['Column1'], 'y': df['Column2'], 'type': 'bar', 'name': 'Column2'},
#                     {'x': df['Column1'], 'y': df['Column3'], 'type': 'bar', 'name': 'Column3'},
#                     {'x': df['Column1'], 'y': df['Column4'], 'type': 'bar', 'name': 'Column4'},
#                     {'x': df['Column1'], 'y': df['Column5'], 'type': 'bar', 'name': 'Column5'},
#                 ],
#                 'layout': {
#                     'title': 'Genesys - Data & Export'
#                 }
#             }
#         )
#     ]
# )

# # Run the app
# if __name__ == '__main__':
#     app.run_server(debug=True)



# import os
# import datetime


# FTP = r'C:\Users\IN10011418\OneDrive - R1\Desktop'
# def get_matching_file(FTP):

#     current_date = datetime.datetime.now().strftime('%Y%m%d')

#     for file_name in os.listdir(FTP):
#         if file_name.endswith(current_date+'.csv'):   
#             return os.path.join(FTP, file_name)
            
#     return None

# if __name__ == '__main__':

#     matching_file = get_matching_file(FTP)

#     if matching_file:
#         Name = os.path.basename(matching_file)
#         print(Name)
#         if 'IMH' in Name:
#             print('IMH')
            
#         elif 'MFS' in Name:
#             print('MFS')
        
###############################################################################################################################################################

#GFE 

#Email Alert

import ServerConn
import smtplib
from email.message import EmailMessage
import Config

def __EmailAlert(__FailedFileName):
    '''
    This will send alert mails for failed file.
    '''
    try:
        print("Sending email notification.")
        msg = EmailMessage()
        msg['From'] = Config._From
        msg['To'] = Config._To
        msg['Cc'] = Config._Cc
        msg['Subject'] = f"{__FailedFileName} File Failed"
        msg.set_content( f"""FYI : File {__FailedFileName} failed data validation check.\nIt has been moved to failed file location.\n\n
                    """)
        with smtplib.SMTP(Config._Host,Config._Port) as server:
            server.send_message(msg)
            server.quit()
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__FailedFileName}','Email','SUCCESS','Mail sent Successfully.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")
    
    except Exception as err:
        print("Unable to send email.")
        
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__FailedFileName}','Email','FAILED','{str(err)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

#File Failed

import shutil
import ServerConn
import EmailAlert
import os
import sys

def __FileFailed(src_path,tgt_path,item):
    '''
    Logic to movie file into failed dir.
    '''
    try:
        if os.path.isfile(src_path):
            shutil.move(src=src_path,dst=tgt_path)
            print(f"{item} File has been moved to failure location.")

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{item}','File Failed','SUCCESS','File has been moved to failed file location.')""")
            except Exception as err:
                print("Unable to insert LoG data in table.")

            EmailAlert.__EmailAlert(item)

    except Exception as err:
        
        print("Unable to move file at failure location.")
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','File Failed','ERROR','{str(err)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")
        
        sys.exit(1)
		
#File Specific check

import pandas as pd
import pandera as pa
from pandera import Column , check , DataFrameSchema
import sys
import FileFailed
import ServerConn
import GFE_SqlConn
import ProcExecAlert

#*********************************************************************************************************************************

def __DuplicateCheck(DF,src_path,tgt_path,item):
    '''
    Check if there is duplicate row in file.
    '''
    try:
        print(f"Checking for duplicate in file {item}.")
        __dup = DF.duplicated()
        __dupCount = int(DF.duplicated().sum())
        __dupRow = DF[DF.duplicated()] #all the duplicate rows

        if True in set(__dup):
            print(f"{__dupCount} duplicate found in file {item}.")

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Duplicate Check','ERROR','{__dupCount} duplicate found in file {item}.')""")
            except Exception as err:
                print("Unable to insert LoG data in table.")

            FileFailed.__FileFailed(src_path,tgt_path,item)

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Sys Exit','ERROR','Code exit file will not be loaded.')""")       
            except Exception as err:
                print("Unable to insert LoG data in table.")   

            print("sys exit")
            #sys.exit(1)

        else:
            print(f"No Duplicate rows in file {item}.")

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Duplicate Check','SUCCESS','Duplicate check passed.')""")
            except Exception as err:
                print("Unable to insert LoG data in table.") 

    except Exception as err:
        
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Duplicate Check','FAILED','{str(err)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")
            
        print(str(err))
        
        sys.exit(1)
#*********************************************************************************************************************************

def __CO_Provider_GroupContact(DF,src_path,tgt_path,item):
    '''
    Pre Checks for Co-Provider group contact
    '''
    print("Starting Data validation for Co-Provider group contact.")

    Data_Check = DataFrameSchema(
        {
        "HospitalFacilityCode" : Column(nullable=False),
        "ProviderGroupName" : Column(nullable=True),
        "ProviderSpecialty" : Column(nullable=False),
        "StreetAddress" : Column(nullable=False),
        "City" : Column(nullable=False),
        "State" : Column(nullable=False),
        "ZipCode" : Column(nullable=False),
        "ContactPersonName" : Column(nullable=False),
        "ContactPersonPhone" : Column(nullable=False),
        "ContactPersonEmail" : Column(nullable=False),
        "NationalProviderIdentifier" : Column(nullable=False),
        "TaxpayerIdentificationNumber" : Column(nullable=True)
        }, strict=True, ordered=True)

    try:
        Data_Check.validate(DF, lazy= True)
        print("Validation Completed Successfully.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','SUCCESS','File data looks good.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

            #Truncate Temp Table           
        try:

            try:
                GFE_SqlConn.GFE_CONN.execute(f"Truncate Table [GFE].[dbo].{GFE_SqlConn.CoProGrpCont} ")
                print('Temp Table has been Truncated.')

            except Exception as err: 
                print('Unable to truncate temp table ::' + str(err)) 

            #Data load into Temp table. 

            #rows = DF.to_sql(name= GFE_SqlConn.CoProGrpCont, con=GFE_SqlConn.GFE_CONN, schema= 'dbo' ,index=False ,method='multi', if_exists='append')

            rows = DF.shape[0]

            DF = DF.astype('str')

            for index, row in DF.iterrows():

                GFE_SqlConn.GFE_CONN.execute(f""" Insert into dbo.{GFE_SqlConn.CoProGrpCont}
                                                    (ProfessionalGroupName,
                                                        ProviderSpecialty,
                                                        StreetAddress,
                                                        City,
                                                        State,
                                                        ZipCode,
                                                        ContactPersonName,
                                                        ContactPersonPhone,
                                                        ContactPersonEmail,
                                                        NationalProviderIdentifier,
                                                        TaxpayerIdentificationNumber) Values(?,?,?,?,?,?,?,?,?,?,?)""",
                                                        row.ProviderGroupName, row.ProviderSpecialty, row.StreetAddress, row.City, row.State, row.ZipCode, row.ContactPersonName,
                                                        row.ContactPersonPhone, row.ContactPersonEmail, row.NationalProviderIdentifier, row.TaxpayerIdentificationNumber)
            print('Data inserted into Temp.')

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Insert Temp','SUCCESS','Data inserted into temp table. Rows inserted :: {rows}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
                #Proc execution after Data load to Temp.
            try:
                __procname = 'GFE_StoreCoProviderGroupContactInformation'

                Tran = GFE_SqlConn.GFE_CONN.begin()

                z = DF["HospitalFacilityCode"].head(1).item()

                ProcStat = pd.read_sql(f"Exec [GFE].[dbo].{__procname} '{z}','Admin'", GFE_SqlConn.GFE_CONN)
                __procStatus = ProcStat.to_string(index=False)

                Tran.commit()

                print(f'{__procname} Proc executed successfully.' )

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','SUCCESS','Proc executed. msg :: {__procStatus}')""")
                    
                    ProcExecAlert.__ProcExecAlert(__procname, __procStatus)
                
                except Exception as err:
                    print('Unable to insert LoG data in table.')

            except Exception as err:

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','Failed','Proc not executed. {str(err)}')""")

                except Exception as err:
                    print('Unable to insert LoG data in table.')

                ProcExecAlert.__ProcFailAlert(__procname, str(err))

                print(f'{__procname} Proc not executed.')

                sys.exit(1)

        except Exception as err:
            print(f'Unable to insert data into Table :: {GFE_SqlConn.FeeSchd}'+ str(err))

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{item}','Insert Temp','FAILED','{str(err)}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
            sys.exit(1)

    except Exception as err:
        print("Validation Failed for Co-Provider group contact.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','FAILED','{str(err.failure_cases)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.") 

        FileFailed.__FileFailed(src_path,tgt_path,item)
        print("sys exit")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Sys Exit','ERROR','Code exit file will not be loaded.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

        sys.exit(1)
#*********************************************************************************************************************************

def __CO_Provider_GroupFee(DF,src_path,tgt_path,item):
    '''
    Pre Checks for Co-Provider group fee
    '''
    print("Starting Data validation for Co-Provider group fee.")

    Data_Check = DataFrameSchema(
        {
        "HospitalFacilityCode" : Column(nullable=False),
        "ServiceLine" : Column(nullable=True),
        "ProviderGroupName" : Column(nullable=False),
        "AcuteCPTCode" : Column(nullable=False),
        "ProfessionalCPTCode" : Column(nullable=False),
        "ServiceDescription" : Column(nullable=False),
        "StandardFee" : Column(nullable=True),
        "SelfPayFee" : Column(nullable=False),
        "EffectiveStartDate" : Column(nullable=False),
        "NationalProviderIdentifier" : Column(nullable=False)
        }, strict=True, ordered=True)

    try:
        Data_Check.validate(DF, lazy= True)
        print("Validation Completed Successfully.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','SUCCESS','File data looks good.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

            #Truncate Temp Table           
        try:

            try:
                GFE_SqlConn.GFE_CONN.execute(f"Truncate Table [GFE].[dbo].{GFE_SqlConn.FeeSchd} ")
                print('Temp Table has been Truncated.')

            except Exception as err: 
                print('Unable to truncate temp table ::' + str(err)) 

            #Data load into Temp table. 

            #rows = DF.to_sql(name= GFE_SqlConn.FeeSchd, con=GFE_SqlConn.GFE_CONN, schema= 'dbo' ,index=False ,method='multi', if_exists='append')

            rows = DF.shape[0]

            DF = DF.astype('str')

            for index, row in DF.iterrows():

                GFE_SqlConn.GFE_CONN.execute(f""" Insert into dbo.{GFE_SqlConn.FeeSchd}
                                                    (HospitalFacilityCode,                                                        
                                                        ProfessionalServiceLine,
                                                        ProfessionalGroupName,
                                                        AcuteCPTCode,
                                                        ProfessionalCPTCode,
                                                        ServiceDescription,
                                                        StandardFee,
                                                        SelfPayFee,
                                                        EffectiveStartDate,
                                                        NationalProviderIdentifier) Values(?,?,?,?,?,?,?,?,?,?)""",
                                                        row.HospitalFacilityCode, row.ServiceLine, row.ProviderGroupName, row.AcuteCPTCode, row.ProfessionalCPTCode, row.ServiceDescription, row.StandardFee,
                                                        row.SelfPayFee, row.EffectiveStartDate, row.NationalProviderIdentifier)
            print('Data inserted into Temp.')

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Insert Temp','SUCCESS','Data inserted into temp table. Rows inserted :: {rows}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
                #Proc execution after Data load to Temp.
            try:
                __procname = 'GFE_StoreProviderFeeSchedules'

                Tran = GFE_SqlConn.GFE_CONN.begin()

                ProcStat = pd.read_sql(f"Exec [GFE].[dbo].{__procname} 'Admin','{item}','CoProviderGroup'", GFE_SqlConn.GFE_CONN)
                __procStatus = ProcStat.to_string(index=False)

                Tran.commit()

                print(f'{__procname} Proc executed successfully.' )

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','SUCCESS','Proc executed. msg :: {__procStatus}')""")
                    
                    ProcExecAlert.__ProcExecAlert(__procname, __procStatus)
                
                except Exception as err:
                    print('Unable to insert LoG data in table.')

            except Exception as err:

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','Failed','Proc not executed. {str(err)}')""")

                except Exception as err:
                    print('Unable to insert LoG data in table.')

                ProcExecAlert.__ProcFailAlert(__procname, str(err))

                print(f'{__procname} Proc not executed.')

                sys.exit(1)

        except Exception as err:
            print(f'Unable to insert data into Table :: {GFE_SqlConn.FeeSchd}'+ str(err))

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{item}','Insert Temp','FAILED','{str(err)}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
            sys.exit(1)

    except Exception as err:
        print("Validation Failed for Co-Provider group fee.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','FAILED','{str(err.failure_cases)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.") 

        FileFailed.__FileFailed(src_path,tgt_path,item)
        print("sys exit")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Sys Exit','ERROR','Code exit file will not be loaded.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

        sys.exit(1)
#*********************************************************************************************************************************

def __CO_Provider_Individual_Contact(DF,src_path,tgt_path,item):
    '''
    Pre Checks for Co-Provider Individual contact
    '''
    print("Starting Data validation for Co-Provider Individual contact.")

    Data_Check = DataFrameSchema(
        {
        "HospitalFacilityCode" : Column(nullable=False),
        "ProviderFirstName" : Column(nullable=True),
        "ProviderLastName" : Column(nullable=True),
        "PhysicianNationalProviderIdentifier" : Column(nullable=False),
        "ProviderGroupName" : Column(nullable=True),
        "ProviderSpecialty" : Column(nullable=True),
        "StreetAddress" : Column(nullable=True),
        "City" : Column(nullable=True),
        "State" : Column(nullable=True),
        "ZipCode" : Column(nullable=True),
        "ContactPersonName" : Column(nullable=True),
        "ContactPersonPhone" : Column(nullable=True),
        "ContactPersonEmail" : Column(nullable=True),
        "GroupNationalProviderIdentifier" : Column(nullable=True),
        "GroupTaxpayerIdentificationNumber" : Column(nullable=True)
        }, strict=True, ordered=True)

    try:
        Data_Check.validate(DF, lazy= True)
        print("Validation Completed Successfully.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','SUCCESS','File data looks good.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

            #Truncate Temp Table           
        try:

            try:
                GFE_SqlConn.GFE_CONN.execute(f"Truncate Table [GFE].[dbo].{GFE_SqlConn.CoProIndvCont} ")
                print('Temp Table has been Truncated.')

            except Exception as err: 
                print('Unable to truncate temp table ::' + str(err))
            
            #Data load into Temp table.

            #rows = DF.to_sql(name= GFE_SqlConn.CoProIndvCont, con=GFE_SqlConn.GFE_CONN, schema= 'dbo' ,index=False ,method='multi', if_exists='append')

            rows = DF.shape[0]

            DF = DF.astype('str')

            for index, row in DF.iterrows():

                GFE_SqlConn.GFE_CONN.execute(f""" Insert into dbo.{GFE_SqlConn.CoProIndvCont}
                                                    (ProviderFirstName,
                                                        ProviderLastName,
                                                        PhysicianNationalProviderIdentifier,
                                                        ProviderGroupName,
                                                        ProviderSpecialty,
                                                        StreetAddress,
                                                        City,
                                                        State,
                                                        ZipCode,
                                                        ContactPersonName,
                                                        ContactPersonPhone,
                                                        ContactPersonEmail,
                                                        GroupNationalProviderIdentifier,
                                                        GroupTaxpayerIdentificationNumber) Values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                                        row.ProviderFirstName, row.ProviderLastName, row.PhysicianNationalProviderIdentifier, row.ProviderGroupName, row.ProviderSpecialty, row.StreetAddress,
                                                        row.City, row.State, row.ZipCode, row.ContactPersonName, row.ContactPersonPhone, row.ContactPersonEmail, row.GroupNationalProviderIdentifier, 
                                                        row.GroupTaxpayerIdentificationNumber)
            print('Data inserted into Temp.')

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Insert Temp','SUCCESS','Data inserted into temp table. Rows inserted :: {rows}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
                #Proc execution after Data load to Temp.
            try:
                __procname = 'GFE_StoreCoProviderIndividualContactInformation'

                Tran = GFE_SqlConn.GFE_CONN.begin()

                z = DF["HospitalFacilityCode"].head(1).item()

                ProcStat = pd.read_sql(f"Exec [GFE].[dbo].{__procname} '{z}','Admin'", GFE_SqlConn.GFE_CONN)
                __procStatus = ProcStat.to_string(index=False)

                Tran.commit()

                print(f'{__procname} Proc executed successfully.' )

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','SUCCESS','Proc executed. msg :: {__procStatus}')""")
                    
                    ProcExecAlert.__ProcExecAlert(__procname, __procStatus)
                
                except Exception as err:
                    print('Unable to insert LoG data in table.')

            except Exception as err:

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','Failed','Proc not executed. {str(err)}')""")

                except Exception as err:
                    print('Unable to insert LoG data in table.')

                ProcExecAlert.__ProcFailAlert(__procname, str(err))

                print(f'{__procname} Proc not executed.')

                sys.exit(1)

        except Exception as err:
            print(f'Unable to insert data into Table :: {GFE_SqlConn.FeeSchd}'+ str(err))

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{item}','Insert Temp','FAILED','{str(err)}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')

            sys.exit(1)

    except Exception as err:
        print("Validation Failed for Co-Provider Individual contact.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','FAILED','{str(err.failure_cases)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.") 

        FileFailed.__FileFailed(src_path,tgt_path,item)
        print("sys exit")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Sys Exit','ERROR','Code exit file will not be loaded.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

        sys.exit(1)
#*********************************************************************************************************************************

def __CO_Provider_IndividualFee(DF,src_path,tgt_path,item):
    '''
    Pre Checks for Co-Provider Individual fee
    '''
    print("Starting Data validation for Co-Provider Individual fee.")

    Data_Check = DataFrameSchema(
        {
        "HospitalFacilityCode" : Column(nullable=False),
        "ServiceLine" : Column(nullable=True),
        "ProviderGroupName" : Column(nullable=False),
        "AcuteCPTCode" : Column(nullable=False),
        "ProfessionalCPTCode" : Column(nullable=False),
        "ServiceDescription" : Column(nullable=False),
        "StandardFee" : Column(nullable=True),
        "SelfPayFee" : Column(nullable=False),
        "EffectiveStartDate" : Column(nullable=False),
        "NationalProviderIdentifier" : Column(nullable=False)
        }, strict=True, ordered=True)

    try:
        Data_Check.validate(DF, lazy= True)
        print("Validation Completed Successfully.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','SUCCESS','File data looks good.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

            #Truncate Temp Table           
        try:

            try:
                GFE_SqlConn.GFE_CONN.execute(f"Truncate Table [GFE].[dbo].{GFE_SqlConn.FeeSchd} ")
                print('Temp Table has been Truncated.')

            except Exception as err: 
                print('Unable to truncate temp table ::' + str(err))
            
            #Data load into Temp table. 

            #rows = DF.to_sql(name= GFE_SqlConn.FeeSchd, con=GFE_SqlConn.GFE_CONN, schema= 'dbo' ,index=False ,method='multi', if_exists='append')

            rows = DF.shape[0]

            DF = DF.astype('str')

            for index, row in DF.iterrows():

                GFE_SqlConn.GFE_CONN.execute(f""" Insert into dbo.{GFE_SqlConn.FeeSchd}
                                                    (HospitalFacilityCode,                                                        
                                                        ProfessionalServiceLine,
                                                        ProfessionalGroupName,
                                                        AcuteCPTCode,
                                                        ProfessionalCPTCode,
                                                        ServiceDescription,
                                                        StandardFee,
                                                        SelfPayFee,
                                                        EffectiveStartDate,
                                                        NationalProviderIdentifier) Values(?,?,?,?,?,?,?,?,?,?)""",
                                                        row.HospitalFacilityCode, row.ServiceLine, row.ProviderGroupName, row.AcuteCPTCode, row.ProfessionalCPTCode, row.ServiceDescription, row.StandardFee,
                                                        row.SelfPayFee, row.EffectiveStartDate, row.NationalProviderIdentifier)

            print('Data inserted into Temp.')

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Insert Temp','SUCCESS','Data inserted into temp table. Rows inserted :: {rows}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
                #Proc execution after Data load to Temp.
            try:
                __procname = 'GFE_StoreProviderFeeSchedules'

                Tran = GFE_SqlConn.GFE_CONN.begin()

                ProcStat = pd.read_sql(f"Exec [GFE].[dbo].{__procname} 'Admin','{item}','CoProviderIndividual'", GFE_SqlConn.GFE_CONN)
                __procStatus = ProcStat.to_string(index=False)

                Tran.commit()

                print(f'{__procname} Proc executed successfully.' )

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','SUCCESS','Proc executed. msg :: {__procStatus}')""")
                    
                    ProcExecAlert.__ProcExecAlert(__procname, __procStatus)
                
                except Exception as err:
                    print('Unable to insert LoG data in table.')

            except Exception as err:

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','Failed','Proc not executed. {str(err)}')""")

                except Exception as err:
                    print('Unable to insert LoG data in table.')

                ProcExecAlert.__ProcFailAlert(__procname, str(err))

                print(f'{__procname} Proc not executed.')

                sys.exit(1)

        except Exception as err:
            print(f'Unable to insert data into Table :: {GFE_SqlConn.FeeSchd}'+ str(err))

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{item}','Insert Temp','FAILED','{str(err)}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
            sys.exit(1)

    except Exception as err:
        print("Validation Failed for Co-Provider Individual fee.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','FAILED','{str(err.failure_cases)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.") 

        FileFailed.__FileFailed(src_path,tgt_path,item)
        print("sys exit")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Sys Exit','ERROR','Code exit file will not be loaded.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

        sys.exit(1)
#*********************************************************************************************************************************

def __FacilityFeeSchedule(DF,src_path,tgt_path,item):
    '''
    Pre Checks for Physician fee
    '''
    print("Starting Data validation for Physician fee.")

    Data_Check = DataFrameSchema(
        {
        "ConveningFacilityCode" : Column(nullable=True),
        "AcuteCPTCode" : Column(nullable=True),
        "ServiceDescription" : Column(nullable=True),
        "StandardFee" : Column(nullable=True),
        "SelfPayFee" : Column(nullable=True),
        "RevenueCode" : Column(nullable=True),
        "EffectiveStartDate" : Column(nullable=True),
        "ExpirationDate" : Column(nullable=True)
        }, strict=True, ordered=True)

    try:
        Data_Check.validate(DF, lazy= True)
        print("Validation Completed Successfully.")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','SUCCESS','File data looks good.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

            #Truncate Temp Table           
        try:

            try:
                GFE_SqlConn.GFE_CONN.execute(f"Truncate Table [GFE].[dbo].{GFE_SqlConn.FeeSchd} ")
                print('Temp Table has been Truncated.')

            except Exception as err: 
                print('Unable to truncate temp table ::' + str(err)) 

            #Data load into Temp table. 

            #rows = DF.to_sql(name= GFE_SqlConn.FeeSchd, con=GFE_SqlConn.GFE_CONN, schema= 'dbo' ,index=False ,method='multi', if_exists='append')

            rows = DF.shape[0]

            DF = DF.astype('str')

            for index, row in DF.iterrows():
                GFE_SqlConn.GFE_CONN.execute(f""" Insert into dbo.{GFE_SqlConn.FeeSchd} 
                                        (ConveningFacilityCode,                                        
                                        AcuteCPTCode,                                        
                                        ServiceDescription,
                                        StandardFee,
                                        SelfPayFee,
                                        RevenueCode,                                        
                                        EffectiveStartDate,
                                        ExpirationDate) Values(?,?,?,?,?,?,?,?)""", 
                                        row.ConveningFacilityCode, row.AcuteCPTCode, row.ServiceDescription, row.StandardFee, row.SelfPayFee, row.RevenueCode, row.EffectiveStartDate, row.ExpirationDate)

            print('Data inserted into Temp.')

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Insert Temp','SUCCESS','Data inserted into temp table. Rows inserted :: {rows}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
                #Proc execution after Data load to Temp.
            try:
                __procname = 'GFE_StoreProviderFeeSchedules'

                Tran = GFE_SqlConn.GFE_CONN.begin()

                ProcStat = pd.read_sql(f"Exec [GFE].[dbo].{__procname} 'Admin','{item}','Facility'", GFE_SqlConn.GFE_CONN)
                __procStatus = ProcStat.to_string(index=False)

                Tran.commit()
                
                print(f'{__procname} Proc executed successfully.' )

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','SUCCESS','Proc executed. msg :: {__procStatus}')""")
                    
                    ProcExecAlert.__ProcExecAlert(__procname, __procStatus)
                
                except Exception as err:
                    print('Unable to insert LoG data in table.')

            except Exception as err:

                try:
                    ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','ProcExec','Failed','Proc not executed. {str(err)}')""")

                except Exception as err:
                    print('Unable to insert LoG data in table.')

                ProcExecAlert.__ProcFailAlert(__procname, str(err))

                print(f'{__procname} Proc not executed.')

                sys.exit(1)

        except Exception as err:
            print(f'Unable to insert data into Table :: {GFE_SqlConn.FeeSchd}'+ str(err))

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                        (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                        VALUES('{ServerConn.LogTime}','{item}','Insert Temp','FAILED','{str(err)}')""")
            except Exception as err:  
                print('Unable to insert LoG data in table.')
            
            sys.exit(1)

    except Exception as err:
        print("Validation Failed for Physician fee.")
      
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Data Validation','FAILED','{str(err.failure_cases)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.") 

        FileFailed.__FileFailed(src_path,tgt_path,item)
        print("sys exit")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Sys Exit','ERROR','Code exit file will not be loaded.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

        sys.exit(1)
#*********************************************************************************************************************************

#GFE Main

import sys
import Config
import ServerConn
import ParsingFile
import FileSpecificChecks


def __Validation():
    
    __DFsName = [] #raw filenames
    __Fnames = {} #parsed filenames
    
    try:
        "Generating parsed file name for matching."
        for _ in ParsingFile.DFs.keys():
            __DFsName.append(_)
            __Fnames[_] = ''.join([i for i in _ if not i.isdigit()])

    except Exception as err:
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','NA','Generate File Name','ERROR','{str(err)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

    try:

        for item in __DFsName:
            print(f"Initiating Validation for file {item}.")

            try:
                ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{item}','Starting Validation','STARTED','Initiating Validation for file {item}')""")
            except Exception as err:
                print("Unable to insert LoG data in table.")

            if 'CPGCD' in __Fnames[item]: #CPGC01D
                DF = ParsingFile.DFs[item]
                src_path = ParsingFile.SrcFilePaths[item]
                tgt_path = Config._FailedLoc+item+'.csv'
                FileSpecificChecks.__DuplicateCheck(DF,src_path,tgt_path,item)
                FileSpecificChecks.__CO_Provider_GroupContact(DF, src_path, tgt_path, item)
                                
            elif 'CPGFD' in __Fnames[item]: #CPGF01D
                DF = ParsingFile.DFs[item]
                src_path = ParsingFile.SrcFilePaths[item]
                tgt_path = Config._FailedLoc+item+'.csv'
                FileSpecificChecks.__DuplicateCheck(DF,src_path,tgt_path,item)
                FileSpecificChecks.__CO_Provider_GroupFee(DF, src_path, tgt_path, item)      

            elif 'CPICD' in __Fnames[item]: #CPIC01D
                DF = ParsingFile.DFs[item]
                src_path = ParsingFile.SrcFilePaths[item]
                tgt_path = Config._FailedLoc+item+'.csv'
                FileSpecificChecks.__DuplicateCheck(DF,src_path,tgt_path,item)
                FileSpecificChecks.__CO_Provider_Individual_Contact(DF, src_path, tgt_path, item)

            elif 'CPIFD' in __Fnames[item]: #CPIF01D
                DF = ParsingFile.DFs[item]
                src_path = ParsingFile.SrcFilePaths[item]
                tgt_path = Config._FailedLoc+item+'.csv'
                FileSpecificChecks.__DuplicateCheck(DF,src_path,tgt_path,item)
                FileSpecificChecks.__CO_Provider_IndividualFee(DF, src_path, tgt_path, item)

            elif 'FFSD' in __Fnames[item]: #FFS01D
                DF = ParsingFile.DFs[item]
                src_path = ParsingFile.SrcFilePaths[item]
                tgt_path = Config._FailedLoc+item+'.csv'
                FileSpecificChecks.__DuplicateCheck(DF,src_path,tgt_path,item)
                FileSpecificChecks.__FacilityFeeSchedule(DF, src_path, tgt_path, item)  
                
            else:
                print('No File type matched')


    except Exception as err:
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','NA','Starting Validation','ERROR','{str(err)}')""") 
        except Exception as err:
            print("Unable to insert LoG data in table.")
            
        print(str(err))
        sys.exit(1)

__Validation()

#SQL CONN
from sqlalchemy import create_engine
import Config
import sys

try:
    GFE_DB = Config._GFE_DataBase
    CoProGrpCont = Config._CoProGrpCont
    CoProIndvCont = Config._CoProIndvCont
    FeeSchd = Config._FeeSchd
    
    driver = 'ODBC Driver 17 for SQL Server'
    ENGINE = create_engine(f'mssql+pyodbc://{Config._GFE_ServerName}/{Config._GFE_DataBase}?driver={driver}',fast_executemany=True)
    GFE_CONN = ENGINE.connect()
    print('Connected to Temp Table Server.')

except Exception as err:
    print("Unable to Connect Server.")
    print(err)

    sys.exit(1)


#Parsing File

import Config
import os,sys
import pandas as pd
import ServerConn

_dir = Config._SrcLoc

DFs = {}
SrcFilePaths = {}

'''
Generate a dictionary of dataframe
for the csv file present in directory.
'''

try:      
    print("Looking for file in dir.") 
    Files = os.listdir(_dir)
    
    __ln = []

    if len(Files) != 0:        
        for i in range(len(Files)):
            if Files[i].endswith('.csv'):
                __ln.append(Files[i])  

        if len(__ln) != 0:     
     
            file = __ln.pop()
            DFs[file.split('.csv')[0]] = pd.read_csv(f"{_dir+file}",encoding= 'unicode_escape')
            SrcFilePaths[file.split('.csv')[0]] = f"{_dir+file}"
            
    if bool(DFs) == False:
        print("No file found.")
        
    else:
        print(f"File found :: {DFs.keys()}")

        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{file}','File Parsing','SUCCESS','File Found :: {file}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")
    
except Exception as err:
    print("Error while parsing file.")
    print("sys exit")

    try:
        ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','NA','File Parsing','FAILED','{str(err)}')""")
    except Exception as err:
        print("Unable to insert LoG data in table.")

    sys.exit(1)

#Proc exec alert

import ServerConn
import smtplib
from email.message import EmailMessage
import Config

def __ProcExecAlert(__procname,__procStatus):
    '''
    This will send alert mails for Proc Exec.
    '''
    try:
        print("Sending ProcExec notification.")
        msg = EmailMessage()
        msg['From'] = Config._From
        msg['To'] = Config._To
        msg['Cc'] = Config._Cc
        msg['Subject'] = f"{__procname} Proc Executed."
        msg.set_content( f"""Proc :: {__procname} has been executed.\n\n Proc msg :\n{__procStatus}.
                    """)
        with smtplib.SMTP(Config._Host,Config._Port) as server:
            server.send_message(msg)
            server.quit()
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','StorProcAlert','SUCCESS','StorProcAlert has been sent.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")
        
        print('Alert Mail Sent Successfully.')
    
    except Exception as err:
        print("Unable to send email.")
        
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','StorProcAlert','FAILED','{str(err)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

def __ProcFailAlert(__procname,__procStatus):
    '''
    This will send alert mails for Proc Exec.
    '''
    try:
        print("Sending ProcExec notification.")
        msg = EmailMessage()
        msg['From'] = Config._From
        msg['To'] = Config._To
        msg['Cc'] = Config._Cc
        msg['Subject'] = f"{__procname} Failed !!"
        msg.set_content( f"""Proc :: {__procname} has been failed.\n\n Error msg :\n{__procStatus}.
                    """)
        with smtplib.SMTP(Config._Host,Config._Port) as server:
            server.send_message(msg)
            server.quit()
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','ProcFailAlert','SUCCESS','ProcFailAlert has been sent.')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")
        
        print('ProcFail Alert Mail Sent Successfully.')
    
    except Exception as err:
        print("Unable to send ProcFail Alert email.")
        
        try:
            ServerConn.CONN.execute(f"""INSERT INTO {ServerConn.DB}.{ServerConn.SCHEMA}.{ServerConn.TABLE}
                    (LOGTIME, FILENAME, INFO, STATUS, DETAILS)
                    VALUES('{ServerConn.LogTime}','{__procname}','ProcFailAlert','FAILED','{str(err)}')""")
        except Exception as err:
            print("Unable to insert LoG data in table.")

#Server Conn

from sqlalchemy import create_engine
import Config
from datetime import datetime
import sys

Today = datetime.now()
LogTime = Today.strftime("%m/%d/%Y, %H:%M:%S")

try:
    DB = Config._Database
    SCHEMA = Config._Schema
    TABLE = Config._Table
    
    driver = 'ODBC Driver 17 for SQL Server'
    ENGINE = create_engine(f'mssql+pyodbc://{Config._ServerName}/{Config._Database}?driver={driver}',fast_executemany=True)
    CONN = ENGINE.connect()
    print('Connected to Log Table Server.')

except Exception as err:
    print("Unable to Connect Server.")
    print(err)

    sys.exit(1)

######################################################################################################################################################################

#Genesys 

#Load Process

#Contact list truncate

import Config
from Config import logging
import sys
import time
import PureCloudPlatformClientV2
import EmailAlert


CLIENT_ID = Config.CLIENT_ID
CLIENT_SECRET = Config.CLIENT_SECRET
ORG_REGION = Config.ORG_REGION
CONTACT_LIST_ID = Config.CONTACT_LIST_ID
CONTACT_LIST_NAME = Config.CONTACT_LIST_NAME
FILENAME = Config.FILENAME
LOGFILE = Config.LOGFILE


try:
    '''
    Setting up org region and getting region API host.
    Authenticating client id , client secret & 
    creating instance of OutboundAPI. 
    '''
    logging.info("This is Clean-UP script to truncate DATA from Contact List.")
    logging.info(f"Initiating Clean-UP for {CONTACT_LIST_NAME}...")

    logging.info("Setting ORG_Region and getting API Host.")

    region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
    PureCloudPlatformClientV2.configuration.host = region.get_api_host()

    logging.info("DONE")

    try:
        logging.info("Getting client_credentials_token Using Client ID and CLient Secret.")

        api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)

        PureCloudPlatformClientV2.configuration.access_token = api_client.access_token
        
        outbound_api = PureCloudPlatformClientV2.OutboundApi()

        logging.info("DONE")

    except Exception as e:
        logging.error(f"Failed while Getting client_credentials_token Using Client ID and CLient Secret :: {str(e)}")

        EmailAlert.__HighImportanceEmailAlert(f"******** TRUNCATION SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError while Getting client_credentials_token :: {str(e)}" , LOGFILE, FILENAME)

        logging.info("Sys exit.")
        sys.exit(1)

except Exception as e:
    logging.error(f"Failed in Setting ORG_Region and getting API Host :: {str(e)}")

    EmailAlert.__HighImportanceEmailAlert(f"******** TRUNCATION SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError while Setting ORG_Region and getting API Host. :: {str(e)}" , LOGFILE, FILENAME)

    logging.info("Sys exit.")
    sys.exit(1)


retry_count = 0
num_retries = 3
retry_delay = 20

while retry_count < num_retries:

    try:
        '''
        Deletes all contacts out of a list.
        All outstanding recalls or rule-scheduled callbacks for non-preview campaigns configured with the contactlist will be cancelled.
        '''
        
        outbound_api.post_outbound_contactlist_clear(CONTACT_LIST_ID)

        logging.info("Wait time :: 30 sec.")
        time.sleep(30)

        logging.info(f"{CONTACT_LIST_NAME} Truncated successfully.")

        EmailAlert.__SuccessEmailAlert(f"******** TRUNCATION SCRIPT ********\n\nScript completed successfully for {CONTACT_LIST_NAME} contact list.\nContact list has been truncated.", LOGFILE, FILENAME)

        logging.info("TRUNCATION SCRIPT completed successfully.")
        logging.info("\n---------------------------------------------END---------------------------------------------\n\n")

        break

    except Exception as e:

        retry_count += 1

        if retry_count < num_retries:

            logging.warning(f"Retrying API call in {retry_delay} seconds...")
            time.sleep(retry_delay)

        else:
            logging.error(f"Max retry limit reached.")
            
            logging.warning(f"Exception when calling PostOutboundContactlistClearRequest->post_outbound_contactlist_clear: {str(e)}")

            EmailAlert.__HighImportanceEmailAlert(f"******** TRUNCATION SCRIPT ********\n\nError while truncating {CONTACT_LIST_NAME} contact list.\n err_msg :: {str(e)}" , LOGFILE, FILENAME)

            logging.info("\n---------------------------------------------END---------------------------------------------\n\n")

            sys.exit(1)


#Contact list upload data

import Config
from Config import logging
import sys
import time
import EmailAlert
import PureCloudPlatformClientV2
from sqlalchemy import create_engine, text
import pandas as pd
import concurrent.futures

import gc
gc.enable()

startT = time.time()

'''
Importing variables from Config file.
'''
CLIENT_ID = Config.CLIENT_ID
CLIENT_SECRET = Config.CLIENT_SECRET
ORG_REGION = Config.ORG_REGION
CONTACT_LIST_ID = Config.CONTACT_LIST_ID
CONTACT_LIST_NAME = Config.CONTACT_LIST_NAME
MAX_THREADS = Config.MAX_THREADS
CHUNK_SIZE = Config.CHUNK_SIZE
SERVER_NAME = Config._ServerName
DATABASE = Config._Database
DRIVER = Config._driver
QUERY = Config.SQL_QUERY
TABLE_NAME = Config.TABLE_NAME
ID_COLUMN_NAME = Config.ID_COLUMN_NAME
COLUMN_UPDATE = Config.COLUMN_UPDATE
UPDATE_CALLQUEUE_FLAG = Config.UPDATE_SQL_TABLE_FLAG
FILENAME = Config.FILENAME
LOGFILE = Config.LOGFILE

'''
Run time Global session variables.
'''
UCD_EMAIL_FLAG = True
UCD_EMAIL_FLAG_API = True
CCD_EMAIL_FLAG = True
UCD_FAILURE_COUNTER = 0
TOTAL_ROWS_TO_BE_LOADED = 0
TOTAL_ROWS_LOADED = 0
TOTAL_ROWS_FAILED = 0


try:
    '''
    Setting up org region and getting region API host.
    Authenticating client id , client secret & 
    creating instance of OutboundAPI. 
    '''

    logging.info("Setting ORG_Region and getting API Host.")

    region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
    PureCloudPlatformClientV2.configuration.host = region.get_api_host()

    logging.info("DONE")

    try:
        logging.info("Getting client_credentials_token Using Client ID and CLient Secret.")

        api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)
        outbound_api = PureCloudPlatformClientV2.OutboundApi(api_client)

        logging.info("DONE")

    except Exception as e:
        logging.error(f"Failed while Getting client_credentials_token Using Client ID and CLient Secret :: {str(e)}")

        EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while Getting client_credentials_token :: {str(e)}" , LOGFILE, FILENAME)

        logging.info("Sys exit...")
        sys.exit(1)

except Exception as e:
    logging.error(f"Failed in Setting ORG_Region and getting API Host :: {str(e)}")

    EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while Setting ORG_Region and getting API Host. :: {str(e)}" , LOGFILE, FILENAME)

    logging.info("Sys exit...")
    sys.exit(1)


def Create_Contact_Data(df): 
    """
    This function takes a pandas DataFrame and converts each row into a dictionary,
    which is then used to create a contact data object. The function returns a
    contact data object.
    """

    try:
        global CCD_EMAIL_FLAG
        data = []

        for i, row in df.iterrows():

            contact_data = {
                "id": "",
                "contactListId": CONTACT_LIST_ID,
                "data": row.to_dict(),
                "callable": True,
                "phoneNumberStatus": {},
                "contactableStatus": {}
            }

            data.append(contact_data)

        return data

    except Exception as e:
        logging.error(f"Error while Converting row in dict format :: {str(e)}")

        if CCD_EMAIL_FLAG:
            CCD_EMAIL_FLAG = False
            EmailAlert.__FailureEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while Converting row in Json format for Genesys API :: {str(e)}" , LOGFILE, FILENAME)
               
        else:
            logging.info(f"CCD_EMAIL_FLAG is {CCD_EMAIL_FLAG} so no email triggered.")         

def Upload_Contact_Data(contact_data_list, num_retries = 3, retry_delay = 20): 
    """
    This function takes a list of contact data objects and uploads them to the
    specified contact list using the PureCloud Outbound API. If an exception occurs,
    the function prints an error message and retries the API call up to num_retries times.
    """

    try:
        global TOTAL_ROWS_LOADED, TOTAL_ROWS_FAILED, UCD_FAILURE_COUNTER, UCD_EMAIL_FLAG, UCD_EMAIL_FLAG_API, UPDATE_CALLQUEUE_FLAG
        
        retry_count = 0
        FLAG = False
        
        while retry_count < num_retries:
        
            try:
                outbound_api.post_outbound_contactlist_contacts(CONTACT_LIST_ID, contact_data_list)
                FLAG = True
                break  # If the call succeeds, exit the retry loop
                
            except Exception as e:
                
                retry_count += 1
                UCD_FAILURE_COUNTER += 1

                logging.warning(f"Exception occurred on API call :: {str(e)}")
                
                if retry_count < num_retries:
                    logging.warning(f"Retrying API call in {retry_delay} seconds...")
                    time.sleep(retry_delay)

                else:
                    logging.error(f"Max retry limit reached.")
                    
                    if UCD_EMAIL_FLAG_API:
                        UCD_EMAIL_FLAG_API = False
                        EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nMax retry limit reached.\nException when calling OutboundApi->post_outbound_contactlist_contacts :: {str(e)}" 
                                                              , LOGFILE, FILENAME)

                    else:
                        logging.info(f"UCD_EMAIL_FLAG_API is {UCD_EMAIL_FLAG_API} so no email triggered.")
                                            
        if FLAG:
        
            try:
                '''
                This block will be executed once the data is loaded into Genesys Cloud.
                '''

                data_dicts = [d['data'] for d in contact_data_list]
                LOADED_DF = pd.DataFrame(data_dicts)
                
                TOTAL_ROWS_LOADED += len(LOADED_DF)

                ids = LOADED_DF[ID_COLUMN_NAME].to_string(index=False).replace('\n', ',').strip(',')
                
                if UPDATE_CALLQUEUE_FLAG == 1:
                    
                    retry = 0
                    while retry < num_retries:

                        try:
                            '''
                            Open SQL Connection every time batch is loaded into Genesys,
                            Update Notes = "Loaded" for those Id's &
                            Close the connection once done.
                            '''
                            
                            ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
                            cnx = ENGINE.connect()

                            try:
                                
                                Update_Query = f"UPDATE {DATABASE}.dbo.{TABLE_NAME} SET {COLUMN_UPDATE} = 'Loaded To Genesys' WHERE {ID_COLUMN_NAME} IN ({ids})"
                                
                                Tnx = cnx.begin()
                                cnx.execute(text(Update_Query))
                                Tnx.commit()
                                cnx.close()
                                
                                break

                            except Exception as e:
                                
                                cnx.close()  #Closing the connection to server, error in Updated query.

                                retry += 1

                                logging.error(f"Failed to Update values in SQL Table ::  {str(e)}")                                

                                if retry < num_retries:
                                    logging.warning(f"Retrying to update table in {retry_delay} seconds...")
                                    time.sleep(retry_delay)
                                
                                else:

                                    logging.error(f"Database :- {DATABASE} Unable to update table :- {TABLE_NAME} for ids :: {ids}")   

                                    if UCD_EMAIL_FLAG:
                                        UCD_EMAIL_FLAG = False
                                        EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nMax retry limit reached.\nFailed to Update values in SQL Table.\nerr_msg ::  {str(e)}" 
                                                                    , LOGFILE, FILENAME)
                                    
                                    else:
                                        logging.info(f"UCD_EMAIL_FLAG is {UCD_EMAIL_FLAG} so no email triggered.")

                        except Exception as e:
                            
                            retry += 1

                            logging.error(f"Unable to Connect to server while updating {TABLE_NAME} Table err_msg :: {str(e)}")

                            if retry < num_retries:
                                logging.warning(f"Retrying to connect to  server : {SERVER_NAME} in {retry_delay} seconds...")
                                time.sleep(retry_delay)
                                
                            else:
                                
                                logging.error(f"Database :- {DATABASE} Unable to update table :- {TABLE_NAME} for ids :: {ids}")

                                if UCD_EMAIL_FLAG:
                                    UCD_EMAIL_FLAG = False
                                    EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nMax retry limit reached.\nUnable to Connect to server : {SERVER_NAME} while updating Table.\nerr_msg :: {str(e)}" 
                                                                , LOGFILE, FILENAME)

                                else:
                                    logging.info(f"UCD_EMAIL_FLAG is {UCD_EMAIL_FLAG} so no email triggered.")

            except Exception as e:
                logging.error(f"Failed to Re Create Pandas Dataframe from Contact Data Batch :: {str(e)}")
                
                if UCD_EMAIL_FLAG:
                    UCD_EMAIL_FLAG = False
                    EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nFailed to Re-Create Pandas Dataframe from Contact Data Batch :: {str(e)}"
                                                    , LOGFILE, FILENAME)
                
                else:
                    logging.info(f"UCD_EMAIL_FLAG is {UCD_EMAIL_FLAG} so no email triggered.")
        
        else:
            
            logging.warning(f"Failed to upload data in Genesys, FLAG is {FLAG}. So no record id will be updated <LOADED TO GENESYS>.")

            try:
                data_dicts = [d['data'] for d in contact_data_list]
                FAILED_DF = pd.DataFrame(data_dicts)
                TOTAL_ROWS_FAILED += len(FAILED_DF)

            except Exception as e:
                logging.error(f"Unable to export failed data batch, occured while loading data into contact list :: {str(e)}")
                EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nUnable to log failed data batch, occured while loading data into contact list :: {str(e)}" 
                                               , LOGFILE, FILENAME)

    except Exception as e:
        logging.error(str(e))
        EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError occured while loading data into contact list :: {str(e)}" 
                                              , LOGFILE, FILENAME)
            
def Processing_Contact_Data_Chunk(sublist): 
    """
    This function takes a sublist of a larger pandas DataFrame, converts each row
    into a contact data object, and uploads them to the specified contact list using
    the PureCloud Outbound API.
    """
    try:

        contact_data_list = Create_Contact_Data(sublist)
        Upload_Contact_Data(contact_data_list)

    except Exception as e:
        logging.error(f"Error while Executing Function Processing_Contact_Data_Chunk :: {str(e)}")


try:
    logging.info(f"Connecting to Server :: {SERVER_NAME}")
    
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
    CONN = ENGINE.connect()

    logging.info("DONE")

    try:
        logging.info("Reading data from SQL Server into Pandas Dataframe.")

        df = pd.read_sql(QUERY, CONN)
        df.fillna('NULL', inplace=True)

        logging.info(f"Total Rows in Table :: {len(df)}.")

        # SiteCount = df['ColumnName'].value_counts().to_dict()
        # Sites = set(df['ColumnName'].unique())

        TOTAL_ROWS_TO_BE_LOADED = len(df)

        if TOTAL_ROWS_TO_BE_LOADED == 0:
            '''
            will exit if there is 0 rows in data frame.
            '''

            logging.info("There was no data returned from SQL query.\n Sys Exit.")
            
            CONN.close()

            EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nThere is no data in Table - {TABLE_NAME}.\n\nSql query -: {QUERY} returned {TOTAL_ROWS_TO_BE_LOADED} rows." 
                                           , LOGFILE, FILENAME)

            sys.exit(1)  

        logging.info("DONE")

        CONN.close()

        try:
            logging.info("Creating List of Dataframes maxsize = (1000)")

            chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

            logging.info(f"Total Sub DataFrames created :: {len(chunks)}.")
            logging.info("DONE")

        except Exception as e:
            logging.error(f"Error while creating Chunk >> list of Dataframes :: {str(e)}")
            logging.warning("SYS Exit.")
            EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while creating Chunk >> list of Dataframes :: {str(e)}", LOGFILE, FILENAME)
            sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected Error while reading data from SQL Server into Dataframe :: {str(e)}")
        logging.warning("SYS Exit.")
        EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while reading data from SQL Server into Dataframe :: {str(e)}", LOGFILE, FILENAME)
        sys.exit(1)

except Exception as e:
    logging.error(f"No Data Source || Unable to Connect to SQL Server {SERVER_NAME} :: {str(e)}")
    logging.warning("SYS Exit.")
    EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nNo Data Source || Unable to Connect to SQL Server {SERVER_NAME} :: {str(e)}"
                                          , LOGFILE, FILENAME)
    sys.exit(1)


try:
    logging.info(f"Creating ThreadPool of {MAX_THREADS} Worker for parallel processing.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futures = []

        for chunk in chunks:
            future = executor.submit(Processing_Contact_Data_Chunk, chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

            if UCD_FAILURE_COUNTER > 6:
                
                logging.info(f"Total rows loaded to Genesys :: {TOTAL_ROWS_LOADED}.")
                _percentage = (TOTAL_ROWS_LOADED / TOTAL_ROWS_TO_BE_LOADED) * 100
                Termination_Per = "Success Percentage :: {:.2f}%".format(_percentage)
                logging.info(Termination_Per)

                EmailAlert.__HighImportanceEmailAlert(f"""********* ALERT ********\n\nData load Process terminated for {CONTACT_LIST_NAME} contact list.\n================================================================\nTotal rows to be loaded :: {TOTAL_ROWS_TO_BE_LOADED}.\nTotal rows loaded :: {TOTAL_ROWS_LOADED}.\n{Termination_Per}================================================================"""
                                                     , LOGFILE, FILENAME)
                
                print("SYS EXIT.")
                
                logging.error(f"SYS EXIT.")
                logging.warning("Processing Terminated due to many failure in API call.")

                logging.info("\n---------------------------------------------END---------------------------------------------\n\n")
                
                sys.exit(1)

        logging.info("Processing completed.")
        endT = time.time()

    TotalT = endT - startT
    hours, remainder = divmod(TotalT, 3600)
    minutes, seconds = divmod(remainder, 60)

    logging.info(f"Total rows loaded to Genesys :: {TOTAL_ROWS_LOADED}.")

    Success_percentage = (TOTAL_ROWS_LOADED / TOTAL_ROWS_TO_BE_LOADED) * 100
    Success_Per = "Success Percentage :: {:.2f}%".format(Success_percentage)
    logging.info(Success_Per)

    Script_Time = "Total time taken :: {:.0f} hr {:.0f} min {:f} sec".format(hours, minutes, seconds)
    logging.info(Script_Time)

    if TOTAL_ROWS_TO_BE_LOADED != TOTAL_ROWS_LOADED:

        EmailAlert.__HighImportanceEmailAlert(f"""********* ALERT ********\n\nProcess completed with failure while loading data into {CONTACT_LIST_NAME} Contact List.\n================================================================\nTotal rows to be loaded :: {TOTAL_ROWS_TO_BE_LOADED}.\nTotal rows loaded :: {TOTAL_ROWS_LOADED}.\n{TOTAL_ROWS_FAILED} rows were not loaded into ContactList {CONTACT_LIST_NAME}.\n{Success_Per}\n================================================================\n\n{Script_Time}"""
                                              , LOGFILE, FILENAME)
        
        logging.info("Process completed with failure while loading data into Contact List.")

        logging.info("\n---------------------------------------------END---------------------------------------------\n\n")

        print("Process completed but with failure.")

    else:

        print("Processing complete.")

        EmailAlert.__SuccessEmailAlert(f"""Data loaded successfully into {CONTACT_LIST_NAME} contact list.\n================================================================\nTotal rows to be loaded :: {TOTAL_ROWS_TO_BE_LOADED}.\nTotal rows loaded :: {TOTAL_ROWS_LOADED}.\n{Success_Per}.\n================================================================\n\n{Script_Time}"""
                                        , LOGFILE, FILENAME)
        
        logging.info("\n---------------------------------------------END---------------------------------------------\n\n")
        sys.exit(0)        
        
except Exception as e:
    logging.error(f"Error while creating the ThreadPool :: {str(e)}")

    EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\n\nError while creating the < Worker Thread > for parallel processing :: {str(e)}", LOGFILE, FILENAME)
    
    logging.warning("SYS Exit.")
    sys.exit(1)

#contact list upload data MFS

import Config
from Config import logging
import sys
import time
import EmailAlert
import PureCloudPlatformClientV2
from sqlalchemy import create_engine, text
import pandas as pd
import concurrent.futures

import gc
gc.enable()

startT = time.time()

'''
Importing variables from Config file.
'''
CLIENT_ID = Config.CLIENT_ID
CLIENT_SECRET = Config.CLIENT_SECRET
ORG_REGION = Config.ORG_REGION
CONTACT_LIST_ID = Config.CONTACT_LIST_ID
CONTACT_LIST_NAME = Config.CONTACT_LIST_NAME
MAX_THREADS = Config.MAX_THREADS
CHUNK_SIZE = Config.CHUNK_SIZE
SERVER_NAME = Config._ServerName
DATABASE = Config._Database
DRIVER = 'SQL Server Native Client 11.0' #Config._driver
QUERY = Config.SQL_QUERY
TABLE_NAME = Config.TABLE_NAME
ID_COLUMN_NAME = Config.ID_COLUMN_NAME
COLUMN_UPDATE = Config.COLUMN_UPDATE
UPDATE_CALLQUEUE_FLAG = Config.UPDATE_SQL_TABLE_FLAG
FILENAME = Config.FILENAME
LOGFILE = Config.LOGFILE

'''
Run time Global session variables.
'''
UCD_EMAIL_FLAG = True
UCD_EMAIL_FLAG_API = True
CCD_EMAIL_FLAG = True
UCD_FAILURE_COUNTER = 0
TOTAL_ROWS_TO_BE_LOADED = 0
TOTAL_ROWS_LOADED = 0
TOTAL_ROWS_FAILED = 0


try:
    '''
    Setting up org region and getting region API host.
    Authenticating client id , client secret & 
    creating instance of OutboundAPI. 
    '''

    logging.info("Setting ORG_Region and getting API Host.")

    region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
    PureCloudPlatformClientV2.configuration.host = region.get_api_host()

    logging.info("DONE")

    try:
        logging.info("Getting client_credentials_token Using Client ID and CLient Secret.")

        api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)
        outbound_api = PureCloudPlatformClientV2.OutboundApi(api_client)

        logging.info("DONE")

    except Exception as e:
        logging.error(f"Failed while Getting client_credentials_token Using Client ID and CLient Secret :: {str(e)}")

        EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while Getting client_credentials_token :: {str(e)}" , LOGFILE, FILENAME)

        logging.info("Sys exit...")
        sys.exit(1)

except Exception as e:
    logging.error(f"Failed in Setting ORG_Region and getting API Host :: {str(e)}")

    EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while Setting ORG_Region and getting API Host. :: {str(e)}" , LOGFILE, FILENAME)

    logging.info("Sys exit...")
    sys.exit(1)


def Create_Contact_Data(df): 
    """
    This function takes a pandas DataFrame and converts each row into a dictionary,
    which is then used to create a contact data object. The function returns a
    contact data object.
    """

    try:
        global CCD_EMAIL_FLAG
        data = []

        for i, row in df.iterrows():

            contact_data = {
                "id": "",
                "contactListId": CONTACT_LIST_ID,
                "data": row.to_dict(),
                "callable": True,
                "phoneNumberStatus": {},
                "contactableStatus": {}
            }

            data.append(contact_data)

        return data

    except Exception as e:
        logging.error(f"Error while Converting row in dict format :: {str(e)}")

        if CCD_EMAIL_FLAG:
            CCD_EMAIL_FLAG = False
            EmailAlert.__FailureEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while Converting row in Json format for Genesys API :: {str(e)}" , LOGFILE, FILENAME)
               
        else:
            logging.info(f"CCD_EMAIL_FLAG is {CCD_EMAIL_FLAG} so no email triggered.")         

def Upload_Contact_Data(contact_data_list, num_retries = 3, retry_delay = 20): 
    """
    This function takes a list of contact data objects and uploads them to the
    specified contact list using the PureCloud Outbound API. If an exception occurs,
    the function prints an error message and retries the API call up to num_retries times.
    """

    try:
        global TOTAL_ROWS_LOADED, TOTAL_ROWS_FAILED, UCD_FAILURE_COUNTER, UCD_EMAIL_FLAG, UCD_EMAIL_FLAG_API, UPDATE_CALLQUEUE_FLAG
        
        retry_count = 0
        FLAG = False
        
        while retry_count < num_retries:
        
            try:
                outbound_api.post_outbound_contactlist_contacts(CONTACT_LIST_ID, contact_data_list)
                FLAG = True
                break  # If the call succeeds, exit the retry loop
                
            except Exception as e:
                
                retry_count += 1
                UCD_FAILURE_COUNTER += 1

                logging.warning(f"Exception occurred on API call :: {str(e)}")
                
                if retry_count < num_retries:
                    logging.warning(f"Retrying API call in {retry_delay} seconds...")
                    time.sleep(retry_delay)

                else:
                    logging.error(f"Max retry limit reached.")
                    
                    if UCD_EMAIL_FLAG_API:
                        UCD_EMAIL_FLAG_API = False
                        EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nMax retry limit reached.\nException when calling OutboundApi->post_outbound_contactlist_contacts :: {str(e)}" 
                                                              , LOGFILE, FILENAME)

                    else:
                        logging.info(f"UCD_EMAIL_FLAG_API is {UCD_EMAIL_FLAG_API} so no email triggered.")
                                            
        if FLAG:
        
            try:
                '''
                This block will be executed once the data is loaded into Genesys Cloud.
                '''

                data_dicts = [d['data'] for d in contact_data_list]
                LOADED_DF = pd.DataFrame(data_dicts)
                
                TOTAL_ROWS_LOADED += len(LOADED_DF)

                ids = LOADED_DF[ID_COLUMN_NAME].to_string(index=False).replace('\n', ',').strip(',')
                
                if UPDATE_CALLQUEUE_FLAG == 1:
                    
                    retry = 0
                    while retry < num_retries:

                        try:
                            '''
                            Open SQL Connection every time batch is loaded into Genesys,
                            Update Notes = "Loaded" for those Id's &
                            Close the connection once done.
                            '''
                            
                            ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
                            cnx = ENGINE.connect()

                            try:
                                
                                Update_Query = f"UPDATE {DATABASE}.dbo.{TABLE_NAME} SET {COLUMN_UPDATE} = 'Loaded To Genesys' WHERE {ID_COLUMN_NAME} IN ({ids})"
                                
                                Tnx = cnx.begin()
                                cnx.execute(text(Update_Query))
                                Tnx.commit()
                                cnx.close()
                                
                                break

                            except Exception as e:
                                
                                cnx.close()  #Closing the connection to server, error in Updated query.

                                retry += 1

                                logging.error(f"Failed to Update values in SQL Table ::  {str(e)}")                                

                                if retry < num_retries:
                                    logging.warning(f"Retrying to update table in {retry_delay} seconds...")
                                    time.sleep(retry_delay)
                                
                                else:

                                    logging.error(f"Database :- {DATABASE} Unable to update table :- {TABLE_NAME} for ids :: {ids}")   

                                    if UCD_EMAIL_FLAG:
                                        UCD_EMAIL_FLAG = False
                                        EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nMax retry limit reached.\nFailed to Update values in SQL Table.\nerr_msg ::  {str(e)}" 
                                                                    , LOGFILE, FILENAME)
                                    
                                    else:
                                        logging.info(f"UCD_EMAIL_FLAG is {UCD_EMAIL_FLAG} so no email triggered.")

                        except Exception as e:
                            
                            retry += 1

                            logging.error(f"Unable to Connect to server while updating {TABLE_NAME} Table err_msg :: {str(e)}")

                            if retry < num_retries:
                                logging.warning(f"Retrying to connect to  server : {SERVER_NAME} in {retry_delay} seconds...")
                                time.sleep(retry_delay)
                                
                            else:
                                
                                logging.error(f"Database :- {DATABASE} Unable to update table :- {TABLE_NAME} for ids :: {ids}")

                                if UCD_EMAIL_FLAG:
                                    UCD_EMAIL_FLAG = False
                                    EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nMax retry limit reached.\nUnable to Connect to server : {SERVER_NAME} while updating Table.\nerr_msg :: {str(e)}" 
                                                                , LOGFILE, FILENAME)

                                else:
                                    logging.info(f"UCD_EMAIL_FLAG is {UCD_EMAIL_FLAG} so no email triggered.")

            except Exception as e:
                logging.error(f"Failed to Re Create Pandas Dataframe from Contact Data Batch :: {str(e)}")
                
                if UCD_EMAIL_FLAG:
                    UCD_EMAIL_FLAG = False
                    EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nFailed to Re-Create Pandas Dataframe from Contact Data Batch :: {str(e)}"
                                                    , LOGFILE, FILENAME)
                
                else:
                    logging.info(f"UCD_EMAIL_FLAG is {UCD_EMAIL_FLAG} so no email triggered.")
        
        else:
            
            logging.warning(f"Failed to upload data in Genesys, FLAG is {FLAG}. So no record id will be updated <LOADED TO GENESYS>.")

            try:
                data_dicts = [d['data'] for d in contact_data_list]
                FAILED_DF = pd.DataFrame(data_dicts)
                TOTAL_ROWS_FAILED += len(FAILED_DF)

            except Exception as e:
                logging.error(f"Unable to export failed data batch, occured while loading data into contact list :: {str(e)}")
                EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nUnable to log failed data batch, occured while loading data into contact list :: {str(e)}" 
                                               , LOGFILE, FILENAME)

    except Exception as e:
        logging.error(str(e))
        EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError occured while loading data into contact list :: {str(e)}" 
                                              , LOGFILE, FILENAME)
            
def Processing_Contact_Data_Chunk(sublist): 
    """
    This function takes a sublist of a larger pandas DataFrame, converts each row
    into a contact data object, and uploads them to the specified contact list using
    the PureCloud Outbound API.
    """
    try:

        contact_data_list = Create_Contact_Data(sublist)
        Upload_Contact_Data(contact_data_list)

    except Exception as e:
        logging.error(f"Error while Executing Function Processing_Contact_Data_Chunk :: {str(e)}")


try:
    logging.info(f"Connecting to Server :: {SERVER_NAME}")
    
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
    CONN = ENGINE.connect()

    logging.info("DONE")

    try:
        logging.info("Reading data from SQL Server into Pandas Dataframe.")

        df = pd.read_sql(QUERY, CONN)
        df.fillna('NULL', inplace=True)

        logging.info(f"Total Rows in Table :: {len(df)}.")

        # SiteCount = df['ColumnName'].value_counts().to_dict()
        # Sites = set(df['ColumnName'].unique())

        TOTAL_ROWS_TO_BE_LOADED = len(df)

        if TOTAL_ROWS_TO_BE_LOADED == 0:
            '''
            will exit if there is 0 rows in data frame.
            '''

            logging.info("There was no data returned from SQL query.\n Sys Exit.")
            
            CONN.close()

            # EmailAlert.__FailureEmailAlert(f"*********** ALERT ***********\n\nContact List :: {CONTACT_LIST_NAME}.\nThere is no data in Table - {TABLE_NAME}.\n\nSql query -: {QUERY} returned {TOTAL_ROWS_TO_BE_LOADED} rows." 
            #                                , LOGFILE, FILENAME)

            sys.exit(0)  

        logging.info("DONE")

        CONN.close()

        try:
            logging.info("Creating List of Dataframes maxsize = (1000)")

            chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

            logging.info(f"Total Sub DataFrames created :: {len(chunks)}.")
            logging.info("DONE")

        except Exception as e:
            logging.error(f"Error while creating Chunk >> list of Dataframes :: {str(e)}")
            logging.warning("SYS Exit.")
            EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while creating Chunk >> list of Dataframes :: {str(e)}", LOGFILE, FILENAME)
            sys.exit(1)

    except Exception as e:
        logging.error(f"Unexpected Error while reading data from SQL Server into Dataframe :: {str(e)}")
        logging.warning("SYS Exit.")
        EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nError while reading data from SQL Server into Dataframe :: {str(e)}", LOGFILE, FILENAME)
        sys.exit(1)

except Exception as e:
    logging.error(f"No Data Source || Unable to Connect to SQL Server {SERVER_NAME} :: {str(e)}")
    logging.warning("SYS Exit.")
    EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\nNo Data Source || Unable to Connect to SQL Server {SERVER_NAME} :: {str(e)}"
                                          , LOGFILE, FILENAME)
    sys.exit(1)


try:
    logging.info(f"Creating ThreadPool of {MAX_THREADS} Worker for parallel processing.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futures = []

        for chunk in chunks:
            future = executor.submit(Processing_Contact_Data_Chunk, chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

            if UCD_FAILURE_COUNTER > 6:
                
                logging.info(f"Total rows loaded to Genesys :: {TOTAL_ROWS_LOADED}.")
                _percentage = (TOTAL_ROWS_LOADED / TOTAL_ROWS_TO_BE_LOADED) * 100
                Termination_Per = "Success Percentage :: {:.2f}%".format(_percentage)
                logging.info(Termination_Per)

                EmailAlert.__HighImportanceEmailAlert(f"""********* ALERT ********\n\nData load Process terminated for {CONTACT_LIST_NAME} contact list.\n================================================================\nTotal rows to be loaded :: {TOTAL_ROWS_TO_BE_LOADED}.\nTotal rows loaded :: {TOTAL_ROWS_LOADED}.\n{Termination_Per}================================================================"""
                                                     , LOGFILE, FILENAME)
                
                print("SYS EXIT.")
                
                logging.error(f"SYS EXIT.")
                logging.warning("Processing Terminated due to many failure in API call.")

                logging.info("\n---------------------------------------------END---------------------------------------------\n\n")
                
                sys.exit(1)

        logging.info("Processing completed.")
        endT = time.time()

    TotalT = endT - startT
    hours, remainder = divmod(TotalT, 3600)
    minutes, seconds = divmod(remainder, 60)

    logging.info(f"Total rows loaded to Genesys :: {TOTAL_ROWS_LOADED}.")

    Success_percentage = (TOTAL_ROWS_LOADED / TOTAL_ROWS_TO_BE_LOADED) * 100
    Success_Per = "Success Percentage :: {:.2f}%".format(Success_percentage)
    logging.info(Success_Per)

    Script_Time = "Total time taken :: {:.0f} hr {:.0f} min {:f} sec".format(hours, minutes, seconds)
    logging.info(Script_Time)

    if TOTAL_ROWS_TO_BE_LOADED != TOTAL_ROWS_LOADED:

        EmailAlert.__HighImportanceEmailAlert(f"""********* ALERT ********\n\nProcess completed with failure while loading data into {CONTACT_LIST_NAME} Contact List.\n================================================================\nTotal rows to be loaded :: {TOTAL_ROWS_TO_BE_LOADED}.\nTotal rows loaded :: {TOTAL_ROWS_LOADED}.\n{TOTAL_ROWS_FAILED} rows were not loaded into ContactList {CONTACT_LIST_NAME}.\n{Success_Per}\n================================================================\n\n{Script_Time}"""
                                              , LOGFILE, FILENAME)
        
        logging.info("Process completed with failure while loading data into Contact List.")

        logging.info("\n---------------------------------------------END---------------------------------------------\n\n")

        print("Process completed but with failure.")

    else:

        print("Processing complete.")

        EmailAlert.__SuccessEmailAlert(f"""Data loaded successfully into {CONTACT_LIST_NAME} contact list.\n================================================================\nTotal rows to be loaded :: {TOTAL_ROWS_TO_BE_LOADED}.\nTotal rows loaded :: {TOTAL_ROWS_LOADED}.\n{Success_Per}.\n================================================================\n\n{Script_Time}"""
                                        , LOGFILE, FILENAME)
        
        logging.info("\n---------------------------------------------END---------------------------------------------\n\n")
        sys.exit(0)        
        
except Exception as e:
    logging.error(f"Error while creating the ThreadPool :: {str(e)}")

    EmailAlert.__HighImportanceEmailAlert(f"Contact List :: {CONTACT_LIST_NAME}.\n\nError while creating the < Worker Thread > for parallel processing :: {str(e)}", LOGFILE, FILENAME)
    
    logging.warning("SYS Exit.")
    sys.exit(1)
	
# Email Alert

import Config
from Config import logging
import smtplib
from email.message import EmailMessage
import time


def __HighImportanceEmailAlert(MSG, LOGFILE, FILENAME, num_retries = 2, retry_delay = 30):
    '''
    This will send alert mails for failed file with Importance as High.
    '''
    retry_count = 0
    while retry_count < num_retries:

        try:
            print("Sending email notification.")
            logging.info("Sending email notification.")

            msg = EmailMessage()
            msg['Importance'] = 'High'
            msg['From'] = Config._From
            msg['To'] = Config.Failure_To
            msg['Cc'] = Config.Failure_Cc
            msg['Subject'] = f"Genesys FAILURE ALERT"
            msg.set_content( f"""{MSG}
            \nRefer to LOG file for more details.\n\n
                        """)
            with open(LOGFILE, 'rb') as f:
                __data = f.read()
                msg.add_attachment(__data, maintype='text', subtype='plain', filename=FILENAME)

            with smtplib.SMTP(Config._Host,Config._Port) as server:
                server.send_message(msg)
                server.quit()

            print("Mail sent successfully.")
            logging.info("Mail sent successfully.")

            break

        except Exception as err:
            retry_count += 1

            if retry_count < num_retries:
                logging.warning(f"Unable to send email. {str(err)}\nRetrying in {retry_delay} sec.")
                time.sleep(retry_delay)
            else:
                logging.error(f"Unable to send email. {str(err)}")


def __FailureEmailAlert(MSG, LOGFILE, FILENAME, num_retries = 2, retry_delay = 30):
    '''
    This will send alert mails for failed file.
    '''
    retry_count = 0
    while retry_count < num_retries:

        try:
            print("Sending email notification.")            
            logging.info("Sending email notification.")

            msg = EmailMessage()
            msg['From'] = Config._From
            msg['To'] = Config.Failure_To
            msg['Cc'] = Config.Failure_Cc
            msg['Subject'] = f"Genesys FAILURE ALERT"
            msg.set_content( f"""{MSG}
            \nRefer to LOG file for more details.\n\n
                        """)
            with open(LOGFILE, 'rb') as f:
                __data = f.read()
                msg.add_attachment(__data, maintype='text', subtype='plain', filename=FILENAME)

            with smtplib.SMTP(Config._Host,Config._Port) as server:
                server.send_message(msg)
                server.quit()

            print("Mail sent successfully.")
            logging.info("Mail sent successfully.")

            break

        except Exception as err:
            retry_count += 1

            if retry_count < num_retries:
                logging.warning(f"Unable to send email. {str(err)}\nRetrying in {retry_delay} sec.")
                time.sleep(retry_delay)
            else:
                logging.error(f"Unable to send email. {str(err)}")


def __EmailAlert(MSG, num_retries = 2, retry_delay = 30):
    '''
    This will send alert mails without attaching log file.
    '''
    retry_count = 0
    while retry_count < num_retries:
         
        try:
            print("Sending email notification.")
            logging.info("Sending email notification.")

            msg = EmailMessage()
            msg['From'] = Config._From
            msg['To'] = Config.Failure_To
            msg['Cc'] = Config.Failure_Cc
            msg['Subject'] = f"Genesys FAILURE ALERT"
            msg.set_content( f"""{MSG}\n""")

            with smtplib.SMTP(Config._Host,Config._Port) as server:
                server.send_message(msg)
                server.quit()

            print("Mail sent successfully.")
            logging.info("Mail sent successfully.")

            break

        except Exception as err:
            retry_count += 1

            if retry_count < num_retries:
                logging.warning(f"Unable to send email. {str(err)}\nRetrying in {retry_delay} sec.")
                time.sleep(retry_delay)
            else:
                logging.error(f"Unable to send email. {str(err)}")


def __SuccessEmailAlert(MSG, LOGFILE, FILENAME, num_retries = 2, retry_delay = 30):
    '''
    This will send success alert mails.
    '''
    retry_count = 0
    while retry_count < num_retries:
         
        try:
            print("Sending email notification.")
            logging.info("Sending email notification.")

            msg = EmailMessage()
            msg['From'] = Config._From
            msg['To'] = Config.Success_To
            msg['Cc'] = Config.Success_Cc
            msg['Subject'] = f"Genesys SUCCESS ALERT"
            msg.set_content( f"""{MSG}
            \n\n
                        """)
            
            '''
            Log file not needed for Success alerts as of now.
            '''

            # with open(LOGFILE, 'rb') as f:
            #     __data = f.read()
            #     msg.add_attachment(__data, maintype='text', subtype='plain', filename=FILENAME)


            with smtplib.SMTP(Config._Host,Config._Port) as server:
                server.send_message(msg)
                server.quit()
            
            print("Mail sent successfully.")
            logging.info("Mail sent successfully.") 

            break

        except Exception as err:
            retry_count += 1

            if retry_count < num_retries:
                logging.warning(f"Unable to send email. {str(err)}\nRetrying in {retry_delay} sec.")
                time.sleep(retry_delay)
            else:
                logging.error(f"Unable to send email. {str(err)}")

#Data Export

#contact list export url

import Config
import sys
from Config import logging
import EmailAlert
import PureCloudPlatformClientV2
import re
import webbrowser 
import urllib.request
import time
from datetime import datetime
import os
import shutil
import gc
gc.enable()

startT = time.time()

CLIENT_ID = Config.CLIENT_ID
CLIENT_SECRET = Config.CLIENT_SECRET
ORG_REGION = Config.ORG_REGION

CONTACT_LIST_ID = Config.CONTACT_LIST_ID
CONTACT_LIST_NAME = Config.CONTACT_LIST_NAME

FILENAME = Config.FILENAME
LOGFILE = Config.LOGFILE
MOVE_FILE_FTP = Config.Move_File_FTP
FTP_PATH = Config.FTP_Path

try:
    '''
    Setting up org region and getting region API host.
    Authenticating client id , client secret & 
    creating instance of OutboundAPI. 
    '''
    logging.info("This is export script to download contact list data.")
    logging.info(f"Initiating export for {CONTACT_LIST_NAME}...")

    logging.info("Setting ORG_Region and getting API Host.")

    region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
    PureCloudPlatformClientV2.configuration.host = region.get_api_host()

    logging.info("DONE")

    try:
        logging.info("Getting client_credentials_token Using Client ID and CLient Secret.")

        api_client = PureCloudPlatformClientV2.api_client.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)

        PureCloudPlatformClientV2.configuration.access_token = api_client.access_token
        
        outbound_api = PureCloudPlatformClientV2.OutboundApi()
        download_api = PureCloudPlatformClientV2.DownloadsApi()

        logging.info("DONE")

    except Exception as e:
        logging.error(f"Failed while Getting client_credentials_token Using Client ID and CLient Secret :: {str(e)}")

        EmailAlert.__HighImportanceEmailAlert(f"******** EXPORT SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError while Getting client_credentials_token :: {str(e)}" , LOGFILE, FILENAME)

        logging.warning("Sys exit.")
        sys.exit(1)

except Exception as e:
    logging.error(f"Failed in Setting ORG_Region and getting API Host :: {str(e)}")

    EmailAlert.__HighImportanceEmailAlert(f"******** EXPORT SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError while Setting ORG_Region and getting API Host. :: {str(e)}" , LOGFILE, FILENAME)

    logging.warning("Sys exit.")
    sys.exit(1)


num_retries = 3
retry_delay = 30


def api_retry(api_call, *args, **kwargs):

    retry_count = 0

    while retry_count < num_retries:

        try:
            logging.info(f"Initiating API call :: {api_call.__name__}")
            api_response = api_call(*args, **kwargs)
            logging.info("API call successful.")
            return api_response
        
        except Exception as e:

            retry_count += 1

            if retry_count < num_retries:
                logging.warning(f"API call failed: {str(e)}")
                logging.warning(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

            else:
                logging.error(f"Max retry limit reached for API call :: {api_call.__name__}")
                logging.error(f"Exception message: {str(e)}")
                
                EmailAlert.__HighImportanceEmailAlert(f"******** EXPORT SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError while calling API - {api_call.__name__}. :: {str(e)}" , LOGFILE, FILENAME)

                logging.warning("Sys exit.")
                sys.exit(1)

try:
    logging.info("Initiating Export URL using post_outbound_contactlist_export.")

    api_response = api_retry(outbound_api.post_outbound_contactlist_export, CONTACT_LIST_ID)

    logging.info("Extracting Self_uri from post_outbound_contactlist_export response.")

    regex = r"self_uri': '(.*?)'"
    match = re.search(regex, str(api_response))
    self_uri = match.group(1)

    logging.info("Self_uri has been extracted successfully.")
    logging.info(f"self_uri :: {self_uri}")

    time.sleep(180)

    logging.info("Initiating Export URL using get_outbound_contactlist_export.")

    api_response = api_retry(outbound_api.get_outbound_contactlist_export, CONTACT_LIST_ID, download=self_uri)

    logging.info("Extracting uri from get_outbound_contactlist_export response.")

    regex = r"uri': '(.*?)'"
    match = re.search(regex, str(api_response))
    uri = match.group(1)

    logging.info("Uri has been extracted successfully.")
    logging.info(f"uri :: {uri}")

    time.sleep(180)

    logging.info("Extracting download_id from get_outbound_contactlist_export response uri.")

    id = re.search(r'/([a-fA-F\d]+)$', str(uri))
    download_id = id.group(1)

    logging.info("download_id has been extracted successfully.")
    logging.info(f"download_id :: {download_id}")

    logging.info("Initiating Download URL using get_download api.")

    api_response = download_api.get_download(download_id, issue_redirect=False, redirect_to_auth=True)

    regex = r"url': '(.*?)'"
    match = re.search(regex, str(api_response))
    url = match.group(1)

    logging.info("Extracting url from get_download response.")
    logging.info("Download url has been extracted successfully.")
    logging.info(f"Download Url :: {url}")

    time.sleep(180)

    try:
        logging.info("Downloading file using default browser.")
        
        downloaded_file_path = ''
        downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
        
        #webbrowser.open(url=url)
        
        downloads_folder = downloads_folder.replace('\\','/')
        FilePath = f'{downloads_folder}/{CONTACT_LIST_ID}-{CONTACT_LIST_NAME}.csv'
        
        urllib.request.urlretrieve(url=url, filename=FilePath)

        '''
        Wait for the download to complete.
        Checking downloads folder for 5 times in 20sec interval.
        '''

        time.sleep(180)

        num_attempts = 1

        while num_attempts <= 5 and (downloaded_file_path == '' or not os.path.isfile(downloaded_file_path)):
            
            logging.info(f"Attempt to look for downloaded file :: {num_attempts}")
            logging.info(f"Scaning folder :: {downloads_folder}, Looking for file name {CONTACT_LIST_ID}-{CONTACT_LIST_NAME}...")

            for file in os.listdir(downloads_folder):
                
                if file.startswith(CONTACT_LIST_ID):

                    downloaded_file_path = os.path.join(downloads_folder, file)

                    logging.info(f"File has been downloaded to {downloads_folder}.")
                    logging.info(f"File path :: {downloaded_file_path}")

                    '''
                    Fetching metadata info about file.
                    '''

                    __file_name = os.path.basename(downloaded_file_path)
                    __file_size = os.path.getsize(downloaded_file_path)

                    file_timestamp = os.path.getmtime(downloaded_file_path)
                    timestamp_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_timestamp))

                    with open(downloaded_file_path, 'r') as f:
                        num_rows = len(f.readlines())

                    logging.info(f"File metadata: FileName :: {__file_name}, TimeStamp :: {timestamp_str}, Size :: {__file_size} bytes, TotalRows :: {num_rows - 1}")

                    endT = time.time()
                    TotalT = endT - startT
                    hours, remainder = divmod(TotalT, 3600)
                    minutes, seconds = divmod(remainder, 60)

                    Script_Time = "Total time taken :: {:.0f} hr {:.0f} min {:f} sec".format(hours, minutes, seconds)

                    EmailAlert.__SuccessEmailAlert(f"""================================================================\nFile exported successfully for {CONTACT_LIST_NAME} contact list from Genesys Cloud.\n\nFile Metadata\nFileName :: {__file_name}\nTimeStamp :: {timestamp_str}\nSize :: {__file_size} bytes\nTotalRows :: {num_rows - 1}\n\n{Script_Time}\n================================================================\n\n"""
                                        , LOGFILE, FILENAME)

                    if MOVE_FILE_FTP == 1:

                        ''' 
                        This logic will move the file from downloads folder
                        to specific FTP location.
                        '''
                        try:
                            
                            logging.info(f"Moving file to requested FTP path :: {FTP_PATH}")

                            if not os.path.exists(FTP_PATH):

                                # os.makedirs(destination_folder)
                                EmailAlert.__HighImportanceEmailAlert(f"******** EXPORT SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError occurred while moving file to FTP location.\nFTP location not found :: {FTP_PATH}." , LOGFILE, FILENAME)
                                
                                logging.warning("Sys exit.")
                                sys.exit(1)

                            try:

                                __now = datetime.now()
                                __timestamp = __now.strftime("%Y%m%d")
                                __file_name, __file_ext = os.path.splitext(downloaded_file_path)
                                __NFN = f"{__file_name}_{__timestamp}{__file_ext}"

                                destination_path = os.path.join(FTP_PATH, os.path.basename(__NFN))
                                shutil.move(downloaded_file_path, destination_path)
                                
                                logging.info(f"File have been successfully moved to requested FTP Path :: {FTP_PATH}.")

                                EmailAlert.__SuccessEmailAlert(f"""================================================================\nFile have been moved to requested FTP Path successfully for {CONTACT_LIST_NAME} contact list.\n\n{Script_Time}\n================================================================\n\n"""
                                                , LOGFILE, FILENAME)
                                
                            except Exception as e:

                                logging.error(f"Error while creating destination_path / moving file to requested FTP path :: {str(e)}")

                                EmailAlert.__HighImportanceEmailAlert(f"******** EXPORT SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError occurred while moving file to FTP location.\nError while creating destination_path / moving file to requested FTP path." , LOGFILE, FILENAME)

                                logging.warning("Sys exit.")
                                sys.exit(1)

                        except Exception as e:

                            logging.error(f"Exception occurred: {str(e)}")
                            logging.warning("Sys exit.")
                            sys.exit(1)

                    logging.info("Process Completed Successfully.")

                    sys.exit(0)

            num_attempts += 1

            logging.info("Wait Time :: 60s")
            time.sleep(60)

        if downloaded_file_path == '':

            if not os.path.isfile(downloaded_file_path):

                logging.error(f"Error :: File not found in downloads folder. PATH :: {downloads_folder}.Download may have failed due to lack of internet connection.")

                EmailAlert.__HighImportanceEmailAlert(f"******** EXPORT SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError occurred while downloading file / No file found in {downloads_folder} folder.\nDownload may have failed due to lack of internet connection.\n\nPlease open this URL into your browser before the next export job is scheduled.\n\n{url}.\n" , LOGFILE, FILENAME)

                logging.warning("Sys exit.")
                sys.exit(1)

    except Exception as e:

        logging.error(f"Error occurred while running subprocess :: {e}")

        EmailAlert.__HighImportanceEmailAlert(f"******** EXPORT SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError occurred while downloading file using subprocess :: {str(e)}.\n\nPlease open this URL into your browser before the next export job is scheduled.\n\n{url}.\n" , LOGFILE, FILENAME)

        logging.warning("Sys exit.")
        sys.exit(1)

except Exception as e:

    logging.error(f"Exception occurred: {str(e)}")

    EmailAlert.__HighImportanceEmailAlert(f"******** EXPORT SCRIPT ********\n\nContact List :: {CONTACT_LIST_NAME}.\nError occurred while extracting url from API response / API calling func / Downloading file.\n\nErr Msg :: {str(e)}" , LOGFILE, FILENAME)

    logging.warning("Sys exit.")
    sys.exit(1)

#insert into server

import Config
from Config import logging
import EmailAlert
import os
import sys
import datetime
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine,event
import concurrent.futures
import gc
gc.enable()

startT = time.time()

SERVER_NAME = Config._ServerName
DATABASE = Config._Database
DRIVER = Config._driver
TABLE_NAME = Config.TABLE_NAME
FTP = Config.FTP_Path
MAX_THREADS = Config.MAX_THREADS
CHUNK_SIZE = Config.CHUNK_SIZE
FILENAME = Config.FILENAME
LOGFILE = Config.LOGFILE

insert_records_failure_flag_counter = 0
rows_inserted = 0
insertion_err = ''
insert_records_failure_flag = True

try:

    logging.info(f"Connecting to Server to insert data into table :: {TABLE_NAME}.")
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

except Exception as e:

    logging.error(f"Unable to connect to server :: {SERVER_NAME} err_msg :: {e}.")

    EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nUnable to connect to server :: {SERVER_NAME}.\n\nerr_msg :: {e}.\n\n",LOGFILE,FILENAME)

    logging.warning("Sys exit.")
    sys.exit(1)

def get_matching_file(FTP):

    try:
        logging.info(f"Looking for file in path :: {FTP}")

        current_date = datetime.datetime.now().strftime('%Y%m%d')

        for file_name in os.listdir(FTP):
            if file_name.endswith(current_date+'.csv'):
                logging.info(f"File found :: {file_name}")
                return os.path.join(FTP, file_name)
        
        logging.warning(f"No matching file found for system current date :: {current_date} in folder :: {FTP}.")
        return None
    
    except Exception as e:
        logging.error(f"Error occured while looking for file in folder :: {FTP}. err_msg :: {e}")

        EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nError occured while looking for file in folder :: {FTP}.\n\nerr_msg :: {e}\n\n",LOGFILE,FILENAME)

        logging.warning("Sys exit.")
        sys.exit(1)

def insert_records(chunk):

    try:
        global rows_inserted, insert_records_failure_flag,insertion_err,insert_records_failure_flag_counter

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        with ENGINE.connect() as CONN:
            chunk.to_sql(TABLE_NAME, CONN, index=False, if_exists="append", schema="dbo")
            rows_inserted += len(chunk)
            
            CONN.commit()
        
        CONN.close()

    except Exception as e:

        CONN.rollback()
        insertion_err += str(e)

        insert_records_failure_flag_counter += 1

        logging.error(f"Unable to insert data in table :: {TABLE_NAME}. err_msg :: {insertion_err}")

        if insert_records_failure_flag:
            insert_records_failure_flag = False

            EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nUnable to insert data in table :: {TABLE_NAME}.\n\nerr_msg :: {insertion_err}.\n\n",LOGFILE,FILENAME)

        else:
            logging.info(f"No Email triggered becuase flag insert_records_failure_flag :: {insert_records_failure_flag}.")

def create_chunk(df):

    global insertion_err

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        
        @event.listens_for(ENGINE, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True
                
        futures = []

        logging.info(f"Inserting data into table :: {TABLE_NAME}.")

        for chunk in chunks:
            future = executor.submit(insert_records,chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

            if insert_records_failure_flag_counter > 1:

                EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nProcess terminated becasue of multiple failure while inserting rows.\nUnable to insert data in table :: {TABLE_NAME}.\n\nerr_msg :: {insertion_err}.\n\n",LOGFILE,FILENAME)

                logging.warning("Sys exit.")
                sys.exit(1)
    
    logging.info(f"Data inserted successfully into table :: {TABLE_NAME}.")
    logging.info(f"Total number of rows inserted :: {rows_inserted}.")
    logging.info("Process completed successfully.")

    Success_percentage = (rows_inserted/len(df)) * 100
    Success_Per = "Success Percentage :: {:.2f}%".format(Success_percentage)

    logging.info(Success_Per)

    endT = time.time()
    TotalT = endT - startT
    hours, remainder = divmod(TotalT, 3600)
    minutes, seconds = divmod(remainder, 60)

    Script_Time = "Total time taken :: {:.0f} hr {:.0f} min {:f} sec".format(hours, minutes, seconds)

    logging.info(Script_Time)

    if rows_inserted == len(df):

        EmailAlert.__SuccessEmailAlert(f"""================================================================\nData has been inserted into table :: {TABLE_NAME}.\n\nTotal rows to be inserted :: {len(df)}.\nTotal rows inserted :: {rows_inserted}.\n{Success_Per}.\n\n{Script_Time}\n================================================================\n\n"""
                                        , LOGFILE, FILENAME)

if __name__ == '__main__':

    matching_file = get_matching_file(FTP)

    if matching_file:
        df = pd.read_csv(matching_file,sep=',',low_memory=False)
        create_chunk(df)

    else:
        EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nNo matching file found for system current date in folder :: {FTP}.",LOGFILE,FILENAME)

        logging.warning("No file found. Sys exit.")
        sys.exit(1)
        
#insert into server OLD

import Config
from Config import logging
import EmailAlert
import os
import sys
import datetime
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine,text, bindparam
import concurrent.futures
import gc
gc.enable()

startT = time.time()

SERVER_NAME = Config._ServerName
DATABASE = Config._Database
DRIVER = Config._driver
TABLE_NAME = Config.TABLE_NAME
FTP = Config.FTP_Path
MAX_THREADS = Config.MAX_THREADS
CHUNK_SIZE = Config.CHUNK_SIZE
FILENAME = Config.FILENAME
LOGFILE = Config.LOGFILE

insert_records_failure_flag_counter = 0
rows_inserted = 0
insertion_err = ''
insert_records_failure_flag = True

try:

    logging.info(f"Connecting to Server to insert data into table :: {TABLE_NAME}.")
    ENGINE = create_engine(f'mssql+pyodbc://{SERVER_NAME}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

except Exception as e:

    logging.error(f"Unable to connect to server :: {SERVER_NAME} err_msg :: {e}.")

    EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nUnable to connect to server :: {SERVER_NAME}.\n\nerr_msg :: {e}.\n\n",LOGFILE,FILENAME)

    logging.warning("Sys exit.")
    sys.exit(1)

def get_matching_file(FTP):

    try:
        logging.info(f"Looking for file in path :: {FTP}")

        current_date = datetime.datetime.now().strftime('%Y%m%d')

        for file_name in os.listdir(FTP):
            if file_name.endswith(current_date+'.csv'):
                logging.info(f"File found :: {file_name}")
                return os.path.join(FTP, file_name)
        
        logging.warning(f"No matching file found for system current date :: {current_date} in folder :: {FTP}.")
        return None
    
    except Exception as e:
        logging.error(f"Error occured while looking for file in folder :: {FTP}. err_msg :: {e}")

        EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nError occured while looking for file in folder :: {FTP}.\n\nerr_msg :: {e}\n\n",LOGFILE,FILENAME)

        logging.warning("Sys exit.")
        sys.exit(1)

def insert_records(chunk):

    try:
        global rows_inserted, insert_records_failure_flag,insertion_err,insert_records_failure_flag_counter

        cnx = ENGINE.connect()

        chunk = chunk.rename(columns=lambda x: x.replace('-', ''))
        chunk.fillna('NULL', inplace=True)

        float_columns = chunk.select_dtypes(include='float').columns
        chunk[float_columns] = chunk[float_columns].replace([np.inf, -np.inf], np.nan)
        chunk[float_columns] = chunk[float_columns].astype(pd.Int64Dtype())

        insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(chunk.columns)}) VALUES ({', '.join([':' + col for col in chunk.columns])})"

        with cnx.begin() as transaction:
            stmt = text(insert_query)
            stmt = stmt.bindparams(*[bindparam(col) for col in chunk.columns])
            cnx.execute(stmt, chunk.to_dict(orient='records'))
            transaction.commit()
        
        cnx.close()
        rows_inserted += len(chunk)

    except Exception as e:

        insertion_err += str(e)

        insert_records_failure_flag_counter += 1

        logging.error(f"Unable to insert data in table :: {TABLE_NAME}. err_msg :: {insertion_err}")

        if insert_records_failure_flag:
            insert_records_failure_flag = False

            EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nUnable to insert data in table :: {TABLE_NAME}.\n\nerr_msg :: {insertion_err}.\n\n",LOGFILE,FILENAME)

        else:
            logging.info(f"No Email triggered becuase flag insert_records_failure_flag :: {insert_records_failure_flag}.")

def create_chunk(df):

    global insertion_err

    chunks = [df[i:i+CHUNK_SIZE] for i in range(0, len(df), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futures = []

        logging.info(f"Inserting data into table :: {TABLE_NAME}.")

        for chunk in chunks:
            future = executor.submit(insert_records,chunk)
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            print(future)

            if insert_records_failure_flag_counter > 1:

                EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nProcess terminated becasue of multiple failure while inserting rows.\nUnable to insert data in table :: {TABLE_NAME}.\n\nerr_msg :: {insertion_err}.\n\n",LOGFILE,FILENAME)

                logging.warning("Sys exit.")
                sys.exit(1)
    
    logging.info(f"Data inserted successfully into table :: {TABLE_NAME}.")
    logging.info(f"Total number of rows inserted :: {rows_inserted}.")
    logging.info("Process completed successfully.")

    Success_percentage = (rows_inserted/len(df)) * 100
    Success_Per = "Success Percentage :: {:.2f}%".format(Success_percentage)

    logging.info(Success_Per)

    endT = time.time()
    TotalT = endT - startT
    hours, remainder = divmod(TotalT, 3600)
    minutes, seconds = divmod(remainder, 60)

    Script_Time = "Total time taken :: {:.0f} hr {:.0f} min {:f} sec".format(hours, minutes, seconds)

    logging.info(Script_Time)

    if rows_inserted == len(df):

        EmailAlert.__SuccessEmailAlert(f"""================================================================\nData has been inserted into table :: {TABLE_NAME}.\n\nTotal rows to be inserted :: {len(df)}.\nTotal rows inserted :: {rows_inserted}.\n{Success_Per}.\n\n{Script_Time}\n================================================================\n\n"""
                                        , LOGFILE, FILENAME)

if __name__ == '__main__':

    matching_file = get_matching_file(FTP)

    if matching_file:
        df = pd.read_csv(matching_file,sep=',')
        create_chunk(df)

    else:
        EmailAlert.__HighImportanceEmailAlert(f"******** Insert data into table SCRIPT ********\n\nNo matching file found for system current date in folder :: {FTP}.",LOGFILE,FILENAME)

        logging.warning("No file found. Sys exit.")
        sys.exit(1) 

###################################################################################################################################################################

#Json Break

DATA = [
{

  "surgeryRequestId": "64501fc779778230c42b288c",

  "eventType": "SCHEDULED",

  "duration": "60",

  "holdEventId": "645092ab6c030c0ebad31dc4",

  "hospitalId": "5e85e614c99ab800147ca401",

  "hospital": "Ascension Saint Thomas Hospital Midtown",

  "unitId": "5e85e614c99ab800147ca502",

  "unit": "BH JRI",

  "roomName": "BH JRI 01",

  "edslId": "W1-CRT10748654",

  "caseNumber": "JRI-2023-85",

  "fin": "570422063",

  "appointmentDetail": {

    "procedureDate": "2023-05-03 14:00"

  },

  "diagnosis": [

    {

      "code": "Q72.43",

      "description": "ICD description",

      "primary": True

    },

    {

      "code": "Q72.55",

      "description": "ICD description2",

      "primary": False

    }

  ],

  "procedures": [

    {

      "primary": True,

      "procedureName": "ORIF ValidateThirteen",

      "cptCodes": [

        "Tests",

        "Tests",

        "Tests"

      ],

      "surgeon": {

        "name": "Raab, Gregory E. MD",

        "primary": True,

        "npi": 1295700110

      }

    },

    {

      "primary": False,

      "procedureName": "ORIF ValidateThirteen",

      "cptCodes": [

        "Tests",

        "Tests",

        "Tests"

      ],

      "surgeon": {

        "name": "Raab, Gregory E. MD",

        "primary": True,

        "npi": 1295700110

      }

    }

  ],

  "createdBy": "ds-ct-ORSCH1 ds-ct-ORSCH1",

  "dateCreated": "2023-05-01 20:23",

  "timestamp": "2023-06-13T12:05:48.265Z"

}]


import pandas as pd

def normalize_data(data):
    normalized_data = []

    for item in data:
        row = {}

        # Extracting top-level attributes
        row["surgeryRequestId"] = item.get("surgeryRequestId")
        row["eventType"] = item.get("eventType")
        row["duration"] = item.get("duration")
        row["holdEventId"] = item.get("holdEventId")
        row["hospitalId"] = item.get("hospitalId")
        row["hospital"] = item.get("hospital")
        row["unitId"] = item.get("unitId")
        row["unit"] = item.get("unit")
        row["roomName"] = item.get("roomName")
        row["edslId"] = item.get("edslId")
        row["caseNumber"] = item.get("caseNumber")
        row["fin"] = item.get("fin")
        row["createdBy"] = item.get("createdBy")
        row["dateCreated"] = item.get("dateCreated")
        row["timestamp"] = item.get("timestamp")

        # Extracting appointment detail
        appointment_detail = item.get("appointmentDetail")
        if appointment_detail:
            row["procedureDate"] = appointment_detail.get("procedureDate")

        # Extracting diagnosis
        diagnosis = item.get("diagnosis")
        if diagnosis:
            for index, diag in enumerate(diagnosis):
                row[f"diagnosis_{index+1}_code"] = diag.get("code")
                row[f"diagnosis_{index+1}_description"] = diag.get("description")
                row[f"diagnosis_{index+1}_primary"] = diag.get("primary")

        # Extracting procedures
        procedures = item.get("procedures")
        if procedures:
            for index, proc in enumerate(procedures):
                row[f"procedure_{index+1}_primary"] = proc.get("primary")
                row[f"procedure_{index+1}_name"] = proc.get("procedureName")

                cpt_codes = proc.get("cptCodes")
                if cpt_codes:
                    for cpt_index, cpt_code in enumerate(cpt_codes):
                        row[f"procedure_{index+1}_cpt_{cpt_index+1}"] = cpt_code

                surgeon = proc.get("surgeon")
                if surgeon:
                    row[f"procedure_{index+1}_surgeon_name"] = surgeon.get("name")
                    row[f"procedure_{index+1}_surgeon_primary"] = surgeon.get("primary")
                    row[f"procedure_{index+1}_surgeon_npi"] = surgeon.get("npi")

        normalized_data.append(row)

    return normalized_data
	
normalized_data = normalize_data(DATA)

DF = pd.DataFrame(normalized_data)

print(DF)



def normalize_data(data):
    normalized_data = []

    for item in data:
        row = {}

        for key, value in item.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    row[f"{key}_{sub_key}"] = sub_value
            elif isinstance(value, list):
                for idx, sub_item in enumerate(value, start=1):
                    if isinstance(sub_item, dict):
                        for sub_key, sub_value in sub_item.items():
                            if isinstance(sub_value, dict):
                                for sub_sub_key, sub_sub_value in sub_value.items():
                                    row[f"{key}_{idx}_{sub_key}_{sub_sub_key}"] = sub_sub_value
                            else:
                                row[f"{key}_{idx}_{sub_key}"] = sub_value
                    else:
                        row[f"{key}_{idx}"] = sub_item
            else:
                row[key] = value

        normalized_data.append(row)

    return normalized_data
	
normalized_data = normalize_data(DATA)

DFF = pd.DataFrame(normalized_data)

print(DFF)
