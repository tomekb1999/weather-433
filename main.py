import subprocess
import socket
import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
import datetime
import requests
from matplotlib import pyplot as plt
import time

class Receiver():
    sock = None
    con = None
    _received_data = None
    
    def __init__(self, socket_connection, sql_connection):
        self.sock = socket_connection
        self.con = sql_connection
    
    def read(self): 
        # Receive chunk of data
        rec_data = sock.recvfrom(2048)
    
        # Create pandas DataFrame
        json_data = rec_data[0].decode('utf-8')
        json_data = json_data.split('- - - ')[1]
        json_data = json.loads(json_data)

        tab = pd.DataFrame([json_data], columns=["time", "model", "id", "temperature_C"])
        tab["time"] = pd.to_datetime(tab["time"])
        tab = tab.set_index("time")
        self._received_data = tab
        print("New data received:")
        print(self._received_data)
        
    def save_to_sql(self):
        print("Saving data to the database")
        self._received_data.to_sql(con=self.con, name="dane", if_exists="append")
        
class Plotter():
    con = None
    _start_time = None
    _end_time = None
    temperatures_from_database = None
    prognosed_temperatures = None
        
    def __init__(self, sql_connection):
        self.con = sql_connection
        
    def update_times(self):
        self._start_time = datetime.datetime.now() - datetime.timedelta(hours=PLOTTING_INTERVAL_HOURS)
        self._end_time = datetime.datetime.now() + datetime.timedelta(hours=PROGNOSE_INTERVAL_HOURS)
        
    def read_from_db(self):
        query = f"SELECT * FROM dane WHERE time > '{str(self._start_time)}' "
        data = pd.read_sql(query, self.con, index_col=["model", "time"])
        data = data.sort_index(axis=0)
        self.temperatures_from_database = data
        
    def read_prognose(self):
        start_date = self._start_time.date()
        end_date = self._end_time.date() + datetime.timedelta(days=1) # We want to include current day in the data
        
        url = f'https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=temperature_2m&start_date={str(start_date)}&end_date={str(end_date)}'
        resp = requests.get(url)
        prognosed_data = json.loads(resp.text)["hourly"]
        
        prognosed_data = pd.DataFrame(prognosed_data)
        prognosed_data["time"] = pd.to_datetime(prognosed_data["time"])
        prognosed_data = prognosed_data.set_index("time")
        prognosed_data = prognosed_data[self._start_time:self._end_time]
        self.prognosed_temperatures = prognosed_data
    
    def plot(self):
        models = self.temperatures_from_database.index.get_level_values("model").unique()
        
        plt.clf()
        for model in models:
            spec_model_temp = self.temperatures_from_database.loc[model]
            plt.plot(spec_model_temp.index, spec_model_temp["temperature_C"], label=model)
        
        plt.plot(self.prognosed_temperatures.index, self.prognosed_temperatures["temperature_2m"], label="Prognosed")
        plt.xticks(rotation = 45)
        plt.legend()
        plt.savefig("current_temperatures.png")
        

# Run rtl_433 with syslog output
RTL_COMMAND = ['rtl_433', '-F', 'syslog:127.0.0.1:1433', '-R', '13']
print(f"Starting rtl_433")
rtl_instance = subprocess.Popen(RTL_COMMAND)

# UDP settings
UDP_IP = "127.0.0.1"
UDP_PORT = 1433

# MySQL settings
MYSQL_USER = "username"
MYSQL_PASS = "password"
MYSQL_IP = "127.0.0.1"
MYSQL_PORT = "3306"

# Prognose localization
LATITUDE = 0
LONGITUDE = 0

# Plotting settings
PLOTTING_INTERVAL_HOURS = 24
PROGNOSE_INTERVAL_HOURS = 5

# Connect to MySQL database
mysql_con = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_IP}:{MYSQL_PORT}/weather_433", echo=False)

# Connect to proper UDP port and start listening
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
sock.bind((UDP_IP, UDP_PORT))
print(f"Receiving data on {UDP_IP}:{UDP_PORT}...")

rec = Receiver(sock, mysql_con)
plotter = Plotter(mysql_con)

while(True):
    try:
        rec.read()
        rec.save_to_sql()
        
        plotter.update_times()
        plotter.read_from_db()
        plotter.read_prognose()
        plotter.plot()
    except Exception as exeption_message:
        logging.error(f"{exeption_message} \n Error! Trying to restart rtl_433...")
        rtl_instance.terminate()
        time.sleep(5)
        rtl_instance = subprocess.Popen(RTL_COMMAND)
        