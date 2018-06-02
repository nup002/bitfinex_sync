import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from src import clients
import configparser
from src import dataset_handler as dh
from colorama import init

"""Collects candles data from Bitfinex and stores them in the local HDF5 dataset.
No parameters are required apart from the local dataset path, which is obtained from the config file.
It is slow due to the DDoS protection that Bitfinex implements. Any faster, and the program is blocked for a minute."""

#Read the config file
config = configparser.ConfigParser()
configpath = os.path.dirname(__file__) + 'config.ini'
if os.path.isfile(configpath):
    config.read(configpath)
    candlespath = config['DATASETS']['candles_dataset_path'] #Get candles dataset filepath

    #Initiate colorama for colored terminal output text
    init(autoreset=True)

    #Get the bitfinex API client
    apiClient = clients.BitfinexPublic()

    #Get the candles dataset handler
    handler = dh.CandlesHandler(path=candlespath)

    #syncronize candles dataset with bitfinex
    handler.syncDatafile(apiClient)
else:
    print("Could not find configuration file \"config.ini\"")