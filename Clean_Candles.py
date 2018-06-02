import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from src import clients
import configparser
from src import dataset_handler as dh
from colorama import init

"""Goes through the candles file that contains raw candles from the exchange, and cleans it up.
At the moment, only outliers are scaled down. Possible to extend this in the future."""

#Read the config file
config = configparser.ConfigParser()
configpath = os.path.dirname(__file__) + 'config.ini'
if os.path.isfile(configpath):
    config.read(configpath)
    candlespath_raw = config['DATASETS']['candles_dataset_path'] #Get raw candles dataset filepath
    candlespath_clean = config['DATASETS']['clean_candles_dataset_path'] #Get clean candles dataset filepath

    #Initiate colorama for colored terminal output text
    init(autoreset=True)

    #Get the clean candles and raw candles dataset handlers
    raw_handler = dh.CandlesHandler(path=candlespath_raw)
    clean_handler = dh.CandlesHandler(path=candlespath_clean)

    #If no system arguments are given when the script runs, we simply go through every dataset in the raw candles file
    #and clean the parts that do not exist in the clean dataset yet, and appends it to the clean dataset.
    latest_clean_timestamps = clean_handler.latestMTS()
    latest_raw_timestamps = raw_handler.latestMTS()
    for timebase in latest_clean_timestamps:
        clean_ts = latest_clean_timestamps[timebase]
        if clean_ts != 0:
            mode = 'append'
        else:
            mode = 'skip'
        raw_ts = latest_raw_timestamps[timebase]
        if clean_ts != raw_ts:
            #We need to update the cleaned dataset.
            #Start by cleaning the candles that do not exist on file
            cleaned, _, _, _, _ = raw_handler.scaleOutliers(timebase, start=clean_ts)
            clean_handler.saveDataset(cleaned, timebase, mode=mode)
else:
    print("Could not find configuration file \"config.ini\"")