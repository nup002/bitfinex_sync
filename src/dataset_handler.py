import os
import sys
import h5py
import pprint
import progressbar
import time
import math
import numpy as np
import datetime
from scipy.signal import gaussian
from colorama import Fore, Back, Style
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from src import converters
import pandas
#Handler class for the HDF5 datasets used in pytrader

class CandlesHandler:
    """A dataset handler class that handles the hdf5 files used for storing raw candles data in pytrader.
    A single CandlesHandler can only handle one datafile at a time. """
    
    def __init__(self, path=None):
        #Initiate variables
        #Check if the dataset exists    
        self.valid_coloumns = ['MTS', 'OPEN', 'CLOSE', 'HIGH', 'LOW', 'VOLUME']
        self.valid_timebases = ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M']
        
        self.datafile_path = path      
        self.candlesfile = None
        self._openHDF5()

        
        
    def _openHDF5(self, silent=False):
        """Opens an hdf5 datafile. Performs checks to make sure it exists and conforms.
        Parameters
        ----------
        silent=False  :   Boolean. If true, it will create a dataset if it cannot find one without prompting the user, nor printing anything."""
        
        if self.datafile_path is not None:
            if not silent:
                datafile_folder, datafile_name = os.path.split(self.datafile_path)
                filelist = os.listdir(datafile_folder)
                if datafile_name not in filelist:
                    print("Could not find the dataset named \"{}\" in the folder \"{}\". Existing files in this path:".format(datafile_name, os.path.dirname(os.path.realpath(__file__))))
                    for file in filelist:
                        print("\t" + file)
                    while True:
                        answer = input("Do you wish to create the file \"{}\"? [y/n] ".format(datafile_name)).lower()
                        if answer == 'y':
                            break
                        elif answer == 'n':
                            print("Exiting pytrader.")
                            exit()
                        else:
                            print("Not a valid answer.")
                        
            self.candlesfile = h5py.File(self.datafile_path, "a")

            #Check if the necessary datasets exists within the hdf5 file
            a = self.candlesfile
            existing_objects = a.keys()
            
            for r in self.valid_timebases:
                if r not in existing_objects:
                    if not silent:
                        print("Could not find dataset \"{}\" in the hdf5 file \"{}\". Creating.".format(r, datafile_name))
                    a.create_dataset(r, (0, 6), maxshape=(None,6), fillvalue=np.NAN)
    
    def _pandasToHDF5(self, set, timebase):
        """Takes a pandas dataframe and makes it ready for a save to a hdf5 dataset.
        It will sort the coloumns into the right order and fill nonexistant coloumns with NaN.
        The returned python array can be saved to the hdf5 candles datafile."""
        length = set.shape[0]
        returnArray = np.zeros((length, 6))
        returnArray[:] = np.NAN
        clmns = set.columns.values
        n = 0
        for clmn in self.valid_coloumns:
            if clmn in clmns:
                returnArray[:,n] = set[clmn]
            n += 1
        return returnArray
        
    def open(self, path=None, new=False):
        """Open a new hdf5 candlesfile. Closes the original file.
        Parameters:
        -----------
        path        :   Optional string. Full path of the file to open. If omitted, uses the current value of candlesHandler.datafile_path.
        new=False   :   optional boolean. If True, open file without prompting user in case file does not exist."""
        
        if path!=None:
            self.datafile_path = path
        self.close()
        self._openHDF5(silent=new)
    
    def close(self):
        #Closes the currently open hdf5 file
        if isinstance(self.candlesfile, h5py.File):
            try:
                self.candlesfile.close()
                self.candlesfile = None
            except:
                pass # Was already closed
                
    def latestMTS(self):
        """Returns a dictionary of the latest MTS timestamps of each dataset in the currently open datafile.
        If no datafile is open, a warning is thrown and all MTS fields will be zero.
        """
        MTSdict = {'1m':0, '5m':0, '15m':0, '30m':0, '1h':0, '3h':0, '6h':0, '12h':0, '1D':0, '7D':0, '14D':0, '1M':0}
        if self.candlesfile is None:
            raise Warning("Tried to get the latest timestamps in a dataset that was not open.")
            return MTSdict
        else:
            for timebase in self.valid_timebases:
                try:
                    MTSdict[timebase] = self.candlesfile[timebase][-1,0]
                except:
                    pass
            return MTSdict
            
    def syncDatafile(self, client):
        #updates the dataset so that it contains all candles for all time.
        #This takes a long time to run the first time.
        file = self.candlesfile
        for dataset_name in file.keys():
            print(Fore.CYAN + "Checking the following dataset: {}".format(dataset_name))
            dataset = file[dataset_name]
            
            #Grab the most recent candle timestamp 
            newestts = client.get_candlesticks(dataset_name, 'tBTCUSD', 'last')[0][0]
            #Check if it matches the most current timestamp of the corresponding dataset
            try:
                latest_time = int(dataset[-1][0]) #Get the latest timestamp
            except ValueError:
                #In case of indexerror, the dataset has no data. Likely because it has just been created.            
                latest_time = 0   
            
            if latest_time != 0:
                latest_time_readable = datetime.datetime.fromtimestamp(int(latest_time)).strftime('%Y-%m-%d %H:%M:%S')
            else:
                latest_time_readable = "Dataset is empty!"
            newestts_readable = datetime.datetime.fromtimestamp(int(newestts)).strftime('%Y-%m-%d %H:%M:%S')
            print("Latest dataset timestamp: {}\tLatest exchange timestamp: {}\t==>\t".format(latest_time_readable, newestts_readable), end='')
            
            #We add 2 minute to each side to allow for errors in going from float to int
            if latest_time - 60*2 < newestts < latest_time + 60*2:
                print(Fore.GREEN + "The dataset is up to date.\n".format(dataset_name))
            else:         
                print(Fore.YELLOW + "The dataset is not up to date.".format(dataset_name))            
                #We get the maximum amount of allowed candles (up to 1000) from bitfinex that are available 
                #since the last candle was added to the dataset.
                candles = client.get_candlesticks(dataset_name, 'tBTCUSD', 'hist', limit=1000, start=int(latest_time+1)*1000, sort=1) 
                
                print(Fore.GREEN + "SYNCHRONIZING WITH BITFINEX...", end='')
                if latest_time == 0:
                    print(Fore.GREEN + " (from the beginning of time)")
                else:
                    print('')
                    
                #We get the first candle timestamp of the first call so we know where we began
                if len(candles)==0:
                    # In this case the api return is buggy. Primarily happens for the >1M return.
                    candlesOldestTs = newestts
                else:
                    candlesOldestTs = candles[0][0] 
                tsdiff = newestts - candlesOldestTs
                #Divide tsdiff (ms) with a number to get it into the timescale of the current dataset.
                if dataset_name == '1m':
                   s = (60)
                if dataset_name == '5m':
                   s = (60*5)   
                if dataset_name == '15m':
                   s = (60*15)   
                if dataset_name == '30m':
                   s = (60*30)    
                if dataset_name == '1h':
                   s = (60*60)
                if dataset_name == '3h':
                   s = (60*60*3)   
                if dataset_name == '6h':
                   s = (60*60*6)   
                if dataset_name == '12h':
                   s = (60*60*12)    
                if dataset_name == '1D':
                   s = (60*60*24)
                if dataset_name == '7D':
                   s = (60*60*24*7)   
                if dataset_name == '14D':
                   s = (60*60*24*14)   
                if dataset_name == '1M':
                   s = (60*60*24*31)    
                   
                callsNeeded = int(tsdiff/(s*1000))+1 #multiply s with 1000 since we get 1000 candles at a time
                if callsNeeded > 0:
                    bar = progressbar.ProgressBar(max_value=callsNeeded)
                    bar.update(0)
                    time.sleep(0.05) #Hack to get it to work
                    bar.update(1)
                callno = 0   
                finalCall = False
                while True:
                    try:
                        latest_time = int(dataset[-1][0]) #Get the latest timestamp of the dataset
                    except ValueError:
                        #In case of indexerror, the dataset has no data. Likely because it has just been created.
                        latest_time = 0
                    
                    if callno > 0 and not finalCall:
                        #We get the maximum amount of allowed candles (up to 1000) from bitfinex that are available 
                        #since the last candle was added to the dataset.
                        candles = client.get_candlesticks(dataset_name, 'tBTCUSD', 'hist', limit=1000, start=int(latest_time+1)*1000, sort=1)
                    
                    if finalCall:
                        #Some candle api calls are buggy and will not return the very last candle. 
                        #We use this if-case to check if it was obtained.
                        candles = client.get_candlesticks(dataset_name, 'tBTCUSD', 'last')
                        if candles[0][0] != latest_time:
                            dataset.resize(dataset.len()+len(candles), 0) #Resize to fit more candles.
                            dataset[-len(candles):, :] = candles #Add candles to the end
                        break
                        
                    #print("Candles returned: {}".format(len(candles)))
                    if len(candles)<1000:
                        finalCall = True

                    callno += 1
                    if callno <= callsNeeded:
                        bar.update(callno)
                    if len(candles)!=0:
                        #print("Got {} candles from the period {}-{}".format(len(candles), candles[0][0], candles[-1][0]))
                        dataset.resize(dataset.len()+len(candles), 0) #Resize to fit more candles.
                        dataset[-len(candles):, :] = candles #Add candles to the end
                print(Fore.GREEN + "\nDone!\n")
                          
    def getDataset(self, timebase, coloumns, start=None, end=None, startIndex=None, endIndex=None, length=None):
        """Returns the dataset specified as a pandas dataframe.
        Provide the third variable as an array of coloumn names that correspond to the coloumns you
        wish to have returned. Valid options are: 'ALL' (gives as str, not arr), or any combination of 
        the following (as arr): 'MTS', 'OPEN', 'CLOSE', 'HIGH', 'LOW', 'VOLUME'.
        
        Give the optional parameters start and end to get slice of dataset.
        There will be no candles returned before start, and no candles after end.
        Start and end can be either integer timestamps, datetime objects, or strings following this
        format: '%Y-%m-%d %H:%M:%S'.
        
        You can also supply the parameters startIndex and endIndex if you know exactly
        the indeces of the dataset subset you wish to extract. This is much faster than
        using start and end.
        
        Finally, you may supply a variable "length" if only one of the variables start, end, startIndex,
        or endIndex have been given.
        
        Parameters
        ----------
        timebase    : The timebase of the candles dataset you want. Valid options: '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'
        coloumns    : The coloumns of the dataset you want. Either the string 'ALL', or an array consisting of any of the following values (repeat values are allowed): 'MTS', 'OPEN', 'CLOSE', 'HIGH', 'LOW', 'VOLUME'.
        start       : Optional. Time start of returned dataset. Integer timestamp, datetime object, or string following this format: '%Y-%m-%d %H:%M:%S'
        end         : Optional. Time end of returned dataset. Follows same logic as the variable start.
        startIndex  : Optional. Start index of dataset. Faster than supplying the variable start. Used when you already know the start-index of your desired dataset.
        endIndex    : Optional. End index of dataset. Follows same logic as the parameter startIndex.
        length      : Optional. Length of the dataset you wish to have returned. 
        
        Return
        ------
        dataframe   : Pandas dataframe containing the requested dataset.
        
        """
        
        returnAllClmns = False
        #Check of coloumns are valid
        if isinstance(coloumns, str):
            if coloumns == "ALL":
                returnAllClmns = True
            else:
                raise RuntimeError("\"{}\" is not a valid value for the parameter \"coloumns\".".format(coloumns))
        elif isinstance(coloumns, list):
            for a in coloumns:
                if a not in self.valid_coloumns:
                    raise RuntimeError("\{}\" is not a valid element of the parameter \"coloumns\". Valid values are: {}".format(a, self.valid_coloumns))
        else:
            raise RuntimeError("The\"coloumns\" parameter must be either the string 'ALL' or an array of any of the following values:{}".format(self.valid_coloumns))
        
        #Check if length is given and valid
        if length is not None:
            if length < 1:
                raise RuntimeError("Length must be positive and greater than 0.")
                
        #Check of timestamps or dataset indeces have been provided
        tsGiven = start is not None or end is not None
        indecesGiven = startIndex is not None or endIndex is not None
        
        #If start or end are passed, startIndex and endIndex may not be passed, and vice versa.
        if tsGiven and indecesGiven:
            raise RuntimeError("You must pass either the parameters \"start\" and/or \"end\", OR the parameters \"startIndex\" and/or \"endIndex\".")
                
        #If the parameters start or end are given, we check if they are given as datetimes or str and if
        #so, they are converted to timestamps.
        if isinstance(start, datetime.datetime) or isinstance(start, str):
            start = converters.dtToTs(start)
        if isinstance(end, datetime.datetime) or isinstance(end, str):
            end = converters.dtToTs(end)   
            
        #Start the extraction of data
        if timebase in self.candlesfile.keys():
            fullset = self.candlesfile[timebase]

            #We employ the fact that the candles datasets are sorted in time to locate the indeces
            #needed to extract a specific subset of.
            if tsGiven:
                timestamps = fullset[:,0] #Very slow! 
                if start is not None:
                    startIndex = np.searchsorted(timestamps, float(start))+1
                if end is not None:
                    endIndex = np.searchsorted(timestamps, float(end))-1
            #Then we see if length is given, and if it is, we use the start or end index to find
            #the end or start index to return an array of length "length"
            if ((startIndex is not None) ^ (endIndex is not None)) and length is not None:
                if startIndex is not None:
                    endIndex = startIndex + length
                else:
                    startIndex = endIndex - length
                    
            #We finally make sure that the start and end indices are within limits.
            length = fullset.shape[0]
            if startIndex is not None:
                if startIndex <0:
                    startIndex = None
                if startIndex > length:
                    startIndex = length
            if endIndex is not None:
                if endIndex <0:
                    endIndex = 0   
                if endIndex > length:
                    endIndex = None 
            if startIndex is not None and endIndex is not None:
                if startIndex>endIndex:
                    startIndex=endIndex
             
            if not tsGiven and not indecesGiven:
                #We work on the entire dataset.
                reducedset = fullset
            else:
                reducedset = fullset[startIndex:endIndex,:]
                
            #We then grab the coloumns that were requested, and return them in the order they were requested.
            if not returnAllClmns:
                n = 0
                numClmns = len(coloumns)
                if len(coloumns) <= 1:
                    returnset = np.zeros(reducedset.shape[0], dtype = np.float)
                else:
                    returnset = np.zeros((reducedset.shape[0], numClmns), dtype = np.float)
                    
                for a in coloumns:
                    i = 0
                    if a == 'OPEN':
                        i = 1
                    if a == 'CLOSE':
                        i = 2
                    if a == 'HIGH':
                        i = 3
                    if a == 'LOW':
                        i = 4
                    if a == 'VOLUME':
                        i = 5
                    if numClmns <= 1:
                        returnset = reducedset[:,i]
                    else:
                        returnset[:,n] = reducedset[:,i]
                    n += 1
                    
                return pandas.DataFrame(returnset, columns=coloumns)
            return pandas.DataFrame(reducedset , columns=self.valid_coloumns)
        else:
            raise RuntimeError("No dataset named \"{}\".".format(timebase))          
            
    def saveDataset(self, set, timebase, mode='append', keepnan = False):
        """Saves a dataset into the currently open datafile. If no datafile is open, the file located at the path specified by
        datafile_path is opened. If it does not exist, it is created. The dataset is saved to the opened or created file.
        The dataset that is provided must be a pandas dataframe that follows the standard candles format. 
        
        Specific requirements for set:
        Its coloumns must all be named, and their names must be from the following list: 'MTS', 'OPEN', 'CLOSE', 'HIGH', 'LOW', 'VOLUME'.
        No two coloumns may have the same name.
        There must be a coloumn named 'MTS' (unless using append mode). Otherwise it is not possible to determine where the data should be inserted.
        
        When saving to a datafile that contains data from before, use the parameter 'mode' to determine how the dataset is saved.
        'append': Every coloumn is merely appended to the existing coloumns. If a coloumn does not exist in the dataset,
        but exist in the datafile, the nonexisting coloumn is created and filled with NaN.
        'skip': To use this mode, the 'MTS' coloumn must exist in both the datafile and dataset to save. The two sets are synced by the 'MTS'
        coloumn. On those locations where data overlaps, the data from the datafile is kept IF IT IS NOT NaN.
        'replace': Same as 'skip', but instead, where data overlaps, the existing data is overwritten by data from the dataset, UNLESS IT IS NaN.
        
        To instead keep NaN, pass keepnan = True.
        
        Parameters
        ----------
        set             :   The dataset you wish to save.
        timebase        :   Timebase of the dataset 'set'. Valid options: '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'
        mode='append'   :   String, optional. One of the following: 'append', 'skip', 'replace'. 
        keepnan = False :   Boolean, optional. Pass True to make NaN be treated as a regular number in the modes 'skip' and 'replace'.
        """
        
        valid_modes = ['append', 'skip', 'replace']
        
        #Check parameter validity
        if timebase not in self.valid_timebases:
            raise ValueError("The variable 'timebase' must be one of the following: {}".format(self.valid_timebases))
        if mode not in valid_modes:
            raise ValueError("The variable 'mode' must be one of the following: {}".format(valid_modes))
        
        #Check set validity of coloumn names
        set_coloumns = set.columns.values
        print(set_coloumns)
        if 'MTS' not in set_coloumns:
            raise RuntimeError("There is no coloumn named 'MTS' in the provided dataset.")
        #Check repetition
        if len(set_coloumns)!=len(np.unique(set_coloumns)):
            raise RuntimeError("There are multiple coloumns in the provided dataset that have the same name.")
        for clmn in set_coloumns:
            #Check that all are named and strings.
            if isinstance(clmn, str):
                if '^Unnamed' in clmn:
                    raise RuntimeError("All coloumns must be named.")
                #Check validity of names
                if clmn not in self.valid_coloumns:
                    raise ValueError("The dataset contained a coloumn named \"{}\". Valid coloumn names are: {}".format(clmn, self.valid_coloumns))
            else:
                raise RuntimeError("Coloumns must be named with strings.")
        
        #Check if candlesfile is open     
        if self.candlesfile == None:
            #If not, we try to open or create it.
            if self.datafile_path != None:
                self.open(self.datafile_path, new=True)
            else:
                raise RuntimeError("""Could not save data to file, since the file path specifier 
                in the candles handler was empty. You must set the file path first: candlesHandler.datafile_path = full path to file""")
        
        #Open the correct dataset in the datafile.
        candles_dataset = self.candlesfile[timebase]
        #Find its length
        length = candles_dataset.shape[0]
        saveSet = self._pandasToHDF5(set, timebase)
        print(saveSet)
        saveset_length = len(saveSet)
        if mode == 'append':
            candles_dataset.resize(length + len(saveSet), 0)
            candles_dataset[-saveset_length:,:] = saveSet
        else:
            #We are either in mode skip or overwrite.
            mode_is_replace = mode == 'replace'
            set_startMts = saveSet[0][0]
            file_startIndex = np.searchsorted(candles_dataset[:, 0], set_startMts) #Index of the first coloumn where data is to be inserted.
            print("file_startIndex: {}".format(file_startIndex))
            growSize = saveset_length - (length - file_startIndex)
            growSize_indexer = -growSize
            print("growSize: {}".format(growSize))
            save_indexEnd = candles_dataset.shape[0]
            if growSize > 0:
                #Start by resizing the datafile.
                candles_dataset.resize(length + growSize, 0)
                #save non-overlapping areas.
                candles_dataset[-growSize:, :] = saveSet[-growSize:][:]
            elif growSize==0:
                #Need to create a growsize that is nonetype if it is actually 0.
                #Also create a negative 
                growSize_indexer = None
            else:
                #In this case, things are more complicated. We are saving _inside_ the datafile.
                #We create indexes for where things are to be saved in the datafile.
                growSize_indexer = None
                save_indexEnd = file_startIndex + saveset_length
            #If keepnan = true, save is easy.
            if keepnan:
                #If mode is skip, we're all done.
                #Else we save the overlapping parts.
                if mode_is_replace:
                    candles_dataset[file_startIndex:save_indexEnd,:] = saveSet[:growSize_indexer]
            else:
                #Otherwise, saving overlap parts of the set must be done element by element.
                for k in range(6):
                    for n in range(file_startIndex, save_indexEnd):
                        setVal = saveSet[n-file_startIndex,k]
                        print("setVal: {}".format(setVal))
                        if mode_is_replace:
                            if not np.isnan(setVal):
                                #Mode is overwrite, and set value is not NAN. We overwrite value in file.
                                candles_dataset[n,k] = setVal
                        else:
                            if np.isnan(candles_dataset[n,k]):
                                #Mode is skip, and file value is NAN. We overwrite value in file.
                                candles_dataset[n,k] = setVal
                    
    def normalize(self, dataset):
        #this should not be here!
        """Normalizes a dataset to percent change. The dataset must be a 1D array."""
        start = dataset[0]
        max = np.amax(dataset) - start #Largest positive deviation from start price
        min = np.amin(dataset) - start #Largest negative deviation from start price

        #We set max equal to the greatest negative or positive deviation from the start value
        if max < abs(min):
            max = min
            
        percentChange = (max+start)/start - 1 #This gives the percentage change of the dataset
        scaler = percentChange/(max) #This gives the scaler that the dataset must be multiplied with
        
        dataset -= start #subtract start value to get the dataset to begin at 0
        dataset *= scaler #Scale it
        return dataset
    
    def scaleOutliers(self, timebase, start=None, end=None, startIndex=None, endIndex=None, length=None, statlength=10, sigmalimit=0.5, set=None, editedPointsHigh=[], editedPointsLow=[]):
        """This function will take a dataset in the currently open datafile and locate clear outliers in the 
        HIGH and LOW values (i.e. the candle wicks/shadows). The outliers are scaled down. The new value
        is the previous OPEN or CLOSE value, whichever is largest/smallest (depending on whether the wick is HIGH/LOW) plus (or minus) 
        the mean of the nearest other HIGH and LOW values.
        
        The algorithm will decide whether a wick is too great by comparing it to the future and past 
        wicks. The window length is determined by the variable "statlength", and is twice this value 
        (statlength number of future values and statlength number of past values). The default value is 10. 
        They are weighted by a symmetrical gaussian function so that nearby wick values have a greater impact.
        
        A wick is considered an outlier if it lies outside a specified amount of sigma from the mean of 
        the distribution that the future and past wicks create. The sigma limit is set by the parameter
        "sigmalimit". Default value is 0.5.
        
        Parameters
        ----------
        timebase            : The timebase of the dataset you want. Valid options: '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'
        start, end, startIndex, endIndex, and length : Variables used to control what part of the dataset should be scaled. See the docs for getDataset for explanation of each one.
        statlength=10       : Integer. Amount of points in future and past to use in calculating the average wick length.
        sigmalimit=0.5      : Float. How many standard deviations a wick must be away from the mean to be scaled down.
        set                 : Internal variable for recursion. Do not use.
        editedPointsHigh    : Internal variable for recursion. Do not use.
        editedPointsLow     : Internal variable for recursion. Do not use.
        
        Returns:
        --------
        set                :   A pandas dataframe containing the scaled dataset.
        outlierXhigh       :   X-values of the located positive outliers. For debugging and plotting purposes.
        outlierYhigh       :   Y-values of the located positive outliers (now scaled down).
        outlierXlow        :   X-values of the located negative outliers. For debugging and plotting purposes.
        outlierXhigh       :   Y-values of the located negative outliers (now scaled down).
        """
        firstCaller = False
        if set is None:
            firstCaller = True
            try:
                set = self.getDataset(timebase, 'ALL', start=start, end=end, startIndex=startIndex, endIndex=endIndex, length=length)
            except:
                return None, None, None, None, None
        x = set['MTS'].values
        high = set['HIGH'].values
        low = set['LOW'].values
        open = set['OPEN'].values
        close = set['CLOSE'].values
        
        #Progress bar
        bar = progressbar.ProgressBar(max_value=len(x))
        bar.update(0)
        
        for i in range(2):
            ishigh = i == 0
            if ishigh:
                polarity = 1
                type = 'HIGH'
                series = high
                editedPoints = editedPointsHigh
            else:
                polarity = -1
                type = 'LOW'
                series = low
                editedPoints = editedPointsLow
            
            #We initialize the arrays required for the search. This speeds up the process.
            outlierX = np.zeros(int(len(x)/10), dtype=int)
            outlierY = np.zeros(int(len(x)/10), dtype=float)
            if editedPoints == []:
                editedPoints = np.zeros(int(len(x)/10), dtype=int)
                ne = 0 #Edited points inserted points counter
            else:
                ne = len(editedPoints) #Edited points inserted points counter
                editedPoints = np.concatenate((editedPoints, np.zeros(int(len(x)/10), dtype=int)))
            no = 0 #Outlier points counter
            
            #Start by differentiating signal.
            diff = np.concatenate(([0]*statlength, np.diff(series), [0]*(statlength+1)))
            
            #The code has been written with a HIGH check in mind. The comments and variable names reflect this.
            #We first generate the weights
            weights = gaussian(2*statlength+2, std=1.25*((2*statlength+2)/9))
            posWeights = weights[0:-1]
            posWeights = np.delete(posWeights, statlength)
            negWeights = weights[1:]
            negWeights = np.delete(negWeights, statlength)
            for n in range(len(diff)-statlength*2-1):
                bar.update(n)
                k = n + statlength #Index of current element under scrutiny
                data = np.array(diff[n:k+statlength+2]) #Current element, next element, plus statlength preceding values and statlength future values.
                dataPositive = data[0:-1] #Positive differentiation, and so we get rid of last element
                dataPositive = np.delete(dataPositive, statlength) #Get rid of current element
                positivePositions = polarity*dataPositive>=0
                dataPositive = dataPositive[positivePositions]#Grab only those elements that are positive, and the corresponding weights
                reducedPosWeights = posWeights[positivePositions]
                posMean, posSigma = weightedAvgAndStd(dataPositive, reducedPosWeights) #Mean and sigma of positive differentiation values
                limpos = posMean + polarity*sigmalimit*posSigma
                #limYpos.append(limpos)
                
                if ishigh:
                    outoflimit = diff[k] > limpos
                else:
                    outoflimit = diff[k] < limpos
                if outoflimit:
                    #In this case we check if the next diff value is below the negative limit
                    dataNegative = data[1:] #Negative differentiation, and so we get rid of first element and corresponding weight
                    dataNegative = np.delete(dataNegative, statlength) #Get rid of current element

                    negativePositions = polarity*dataNegative<=0
                    dataNegative = dataNegative[negativePositions]
                    reducedNegWeights = negWeights[negativePositions]
                    negMean, negSigma = weightedAvgAndStd(dataNegative, reducedNegWeights) #Mean and sigma of negative differentiation values
                    limneg = negMean - polarity*sigmalimit*negSigma
                    #limYneg.append(limneg)

                    if ishigh:
                        outoflimit = diff[k+1] < limneg
                    else:
                        outoflimit = diff[k+1] > limneg
                    if outoflimit and (x[n+1] not in editedPoints[:ne]):
                        #In this case we have determined that the current element is an outlier.
                        #We also know that it was not found in the previous recursion.
                        editedPoints[ne] = x[n+1]
                        ne += 1
                        outlierX[no] = x[n+1]
                        outlierY[no] = series[n+1]
                        no += 1
                        openVal = open[n+1]
                        closeVal = close[n+1]
                        
                        #Based on whether we are finding outlier peaks or throughs, and whether 
                        #the open or close is greatest (or smallest), we pick the one to use.
                        if openVal >= closeVal:
                            if ishigh:
                                valToUse = openVal
                            else:
                                valToUse = closeVal
                        if openVal < closeVal:
                            if ishigh:
                                valToUse = openVal
                            else:
                                valToUse = closeVal
                        set.at[n+1, type] = valToUse + posMean
                        #print("Set element {} in column {} to {}.".format(n+1, type, valToUse + posMean))
            #Remove parts of arrays that were never used.
            if ishigh:
                outlierXhigh = outlierX[:no]
                outlierYhigh = outlierY[:no]
                editedPointsHigh = editedPoints[:ne]
                noPos = no
            else:
                outlierXlow = outlierX[:no]
                outlierYlow = outlierY[:no]
                editedPointsLow = editedPoints[:ne]
                noNeg = no
                
        #Recursively check until all outliers have been removed.
        if noPos != 0 or noNeg != 0:
            print('\nFound {} positive outliers and {} negative outliers in this pass.'.format(noPos, noNeg))
            set, outlierXhighRecur, outlierYhighRecur, outlierXlowRecur, outlierYlowRecur = self.scaleOutliers(timebase, set=set, statlength=statlength, sigmalimit=sigmalimit, editedPointsHigh=editedPointsHigh, editedPointsLow=editedPointsLow)
            outlierXhigh = np.concatenate((outlierXhigh,outlierXhighRecur))
            outlierYhigh = np.concatenate((outlierYhigh,outlierYhighRecur))
            outlierXlow = np.concatenate((outlierXlow,outlierXlowRecur))
            outlierYlow = np.concatenate((outlierYlow,outlierYlowRecur))
        
        if firstCaller:
            #Need it inside the if-case to make sure only the top caller of the recursion prints.
            print('\nDataset has been succesfully cleaned for outliers.')
            
        return set, outlierXhigh, outlierYhigh, outlierXlow, outlierYlow
        

        
def weightedAvgAndStd(series, weights=[]):
    """
    Return the weighted average and standard deviation.
    values, weights -- Numpy ndarrays with the same shape.
    Weights can be omitted.
    """
    if sum(weights)==0 and sum(series)==0:
        return(0,0)
    else:
        if sum(weights)==0:
            average = np.average(series)
            variance = np.average((series-average)**2)  # Fast and numerically precise  
        else:
            average = np.average(series, weights=weights)
            variance = np.average((series-average)**2, weights=weights)  # Fast and numerically precise

            
    return (average, math.sqrt(variance))