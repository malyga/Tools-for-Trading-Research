import pandas as pd
import numpy as np
import math
import os.path
import time
from binance.client import Client
from datetime import timedelta, datetime
from dateutil import parser
import tqdm

'''
This script downloads trading data ['timestamp', 'open', 'high', 'low', 'close',
'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av'] from 
Binance using Binance API and generates .csv files filled with the data separately for each of the 
requested tickers. Also, a large portion of the code below was taken 
https://github.com/sammchardy/python-binance.'''


path = "/Users/"# set your current workind directory
os.chdir(path) # change a workind director

# Create a connection to (your) Binance account
binance_api_key = '[]' 
binance_api_secret = '[]'
client = Client(binance_api_key, binance_api_secret)
binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
batch_size = 750
binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)

def minutes_of_new_data(symbol, kline_size, data, source):
    
    if len(data) > 0:
        old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance":
        old = datetime.strptime('1 Jan 2017', '%d %b %Y')
    if source == "binance":
        new = pd.to_datetime(binance_client.get_klines(symbol=symbol,
                                                       interval=kline_size)[-1][0], unit='ms')
    return old, new

def downloadAllBinance(symbol, kline_size, save=False):

    filename = '%s-%s-data.csv' % (symbol, kline_size)
    if os.path.isfile(filename):
        data_df = pd.read_csv(filename)
    else:
        data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(
        symbol, kline_size, data_df, source="binance")
    delta_min = (newest_point - oldest_point).total_seconds() / 60
    available_data = math.ceil(delta_min / binsizes[kline_size])
    if oldest_point == datetime.strptime('1 Jan 2017', '%d %b %Y'):
        print('Downloading all available %s data for %s.' %
              (kline_size, symbol))
    else:
        print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (
            delta_min, symbol, available_data, kline_size))
    klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime(
        "%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))
    data = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close',
                                         'volume', 'close_time', 'quote_av', 'trades',
                                         'tb_base_av', 'tb_quote_av', 'ignore'])
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        data_df = data_df.append(temp_df)
    else:
        data_df = data
    data_df.set_index('timestamp', inplace=True)
    if save:
        data_df.to_csv(filename)
        
    return data_df

timeElapsed = [] # empty list to store a period of one interation
# to compute the expectate time of downloading all data      
symbols = [] # enter pairs for which you would like to download the data
numOfTickers = len(tickers)
iterations = numOfTickers
freq = "1m" # data frequency

for symbol in tickers:

    startTimer = time.time()
    downloadAllBinance(symbol, freq, save=True)
    endTimer = time.time()
    timeElapsedPoint = endTimer - startTimer
    iterations += -1
    timeElapsed.append(timeElapsedPoint)
    expetedTimeLeftMinutes = round(np.mean(timeElapsed) * iterations / 60, 1)
    percentageUploaded = round((1 - iterations / numOfTickers) * 100, 1)
    print('All available data for ' + symbol + ' downloaded.')
    print(str(percentageUploaded) + "% " + "downloaded/updated. The expected time to"
          + " complete is " + str(expetedTimeLeftMinutes)
          + " minutes.")
    print()
