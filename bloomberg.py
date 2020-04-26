"""
    NEW BLOOMBERG TO DATA IMPORTER

    CPSC408: This program cleans data that I collected from
    the Janes financial center from Bloomberg. I will include a
    folder with a few sheets of data just so you can recreate the test.
    Its a very extensive clean, so I wouldn't worry on how this works,
    more so that it works. This is also multi-process to increase the speed
    of completion. That is controled by the worker_number variable. 
    Default it will be set to 1

    *This does need to be connected to a database to run*

"""

from crud import CRUD
import pandas as pd 
import numpy as np
from datetime import datetime, timedelta
import os

from multiprocessing import Process

worker_number = 1

unproc_path = "/hdd/data/unprocessed/"
proc_path = "/hdd/data/processed/"

ticker_list = os.listdir(unproc_path)
sub_ticker_list = [ticker_list[x:x+(int(len(ticker_list)/worker_number))] for x in range(0, len(ticker_list), int(len(ticker_list)/worker_number))]

'''
   Adds rows for up sampling
'''
def up_row(df, count):
    for x in range(count):
        new_row = pd.DataFrame({"ticker":df.iloc[0]['ticker'],'price':np.NaN,'volume':np.NaN},index=[df.index[0]-timedelta(seconds=10)])
        df = pd.concat([new_row,df], sort=True)
    df.index.name = 'timestamp'
    return df
 
 
'''
    Upsamples df time values
'''
def up_sample(df):
    #compare_time = datetime.strptime(str(df.index.min().date())+ " 09:30:00", "%Y-%m-%d %H:%M:%S")
    compare_time = datetime.strptime(str(df.index[0].date())+ " 09:30:00", "%Y-%m-%d %H:%M:%S")
    #dif = df.index[0] - compare_time
    dif = df.index.min() - compare_time
    seconds = int(dif.total_seconds()/10)
    if seconds != 0:
        df = up_row(df, seconds)
    return df

'''
    Adds rows for down sampling
'''
def down_row(df, count):
    for x in range(count):
       new_row = pd.DataFrame({"ticker":df.iloc[0]['ticker'],'price':np.NaN,'volume':np.NaN},index=[df.index[len(df)-1]+timedelta(seconds=10)])
       #new_row = pd.DataFrame({"ticker":df.iloc[0]['ticker'],'price':np.NaN,'volume':np.NaN},index=[df.index.max()]+timedelta(seconds=10)])
       df = pd.concat([df, new_row], sort=True)

    df.index.name = 'timestamp'
    return df
 
 
'''
    Upsamples the lower half of the 
'''
def down_sample(df):
   compare_time = datetime.strptime(str(df.index[0].date())+ " 16:00:00", "%Y-%m-%d %H:%M:%S")
    #compare_time = datetime.strptime(str(df.index.max().date())+ " 16:00:00", "%Y-%m-%d %H:%M:%S")
 
   dif = compare_time - df.index.max()
   seconds = int(dif.total_seconds()/10)
   if seconds != 0:
       df = down_row(df, seconds)
   return df

def boundry_check(df):
    df.loc[(df.price.shift(-1) > df.price*1.01) , 'price'] = df.price.shift(-1)
    df.loc[(df.price.shift(-1) < df.price*.99) , 'price'] = df.price.shift(-1)

    df.loc[(df.ask.shift(-1) > df.ask*1.01) , 'ask'] = df.ask.shift(-1)
    df.loc[(df.ask.shift(-1) < df.ask*.99) , 'ask'] = df.ask.shift(-1)

    df.loc[(df.bid.shift(-1) > df.bid*1.01) , 'bid'] = df.bid.shift(-1)
    df.loc[(df.bid.shift(-1) < df.bid*.99) , 'bid'] = df.bid.shift(-1)
    return df 

### THIS IS THE CORRECT BOUNDRY CHECK
def boundry_check3(df, cutoff):
    for x in range(1, len(df)):
        if df.price.iloc[x] > df.price.iloc[x-1]*(1+cutoff):
              df.price.iloc[x] = df.price.iloc[x-1]
        if df.price.iloc[x] < df.price.iloc[x-1]*(1-cutoff):
              df.price.iloc[x] = df.price.iloc[x-1]
    return df

class bloomberg_clean():
    def __init__(self, ticker_list):
#        print(ticker_list)
        self.ticker_list = ticker_list
        self.crud = CRUD()
        self.start_cleaning()

    def start_cleaning(self):
        for ticker in self.ticker_list:
            file_list = os.listdir(unproc_path+ticker)
            for file_ in file_list:
                df = self.getDay(ticker, file_)

                #print(df)
                #exit(0)
    
                price, ask, bid = self.seperateType(df)

                if price.empty:
                    print("EMPTY CSV, " + ticker + ", " + file_)
                    continue
                #print(price) 
                #print(ask) 
                #print(bid) 

                full_df = self.resampleANDmerge(price,ask,bid)
                #print(full_df)
                self.crud.insertDf(full_df, "market_tick")

                ohlc_df = self.OHLC(full_df)
                #print(ohlc_df)
                self.crud.insertDf(ohlc_df, "ohlc")

                #exit(0)
#            exit(0)

    ### ADD MORE CLEANING HERE! --> timeStamp???
    def getDay(self, ticker, file_):
        df = pd.read_csv(unproc_path+ticker+"/"+file_, index_col='times').drop(['Unnamed: 0'], axis=1).drop(['condcode'],axis=1)
        #if df.empty:
            #print("DAY " + file_[-4:]+"was empty")

        df = df.rename({'value':'price'}, axis=1)
        df['ticker'] = ticker
        df['type'] = df['type'].replace({'TRADE':'last'})
        df['type'] = df['type'].replace({'ASK':'ask'})
        df['type'] = df['type'].replace({'BID':'bid'})


        ### RIGHT HERE ADD TIME CHECK. IF TIME IS lESS THEN 9:30, ADD ONE HOUR
        df.index = pd.to_datetime(df.index)
    

        if df.empty:
            print("DAY " + file_[-4:]+"was empty")
        else:
            compare_time = datetime.strptime(str(df.index[0].date())+ " 09:30:00", "%Y-%m-%d %H:%M:%S")
            print("DF TIME: " + str(df.index[0]))
            if df.index[0] < compare_time:
                print("ITS LESS!")
                df.index = df.index+timedelta(hours=1)

        df.index.name = 'timestamp'

        return df
        
    def seperateType(self, df):

        ###Cleaning for price df
        price_df = df[df['type'] == 'last']
        del price_df['type']
        price_df = price_df.rename({'size':'volume'}, axis=1)
        price_df['volume'] = price_df['volume'].cumsum()
        
        #price_df = boundry_check3(price_df, 0.04)

        ###Cleaning for ask df
        ask_df = df[df['type'] == 'ask']
        del ask_df['type']
        del ask_df['ticker']
        ask_df = ask_df.rename({'size':'ask_size'}, axis=1)
        ask_df = ask_df.rename({'price':'ask'}, axis=1)
        ask_df['ask_size'] = ask_df['ask_size'].cumsum()

        ###Cleaning for bid df
        bid_df = df[df['type'] == 'bid']
        del bid_df['type']
        del bid_df['ticker']
        bid_df = bid_df.rename({'size':'bid_size'}, axis=1)
        bid_df = bid_df.rename({'price':'bid'}, axis=1)
        bid_df['bid_size'] = bid_df['bid_size'].cumsum()


        return price_df, ask_df, bid_df

    def resampleANDmerge(self, price_df, ask_df, bid_df):
        price_last = price_df.resample('10s', label='right', closed='right').last().fillna(method='ffill')
        price_last = up_sample(price_last)
        price_last = down_sample(price_last)

        #print(price_last)
        ask_last = ask_df.resample('10s', label='right', closed='right').last().fillna(method='ffill')
        bid_last = bid_df.resample('10s', label='right', closed='right').last().fillna(method='ffill')

        full_df = pd.merge(pd.merge(price_last, ask_last, on='timestamp', how='outer').fillna(method="ffill"),bid_last, on='timestamp', how='outer').fillna(method="ffill").fillna(method="backfill")
        
        full_df['date']=[d.date().strftime('%Y-%m-%d') for d in full_df.index]
        full_df['time']=[d.time() for d in full_df.index]

        #print(full_df)
        return full_df

    def OHLC(self, full_df):
        one_min_ohlc = full_df.resample('1Min', label="right", closed='right', axis=0).agg({'price':'ohlc', 'ticker':'last', 'volume':'last'}).fillna(method="ffill")
        one_min_ohlc.columns = one_min_ohlc.columns.droplevel(0)
        one_min_ohlc.index = pd.to_datetime(one_min_ohlc.index)
        one_min_ohlc.index.name = 'timestamp'

        #one_min_ohlc['date']=[d.date() for d in one_min_ohlc.index]
        one_min_ohlc['date']=[d.date().strftime('%Y-%m-%d') for d in one_min_ohlc.index]
        one_min_ohlc['time']=[d.time() for d in one_min_ohlc.index]

        #print(one_min_ohlc)
        return one_min_ohlc 

    def removeFile(self, ticker, file_):

        if os.path.isdir(proc_path + ticker):
            shutil.move(unproc_path + ticker + "/" + file_, proc_path + ticker)
        else:
            os.makedirs(proc_path + ticker)
            shutil.move(unproc_path + ticker + "/" + file_, proc_path + ticker)

    
if __name__ == "__main__":
    workers = []

    for i in range(worker_number):
        workers.append(Process(target=bloomberg_clean, args=(sub_ticker_list[i],)))
        workers[i].start()

    for i in range(worker_number):
        workers[i].join()


    #print("HI")
