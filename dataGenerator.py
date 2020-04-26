'''
    Charlie Raymond, CPSC408, 2020-04-27
    
    The intent of this program is to generate fake order information to seed my database.
    I found that it was hard to use a program like Faker because I have more finite
    choices. SO instead I predefined my categories and randomly draw each time.
    I export this straight to my database, not a csv. I have unstructured tick data that 
    I import from csv's and will be in a different app.
'''

from crud import CRUD
import random
from random import randrange
import pandas as pd
import numpy as np
import sys

columns = ['trader_id', 'algorithm_id', 'ticker', 'order_type', 'price', 'size','date','time']

#tickers = ['AAPL']
tickers = ['AAPL','MSFT','ADBE','BIIB','MMM','COST','CVS']
size = [50,100,150,200,-50,-100,-150,-200]
order_type = ['MKT','LMT']
date_range = pd.date_range(start="2019-10-8",end="2020-04-24").strftime("%Y-%m-%d")
time_range = pd.date_range(start="9:30:00",end="16:00:00", freq="10s").strftime("%H:%M:%S")
#print(time_range)

def generateOrders(total):
    data = []
    for i in range(total):
        order = []
        order.append(1000) #These are hardcoded values for trader id and algorithm id.
        order.append(1000) #I may have more traders/algorithms but not for a while.
        order.append(random.choice(tickers))
        order.append(random.choice(order_type))
        order.append(randrange(100,400)) # Price targets will be off
        order.append(random.choice(size))
        order.append(random.choice(date_range))
        order.append(random.choice(time_range))
    
        data.append(order)
    return data 

if __name__=="__main__":
    crud = CRUD()
    n = len(sys.argv)

    if n >= 3:
        file_name = sys.argv[1]
        total = sys.argv[2]
    else:
        print("PASS IN ARGUMENTS!")
        exit(0) 

    data = generateOrders(int(total))
    df = pd.DataFrame.from_records(data, columns=columns) # CONVERT TO DF
    #df.to_csv(file_name, index=False) # I'm leaving this in to show I can do it
    crud.insertTrade(df) #I export straight to my db
    
