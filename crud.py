'''
    THIS IS GOING TO BE MY CRUD CLASS
    WILL ENABLE FAST REUSE OF CODE FOR DB QUERYING AND INSTERTING
'''


import mysql.connector
from mysql.connector import IntegrityError
import sqlalchemy
from sqlalchemy import exc
import pandas as pd

### example (Does not work need to put own credentials)
host = "172.23.0.2"
port = "3306"
user = "myuser"
password = "mypassword"
database = "QRA"

engine =  sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.
                                           format(user, password,
                                                  host, port, database))




class CRUD():
    def __init__(self):
        self.conn = engine.connect()


    '''
        FUNCTION THAT WILL QUERY A TABLE BY TICKER AND DATE
    '''
    def selectTable(self, table, where):
        df = pd.read_sql_query("SELECT * FROM "+table+" WHERE "
                                "ticker = %s AND date = %s AND "
                                "time >= '09:30:00' AND time <= '16:00:00'"
                                " order by time;", 
                                con=self.conn, params=where)
        return df
   
    '''
        Inserts a df to the db
        NEED: we should double check to make sure, or even implement a try except block
    ''' 
    def insertDf(self, df, tablename):
        try:
            df.to_sql(con=self.conn, name=tablename, index=False, if_exists="append")
        except exc.IntegrityError:
            print("DUPLICATE ENTRY")    
            pass
    
    def selectTickers(self, date):
        ticker_list = pd.read_sql_query("SELECT DISTINCT TICKER "
                                "from tick_data where date = %s ;" , 
                                con=self.conn,params=[date]).values.T.tolist()[0]        
        return ticker_list

    def insertTrade(self, df):
        try:
            df.to_sql(con=self.conn, name="Trades", index=False, if_exists="append")
        except exe.IntegrityError:
            print("DUP Entry")
            pass



