#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 14 12:00:11 2018

@author: linsam
"""

from pandas_datareader import data as pdr
import os,sys
import pandas as pd
import datetime
import fix_yahoo_finance as yf
import numpy as np
import pymysql

os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')
import stock_sql 
import TakeStockID
import FinancialKey

host = FinancialKey.host
user = FinancialKey.user
password = FinancialKey.password
#-------------------------------------------------------------------    

#-------------------------------------------------------------------   
'''
# GET TAIWAN STOCK INFO, TO CRAWLER ALL STOCK PRICE
stock_info = stock_sql.get_stock_info_by_sql()
# get history stock price
stock_log = crawler_history_stock_price(stock_info)
'''
def crawler_history_stock_price(stock_info):
    def upload_stock_price2sql(data,dataset_name):
        
        dataset_name = dataset_name.replace('.','_')
    
        conn = ( pymysql.connect(host = host,# SQL IP
                         port = 3306,
                         user = user,
                         password = password,
                         database='StockPrice',  
                         charset="utf8") )   
    
        for j in range(len(data)):
    
            ( conn.cursor().execute( 'insert into ' + dataset_name + 
             '( Date ,stock_id  ,Open ,High ,Low ,Close ,'+
             ' Adj_Close , Volume )'+' values(%s,%s,%s,%s,%s,%s,%s,%s)',
             ( data['Date'][j],data['stock_id'][j],
              float( data['Open'][j] ),
              float( data['High'][j] ),
              float( data['Low'][j] ),
              float( data['Close'][j] ),
              float( data['Adj Close'][j] ),
              int( data['Volume'][j] )
              ) ) )
      
        conn.commit()
        conn.close() 
    #----------------------------------------------------------------
    yf.pdr_override() # <== that's all it takes :-)   \
    stock_id_log = []
    len_data_log = []
      
    now = datetime.datetime.now()
    end = str( now.year ) + '-' + str( now.month )+ '-' +str( now.day+1 )

    for i in range(len(stock_info)):
        print(str(i)+'/'+str(len(stock_info)))
        # stock_id = '1593'
        stock_id = str( stock_info.stock_id[i] ) + '.TW'
        stock_id2 = str( stock_info.stock_id[i] ) + '.TWO'
        #print(stock_info.stock_id[i])
        bo = 1
        while( bo ):
            data = pdr.get_data_yahoo(stock_id, start='1900-1-10', end=end)
            data['stock_id'] = stock_id
            dataset_name = '_'+stock_id
            if len(data) == 0:
                data = pdr.get_data_yahoo(stock_id2, start='1900-1-10', end=end)
                data['stock_id'] = stock_id2
                dataset_name = '_'+stock_id2
            if len(data) != 0:
                bo=0
            #print(stock_id2)
            #print('len(data) = ' + str( len(data) ) )

        stock_id_log.append(stock_id)
        len_data_log.append(len(data))
        
        data['Date'] = data.index
        data.index = range(len(data))
        #data['stock_name'] = stock_name
        data.Date = np.array( [ str(data.Date[i]).split(' ')[0] for i in range(len(data)) ] )
        upload_stock_price2sql(data,dataset_name)
        
    stock_log = pd.DataFrame({'stock_id_log':stock_id_log,
                              'len_data_log':len_data_log})
    return stock_log



def main():
    

    # crawler stock info, contain stock id, stock name, stock class
    stockfun = TakeStockID.main()
    stockfun.run()
    stock_info = stockfun.stock_info
    
    # creat StockInfo SQL table
    dataset_name = 'StockInfo'
    sql_string = ('create table ' + dataset_name + 
                  '( stock_id text(100),stock_name text(100), stock_class text(100) )' )
    
    stock_sql.creat_sql_file(sql_string,dataset_name)  
    
    # upload stock info
    stock_sql.upload_stock_info2sql(stock_info)

    # GET TAIWAN STOCK INFO, TO CRAWLER ALL STOCK PRICE
    stock_info = stock_sql.get_stock_info_by_sql()
    # get history stock price
    stock_log = crawler_history_stock_price(stock_info)



    
    
    
    
    