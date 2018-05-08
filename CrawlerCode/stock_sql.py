#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 14 16:01:28 2018

@author: linsam
"""
import pymysql
import os,sys
import pandas as pd
import numpy as np
os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')
import FinancialKey

host = FinancialKey.host
user = FinancialKey.user
password = FinancialKey.password
#----------------------------------------

def creat_sql_file(sql_string,dataset_name):
    conn = ( pymysql.connect(host = host,# SQL IP
                             port = 3306,
                             user = user,
                             password = password,
                             database='Financial_DataSet', 
                             charset="utf8") )       
    c=conn.cursor()
    c.execute( sql_string )

    try:
        c.execute('ALTER TABLE `'+dataset_name+'` ADD id BIGINT(64) NOT NULL AUTO_INCREMENT PRIMARY KEY;')
        c.close() 
        conn.close()
    except:
        c.close() 
        conn.close()
    
#------------------------------------------------------------
def get_stock_info_by_sql():
    conn = ( pymysql.connect(host = host,# SQL IP
                         port = 3306,
                         user = user,
                         password = password,
                         database='Financial_DataSet', 
                         charset="utf8") )  
                             
    cursor = conn.cursor()                         
    cursor.execute('select * from ' + 'StockInfo')
    # 抓所有的 data
    sql_data = cursor.fetchall()
    # close connect
    conn.close()
    
    stock_id = []
    stock_name = []
    stock_class = []
    for d in sql_data:
        stock_id.append( d[0] )
        stock_name.append(  d[1] )
        stock_class.append( d[2] )

    stock_info = {
            'stock_id' : stock_id,
            'stock_name' : stock_name ,
            'stock_class' : stock_class
            }
        
    stock_info = pd.DataFrame(stock_info)
    # 轉換成功
    return stock_info
#------------------------------------------------------------
def take_stock_class_by_sql():
    conn = ( pymysql.connect(host = host,# SQL IP
                         port = 3306,
                         user = user,
                         password = password,
                         database='Financial_DataSet', 
                         charset="utf8") )  
                             
    cursor = conn.cursor()                   
    cursor.execute('SELECT DISTINCT `stock_id` FROM `StockPrice`')
    # 抓所有的 data
    sql_data = cursor.fetchall()
    # close connect
    conn.close()
    
    stock_id = []
    for d in sql_data:
        stock_id.append( d[0] )

    stock_id_class = {
            'stock_id' : stock_id
            }
        
    stock_id_class = pd.DataFrame(stock_id_class)

    # 轉換成功
    return stock_id_class

#------------------------------------------------------------
def upload_stock_info2sql(stock_info):

    conn = ( pymysql.connect(host = host,# SQL IP
                             port = 3306,
                             user = user,
                             password = password,
                             database='Financial_DataSet',  
                             charset="utf8") )       
    
    # upload stock info to sql
    for i in range(len(stock_info)):
        print(i)
        ( conn.cursor().execute( 'insert into ' + 'StockInfo' + 
                     '(stock_id,stock_name,stock_class)'+' values(%s,%s,%s)',
                      (stock_info['stock_id'][i],
                       stock_info['stock_name'][i],
                       stock_info['stock_class'][i]) ) )
    conn.commit()
    conn.close()    


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

def execute_sql(sql_text):
    
    conn = ( pymysql.connect(host = host,# SQL IP
                     port = 3306,
                     user = user,
                     password = password,
                     database='StockPrice',  
                     charset="utf8") )   
                             
    cursor = conn.cursor()    
    # sql_text = "SELECT * FROM `_0050_TW` ORDER BY `Date` DESC LIMIT 1"
    cursor.execute(sql_text)
    data = cursor.fetchall()
    conn.close()

    return data

def execute_sql2(host,user,password,database,sql_text):
    
    conn = ( pymysql.connect(host = host,# SQL IP
                     port = 3306,
                     user = user,
                     password = password,
                     database = database,  
                     charset="utf8") )  
                             
    cursor = conn.cursor()    
    # sql_text = "SELECT * FROM `_0050_TW` ORDER BY `Date` DESC LIMIT 1"
    cursor.execute(sql_text)
    data = cursor.fetchall()
    conn.close()

    return data

'''def change_stock_id(stock,stock_id = take_stock_id_by_sql()):
    try:
        tem = stock_id[ stock_id['stock_cid'] == str(stock)].stock_id
        data_name = tem[ tem.index[0] ]
        data_name = '_' + data_name.replace('.','_')
        return data_name
    except:
        print("the stock isn't exist")
        return '' '''
    
    