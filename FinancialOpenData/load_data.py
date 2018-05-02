# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 16:48:00 2018

@author: Owner
"""
import pandas as pd
import numpy as np
import pymysql

host = '114.34.138.146'
user = 'guest'
password = '123'

#---------------------------------------------------------

def execute_sql2(host,user,password,database,sql_text):
    
    conn = ( pymysql.connect(host = host,# SQL IP
                     port = 3306,
                     user = user,# 帳號
                     password = password,# 密碼
                     database = database,  # 資料庫名稱
                     charset="utf8") )   #  編碼
                             
    cursor = conn.cursor()    
    # sql_text = "SELECT * FROM `_0050_TW` ORDER BY `Date` DESC LIMIT 1"
    try:   
        cursor.execute(sql_text)
        data = cursor.fetchall()
        conn.close()
        return data
    except:
        conn.close()
        return ''
#---------------------------------------------------------
''' test
FS = FinancialStatements()
data = FS.load('2330')# 讀取 2330 歷史財報
data = FS.load_all()# 讀取 '所有股票' 歷史財報

'''    
class FinancialStatements:
    def __init__(self):
        tem = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = 'Financial_DataSet',
                sql_text = 'SELECT distinct `stock_id` FROM `FinancialStatements`')

        self.stock_cid = [ te[0] for te in tem ]        
        #self.stock = stock
        
    def check_stock(self):
        tem = list( set([self.stock]) & set(self.stock_cid) )
        if len(tem) == 0:
            print("the stock isn't exist")
            return 0
        else:
            return 1
        
    def get_col_name(self,database,data_name):
       
        tem_col_name = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = database,
                sql_text = 'SHOW columns FROM '+ data_name )
    
        col_name = []
        for i in range(len(tem_col_name)):
            col_name.append( tem_col_name[i][0] )
        col_name.remove('id')    
        self.col_name = col_name
        
    def load(self,stock):
        
        self.stock = stock
        if self.check_stock() ==0:
            return ''
        
        self.get_col_name('Financial_DataSet','FinancialStatements')

        data = pd.DataFrame()
        for j in range(len(self.col_name)):
            print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + 'FinancialStatements'
            text = text + " WHERE `stock_id` LIKE '"+str(self.stock)+"'"

            tem = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = 'Financial_DataSet',
                sql_text = text)
            
            if col=='Date':
                tem = [np.datetime64(x[0]) for x in tem]
                tem = pd.DataFrame(tem)
                data[col] = tem.loc[:,0]
            else:
                tem = np.concatenate(tem, axis=0)
                tem = pd.DataFrame(tem)
                data[col] = tem.T.iloc[0]
                
        return data
    
    def load_all(self):
        
        self.get_col_name('Financial_DataSet','FinancialStatements')

        data = pd.DataFrame()
        for j in range(len(self.col_name)):
            print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + 'FinancialStatements'

            tem = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = 'Financial_DataSet',
                sql_text = text)
            
            if col=='Date':
                tem = [np.datetime64(x[0]) for x in tem]
                tem = pd.DataFrame(tem)
                data[col] = tem.loc[:,0]
            else:
                tem = np.concatenate(tem, axis=0)
                tem = pd.DataFrame(tem)
                data[col] = tem.T.iloc[0]
                
        return data
        
#-------------------------------------------------------------
''' test
SP = StockPrice()
data = SP.load('2330')

'''
class StockPrice(FinancialStatements):
    #---------------------------------------------------------------    
    def __init__(self):                        
        tem = execute_sql2(host = host,user = user,password = password,
                           database = 'StockPrice',sql_text = 'SHOW TABLES')
        #---------------------------------------------------------------                         
        stock_id = []
        stock_cid = []
        for d in tem:
            d = d[0][1:]
            d = d.replace('_','.')
            stock_id.append( d )
            stock_cid.append( d.split('.')[0] )
    
        self.stock_id = pd.DataFrame({
                'stock_id' : stock_id,
                'stock_cid' : stock_cid})
        #---------------------------------------------------------------
    def check_stock(self):
        try:
            tem = self.stock_id[ self.stock_id['stock_cid'] == str(self.stock)].stock_id
            data_name = tem[ tem.index[0] ]
            data_name = '_' + data_name.replace('.','_')
            return data_name
        except:
            print("the stock isn't exist")
            return 0
    
    def load(self,stock):
        
        self.stock = stock
        data_name = self.check_stock()
        if data_name ==0 :
            return ''
        
        self.get_col_name('StockPrice',data_name)
    
        data = pd.DataFrame()
        for j in range(len(self.col_name)):
            print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + data_name
            
            tem = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = 'StockPrice',
                sql_text = text)
            
            if col=='Date':
                tem = [np.datetime64(x[0]) for x in tem]
                tem = pd.DataFrame(tem)
                data[col] = tem.loc[:,0]
            else:
                tem = np.concatenate(tem, axis=0)
                tem = pd.DataFrame(tem)
                data[col] = tem
                
        return data
    

#--------------------------------------------------------------- 
''' test
SI = StockInfo()
data = SI.load()

'''
class StockInfo(FinancialStatements):
    #---------------------------------------------------------------    
    def load(self):                        
        
        self.get_col_name('Financial_DataSet','StockInfo')
        #---------------------------------------------------------------   
        data = pd.DataFrame()
        for j in range(len(self.col_name)):
            print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + '`StockInfo`'
            
            tem = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = 'Financial_DataSet',
                sql_text = text)
            
            if col=='Date':
                tem = [np.datetime64(x[0]) for x in tem]
                tem = pd.DataFrame(tem)
                data[col] = tem.loc[:,0]
            else:
                tem = np.concatenate(tem, axis=0)
                tem = pd.DataFrame(tem)
                data[col] = tem.T.iloc[0]
        return data
    


