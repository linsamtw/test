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
# based class 
class LoadDate:
    def __init__(self,database='',data_name=''):
        self.data_name = data_name        
        self.database = database   
        tem = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = self.database,
                sql_text = 'SELECT distinct `stock_id` FROM `' + self.data_name + '`')

        self.stock_cid = [ te[0] for te in tem ]        

        
    def check_stock(self):
        tem = list( set([self.stock]) & set(self.stock_cid) )
        if len(tem) == 0:
            print("the stock isn't exist")
            return 0
        else:
            return 1

    def get_col_name(self):
       
        tem_col_name = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = self.database,
                sql_text = 'SHOW columns FROM '+ self.data_name )
    
        col_name = []
        for i in range(len(tem_col_name)):
            col_name.append( tem_col_name[i][0] )
        col_name.remove('id')    
        self.col_name = col_name
    
    def execute2sql(self,text,col):
        tem = execute_sql2(
            host = host,
            user = user,
            password = password,
            database = self.database,
            sql_text = text)
        
        if col=='Date':
            tem = [np.datetime64(x[0]) for x in tem]
            tem = pd.DataFrame(tem)
            self.data[col] = tem.loc[:,0]
        else:
            tem = np.concatenate(tem, axis=0)
            tem = pd.DataFrame(tem)
            self.data[col] = tem.T.iloc[0]
    
    def get_data(self,all_data = ''):
        
        self.data = pd.DataFrame()
        for j in range(len(self.col_name)):
            print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + self.data_name
            
            if all_data == 'T': 
                123
            else:
                text = text + " WHERE `stock_id` LIKE '"+str(self.stock)+"'"
                
            self.execute2sql(text,col)

        return self.data
            
    def load(self,stock):
        
        self.stock = str( stock )
        if self.check_stock() ==0:
            return ''
        
        self.get_col_name()
        data = self.get_data()
                
        return data
    
#--------------------------------------------------------------- 
''' test StockInfo
SI = StockInfo()
data = SI.load()

'''
class StockInfo(LoadDate):
    def __init__(self):
        super(StockInfo, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'FinancialStatements')
        self.data_name = 'StockInfo'
        
    def load(self):                        
        self.get_col_name()
        #---------------------------------------------------------------   
        self.data = pd.DataFrame()
        self.data = self.get_data(all_data='T')
                
        return self.data
    
#-------------------------------------------------------------
''' test StockPrice

SP = StockPrice()  
data = SP.load('2330')

'''
class StockPrice(LoadDate):
    #---------------------------------------------------------------    
    def __init__(self):    
        self.database = 'StockPrice'
        tem = execute_sql2(host = host,user = user,password = password,
                           database = self.database,
                           sql_text = 'SHOW TABLES')
        #---------------------------------------------------------------                         
        stock_id,stock_cid = [],[]
        for d in tem:
            d = d[0][1:].replace('_','.')
            stock_id.append( d )
            stock_cid.append( d.split('.')[0] )
    
        self.stock_id = pd.DataFrame({
                'stock_id' : stock_id,
                'stock_cid' : stock_cid})
        #---------------------------------------------------------------

    def change_stock_name(self):
        try:
            tem = self.stock_id[ self.stock_id['stock_cid'] == str(self.stock)].stock_id
            data_name = tem[ tem.index[0] ]
            data_name = '_' + data_name.replace('.','_')
            return data_name
        except:
            print("the stock isn't exist")
            return 0
    
    def load(self,stock):
        
        self.stock = str( stock )
        self.data_name = self.change_stock_name()
        if self.data_name ==0 :
            return ''
        
        self.get_col_name()
    
        self.data = pd.DataFrame()
        for j in range(len(self.col_name)):
            print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + self.data_name
            self.execute2sql(text,col)
                
        return self.data    
#--------------------------------------------------------------- 
''' test FinancialStatements

FS = FinancialStatements()  
data = FS.load('2330')# 讀取 2330 歷史財報
data = FS.load_all()# 讀取 '所有股票' 歷史財報

'''    
        
class FinancialStatements(LoadDate):
    def __init__(self):
        super(FinancialStatements, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'FinancialStatements')
        
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
        
#--------------------------------------------------------------- 
''' test StockDividend
SD = StockDividend()
data = SD.load('2330')
data.iloc[8]

'''
class StockDividend(LoadDate):
    def __init__(self):
        super(StockDividend, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'StockDividend')
        #self.data_name = 'StockInfo'    
        
        
#--------------------------------------------------------------- 
''' test InstitutionalInvestors
II = InstitutionalInvestors()
data = II.load()

'''
class InstitutionalInvestors(LoadDate):
    def __init__(self):
        super(InstitutionalInvestors, self).__init__(
                database = 'Financial_DataSet',
                data_name = 'InstitutionalInvestors')
    def load(self):                        
        self.get_col_name()
        #---------------------------------------------------------------   
        self.data = pd.DataFrame()
        self.data = self.get_data(all_data='T')
                
        return self.data
        
        
        
        
        

        
 


