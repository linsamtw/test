#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 19:55:24 2018

@author: linsam
"""
'''
0 REV：營業收入 (Gross Revenue)
1 COST：營業成本 (Cost of Goods Sold or Manufacturing)
2 PRO：營業毛利 (Gross Profit)
3 GM : 毛利率 (Gross margin)
4 OE：營業費用 (Operating Expenses)
5 OI：營業淨利(損) (Operating Income)
6 OIM : 營業利益率 (Operating Profit Margin)
*** NNOI：營業外收支淨額 (Net Non-operating Income)
7 BTAX：稅前純益 (Income before Tax)
  BTAXM 稅前純益率 (Income before Tax Margin)
8 TAX：所得稅費用 (Income Tax Expense)
10 NI：本期稅後淨利 (Net Income)
11 NPM : 純益率 ( Net Profit Margin)
12 EPS：每股盈餘 (Earnings Per Share)
單位: 仟元
'''

import requests
import os, sys
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import pymysql
import copy
os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')
import stock_sql
import FinancialKey

# url = 'https://stock.wearn.com/Income_detial.asp?kind=2317&y=10604'

# self = Crawler2SQL(host,user,password,self.stock_financial_statements)
class Crawler2SQL:

    def __init__(self,host,user,password,stock_financial_statements):
        self.host = host
        self.user = user
        self.password = password
        self.stock_financial_statements = stock_financial_statements

    def creat_sql_file(self,sql_string,dataset_name,database):
        
        conn = ( pymysql.connect(host = self.host,# SQL IP
                                 port = 3306,
                                 user = self.user,
                                 password = self.password,
                                 database = database,  
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
    
    def create_table(self):
        dataset_name = 'FinancialStatements'
        sql_string = ('create table ' + dataset_name + 
                      '( BTAXM FLOAT(16), COST FLOAT(16), EPS FLOAT(16),'+
                      ' GM FLOAT(16), NI FLOAT(16), BTAX FLOAT(16), NIM FLOAT(16),'+
                      ' OE FLOAT(16), OI FLOAT(16), OIM FLOAT(16),'+ 
                      ' PRO FLOAT(16), REV FLOAT(16), TAX FLOAT(16),'+
                      ' stock_id text(100), year INT(8), quar INT(8),'+
                      '  url text(100) )' )
    
        self.creat_sql_file(sql_string,dataset_name,'Financial_DataSet')  

    def upload2sql(self,database,dataset_name ):
        # dataset_name = 'FinancialStatements'
        # database = 'Financial_DataSet'
        conn = ( pymysql.connect(host = self.host,# SQL IP
                         port = 3306,
                         user = self.user,# 帳號
                         password = self.password,# 密碼
                         database = database,  # 資料庫名稱
                         charset="utf8") )   #  編碼

        upload_string = ('insert into ' + dataset_name + 
         '( BTAXM, COST, EPS, GM, NI, BTAX, NIM, OE, OI, OIM,' +
         ' PRO, REV, TAX, stock_id, year, quar,url )'+
         ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
         
        for i in range(len(self.stock_financial_statements)):
            
            print(str(i)+'/'+str(len(self.stock_financial_statements)))
            # self.stock_financial_statements.iloc[4513]
            ( conn.cursor().execute( upload_string,
             (float( self.stock_financial_statements['BTAXM'][i] ),
              float( self.stock_financial_statements['COST'][i] ),
              float( self.stock_financial_statements['EPS'][i] ),
              float( self.stock_financial_statements['GM'][i] ),
              float( self.stock_financial_statements['NI'][i] ),
              float( self.stock_financial_statements['BTAX'][i] ),
              float( self.stock_financial_statements['NIM'][i] ),
              float( self.stock_financial_statements['OE'][i] ),
              float( self.stock_financial_statements['OI'][i] ),
              float( self.stock_financial_statements['OIM'][i] ),
              float( self.stock_financial_statements['PRO'][i] ),
              float( self.stock_financial_statements['REV'][i] ),
              float( self.stock_financial_statements['TAX'][i] ),
              self.stock_financial_statements['stock_id'][i],
              int( self.stock_financial_statements['year'][i] ),
              int( self.stock_financial_statements['quar'][i] ),
              self.stock_financial_statements['url'][i]
              )  ) )
             
        conn.commit()
        conn.close()     
        
        
# self = CrawlerFinancialStatements(stock_id_set)
class CrawlerFinancialStatements:
    
    def __init__(self,stock_id_set):

        self.stock_id_set = stock_id_set


    def create_url(self):
        
        url = []
        for j in range(len(self.stock_id_set)):
            print(str(j)+'/'+str(len(self.stock_id_set)))
            index_url = 'https://stock.wearn.com/income.asp?kind='
            index_url = index_url + self.stock_id_set[j]
            
            res = requests.get(index_url,verify = True)  # 擷取該網站 原始碼         
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text, "lxml")# beautiful 漂亮的呈現原始碼
            tem = soup.find_all('a') 
            
            for i in range(len(tem)):
                if 'Income_detial.asp' in str(tem[i]):
                    #print(str(i)+str(tem[i]))
                    url.append( 'https://stock.wearn.com/' + tem[i]['href'])
                    
        return url
#---------------------------------------------------------------------
    def take_sotck_id(self,url):
        stock_id = re.search('kind=[0-9]*',url).group(0)
        stock_id = stock_id.replace('kind=','')
        return stock_id
    #--------------------------------------------------
    def take_stock_value(self,url):
        
        res = requests.get(url,verify = True)  # 擷取該網站 原始碼         
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text, "lxml")# beautiful 漂亮的呈現原始碼
        
        index = [0,1,2,3,5,6,7,
                 10,11,13,14,15,17]
        # 營業收入	營業成本 	營業毛利	 毛利率 營業費用	營業淨利	  營業利益率 
        # 稅前純益 稅前純益率 所得稅	  稅後純益	稅後純益率 EPS
        REV,COST,PRO,GM,OE,OI,OIM,BTAX,BTAXM,TAX,NI,NIM,EPS = [],[],[],[],[],[],[],[],[],[],[],[],[]
            
        stock_value = pd.DataFrame({'REV' : REV, 'COST' : COST, 'PRO' : PRO,
                                    'GM' : GM, 'OE' : OE, 'OI' : OI,
                                    'OIM' : OIM, 'BTAX' : BTAX, 'BTAXM' : BTAXM,
                                    'TAX' : TAX, 'NI' : NI, 'NIM' : NIM,
                                    'EPS' : EPS
                                    })
    
        col_name = ['REV','COST','PRO','GM','OE','OI','OIM','BTAX','BTAXM','TAX','NI','NIM','EPS']        
        tem = soup.find_all('td')
        try:
            for i in range(len(index)):
                value = tem[ index[i] ].text
                #print(value)
                value = value.replace(',','')
                if '%' in value:
                    value = value.replace('%','')
                    value = float(value)/100
                elif value == '':
                    value = -1
                    
                stock_value[col_name[i]] = [value]
                
            return stock_value ,1    
        except:
            return '',0
    #--------------------------------------------------        
    def take_year_and_quar(self,url):
        yearquar = re.search('y=[0-9]*',url).group(0)
        yearquar = yearquar.replace('y=','')
        
        quar = int( yearquar[len(yearquar)-2:len(yearquar)] )
        year = int( yearquar[0:len(yearquar)-2] )
        
        return year,quar
    #---------------------------------------------------------------------
    def crawler(self):
        # main
        self.url_set = self.create_url()

        stock_financial_statements = pd.DataFrame()        
        #error_log = []
        for i in range(len(self.url_set)) :# 
            print(str(i)+'/'+str(len(self.url_set)))
            url = self.url_set[i]
            #time.sleep(0.5)
            #print(url)
            stock_value,bo = self.take_stock_value(url)
            if bo == 0:
                print('error')
                #error_log.append(url)
            elif bo == 1:
                stock_value['stock_id'] = self.take_sotck_id(url)
                stock_value['year'],stock_value['quar'] = self.take_year_and_quar(url)
                stock_value['url'] = url
                stock_financial_statements = stock_financial_statements.append(stock_value)

        stock_financial_statements = stock_financial_statements.sort_values(['stock_id','year','quar'])
        stock_financial_statements.index = range(len(stock_financial_statements))

        self.stock_financial_statements = stock_financial_statements
        # CFS.stock_financial_statements = stock_financial_statements
    #------------------------------------------------------------------------------
    def fix(self,k):

        col_name = ['REV','COST','PRO','GM','OE','OI','OPR','NNOI','BTAX','TAX','NI','NPM','EPS']        
        error_col = []
        for i in range(len(col_name)):
            try:
                np.float32(self.stock_financial_statements[col_name[i]])
            except:
                print(col_name[i])
                error_col.append(col_name[i])
        
        error_index = []
        for j in range(len(error_col)):
            for i in range(len(self.stock_financial_statements)):
                try:
                    float(self.stock_financial_statements[error_col[j]][i])
                except:
                    error_index.append(i)
        error_index = list( set(error_index) & set(error_index) )
        print(len(error_index))

        fix_data = copy.deepcopy(self.stock_financial_statements)
        error_amount = 0
        for i in error_index:# 
            #print(i)
            url = self.stock_financial_statements['url'][i]
            #time.sleep(0.5)
            #print(url)
            stock_value,bo = self.take_stock_value(url)
            if bo == 0 and k==0:
                print('error')
                error_amount = error_amount+1
                #stock_value = pd.DataFrame([ '' for i in range(len(stock_value.iloc[0])) ])
            elif bo == 0 and k == 1:
                stock_value = pd.DataFrame([ -1 for i in range(13) ])
                stock_value = stock_value.T
                stock_value.columns = col_name
                
                stock_value['stock_id'] = self.take_sotck_id(url)
                stock_value['year'],stock_value['quar'] = self.take_year_and_quar(url)
                stock_value['url'] = url
                fix_data.iloc[i] = stock_value.iloc[0]
                
            elif bo == 1:
                stock_value['stock_id'] = self.take_sotck_id(url)
                stock_value['year'],stock_value['quar'] = self.take_year_and_quar(url)
                stock_value['url'] = url
                fix_data.iloc[i] = stock_value.iloc[0]
                # stock_financial_statements.iloc[i]
        self.stock_financial_statements = fix_data
        del fix_data
        #self.error_amount = error_amount
        return error_amount
        
    def main_fix(self):
        
        for k in range(5):
            error_amount = self.fix(k)
            print('k = ' + str(k))
            if error_amount == 0:
                break
            if k == 0:
                self.error_amount = error_amount
            if k > 0:
                print(self.error_amount)
                if error_amount == self.error_amount:
                    break
                else:
                    self.error_amount = error_amount
        
# https://www.cnyes.com/twstock/financial11.aspx?pi=2&mtype=T&stype=24&year=2016

def main():
    
    stock_info = stock_sql.get_stock_info_by_sql()
    stock_id_set = stock_info.stock_id
    
    CFS = CrawlerFinancialStatements(stock_id_set)
    CFS.crawler()
    CFS.main_fix()

    host = FinancialKey.host
    user = FinancialKey.user
    password = FinancialKey.password
    
    sql = Crawler2SQL(host,user,password,CFS.stock_financial_statements)
    #sql = Crawler2SQL(host,user,password,data)
    try:
        sql.create_table()
    except:
        123
    
    dataset_name = 'FinancialStatements'
    database = 'Financial_DataSet'
    sql.upload2sql(database,dataset_name )



