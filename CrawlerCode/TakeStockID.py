#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 14 15:04:04 2018

@author: linsam
"""

import pandas as pd
from selenium import webdriver
import numpy as np
from pandas_datareader import data as pdr
import fix_yahoo_finance as yf
import os,sys

os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')

#self = TakeStockID()
class main:
    def __init__(self):
        self.driver = webdriver.Firefox()
    
    #-------------------------------------------------------------------
    def find_stock_class_name(self):
        url = 'https://goodinfo.tw/StockInfo/StockList.asp'
        self.driver.get(url)
        stock_class_name = []
        for i in range(2,11):
            #print(i)
            # //*[@id="STOCK_LIST_ALL"]/tbody/tr[2]
            # //*[@id="STOCK_LIST_ALL"]/tbody/tr[10]
            stock_index = self.driver.find_element_by_xpath(
                    '//*[@id="STOCK_LIST_ALL"]/tbody/tr['+str(i)+']')
            x = stock_index.text
            x = x.split(' ')
            for te in x:
                if te !='': stock_class_name.append( te )
        #return stock_class_id
        self.stock_class_name = stock_class_name
    
    def find_all_stock_id(self,i):
        url = 'https://goodinfo.tw/StockInfo/StockList.asp?MARKET_CAT=全部&INDUSTRY_CAT='
        #url = url + '00'
        url = url + str( self.stock_class_name[i] )# '半導體業'
        url = url + '&SHEET=交易狀況&SHEET2=日&RPT_TIME=最新資料'
    
        self.driver.get(url)
        
        tem = self.driver.find_elements_by_class_name('solid_1_padding_3_1_tbl')
        try:
            tex = tem[1]
        except:
            return 0,0,0
        tem2 = tex.text.split('\n')
        
        stock_id = []
        stock_name = []
        stock_class = []
        for tex in tem2:
            if tex == '代號' or tex == '名稱':
                123
            else:
                #print(tex)
                te = tex.split(' ')
                stock_id.append(te[0])
                stock_name.append(te[1])
                stock_class.append(self.stock_class_name[i])
                
        return stock_id,stock_name,stock_class

    def run(self):
        self.find_stock_class_name()

        stock_id,stock_name,stock_class = [],[],[]
        for i in range(len(self.stock_class_name)):
        #for i in range(3): # for test
            print(str(i)+'/'+str(len(self.stock_class_name)))
            tem_stock_id,tem_stock_name,tem_stock_class = self.find_all_stock_id(i)
            if tem_stock_id == 0:
                print('break')
            else:
                stock_id.append(tem_stock_id)
                stock_name.append(tem_stock_name)
                stock_class.append(tem_stock_class)
            
        stock_id = np.concatenate(stock_id, axis=0)
        stock_name = np.concatenate(stock_name, axis=0)
        stock_class = np.concatenate(stock_class, axis=0)
        
        self.stock_info = pd.DataFrame({'stock_id':stock_id,
                                        'stock_name':stock_name,
                                        'stock_class':stock_class})
        self.driver.close()
    
    

    
    
    
    
    
    
    
    
    
    
    