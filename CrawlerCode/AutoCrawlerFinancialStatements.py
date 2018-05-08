


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
import FinancialKey
import load_data
import CrawlerFinancialStatements

host = FinancialKey.host
user = FinancialKey.user
password = FinancialKey.password



class AutoCrawlerFinancialStatements(CrawlerFinancialStatements.CrawlerFinancialStatements):
    def __init__(self,host,user,password,database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        
    def get_stock_id_set(self):
        
        data =  load_data.execute_sql2(
                host = self.host,
                user = self.user,
                password = self.password,
                database = self.database,
                sql_text = 'SELECT distinct `stock_id` FROM FinancialStatements')
        
        self.stock_id_set = [ da[0] for da in data]
    
    def get_yearquar(self,stock):
        #stock = stock_id_set[i]
        
        sql_text = "SELECT year,quar FROM FinancialStatements  WHERE stock_id = '"
        sql_text = sql_text + stock + "' and id=( SELECT max(id) FROM FinancialStatements WHERE stock_id = '"
        sql_text = sql_text + stock +"' )"
    
        tem =  load_data.execute_sql2(
                host = self.host,
                user = self.user,
                password = self.password,
                database = self.database,
                sql_text = sql_text)
        year = tem[0][0] - 1911
        quar = tem[0][1]
        return year,quar

    def crawler(self):

        stock_financial_statements = pd.DataFrame()        
        for i in range(len(self.url)) :# 
            #print(str(i)+'/'+str(len(self.url)))
            url = self.url[i]
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
        #self.stock_financial_statements = stock_financial_statements
        return stock_financial_statements

    def crawler_new_data(self):
        def get_new_url(tem,date):
            url = []
            for i in range(len(tem)):
                if 'Income_detial.asp' in str(tem[i]):
                    #print(tem[i].text)
                    
                    x = tem[i].text
                    y = int( re.search('[0-9]+年',x).group(0).replace('年','') )
                    q = int( re.search('[0-9]+季',x).group(0).replace('季','') )
                    da = y+q
                    if da > date:
                        url.append( 'https://stock.wearn.com/' + tem[i]['href'] )    
            return url
        #------------------------------------------------------------------------------
        #------------------------------------------------------------------------------
        self.stock_financial_statements = pd.DataFrame()
        for i in range(len(self.stock_id_set)):
            print(str(i)+'/'+str(len(self.stock_id_set)))
            stock = self.stock_id_set[i]
            year,quar = self.get_yearquar(stock)
            date = year + quar
            
            index_url = 'https://stock.wearn.com/income.asp?kind='
            index_url = index_url + stock
            
            res = requests.get(index_url,verify = True)  # 擷取該網站 原始碼         
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text, "lxml")# beautiful 漂亮的呈現原始碼
            tem = soup.find_all('a') 
            
            self.url = get_new_url(tem,date)
            if self.url == []:
                123
            else:
                tem = self.crawler()
                self.stock_financial_statements = self.stock_financial_statements.append(tem)
    
    def main(self):
        self.get_stock_id_set()
        self.crawler_new_data()

        


def main():

    ACFS = AutoCrawlerFinancialStatements(host = host,
                                          user = user,
                                          password = password,
                                          database = 'Financial_DataSet')
    ACFS.main()

    sql = CrawlerFinancialStatements.Crawler2SQL(host,user,password,ACFS.stock_financial_statements)
    sql.upload2sql(dataset_name = 'FinancialStatements',database = 'Financial_DataSet' )



