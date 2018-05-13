# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import requests
import os,sys
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')
import FinancialKey
import CrawlerStockDividend

host = FinancialKey.host
user = FinancialKey.user
password = FinancialKey.password

class Crawler2SQL(CrawlerStockDividend.Crawler2SQL):    

    def create_table(self,colname):
        # colname = data.columns
        sql_string = 'create table ' + self.dataset_name + '('
        
        for col in colname:
            if col == 'date':
                sql_string = sql_string + col + ' Date,'
            else:
                sql_string = sql_string + col + ' FLOAT(16),'
            
        sql_string = sql_string[:len(sql_string)-1] + ')'
    
        self.creat_sql_file(sql_string,'Financial_DataSet')  

    def upload2sql(self,data ):
       
        def create_upload_string(data,dataset_name,i):
            colname = data.columns
            upload_string = ('insert into ' + dataset_name + '(')
            for col in colname:
                if data[col][i] not in ['NaT','']:
                    upload_string = upload_string+col+','
            upload_string = upload_string[:len(upload_string)-1] +') values('
            
            for col in colname:
                if data[col][i] not in ['NaT','']:
                    upload_string = upload_string+'%s,'
                    
            upload_string = upload_string[:len(upload_string)-1] + ')'
            return upload_string
        
        def create_upload_value(data,i):
            
            colname = data.columns
            value = []
            for col in colname:
                tem = data[col][i]            
                if col in ['date']:
                    value.append( tem )
                else:
                    value.append( float( tem ) )
            return value

        # database = 'Financial_DataSet'
        conn = ( pymysql.connect(host = self.host,# SQL IP
                         port = 3306,
                         user = self.user,
                         password = self.password,
                         database = self.database,  
                         charset="utf8") )             
         
        for i in range(len(data)):
            print(str(i)+'/'+str(len(data)))
            upload_string = create_upload_string(data,self.dataset_name,i)
            value =  create_upload_value(data,i)
            
            ( conn.cursor().execute( upload_string,tuple(value) ) )
             
        conn.commit()
        conn.close()     

'''
self = CrawlerInstitutionalInvestors()
self.main()

'''
class CrawlerInstitutionalInvestors:
    def __init__(self):
        self.url = 'https://stock.wearn.com/fundthree.asp?mode=search'
        #url = 20040101'
        # url = 'http://www.twse.com.tw/fund/BFI82U?response=json&dayDate=20180302'
    def create_date(self):
        year = [ str(i) for i in range(93,108,1) ]
        month = [ '0' + str(i) for i in range(1,10) ]
        [ month.append(str(m)) for m in range(10,13,1)  ]
        days = [ '0' + str(i) for i in range(1,10) ]
        [ days.append(str(d)) for d in range(10,32,1)  ]
        
        self.date = []
        for y in year:
            for m in month:
                for d in days:
                    self.date.append(y+m+d)
    def crawler(self):
        # 買進金額	賣出金額	買賣差額
        # 自營商 Dealer
        # 投信 Investment Trust 
        # 外資 Foreign Investor     
        def create_soup(date):
            form_data = {
                    'yearE': date[:len(date)-4],
                    'monthE': date[len(date)-4:len(date)-2],
                    'dayE': date[len(date)-2:],
                    'Submit1': '(unable to decode value)'
                    }
            res = requests.get(self.url,verify = True,data = form_data)     
            #res = requests.post(url,verify = True,data = form_data)  
            res.encoding = 'big5'      
            soup = BeautifulSoup(res.text, "lxml")        
            return soup
        
        def get_value(i):
            date = self.date[i]
            soup = create_soup(date)
            #tem = soup.find_all('div',{'align':"center"})
            
            if '自營商(自行買賣)' not in soup.text :
                return ''  
            # (億元)
            tem = soup.find_all('td')
            x = []
            for i in range(len(tem)):
                if tem[i].text != '三大法人買賣超(億)':
                    te = tem[i].text
                    for tex in ['\xa0','+',' ',','] :
                        te = te.replace(tex,'')
                    x.append( te ) 
                else:
                    break
            x = x[1:]
            buy_set = []
            sell_set = []
            difference_set = []

            for i in [ 1+i*4 for i in range(5) ]:
                try:
                    buy_set.append(float(x[i]))
                    sell_set.append(float(x[i+1]))
                    difference_set.append(float(x[i+2]))
                except:
                    buy_set.append(float(0))
                    sell_set.append(float(0))
                    difference_set.append(float(0))
                
            buy_set[1] = buy_set[0]+buy_set[1]
            buy_set = buy_set[1:]
            sell_set[1] = sell_set[0]+sell_set[1]
            sell_set = sell_set[1:]
            difference_set[1] = difference_set[0]+difference_set[1]
            difference_set = difference_set[1:]            
                    
            colname = ['Dealer_buy','Dealer_sell','Dealer_difference',
                       'Investment_Trust_buy','Investment_Trust_sell','Investment_Trust_difference',
                       'Foreign_Investor_buy','Foreign_Investor_sell','Foreign_Investor_difference',
                       'total_buy','total_sell','total_difference']
            value = pd.DataFrame()
            for i in range(len(buy_set)):
                value[colname[0+i*3]] = [buy_set[i]]
                value[colname[1+i*3]] = [sell_set[i]]
                value[colname[2+i*3]] = [difference_set[i]]
            value['date'] = ( str(int(date[:len(date)-4])+1911) + 
                             '-'+ date[len(date)-4:len(date)-2]+ 
                             '-'+ date[len(date)-2:] )
            return value
        #------------------------------------------------------------------------
        self.create_date()
        self.data = pd.DataFrame()
        for i in range(len(self.date)):
            print(str(i)+'/'+str(len(self.date)))
            value = get_value(i)
            if str(type(value)) != "<class 'str'>":
                self.data = self.data.append(value)

    def main(self):
        self.crawler()
        self.data.index = range(len(self.data))

def main():
    
    CII = CrawlerInstitutionalInvestors()
    CII.main()
    CII.data

    C2S = Crawler2SQL(host,user,password,'InstitutionalInvestors','Financial_DataSet')
    try:
        C2S.create_table(CII.data.columns)
    except:
        123
    
    C2S.upload2sql(CII.data)





