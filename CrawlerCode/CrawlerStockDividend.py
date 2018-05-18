
'''
台股歷史配股
'''
# https://stock.wearn.com/dividend.asp?kind=2317

import requests
import os, sys
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import pymysql
import re
os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')
import stock_sql
from FinancialKey import host,user,password
import load_data
#-----------------------------------------------------------------
# 股東會日期 Shareholders meeting date
# 盈餘配股(元/股)	 Retained_Earnings 公積配股(元/股) Capital_Reserve
# 除權交易日 Ex_right_trading_day
# 員工配股(總張數) total employee bonus stock shares
# 現金股利 Cash dividend
# 除息交易日 Ex-dividend transaction day
# 員工紅利(總金額)(仟元) total employee bonus shares 
# 董監酬勞 (仟元) Directors remuneration

def UPDATE_sql(host,user,password,database,sql_text):
    # text = sql_text
    conn = ( pymysql.connect(host = host,# SQL IP
                     port = 3306,
                     user = user,# 帳號
                     password = password,# 密碼
                     database = database,  # 資料庫名稱
                     charset="utf8") )   #  編碼
                             
    cursor = conn.cursor()    

    try:   
        for i in range(len(sql_text)):
            cursor.execute(sql_text[i])
        conn.commit()
        conn.close()
        return 1
    except:
        conn.close()
        return 0

class Crawler2SQL:
    
    def __init__(self,host,user,password,dataset_name,database):
        self.host = host
        self.user = user
        self.password = password
        self.dataset_name = dataset_name
        self.database = database

    def creat_sql_file(self,sql_string,database):
        
        conn = ( pymysql.connect(host = self.host,# SQL IP
                                 port = 3306,
                                 user = self.user,# 帳號
                                 password = self.password,# 密碼
                                 database = self.database,  # 資料庫名稱
                                 charset="utf8") )   #  編碼           
        c=conn.cursor()
        c.execute( sql_string )
        c.execute('ALTER TABLE `'+self.dataset_name+'` ADD id BIGINT(64) NOT NULL AUTO_INCREMENT PRIMARY KEY;')
        c.close() 
        conn.close()
    
    def create_table(self):
        #dataset_name = 'StockDividend'
        sql_string = ('create table ' + self.dataset_name + 
                      '( meeting_data Date, Retained_Earnings FLOAT(16), Capital_Reserve FLOAT(16),'+
                      ' Ex_right_trading_day Date, total_employee_bonus_stock_shares FLOAT(16),' +
                      ' Cash_dividend FLOAT(16), Ex_dividend_transaction_day Date,'+
                      ' total_employee_bonus_shares FLOAT(16), Directors_remuneration FLOAT(16),'+ 
                      ' stock_id text(10))' )
    
        self.creat_sql_file(sql_string,'Financial_DataSet')  

    def upload2sql(self,data ):
        def datechabge(date):# date = self.data.meeting_data
           date = np.array( [ str(date[i]).split(' ')[0] for i in range(len(date)) ] )
           return date
       
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
                if tem in ['NaT','']:
                    123                
                elif col in ['meeting_data','Ex_right_trading_day',
                             'Ex_dividend_transaction_day','stock_id']:
                    value.append( tem )
                else:
                    value.append( float( tem ) )
            return value
        
        data.meeting_data = datechabge( data.meeting_data )
        data.Ex_right_trading_day = datechabge( data.Ex_right_trading_day )
        data.Ex_dividend_transaction_day = datechabge( data.Ex_dividend_transaction_day )
        
        # database = 'Financial_DataSet'
        conn = ( pymysql.connect(host = self.host,# SQL IP
                         port = 3306,
                         user = self.user,
                         password = self.password,
                         database = self.database,  
                         charset="utf8") )             
         
        for i in range(len(data)):
            #print(str(i)+'/'+str(len(data)))
            upload_string = create_upload_string(data,self.dataset_name,i)
            value =  create_upload_value(data,i)
            
            ( conn.cursor().execute( upload_string,tuple(value) ) )
             
        conn.commit()
        conn.close()     

class CrawlerStockDividend:
    
    def __init__(self):
        stock_info = stock_sql.get_stock_info_by_sql()
        self.stock_id_set = stock_info.stock_id

    def create_url_set(self):
        
        self.url_set = []
        for j in range(len(self.stock_id_set)):
            #print(str(j)+'/'+str(len(self.stock_id_set)))
            index_url = 'https://stock.wearn.com/dividend.asp?kind='
            index_url = index_url + self.stock_id_set[j]
            self.url_set.append(index_url) 
        
    def get_value(self,k):
        
        def take_value(soup,index):# index=4
            tr = soup.find_all('tr',{'class':'stockalllistbg2'})#stockalllistbg2 
            tr2 = soup.find_all('tr',{'class':'stockalllistbg1'})#stockalllistbg2 
            # tr is row
            # td is value
            data = []
            for i in range(len(tr)):
                td = tr[i].find_all('td')
                x = td[index].text.replace('\xa0','')
                if x == '':
                    data.append('')
                else:
                    x = x.replace('\n','').replace('\r','')
                    x = x.replace(' ','').replace(',','')
                    data.append(x)
                
            for i in range(len(tr2)):
                td = tr2[i].find_all('td')
                x = td[index].text.replace('\xa0','')
                if x == '':
                    data.append('')
                else:
                    x = x.replace('\n','').replace('\r','')
                    x = x.replace(' ','').replace(',','')
                    data.append(x)
                
            return data
        
        def change_date2AD(date):
            new_date = []
            
            for dat in date:
                if dat == '':
                    new_date.append('')
                else:
                    year = ( str( int( dat.split('/')[0] ) + 1911 ) )
                    month = ( dat.split('/')[1] )
                    day = ( dat.split('/')[2] )
                    new_date.append(year+'-'+month+'-'+day)
                    
            return new_date
            
        # index_url = 'https://stock.wearn.com/dividend.asp?kind=2330'
        index_url = self.url_set[k]# self = CTD
        
        res = requests.get(index_url,verify = True)        
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text, "lxml")

        col_name = ['meeting_data',
                    'Retained_Earnings',
                    'Capital_Reserve',
                    'Ex_right_trading_day',
                    'total_employee_bonus_stock_shares',
                    'Cash_dividend',
                    'Ex_dividend_transaction_day',
                    'total_employee_bonus_shares',
                    'Directors_remuneration']
        
        data = pd.DataFrame()
        for i in range(9):
            value = take_value(soup,i)
            if col_name[i] in ['meeting_data','Ex_right_trading_day','Ex_dividend_transaction_day']:
                value = change_date2AD(value)
            data[col_name[i]] = value

    
        data.meeting_data = pd.to_datetime(data.meeting_data)
        data = data.sort_values('meeting_data',ascending=False)
        data.Ex_right_trading_day = pd.to_datetime(data.Ex_right_trading_day)
        data.Ex_dividend_transaction_day = pd.to_datetime(data.Ex_dividend_transaction_day)
        
        data.index = range(len(data))
        stock_id = index_url.replace('https://stock.wearn.com/dividend.asp?kind=','')
        data['stock_id'] = stock_id 
        return data
    
    def main(self):
        self.create_url_set()
#-----------------------------------------------------------------------------
'''
self = AutoCrawlerStockDividend(host,user,password)

'''
class AutoCrawlerStockDividend(CrawlerStockDividend):
    def __init__(self,host,user,password):
        self.host = host
        self.user = user
        self.password = password
        self.database = 'Financial_DataSet'
        
    def create_url_set(self):
        sql_text = "SELECT DISTINCT `stock_id` FROM `StockDividend` "
        tem = load_data.execute_sql2(
                self.host,self.user,self.password,self.database,sql_text)
        
        self.stock_id_set = [te[0] for te in tem]
        
        self.url_set = []
        for j in range(len(self.stock_id_set)):
            #print(str(j)+'/'+str(len(self.stock_id_set)))
            index_url = 'https://stock.wearn.com/dividend.asp?kind='
            index_url = index_url + self.stock_id_set[j]
            self.url_set.append(index_url) 
            
    def get_data_id(self):
        #old_date = old_data['meeting_data']
        # sql_text = []
        sql_text = "SELECT id FROM `StockDividend` WHERE `meeting_data` = '"
        sql_text = sql_text + str(self.new_date.date()) + "' AND `stock_id` LIKE "
        sql_text = sql_text + self.new_data['stock_id'] 
        #sql_text.append()
        self.data_id = load_data.execute_sql2(self.host,self.user,self.password,self.database,sql_text)[0][0]     
        #return text
    def change_sql_data(self,col):# col = change_name[0]
        if str( self.new_data[col] ) == 'NaT':
            return ''
        else:
            tem = str( self.new_data[col] )
            if 'day' in col:
                tem = re.search('[0-9]+-[0-9]+-[0-9]+',tem).group(0)
                
            text = " UPDATE `StockDividend`" + " SET `"  + col + "` = "
            text = text + " " + tem  + " "
            text = text + "   WHERE `id` = " + str(self.data_id) +"; "
            #UPDATE `StockDividend` SET `Retained_Earnings` = '1' WHERE `StockDividend`.`id` = 1;
            #UPDATE_sql(self.host,self.user,self.password,self.database,sql_text)
            return text        
    def get_new(self):
        # stock = CTD.stock_id_set[0]
        
        #data_name = 'StockDividend'
        #sql_text = "SELECT * FROM `StockDividend` WHERE `stock_id` = " + stock
        #sql_text = sql_text + " AND `meeting_data`=( SELECT max(`meeting_data`) "
        #sql_text = sql_text + "FROM `StockDividend` WHERE `stock_id` = "+ stock
        #sql_text = sql_text + " )"
        #old_data = load_data.execute_sql2(host,user,password,database,sql_text)
        SD = load_data.StockDividend()
        self.old_data = SD.load(self.stock).iloc[0]
        self.old_date = self.old_data['meeting_data']
        self.new_date = self.new_data['meeting_data']
        
        change_name = list( self.new_data.index )
        self.sql_text = []
        
        if self.old_date == self.new_date.date():
            [ change_name.remove(col) for col in ['meeting_data','stock_id'] ]
            self.get_data_id()
            for col in change_name:
                tem = self.change_sql_data(col)
                if tem != '':
                    self.sql_text.append( tem )
        elif self.old_date < self.new_date.date():
            # add new data
            data = pd.DataFrame(self.new_data)
            data = data.T
            C2S = Crawler2SQL(self.host,self.user,self.password,'StockDividend','Financial_DataSet')
            C2S.upload2sql(data)
 
    def main(self):
        CTD = CrawlerStockDividend()
        CTD.main()
        C2S = Crawler2SQL(self.host,self.user,self.password,'StockDividend','Financial_DataSet')
        try:
            C2S.create_table()
        except:
            123
        self.create_url_set()
        for i in range(len(self.url_set)):
            print(str(i)+'/'+str(len(self.url_set)))# i=0
            self.new_data = self.get_value(i).iloc[0]
            self.stock = self.stock_id_set[i]
            self.get_new() 
            UPDATE_sql(self.host,self.user,self.password,
                       self.database,self.sql_text)
            
            #C2S.upload2sql(data)            
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------

        
def crawler_history():
    CTD = CrawlerStockDividend()
    CTD.main()
    C2S = Crawler2SQL(host,user,password,'StockDividend','Financial_DataSet')
    try:
        C2S.create_table()
    except:
        123
    
    for i in range(len(CTD.url_set)):
        print(str(i)+'/'+str(len(CTD.url_set)))#i=0
        data = CTD.get_value(i)
        C2S.upload2sql(data)
        
def auto_crawler_new():

    ACSD = AutoCrawlerStockDividend(host,user,password)
    ACSD.main()

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        # python3 /home/linsam/project/Financial_Crawler/CrawlerFinancialStatements.py new
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)






