
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
import datetime
os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')
import stock_sql
import FinancialKey
import load_data

#------------------------------------------------------------------------------
host = FinancialKey.host
user = FinancialKey.user
password = FinancialKey.password
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
                         user = self.user,
                         password = self.password,
                         database = database,  
                         charset="utf8") )   

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
            
            res = requests.get(index_url,verify = True)        
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text, "lxml")
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
        
        res = requests.get(url,verify = True)      
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text, "lxml")
        
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
    def fix(self):
        def find_error_index(data,error_col,j):
            error_index = []
            for i in range(len(data)):
                #print(i)
                try:
                    float(data[error_col[j]][i])
                    #fix_data = fix_data.append(self.stock_financial_statements.iloc[i])
                except:
                    error_index.append(i)
            return error_index
        #---------------------------------------------------------------------
                        
        col_name = list( self.stock_financial_statements.columns    )
        [col_name.remove(x) for x in ['stock_id','year','quar','url'] ]
        error_col = []
        for i in range(len(col_name)):
            try:
                np.float32(self.stock_financial_statements[col_name[i]])
            except:
                print(col_name[i])
                error_col.append(col_name[i])
        
        error_index = []
        for i in range(len(error_col)):
            print(i)
            tem = find_error_index(self.stock_financial_statements,error_col,i)
            error_index.append(set(tem))

    
        for i in range(len(error_index)):
            if i == 0 :
                error = set( error_index[0] & error_index[1] )
            else:
                error = set( error & error_index[i] )
        if len(error) != 0:
            error = list(error)

            fix_data = self.stock_financial_statements.drop(
                    self.stock_financial_statements.index[error])
    
            for i in range(len(col_name)):
                fix_data[col_name[i]] = np.float32(fix_data[col_name[i]])
    
            fix_data.index = range(len(fix_data))
            self.stock_financial_statements = fix_data
            del fix_data
#----------------------------------------------------------------------------------------------------
class AutoCrawlerFinancialStatements(CrawlerFinancialStatements):
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
        
        sql_text = "SELECT year,quar FROM FinancialStatements  WHERE stock_id = "
        sql_text = sql_text + stock 
        
        tem =  load_data.execute_sql2(
                host = self.host,
                user = self.user,
                password = self.password,
                database = self.database,
                sql_text = sql_text)
        year,quar = [],[]
        for te in tem:
            year.append(te[0]-1911)
            quar.append(te[1])

        return year,quar

    def crawler(self):

        stock_financial_statements = pd.DataFrame()        
        for i in range(len(self.url)) :# 
            print(str(i)+'/'+str(len(self.url)))
            url = self.url[i]
            stock_value,bo = self.take_stock_value(url)
            if bo == 0:
                print('error')
            elif bo == 1:
                stock_value['stock_id'] = self.take_sotck_id(url)
                stock_value['year'],stock_value['quar'] = self.take_year_and_quar(url)
                stock_value['url'] = url
                stock_financial_statements = stock_financial_statements.append(stock_value)
        if len(stock_financial_statements) > 0:
            stock_financial_statements = stock_financial_statements.sort_values(['stock_id','year','quar'])
            stock_financial_statements.index = range(len(stock_financial_statements))
        #self.stock_financial_statements = stock_financial_statements
        return stock_financial_statements

    def crawler_new_data(self):
        def get_new_url(tem,date):
            url = []
            for i in range(len(tem)):
                if 'Income_detial.asp' in str(tem[i]):
                    #print(i)
                    x = tem[i].text
                    y = int( re.search('[0-9]+年',x).group(0).replace('年','') )
                    q = int( re.search('[0-9]+季',x).group(0).replace('季','') )
                    da = y*10 + q
                    if da not in date:
                        url.append( 'https://stock.wearn.com/' + tem[i]['href'] )    
            return url
        #------------------------------------------------------------------------------
        #------------------------------------------------------------------------------
        self.stock_financial_statements = pd.DataFrame()
        for i in range(len(self.stock_id_set)):
            print(str(i)+'/'+str(len(self.stock_id_set)))
            stock = self.stock_id_set[i]
            year,quar = self.get_yearquar(stock)
            date = [year[i]*10 + quar[i] for i in range(len(year)) ]
            
            index_url = 'https://stock.wearn.com/income.asp?kind='
            index_url = index_url + stock
      
            res = requests.post(index_url,verify = True)  
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text, "lxml")#
            tem = soup.find_all('a') 
            
            self.url = get_new_url(tem,date)
            if self.url == []:
                123
            else:
                tem = self.crawler()
                self.stock_financial_statements = self.stock_financial_statements.append(tem)
        if len(self.stock_financial_statements) > 0:
            self.stock_financial_statements = self.stock_financial_statements.sort_values(['stock_id','year','quar'])
            self.stock_financial_statements.index = range(len(self.stock_financial_statements))
            self.stock_financial_statements['year'] = self.stock_financial_statements['year'] + 1911
            
    def main(self):
        self.get_stock_id_set()
        self.crawler_new_data()
#----------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------
def crawler_history():
    
    stock_info = stock_sql.get_stock_info_by_sql()
    stock_id_set = stock_info.stock_id
    
    CFS = CrawlerFinancialStatements(stock_id_set)
    CFS.crawler()
    CFS.fix()
    CFS.stock_financial_statements['year'] = CFS.stock_financial_statements['year'] + 1911
    
    sql = Crawler2SQL(host,user,password,CFS.stock_financial_statements)
    #sql = Crawler2SQL(host,user,password,data)
    try:
        sql.create_table()
    except:
        123
    
    dataset_name = 'FinancialStatements'
    database = 'Financial_DataSet'
    sql.upload2sql(database,dataset_name )

def auto_crawler_new():
    ACFS = AutoCrawlerFinancialStatements(host = host,
                                          user = user,
                                          password = password,
                                          database = 'Financial_DataSet')
    # self = ACFS
    ACFS.main()
    if len(ACFS.stock_financial_statements) != 0 :    
        try:
            ACFS.fix()
        except:
            123
            
        if ACFS.stock_financial_statements.columns[0] == 0:
            ACFS.stock_financial_statements = ACFS.stock_financial_statements.T
            
        sql = Crawler2SQL(host,user,password,ACFS.stock_financial_statements)
        sql.upload2sql(dataset_name = 'FinancialStatements',database = 'Financial_DataSet' )

    #------------------------------------------------------
    try:
        sql_string = 'create table FinancialStatements ( name text(100),FSdate datetime)'
        FinancialKey.creat_datatable(host,user,password,'python',sql_string,'FinancialStatements')
    except:
        123
    text = 'insert into FinancialStatements (name,FSdate) values(%s,%s)'
    tem = str( datetime.datetime.now() )
    time = re.split('\.',tem)[0]
    value = ('FinancialStatements',time)

    stock_sql.Update2Sql(host,user,password,
                         'python',text,value)   
    
    
def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        # python3 /home/linsam/project/Financial_Crawler/CrawlerFinancialStatements.py new
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)




