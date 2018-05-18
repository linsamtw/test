

import requests
import os,sys
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import datetime,re
os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')
import FinancialKey
import CrawlerStockDividend
import load_data
import stock_sql
#import CrawlerFinancialStatements

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
            #print(str(i)+'/'+str(len(data)))
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
        # response=json&dayDate=20180507&weekDate=20180514&monthDate=20180517&type=day&_=1526626688921
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
    def create_soup(self,date):
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
    def get_value(self,i):
        date = self.date[i]
        soup = self.create_soup(date)
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
    def crawler(self):
        # 買進金額	賣出金額	買賣差額
        # 自營商 Dealer
        # 投信 Investment Trust 
        # 外資 Foreign Investor
        #------------------------------------------------------------------------
        self.data = pd.DataFrame()
        for i in range(len(self.date)):
        #for i in range(10):
            print(str(i)+'/'+str(len(self.date)))
            value = self.get_value(i)
            if str(type(value)) != "<class 'str'>":
                self.data = self.data.append(value)

    def main(self):
        self.create_date()
        self.crawler()
        self.data.index = range(len(self.data))
        
        
'''
self = AutoCrawlerInstitutionalInvestors(host,user,password)
self.main()

'''
class AutoCrawlerInstitutionalInvestors(CrawlerInstitutionalInvestors):
    def __init__(self,host,user,password):
        super(AutoCrawlerInstitutionalInvestors, self).__init__()        
        self.host = host
        self.user = user
        self.password = password
        self.database = 'Financial_DataSet'
    def get_max_old_date(self):
        sql_text = "SELECT MAX(date) FROM `InstitutionalInvestors`"
        tem = load_data.execute_sql2(self.host,self.user,self.password,self.database,sql_text)
        self.old_date = tem[0][0]

        
    def create_date(self):
        self.get_max_old_date()
        
        today = datetime.datetime.now().date()
        delta = today - self.old_date      
        
        date = [ self.old_date + datetime.timedelta(i+1) for i in range(delta.days) ]
        # '950809','950810',
        year = [ str( da.year - 1911 ) for da in  date ] 
        month = [ str( da.month ) for da in  date ] 
        days = [ str( da.day ) for da in  date ] 
        
        self.date = []
        for i in range(len(year)):
            y = year[i]
            m = month[i]
            d = days[i]
            if len(m) == 1:m = '0'+m
            if len(d) == 1:d = '0'+d
            self.date.append( y+m+d )
            
    def main(self):
        self.create_date()
        self.crawler()
        self.data.index = range(len(self.data))
        
        
def crawler_history():
    
    CII = CrawlerInstitutionalInvestors()
    CII.main()
    #CII.data

    C2S = Crawler2SQL(host,user,password,'InstitutionalInvestors','Financial_DataSet')
    try:
        C2S.create_table(CII.data.columns)
    except:
        123
    
    C2S.upload2sql(CII.data)

def auto_crawler_new():
    ACII = AutoCrawlerInstitutionalInvestors(host,user,password)
    ACII.main()

    C2S = Crawler2SQL(host,user,password,'InstitutionalInvestors','Financial_DataSet')
    C2S.upload2sql(ACII.data)

    try:
        sql_string = 'create table InstitutionalInvestors ( name text(100),CrawlerDate datetime)'
        FinancialKey.creat_datatable(host,user,password,'python',sql_string,'InstitutionalInvestors')
    except:
        123
    text = 'insert into InstitutionalInvestors (name,CrawlerDate) values(%s,%s)'
    
    tem = str( datetime.datetime.now() )
    time = re.split('\.',tem)[0]
    value = ('InstitutionalInvestors',time)

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

