

from pandas_datareader import data as pdr
import os,sys
import pandas as pd
import fix_yahoo_finance as yf
import numpy as np
import datetime,re
import pymysql
os.chdir('/home/linsam/project/Financial_Crawler')
sys.path.append('/home/linsam/project/Financial_Crawler')
import FinancialKey
import stock_sql

host = FinancialKey.host
user = FinancialKey.user
password = FinancialKey.password
#-----------------------------------------------    
def execute_sql2(host,user,password,database,sql_text):
    
    conn = ( pymysql.connect(host = host,
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
    
def take_stock_id_by_sql():
    #---------------------------------------------------------------                         
    tem = execute_sql2(
        host = host,
        user = user,
        password = password,
        database = 'StockPrice',
        sql_text = 'SHOW TABLES')                      
    stock_cid = [ d[0][1:].replace('_','.').split('.')[0] for d in tem ]
    stock_id = [ d[0][1:].replace('_','.') for d in tem ]
    
    stock_id = pd.DataFrame({'stock_id' : stock_id,
                             'stock_cid' : stock_cid})    
    
    return stock_id
    
class crawler_new_stock_price:
    def __init__(self,stock,stock_id):
        yf.pdr_override()
        self.stock = str( stock )
        self.stock_id = stock_id
          
    def get_start_and_today(self):
        #stock = '2330'
        try:
            tem = self.stock_id[ self.stock_id['stock_cid'] == str(self.stock)].stock_id
            data_name = tem[ tem.index[0] ]
            self.data_name = '_' + data_name.replace('.','_')
        except:
            print("the stock isn't exist")
            return ''
        
        sql_text = "SELECT `Date` FROM `"+self.data_name+"` ORDER BY `Date` DESC LIMIT 1"
        
        start = execute_sql2(
                host = host,
                user = user,
                password = password,
                database = 'StockPrice',
                sql_text = sql_text)

        start = start[0][0] #+ datetime.timedelta(days=1)
        self.start = np.datetime64(start).astype('str')
        
        now = datetime.datetime.now()
        today = now + datetime.timedelta(days=1)
        self.today = str( today.year ) + '-' + str( today.month )+ '-' +str( today.day+1 )

    def get_new_data(self):
        tem = self.stock_id[ self.stock_id['stock_cid'] == str(self.stock)]['stock_id']
        self.stock = np.array(tem)[0]
        
        self.data = pdr.get_data_yahoo( self.stock, start =  self.start, end = self.today)
        #self.data = pdr.get_data_yahoo( '2330.TW', start =  '2018-4-9', end = '2018-5-9')
        
    def upload_data2sql(self):
        def datechabge(date):# date = self.data.meeting_data
           date = np.array( [ str(date[i]).split(' ')[0] for i in range(len(date)) ] )
           return date
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
        #------------------------------------------------------------------
        self.data['Date'] = self.data.index
        self.data['stock_id'] = self.stock
        #data['stock_name'] = stock_name
        self.data = self.data[self.data['Date'] > np.datetime64( self.start )]
        self.data.index = range(len(self.data))
        #self.data.Date = np.array( [ str(self.data.Date[i]).split(' ')[0] for i in range(len(self.data)) ] )
        self.data.Date = datechabge( self.data.Date )
        # self.data.dtypes
        if len(self.data)==0:
            print("data doesn't need to upload")
        upload_stock_price2sql(self.data,self.data_name)

    def main(self):
        self.get_start_and_today()
        self.get_new_data()
        self.upload_data2sql()
  
def save_new_craw_process(stock):
    
    stock_id = take_stock_id_by_sql()
    try:
        tem = stock_id[ stock_id['stock_cid'] == str(stock)].stock_id
        data_name = tem[ tem.index[0] ]
        data_name = '_' + data_name.replace('.','_')
    except:
        print("the stock isn't exist")
        return ''
    
    sql_text = "SELECT `Date` FROM `"+data_name+"` ORDER BY `Date` DESC LIMIT 1"
    start = execute_sql2(
            host = host,
            user = user,
            password = password,
            database = 'StockPrice',
            sql_text = sql_text)
    start = start[0][0] #+ datetime.timedelta(days=1)
    start = np.datetime64(start).astype('str')
        
    conn = ( pymysql.connect(host = host,# SQL IP
                     port = 3306,
                     user = user,
                     password = password,
                     database='python',  
                     charset="utf8") )   
    cursor = conn.cursor()
    tem = str( datetime.datetime.now() )
    time = re.split('\.',tem)[0]
    #---------------------------------------------------------------------------        
    ( cursor.execute('insert into '+ 'StockPriceProcess'  +
                    '(name,stockdate,time)'+ ' values(%s,%s,%s)', 
              (stock,str(start),time) ) )
     
    conn.commit()
    cursor.close()
    conn.close()  
    
def main():      
    # FinancialKey.creat_StockPriceProcess_file()
    yf.pdr_override()
    stock_id = take_stock_id_by_sql()
    #-----------------------------------------------    
    i = 1 
    for stock in stock_id['stock_cid']:
        print(str(i)+'/'+str(len(stock_id)) + ' : ' + stock)
        self = crawler_new_stock_price(stock,stock_id)
        self.main()
        save_new_craw_process(stock)
        i=i+1
        # stock='0053'
    
    #------------------------------------------------------
    text = 'insert into StockPriceProcess (name,stockdate,time) values(%s,%s,%s)'
    today = str( datetime.datetime.now().strftime("%Y-%m-%d") )
    tem = str( datetime.datetime.now() )
    time = re.split('\.',tem)[0]
    value = ('StockPrice',today,time)

    stock_sql.Update2Sql(host,user,password,
                         'python',text,value)        

#----------------------------------------------------------
    

main()


