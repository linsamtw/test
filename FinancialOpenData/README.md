
## Financial Open Data

平台網址：http://114.34.138.146/phpmyadmin/ <br>
user : guest <br>
password : 123 <br>

------------------------------------------------------------
### 目前現有 data 如下

請先下載
[ load_data.py ](https://github.com/f496328mm/FinancialMining/blob/master/FinancialOpenData/load_data.py) <br>
ps : 可藉由 stock_id, data 進行資料合併


------------------------------------------------------------
#### 1. 台股股票一般資訊( 代號、名稱、產業 ) 
##### 1.1 讀取 data 教學 : 
```sh
SI = StockInfo()
data = SI.load()
```
##### 1.2 變數介紹 --- 1815 檔股票

| variable name | 變數名稱 | example |
|---------------|---------|----------|
| stock_id | 股票代號 | 1101 |
| stock_name | 股票中文名 | 台泥 |
| stock_class | 股票產業類別 | 水泥工業 |
| id | 索引 | 1 |

資料來源 :  <br>
https://goodinfo.tw/StockInfo/StockList.asp

------------------------------------------------------------
#### 2. history taiwan stock prices ( 台股歷史股價 )
##### 2.1 讀取 data 教學 : 
```sh
SP = StockPrice()
data = SP.load('2330')# 2330 is stock id
```
##### 2.2 變數介紹 --- 1815 檔股票，460萬筆 data， data size is 371 MB

| variable name | 變數名稱 | example |
|---------------|---------|----------|
|Date|日期| 2008-01-02 |
| stock_id | 股票代號, 以 [yahoo finance](https://finance.yahoo.com/) 為準 | 台股 0050 -> 美股 0050.TW |
| Open | 開盤 | 60.1 |
| High | 最高 | 61.3  |
| Low | 最低 | 60 |
| Close | 收盤 | 60.1 |
| Adj_Close  | 調整後的收盤價 | 57.2 |
| Volume | 成交量 | 4975000 |
| id | 索引 | 1 |

資料來源 :  <br>
fix_yahoo_finance

------------------------------------------------------------
#### 3. history taiwan stock Financial Statements ( 台股歷史財報 )
##### 3.1 讀取 data 教學 : 
```sh
FS = FinancialStatements()
data = FS.load('2330')# 讀取 2330 歷史財報
data = FS.load_all(')# 讀取 '所有股票' 歷史財報
```
##### 3.2 變數介紹 --- 1667 檔股票 ( 部分股票無財報 )，88,916 筆 data

| variable name | 變數名稱 | example (單位: 仟元) |
|---------------|---------|----------|
| BTAXM (Income before Tax Margin) | 稅前純益 | 0.1602 |
| COST (Cost of Goods Sold or Manufacturing)|營業成本|3963330|
| EPS (Earnings Per Share) |每股盈餘|0.17|
|GM  (Gross margin)|毛利率|0.1056|
|NI (Net Income)|稅後淨利|275033|
|BTAX (Income before Tax)|稅前純益|710033|
|NIM  ( Net Profit Margin)|純益率|0.0621|
|OE (Operating Expenses)|營業費用|260302|
|OI (Operating Income)|營業淨利|207582|
|OIM  (Operating Profit Margin)|營業利益率|0.0468|
|PRO (Gross Profit)|營業毛利|467884|
|REV (Gross Revenue)|營業收入|4431220|
|TAX (Income Tax Expense)|所得稅費用|435000|
|stock_id|股票代號|1101|
|year|年|1997|
|quar|季|4|
|url|資料來源|https://stock.wearn.com/Income_detial.asp?kind=1101&y=8604|

資料來源 : <br>
https://stock.wearn.com/Income.asp <br>
http://www.tedc.org.tw/tedc/bank/otccomp/ch1.3.4.htm




