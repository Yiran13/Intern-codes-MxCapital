import pandas as pd
import numpy as np 
import os
import re
import requests
import xlwt
import os
import json
import tushare as ts
from bs4 import BeautifulSoup
import pickle
from lxml import etree 
import time
df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()

save_path='D:/大商所日频数据抓取/'
if not os.path.exists(save_path):
    os.makedirs(save_path)

HEADERS={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
'Accept-Encoding': 'gzip, deflate',
'Accept-Language': 'zh,zh-TW;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6',
'Cache-Control': 'max-age=0',
'Connection': 'keep-alive',
'Content-Length': '70',
'Content-Type': 'application/x-www-form-urlencoded',
'Cookie': 'JSESSIONID=E643B59A02481AAFA711E7102AAD1F10; WMONID=PKtEeOFRpjR; eeeeeee=274a6704eeeeeee_274a6704; Hm_lvt_a50228174de2a93aee654389576b60fb=1574824197,1574837128,1575264224,1575335116; Hm_lpvt_a50228174de2a93aee654389576b60fb=1575335124',
'Host': 'www.dce.com.cn',
'Origin': 'http://www.dce.com.cn',
'Referer': 'http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}
for index in range(len(whole_date_list)):
    if whole_date_list[index]=='2017-12-06':
        print(index)
        break
for one_date in whole_date_list[index:]:

    one_date=one_date.replace('-','')
    year=one_date[0:4]
    month=one_date[4:6]
    day=one_date[6:]
    data={'dayQuotes.variety': 'all',
    'dayQuotes.trade_type': '0',
    'year': year,
    'month': str(int(month)-1),
    'day': day}
    url='http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html'
    r = requests.post(url,headers=HEADERS,data=data)
  
    #先编码再解码,转化源码
    text=r.content.decode('utf-8')
    res=etree.HTML(text)
    target_tr=res.xpath('''//table[@cellpadding='0']/tr''')
    length=len(target_tr)
    combine_df=pd.DataFrame(columns=['商品名称','交割月份','开盘价',
    '最高价',
    '最低价',
    '收盘价',
    '前结算价',
    '结算价',
    '涨跌',
    '涨跌1',
    '成交量',
    '持仓量',
    '持仓量变化',
    '成交额'])
    for index in range(length):
        index=str(index)
        tr=res.xpath('''//table[@cellpadding='0']/tr'''+'''['''+index+''']'''+'''//td/text()''')
        if tr:
            tr=pd.Series(tr).str.strip()
            tr=pd.DataFrame(tr.values.reshape(1,-1),columns=['商品名称','交割月份','开盘价',
            '最高价',
            '最低价',
            '收盘价',
            '前结算价',
            '结算价',
            '涨跌',
            '涨跌1',
            '成交量',
            '持仓量',
            '持仓量变化',
            '成交额'])
            combine_df=pd.concat([combine_df,tr])

    combine_df=combine_df.loc[(combine_df['商品名称'].str.match(r'.+小计')==0)]
    combine_df['date']=one_date
    combine_df.to_csv(save_path+one_date+'.csv',index=False,encoding='utf-8-sig')
    print (f'{one_date } finished')
    
    time.sleep(1)