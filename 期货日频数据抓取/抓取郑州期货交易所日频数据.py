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
HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
'Referer': 'http://www.czce.com.cn/cn/jysj/mrhq/H770301index_1.htm' }
def save_pickle(save_path:str,file_name:str,data):

    with open (save_path+file_name+'.pickle','wb') as f:
        pickle.dump(data,f)
def read_pickle(save_path:str,file_name:str):

    with open (save_path+file_name+'.pickle','rb') as f:
        data=pickle.load(f)
    return data
df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
result_list=[]
save_path='D:/郑交所日频数据抓取/'
for one_date in whole_date_list:
    

    one_date=one_date.replace('-','')
    
    url='http://www.czce.com.cn/cn/exchange/jyxx/hq/hq'+one_date+'.html'

    r = requests.get(url,headers=HEADERS)
 
    #先编码再解码,转化源码
    text=r.content.decode('utf-8')
    res=etree.HTML(text)
    #进行字符串操作
    #html写的比较乱，所以不太好爬取
    close_list=res.xpath('''//td[@class='tdformat'][5]//text()''')
    settle_price_list=res.xpath('''//td[@class='tdformat'][6]//text() ''')
    volume_list=res.xpath('''//td[@class='tdformat'][8]//text() ''')
    original_contract_list=res.xpath('''//td[@class='lefttdformat'][1]//text() ''')

    close_list=pd.Series(close_list)
    close_list=close_list.str.replace(',','')
    close_list=close_list.astype('float').tolist()
   
   
    settle_price_list=pd.Series(settle_price_list)
    settle_price_list=settle_price_list.str.replace(',','')
    settle_price_list=settle_price_list.astype('float').tolist()
    

    clean_contract_list=[]
    for contract in original_contract_list:
        clean_contract_list.append(contract.strip())
    original_df=pd.DataFrame({'contract':clean_contract_list,'volume':volume_list})
    original_df['volume']=original_df['volume'].str.replace(',','')
    original_df['volume']=original_df['volume'].astype('float')
    #用正则去筛选
    original_df=original_df.loc[original_df['contract'].str.match(r'^[a-zA-Z]+[0-9]+')]
    original_df['close']=close_list
    original_df['settle_price']=settle_price_list
    original_df['date']=one_date
    original_df.to_csv(save_path+one_date+'.csv',index=False)
    #保存




            
        
    if one_date=='20100824':


        break
    time.sleep(1)
    
    print(f'{one_date} finisehd')

contract_pattern=re.compile(r'[a-zA-Z]+[0-9]+')
np_whole_date_list=np.array(whole_date_list)
stage2_start=np.where(np_whole_date_list=='2010-08-25')[0]
stag2_end=np.where(np_whole_date_list=='2015-09-30')[0]

HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
'Referer': 'http://www.czce.com.cn/cn/jysj/mrhq/H770301index_1.htm' }

#for one_date in ['2015-09-30']:

for one_date in np_whole_date_list[int(stage2_start):int(stag2_end+1)]:
    one_date=one_date.replace('-','')
    year=one_date[:4]
    url='http://www.czce.com.cn/cn/exchange/'+year+'/datadaily/'+one_date+'.htm'
    r = requests.get(url,headers=HEADERS)
    text=r.content.decode('utf-8')
    res=etree.HTML(text)
    close_list=res.xpath('''//tr/td[6]//text()''')
    settle_price_list=res.xpath('''//tr/td[7]//text() ''')
    volume_list=res.xpath('''//tr/td[10]//text() ''')
    original_contract_list=res.xpath('''//tr/td[not(@class or @bgcolor)][1]/text()''')
    combine_df=pd.DataFrame({'contract':original_contract_list,'close':close_list,'settle_price':settle_price_list,'date':one_date,'volume':volume_list})
    combine_df['close']=combine_df['close'].str.replace(',','')
    combine_df['settle_price']=combine_df['settle_price'].str.replace(',','')
    combine_df['volume']=combine_df['volume'].str.replace(',','')
    combine_df=combine_df.loc[combine_df['contract'].str.match(r'^[a-zA-Z]+[0-9]+')]
    combine_df['close']=combine_df['close'].astype('float')
    combine_df['settle_price']=combine_df['settle_price'].astype('float')
    combine_df['volume']=combine_df['volume'].astype('float')
    combine_df.to_csv(save_path+one_date+'.csv',index=False)
    print(f'{one_date} finished')
  



stage3_start=np.where(np_whole_date_list=='2015-10-08')[0]

result_list3=[]

HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
'Referer': 'http://www.czce.com.cn/cn/jysj/mrhq/H770301index_1.htm' }

#for one_date in ['2015-09-30']:

for one_date in np_whole_date_list[int(stage3_start):]:
    one_date=one_date.replace('-','')
    year=one_date[:4]
    url='http://www.czce.com.cn/cn/DFSStaticFiles/Future/'+year+'/'+one_date+'/FutureDataDaily.htm'
    r = requests.get(url,headers=HEADERS)
    text=r.content.decode('utf-8')
    res=etree.HTML(text)
    close_list=res.xpath('''//tr/td[not(@valign='top')][6]//text()''')
    settle_price_list=res.xpath('''//tr/td[not(@valign='top')][7]//text()''')
    volume_list=res.xpath('''//tr/td[not(@valign='top')][10]//text() ''')
    original_contract_list=res.xpath('''//tr/td[not(@valign='top' or @align or @ class="td-noborder")][1]//text()''')
    combine_df=pd.DataFrame({'contract':original_contract_list,'close':close_list,'settle_price':settle_price_list,'date':one_date,'volume':volume_list})
    combine_df['close']=combine_df['close'].str.replace(',','')
    combine_df['settle_price']=combine_df['settle_price'].str.replace(',','')
    combine_df['volume']=combine_df['volume'].str.replace(',','')
    combine_df=combine_df.loc[combine_df['contract'].str.match(r'^[a-zA-Z]+[0-9]+')]
    combine_df['close']=combine_df['close'].astype('float')
    combine_df['settle_price']=combine_df['settle_price'].astype('float')
    combine_df['volume']=combine_df['volume'].astype('float')
    combine_df.to_csv(save_path+one_date+'.csv',index=False)
    print(f'{one_date} finished')

  
    time.sleep(1)
   
  

#所有数据储存完成