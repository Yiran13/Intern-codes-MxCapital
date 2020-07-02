import pandas as pd
import numpy as np 
import os
import re
import requests
import xlwt
import os
import json
import tushare as ts
df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
path=r'D:/大商所日频数据抓取/'
contract_dict={'豆一':'a',
'豆二':'b',
'豆粕':'m',
'豆油':'y',
'棕榈油':'p',
'玉米':'c',
'玉米淀粉':'cs',
'鸡蛋':'jd',
'粳米':'rr',
'纤维板':'fb',
'胶合板':'bb',
'聚乙烯':'l',
'聚氯乙烯':'v',
'聚丙烯':'pp',
'苯乙烯':'eb',
'焦炭':'j',
'焦煤':'jm',
'铁矿石':'i',
'乙二醇':'eg'

}
#remind：有大宗商品交易在收盘后导致，volume不为0，但是open/high/low 为0
combine_df=pd.DataFrame()
for one_date in whole_date_list:
    one_date=one_date.replace('-','')
    load_path=path+one_date+'.csv'
    df=pd.read_csv(load_path)
    df['交割月份']=df['交割月份'].astype(str)
    df['contract_code']=df['商品名称'].map(contract_dict)+df['交割月份']
    df['contract_code']=df['contract_code'].str.upper()
    empty_list=df.loc[df['开盘价']=='-'].index.tolist()
    df.iloc[empty_list,10]='0'
    df.iloc[empty_list,13]='0'
   
    volume_0_list=df.loc[df['成交量']=='0'].index.tolist()
    
    for col_index in [2,3,4]:
        df.iloc[volume_0_list,col_index]=df.iloc[volume_0_list,7]

    df['tclose']=df['收盘价'].str.replace(',','').astype(float)
    df['tsettle']=df['结算价'].str.replace(',','').astype(float)
    df['topen']=df['开盘价'].str.replace(',','').astype(float)
    df['thigh']=df['最高价'].str.replace(',','').astype(float)
    df['tlow']=df['最低价'].str.replace(',','').astype(float)
    df['tvolume']=df['成交量'].str.replace(',','').astype(float)
    df['toi']=df['持仓量'].str.replace(',','').astype(float)
    df['tamount']=df['成交额'].str.replace(',','').astype(float)
    df['date']=one_date[:4]+'-'+one_date[4:6]+'-'+one_date[6:]
    
    clean_df=df[['date','contract_code','topen','thigh','tlow','tclose','tvolume','tsettle','tamount','toi']]
    clean_df.to_csv(path+'clean/'+one_date+'.csv',index=False)
    print(one_date+'finished')
    combine_df=pd.concat([combine_df,clean_df])

combine_df.to_csv(path+'clean_汇总数据.csv',index=False)