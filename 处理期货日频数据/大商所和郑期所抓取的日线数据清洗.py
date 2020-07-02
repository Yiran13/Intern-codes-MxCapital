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
'焦煤':'i',
'乙二醇':'eg'
}
for one_date in whole_date_list:
    one_date=one_date.replace('-','')
    load_path=path+one_date+'.csv'
    df=pd.read_csv(load_path)
    df['交割月份']=df['交割月份'].astype(str)
    df['contract']=df['商品名称'].map(contract_dict)+df['交割月份']
    df['close']=df['收盘价'].str.replace(',','').astype(float)
    df['settle']=df['结算价'].str.replace(',','').astype(float)
    break
    clean_df=df[['contract','close','settle','date']]
    clean_df.to_csv(path+'clean/'+one_date+'.csv',index=False)
    print(one_date+'finished')

#处理郑州交易所日线数据，交易代码的替换与年月日保持一致
czce_path='D:/郑交所日频数据抓取/'
for one_date in whole_date_list:
    one_date=one_date.replace('-','')
    load_path=czce_path+one_date+'.csv'
    combine_df=pd.read_csv(load_path)
    combine_df['date']=pd.to_datetime(combine_df['date'],format='%Y%m%d').dt.strftime('%Y-%m-%d')

    combine_df['contract']=combine_df['contract'].str.replace('ME',',MA').str.replace('TC','ZC')
    combine_df['contract_type']=np.array((combine_df['contract'].str.findall(r'[a-zA-Z]+').tolist())).reshape(-1,)
    combine_df['deliver_month']=np.array((combine_df['contract'].str.findall(r'[0-9]+').tolist())).reshape(-1,)
    combine_df['first_part']='1'
    combine_df['year']=(pd.to_datetime(combine_df['date']).dt.strftime('%Y')).astype(int)
    combine_df['second_part']=combine_df['deliver_month'].str[0]
    change_index=combine_df.loc[np.where((combine_df['second_part']=='0') & (combine_df['year']>2010))]['first_part'].index.tolist()
    for index in change_index:

        combine_df.iloc[index,-3]='2'
    combine_df.loc[np.where((combine_df['second_part']=='0') & (combine_df['year']>2010))]
    combine_df['contract_code']=combine_df['contract_type']+combine_df['first_part']+combine_df['deliver_month']
    clean_df=combine_df[['contract_code','close','settle_price','date']]
    clean_df.to_csv(czce_path+'clean/'+one_date+'.csv',index=False)
   
