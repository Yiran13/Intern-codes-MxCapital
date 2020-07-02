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
for one_date in whole_date_list:
    

    one_date=one_date.replace('-','')
    
    url='http://www.czce.com.cn/cn/exchange/jyxx/hq/hq'+one_date+'.html'

    r = requests.get(url,headers=HEADERS)
    res=BeautifulSoup(r.content)
    #先编码再解码,转化源码
    text=r.content.decode('utf-8')
    res=etree.HTML(text)
    target_result=res.xpath('''//td[@align='left']/text()''')
    record_dict=dict()
    record_list=[]
    for target in target_result:
        target=target.strip()
        
        if target!='小计' and target!='总计':
            record_result=one_date+target
            result_list.append(record_result)
            print(record_result)
            
        
    if one_date=='20100824':


        break
    time.sleep(1)
    print(f'{one_date} finisehd')
#保存
save_pickle('D:/','czce_20100104-20100824',result_list)
#读取read_pickle('D:/','czce_20100104-20100824')
contract_pattern=re.compile(r'[a-zA-Z]+[0-9]+')
np_whole_date_list=np.array(whole_date_list)
stage2_start=np.where(np_whole_date_list=='2010-08-25')[0]
stag2_end=np.where(np_whole_date_list=='2015-09-30')[0]
result_list2=[]

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
    target_result=res.xpath('//td/text()')
    for target in target_result:
        target=target.strip()
        re_result=re.findall(contract_pattern,target)
        
        if re_result:
            final_result=one_date+re_result[0]
            result_list2.append(final_result)
            print(final_result)
    time.sleep(1)
    print(f'{one_date} finished')
  

save_pickle('D:/','czce_20100825-20150930',result_list2)

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
    target_result=res.xpath('//td/text()')
    for target in target_result:
        target=target.strip()
        re_result=re.findall(contract_pattern,target)
        
        if re_result:
            final_result=one_date+re_result[0]
            result_list3.append(final_result)
            print(final_result)
    time.sleep(1)
    print(f'{one_date} finished')
  

save_pickle('D:/','czce_20151008',result_list3)
combine_list=np.hstack((result_list1,result_list2,result_list3))
test_df=pd.DataFrame(combine_list)
date_array=np.array((test_df[0].str.findall(r'[0-9]{4}[0-9]{2}[0-9]{2}')).tolist()).reshape(-1,)
contract_array=np.array((test_df[0].str.findall(r'[a-zA-Z]+.+')).tolist()).reshape(-1,)
combine_df=pd.DataFrame({'date':date_array,'contract_code':contract_array})
combine_df['date']=pd.to_datetime(combine_df['date'],format='%Y%m%d').dt.strftime('%Y-%m-%d')
combine_date_list=combine_df['date'].unique().tolist()
#检验是否漏了日期
[x for x in whole_date_list if x not in combine_date_list]
#替换代码,全是str操作
combine_df['contract_code']=combine_df['contract_code'].str.replace('ME',',MA').str.replace('TC','ZC')
combine_df['contract_type']=np.array((combine_df['contract_code'].str.findall(r'[a-zA-Z]+').tolist())).reshape(-1,)
combine_df['deliver_month']=np.array((combine_df['contract_code'].str.findall(r'[0-9]+').tolist())).reshape(-1,)
#
combine_df['first_part']='1'
combine_df['year']=(pd.to_datetime(combine_df['date']).dt.strftime('%Y')).astype(int)
combine_df['second_part']=combine_df['deliver_month'].str[0]
change_index=combine_df.loc[np.where((combine_df['second_part']=='0') & (combine_df['year']>2010))]['first_part'].index.tolist()
for index in change_index:

    combine_df.iloc[index,-3]='2'
    
#np.where(combine_df['year']>2010)
#检查部分
combine_df.loc[np.where((combine_df['second_part']=='0') & (combine_df['year']>2010))]
combine_df['contract_code']=combine_df['contract_type']+combine_df['first_part']+combine_df['deliver_month']
#combine_df.to_csv('D:/czce_每日合约详细.csv',index=False)
check_list=[]
czce_kind_pool=['CF','SR','RM','TA','MA','OI','FG','ZC','PM','WH','RI','LR','JR','RS','CY','AP','CJ','SF','SM','UR']
for czce_kind in czce_kind_pool:
    parent_path='D:/郑州日频数据new/'+czce_kind
    if os.path.exists(parent_path):
        file_pool=os.listdir(parent_path)
        contract_file_name_pattern=re.compile(r'(.+)_(.+)_(.+)\.csv$')

        for index in range(len(file_pool)):
            ret=re.search(contract_file_name_pattern,file_pool[index])
            if ret:

                contract_date=ret.group(2).replace('-','')
                contract_code=ret.group(3)
                final_contract_code=contract_date+contract_code
            
                check_list.append(final_contract_code)
        print(f'{czce_kind} finished')
combine_df['check_type']=(combine_df['date']+combine_df['contract_code']).str.replace('-','')
result_list=combine_df['check_type'].values.tolist()
for czce_kind in czce_kind_pool:

    contract_code_type_pattern=re.compile(r'[0-9]+'+czce_kind+'[0-9]+')
    result_clean=[]
    check_clean=[]
    for result in result_list:
        ret=re.findall(contract_code_type_pattern,result)
        if ret:
            result_clean.append(ret[0])
    for check in check_list:
        ret2=re.findall(contract_code_type_pattern,check)
        if ret2:
            check_clean.append(ret2[0])
    lack_list=[x for x in result_clean if x not in check_clean]
    if len(lack_list)>0:
        test_df=pd.DataFrame(lack_list)
        date_array=np.array((test_df[0].str.findall(r'[0-9]{4}[0-9]{2}[0-9]{2}')).tolist()).reshape(-1,)
        contract_array=np.array((test_df[0].str.findall(r'[a-zA-Z]+.+')).tolist()).reshape(-1,)
        result_lack_df=pd.DataFrame({'date':date_array,'contract_code':contract_array})
        result_lack_df['date']=pd.to_datetime(result_lack_df['date'],format='%Y%m%d').dt.strftime('%Y-%m-%d')
        lack_date_list=result_lack_df['date'].unique()
        combine_target_list=[]
        for date in lack_date_list:
            target_df=result_lack_df.loc[result_lack_df['date']==date]
            target_array=target_df['contract_code'].values.tolist()
            combine_target_list.append(target_array)
            #combine_target_l
        final_result_df=pd.DataFrame({'date':lack_date_list,'lack contract':combine_target_list})
        final_result_df.to_csv('D:/整理日频数据/check_result/lack_contract/'+czce_kind.upper()+'_lack_contract.csv',index=False)
        print(f'{czce_kind} finished')
    else:
        print(f'{czce_kind} no lack')

   