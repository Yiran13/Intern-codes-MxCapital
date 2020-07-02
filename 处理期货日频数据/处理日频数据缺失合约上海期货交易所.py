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


result_list=[]
for one_date in whole_date_list:

    one_date=one_date.replace('-','')
    url='http://www.shfe.com.cn/data/dailydata/kx/kx'+one_date+'.dat'

    root = 'D:/shse_每日交易合约/'
    if not os.path.exists(root):
        os.makedirs(root)
    path = root + 'SH'+'_'+one_date #指定下载的目录,保存为txt文件
    r = requests.get(url)
    with open(path+'.txt', 'wb') as f:
        f.write(r.content)  
        f.close()
        print(f'{one_date} file is saved')
    file = open(path+'.txt', 'r', encoding='UTF-8')
    js = file.read()
    dic = json.loads(js)
    file.close()
    result_dict=dic['o_curinstrument']
    #进行解析json
    for result in result_dict:
        
        if result['DELIVERYMONTH']!='小计' and result['PRODUCTID'] !='总计':

            
                contract_name=result['PRODUCTID']
                contract_name=contract_name.replace(' ','').replace('_f','')
                deliver_month=result['DELIVERYMONTH']
                contract_code=contract_name+deliver_month
                final_result=one_date+contract_code
                result_list.append(final_result)
                
    print(f'{one_date} finished')

check_list=[]
shfe_kind_pool=['cu','al','zn','pb','ni','sn','rb','hc','bu','au','ag','ru','wr','ss','sc','fu','nr','sp']
for shfe_kind in shfe_kind_pool:
    parent_path='D:/日频数据按日期整理/'+shfe_kind
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
        print(f'{shfe_kind} finished')

for shfe_kind in shfe_kind_pool[1:]:

    contract_code_type_pattern=re.compile(r'[0-9]+'+shfe_kind+'[0-9]+')
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
    final_result_df.to_csv('D:/整理日频数据/check_result/lack_contract/'+shfe_kind.upper()+'_lack_contract.csv',index=False)
    print(f'{shfe_kind} finished')
   
    





