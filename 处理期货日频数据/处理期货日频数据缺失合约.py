import pandas as pd
import numpy as np 
import os
import re

df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()

#处理大连期货交易所，没有用到爬虫

load_path='D:/dce_合约.xls'
df=pd.read_excel(load_path)
df['最后交易日']=pd.to_datetime(df['最后交易日'],format='%Y%m%d')
df['开始交易日']=pd.to_datetime(df['开始交易日'],format='%Y%m%d')
df=df.loc[df['最后交易日']>=pd.to_datetime('2010-01-04')]
df['type']=np.array((df['合约代码'].str.findall(r'[a-zA-Z]+')).tolist()).reshape(-1,)
dce_kind_pool=df['type'].unique().tolist()
#测试1
#dce_kind='a'
for dce_kind in dce_kind_pool:
    slice_df=df.loc[df['type']==dce_kind]
    #测试2
    #combine_df=pd.DataFrame()
    combine_list=[]
    #date_index=0
    for date_index in range(len(whole_date_list)):
            con_1=slice_df['最后交易日']>=pd.to_datetime(whole_date_list[date_index])
            con_2=slice_df['开始交易日']<=pd.to_datetime(whole_date_list[date_index])
            result_df=slice_df.loc[con_1&con_2]
            result_df['date']=whole_date_list[date_index]
            result_df=result_df[['合约代码','date']]
            #combine_df=pd.concat([combine_df,result_df])
            result_list=(result_df['date']+result_df['合约代码']).tolist()
            combine_list=np.hstack((combine_list,result_list))
    #clean_df=combine_df[['合约代码','date']]
    #clean_df=clean_df.rename(columns={'合约代码':'code'})
    #测试3
    #load_path2='D:/整理日频数据/check_result/'+dce_kind.upper()+'_detail.csv'
    #check_df=pd.read_csv(load_path2)

    #生成check list
    parent_path='D:/日频数据按日期整理/'+dce_kind
    file_pool=os.listdir(parent_path)
    contract_file_name_pattern=re.compile(r'(.+)_(.+)_(.+)\.csv$')
    date_list=[]
    contract_code_list=[]
    for index in range(len(file_pool)):
        ret=re.search(contract_file_name_pattern,file_pool[index])
        if ret:

            contract_date=ret.group(2)
            contract_code=ret.group(3)
      
            date_list.append(contract_date)
            contract_code_list.append(contract_code)

    #print(contract_index,contract_date,contract_code)

    check_df=pd.DataFrame({'date':date_list,'code':contract_code_list})
    check_list=(check_df['date']+check_df['code']).tolist()
    lack_list=[x for x in combine_list if x not in check_list]
    test_df=pd.DataFrame(lack_list)
    date_array=np.array((test_df[0].str.findall(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')).tolist()).reshape(-1,)
    contract_array=np.array((test_df[0].str.findall(r'[a-zA-Z]+.+')).tolist()).reshape(-1,)
    result_lack_df=pd.DataFrame({'date':date_array,'contract_code':contract_array})

    lack_date_list=result_lack_df['date'].unique()
    combine_target_list=[]
    for date in lack_date_list:
        target_df=result_lack_df.loc[result_lack_df['date']==date]
        target_array=target_df['contract_code'].values.tolist()
        combine_target_list.append(target_array)
        #combine_target_l
    final_result_df=pd.DataFrame({'date':lack_date_list,'lack contract':combine_target_list})
    final_result_df.to_csv('D:/整理日频数据/check_result/lack_contract/'+dce_kind.upper()+'_lack_contract.csv',index=False)