import pandas as pd
import numpy as np 
import os
import re
import requests
import xlwt
import os
import json
import tushare as ts
# 提取交易时间
df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
#由于在上一次抓取中已经保存了json数据，所以直接解析json就可以

result_list=[]
contract_code_list=[]
record_date_list=[]
volume_list=[]
close_list=[]
settle_list=[]
open_list=[]
low_list=[]
high_list=[]
oi_list=[]

for one_date in whole_date_list:

    one_date=one_date.replace('-','')
    url='http://www.shfe.com.cn/data/dailydata/kx/kx'+one_date+'.dat'
    # 存放路径，可以修改
    root = 'D:/上期所日频数据抓取/'
    if not os.path.exists(root):
        os.makedirs(root)
    path = root + 'SH'+'_'+one_date #指定下载的目录,保存为txt文件
  
    file = open(path+'.txt', 'r', encoding='UTF-8')
    js = file.read()
    dic = json.loads(js)
    file.close()
    result_dict=dic['o_curinstrument']
    #进行解析json

    for result in result_dict:
        
        if result['DELIVERYMONTH']!='小计' and result['PRODUCTID'] !='总计':
            #二次筛选,合计/小计/总计改的太多了
            if result['CLOSEPRICE']:

            
                    contract_name=result['PRODUCTID']
                    contract_name=contract_name.replace(' ','').replace('_f','')
                    deliver_month=result['DELIVERYMONTH']
                    close_price=result['CLOSEPRICE']
                    open_price=result['OPENPRICE']
                    low_price=result['LOWESTPRICE']
                    high_price=result['HIGHESTPRICE']
                    oi=result['OPENINTEREST']
                    volume=result['VOLUME']
                    settle_price=result['SETTLEMENTPRICE']
                    type_date=one_date[:4]+'-'+one_date[4:6]+'-'+one_date[6:]
                    contract_code=contract_name+deliver_month
                    contract_code_list.append(contract_code)
                    record_date_list.append(type_date)
                    volume_list.append(volume)
                    settle_list.append(settle_price)
                    close_list.append(close_price)
                    high_list.append(high_price)
                    low_list.append(low_price)
                    open_list.append(open_price)
                    oi_list.append(oi)
                    #final_result=one_date+contract_code
                    #result_list.append(final_result)
                
    print(f'{one_date} finished')
    


combine_df=pd.DataFrame({'date':record_date_list,"contract_code":contract_code_list,'topen':open_list,'thigh':high_list,'tlow':low_list,
'tclose':close_list,'tvolume':volume_list,'tsettle':settle_list,'toi':oi_list})
combine_df['contract_code']=combine_df['contract_code'].str.upper()

#加入contract_type 列,利用list 操作再合成dataframe
pattern=re.compile(r'[a-zA-Z]+')
contract_type_list=[]
for contract in contract_code_list:


    result=re.findall(pattern,contract)

    if result:
            contract_type_list.append(result[0])
    else:
        print(contract)
#出了个bug，当str函数使用有问题的时候，总计1，总计2 出来了

drop_row=combine_df.loc[(combine_df['contract_code']=='总计1')|(combine_df['contract_code']=='总计2')].index
combine_df.drop(drop_row,inplace=True)
contract_code_list=combine_df['contract_code'].tolist()

#再次运行
pattern=re.compile(r'[a-zA-Z]+')
contract_type_list=[]
for contract in contract_code_list:


    result=re.findall(pattern,contract)

    if result:
            contract_type_list.append(result[0])
    else:
        print(contract)
#现在都是品种,没有掺杂其他数据
combine_df['contract_type']=contract_type_list
volume_0_list=combine_df.loc[combine_df['tvolume']==0].index.tolist()
for col_index in [2,3,4]:
    combine_df.iloc[volume_0_list,col_index]=combine_df.iloc[volume_0_list,5]

combine_df.to_csv('D:/上期所日频数据抓取/clean_汇总数据.csv',index=False)
# combine_df.to_csv('D:/shse_每日交易合约/日频数据总汇.csv',index=False)














# # 监测数目的变化
# #生成两个dict 来存放 变化的和没有变化的
# no_change_dict=dict()
# have_change_dict=dict()
# shfe_kind_pool=['cu','al','zn','pb','ni','sn','rb','hc','bu','au','ag','ru','wr','ss','sc','fu','nr','sp']
# for shfe_kind in shfe_kind_pool:
#     slice_df=combine_df.loc[combine_df['contract_type']==shfe_kind]
#     num_df=slice_df.groupby('record_date')['contract_code'].count()
#     change_num=num_df.unique()
#     if len(change_num)>1:
#         have_change_dict[shfe_kind]=change_num.tolist()
#     elif len(change_num)==1:
#         no_change_dict[shfe_kind]=change_num.tolist()
#     print(f'{shfe_kind} finished')






#处理 have_change_dict
#具体处理，将date 分为年 月 ，对年和月再分别统计
for shfe_kind,num_list in have_change_dict.items():

    slice_df=combine_df.loc[combine_df['contract_type']==shfe_kind]
    count_array=slice_df.groupby('record_date')['contract_code'].count().values
    date_array=slice_df['record_date'].unique()
    check_df=pd.DataFrame({'record_date':date_array,
    'num':count_array})
    num_pool=check_df['num'].unique().tolist()
    
    for num in num_pool:
        num_df=check_df.loc[check_df['num']==num]
        check_date_array=pd.to_datetime(num_df['record_date'])
        #确保月份是不是连续的就行了
        path='D:/统计每个品种个数/'
        if not os.path.exists(path):
            os.makedirs(path)
        check_date_array.to_csv(path+shfe_kind+'_'+str(num)+'.csv',index=False)

        
 
have_change_dict
   


for shfe_kind,num_list in have_change_dict.items():
    slice_df=combine_df.loc[combine_df['contract_type']==shfe_kind]
    slice_df['year']=pd.to_datetime(slice_df['record_date'],format='%Y%m%d').dt.strftime('%Y')
    slice_df['month']=pd.to_datetime(slice_df['record_date'],format='%Y%m%d').dt.strftime('%m')
    slice_df['year_month']=pd.to_datetime(slice_df['record_date'],format='%Y%m%d').dt.strftime('%Y%m')

  
    
    count_array=slice_df.groupby('record_date')['contract_code'].count().values
    date_array=slice_df['record_date'].unique()
    check_df=pd.DataFrame({'record_date':date_array,
    'num':count_array})
    num_pool=check_df['num'].unique().tolist()
    check_df['year']=pd.to_datetime(check_df['record_date'],format='%Y%m%d').dt.strftime('%Y')
    check_df['month']=pd.to_datetime(check_df['record_date'],format='%Y%m%d').dt.strftime('%m')
    #其实是天数。。。
    year_month_num=check_df.groupby(['year','month'])['num'].count()
    break




















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
   
    





