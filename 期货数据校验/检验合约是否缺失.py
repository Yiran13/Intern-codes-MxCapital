# 问题:原先版本的持仓量排名和手动做出来的不一样
# 分析：经过检验，持仓量差值部分正好是一张（缺失）合约的持仓量，该合约即在批量下载中，又在网页中，不知道到什么原因缺失了（解压出了问题？）
# 解决：
#       step1:利用原先抓取到的日频数据，确定每日的品种对应的合约
#       step2:对做好的持仓量数据，扫描每个品种，确定每日处理了那些合约
#       step3：对比找出，那些合约在那些交易日是有问题的
import pandas as pd
import numpy as np 
import os
import pickle
def save_pickle(save_path:str,file_name:str,data):

    with open (save_path+file_name+'.pickle','wb') as f:
        pickle.dump(data,f)

def read_pickle(save_path:str,file_name:str):

    with open (save_path+file_name+'.pickle','rb') as f:
        data=pickle.load(f)
    return data
contract_list=['A','C','CS','I','J','JD','JM','L','M','P','PP','V','Y']
lack_contracts=[]
for contract_type in contract_list:
# contract_type='A'
    refer_df=pd.read_csv('D:/大商所日频数据抓取/clean_汇总数据.csv')
    # =================================================2016-2019===========================
    # 把所有数据分解成 合约品种 和 交易日期两列的dataframe,2016-2019数据
    file_pool_2016_2019=os.listdir('D:/contract_hold_dce_2016_2019/'+contract_type+'/long/')
    # 利用pandas 处理str
    file_pool_2016_2019=pd.Series(file_pool_2016_2019)
    file_pool_2016_2019=file_pool_2016_2019.loc[file_pool_2016_2019.str.match(r'[A-Z]+[0-9]+')]
    one_check_df=pd.DataFrame({'contract_code':file_pool_2016_2019.str.findall(r'[A-Z]+[0-9]+').str[0].values,
    'date':file_pool_2016_2019.str[-12:-4].values})
    one_check_df.sort_values('date',inplace=True)

    # ===================================================2010-2016=========================
    # 2010-2016数据部分
    file_pool_2010_2016=os.listdir('D:/dce_2010_2016/'+contract_type+'/long/')
    file_pool_2010_2016=pd.Series(file_pool_2010_2016)
    file_pool_2010_2016=file_pool_2010_2016.loc[file_pool_2010_2016.str.match(r'[A-Z]+[0-9]+')]
    one_check_df2=pd.DataFrame({'contract_code':file_pool_2010_2016.str.findall(r'[A-Z]+[0-9]+').str[0].values,
    'date':file_pool_2010_2016.str[-12:-4].values})
    one_check_df2.sort_values('date',inplace=True)
    # ===========================================生成合并数据=================================
    one_check_df_combine=pd.concat([one_check_df,one_check_df2])
    one_check_df_combine.sort_values('date',inplace=True)
    check_dict=dict()
    check_date_pool=one_check_df_combine['date'].unique()
    for date in check_date_pool:
        if int(date)<=20191031:
            slice_df=one_check_df_combine.loc[one_check_df_combine['date']==date]
            contract_code_pool=slice_df['contract_code'].values.tolist()
            check_dict[date]=contract_code_pool

    # =====================================生成refer合并数据==========================
    # file_pool_2016_2019.str.findall(r'[A-Z]+[0-9]+').str[0]
    # file_pool_2016_2019.str[-12:-4]
    refer_df['contract_kind']=refer_df['contract_code'].str.findall(r'[A-Z]+').str[0]
    # 以2W为标准进行筛选
    # 这个标准是持仓量
    refer_df=refer_df.loc[refer_df['toi']>20000]
    one_refer_df=refer_df.loc[refer_df['contract_kind']==contract_type]
    one_refer_df['new_date']=one_refer_df['date'].str.replace('-','')
    refer_dict=dict()
    refer_date_pool=one_refer_df['new_date'].unique()
    for date in refer_date_pool:
        slice_df=one_refer_df.loc[one_refer_df['new_date']==date]
        contract_code_pool=slice_df['contract_code'].values.tolist()
        refer_dict[date]=contract_code_pool
    lack_dict=dict()
    for date in refer_dict:
        refer_pool=refer_dict[date]
        check_pool=check_dict[date]
        lack_pool=[x for x in refer_pool if x not in check_pool]
        if lack_pool:
            lack_dict[date]=lack_pool
    if len(lack_dict)==0:
        print(f'{contract_type} 没有缺失')
    else:
        print(f'{contract_type} 缺失 {len(lack_dict)}')
        with open ('D:/大商所合约缺失/'+contract_type+'.pickle','wb') as f:
            pickle.dump(lack_dict,f)
# 所有的品种都是？还是只存在个别现象？
lack_files=os.listdir('D:/大商所合约缺失/')
lack_contracts=pd.Series(lack_files).str[:-7].values
combine_df=pd.DataFrame()
for lack_contract in lack_contracts:
    print(lack_contract)
    lack_dict=read_pickle('D:/大商所合约缺失/',lack_contract)
    date_list=[x for x in lack_dict]
    contract_record=[lack_dict[x] for x in lack_dict]
    one_df=pd.DataFrame({'date':date_list,'contract_list':contract_record})
    combine_df=pd.concat([combine_df,one_df])
# 找到缺失的时间轴，全覆盖到原始的文件中
# 然后再次运行文件。检查是否还有问题
lack_dates=combine_df['date'].unique()
for date in lack_dates[16:]:
    get_data_from_dce(date,'D:/大商所缺失数据下载/','D:/大商所缺失数据clean/')
# 补上最后一个交易代码