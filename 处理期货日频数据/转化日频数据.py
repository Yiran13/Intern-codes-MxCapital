import pandas as pd 
import numpy as np 
import os
import re

df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
def get_date(x):
    return x[:10]




#需要改的地方，每个合约固定的数目不同需要改
#对应交易所要读取的路径不同
contract_kind_pool=['cu','al','zn','pb','ni','sn','rb','hc','bu','au','ag','ru','wr','ss','sc','fu','nr','sp']
#contract_kind='m'
for contract_kind in contract_kind_pool:
    contract_file_pattern=re.compile(r'^'+contract_kind+r'[0-9]+\.csv')
    #需要改对应交易所
    file_path='D:/日频数据/SC/'
    file_pool=os.listdir(file_path)
    target_file_list=[]

    def handle_not_czce(x):
        '''
        用于对合约进行排序
        '''
        return int(x[-4:])
    for target in file_pool:
        if re.findall(contract_file_pattern,target):
            #print(target)
            result=re.findall(contract_file_pattern,target)[0]
            target_file_list.append(result)
    combine_df=pd.DataFrame()
    for index in range(len(target_file_list)):
        full_file_path=file_path+target_file_list[index]
        df=pd.read_csv(full_file_path,encoding='gbk')
        df['时间']=pd.to_datetime(df['时间'])

        if (df['时间']>=pd.to_datetime('2010-01-01')).sum()>0:
            target_df=df.loc[df['时间']>=pd.to_datetime('2010-01-01')]
            combine_df=pd.concat([combine_df,target_df])

    #处理已经按照时间整理好的数据 注意数值是np.float64
    #注意这里是非郑州交易所版本
    for date in whole_date_list:
        #date='2010-01-04'
        data=combine_df.loc[combine_df['时间']==pd.to_datetime(date)]
        trade_num=data.shape[0]
        trade_contract_pool=data['合约'].tolist()
        contract_code_num=data['合约'].apply(handle_not_czce)
        #由小到大的顺序输出index
        contract_code_num_by_value_index=np.argsort(contract_code_num)
        #按日依次输出csv,类似于 日期_index_品种
        for row in range(trade_num):
            row_value=data.iloc[row].values.reshape(1,-1)
            col_value=data.iloc[row].index.tolist()
            row_df=pd.DataFrame(row_value,columns=col_value)
            #合约代码
            contract_signal=data.iloc[row]['合约']
            full_save_date_path='D:/整理日频数据/'+contract_kind+'/'
            if not os.path.exists(full_save_date_path):
                os.makedirs(full_save_date_path)
            row_df.to_csv(full_save_date_path+str(contract_code_num_by_value_index.iloc[row])+'_'+date+'_'+contract_signal+'.csv',encoding='utf-8-sig',index=False)

    #按照index 开始合并

    contract_index_list=['0','1','2','3','4','5','6','7','8','9','10','11']
    #contract_index_list=['0']
    file_list=os.listdir(full_save_date_path)
    
 
    for contract_index in contract_index_list:
        target_file_list=[]
        contract_index_pattern=re.compile(r'^'+contract_index+r'.+')
        if contract_index=='1':
            contract_index_pattern=re.compile(r'^[1]{1}_.+')
        for file_name in file_list:
            if re.findall(contract_index_pattern,file_name):
                result=re.findall(contract_index_pattern,file_name)[0]
                target_file_list.append(result)
    #开始 合并表格
        if target_file_list:
            concat_df=pd.DataFrame()
            print(f'{contract_index} for {contract_kind} found')

            for target_file in target_file_list:

                df=pd.read_csv(full_save_date_path+target_file)
                concat_df=pd.concat([concat_df,df])
            concat_df['时间']=concat_df['时间'].astype(str).apply(get_date)
            date_index_list=concat_df['时间'].tolist()
            lack_date_list=[x for x in whole_date_list if x not in date_index_list]
            if lack_date_list:
                #将不足的时间补为NA
                print(f'{concat_df.shape} shape 不足 {contract_kind} {contract_index}  ')
              
                wholetime_df=pd.DataFrame({'时间':lack_date_list})
                wholetime_df['合约']=np.nan
                concat_df=pd.concat([wholetime_df,concat_df])
               
                print(f'补完 nan 之后 shape: {concat_df.shape}') 


            #还有一个是有些日期的交易量不够
            

            concat_df.sort_values('时间',inplace=True)
            column_pool=['收盘价','今结算','合约']
            column_name=['close','settlement','contract_code']
            for index in range(len(column_pool)):
                column=column_pool[index]
                save_name=column_name[index]
                
                value_array=concat_df[column].values.reshape(2388,1,1)
                if index==2:
                    value_array.astype(str)
                
                np.save('D:/整理日频数据/'+contract_kind+'0'+contract_index+'_'+save_name+'.npy',value_array)
                print(f'{contract_kind} {index} {save_name}finished')
            
