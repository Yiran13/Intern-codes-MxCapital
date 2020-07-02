import pandas as pd
import numpy  as np
load_path='D:/期限结构原始数据/clean_dce.csv'
# load_path='D:/上期所日频数据抓取/日频数据总汇_2.csv'
# load_path='D:/郑交所日频数据抓取/日频数据汇总_czce.csv'
save_path='D:/期限结构新/'

load_path_dce='D:/大商所日频数据抓取/日频数据汇总_dce.csv'
df_base=pd.read_csv(load_path_dce)
whole_date_list=df_base.loc[df_base['contract_type']=='A']['record_date'].unique().tolist()

df=pd.read_csv(load_path)
df['contract_type']=df['contract_code'].str.findall(r'[A-Z]+').str[-1]
df['contract_type']=df['contract_type'].str.upper()
df['contract_code']=df['contract_code'].str.upper()
data_type_pool=['close','settle','contract_code']
df['contract_type']=df['contract_type'].str.replace('RO','OI')
df['contract_code']=df['contract_code'].str.replace('RO','OI')
# 替换旧合约名称后，再次处理
# contract_type_pool=['CU','ZN','AL','RB','NR','WR','PB','HC','BU','AU','AG','WR','FI','RU','SP','NI','SN']
# contract_type_pool=['CF','AP','CY','CJ','FG','MA','OI','RM','SF','SM','SR','TA','UR','ZC']
contract_type_pool=['A','B','CS','C','EB','EG','I','JD','JM','J','L','M','PP','P','V','Y']
# data_type='contract_code'
# contract_type='CS'
for contract_type in contract_type_pool:
    for data_type in data_type_pool:
        
        combine_list=[]
        for date_index in range(len(whole_date_list)):
        #date_index=0
            contract_df=df.loc[df['contract_type']==contract_type]
            slice_df=contract_df.loc[contract_df['record_date']==whole_date_list[date_index]]
            value_array=slice_df[data_type].values
            #value_array=value_array.reshape(1,1,1,-1)
            combine_bool=True
            if len(slice_df)==20:

                combine_bool=False
            elif len(slice_df)==0:
                empty_array=np.empty(shape=(20))
                empty_array.fill(np.nan)
                


            elif len(slice_df)<20:
                diff_length=20-len(slice_df)
                empty_array=np.empty(shape=(diff_length))
                empty_array.fill(np.nan)


            else:
                print(f'需要检查,{contract_type}在{whole_date_list[date_index]}合约数量超过12张')
            if combine_bool==True:
                value_array=np.hstack((value_array,empty_array))
            value_array=value_array.reshape(1,1,1,20)
            combine_list.append(value_array)
            print(f'{contract_type} {whole_date_list[date_index]} {data_type}完成')
           
        clean_array=np.array(combine_list).reshape(-1,1,1,20)
    
        np.save(save_path+contract_type+'_'+data_type+'.npy',clean_array)

