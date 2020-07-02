import pandas as pd
import os
import numpy as np
df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
whole_date_array=np.array(whole_date_list)
# need_to_handle=['B','CS','C','EB','EG','I','JD','JM','J','L','M','P','PP','V','Y',
# 'CF','AP','CY','FG','MA','OI','RM','SF','SM','TA']
# dce_path='D:/大商所日频数据抓取/日频数据汇总_dce.csv'
# df=pd.read_csv(dce_path)
# df_pool=df['contract_type'].unique()
# need_to_work=[x for x in need_to_handle if x in df_pool]
#大商所需要处理
# ['B',
#  'CS',
#  'C',
#  'I',
#  'JD',
#  'JM',
#  'J',
#  'L',
#  'M',
#  'P',
#  'PP',
#  'V',
#  'Y']


#缺失X
# tuple_list=[('J','2011-08-15'),('L','2010-01-04'),('M','2010-01-04'),('P','2010-01-04'),('V','2010-01-04'),('Y','2010-01-04')]
#缺失CF 
# czce_path='D:/郑交所日频数据抓取/日频数据汇总_czce.csv'
# df=pd.read_csv(czce_path)
# ('AP','2018-03-15'),('CY','2017-12-15'),('FG','2013-02-22'),
#tuple_list=[('MA','2012-02-15'),('OI','2013-05-16'),('RM','2013-03-15'),('SF','2014-10-22'),('SM','2014-10-22'),('TA','2010-01-04')]
# tuple_list=[('PP','2014-04-16'),('P','2010-01-04')]
# tuple_list=[('CF','2012-12-25')]
shfe_path='D:/shse_每日交易合约/日频数据总汇_2.csv'
df=pd.read_csv(shfe_path)
df['contract_code']=df['contract_code'].str.upper()
df['contract_type']=df['contract_type'].str.upper()
df.sort_values('contract_code',inplace=True)
# ('RU','2010-01-04'),('SP','2019-05-16'),
# ('AG','2012-08-16'),('BU','2014-01-16'),('PB','2011-08-16'),('CU','2010-01-04'),('ZN','2010-01-04')('AL','2010-01-04')('RB','2010-01-04')
# ('AG','2012-08-16'),('BU','2014-01-16'),('PB','2011-08-16'),
tuple_list=[('CU','2010-01-04'),('ZN','2010-01-04'),('AL','2010-01-04'),('RB','2010-01-04')]
for tuple_set in tuple_list:
    contract_slice,start_date=tuple_set
    contract_df=df.loc[df['contract_type']==contract_slice]
    slice_date_list=pd.Series(whole_date_list[(np.where(whole_date_array==start_date))[0][0]:]).str.replace('-','').values.tolist()

    contract_df['deliver_time']=(contract_df['contract_code'].str[-4:]).astype(float)
    contract_df['date']=pd.to_datetime(contract_df['record_date'],format='%Y%m%d')
    contract_df.set_index('date',inplace=True)
    contract_df['record_date']=contract_df['record_date'].astype(str)
    start_index=whole_date_list[(np.where(whole_date_array==start_date))[0][0]]
    end_index=whole_date_list[-1]
    contract_df=contract_df[start_index:end_index]
    #check 
    #有没有什么更好的方法
    contract_num_list=contract_df.groupby('record_date')['contract_code'].count().unique().tolist()
  
    
    if len(contract_num_list)==1:
        print(f'the contract num of {contract_slice} is constant with {contract_num_list[0]}')
        combine_df=pd.DataFrame()
        for index in range(len(slice_date_list)):
            singel_df=contract_df.loc[contract_df['record_date']==slice_date_list[index]]
            #singel_df.sort_values('deliver_time',inplace=True)
            sort_indexs=np.argsort(singel_df['deliver_time'])
            index_list=[0 for x in range(len(sort_indexs))]
            for sort_index in range(len(sort_indexs)):
                index_list[sort_indexs[sort_index]]=sort_index
            singel_df['sort_index']=index_list
            # singel_df['sort_index']=np.argsort(singel_df['deliver_time'])
            singel_df.reset_index(inplace=True)
            singel_df=singel_df[['contract_code','record_date','close','sort_index']]
            combine_df=pd.concat([combine_df,singel_df])
            print(f'{slice_date_list[index]} finished')
        # for contract_deliver_index in range(len(contract_num_list)):
        #     contract_index_df=singel_df.iloc[contract_deliver_index]
        #     contract_index_df
        #     break
        
        sort_index_pool=combine_df['sort_index'].unique().tolist()
        for sort_index in sort_index_pool:
            sort_index_df=combine_df.loc[combine_df['sort_index']==sort_index]
            sort_index_df.sort_values('record_date',inplace=True)
            close_index_df=sort_index_df['close'].values.reshape(-1,1,1)
            name_index_df=sort_index_df['contract_code'].values.reshape(-1,1,1)
            record_date_df=sort_index_df['record_date'].values.reshape(-1,1,1)
            save_path='D:/远近合约日频数据/'+contract_slice
            np.save(save_path+'_'+str(sort_index)+'_close.npy',close_index_df)
            np.save(save_path+'_'+str(sort_index)+'_name.npy',name_index_df)
            np.save(save_path+'_'+str(sort_index)+'_date.npy',record_date_df)
            print(f'{contract_slice}{str(sort_index)} finished')
        
    else:
        print(f'check!!! something wrong with {contract_slice} with {contract_num_list}')




