save_file_path='D:/DIY_主力合约/BU主力连续.csv'
contract_type='BU'

test=pd.read_csv(save_file_path)
test['time']=test['时间'].apply(get_time)
test['date']=test['时间'].apply(get_date)
test['time']=test['time'].str.strip()
#原先的combine_df加过集合竞价的时间，这个版本取消所以之间赋值
combine_df=test
combine_df=combine_df.sort_values('时间')
end_index=np.where(combine_df['time']=='15:00:00')[0]+1
end_index=np.hstack(([0],end_index))
start=end_index[:-1]
end=end_index[1:]
col_type_list=['开','高','低','收','成交量','成交额','持仓量']
dir_name_list=['open','high','low','close','volume','amount','position']
have_night=True
#交易到凌晨01
#merge_df=pd.DataFrame({'time':with_night_01})
#交易到凌晨0230,version中没有集合竞价时间，with_night_0230去掉9：00，21：00
merge_df=pd.DataFrame({'time':with_night_0230})
if have_night:

    for index in range(len(col_type_list)):
        combine_list=[]
        col_type=col_type_list[index]
        date_index_list=[]
        main_contract_list=[]
        for s_index,e_index in zip(start,end):
            res=combine_df.iloc[s_index:e_index,:]
            combine=pd.merge(merge_df,res,how='outer')
            date_index=res.iloc[-1]['date']
            main_contract=res.iloc[-1]['合约代码']
            result_array=combine[col_type].values.tolist()
            combine_list.append(result_array)
            date_index_list.append(date_index)
            main_contract_list.append(main_contract)
        combine_list=np.array(combine_list)
        test_df=pd.DataFrame(combine_list,columns=with_night_0230,index=date_index_list)
        have_index=test_df.iloc[:,0].index.tolist()
        lack_date=[x for x in whole_date_list if x not in have_index]
        insert_array=np.empty(shape=(555))
        insert_array.fill(np.nan)
        test_df.reset_index(inplace=True)
        combine_df_2=pd.DataFrame()
        for date in lack_date:
            insert_df=pd.DataFrame(insert_array.reshape(1,555),columns=with_night_0230)
            insert_df['index']=date
            combine_df_2=pd.concat([combine_df_2,insert_df])

        combine_df_2=pd.concat([combine_df_2,test_df])
        combine_df_2.set_index('index',inplace=True)
        combine_df_2.sort_index(inplace=True)
        combine_df_2=combine_df_2[with_night_0230]
        main_contract=np.load('D:/主力合约/新建文件夹/'+contract_type+'_1day_main.npy')
        #不同交易所要进行替换后缀
        combine_df_2['main_contract_code']=(pd.Series(main_contract.reshape(-1)).str.replace('.SHF','')).str.replace('None','').values
        #(pd.Series(main_contract.reshape(-1)).str.replace('.CZC','')).values
        combine_df_2.to_csv('D:/DIY_主力合约纠错/'+contract_type+'_'+'1min_'+dir_name_list[index]+'.csv')
        print(f'{contract_type} {dir_name_list[index]} finished')
