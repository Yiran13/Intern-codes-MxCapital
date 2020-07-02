import pandas as pd
import numpy as np
import os
import re
# =================================================开始=================================
df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
# =========================================获取完整的样本时间20100104-20191031===========
df=pd.read_csv('D:/DIY_主力合约/'+'IF'+'主力连续.csv')
df['time']=df['时间'].str[10:].str.strip()
df['date']=df['时间'].str[:10]
time_0916_1515=df.loc[df['date']=='2010-04-16']['time'].values.tolist()
# ========================================设置交易时间段0916-1515=========================

contract_kind='IF'
# ======================================品种设置========================================
contract_type=contract_kind
type_pattern=re.compile(r'\.[A-Z]+')
contract_pattern=re.compile(r'[A-Z]+[0-9]+')
#通过ywhole_yearlist 取
#year_pattern=re.compile(r'[0-9]+')
row_1=['市场代码','合约代码',	'时间',	'开','高',	'低',	'收',	'成交量',	'成交额',	'持仓量']
path='D:/主力合约/新建文件夹/'+contract_kind+'_1day_main.npy'
error_date=[]
error_code=[]
error_term=[]
#加载主力合约
data=np.load(path)
data=data.reshape(-1)
#开始按照index,读取
orignal_data=[]
orignal_data.append(row_1)
for index in range(data.shape[0]):

#    if data[index]=='None':
#        row=['']
#        orignal_data.append(row)
#    else:
    if  data[index]!='None':

        contract_name=data[index]
        market_type=re.findall(type_pattern,contract_name)[0][1:]
        contract_code=re.findall(contract_pattern,contract_name)[0]
        #读取对应的数据存放的路径
        year=whole_date_list[index][:4]
        date=whole_date_list[index]

#         readfile_path='D:/1分钟数据/FutAC_Min1_Std_'+year+'/'
        readfile_path='C:/Users/lenovo/Documents/WeChat Files/yiranli13/FileStorage/File/2019-12/FutSF_Min1_Std_'+year+'/'
        file_name=contract_code+'.csv'
        if market_type=='CZC':
            file_name=contract+contract_code[-3:]+'.csv'
        #contract_code是大写 但有的文件名是小写，所以加一层逻辑判断
        if file_name not in os.listdir(readfile_path):
            file_name=file_name.lower()
        full_file_path=readfile_path+file_name
        if os.path.exists(full_file_path):
            df=pd.read_csv(full_file_path,encoding='gbk')
            df['date']=df['时间'].str[:10]
            result=df.loc[df['date']==date]
            if result.shape[0]>0:
                for row_index in range(len(result)):
                    target_row=result.iloc[row_index].tolist()
                    clean_row=target_row[:-1]
                    orignal_data.append(clean_row)
                print(f'{contract_kind} {date} finished!')
            else:
                print(f'{contract_kind} {date} no data!')
                error_date.append(whole_date_list[index])
                error_term.append('lack data in the date')
                error_code.append(contract_code)
                
                

                
    else:
            error_date.append(whole_date_list[index])
            error_term.append('no csv file in the data source')
            error_code.append(contract_code)
            
print(f'{contract_kind} 主力合约数据读取完成')
final_df=pd.DataFrame(orignal_data[1:],columns=orignal_data[0])
final_df.to_csv('D:/DIY_主力合约/'+contract_kind+'主力连续.csv',index=False,encoding='utf-8-sig')
# =========================================完成读取任务=====================================
# 填充
combine_df=pd.read_csv('D:/DIY_主力合约/'+contract_kind+'主力连续.csv')
contract_type=contract_kind
combine_df['date']=combine_df['时间'].str[:10]
combine_df['time']=combine_df['时间'].str[10:].str.strip()
combine_df['时间']=combine_df['date']+' '+combine_df['time']
combine_df=combine_df.sort_values('时间')
combine_df['合约代码']=combine_df['合约代码'].str.upper()
combine_df=combine_df.sort_values('时间')
combine_all_df=pd.DataFrame()

for date_index in range(len(whole_date_list)):

    #按日期进行分割
    target_df=combine_df.loc[combine_df['date']==whole_date_list[date_index]]
    #分割到的长度放入容器中
    target_num=len(target_df)
    #理论长度
    theory_num=len(time_0916_1515)
    if target_num>0:
        #小于理论长度开始填充
        
      
        have_time=target_df['time'].values.tolist()
        lack_time=[x for x in time_0916_1515 if x not in have_time]
        #检查有没有空的时间
        if lack_time:
             print(f'{whole_date_list[date_index]} 不连续')
        #一共12列，先全部填充nan的时候，最后再把已知填入
        insert_array=np.empty(shape=(len(lack_time),12))
        insert_array.fill(np.nan)
        insert_df=pd.DataFrame(insert_array,columns=['市场代码','合约代码','时间','开','高','低','收','成交量','成交额','持仓量','date','time'])
        insert_df['date']=whole_date_list[date_index]
        insert_df['time']=lack_time
        #缺少时间的个数小于time_0916_1515则说明，当天并不是完全没数据,则对主力合约进行填充
        if len(lack_time)<len(time_0916_1515):
             
            insert_df['合约代码']=target_df['合约代码'].unique()[-1]
        combine_insert_df=pd.concat([target_df,insert_df])
        combine_all_df=pd.concat([combine_all_df,combine_insert_df])
            
        

    else:
        print(f'{whole_date_list[date_index]}empty ')
        lack_time=[x for x in time_0916_1515]
        #一共12列，先全部填充nan的时候，最后再把已知填入
        insert_array=np.empty(shape=(len(lack_time),12))
        insert_array.fill(np.nan)
        insert_df=pd.DataFrame(insert_array,columns=['市场代码','合约代码','时间','开','高','低','收','成交量','成交额','持仓量','date','time'])
        insert_df['date']=whole_date_list[date_index]
        insert_df['time']=lack_time
        combine_all_df=pd.concat([combine_all_df,insert_df])
combine_all_df['时间']=combine_all_df['date']+' '+combine_all_df['time']
#调整时间
combine_all_df=combine_all_df.sort_values('时间')
combine_all_df.reset_index(inplace=True)
stop_point=combine_all_df.loc[(combine_all_df['date']=='2019-10-31')&(combine_all_df['time']=='15:15:00')].index[0]
combine_all_df=combine_all_df.iloc[:stop_point+1]
combine_all_df=combine_all_df[['市场代码', '合约代码', '时间', '开', '高', '低', '收', '成交量', '成交额', '持仓量','date','time']]
combine_all_df.to_csv('D:/1_min_补充品种/'+contract_kind+'主力连续.csv',index=False,encoding='utf-8-sig')
# ====================================================================完成检查，填充===================================================
combine_df=pd.read_csv('D:/1_min_补充品种/'+contract_kind+'主力连续.csv')
contract_type=contract_kind

combine_df['date']=combine_df['时间'].str[:10]
combine_df['time']=combine_df['时间'].str[10:].str.strip()
combine_df['时间']=combine_df['date']+' '+combine_df['time']
combine_df=combine_df.sort_values('时间')
combine_df['合约代码']=combine_df['合约代码'].str.upper()
combine_df=combine_df.sort_values('时间')
end_index=np.where(combine_df['time']=='15:15:00')[0]+1
end_index=np.hstack(([0],end_index))
start=end_index[:-1]
end=end_index[1:]
#col_type='开'
col_type_list=['开','高','低','收','成交量','成交额','持仓量']
dir_name_list=['open','high','low','close','volume','amount','position']
have_night=True
#交易到凌晨01
#merge_df=pd.DataFrame({'time':with_night_01})
#交易到凌晨0230,version中没有集合竞价时间，time_0916_1515去掉9：00，21：00
merge_df=pd.DataFrame({'time':time_0916_1515})
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
        
        #交易到凌晨1点
        #csv_df=pd.DataFrame(combine_list,columns=with_night_01,index=date_index_list)
        #交易到凌晨2点30
        csv_df=pd.DataFrame(combine_list,columns=time_0916_1515,index=date_index_list)
        csv_df['main_contract_code']=main_contract_list
        
        #交易到凌晨2点30
        if date_index_list[0] !='2010-01-04':
            print(f'{contract_type} starts at {date_index_list[0]} ')

            lack_date_list=[x for x in whole_date_list if x not in date_index_list]
            #将不足的时间补为NA
            wholetime_df=pd.DataFrame(index=lack_date_list,columns=time_0916_1515)
            wholetime_df['main_contract_code']=np.nan

            csv_df=pd.concat([wholetime_df,csv_df])


        save_path='D:/1_min_补充品种/'
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        #np.save(save_path+contract_type+'.npy',combine_list)
        csv_df.to_csv(save_path+contract_type.upper()+'_1min_'+dir_name_list[index]+'.csv')
        print(f'{contract_type} {dir_name_list[index]} finished')
#    