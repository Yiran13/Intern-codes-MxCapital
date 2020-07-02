import pandas as pd  
import numpy as np 
import os  
import re
import csv
import pickle
def read_pickle(save_path:str,file_name:str):

    with open (save_path+file_name+'.pickle','rb') as f:
        data=pickle.load(f)
    return data
#需要的第1个全局变量
with_night_0230=read_pickle('D:/','trading_hours')
#需要的第二个全局变量
df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
#用于分割出时间使用,
def get_date(x):
    return x[:10]
def get_time(x):
    return x[11:]
def classify_time_period(year:str,contract_type:str):
    test=pd.read_csv('D:/1分钟数据/FutAC_Min1_Std_'+year+'/'+contract_type+'主力连续.csv',encoding='gbk')
    test_time=test['时间']
    result=test_time.apply(get_date)
    record_date=[]
    record_num=[]
    for date in result.unique():
        num=len(result.loc[result==date])
        record_num.append(num)
        record_date.append(date)
    return record_date,record_num
#写csv函数
def write_csv(save_path:str,file_name:str,data:list):
    '''
    将嵌套为list的数据data 照row逐个写入
    '''
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    #创造一个csv在指定路径下
    csv_file=open(save_path+file_name,'w',newline='',encoding='utf-8-sig')
    #创造一个writer函数
    writer=csv.writer(csv_file)
    for row in data:
        writer.writerow(row)
    csv_file.close()
def parse_main_contract(save_file_path:str,contract_type:str):
    '''
    需要先加载withnigh0230
    '''
    test=pd.read_csv(save_file_path)
    test['time']=test['时间'].apply(get_time)
    test['date']=test['时间'].apply(get_date)  
    #原先的combine_df加过集合竞价的时间，这个版本取消所以之间赋值
    combine_df=test
    combine_df=combine_df.sort_values('时间')
    end_index=np.where(combine_df['time']=='15:00:00')[0]+1
    end_index=np.hstack(([0],end_index))
    start=end_index[:-1]
    end=end_index[1:]
    #col_type='开'
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
            
            #交易到凌晨1点
            #csv_df=pd.DataFrame(combine_list,columns=with_night_01,index=date_index_list)
            #交易到凌晨2点30
            csv_df=pd.DataFrame(combine_list,columns=with_night_0230,index=date_index_list)
            csv_df['main_contract_code']=main_contract_list
            #break
            #交易到凌晨2点30
            if date_index_list[0] !='2010-01-04':
                print(f'{contract_type} starts at {date_index_list[0]} ')

                lack_date_list=[x for x in whole_date_list if x not in date_index_list]
                #将不足的时间补为NA
                wholetime_df=pd.DataFrame(index=lack_date_list,columns=with_night_0230)
                wholetime_df['main_contract_code']=np.nan

                csv_df=pd.concat([wholetime_df,csv_df])
            

            save_path='D:/DIY_主力合约/1_min_data/'
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            #np.save(save_path+contract_type+'.npy',combine_list)
            csv_df.to_csv(save_path+contract_type.upper()+'_1min_'+dir_name_list[index]+'.csv')
            print(f'{contract_type} {dir_name_list[index]} finished')


def handle_one_contract(contract_kind:str):
    type_pattern=re.compile(r'\.[A-Z]+')
    contract_pattern=re.compile(r'[A-Z]+[0-9]+')
    #通过ywhole_yearlist 取
    #year_pattern=re.compile(r'[0-9]+')
    row_1=['市场代码','合约代码',	'时间',	'开','高',	'低',	'收',	'成交量',	'成交额',	'持仓量']
    path='D:/主力合约/新建文件夹/'+contract_kind+'_1day_main.npy'

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
            year=whole_date_list[index][:4]
            date=whole_date_list[index]
            readfile_path='D:/1分钟数据/FutAC_Min1_Std_'+year+'/'
            file_name=contract_code+'.csv'
            if market_type=='CZC':
                file_name=contract+contract_code[-3:]+'.csv'
            #contract_code是大写 但有的文件名是小写，所以加一层逻辑判断
            if file_name not in os.listdir(readfile_path):
                file_name=file_name.lower()
            full_file_path=readfile_path+file_name
            df=pd.read_csv(full_file_path,encoding='gbk')
            df['date']=df['时间'].apply(get_date)
            result=df.loc[df['date']==date]
            for row_index in range(len(result)):
                target_row=result.iloc[row_index].tolist()
                clean_row=target_row[:-1]
                orignal_data.append(clean_row)
            print(f'{contract_kind} {date} finished!')
    print(f'{contract_kind} 主力合约数据读取完成')
    save_file_path='D:/DIY_主力合约/'+contract_kind+'主力连续.csv'
    write_csv('D:/DIY_主力合约/',contract_kind+'主力连续.csv',orignal_data)
    parse_main_contract(save_file_path,contract_kind)

#从ru开始
contract_0100pool=['FU']
for contract in contract_0100pool:
    contract=contract.upper()
    handle_one_contract(contract)


#修改MA TC
file_path='D:/DIY_主力合约/1_min_data/'
contract_type='ZC'
last_part_list=['_1min_close.csv','_1min_high.csv','_1min_low.csv','_1min_open.csv','_1min_position.csv']
file_path_pool=[file_path+contract_type+last_part for last_part in last_part_list]
for one_file in file_path_pool:
    df=pd.read_csv(one_file)
    df['main_contract_code']=df['main_contract_code'].str.replace('ME','MA').str.replace('TC','ZC')
    df.to_csv(one_file,index=False)


contract_kind='FU'
type_pattern=re.compile(r'\.[A-Z]+')
contract_pattern=re.compile(r'[A-Z]+[0-9]+')
#通过ywhole_yearlist 取
#year_pattern=re.compile(r'[0-9]+')
row_1=['市场代码','合约代码',	'时间',	'开','高',	'低',	'收',	'成交量',	'成交额',	'持仓量']
path='D:/主力合约/新建文件夹/'+contract_kind+'_1day_main.npy'
error_date=[]
error_code=[]
error_term=[]
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
        year=whole_date_list[index][:4]
        date=whole_date_list[index]
        readfile_path='D:/1分钟数据/FutAC_Min1_Std_'+year+'/'
        file_name=contract_code+'.csv'
        if market_type=='CZC':
            file_name=contract+contract_code[-3:]+'.csv'
        #contract_code是大写 但有的文件名是小写，所以加一层逻辑判断
        if file_name not in os.listdir(readfile_path):
            file_name=file_name.lower()
        full_file_path=readfile_path+file_name
        if os.path.exists(full_file_path):
            df=pd.read_csv(full_file_path,encoding='gbk')
            df['date']=df['时间'].apply(get_date)
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
save_file_path='D:/DIY_主力合约/'+contract_kind+'主力连续.csv'
write_csv('D:/DIY_主力合约/',contract_kind+'主力连续.csv',orignal_data)
#B解决方法就对于缺失部分，先用对应主力合约的时间去制作空的df，如果还差一些，自己手动想办法。
#OI 可以做一个新main.py然后合一起
#郑州的文件 直接拿来替换，处理的df，构造出一个date 然后date和main的date merge 或者直接换后面的字

