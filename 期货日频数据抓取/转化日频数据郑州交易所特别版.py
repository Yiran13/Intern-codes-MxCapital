import pandas as pd 
import numpy as np 
import os
import re

df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
def get_date(x):
    return x[:10]
def handle_czce2(x):
    return x[-3:]
def handle_czce1(x):
    return x[:-3]




#需要改的地方，每个合约固定的数目不同需要改
#对应交易所要读取的路径不同
contract_kind_pool=['CF','SR','RM','TA','OI','FG','PM','WH','RI','LR','JR','RS','CY','AP','CJ','SF','SM','UR']
#contract_kind='m'
for contract_kind in contract_kind_pool:
    contract_file_pattern=re.compile(r'^'+contract_kind+r'[0-9]+\.csv')
    #需要改对应交易所
    file_path='D:/日频数据/ZC/'
    

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

    combine_df['last_part']=combine_df['合约'].apply(handle_czce2)
    combine_df['first_part']=combine_df['合约'].apply(handle_czce1)
    combine_df['year']=combine_df['时间'].dt.strftime('%Y')
    combine_df['first_num_code']='1'
    total_row=len(combine_df)
    for row in range(total_row):
        if combine_df.iloc[row,-4][0]=='0':
            if combine_df.iloc[row,-2]=='2019':
                combine_df.iloc[row,-1]='2'
    
    combine_df['合约']=combine_df['first_part']+combine_df['first_num_code']+combine_df['last_part']   
    combine_df=combine_df.iloc[:,:14]
    if contract_kind=='MA':
        combine_df['合约']=combine_df['合约'].str.replace('ME','MA')
    elif contract_kind=='ZC':
        combine_df['合约']=combine_df['合约'].str.replace('TC','ZC')

    #处理已经按照时间整理好的数据 注意数值是np.float64
    #注意这里是非郑州交易所版本,应为上面已经改好了可以使用
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


#改名模块
def change_file_name(save_path:str,file_name:str,change_to_name:str):
    '''
    修改文件的名字
    支持批量修改，反复调用即可
    原理即调用pathlib中的Path
    把字符串路径名变为Path（路径名)
    然后调用其rename属性
    '''
    file_path=Path(save_path+file_name)
    file_path.rename(save_path+change_to_name)



#需要改的地方，每个合约固定的数目不同需要改
#对应交易所要读取的路径不同
contract_kind_pool=['ME','TC']
#contract_kind='m'
for contract_kind in contract_kind_pool:
    contract_file_pattern=re.compile(r'^'+contract_kind+r'[0-9]+\.csv')
    #需要改对应交易所
    file_path='D:/日频数据/ZC/'
    

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
    for index in range(len(target_file_list)):
        file_name=target_file_list[index]
        print(f'change file name {file_name}')
        #设置对应合约代码
        sub_pattern=re.compile(contract_kind)
        if contract_kind=='ME':
            contract_kind_sub='MA'
        elif contract_kind=='TC':
            contract_kind_sub='ZC'
        change_to_name=re.sub(sub_pattern,contract_kind_sub,file_name)
        change_file_name(file_path,file_name,change_to_name)
        print(f'changed to {change_to_name}')

#将所有文件合约代码
load_file_path='D:/整理日频数据/'
file_pool=os.listdir(load_file_path)
contract_index_pattern1=re.compile('[a-zA-Z]+[0-9]+_')
contract_index_pattern2=re.compile('_.+')

for index in range(len(file_pool)):
    if re.findall(contract_index_pattern1,file_pool[index]):
        part1=re.findall(contract_index_pattern1,file_pool[index])[0][:-1].upper()
        part2=re.findall(contract_index_pattern2,file_pool[index])[0]
        change_to_name=part1+part2
        file_name=file_pool[index]
        change_file_name(load_file_path,file_name,change_to_name)
        print(f'{file_pool[index]} changed to',part1+part2)

#将所有合约的 代码改成大写
parent_path='D:/整理日频数据/'
file_pool=os.listdir(parent_path)
contract_file_name_pattern=re.compile(r'.+_contract_code.npy')
target_file_list=[]
for index in range(len(file_pool)):
    if re.findall(contract_file_name_pattern,file_pool[index]):
        result=re.findall(contract_file_name_pattern,file_pool[index])[0]
        target_file_list.append(result)
for index in range(len(target_file_list)):
    load_path='D:/整理日频数据/'+target_file_list[index]
    print(load_path)
    test=np.load(load_path,allow_pickle=True)
    test=pd.Series(test.reshape(-1,)).str.upper().values.reshape(2388,1,1)
    np.save(load_path,test)
#进行汇总统计
parent_path='D:/整理日频数据/'
file_pool=os.listdir(parent_path)
contract_index_name_pattern=re.compile(r'([A-Z]+[0-9]+)_.+')
target_file_list=[]
for index in range(len(file_pool)):
    ret=re.search(contract_index_name_pattern,file_pool[index])
    if ret:
        result=ret.group(1)
        target_file_list.append(result)
target_file_index=list(set(target_file_list))
df=pd.DataFrame({'contract_index':target_file_index})
def get_type(x):
    if x[:-2][-1]=='0':
        return x[:-3]
    else:
    
        return x[:-2]
df['contract_type']=df['contract_index'].apply(get_type)
df=df.groupby('contract_type')['contract_index'].count()
df.to_csv('D:/合约数量.csv')

#解析名字
contract_kind_pool=['au']
theory_num=8
for contract_kind in contract_kind_pool:
    parent_path='D:/日频数据按日期整理/'+contract_kind
    file_pool=os.listdir(parent_path)
    contract_file_name_pattern=re.compile(r'(.+)_(.+)_(.+)\.csv$')
    index_list=[]
    date_list=[]
    contract_code_list=[]
    for index in range(len(file_pool)):
        ret=re.search(contract_file_name_pattern,file_pool[index])
        if ret:

            contract_index=ret.group(1)
            contract_date=ret.group(2)
            contract_code=ret.group(3)
            index_list.append(contract_index)
            date_list.append(contract_date)
            contract_code_list.append(contract_code)
   
        #print(contract_index,contract_date,contract_code)
    
    df=pd.DataFrame({'index_num':index_list,'date':date_list,'code':contract_code_list})
    result_df=df.groupby('date')['code'].count()
    date_array=result_df.index.values
    value_array=result_df.values
    check_date_array=date_array[np.where(value_array!=theory_num)]
    check_value_array=value_array[np.where(value_array!=theory_num)]
    combine_df=pd.DataFrame()
    for date in check_date_array:
        slice_df=df.loc[df['date']==date]
        combine_df=pd.concat([combine_df,slice_df])

    combine_df['index_num']=combine_df['index_num'].astype(float)
    combine_df.sort_values(['date','index_num'],inplace=True)
    check_df2=pd.DataFrame({'date':check_date_array,'num':check_value_array})
    check_df2['theroy_num']=theory_num
    check_df2['type']=contract_kind
    check_df2.to_csv('D:/check_result/'+contract_kind+'.csv',index=False)
    combine_df.to_csv('D:/check_result/'+contract_kind+'_detail.csv',index=False)

