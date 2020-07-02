import pandas as pd
import numpy as np
import os

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

contract_type='AU'   
record_date1,record_num1=classify_time_period('2010',contract_type)
record_date2,record_num2=classify_time_period('2011',contract_type)
record_date3,record_num3=classify_time_period('2012',contract_type)
record_date4,record_num4=classify_time_period('2013',contract_type)
record_date5,record_num5=classify_time_period('2014',contract_type)
record_date6,record_num6=classify_time_period('2015',contract_type)
record_date7,record_num7=classify_time_period('2016',contract_type)
record_date8,record_num8=classify_time_period('2017',contract_type)
record_date9,record_num9=classify_time_period('2018',contract_type)
record_date10,record_num10=classify_time_period('2019',contract_type)

record_num1=np.array(record_num1)
record_num2=np.array(record_num2)
record_num3=np.array(record_num3)
record_num4=np.array(record_num4)
record_num5=np.array(record_num5)
record_num6=np.array(record_num6)
record_num7=np.array(record_num7)
record_num8=np.array(record_num8)
record_num9=np.array(record_num9)
record_num10=np.array(record_num10)

record_date1=np.array(record_date1)
record_date2=np.array(record_date2)
record_date3=np.array(record_date3)
record_date4=np.array(record_date4)
record_date5=np.array(record_date5)
record_date6=np.array(record_date6)
record_date7=np.array(record_date7)
record_date8=np.array(record_date8)
record_date9=np.array(record_date9)
record_date10=np.array(record_date10)

combine_num=np.hstack((record_num1,record_num2,record_num3,record_num4,record_num5,record_num6,record_num7,record_num8,record_num9,record_num10))
combine_date=np.hstack((record_date1,record_date2,record_date3,record_date4,record_date5,record_date6,record_date7,record_date8,record_date9,record_date10))

#更换品种，导入对应时间list即可
#导入所有0100的品种，只有上海期货交易所
#contract_0100pool=['cu','zn','pb','ni','sn','rb','hc','bu']
#contract_0230pool=['ag','p','j','a','b','m','y','jm','i']








for contract_type in contract_0230pool:
    #contract_type='al'
    path1='D:/1分钟数据/FutAC_Min1_Std_2010/'+contract_type+'主力连续.csv'
    path2='D:/1分钟数据/FutAC_Min1_Std_2011/'+contract_type+'主力连续.csv'
    path3='D:/1分钟数据/FutAC_Min1_Std_2012/'+contract_type+'主力连续.csv'
    path4='D:/1分钟数据/FutAC_Min1_Std_2013/'+contract_type+'主力连续.csv'
    path5='D:/1分钟数据/FutAC_Min1_Std_2014/'+contract_type+'主力连续.csv'
    path6='D:/1分钟数据/FutAC_Min1_Std_2015/'+contract_type+'主力连续.csv'
    path7='D:/1分钟数据/FutAC_Min1_Std_2016/'+contract_type+'主力连续.csv'
    path8='D:/1分钟数据/FutAC_Min1_Std_2017/'+contract_type+'主力连续.csv'
    path9='D:/1分钟数据/FutAC_Min1_Std_2018/'+contract_type+'主力连续.csv'
    path10='D:/1分钟数据/FutAC_Min1_Std_2019/'+contract_type+'主力连续.csv'

    if os.path.exists(path1):
        df1=pd.read_csv(path1,encoding='gbk')
    else:
        df1=pd.DataFrame()

    if os.path.exists(path2):
        df2=pd.read_csv(path2,encoding='gbk')
    else:
        df2=pd.DataFrame()
    df2=pd.concat([df1,df2])

    if os.path.exists(path3):
        df3=pd.read_csv(path3,encoding='gbk')
    else:
        df3=pd.DataFrame()
    df3=pd.concat([df2,df3])

    if os.path.exists(path4):
        df4=pd.read_csv(path4,encoding='gbk')
    else:
        df4=pd.DataFrame()
    df4=pd.concat([df3,df4])

    if os.path.exists(path5):
        df5=pd.read_csv(path5,encoding='gbk')
    else:
        df5=pd.DataFrame()
    df5=pd.concat([df4,df5])

    if os.path.exists(path6):
        df6=pd.read_csv(path6,encoding='gbk')
    else:
        df6=pd.DataFrame()
    df6=pd.concat([df5,df6])

    if os.path.exists(path7):
        df7=pd.read_csv(path7,encoding='gbk')
    else:
        df7=pd.DataFrame()
    df7=pd.concat([df6,df7])

    if os.path.exists(path8):
        df8=pd.read_csv(path8,encoding='gbk')
    else:
        df8=pd.DataFrame()
    df8=pd.concat([df7,df8])

    if os.path.exists(path9):
        df9=pd.read_csv(path9,encoding='gbk')
    else:
        df9=pd.DataFrame()
    df9=pd.concat([df8,df9])

    if os.path.exists(path10):
        df10=pd.read_csv(path10,encoding='gbk')
    else:
        df10=pd.DataFrame()
    df10=pd.concat([df9,df10])

    test=df10.copy()



    test['time']=test['时间'].apply(get_time)
    test['date']=test['时间'].apply(get_date)
    time_21_pool=np.where(test['time']=='21:01:00')[0]
    time_21_df=pd.DataFrame()
    
    if np.sum(time_21_pool>0):
        for index in time_21_pool:
            
            reference=test.iloc[index]
            date=reference['date']
            time=date+' '+'21:00:00'
            value=reference['开']
            hold=reference['持仓量']
            df1=pd.DataFrame({'时间':time,
            '开':value,
            '高':value,
            '低':value,
            '收':value,
            '成交量':np.float(0),
            '成交额':np.float(0),
            '持仓量':hold,
            'time':'21:00:00',
            'date':date},index=[1])
            time_21_df=pd.concat([time_21_df,df1])

    time_09_pool=np.where(test['time']=='09:01:00')[0]
    time_09_df=pd.DataFrame()
    if np.sum(time_09_pool)>0:

        for index in time_09_pool:
            
            reference=test.iloc[index]
            date=reference['date']
            time=date+' '+'09:00:00'
            value=reference['开']
            hold=reference['持仓量']
            df1=pd.DataFrame({'时间':time,
            '开':value,
            '高':value,
            '低':value,
            '收':value,
            '成交量':np.float(0),
            '成交额':np.float(0),
            '持仓量':hold,
            'time':'09:00:00',
            'date':date},index=[1])
            time_09_df=pd.concat([time_09_df,df1])

    combine_df=pd.concat([time_09_df,time_21_df])
    combine_df['市场代码']=test.iloc[0,0]
    combine_df['合约代码']=test.iloc[0,1]
    combine_df=pd.concat([test,combine_df])
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
    #交易到凌晨0230
    merge_df=pd.DataFrame({'time':with_night_0230})
    if have_night:

        for index in range(len(col_type_list)):
            combine_list=[]
            col_type=col_type_list[index]
            date_index_list=[]

            for s_index,e_index in zip(start,end):
                res=combine_df.iloc[s_index:e_index,:]
                combine=pd.merge(merge_df,res,how='outer')
                date_index=res.iloc[-1,0]
                result_array=combine[col_type].values.tolist()
                combine_list.append(result_array)
                date_index_list.append(date_index)
            combine_list=np.array(combine_list)
            #交易到凌晨1点
            #csv_df=pd.DataFrame(combine_list,columns=with_night_01,index=date_index_list)
            #交易到凌晨2点30
            csv_df=pd.DataFrame(combine_list,columns=with_night_0230,index=date_index_list)
            
            #交易到凌晨2点30
            if date_index_list[0] !='2010-01-04':
                print(f'{contract_type} starts at {date_index_list[0]} ')

                lack_date_list=[x for x in whole_date_list if x not in date_index_list]
                #将不足的时间补为NA
                wholetime_df=pd.DataFrame(index=lack_date_list,columns=with_night_0230)
    
                csv_df=pd.concat([wholetime_df,csv_df])
            

            save_path='D:/1_min_data/'
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            #np.save(save_path+contract_type+'.npy',combine_list)
            csv_df.to_csv(save_path+contract_type.upper()+'_1min_'+dir_name_list[index]+'.csv')
            print(f'{contract_type} {dir_name_list[index]} finished')
    

'''
#意外查询
if (test['time'].iloc[0]=='21:01:00') or (test['time'].iloc[0]=='09:01:00'):
    pass

   
else:

    print('注意开盘时间！ 不是 21：00 或者 09：00 注意！')


'''


'''
#确定交易品种
import re
path1='D:/1分钟数据/FutAC_Min1_Std_2010/'
path2='D:/1分钟数据/FutAC_Min1_Std_2011/'
path3='D:/1分钟数据/FutAC_Min1_Std_2012/'
path4='D:/1分钟数据/FutAC_Min1_Std_2013/'
path5='D:/1分钟数据/FutAC_Min1_Std_2014/'
path6='D:/1分钟数据/FutAC_Min1_Std_2015/'
path7='D:/1分钟数据/FutAC_Min1_Std_2016/'
path8='D:/1分钟数据/FutAC_Min1_Std_2017/'
path9='D:/1分钟数据/FutAC_Min1_Std_2018/'
path10='D:/1分钟数据/FutAC_Min1_Std_2019/'
path_list=[path1,path2,path3,path4,path5,path6,path7,path8,path9,path10]
for path in path_list:
    file_pool=os.listdir(path)
    file_pattern2=re.compile('[A-Z]+')
    file_pattern1=re.compile('[a-z]+')
    record_list=[]
    for file_name in file_pool:
        result1=re.findall(file_pattern,file_name[:-3])
        result2=re.findall(file_pattern2,file_name[:-3])
        if result1:
            record_list.append(result1[0])
        if result2:
            record_list.append(result2[0])
    result=list(set(record_list))
    print(len(result))
'''