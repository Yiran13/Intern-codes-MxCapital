import pandas as pd
import numpy as np
import os
import re
# ==========================================================================================================================================================
# 
# 金融单个数据
# ===========================================================================================================================================================
# run 文件前必须要明确和修改的变量：
#                                 contract_kind：期货品种
#                                 constant_time：交易时间稳定开始的日期，IF,IC,IH是在2016-01-04，还要使得第一个数据是非None，对于单个品种数据
#                                 time_0931_15：交易时间段稳定的时间区间，IF,IC,IH在2019-10-31的稳定交易时间是9:31-15:00,其它的品种肯定会有所不同，有的还要夜盘
#                                 end_time：    最终分割的时间点，目前设定在15：00之后的交易日期都是第二个交易日，对于15：15的交易品种可能会改变在单个品种的时候
#                                 read_file:    文件读取的父路径名，金融期货和商品期货的路径放置不同的地方
#                                 file_name:    文件的名字，有大小写区分的问题。郑州和金融期货都是大写。

# 变量的相互作用：
#                constant_time 会影响whole_date_list 样本的个数,也会影响read_start_index,开始读取数据的起始点
#                time_0931_15  会影响填充数据的多少
#                end_time      会影响切割出来数据的shape
# 要思考的问题：
#                如果有填充夜盘的数据，最后一个交易日夜盘的数据是否要进行截取
#                如果有填充夜盘的数据，第一个交易的数据是不是也要进行额外的填充，填充前一个交易日夜盘的数据
# ================================================================================================================================================================
# 
# 'IH''IC''IF' contstant_time:'2016-01-04' 
#              time_0931_15:时间从9点31到下午三点
#              end_time:15:00:00
#              read_file:金融路径
#              file_name：不需要修改本身就是大写
# 'TF'         constant_time: '2013-09-06'(上市以来，交易时间就没变过)（5年期）
# 'T'          constant_time: '2015-03-20'(上市以来，交易时间就没变过)(十年期)
# 'TS'         constant_time: '2018-08-17'(上市以来，交易时间就没变过)(2年期)
#              time_0931_15:时间从9点16到下午三点15分,使用的IF2010-04-16的交易时间数据
#              end_time:15:15:00
#              read_file:金融路径
#              file_name：不需要修改本身就是大写
# 
# =============================================================================================================================================================
# 修改：找寻稳定的交易时间，从constant_time，IF
# 添加稳定时间开始节点：constant_time,交易时间段稳定的时间
# 确保作为reference 的时间戳的时间不能少
# whole_date_list也开始从这个时间段开始截取
# =================================================开始=======================================
df=pd.read_csv('D:/1_min_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
# =========================================获取完整的样本时间constant_time-20191031===========
df=pd.read_csv('D:/DIY_主力合约/'+'IF'+'主力连续.csv')
df['time']=df['时间'].str[10:].str.strip()
df['date']=df['时间'].str[:10]
constant_time='2018-08-17'
time_0931_15=df.loc[df['date']=='2010-04-16']['time'].values.tolist()
#开始读取数据的index，因为读取是从主力合约的npy文件开始
read_start_index=(np.where(np.array(whole_date_list)==constant_time))[0][0]
whole_date_list=whole_date_list[(np.where(np.array(whole_date_list)==constant_time))[0][0]:]

# ========================================设置交易时间段0=========================

contract_kind='TS'
end_time='15:15:00'
# ======================================品种设置========================================
contract_type=contract_kind
type_pattern=re.compile(r'\.[A-Z]+')
contract_pattern=re.compile(r'[A-Z]+[0-9]+')
#通过ywhole_yearlist 取
#year_pattern=re.compile(r'[0-9]+')
row_1=['市场代码','合约代码',	'时间',	'开','高',	'低',	'收',	'成交量',	'成交额',	'持仓量']
path='D:/主力合约/新建文件夹/'+contract_kind+'_1day_main.npy'
#加载主力合约
data=np.load(path)
data=data.reshape(-1)
data=data[read_start_index:]
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
# ===============================增加了从constant_time进行截取================================
combine_df['transf_date']=pd.to_datetime(combine_df['date'])
combine_df.set_index('transf_date',inplace=True)
combine_df=combine_df.loc[constant_time:]
combine_df.reset_index(inplace=True)
# ===============================截取完成，开始进行填充检查======================================
combine_all_df=pd.DataFrame()

for date_index in range(len(whole_date_list)):

    #按日期进行分割
    target_df=combine_df.loc[combine_df['date']==whole_date_list[date_index]]
    #分割到的长度放入容器中
    target_num=len(target_df)
    #理论长度
    theory_num=len(time_0931_15)
    #实际上两种情况：1.是交易日但完全没有数据2.是交易日，只有部分数据 3.是交易日，数据也是完整的
    if target_num>0:
        #开始区分2，3情况
        
      
        have_time=target_df['time'].values.tolist()
        lack_time=[x for x in time_0931_15 if x not in have_time]
        #检查是不是情况2
        if lack_time:
             print(f'{whole_date_list[date_index]} 不连续')
        #一共12列，先全部填充nan的时候，最后再把已知填入
        insert_array=np.empty(shape=(len(lack_time),12))
        insert_array.fill(np.nan)
        insert_df=pd.DataFrame(insert_array,columns=['市场代码','合约代码','时间','开','高','低','收','成交量','成交额','持仓量','date','time'])
        insert_df['date']=whole_date_list[date_index]
        insert_df['time']=lack_time
        #缺少时间的个数小于time_0931_15则说明，当天并不是完全没数据,只是部分数据缺失，因此要对合约代码进行填充
        if len(lack_time)<len(time_0931_15):
             
            insert_df['合约代码']=target_df['合约代码'].unique()[-1]
        #生成一天完整的数据
        combine_insert_df=pd.concat([target_df,insert_df])
        #将数据添加到容器中
        combine_all_df=pd.concat([combine_all_df,combine_insert_df])
            
        
    #完全没有数据，直接填充 
    else:
        print(f'{whole_date_list[date_index]}empty ')
        lack_time=[x for x in time_0931_15]
        #一共12列，先全部填充nan的时候，最后再把已知填入
        insert_array=np.empty(shape=(len(lack_time),12))
        insert_array.fill(np.nan)
        insert_df=pd.DataFrame(insert_array,columns=['市场代码','合约代码','时间','开','高','低','收','成交量','成交额','持仓量','date','time'])
        insert_df['date']=whole_date_list[date_index]
        insert_df['time']=lack_time
        #将数据添加到容器
        combine_all_df=pd.concat([combine_all_df,insert_df])
combine_all_df['时间']=combine_all_df['date']+' '+combine_all_df['time']
#调整时间
combine_all_df=combine_all_df.sort_values('时间')

combine_all_df.reset_index(inplace=True)
# 原先的数据中需要删除15：15的数据，因为会算到数据终点的第二个交易日
# stop_point=combine_all_df.loc[(combine_all_df['date']=='2019-10-31')&(combine_all_df['time']=='15:15:00')].index[0]
# combine_all_df=combine_all_df.iloc[:stop_point+1]
#数据输出，按设定的顺序
combine_all_df=combine_all_df[['市场代码', '合约代码', '时间', '开', '高', '低', '收', '成交量', '成交额', '持仓量','date','time']]
combine_all_df.to_csv('D:/1_min_补充品种/'+contract_kind+'主力连续.csv',index=False,encoding='utf-8-sig')
# ====================================================================填充步骤全部完成===================================================
combine_df=pd.read_csv('D:/1_min_补充品种/'+contract_kind+'主力连续.csv')
contract_type=contract_kind
combine_df=combine_df.sort_values('时间')
# ==============================================================股指期货三个都是15：00收盘===========================================
# end_time+1其实是可以作为每次截取的起点，终点下一个就是起点，不过要加上0，而终点的位置也可以是end_time+1,因为end_time+1只能取end_time
end_index=np.where(combine_df['time']==end_time)[0]+1
end_index=np.hstack(([0],end_index))
start=end_index[:-1]
end=end_index[1:]
#col_type='开'
col_type_list=['开','高','低','收','成交量','成交额','持仓量']
dir_name_list=['open','high','low','close','volume','amount','position']
#这个变量现在没有用
have_night=True
#交易到凌晨01
#merge_df=pd.DataFrame({'time':with_night_01})
#交易到凌晨0230,version中没有集合竞价时间，time_0931_15去掉9：00，21：00
merge_df=pd.DataFrame({'time':time_0931_15})
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
        
        # 生成单个类型的df，列名是稳定的交易时间
        csv_df=pd.DataFrame(combine_list,columns=time_0931_15,index=date_index_list)
        csv_df['main_contract_code']=main_contract_list
        # 再次检查是否有缺失日期
        lack_date_list=[x for x in whole_date_list if x not in date_index_list]
        if lack_date_list:
            print('程序自动结束因为出现了不应该出现的数据缺失问题,请从填充部分开始debug')
            break 
       
        


        save_path='D:/1_min_补充品种/'
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        #np.save(save_path+contract_type+'.npy',combine_list)
        csv_df.to_csv(save_path+contract_type.upper()+'_1min_'+dir_name_list[index]+'.csv')
        print(f'{contract_type} {dir_name_list[index]} finished')
#    