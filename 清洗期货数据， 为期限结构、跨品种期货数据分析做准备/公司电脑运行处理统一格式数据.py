import pandas as pd
import numpy as np
import os
import re
import pickle
# =========================================================================================================================================================
# 
# 金融数据统一数据格式，应用于公司电脑
# 迁移准备：
#          修改文件储存路径 save_data / 1min_补充
#          修改文件读取路径 read_data
# 
# ===========================================================================================================================================================
# run 文件前必须要明确和修改的变量：
#                                 contract_kind：期货品种
#                                 constant_time：统一的时间2010-01-04
#                                 time_0931_15： 统一的交易时间合并了商品期货和金融期货的交易时间bar的个数从555到615，金融期货下午多了半小时另外加上国债期货额外竞价时间
#                                 end_time：     统一为15：00
#                                 read_file:    文件读取的父路径名，金融期货和商品期货的路径放置不同的地方
#                                 file_name:    文件的名字，有大小写区分的问题。郑州和金融期货都是大写。

# 变量的相互作用：
#                constant_time 会影响whole_date_list 样本的个数,也会影响read_start_index,开始读取数据的起始点
#                time_0931_15  会影响填充数据的多少
#                end_time      会影响切割出来数据的shape
# 要思考的问题：
#                对统一格式的数据，填充之后要进行截取，最后一个样本的夜盘数据要进行截取，3点之后的数据（国债期货也是这样）
#                两个不同类型的期货，交易时间是否一致？两个类型的交易范围分别是多少？
#                如果有填充夜盘的数据，第一个交易的数据是不是也要进行额外的填充，填充前一个交易日夜盘的数
#                时间戳和数据没有对齐
#                品种TF按照切法，一共有4个interval 376, 555, 615, 600，555忽略了两种类型的期货交易时间不一致，第一个交易日缺少上个日期的夜盘
#             

                 
# ================================================================================================================================================================
# 
# 'IH''IC''IF' 'TF' 'T' 'TS'        
#              constant_time: '2010-01-04'
#              time_0931_15:合并了期货品种的交易时间
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
df=pd.read_csv('D:/read_data/AL_1min_open.csv')
whole_date_list=df.iloc[:,0].tolist()
# =========================================获取完整的样本时间constant_time-20191031===========
df=pd.read_csv('D:/read_data/'+'IF'+'主力连续.csv')
df['time']=df['时间'].str[10:].str.strip()
df['date']=df['时间'].str[:10]
constant_time='2010-01-04'
# =========================================读取储存的统一时间================================
def read_pickle(save_path:str,file_name:str):

    with open (save_path+file_name+'.pickle','rb') as f:
        data=pickle.load(f)
    return data
# 统一的时间结点需要把两个类型的品种交易时间进行合并，去重，排序
# ============================读取商品期货交易时间===========================================
time_0931_15=read_pickle('D:/read_data/','trading_hours')
# ============================读取金融期货交易时间===========================================
df=pd.read_csv('D:/read_data/'+'IF'+'主力连续.csv')
df['time']=df['时间'].str[10:].str.strip()
df['date']=df['时间'].str[:10]
time_0916_1515=df.loc[df['date']=='2010-04-16']['time'].values.tolist()
# =====================商品和金融期货交易时间合并，去重，排序================================
time_combine=np.concatenate((time_0931_15,time_0916_1515)).tolist()
time_combine=list(set(time_combine))
time_combine.sort()
time_0931_15=time_combine
#开始读取数据的index，因为读取是从主力合约的npy文件开始
read_start_index=(np.where(np.array(whole_date_list)==constant_time))[0][0]
whole_date_list=whole_date_list[(np.where(np.array(whole_date_list)==constant_time))[0][0]:]

# ========================================设置期货品种与交易时间段=========================
# 上期所
# 
# 大商所
# 
# 中金所
for contract_kind in ['IH','IC','IF','TF', 'T','TS']:

    
    end_time='15:15:00'
    # ======================================品种设置========================================
    contract_type=contract_kind
    type_pattern=re.compile(r'\.[A-Z]+')
    contract_pattern=re.compile(r'[A-Z]+[0-9]+')
    #通过ywhole_yearlist 取
    #year_pattern=re.compile(r'[0-9]+')
    row_1=['市场代码','合约代码',	'时间',	'开','高',	'低',	'收',	'成交量',	'成交额',	'持仓量']
    path='D:/read_data/'+contract_kind+'_1day_main.npy'
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
    # ===============================================================不同交易所文件大小不同，读取的路径也不同===================================
    # FutSF金融 FutAC 商品
    # 非中金和郑州名字是小写
            readfile_path='D:/read_data/FutAC_Min1_Std_'+year+'/'
    # ==========================================================================================================================================
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
    final_df.to_csv('D:/save_data/'+contract_kind+'主力连续.csv',index=False,encoding='utf-8-sig')
    # =========================================完成读取任务=====================================
    # 填充
    combine_df=pd.read_csv('D:/save_data/'+contract_kind+'主力连续.csv')
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
    combine_all_df.to_csv('D:/1_min补充统一/'+contract_kind+'主力连续.csv',index=False,encoding='utf-8-sig')
    # ====================================================================填充步骤全部完成===================================================
    combine_df=pd.read_csv('D:/1_min补充统一/'+contract_kind+'主力连续.csv')
    contract_type=contract_kind
    combine_df=combine_df.sort_values('时间')
    # ====================================================================开始截取============================================================
    # end_time+1其实是可以作为每次截取的起点，终点下一个就是起点，不过要加上0，而终点的位置也可以是end_time+1,因为end_time+1只能取end_time
    # 按照下午15：15统一截取
    end_index=np.where(combine_df['time']==end_time)[0]+1
    end_index=np.hstack(([0],end_index))
    start=end_index[:-1]
    end=end_index[1:]
    # ================================================================缺失第一个交易日前一天的夜盘数据==========================================
    # 这里的选择构造一个虚拟的时间戳，来满足缺失的夜盘数据
    # 按照上一步的截取方法，第一个交易日缺少前一天的夜盘数据
    first_day_have=combine_df[start[0]:end[0]]['time'].values
    full_time=combine_df[start[1]:end[1]]['time'].values
    first_day_lack=[x for x in full_time if x not in first_day_have]
    first_day_lack.sort()
    lack_array=np.empty(shape=(len(first_day_lack),12))
    lack_array.fill(np.nan)
    # ===============================准备缺失部分df==========================================================================================
    first_day_lack_df=pd.DataFrame(lack_array,columns=combine_df.columns)
    first_day_lack_df['time']=first_day_lack
    first_day_lack_df['date']='2010-01-03'
    first_day_lack_df['时间']=first_day_lack_df['date']+' '+first_day_lack_df['time']
    # =================================缺失部分填充=========================================================================================
    combine_df=pd.concat([first_day_lack_df,combine_df])
    # ================================重新按时间排序========================================================================================
    combine_df=combine_df.sort_values('时间')
    # ============================重新进行切割===============================================================================================
    end_index=np.where(combine_df['time']==end_time)[0]+1
    end_index=np.hstack(([0],end_index))
    start=end_index[:-1]
    end=end_index[1:]
    # ==============================进行分割按照特定时间,明确col===============================================================================
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
        
            col_type=col_type_list[index]
    #       用来接收分col数据的容器
            csv_df=pd.DataFrame()

            
            for s_index,e_index in zip(start,end):
                
    # =========================================截取每个交易日数据==============================================================================
                res=combine_df.iloc[s_index:e_index,:]
                one_date_df=pd.DataFrame(res[col_type].values.reshape(1,-1),columns=res['time'].values.tolist())
                one_date_df['main_contract_code']=res.iloc[-1]['合约代码']
                one_date_df['date']=res.iloc[-1]['date']
    # =======================================设置输出格式====================================================================================

                col_layout=['date']
                col_layout=np.hstack((col_layout,res['time'].values.tolist()))
                col_layout=np.hstack((col_layout,['main_contract_code']))
                one_date_df=one_date_df[col_layout]
    # =======================================合并数据========================================================================================
                csv_df=pd.concat([csv_df,one_date_df])
    # =====================================循环合并===========================================================================================
        
            save_path='D:/1_min补充统一/'
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            #np.save(save_path+contract_type+'.npy',combine_list)
    #==================================输出前数据的日期个数检查==============================================================================
            if csv_df['date'].values.shape[0] != len(whole_date_list):
            
            
                
                print('程序自动结束.因为出现了不应该出现的日期缺失问题,请从填充部分开始debug')
                break 
    # =================================输出前数据的日期顺序检查============================================================================
            else:
                if np.sum(csv_df['date'].values!=whole_date_list[:csv_df['date'].values.shape[0]])>0:
                    print('程序自动结束.因为出现了不应该出现日期顺序问题,请从填充部分开始debug')
                else:
                    print('日期顺序和个数均没问题,即将输出结果')
            
        
            csv_df.to_csv(save_path+contract_type.upper()+'_1min_'+dir_name_list[index]+'.csv',index=False)
            print(f'{contract_type} {dir_name_list[index]} finished')

