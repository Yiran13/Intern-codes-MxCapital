import numpy as np 
import pandas as pd
from unrar import rarfile
import numpy as np 
import pandas as pd
import tushare as ts
import os

year_month='20200106'
contract_kind='NI'
rar_data_file_path='C:/Users/lenovo/Documents/WeChat Files/yiranli13/FileStorage/File/2020-01/'
main_code_path='C:/Users/lenovo/Documents/WeChat Files/yiranli13/FileStorage/File/2020-01/main/main_/'
clean_data_path='D:/1_min补充统一/'
end_date='20200107'
time_range_path='D:/统一所有品种时间范围.csv'
commodity_bool=True

def renew_commodity_future_daily(year_month:str,contract_kind:str,main_code_path:str,rar_data_file_path:str,clean_data_path:str,time_range_path:str,end_date:str,commodity_bool=True):
    '''
    用于更新日频的商品期货数据
    和月度更新函数比，主要的区别在于date的寻找和文件的读取上：

    月度数据是通过tushare的交易日历找到对应月份的每个交易日，进行循环读取添加
    日频数据是直接通过tushare的交易日历对应到每个交易日
    
    月度数据的原始数据文件命名以品种命名，但是日频数据的文件名多了_年月日后缀

    year_month:'20200106'字符串年份和月份，对应的是FutAC_Min1_Std_后面的数字，如FutAC_Min1_Std_20200106
    contract_kind：放对应品种的字符串
    main_code_path:对应存放主力合约的地方
    rar_data_file_path: 对应的是存放rar数据如FutAC_Min1_Std_20200106.rar的位置,不包括对应的文件名
    clean_data_path:对应存放分钟数据的位置，处理好的新数据会追加到对应位置下的对应品种处
    time_range_path:放置交易时间文件的路径,包括文件名 如 D:/统一所有品种时间范围.csv
    end_date :'20200108' 今日的日期，用来请求tushare中的交易日历，数据的读取合并都是以交易日历的时间驱动，不要晚于要更新的日期即可
    commodity_bool:商品期货对应True，金融期货False,默认商品期货
    '''
    month=year_month
    if commodity_bool:  
        file_name=rar_data_file_path+'FutAC_Min1_Std_'+month+'.rar'
    else:
            file_name=rar_data_file_path+'FutSF_Min1_Std_'+month+'.rar'
    orignial_path=main_code_path
    specifi_path=orignial_path+contract_kind+'_1day_main.npy'
    rar = rarfile.RarFile(file_name,pwd='www.jinshuyuan.net')
    # 原始的处理好的数据
    orignal_clean_csv_path=clean_data_path
    pwd='www.jinshuyuan.net'
    data=np.load(specifi_path)
    time_0931_15=pd.read_csv(time_range_path)['date'].values.tolist()
    rar.extractall(path=file_name.split('.')[0])
    # 首先需要输入end_date 确保截取的时间长度和main主力合约的时间对齐
    #  按照月份确定位置
    pro = ts.pro_api('3d832df2966f27c20e6ff243ab1d53a35a4adc1c64b353cc370ac7d6')
    ts.set_token('3d832df2966f27c20e6ff243ab1d53a35a4adc1c64b353cc370ac7d6')
    date_df=pro.trade_cal(exchange='DCE', start_date='20100101', end_date=end_date)
    date_df=date_df.loc[date_df['is_open']==1]
    date_list=date_df['cal_date'].tolist()
    # ==========================================================================
    # 针对的是201911月数据，对应的合约index 放在 target_date_index中
    date_df=pd.DataFrame({'date':date_list})
    date_df['month']=date_df['date'].str[:6]
    target_date=date_df.loc[date_df['date']==month]
    target_date_index=target_date.index.values
    target_date=target_date['date'].values
    # 获取对应目标
    data=data.reshape(-1)
    contract_main_pool=data[target_date_index]
    # 去掉交易所的代码编号
    contract_main_pool=(pd.Series(contract_main_pool).str.split('.').str[0]+'_'+month+'.csv').values
    file_pools=os.listdir(file_name.split('.')[0])
    # 郑州期货交易所是大写，其它都是小写，这里需要逻辑判断
    if contract_main_pool[0] not in file_pools:
        contract_main_pool=[contract_file.lower() for contract_file in contract_main_pool]
    if contract_main_pool[0] not in file_pools:
        print(f'找不到{contract_main_pool[0]}')
    # 读取好所有的路径
    contract_main_pool=(file_name.split('.')[0]+'/'+pd.Series(contract_main_pool)).values
    # (len(target_date),contract_main_pool.shape[0])
    row_1=['市场代码','合约代码',	'时间',	'开','高',	'低',	'收',	'成交量',	'成交额',	'持仓量']
    orignal_data=[]
    orignal_data.append(row_1)
    for index in range(len(target_date)):
        date=target_date[index]
        one_file_path=contract_main_pool[index]
        df=pd.read_csv(one_file_path,encoding='gbk')
        df['date']=df['时间'].str[:10]
        df['date2']=df['date'].str.replace('-','')
        result=df.loc[df['date2']==date]
        if result.shape[0]>0:
            for row_index in range(len(result)):
                target_row=result.iloc[row_index].tolist()
                clean_row=target_row[:-2]
                orignal_data.append(clean_row)
            print(f'{contract_kind} {date} finished!')
        else:
            print(f'没找到合约品种{contract_kind}在{date}')
    print(f'{contract_kind}在{month}月的主力合约数据读取完成')
    final_df=pd.DataFrame(orignal_data[1:],columns=orignal_data[0])

    final_df['date']=final_df['时间'].str[:10]
    final_df_date=final_df['date'].unique()

    final_df['date']=final_df['时间'].str[:10]
    final_df['time']=final_df['时间'].str[10:].str.strip()
    final_df['时间']=final_df['date']+' '+final_df['time']
    final_df=final_df.sort_values('时间')
    final_df['合约代码']=final_df['合约代码'].str.upper()
    final_df=final_df.sort_values('时间')
    # ===============================增加了从constant_time进行截取================================
    final_df['transf_date']=pd.to_datetime(final_df['date'])
    final_df.set_index('transf_date',inplace=True)
    combine_all_df=pd.DataFrame()
    final_df['date2']=final_df['date'].str.replace('-','')
    # 按月进行填充
    # 设置了存放按月填充的路径
    for date_index in range(len(target_date)):

            #按日期进行分割
            target_df=final_df.loc[final_df['date2']==target_date[date_index]]
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
                    print(f'{target_date[date_index]} 不连续')
                #一共12列，先全部填充nan的时候，最后再把已知填入
                insert_array=np.empty(shape=(len(lack_time),12))
                insert_array.fill(np.nan)
                insert_df=pd.DataFrame(insert_array,columns=['市场代码','合约代码','时间','开','高','低','收','成交量','成交额','持仓量','date','time'])
                insert_df['date']=target_date[date_index]
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
                print(f'{target_date[date_index]}empty ')
                lack_time=[x for x in time_0931_15]
                #一共12列，先全部填充nan的时候，最后再把已知填入
                insert_array=np.empty(shape=(len(lack_time),12))
                insert_array.fill(np.nan)
                insert_df=pd.DataFrame(insert_array,columns=['市场代码','合约代码','时间','开','高','低','收','成交量','成交额','持仓量','date','time'])
                insert_df['date']=target_date[date_index]
                insert_df['time']=lack_time
                #将数据添加到容器
                combine_all_df=pd.concat([combine_all_df,insert_df])
    combine_all_df['时间']=combine_all_df['date']+' '+combine_all_df['time']
    #调整时间
    combine_all_df=combine_all_df.sort_values('时间')

    combine_all_df.reset_index(inplace=True)
    #数据输出，按设定的顺序
    combine_all_df=combine_all_df[['市场代码', '合约代码', '时间', '开', '高', '低', '收', '成交量', '成交额', '持仓量','date','time']]
    combine_all_df['时间']=combine_all_df['时间'].str.replace('-','')
    combine_all_df['date']=combine_all_df['date'].str.replace('-','')
    # combine_all_df.to_csv(save_month_fill_data_path,index=False,encoding='utf-8-sig')
    # ==========================储存数据=================================================
    combine_df=combine_all_df.copy()
    contract_type=contract_kind
    combine_df=combine_df.sort_values('时间')
    # ====================================================================开始截取============================================================
    # end_time+1其实是可以作为每次截取的起点，终点下一个就是起点，不过要加上0，而终点的位置也可以是end_time+1,因为end_time+1只能取end_time
    # 按照下午15：15统一截取
    end_time='15:15:00'
    end_index=np.where(combine_df['time']==end_time)[0]+1
    end_index=np.hstack(([0],end_index))
    start=end_index[:-1]
    end=end_index[1:]
    # ================================================================缺失第一个交易日前一天的夜盘数据==========================================
    # 这里的选择构造一个虚拟的时间戳，来满足缺失的夜盘数据
    # 按照上一步的截取方法，第一个交易日缺少前一天的夜盘数据
    last_day=date_df['date'].iloc[target_date_index[0]-1]
    last_day=last_day[:4]+'-'+last_day[4:6]+'-'+last_day[6:]
    first_day_have=combine_df[start[0]:end[0]]['time'].values
    full_time=combine_df['time'].unique()
    full_time.sort()
    first_day_lack=[x for x in full_time[-179:]]
    first_day_lack.sort()
    lack_array=np.empty(shape=(len(first_day_lack),12))
    lack_array.fill(np.nan)
    # ===============================准备缺失部分df==========================================================================================
    first_day_lack_df=pd.DataFrame(lack_array,columns=combine_df.columns)
    first_day_lack_df['time']=first_day_lack
    first_day_lack_df['date']=last_day
    first_day_lack_df['时间']=first_day_lack_df['date']+' '+first_day_lack_df['time']

    last_df=pd.read_csv(contract_main_pool[0],encoding='gbk')
    # 确定之前的有没有夜盘
    last_df['date']=last_df['时间'].str[:10]
    last_df['time']=last_df['时间'].str[11:]
    # 补夜盘数据
    last_time_pool=last_df.loc[last_df['date']==last_day]['time'].values

    last_day_have_date=[]
    # 说明在上个交易日有数据
    if last_time_pool.shape[0]>0:
        
        print(f'期货品种{contract_kind}在前一个交易日{last_day}有夜盘数据，需要读取覆盖')
        last_day_have_date=[x for x in last_time_pool]
    if last_day_have_date:
        for index in range(len(last_day_have_date)):
            origanl_index=last_df.loc[(last_df['date']==last_day)&(last_df['time']==last_day_have_date[index])].index[0]
            target_index=first_day_lack_df.loc[first_day_lack_df['time']==last_day_have_date[index]].index[0]
            first_day_lack_df.iloc[target_index]=last_df.iloc[origanl_index]
    else:
        print(f'期货品种{contract_kind}在前一个交易日{last_day}没有夜盘数据，不需要读取覆盖')
        print('直接使用np.nan填充上一个交易日的夜盘数据')
    for index in range(first_day_lack_df.shape[0]):
        combine_df=combine_df.append(first_day_lack_df.iloc[index])
    combine_df['时间']=combine_df['时间'].str.replace('-','')
    combine_df['date']=combine_df['date'].str.replace('-','')
    combine_df.sort_values('时间',inplace=True)
    # =================================缺失部分填充=========================================================================================
    # combine_df=pd.concat([first_day_lack_df,combine_df])
    # # ================================重新按时间排序========================================================================================
    # combine_df=combine_df.sort_values('时间')
    # ============================重新进行切割===============================================================================================
    end_index=np.where(combine_df['time']==end_time)[0]+1
    end_index=np.hstack(([0],end_index))
    start=end_index[:-1]
    end=end_index[1:]

    # ==============================进行分割按照特定时间,明确col===============================================================================

    col_type_list=['开','高','低','收','成交量','成交额','持仓量']
    dir_name_list=['open','high','low','close','volume','amount','position']
    #这个变量现在没有用
    #交易到凌晨01
    #merge_df=pd.DataFrame({'time':with_night_01})
    #交易到凌晨0230,version中没有集合竞价时间，time_0931_15去掉9：00，21：00
    merge_df=pd.DataFrame({'time':time_0931_15})

    combine_df['date']=combine_df['时间'].str[:8]
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
    # ========================追加原始数据=======================================
        # 时间问题需要处理，不然对不齐
        # 在测试文件中测试，所以修改了路径
        orignal_csv_df=pd.read_csv(orignal_clean_csv_path+contract_kind+'_1min_'+dir_name_list[index]+'.csv')
        column_ouput_form=orignal_csv_df.columns.values
        orignal_date_pool=pd.to_datetime(orignal_csv_df['date'],format='%Y-%m-%d').values
        current_date_pool=pd.to_datetime(csv_df['date'],format='%Y-%m-%d').values
        orignal_csv_df['date']=pd.to_datetime(orignal_csv_df['date'],format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
        csv_df['date']=pd.to_datetime(csv_df['date'],format='%Y%m%d').dt.strftime('%Y-%m-%d')
        # check代码中的数字个数等于四个
        main_code=csv_df['main_contract_code'].iloc[0]
        main_code_num=csv_df['main_contract_code'].str.findall(r'[0-9]+').iloc[0][0]
        if len(main_code_num)==3:
            print(f'合约代码{main_code}缺少一位数字,将被替换')
            csv_df['main_contract_code']=csv_df['main_contract_code'].str[:2]+month[0]+csv_df['main_contract_code'].str[2:]
            main_code=csv_df['main_contract_code'].iloc[0]
            print(f'合约代码{main_code}')
        # 查看有没有交集，如果有交集会停止，说明进行了重复操作
        
        intersection_pool=[date for date in orignal_date_pool if date in current_date_pool]
        if not intersection_pool:
            print(f'新旧数据没有时间交集，{contract_kind} {dir_name_list[index]} 将被添加到先前数据中')
            orignal_csv_df=pd.concat([orignal_csv_df,csv_df])   
            orignal_csv_df.sort_values('date',inplace=True)
            orignal_csv_df=orignal_csv_df[column_ouput_form]
            orignal_csv_df.to_csv(orignal_clean_csv_path+contract_kind+'_1min_'+dir_name_list[index]+'.csv',index=False)
            print(f'期货品种{contract_kind} {dir_name_list[index]} 完成')
        else:
            print(f'新旧数据的时间出现交集！！{contract_kind} {dir_name_list[index]} 将不会被添加到先前数据中')
                
