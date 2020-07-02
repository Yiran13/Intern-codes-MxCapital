from csv import reader
import numpy as np 
import pandas as pd 
#  txt的列名2014年开始修改
# for year in range(2010,2014):
# # year=2010
#     path='D:/郑州日线官方历史数据/'+str(year)+'.txt'
#     data_file=open(path,'r')

#     data_line=reader(data_file)
#     data_list=list(data_line)
#     data_file.close()
#     # data_list[0] txt 年度标题没有用
#     data_list[1]
#     # 列名和数据的分割符也不一致,非常奇怪
#     # ['交易日期\t|品种月份\t|昨结算\t|今开盘\t|最高价\t|最低价\t|今收盘\t|今结算\t|涨跌1\t|涨跌2\t|成交量(手)|空盘量\t|增减量\t|成交额(万元)\t|交割结算价\t|

#     row_num=len(data_list)
#     combine_data=[]
#     for row_index in range(row_num)[1:]:
#         # if row_index==1:
#         #     clean_row=''.join(''.join(data_list[row_index]).split('|')).split('\t')
#         #     # first_row=np.vstack((first_row,clean_row))
#         # else:
#         #     half_clean_row=''.join(data_list[row_index]).split('|')
#         #     date=[]
#         #     date.append(half_clean_row[0])
#         #     half_clean_row=''.join(half_clean_row[1:]).split('\t')
#         #     clean_row=(np.hstack((date,half_clean_row))).tolist()
#         #     first_row=np.vstack((first_row,clean_row))
#         #     break
#         clean_row=''.join(data_list[row_index]).replace('\t','').split('|')
#         combine_data.append(clean_row)
#     combine_df=pd.DataFrame(combine_data)
#     combine_df=pd.DataFrame(combine_df.iloc[1:].values,columns=combine_df.iloc[0].values)
#     for col_name in combine_df.columns.tolist()[2:-1]:
#         combine_df[col_name]=combine_df[col_name].astype(float)

#     #处理volume==0
#     volume_0_list=combine_df.loc[combine_df['成交量(手)']==0].index.tolist()

#     for col_index in [3,4,5,6]:
#         combine_df.iloc[volume_0_list,col_index]=combine_df.iloc[volume_0_list,7]

#     #名字修改list
#     combine_df['品种月份']=combine_df['品种月份'].str.upper()
#     old_name_list=['TC','ME','RO']
#     new_name_list=['ZC','MA','OI']
#     for old_name,new_name in zip(old_name_list,new_name_list):
#         combine_df['品种月份']=combine_df['品种月份'].str.replace(old_name,new_name)
#     #原始3位数
#     combine_df['three_digit']=combine_df['品种月份'].str[-3:]
#     combine_df['contract_type']=combine_df['品种月份'].str[:-3]
#     combine_df['add_digit']='1'
#     combine_df['second_digit_year']=combine_df['three_digit'].str[0]
#     combine_df['year']=combine_df['交易日期'].str[:4]
#     filter_con=(combine_df['year']=='2019')&(combine_df['second_digit_year']=='0')
#     change_pool=combine_df.loc[filter_con].index.tolist()
#     for index in change_pool:
#         combine.iloc[index,-3]=='2'
#     combine_df['品种月份']=combine_df['contract_type']+combine_df['add_digit']+combine_df['three_digit']
#     combine_df=combine_df.iloc[:,:-6]
#     combine_df.to_csv('D:/郑州日线官方历史数据/'+str(year)+'.csv',index=False,encoding='utf-8-sig')
#  编码更换
# for year in range(2014,2017):
# # year=2010
#     path='D:/郑州日线官方历史数据/'+str(year)+'.txt'
#     data_file=open(path,'r')

#     data_line=reader(data_file)
#     data_list=list(data_line)
#     data_file.close()
#     # data_list[0] txt 年度标题没有用
#     data_list[1]
#     # 列名和数据的分割符也不一致,非常奇怪
#     # ['交易日期\t|品种月份\t|昨结算\t|今开盘\t|最高价\t|最低价\t|今收盘\t|今结算\t|涨跌1\t|涨跌2\t|成交量(手)|空盘量\t|增减量\t|成交额(万元)\t|交割结算价\t|

#     row_num=len(data_list)
#     combine_data=[]
#     for row_index in range(row_num)[1:]:
#         # if row_index==1:
#         #     clean_row=''.join(''.join(data_list[row_index]).split('|')).split('\t')
#         #     # first_row=np.vstack((first_row,clean_row))
#         # else:
#         #     half_clean_row=''.join(data_list[row_index]).split('|')
#         #     date=[]
#         #     date.append(half_clean_row[0])
#         #     half_clean_row=''.join(half_clean_row[1:]).split('\t')
#         #     clean_row=(np.hstack((date,half_clean_row))).tolist()
#         #     first_row=np.vstack((first_row,clean_row))
#         #     break
#         clean_row=''.join(data_list[row_index]).replace('\t','').split('|')
#         combine_data.append(clean_row)
#     combine_df=pd.DataFrame(combine_data)
#     combine_df=pd.DataFrame(combine_df.iloc[1:].values,columns=combine_df.iloc[0].str.strip().values)
#     for col_name in combine_df.columns.tolist()[2:-1]:
#         combine_df[col_name]=combine_df[col_name].astype(float)

#     for col_name in combine_df.columns.tolist()[:2]:
#         combine_df[col_name]=combine_df[col_name].str.strip()

#     #处理volume==0
#     volume_0_list=combine_df.loc[combine_df['成交量(手)']==0].index.tolist()

#     for col_index in [3,4,5,6]:
#         combine_df.iloc[volume_0_list,col_index]=combine_df.iloc[volume_0_list,7]

#     #名字修改list
#     combine_df['品种代码']=combine_df['品种代码'].str.upper()
#     old_name_list=['TC','ME','RO']
#     new_name_list=['ZC','MA','OI']
#     for old_name,new_name in zip(old_name_list,new_name_list):
#         combine_df['品种代码']=combine_df['品种代码'].str.replace(old_name,new_name)
#     #原始3位数
#     combine_df['three_digit']=combine_df['品种代码'].str[-3:]
#     combine_df['contract_type']=combine_df['品种代码'].str[:-3]
#     combine_df['add_digit']='1'
#     combine_df['second_digit_year']=combine_df['three_digit'].str[0]
#     combine_df['year']=combine_df['交易日期'].str[:4]
#     filter_con=(combine_df['year']=='2019')&(combine_df['second_digit_year']=='0')
#     change_pool=combine_df.loc[filter_con].index.tolist()
#     for index in change_pool:
#         combine.iloc[index,-3]=='2'
#     combine_df['品种代码']=combine_df['contract_type']+combine_df['add_digit']+combine_df['three_digit']
#     combine_df=combine_df.iloc[:,:-6]
#     combine_df.to_csv('D:/郑州日线官方历史数据/'+str(year)+'.csv',index=False,encoding='utf-8-sig')


# for year in range(2017,2020):
# # year=2010
#     path='D:/郑州日线官方历史数据/'+str(year)+'.txt'
#     data_file=open(path,'r',encoding='utf-8')

#     data_line=reader(data_file)
#     data_list=list(data_line)
#     data_file.close()
#     # data_list[0] txt 年度标题没有用
#     data_list[1]
#     # 列名和数据的分割符也不一致,非常奇怪
#     # ['交易日期\t|品种月份\t|昨结算\t|今开盘\t|最高价\t|最低价\t|今收盘\t|今结算\t|涨跌1\t|涨跌2\t|成交量(手)|空盘量\t|增减量\t|成交额(万元)\t|交割结算价\t|

#     row_num=len(data_list)
#     combine_data=[]
#     for row_index in range(row_num)[1:]:
#         # if row_index==1:
#         #     clean_row=''.join(''.join(data_list[row_index]).split('|')).split('\t')
#         #     # first_row=np.vstack((first_row,clean_row))
#         # else:
#         #     half_clean_row=''.join(data_list[row_index]).split('|')
#         #     date=[]
#         #     date.append(half_clean_row[0])
#         #     half_clean_row=''.join(half_clean_row[1:]).split('\t')
#         #     clean_row=(np.hstack((date,half_clean_row))).tolist()
#         #     first_row=np.vstack((first_row,clean_row))
#         #     break
#         clean_row=''.join(data_list[row_index]).replace('\t','').split('|')
#         combine_data.append(clean_row)
#     combine_df=pd.DataFrame(combine_data)
#     combine_df=pd.DataFrame(combine_df.iloc[1:].values,columns=combine_df.iloc[0].str.strip().values)
#     for col_name in combine_df.columns.tolist()[2:-1]:
#         combine_df[col_name]=combine_df[col_name].astype(float)
#     #取出列名的空格
#     for col_name in combine_df.columns.tolist()[:2]:
#         combine_df[col_name]=combine_df[col_name].str.strip()

#     #处理volume==0
#     volume_0_list=combine_df.loc[combine_df['成交量(手)']==0].index.tolist()

#     for col_index in [3,4,5,6]:
#         combine_df.iloc[volume_0_list,col_index]=combine_df.iloc[volume_0_list,7]

#     #名字修改list
#     combine_df['品种代码']=combine_df['品种代码'].str.upper()
#     old_name_list=['TC','ME','RO']
#     new_name_list=['ZC','MA','OI']
#     for old_name,new_name in zip(old_name_list,new_name_list):
#         combine_df['品种代码']=combine_df['品种代码'].str.replace(old_name,new_name)
#     #原始3位数
#     combine_df['three_digit']=combine_df['品种代码'].str[-3:]
#     combine_df['contract_type']=combine_df['品种代码'].str[:-3]
#     combine_df['add_digit']='1'
#     combine_df['second_digit_year']=combine_df['three_digit'].str[0]
#     combine_df['year']=combine_df['交易日期'].str[:4]
#     filter_con=(combine_df['year']=='2019')&(combine_df['second_digit_year']=='0')
#     change_pool=combine_df.loc[filter_con].index.tolist()
#     for index in change_pool:
#         combine_df.iloc[index,-3]='2'
#     combine_df['品种代码']=combine_df['contract_type']+combine_df['add_digit']+combine_df['three_digit']
#     combine_df=combine_df.iloc[:,:-6]
#     combine_df.to_csv('D:/郑州日线官方历史数据/'+str(year)+'.csv',index=False,encoding='utf-8-sig')

combine_df=pd.DataFrame()
for year in range(2010,2020):
    path='D:/郑州日线官方历史数据/'+str(year)+'.csv'
    df=pd.read_csv(path)


    if year < 2014:


        df.rename(columns={'品种月份':'品种代码'},inplace=True)
    
    combine_df=pd.concat([combine_df,df])

combine_df.dropna(inplace=True)
combine_df.to_csv('D:/郑州日线官方历史数据/汇总数据.csv',index=False,encoding='utf-8-sig')
change_col=['date','contract_code','topen','thigh','tlow','tclose','tvolume','tsettle','tamount']
corr_col=['交易日期','品种代码','今开盘','最高价','最低价','今收盘','成交量(手)','今结算','成交额(万元)']
need_df=combine_df[corr_col]
for corr,change in zip(corr_col,change_col):
    need_df.rename(columns={corr:change},inplace=True)
need_df.to_csv('D:/郑州日线官方历史数据/clean_汇总数据.csv',index=False)