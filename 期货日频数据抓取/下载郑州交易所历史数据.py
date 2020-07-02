import os
import requests
def download_czce_history_zip(years:list,save_path:str):
    '''
    years:[2010,2011,2021]
    save_path:储存路径
    下载地址的有两个变化
    '''
    if not os.path.exists(save_path):
        
        os.makedirs(save_path)
    
    for year in years:  
        if year >=2015:
            year=str(year)
            download_url='http://www.czce.com.cn/cn/DFSStaticFiles/Future/'+year+'/FutureDataHistory.zip'
        else:
            year=str(year)
            download_url='http://www.czce.com.cn/cn/exchange/datahistory'+year+'.zip'
        data=requests.get(download_url)
        output=open(save_path+'FutureDataHistory_'+year+'.zip','wb')
        output.write(data.content)
        output.close()
        print(f'{year}年zip数据下载完成')
download_czce_history_zip([x for x in range(2010,2021)],'D:/test_zip/')