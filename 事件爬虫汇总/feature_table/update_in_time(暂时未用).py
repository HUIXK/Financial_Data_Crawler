import pandas as pd
import numpy as np
import warnings
import time
import os
import re

#多线程运行
from concurrent.futures import ThreadPoolExecutor

# 忽略所有警告
warnings.filterwarnings("ignore")

from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver import ActionChains
from sqlalchemy.orm import sessionmaker
from datetime import datetime




# 从服务器读取数据到本地
def read_db_df(table_name):
    """
    Input: table_name(数据库下的表名)   !!注意:数据库名字的修改在engine_params中修改database!!
    Output: df(数据库中的表转化为dataframe)
    """
    engine_params = {
        'dialect': 'mssql',
        'driver': 'pymssql',
        'username': 'sa',
        'password': 'Goldriven123',
        'host': 'sh1.goldriven.com',
        'port': 1433,
        'database': 'KG_FCT_ECO_DA'
    }

    # Create the SQLAlchemy engine
    engine = create_engine(
        "{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}?charset=utf8"
        .format(**engine_params))

    # SQL query to get the data from the table
    query = f"SELECT * FROM {table_name};"  # sql语言查询所有数据

    # Use pandas to read the data from the database into a DataFrame
    df = pd.read_sql(query, engine)
    return df

# 将本地添加到阿里云数据库
def append_df_to_db_2(df,table_name):
    '''
    Input:
        df: 即将存入database的dataframe,要求和database的字段结构一样,否则会报错！(如果存在自增主键,可以不用管)
        table_name: 数据库下即将要插入的表名
    Output: None
    '''
    engine_params = {
        'dialect': 'mssql',
        'driver': 'pymssql',
        'username': 'sa',
        'password': 'Goldriven123',
        'host': 'sh1.goldriven.com',
        'port': 1433,
        'database': 'KG_FCT_ECO_DA'
    }
    engine = create_engine(
        "{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}?charset=utf8"
        .format(**engine_params))

    #session = sessionmaker(bind=engine)
    with engine.connect() as connection:
        delete_query = f"DELETE FROM {table_name}"
        connection.execute(delete_query)

    df.to_sql(con=engine,name=table_name,if_exists='append',index=False)
    print(f"{table_name} is successfully stored in the database: {engine_params['database']}")


# 正则表达式替换
def replace_spaces(text):
    # 替换其中的K等数值
    text = re.sub(r"[%,K,k,M]", "", text)
    #替换其中的括号
    text = re.sub(r"[年,月]", "-", text)
    #替换其中的括号
    text = re.sub(r" \(.*\)|日", "", text)
    #替换连续五个空格的情况
    text = re.sub(r'(?<= ) {2}(?= ) {2}', 'None None ', text)
    #替换连续三个空格的情况
    text = re.sub(r'(?<= ) {3}(?= )', 'None', text)
    return text


# 爬虫爬取对应的数据
def get_text(url,name):
    """
    Input:
        url:需要爬取的网站链接
        name:investing总表中对应数据库的表名
    Output:
        爬取的
    """
    # 浏览器设置
    options = webdriver.ChromeOptions()  # 谷歌浏览器
    options.page_load_strategy = 'eager'
    options.add_argument("blink-settings=imagesEnabled=false")  # 不加载图片
    options.add_argument("--disable-extensions")  # 禁用插件加载
    options.add_argument("--disable-gpu")  # 禁止gpu
    #options.add_argument("--disable-software-rasterizer")  # 无界面
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")  # 窗口全屏显示
    driver = webdriver.Chrome(options=options)
    driver.get(url)


    # 注意，加载更多这个按钮的xpath定位与url有关系
    xpath_num = url.split('-')[-1]
    action = ActionChains(driver) #定义鼠标的操作

    #鼠标点击操作(显示更多)
    # for i in range(3):
    #     try:
    #         click_element = driver.find_element(By.XPATH, f'//*[@id="showMoreHistory{xpath_num}"]/a'.format(xpath_num))
    #         click_element.click()
    #         action.key_down(Keys.DOWN).key_up(Keys.DOWN).perform() #鼠标往下滚动一点点
    #         time.sleep(1)
    #     except:
    #         print('找不到"显示更多"这个按键,xpath路径可能不对!!')
    #         pass


    #获得正文文本,存在的xpath路径太多太多了！！这部分特别重要！！
    try:
        data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[12]/table')
        data_text=data_element.text
    except:
        try:
            data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[9]/table')
            data_text=data_element.text
        except:
            try:
                data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[10]/table')
                data_text=data_element.text
            except:
                try:
                    data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[11]/table')
                    data_text=data_element.text
                except:
                    try:
                        data_element = driver.find_element(By.XPATH, '/html/body/div[6]/section/div[12]/table')
                        data_text = data_element.text
                    except:
                        try:
                            data_element = driver.find_element(By.XPATH, '/html/body/div[6]/section/div[9]/table')
                            data_text = data_element.text
                        except:
                            try:
                                data_element = driver.find_element(By.XPATH,
                                                                    '/html/body/div[6]/section/div[10]/table')
                                data_text = data_element.text
                            except:
                                data_element = driver.find_element(By.XPATH,
                                                                    '/html/body/div[6]/section/div[11]/table')
                                data_text = data_element.text
    
    # 保存初始文档在本地
    with open(rf"../data/TXT/{upcoming_event}/{name}.txt", "w", encoding="utf-8") as f:  #覆盖写，最初始的txt文档
        f.write(data_text)
    print(f'{name}.txt  已保存到本地')

def txt_to_csv(path):
    '''
    Input:
        path:文件夹路径,该路径下为爬取到的初始txt文档
    Output:
        return:None
    '''
    # 获取每个txt文件的完整地址
    path_list = [os.path.join(path,i) for i in os.listdir(path)]
    for path_ in path_list:
        # 可能有些文件完全没有值，暂时不用管
        try:
            with open(path_, "r", encoding="utf-8") as f:
                text = f.read()
                text = replace_spaces(text)  #按照要求进行替换，替换后就可以关闭文件了

            # 获取表名
            name = path_.split('\\')[-1][:-4]
            print(name)

            # 读取修正后的txt文档
            df_1 = pd.DataFrame([i.split(' ') for i in text.split('\n')])
            df_1[0] = df_1[0] + ' ' + df_1[1] #合并日期和时间列
            df_1.drop(1, axis=1, inplace=True) #删除时间列

            # 判断是否存在p值
            p_list = []
            for k in [i.split(' ') for i in text.split('\n')]:
                if len(k) >= 7:
                    p_list.append(1)
                else:
                    p_list.append(0)

            # 定义旗帜
            flag = 1 in p_list
            if flag:
                df_2 = pd.DataFrame(columns=['datetime',
                                        f'{name}'+'_Actual',
                                        f'{name}'+'_Srv_Med',
                                        f'{name}'+'_Revised',
                                        f'{name}'+'_Prior',
                                        'Is_P']
                                        )
            else:
                df_2 = pd.DataFrame(columns=['datetime',
                                f'{name}'+'_Actual',
                                f'{name}'+'_Srv_Med',
                                f'{name}'+'_Revised',
                                f'{name}'+'_Prior']
                                )

            # 如果判断出是带p值则如下操作
            if flag:
                for index, value in enumerate(p_list):
                    if value == 0:
                        if df_1.loc[index, 5] == None and df_1.loc[index, 6] == None:
                            datetime = df_1.loc[index, 0]
                            Actual = df_1.loc[index, 2]
                            Srv_Med = df_1.loc[index, 3]
                            Revised = df_1.loc[index, 4]
                            Prior = None
                            Is_P = value
                            df_2.loc[len(df_2)] = [datetime, Actual, Srv_Med, Revised, Prior, Is_P]
                        else:
                            list_1 = df_1.loc[index].to_list()
                            while list_1[-1] == None:
                                list_1.pop()
                            datetime = df_1.loc[index, 0]
                            Revised = list_1.pop()
                            Srv_Med = list_1.pop()
                            Actual = list_1.pop()
                            Prior = None
                            Is_P = value
                            df_2.loc[len(df_2)] = [datetime, Actual, Srv_Med, Revised, Prior, Is_P]
                    else:
                        datetime = df_1.loc[index, 0]
                        Actual = df_1.loc[index, 4]
                        Srv_Med = df_1.loc[index, 5]
                        Revised = df_1.loc[index, 6]
                        Prior = None
                        Is_P = value
                        df_2.loc[len(df_2)] = [datetime, Actual, Srv_Med, Revised, Prior, Is_P]
            # 如果不带p值则如下操作：
            else:
                for index, value in enumerate(p_list):
                    datetime = df_1.loc[index, 0]
                    Actual = df_1.loc[index, 2]
                    Srv_Med = df_1.loc[index, 3]
                    Revised = df_1.loc[index, 4]
                    Prior = None
                    # Is_P = value
                    df_2.loc[len(df_2)] = [datetime, Actual, Srv_Med, Revised, Prior]

            # 对于带p和不带p的前值字段有不同处理方法!
            if flag:
                df_2 = df_2[1:]  # 删除掉第一行
                df_5 = df_2[df_2['Is_P'] == 0]
                df_6 = df_2[df_2['Is_P'] == 1]
                for l in range(len(df_5) - 1):
                    df_5.iloc[l, 4] = df_5.iloc[l + 1, 1]
                for l in range(len(df_6) - 1):
                    df_6.iloc[l, 4] = df_6.iloc[l + 1, 1]
                df_2 = pd.concat([df_5, df_6])
                df_2.sort_index(inplace=True)
            else:
                df_2 = df_2[1:]
                df_2.reset_index(inplace=True, drop=True)
                df_2.reindex()
                for l in range(len(df_2) - 1):
                    df_2.iloc[l, 4] = df_2.iloc[l + 1, 1]

            #如果本地文件不存在则会报错
            try:
                df_3 = read_db_df(name)
                df_3 = pd.concat([df_2,df_3])
                df_3 = df_3.drop_duplicates(subset = 'datetime')
                if flag:
                    df_5 = df_3[df_3['Is_P'] == 0]
                    df_6 = df_3[df_3['Is_P'] == 1]
                    for l in range(len(df_5) - 1):
                        df_5.iloc[l, 4] = df_5.iloc[l + 1, 1]
                    for l in range(len(df_6) - 1):
                        df_6.iloc[l, 4] = df_6.iloc[l + 1, 1]
                    df_3 = pd.concat([df_5, df_6])
                    df_3.sort_index(inplace=True)
                else:
                    df_3.reset_index(inplace=True, drop=True)
                    df_3.reindex()
                    for l in range(len(df_3) - 1):
                        df_3.iloc[l, 4] = df_3.iloc[l + 1, 1]
                df_3.to_csv(rf'../data/CSV/{upcoming_event}/{name}.csv', index=False)
            # 如果没有本地文件的话，就保存df_2
            except:
                df_2.to_csv(rf'../data/CSV/{upcoming_event}/{name}.csv',index=False)
        except:
            print(f'{name}存在问题，请注意！')
            pass


# =====主程序
# 只要今年没结束，程序就会一直运行
while (pd.to_datetime('2023-12-31') - datetime.now()).total_seconds() > 0:
    event_path = r"C:\Users\Dell\Desktop\EP\trading_calendar\trading_calendar.csv"
    event_df = pd.read_csv(event_path)
    event_df['datetime'] = pd.to_datetime(event_df['datetime'])
    upcoming_time = event_df[event_df['datetime'] > datetime.now()].min()['datetime']
    upcoming_event = event_df[event_df['datetime'] == upcoming_time]['model'].values[0]
    # 只要最近的事件未发生，则一直循环打印时间！
    while (pd.to_datetime('2023-08-09 12:27:59.518661') - datetime.now()).total_seconds() > 0:
    #while (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 0:
        if (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 5*60:
            print(f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}")
            time.sleep(5)
        elif (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() < 5*60:
            print(f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}")
            time.sleep(3)
        else:
            pass

    # 超过了事件时间，进行抓取
    df = pd.read_excel(r'investing彭博字段表_最终版.xlsx',sheet_name='EP_EVENT(最新)')
    df['Model_Name'].fillna(method='ffill',inplace=True)
    base_url = 'https://cn.investing.com/economic-calendar/'
    list_url = [base_url+i for i in df[df['Model_Name'] == upcoming_event]['Investing_Id']] #需要爬虫的url列表
    list_table_name = df[df['Model_Name'] == upcoming_event]['Table_Name'].tolist() #数据库表名

    #储存txt文件
    with ThreadPoolExecutor(max_workers=8) as t:  # 创建一个最大容纳数量为3的线程池
        for i, j ,k in zip(list_url, list_table_name,range(len(list_url))):
            task = t.submit(get_text, i, j)
            print(f"task{k+1}: {task.done()}")  # 监督进度

    # txt转化为csv
    path_1 = rf'../data/TXT/{upcoming_event}'
    txt_to_csv(path_1)

    # 原表csv存入数据库
    path_2 = rf'../data/CSV/{upcoming_event}'
    path_list = [os.path.join(path_2,i) for i in os.listdir(path_2)]
    for path_ in path_list:
        df = pd.read_csv(path_)
        df.replace('None', None, inplace=True)
        name = path_.split('\\')[-1][:-4]
        try:
            append_df_to_db_2(df,name)
        except:
            print(f'{name}未加载到数据库！！请注意检查！！')
            pass