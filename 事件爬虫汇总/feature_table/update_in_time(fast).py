import pandas as pd
import numpy as np
import warnings
import time
import os
import re

# 多线程运行
from concurrent.futures import ThreadPoolExecutor, as_completed

# 忽略所有警告
warnings.filterwarnings("ignore")

from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver import ActionChains
from selenium.webdriver.common.action_chains import ActionChains
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
    name_1 = '[' + table_name + ']' if '-' in table_name else table_name
    # SQL query to get the data from the table
    query = f"SELECT * FROM {name_1};"  # sql语言查询所有数据

    # Use pandas to read the data from the database into a DataFrame
    df = pd.read_sql(query, engine)
    return df

# 将本地添加到阿里云数据库
def append_df_to_db_1(df, table_name):
    '''
    Input:
        df: 存入database的dataframe,要求和database的字段结构一样,否则会报错！
        table_name: 表名
    Output:
        return:None
    '''
    engine_params = {
        'dialect': 'mssql',
        'driver': 'pymssql',
        'username': 'DAUser2',
        'password': 'Xpal290290',
        'host': 'WIN-CPE38BHN8G8',
        'port': 1433,
        'database': 'KG_FCT_ECO_DA'
    }
    engine = create_engine(
        "{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}?charset=utf8"
        .format(**engine_params))
    try:
        name_1 = '[' + table_name + ']' if '-' in table_name else table_name
        # session = sessionmaker(bind=engine)
        with engine.connect() as connection:
            delete_query = f"DROP TABLE {name_1}"
            #delete_query = f"DELETE FROM {name_1}"
            connection.execute(delete_query)
        df.to_sql(con=engine, name=table_name, if_exists='append', index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")
    except:
        df.to_sql(con=engine, name=table_name, if_exists='append', index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")


# 将本地添加到阿里云数据库
def append_df_to_db_2(df, table_name):
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

    session = sessionmaker(bind=engine)
    try:
        name_1 = '['+table_name+']' if '-' in table_name else table_name
        with engine.connect() as connection:
            delete_query = f"DELETE FROM {name_1}"
            connection.execute(delete_query)
        df.to_sql(con=engine, name=table_name, if_exists='append', index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")
    except:
        df.to_sql(con=engine, name=table_name, if_exists='append', index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")

# 正则表达式替换函数1
def replace_time(text):
    # map映射
    month_mapping = {
        "Jan": "1月",
        "Feb": "2月",
        "Mar": "3月",
        "Apr": "4月",
        "May": "5月",
        "Jun": "6月",
        "Jul": "7月",
        "Aug": "8月",
        "Sep": "9月",
        "Oct": "10月",
        "Nov": "11月",
        "Dec": "12月"
    }
    pattern = r"([A-Za-z]{3} \d{2}, \d{4} )(\d{2}:\d{2})"
    def replace_with_chinese(match):
        # 定义正则表达式模式
        date_part, time_part = match.groups()
        month, day, year = re.match(r"([A-Za-z]{3}) (\d{2}), (\d{4})", date_part).groups()
        month_chinese = month_mapping[month]
        chinese_date = f"{year}年{month_chinese}{day}日"
        return chinese_date + f" {time_part}"

    return re.sub(pattern, replace_with_chinese, text)

# 正则表达式替换函数
def replace_spaces(text):
    # 替换其中的K等数值
    text = re.sub(r"[%,K,k,M]", "", text)
    # 替换其中的括号
    text = re.sub(r"[年,月]", "-", text)
    # 替换其中的括号
    text = re.sub(r" \(.*\)|日", "", text)
    # 替换连续六个空格的情况
    text = re.sub(r'\s{6}', ' None None None', text)
    # 替换连续五个空格的情况
    text = re.sub(r'\s{5}', ' None None ', text)
    # 替换连续四个空格的情况
    text = re.sub(r'\s{5}', ' None None', text)
    # 替换连续三个空格的情况
    text = re.sub(r'\s{3}', ' None ', text)
    return text



# =====主程序
# 只要今年没结束，程序就会一直运行
while (pd.to_datetime('2023-12-31') - datetime.now()).total_seconds() > 0:
    event_path = r"../trading_calendar/trading_calendar.csv"
    event_df = pd.read_csv(event_path)
    event_df['datetime'] = pd.to_datetime(event_df['datetime'])
    upcoming_time = event_df[event_df['datetime'] > datetime.now()].min()['datetime']
    upcoming_event = event_df[event_df['datetime'] == upcoming_time]['model'].values[0]
    #upcoming_event = 'GEPMI'
    #upcoming_time = datetime.now()+pd.Timedelta(seconds=130)
    #upcoming_time = pd.to_datetime('2023-08-23 15:30')
    # 只要最近的事件未发生，则一直循环打印时间！
    while (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 0:
        if (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 60*8:
            print(f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}")
            time.sleep(3)
        else:
            print('已打开浏览器随时待命！！')
            df = pd.read_excel(r'investing彭博字段表_最终版.xlsx', sheet_name='EP_EVENT(最新)')
            df['Model_Name'].fillna(method='ffill', inplace=True)
            base_url = 'https://cn.investing.com/economic-calendar/'
            list_url = [base_url + i for i in df[df['Model_Name'] == upcoming_event]['Investing_Id']]
            list_table_name = df[df['Model_Name'] == upcoming_event]['Table_Name'].tolist()
            options = webdriver.ChromeOptions()  # 谷歌浏览器
            options.page_load_strategy = 'eager'
            options.add_experimental_option("detach", True)
            options.add_argument("blink-settings=imagesEnabled=false")  # 不加载图片
            options.add_argument("--disable-extensions")  # 禁用插件加载
            options.add_argument("--disable-gpu")  # 禁止gpu
            # options.add_argument("--disable-software-rasterizer")  # 无界面
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument("--start-maximized")  # 窗口全屏显示
            drivers = [webdriver.Chrome(options=options) for _ in range(len(list_url))]  # 创建浏览器实例列表
            for driver, url in zip(drivers, list_url):  # 同时打开所有的浏览器
                driver.get(url)
            while (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 0:
                print(f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}\t >>>>>>>>>>> \t剩余时间{(pd.to_datetime(upcoming_time) - datetime.now()).total_seconds()}")
                time.sleep(2)
    # 超过了事件时间，进行抓取
    while True:
        flag_list = []
        # 保存csv到本地文件
        for driver, url ,name in zip(drivers, list_url, list_table_name):
            # 获得正文文本,存在的xpath路径太多太多了！！这部分特别重要！！
            try:
                data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[12]/table')
                data_text = data_element.text
            except:
                try:
                    data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[9]/table')
                    data_text = data_element.text
                except:
                    try:
                        data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[10]/table')
                        data_text = data_element.text
                    except:
                        try:
                            data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[11]/table')
                            data_text = data_element.text
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
            text = replace_time(data_text)
            text = replace_spaces(text)  # 按照要求进行替换，替换后就可以关闭文件了
            #driver.quit()
            # 读取修正后的txt文档
            df_1 = pd.DataFrame([i.split(' ') for i in text.split('\n')])
            df_1[0] = df_1[0] + ' ' + df_1[1]  # 合并日期和时间列
            df_1.drop(1, axis=1, inplace=True)  # 删除时间列

            # 判断是否存在p值
            p_list = []
            for k in [i.split(' ') for i in text.split('\n')]:
                if len(k) >= 6:
                    p_list.append(1)
                else:
                    p_list.append(0)

            # 定义旗帜
            flag = 1 in p_list
            if flag:
                df_2 = pd.DataFrame(columns=['datetime',
                                             f'{name}' + '_Actual',
                                             f'{name}' + '_Srv_Med',
                                             f'{name}' + '_Revised',
                                             f'{name}' + '_Prior',
                                             'Is_P']
                                    )
            else:
                df_2 = pd.DataFrame(columns=['datetime',
                                             f'{name}' + '_Actual',
                                             f'{name}' + '_Srv_Med',
                                             f'{name}' + '_Revised',
                                             f'{name}' + '_Prior']
                                    )

            # 如果判断出是带p值则如下操作
            if flag:
                for index, value in enumerate(p_list):
                    if value == 0:
                        datetime = df_1.loc[index, 0]
                        Actual = df_1.loc[index, 2]
                        Srv_Med = df_1.loc[index, 3]
                        Revised = df_1.loc[index, 4]
                        Prior = None
                        Is_P = value
                        df_2.loc[len(df_2)] = [datetime, Actual, Srv_Med, Revised, Prior, Is_P]
                    else:
                        datetime = df_1.loc[index, 0]
                        Actual = df_1.loc[index, 3]
                        Srv_Med = df_1.loc[index, 4]
                        Revised = df_1.loc[index, 5]
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
                df_2['datetime'] = pd.to_datetime(df_2['datetime'])  # 转化为时间格式
            else:
                df_2 = df_2[1:]
                df_2.reset_index(inplace=True, drop=True)
                df_2.reindex()
                for l in range(len(df_2) - 1):
                    df_2.iloc[l, 4] = df_2.iloc[l + 1, 1]
                df_2['datetime'] = pd.to_datetime(df_2['datetime'])  # 转化为时间格式

            # 如果本地文件不存在则会报错
            try:
                df_3 = read_db_df(name)
                df_3['datetime'] = pd.to_datetime(df_3['datetime'])  #转化为时间格式
                df_3 = pd.concat([df_2,df_3])
                df_3.drop_duplicates(subset = 'datetime',inplace=True)
                df_3.sort_values('datetime', ascending=False, inplace=True)  # 排序
                df_3.reset_index(drop=True)  # 重设索引
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
                df_3.sort_values('datetime', ascending=False, inplace=True)  # 排序
                df_3.to_csv(rf'../data/CSV/{upcoming_event}/{name}.csv', index=False)
            # 如果没有本地文件的话，就保存df_2
            except:
                #df_2.to_csv(rf'../data/CSV/{upcoming_event}/{name}.csv', index=False)
                print(f'{name}没有与数据库合并！！')
                pass
            driver.refresh()  # 老是不出现，需要刷新一下
            #time.sleep(3)

        # 原表csv存入数据库
        path_2 = rf'../data/CSV/{upcoming_event}'
        path_list = [os.path.join(path_2, i) for i in os.listdir(path_2)]
        for path_ in path_list:
            df = pd.read_csv(path_)
            df.replace('None', None, inplace=True)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.drop_duplicates(subset='datetime')
            name = path_.split('\\')[-1][:-4]
            if df[df['datetime'] == upcoming_time][f'{name}_Actual'].values[0] == None:
            #if df.iloc[1,1] == None:
                flag_list.append(0)
            else:
                flag_list.append(1)
            try:
                append_df_to_db_2(df, name)
                append_df_to_db_1(df, name)
            except:
                print(f'{name}未加载到数据库！！请注意检查！！')
                pass

        # 更新feature标签并保存到数据库
        try:
            path_ = rf'../data/CSV/'
            path = os.path.join(path_, upcoming_event)
            path_list = [os.path.join(path, i) for i in os.listdir(path)]
            df_list = []
            for path__ in path_list:
                df = pd.read_csv(path__)
                df['datetime'] = pd.to_datetime(df['datetime'])
                df['datetime'] = df['datetime'].dt.date
                try:
                    df.drop('Is_P', axis=1, inplace=True)
                except:
                    pass
                df_list.append(df)
            df = df_list[0]
            for k in range(1, len(df_list)):
                df = pd.merge(df, df_list[k], on='datetime', how='inner')
            df.replace('None', 0, inplace=True)
            df.fillna(0, inplace=True)
            df_new = pd.DataFrame({'date': df['datetime'], 'feature': None})
            df_new['feature'] = df.iloc[:, 1:].apply(lambda row: ','.join(row.astype(str)), axis=1)
            try:
                append_df_to_db_2(df_new,upcoming_event)
                append_df_to_db_1(df_new,upcoming_event)
            except:
                print(f'{upcoming_event}未存入数据库，请注意检查')
                pass
            df_new.to_csv(rf'../data/CSV/EVENT/{upcoming_event}.csv', index=False)
        except:
            print(f'{upcoming_event}的feature有问题，请注意检查')
            pass

        # 检查时候跳出循环
        if (pd.Series(flag_list) == 1).all():
            print(f'{upcoming_event}抓取结束，已存入数据库，请检查！')
            break
        else:
            print('抓取失败，还未出现实时更新的值！')
            time.sleep(3)
    break





