import pandas as pd
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
from selenium.webdriver import ActionChains


# 查询数据库对应表，转化为dataframe
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

    name_1 = '['+table_name+']' if '-' in table_name else table_name
    # SQL query to get the data from the table
    query = f"SELECT * FROM {name_1};"  # sql语言查询所有数据

    # Use pandas to read the data from the database into a DataFrame
    df = pd.read_sql(query, engine)
    return df

#将本地添加到本地数据库(一直未用)
def append_df_to_db_1(df,table_name):
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
        name_1 = '['+table_name+']' if '-' in table_name else table_name
        #session = sessionmaker(bind=engine)
        with engine.connect() as connection:
            delete_query = f"DROP TABLE {name_1}"
            #delete_query = f"DELETE FROM {name_1}"
            connection.execute(delete_query)
        df.to_sql(con=engine,name=table_name,if_exists='append',index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")
    except:
        df.to_sql(con=engine,name=table_name,if_exists='append',index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")

#将本地添加到阿里云数据库
def append_df_to_db_2(df,table_name):
    '''
    Input:
        df: 即将存入database的dataframe,要求和database的字段结构一样,否则会报错！(如果存在自增主键,可以不用管)
        table_name: 数据库下即将要插入的表名
    Output: 先尝试删除原表，然后再将本地保存的dataframe添加到数据库，若是原表不能删除(百分之99是因为不存在这个表，另外的百分之一的可能是因为数据库的表名不能出现' '以及'-'等符号)，则直接添加表就行。
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

    try:
        name_1 = '['+table_name+']' if '-' in table_name else table_name
        #session = sessionmaker(bind=engine)
        with engine.connect() as connection:
            delete_query = f"DROP TABLE {name_1}"
            #delete_query = f"DELETE FROM {name_1}"
            connection.execute(delete_query)
        df.to_sql(con=engine,name=table_name,if_exists='append',index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")
    except:
        df.to_sql(con=engine,name=table_name,if_exists='append',index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")

# 爬虫爬取对应的数据
def get_text(url,name):
    """
    Input:
        url:需要爬取的网站链接
        name:investing总表中对应数据库的表名
    Output:
        爬取的数据返回未TXT
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
    time.sleep(3)

    #鼠标点击操作(显示更多)
    for i in range(1):
        try:
            click_element = driver.find_element(By.XPATH, f'//*[@id="showMoreHistory{xpath_num}"]/a'.format(xpath_num))
            click_element.click()
            action.key_down(Keys.DOWN).key_up(Keys.DOWN).perform() #鼠标往下滚动一点点
            time.sleep(1)
        except:
            print(f'{name}找不到"显示更多"这个按键,xpath路径可能不对!!')
            pass

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
    with open(rf"总表数据\初始txt文档\{name}.txt", "w", encoding="utf-8") as f:  #覆盖写，最初始的txt文档
        f.write(data_text)
    print(f'{name}.txt  已保存到本地')

# 正则表达式替换函数1(替换其中的英文时间)
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

# 正则表达式替换函数2
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

#本地的txt文件和数据库文件进行合并，合并后并保存到本地的csv文件夹下
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
        try:
            with open(path_, "r", encoding="utf-8") as f:
                text = f.read()
                text = replace_time(text)
                text = replace_spaces(text)

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
                if len(k) >= 6:
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

            # 处理好后的数据与数据库进行合并
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
                df_3.to_csv(rf'总表数据\处理好后的csv\{name}.csv', index=False)
            # 如果数据库没有文件的话，就直接保存df_2
            except:
                #一定要注意，如果数据库存在数据，而未被读到本地的情况下，贸然输出df_2会替换掉数据库的数据，新增表的时候才建议打开这行代码！！
                #df_2.sort_values(by='datetime', ascending=False, inplace=True)
                #df_2.to_csv(rf'总表数据\处理好后的csv\{name}.csv',index=False)
                print(f'{name}没有与数据库合并！！')
                pass
        except:
            print(f'{name}表存在错误！！请检查')


# ========主程序
#一定要在爬虫运行前进行一个备份！！最好手动备份一份文件


# 获取需要爬虫爬取的url地址和文件名称
base_url = 'https://cn.investing.com/economic-calendar/'
df = pd.read_excel(r'./investing彭博字段表_最终版(新版).xlsx', sheet_name='总表')
df = df[~df['INVESTING_CN_INDEX'].str.contains('国债收益率')]
df['INVESTING_INDEX'] = base_url + df['INVESTING_INDEX']
urllst = df['INVESTING_INDEX'].tolist()
titlelst = df['TABLE_NAME'].tolist()


# 储存txt文件
with ThreadPoolExecutor(max_workers=6) as t:  # 创建一个最大容纳数量为6的线程池
    for i, j ,k in zip(urllst, titlelst,range(len(urllst))):
        task = t.submit(get_text, i, j)
        print(f"task{k + 1}: {task.done()}")  # 监督进度

path_1 = rf'总表数据\初始txt文档'
txt_to_csv(path_1)


#csv存入数据库
path_2 = rf'总表数据\备份csv'
path_list = [os.path.join(path_2,i) for i in os.listdir(path_2)]
for path_ in path_list:
    df = pd.read_csv(path_)
    df.replace('None', None, inplace=True) #将字符串None替换为空
    df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S') #时间格式转化为指定格式的字符串
    df.drop_duplicates(subset='datetime', inplace=True) #去重
    df.sort_values('datetime', ascending=False, inplace=True) #排序
    name = path_.split('\\')[-1][:-4]
    try:
        append_df_to_db_2(df,name)
        append_df_to_db_1(df,name)
    except:
        print(f'{name}未加载到数据库！！请注意检查！！')