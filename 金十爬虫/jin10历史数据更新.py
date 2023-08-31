import datetime
import json
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 从服务器读取数据到本地
def read_db_df(table_name):
    engine_params = {
        'dialect': 'mssql',
        'driver': 'pymssql',
        'username': 'sa',
        'password': 'Goldriven123',
        'host': 'sh1.goldriven.com',
        'port': 1433,
        'database': 'BANK_PLT'
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


# 存入数据库
def append_df_to_db_2(df,table_name):
    engine_params = {
        'dialect': 'mssql',
        'driver': 'pymssql',
        'username': 'sa',
        'password': 'Goldriven123',
        'host': 'sh1.goldriven.com',
        'port': 1433,
        'database': 'JINSHI_FCT'
    }
    engine = create_engine(
        "{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}?charset=utf8"
        .format(**engine_params))
    
    # 尝试清空原表格数据
    try:
        name_1 = '['+table_name+']' if '-' in table_name else table_name
        #session = sessionmaker(bind=engine)
        with engine.connect() as connection:
            #delete_query = f"DELETE FROM {name_1}"
            delete_query = f"DROP TABLE {name_1}"
            connection.execute(delete_query)
        df.to_sql(con=engine,name=table_name,if_exists='append',index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")
    except:
        df.to_sql(con=engine,name=table_name,if_exists='append',index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")


# 按照要求新建金十本地的csv
def create(path = r'./data'):
    df= pd.read_excel('./Jin10_Calendar.xlsx',sheet_name='EP_EVENT(金十)')
    df['Model_Name'].fillna(method='ffill', inplace=True)
    df.head()
    for Model,unit,name in zip(df['Model_Name'],df['unit'],df['Table_name']):
        try:
            os.mkdir(os.path.join(path,Model))
            df = pd.DataFrame(columns=['datetime',f'{name}_Actual',f'{name}_Srv_Med',f'{name}_Revised',f'{name}_Prior','unit'])
            df.to_csv(rf'./data/{Model}/{name}.csv',index=False)
        except:
            df = pd.DataFrame(columns=['datetime',f'{name}_Actual',f'{name}_Srv_Med',f'{name}_Revised',f'{name}_Prior','unit'])
            df.to_csv(rf'./data/{Model}/{name}.csv',index=False)



# 主程序爬虫
def main():
    # 国际时间转化为北京时间
    def trs_time(x):
        datetime_object = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ')
        datetime_object = datetime_object + timedelta(hours=8)  # 转化为北京时间
        return datetime_object

    #读取数据
    #df_ = pd.read_excel('Jin10_Calendar.xlsx')  #本地读取
    df_ = read_db_df('financial_calendar')  #数据库读取
    df_['pub_time'] = df_['pub_time'].apply(trs_time)  #转化为北京时间
    df_.sort_values('pub_time',ascending=True,inplace=True)  #按照时间排序
    df_ = df_[df_['pub_time']>pd.to_datetime('2022-01-01')]  #筛选出年份大于2022年的数据
    df_ = df_[df_['pub_time']<datetime.now()]  #筛选出年份小于现在的数据
    feature_df = pd.read_excel('Jin10_Calendar.xlsx',sheet_name='EP_EVENT(金十)')
    feature_df['Model_Name'] = feature_df['Model_Name'].fillna(method='ffill')

    # 通过唯一值id来进行查找
    for i in feature_df['id']:
        df = pd.read_csv(f'./data/{feature_df[feature_df.id == i].Model_Name.values[0]}/{feature_df[feature_df.id == i].Table_name.values[0]}.csv')
        df_2 = df_[df_['indicator_id']==i][['pub_time','actual','consensus','revised','previous','unit']].rename(columns=
                                                                                                {'pub_time':'datetime',
                                                                                                'actual':f'{feature_df[feature_df.id == i].Table_name.values[0]}_Actual',
                                                                                                'consensus':f'{feature_df[feature_df.id == i].Table_name.values[0]}_Srv_Med',
                                                                                                'revised':f'{feature_df[feature_df.id == i].Table_name.values[0]}_Revised',
                                                                                                'previous':f'{feature_df[feature_df.id == i].Table_name.values[0]}_Prior'})
        df = pd.concat([df_2,df])
        df.drop_duplicates(subset='datetime',inplace=True) #去重
        df.sort_values('datetime',ascending=False,inplace=True) #排序
        df.to_csv(f'./data/{feature_df[feature_df.id == i].Model_Name.values[0]}/{feature_df[feature_df.id == i].Table_name.values[0]}.csv',index=False)
        print(f'已完成{feature_df[feature_df.id == i].Model_Name.values[0]}>>>>>>>>{feature_df[feature_df.id == i].jinshi_Name.values[0]}')
    


## 主程序
if __name__ == '__main__':
    # 注意切换路径到当前文件夹
    # path_ = r'C:\Users\Dell\Desktop\金十爬虫'
    # os.chdir(path_)
    # print(os.getcwd())

    # 如果存在本地文件，则不需要创建本地文件
    create(path = r'./data')

    # 运行主程序  
    main()
    
    # 添加到数据库
    # for path_ in os.listdir(r'./data'):
    #     path_2 = os.path.join(r'./data', path_)
    #     path_list = [os.path.join(path_2, i) for i in os.listdir(path_2)]
    #     for path__ in path_list:
    #         name = path__.split('\\')[-1][:-4]
    #         df = pd.read_csv(path__)
    #         try:
    #             append_df_to_db_2(df, name)
    #         except:
    #             print(f'{name}未加载到数据库！！请注意检查！！')
                