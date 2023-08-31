# -*- coding: utf-8 -*-
import pandas as pd
import requests
import json
import random
import warnings
import time
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")


# 将本地添加到阿里云数据库
def append_df_to_db_2(df, table_name):
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

    session = sessionmaker(bind=engine)
    try:
        name_1 = '[' + table_name + ']' if '-' in table_name else table_name
        with engine.connect() as connection:
            delete_query = f"DROP TABLE {name_1}"
            # delete_query = f"DELETE FROM {name_1}" #清空数据，表空则不能清空
            connection.execute(delete_query)
        df.to_sql(con=engine, name=table_name, if_exists='append', index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")
    except:
        df.to_sql(con=engine, name=table_name, if_exists='append', index=False)
        print(f"{table_name} is successfully stored in the database: {engine_params['database']}")


def main():
    # 国际时间转化为北京时间
    def trs_time(x):
        datetime_object = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ')
        datetime_object = datetime_object + timedelta(hours=8)  # 转化为北京时间
        return datetime_object

    event_path = r"trading_calendar.csv"
    event_df = pd.read_csv(event_path)
    event_df['datetime'] = pd.to_datetime(event_df['datetime'])
    upcoming_time = event_df[event_df['datetime'] > datetime.now()].min()['datetime']  # 事件时间
    upcoming_event = event_df[event_df['datetime'] == upcoming_time]['model'].values[0]  # 事件名称
    upcoming_time = pd.to_datetime('8/30/2023 22:30')
    upcoming_event = 'EIA'

    feature_df = pd.read_excel(r'Jin10_Calendar.xlsx', sheet_name='EP_EVENT(金十)')
    feature_df['Model_Name'] = feature_df['Model_Name'].fillna(method='ffill')
    feature_df = feature_df[feature_df['Model_Name'] == upcoming_event]

    year = str(upcoming_time.year)
    month = str(upcoming_time.month).zfill(2)
    day = str(upcoming_time.day).zfill(2)
    url = rf'https://cdn-rili.jin10.com/web_data/{year}/daily/{month}/{day}/economics.json'

    # 等待事件到来
    while (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 0:
        if (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 80:
            print(
                f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}")
            time.sleep(random.randint(30, 40))
        else:
            print(
                f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}")
            time.sleep(random.randint(1, 3))

    while True:
        flag_list = []

        response = requests.get(url)
        data = json.loads(response.text)
        df_json = pd.DataFrame(data)
        df_json['pub_time'] = df_json['pub_time'].apply(trs_time)

        for i in feature_df['jinshi_id']:
            df = pd.read_csv(f'./data/{feature_df[feature_df.jinshi_id == i].Model_Name.values[0]}/{feature_df[feature_df.jinshi_id == i].Table_name.values[0]}.csv',encoding='ISO-8859-1')  # 读取本地数据
            df['datetime'] = pd.to_datetime(df['datetime'])
            try:
                time_ = upcoming_time
                Actual_ = df_json[df_json['indicator_id'] == i].actual.values[0]
                Srv_Med_ = df_json[df_json['indicator_id'] == i].consensus.values[0]
                Revised_ = df_json[df_json['indicator_id'] == i].revised.values[0]
                Prior_ = df_json[df_json['indicator_id'] == i].previous.values[0]
                unit_ = df_json[df_json['indicator_id'] == i].unit.values[0]
                df.loc[len(df)] = [time_, Actual_, Srv_Med_, Revised_, Prior_, unit_]
                if Actual_ != None:
                    flag_list.append(1)
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df.drop_duplicates(subset='datetime', inplace=True)  # 去重
                    df.sort_values('datetime', ascending=False, inplace=True)  # 排序
                    df.to_csv(
                        f'./data/{feature_df[feature_df.jinshi_id == i].Model_Name.values[0]}/{feature_df[feature_df.jinshi_id == i].Table_name.values[0]}.csv',
                        index=False)
                else:
                    flag_list.append(0)
            except:
                print(f'事件jinshi_id:{i}事件出现错误!!')

        if (pd.Series(flag_list) == 1).all():
            path_2 = os.path.join(r'./data', upcoming_event)
            path_list = [os.path.join(path_2, i) for i in os.listdir(path_2)]
            for path__ in path_list:
                name = path__.split('\\')[-1][:-4]
                df = pd.read_csv(path__)
                try:
                    append_df_to_db_2(df, name)
                except:
                    print(f'{name}未加载到数据库！！请注意检查！！')
            # 更新feature标签并保存到数据库
            try:
                path = os.path.join('./data', upcoming_event)
                path_list = [os.path.join(path, i) for i in os.listdir(path)]
                df_list = []
                for path__ in path_list:
                    df = pd.read_csv(path__)
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df['datetime'] = df['datetime'].dt.date
                    df = df.iloc[:, :-1]
                    df_list.append(df)
                df = df_list[0]
                for k in range(1, len(df_list)):
                    df = pd.merge(df, df_list[k], on='datetime', how='inner')
                df.replace('None', 0, inplace=True)
                df.fillna(0, inplace=True)
                df_new = pd.DataFrame({'date': df['datetime'], 'feature': None})
                df_new['feature'] = df.iloc[:, 1:].apply(lambda row: ','.join(row.astype(str)), axis=1)
                try:
                    append_df_to_db_2(df_new, upcoming_event)
                except:
                    print(f'{upcoming_event}未存入数据库，请注意检查')
                df_new.to_csv(rf'./data/EVENT/{upcoming_event}.csv', index=False)
            except:
                print(f'{upcoming_event}的feature有问题，请注意检查')
                pass
            print(f'{upcoming_event}抓取结束，已存入数据库，请检查！')
            break
        else:
            print('抓取失败，还未出现实时更新的值！')
        break


if __name__ == '__main__':
    while True:
        # 注意切换路径到当前文件夹
        # path_ = r'C:\Users\Dell\Desktop\金十爬虫'
        # os.chdir(path_)
        main()
        break
