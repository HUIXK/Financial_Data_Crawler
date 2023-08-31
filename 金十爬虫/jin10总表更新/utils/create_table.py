import pymysql
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 创建数据库连接
conn = pymysql.connect(
    host="",
    user="",
    password="",
    database="",
    port=,
)
cursor = conn.cursor()

table_name = 'financial_calendar'
sql_drop_table = f"DROP TABLE IF EXISTS {table_name};"
cursor.execute(sql_drop_table)
# 定义创建表的 SQL 语句
create_table_query = f"""
CREATE TABLE {table_name} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data_id INT,
    name VARCHAR(100),
    country VARCHAR(50),
    previous VARCHAR(20),
    consensus DECIMAL(10, 2),
    actual DECIMAL(10, 2),
    revised DECIMAL(10, 2),
    unit VARCHAR(50),
    pub_time_unix BIGINT,
    pub_time VARCHAR(50),
    crawl_time BIGINT,
    star INT,
    time_period VARCHAR(50),
    affect INT,
    show_affect INT,
    indicator_id INT
);
"""

# 执行创建表的 SQL 语句
cursor.execute(create_table_query)

# 提交更改
conn.commit()
logging.info("创建表成功！")

data_id, name, country, previous, consensus, actual, revised, unit, pub_time_unix, pub_time, crawl_time, star, time_period, affect, show_affect, indicator_id = 303635, '当周石油钻井总数', '美国', '525', None, '525', None, '%', 1691773200000, '2023-08-11T17:00:00.000Z', 1692257998246, 3, '至8月11日', 1, 1, 951
sql = "insert into financial_calendar(data_id, name, country, previous, consensus, actual, revised, unit, " \
    "pub_time_unix, pub_time, crawl_time, star, time_period, affect, show_affect, indicator_id)" \
    " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
cursor.execute(sql, (data_id, name, country, previous, consensus, actual, revised,
                     unit, pub_time_unix, pub_time, crawl_time, star,
                     time_period, affect, show_affect, indicator_id))
conn.commit()
logging.info("插入成功")
cursor.close()
conn.close()

# [
#     id,
#     name,
#     country,
#     previous,
#     consensus,
#     actual,
#     revised,
#     unit,
#     pub_time_unix,
#     pub_time,
#     crawl_time,
#     star,
#     time_period,
#     affect,
#     show_affect,
#     indicator_id,
# ]
