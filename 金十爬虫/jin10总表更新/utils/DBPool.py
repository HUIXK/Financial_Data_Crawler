from utils.dbutils import PooledDB
import pymysql
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class DBPool(object):
    __pool = None

    def __init__(self,
                 host="",
                 database=""):
        self.__pool = PooledDB(creator=pymysql,
                               mincached=1,
                               maxcached=50,
                               host=host,
                               port=,
                               user="",
                               password="",
                               database=database,)

    # 调用此方法，返回一个数据库连接，或创建连接池后返回一个数据库连接
    # @staticmethod
    def get_conn(self):
        return self.__pool.get_connection()

    # xxxxx __pool.close()是关闭连接池
    # conn.close()是使连接返回连接池
    def close(self):
        self.__pool.close()

    # mysql
    def update(self, sql, params=()):
        # print('sql:', sql)
        # print('params:', params)
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        logging.info(f"{params}插入成功")
        conn.commit()
        cur.close()
        conn.close()

    def select(self, sql, params=()):
        # print('sql:', sql)
        # print('params:', params)
        conn = self.get_conn()
        # cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur = conn.cursor()
        cur.execute(sql, params)
        select_res = cur.fetchall()
        # print('select res: ', select_res)
        cur.close()
        conn.close()
        return select_res
