import pymysql
from queue import Queue
from threading import Lock


class PooledDB(object):
    def __init__(self, creator, mincached=2, maxcached=10, **kwargs):
        self.creator = creator
        self.mincached = mincached
        self.maxcached = maxcached
        self.kwargs = kwargs
        self._lock = Lock()
        self._connections = Queue()
        print(self.kwargs)

        for _ in range(mincached):
            self._connections.put(self._create_connection())

    def _create_connection(self):
        return self.creator.connect(**self.kwargs)

    def get_connection(self):
        if not self._connections.empty():
            return self._connections.get()
        elif self._connections.qsize() < self.maxcached:
            return self._create_connection()
        else:
            raise Exception("Connection pool exhausted")

    def release_connection(self, conn):
        self._connections.put(conn)

    def close(self):
        while not self._connections.empty():
            conn = self._connections.get()
            conn.close()

    # def select(self, sql, params=()):
    #     # print('sql:', sql)
    #     # print('params:', params)
    #     conn = self.get_connection()
    #     # cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    #     cur = conn.cursor()
    #     cur.execute(sql, params)
    #     select_res = cur.fetchall()
    #     # print('select res: ', select_res)
    #     cur.close()
    #     conn.close()
    #     return select_res

    # def update(self, sql, params=()):
    #     # print('sql:', sql)
    #     # print('params:', params)
    #     conn = self.get_connection()
    #     cur = conn.cursor()
    #     cur.execute(sql, params)
    #     conn.commit()
    #     cur.close()
    #     conn.close()


# 测试代码
if __name__ == '__main__':
    # 创建 DBPool 对象
    db_pool = PooledDB(creator=pymysql,
                       mincached=1,
                       maxcached=100,
                       host="",
                       user="",
                       password="",
                       port=,
                       database="",
                       charset="utf8")

    # 执行查询操作
    select_sql = 'SELECT * FROM yourtable'
    result = db_pool.select(select_sql)
    print('Query Result:')
    for row in result:
        print(row)

    # 执行更新操作
    update_sql = 'UPDATE yourtable SET name = %s, age = %s, email = %s WHERE id = %s'
    update_params = ('new_value', 12, 'dsahifhdsakfkdsa', 1)
    db_pool.update(update_sql, update_params)

    # 关闭连接池
    db_pool.close()