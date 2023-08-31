from utils.DBPool import DBPool
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Dao:
    db_pool = None

    def __init__(self,
                 host="",
                 database=""):
        self.db_pool = DBPool(host=host, database=database)

    def insert(self, data_id, name, country, previous, consensus, actual, revised,
               unit, pub_time_unix, pub_time, crawl_time, star, time_period,
               affect, show_affect, indicator_id):

        # # 根据时间和名称更新
        # # sql1 = "select id as old_id from financial_calendar where name=%s and country=%s and unit=%s and pub_time_unix=%s and time_period=%s"
        # sql1 = "select id as old_id from financial_calendar where name=%s and country=%s and pub_time_unix=%s and time_period=%s"
        # rs = self.db_pool.select(sql1,
        #                          (name, country, pub_time_unix, time_period))
        # # print(f"数量: {len(rs)}")
        # # 删除多余的
        # for r in rs[1:]:
        #     old_id = r[0]
        #     sql1 = "delete from financial_calendar where id=%s"
        #     self.db_pool.update(sql1, (old_id, ))
        # # 替换第一个
        # if len(rs) != 0:
        #     old_id = rs[0][0]
        #     sql1 = "update financial_calendar set name=%s, country=%s, previous=%s, consensus=%s, actual=%s, " \
        #            "revised=%s, unit=%s, pub_time_unix=%s, pub_time=%s, crawl_time=%s, star=%s, time_period=%s, " \
        #            "affect=%s, show_affect=%s, indicator_id=%s where id=%s"
        #     self.db_pool.update(
        #         sql1, (name, country, previous, consensus, actual, revised,
        #                unit, pub_time_unix, pub_time, crawl_time, star,
        #                time_period, affect, show_affect, indicator_id, old_id))
        #     return

        # 根据id更新
        sql0 = "select count(0) as c from financial_calendar where data_id = %s"
        rs = self.db_pool.select(sql0, (data_id, ))
        if rs[0][0] != 0:
            sql0 = "update financial_calendar set name=%s, country=%s, previous=%s, consensus=%s, actual=%s, " \
                   "revised=%s, unit=%s, pub_time_unix=%s, pub_time=%s, crawl_time=%s, star=%s, time_period=%s, " \
                   "affect=%s, show_affect=%s, indicator_id=%s where data_id=%s"
            self.db_pool.update(
                sql0, (name, country, previous, consensus, actual, revised,
                       unit, pub_time_unix, pub_time, crawl_time, star,
                       time_period, affect, show_affect, indicator_id, data_id))
            logging.info("id: %s 已存在, 重新更新成功" % data_id)
            return
        else:
            sql = "insert into financial_calendar(data_id, name, country, previous, consensus, actual, revised, unit, " \
                "pub_time_unix, pub_time, crawl_time, star, time_period, affect, show_affect, indicator_id)" \
                " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.db_pool.update(
                sql, (data_id, name, country, previous, consensus, actual, revised,
                      unit, pub_time_unix, pub_time, crawl_time, star,
                      time_period, affect, show_affect, indicator_id))
            # logging.info("插入成功")


if __name__ == "__main__":
    print(1)
