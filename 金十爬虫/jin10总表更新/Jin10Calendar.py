import datetime
import json
import time
from concurrent.futures import ThreadPoolExecutor
import requests

import sys

sys.path.append(r"C:\Users\Dell\Desktop\Jin10")
from utils.calendar_dao import Dao


class Jin10Calendar:
    # dao = Dao(host='172.24.20.139')
    dao = Dao()

    def __get_all_thread(self, date_obj):
        url = f"https://cdn-rili.jin10.com/web_data/{date_obj.year}/daily/{str(date_obj.month).zfill(2)}/{str(date_obj.day).zfill(2)}/economics.json"
        # print(url)
        response = requests.get(url)
        data = json.loads(response.text)
        # print(data)
        for item in data:

            data_id = item["id"]
            name = item["name"]
            country = item["country"]
            previous = item["previous"]
            consensus = item["consensus"]
            actual = item["actual"]
            revised = item["revised"]
            unit = item["unit"]
            pub_time_unix = item["pub_time_unix"] * 1000
            pub_time = item["pub_time"]
            crawl_time = int(time.time() * 1000)  # now
            star = item["star"]
            time_period = item["time_period"]
            affect = item["affect"]
            show_affect = item["show_affect"]
            indicator_id = item["indicator_id"]
            # print(time_period, name, pub_time)
            # print(data_id, name, country, previous, consensus, actual, revised,
            #       unit, pub_time_unix, pub_time, crawl_time, star, time_period,
            #       affect, show_affect, indicator_id)
            self.dao.insert(data_id, name, country, previous, consensus, actual,
                            revised, unit, pub_time_unix, pub_time, crawl_time,
                            star, time_period, affect, show_affect,
                            indicator_id)

    def get_all(self):
        #start_date = datetime.date(2023, 1, 1)  # 最早的数据在这一天
        #end_date = datetime.date(2023, 3, 1)
        start_date = datetime.date.today() + datetime.timedelta(days=-10)
        end_date = datetime.date.today() + datetime.timedelta(days=1)
        executor = ThreadPoolExecutor(max_workers=30)
        while start_date <= end_date:
            executor.submit(self.__get_all_thread, start_date)
            # self.__get_all_thread(start_date)
            start_date += datetime.timedelta(days=1)


if __name__ == "__main__":
    Jin10Calendar().get_all()