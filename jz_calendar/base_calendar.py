import datetime

import pymongo
import pymysql

import pandas as pd

from jz_calendar.configs import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MONGO_URL, MONGO_DB, \
    MONGO_TABLE_2


class BaseSync(object):

    def DC(self):
        """
        mysql 连接对象
        :return:
        """
        return pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            charset='utf8mb4',
            db=MYSQL_DB,
        )

    def gen_calendars_mongo_coll(self):
        """
        生成 calendars 表的 mongo 连接
        :return:
        """
        return pymongo.MongoClient(MONGO_URL)[MONGO_DB][MONGO_TABLE_2]

    def yyyymmdd_date(self, dt: datetime.datetime) -> int:
        """
        将 datetime 转换为 date_int 格式
        datetime.datetime(2020,1,1) --> 20200101
        :param dt:
        :return:
        """
        return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day

    def get_date_list(self, start: datetime.datetime = None, end: datetime.datetime = None) -> list:
        """
        生成包含起止时间的 datetime 列表
        for example: get_data_list(datetime.datetime(2020,1,1), datetime.datetime(2020,1,3))
        return [datetime.datetime(2020,1,1), datetime.datetime(2020,1,2), datetime.datetime(2020,1,3)]
        :param start:
        :param end:
        :return:
        """
        dates = pd.date_range(start=start, end=end, freq='1d')
        dates = [date.to_pydatetime(date) for date in dates]
        return dates

    def gen_next_date(self):
        """
        生成当前时间的下一天，即 today 的下一天的零点时间
        :return:
        """
        limit_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min) + datetime.timedelta(days=1)
        return limit_date

    def log(self, some, *args, **kwargs):
        if some:
            print("### {} ###".format(some))
        if args:
            print("### {} ###".format(args))
        if kwargs:
            print("### {} ###".format(kwargs))


# if __name__ == "__main__":
#     d = BaseSync()
#     ret = d.get_date_list(datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 8))
#     print(ret)
