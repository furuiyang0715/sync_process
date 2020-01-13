import datetime

import pymysql

from jz_calendar.configs import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB


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

    def yyyymmdd_date(self, dt: datetime.datetime) -> int:
        """
        将 datetime 转换为 date_int 格式
        datetime.datetime(2020,1,1) --> 20200101
        :param dt:
        :return:
        """
        return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day
