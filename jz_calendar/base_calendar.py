import datetime
import pymongo
import pymysql

import pandas as pd
from sqlalchemy import create_engine

from jz_calendar.configs import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MONGO_URL, MONGO_DB, \
    MONGO_TABLE_2, MONGO_TABLE_1

from jz_calendar.my_log import logger


class BaseSync(object):

    def DC(self):
        """
        基于 pymysql 生成的 mysql 连接对象
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

    def DC2(self):
        """
        基于 sqlalchemy 生成的 mysql 连接对象
        :return:
        """
        mysql_string = f"""mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/
                           {MYSQL_DB}?charset=gbk"""
        cli = create_engine(mysql_string)

        return cli

    def gen_calendars_mongo_coll(self):
        """
        生成 calendars 表的 mongo 连接
        :return:
        """
        return pymongo.MongoClient(MONGO_URL)[MONGO_DB][MONGO_TABLE_2]

    def gen_calendar_mongo_coll(self):
        """
        生成 calendar 表的 mongo 连接
        :return:
        """
        return pymongo.MongoClient(MONGO_URL)[MONGO_DB][MONGO_TABLE_1]

    def yyyymmdd_date(self, dt: datetime.datetime) -> int:
        """
        将 datetime 转换为 date_int 格式
        datetime.datetime(2020,1,1) --> 20200101
        :param dt:
        :return:
        """
        return dt.year * 10 ** 4 + dt.month * 10 ** 2 + dt.day

    def back_convert_date_int(self, date_int):
        """
        convert date_int to datetime
        :param date_int:
        :return:
        """
        _year, _ret = divmod(date_int, 10000)
        _month, _day = divmod(_ret, 100)
        return datetime.datetime(_year, _month, _day)

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

    def convert_6code(self, code):
        """
        将代码加上前缀
        :param code:
        :return:
        """
        if str(code)[0] in ["3", "0"]:
            return "SZ" + code
        elif str(code)[0] in ['6']:
            return "SH" + code
        else:
            raise ValueError("格式转换异常值")

    def gen_bulk(self, code, suspended, start, end):
        suspended = sorted(list(set(suspended)))
        bulk = list()
        dt = start

        if not suspended:
            logger.info("无 sus_bulk")

            for _date in self.get_date_list(dt, end):
                bulk.append({"code": code, "date": _date, 'date_int': self.yyyymmdd_date(_date), "ok": True})

        else:
            for d in suspended:  # 对于其中的每一个停牌日
                # 转换为 整点 模式,  为了 {datetime.datetime(1991, 4, 14, 1, 0)} 这样的数据的存在
                d = datetime.datetime.combine(d.date(), datetime.time.min)

                while dt <= d:

                    if dt < d:
                        bulk.append({"code": code, "date": dt, "date_int": self.yyyymmdd_date(dt), "ok": True})
                        # print(f"{yyyymmdd_date(dt)}: True")

                    else:  # 相等即为非交易日
                        bulk.append({"code": code, "date": dt, "date_int": self.yyyymmdd_date(dt), "ok": False})
                        # print(f"{yyyymmdd_date(dt)}: False")

                    dt += datetime.timedelta(days=1)

            # print(dt)  # dt 此时已经是最后一个停牌日期 + 1 的状态了
            # print(end)

            # dt > d:  已经跳出停牌日 在(停牌日+1) 到 截止时间 之间均为交易日
            if dt <= end:
                for _date in self.get_date_list(suspended[-1] + datetime.timedelta(days=1), end):
                    bulk.append({"code": code, "date": _date, 'date_int': self.yyyymmdd_date(_date), "ok": True})
        logger.info(f"{code}  \n{bulk[0]}  \n{bulk[-1]}")

        return bulk

    def bulk_insert(self, mon, code, suspended, start, end):
        """
        按照顺序生成起止时间内的全部交易日历信息
        :param mon:
        :param code: 前缀形式的股票代码
        :param suspended: 非交易日
        :param start: 开始时间
        :param end: 结束时间
        :return:
        """
        # # 保险起见 将 suspend 去重排序
        # suspended = sorted(list(set(suspended)))
        # bulk = list()
        # dt = start
        #
        # if not suspended:
        #     logger.info("无 sus_bulk")
        #
        #     for _date in self.get_date_list(dt, end):
        #         bulk.append({"code": code, "date": _date, 'date_int': self.yyyymmdd_date(_date), "ok": True})
        #
        # else:
        #     for d in suspended:  # 对于其中的每一个停牌日
        #         # 转换为 整点 模式,  为了 {datetime.datetime(1991, 4, 14, 1, 0)} 这样的数据的存在
        #         d = datetime.datetime.combine(d.date(), datetime.time.min)
        #
        #         while dt <= d:
        #
        #             if dt < d:
        #                 bulk.append({"code": code, "date": dt, "date_int": self.yyyymmdd_date(dt), "ok": True})
        #                 # print(f"{yyyymmdd_date(dt)}: True")
        #
        #             else:  # 相等即为非交易日
        #                 bulk.append({"code": code, "date": dt, "date_int": self.yyyymmdd_date(dt), "ok": False})
        #                 # print(f"{yyyymmdd_date(dt)}: False")
        #
        #             dt += datetime.timedelta(days=1)
        #
        #     # print(dt)  # dt 此时已经是最后一个停牌日期 + 1 的状态了
        #     # print(end)
        #
        #     # dt > d:  已经跳出停牌日 在(停牌日+1) 到 截止时间 之间均为交易日
        #     if dt <= end:
        #         for _date in self.get_date_list(suspended[-1] + datetime.timedelta(days=1), end):
        #             bulk.append({"code": code, "date": _date, 'date_int': self.yyyymmdd_date(_date), "ok": True})
        # logger.info(f"{code}  \n{bulk[0]}  \n{bulk[-1]}")

        bulk = self.gen_bulk(code, suspended, start, end)
        try:
            mon.insert_many(bulk)
        except Exception as e:
            logger.info(f"批量插入失败 {code}, 原因是 {e}")

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


# logger.info("base")