import datetime
import logging
import bson

from jz_calendar.my_log import logger_1 as logger
from jz_calendar.sync_calendars import CalendarsSync


class CalendarSync(CalendarsSync):

    def gen_calendar_mongo_info(self, code, s, e):
        mon = self.gen_calendar_mongo_coll()
        cursor = mon.find({"code": code, "date": {"$gte": s, "$lte": e}},
                          # {"date": 1, "ok": 1, "_id": 0}
                          )
        # print("list: ", list(cursor))
        info = {}
        for j in cursor:

            info.update({j.get("date"): j.get("ok")})
        return info

    def calendar_sync(self, codes, ts):

        mon = self.gen_calendar_mongo_coll()
        # 每日检查一遍 SH000001 的市场交易日情况
        self.check_market_calendar(mon, ts)

        # 检验的截止时间
        limit_date = self.gen_next_date()
        new_start_2020 = datetime.datetime(2020, 1, 1)

        for code in codes:
            logger.info(code)

            mysql_all_sus, _ = self.gen_mysql_sus_info(
                code, new_start_2020, limit_date, ts)
            logger.info("sql: ", mysql_all_sus)

            f_code = self.convert_6code(code)
            calendar_mongo_info = self.gen_calendar_mongo_info(f_code, new_start_2020, limit_date)
            logger.info("mon: ",  calendar_mongo_info)

            for d in self.get_date_list(new_start_2020, limit_date):
                if d in mysql_all_sus:   # 停牌日
                    if calendar_mongo_info.get(d) is None:
                        mon.insert_one({"_id": bson.ObjectId(), "code": f_code, "date": d,
                                        "date_int": self.yyyymmdd_date(d), "ok": False})
                    elif calendar_mongo_info.get(d) is False:
                        pass
                    elif calendar_mongo_info.get(d) is True:
                        mon.update_one(
                            {"code": f_code, "date": d}, {"$set": {"ok": False}})
                else:    # 交易日
                    if calendar_mongo_info.get(d) is None:
                        mon.insert_one({"_id": bson.ObjectId(), "code": f_code, "date": d,
                                        "date_int": self.yyyymmdd_date(d), "ok": True})
                    elif calendar_mongo_info.get(d) is True:
                        pass
                    elif calendar_mongo_info.get(d) is False:
                        mon.update_one(
                            {"code": f_code, "date": d}, {"$set": {"ok": True}})


def task_day():
    d = CalendarSync(datetime.datetime.now())
    d.calendar_sync(d.all_codes, d.timestamp)


# task_day()

# logger.info("hello")
