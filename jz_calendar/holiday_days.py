import datetime

from chinese_calendar import get_holidays
from jz_calendar.base_calendar import BaseSync
from jz_calendar.my_log import logger


class HolidaysMixin(BaseSync):
    """
    混入标记法定节假日的功能
    """
    def generate_holidays(self, start_date, end_date):
        """
        利用 chinese-calendar 的第三方库生成起止时间段内的法定节假日列表 并将其转换为 date_int 的格式
        :param start_date:
        :param end_date:
        :return:
        """
        holidays = get_holidays(start_date, end_date, include_weekends=False)
        int_holidays = [self.yyyymmdd_date(h) for h in holidays]
        return int_holidays

    def add_holiday_field(self, start, end):
        """
        为指定时间段内的数据根据 holidays 列表添加 holiday 字段
        :param start:
        :param end:
        :return:
        """
        int_holidays = self.generate_holidays(start, end)
        mon = self.gen_calendars_mongo_coll()

        try:
            mon.update_many(
                {"code": "SH000001", "ok": False,
                 "date_int": {"$in": int_holidays}},
                {"$set": {"holiday": True}}, upsert=True)

            mon.update_many(
                {"code": "SH000001",
                 "ok": False,
                 "date": {"$gte": start, "$lte": end},
                 "date_int": {"$nin": int_holidays}},
                {"$set": {"holiday": False}}, upsert=True)
        except Exception as e:
            raise RuntimeError(f"批量更新 holiday 字段失败，原因是 {e}")

    def check(self, s, e):
        """
        数据核对
        :param s: 插入的开始日期
        :param e: 插入的结束日期
        :return:
        """
        mon = self.gen_calendars_mongo_coll()
        # 这段时间内 mongo 数据库中的节假日列表
        cur = mon.find({"code": "SH000001", "holiday": True}, {"date_int": 1, "_id": 0})
        mongo_holidays = [r.get("date_int") for r in cur]
        # 利用第三方库查询这段时间内的节假日列表
        api_holidays = self.generate_holidays(s, e)
        if mongo_holidays != api_holidays:
            raise ValueError("holiday 数据不一致，请检查！")

    def holiday_mark(self):
        """
        mark 的主程序入口
        :return:
        """
        logger.info("开始检查更新节假日字段")
        s = datetime.datetime(2004, 1, 1)
        e = datetime.datetime(2020, 12, 30)
        self.add_holiday_field(s, e)
        self.check(s, e)


# if __name__ == "__main__":
#     d = HolidaysMixin()
#     d.holiday_mark()


# logger.info("holiday")