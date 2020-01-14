import datetime
import logging

import bson

from jz_calendar.configs import RUN_ENV
from jz_calendar.delisted_days import DelistedDaysMixin
from jz_calendar.holiday_days import HolidaysMixin
from jz_calendar.info_mixin import SyncInfoMixin
from jz_calendar.market_days import MarketDaysMixin
from jz_calendar.my_log import logger
from jz_calendar.suspend_days import SuspendDaysMixin


class CalendarsSync(SyncInfoMixin, MarketDaysMixin, SuspendDaysMixin, DelistedDaysMixin, HolidaysMixin):
    """
    针对于 calendars 数据库的更新服务

    calendars 数据库中 sh0000001 存放全部的情况

    其他的股票代码只存放除去市场不交易之外 自身不交易的情况
    """
    def __init__(self, timestamp):
        # 日历检查的快照时间戳
        self.timestamp = datetime.datetime.now()
        pass

    def gen_calendars_mongo_sus(self, code):
        """
        计算 calendars 中的某只股票的全部停牌 已排序
        :param code: 前缀形式的股票代码
        :return:
        """
        mon = self.gen_calendars_mongo_coll()
        # cursor = mon.find({"code": code, "ok": False}, {"date_int": 1, "_id": 0})
        cursor = mon.find({"code": code, "ok": False}, {"date": 1, "_id": 0})
        mongo_sus = sorted([j.get("date") for j in cursor])
        return mongo_sus

    def gen_mysql_sus_info(self, code, start, end, ts):
        """
        在 ts 时间戳时效下 某只代码 从开始时间到结束时间的 从 mysql 数据库中计算的全部停牌日
        :param code: 股票代码 原始数字格式
        :param start: 开始时间【包括】
        :param end: 结束时间 【包括】
        :param ts: 时间戳
        :return:
        """
        logger.info("")
        logger.info("")
        logger.info(f"code: {code}")
        logger.info("市场停牌： ")
        market_sus, _ = self.gen_sh000001(start, end, ts)
        if market_sus:
            logger.info(f"market_sus_0: {market_sus[0]}")
            logger.info(f"market_sus_-1: {market_sus[-1]}")

        logger.info("个股停牌： ")
        code_sus = self.gen_inc_code_sus(code, start, end, ts)
        if code_sus:
            logger.info(f"code_sus_0: {code_sus[0]}")
            logger.info(f"code_sus_-1: {code_sus[-1]}")
        else:
            logger.info(f"{code} no suspended days")

        logger.info("个股退市： ")
        delisted = self.delisted_days(code, ts, end)

        if delisted:
            logger.info(f"delisted_0: {delisted[0]}")
            logger.info(f"delisted_-1: {delisted[-1]}")
        else:
            logger.info(f"{code} no delisted")

        _all_sus = set(market_sus + code_sus + delisted)

        mysql_all_sus = sorted(_all_sus)

        mysql_calendars_sus = sorted(_all_sus - set(market_sus))

        return mysql_all_sus, mysql_calendars_sus

    def update_calendars(self, code, real_sus_dates, real_trading_dates):
        """
        针对未核对上的情况 对 calendars 数据库进行更新
        :param code: 前缀模式的股票代码
        :param real_sus_dates:
        :param real_trading_dates:
        :return:
        """
        mon = self.gen_calendars_mongo_coll()
        # for sus in real_sus_dates:
        #     # 有则更新
        #     if list(mon.find({"code": code, "date": sus})):
        #         mon.update_one({"code": code, "date": sus}, {"$set": {"ok": False}})
        #     else:
        #         # 无则插入
        #         data = {"code": code, "date": sus, 'date_int': self.yyyymmdd_date(sus), "ok": False}
        #         mon.insert_one(data)
        #
        # if real_trading_dates:
        #     mon.delete_many({"code": code, "date": {"$in": list(real_trading_dates)}})

        exist_dates_cursor = mon.find({"code": code, "date": {"$in": list(real_sus_dates)}}, {"_id": 0, "date": 1})
        exist_dates = [r.get("date") for r in exist_dates_cursor]

        mon.update_many({"code": code, "date": {"$in": exist_dates}},
                        {"$set": {"ok": False}},
                        )

        non_exist_dates = set(real_sus_dates) - set(exist_dates)
        bulks = []
        for sus in non_exist_dates:
            data = {"_id": bson.ObjectId(), "code": code, "date": sus,
                    'date_int': self.yyyymmdd_date(sus), "ok": False}
            bulks.append(data)

        mon.insert_many(bulks)

        if real_trading_dates:
            mon.delete_many({"code": code, "date": {"$in": list(real_trading_dates)}})

    def calendars_check(self, codes, ts):
        logger.info(f"开始检查数据的一致性，本次检查的快照时间戳是 {ts}")
        # 检验的截止时间
        limit_date = self.gen_next_date()

        # 拿到有记录的市场交易日的第一天
        market_start = self.market_first_day()

        # 拿到所有的codes
        # codes = self.all_codes
        # codes_map = convert_front_map(codes)

        for code in codes:

            _, mysql_calendars_sus = self.gen_mysql_sus_info(
                code, market_start, limit_date, ts)

            f_code = self.convert_6code(code)

            calendars_mongo_sus = self.gen_calendars_mongo_sus(f_code)

            if calendars_mongo_sus == mysql_calendars_sus:
                logger.info("check right!")
                continue
            else:
                logger.warning("check wrong!")
                real_sus_dates = set(mysql_calendars_sus) - set(calendars_mongo_sus)
                logger.info(f"real_sus_dates: {real_sus_dates}")

                real_trading_dates = set(calendars_mongo_sus) - set(mysql_calendars_sus)
                logger.info(f"real_trading_dates: {real_trading_dates}")

                self.update_calendars(f_code, real_sus_dates, real_trading_dates)

        # mark 一遍 holiday 字段
        # 只在 124 服务上添加该字段
        if RUN_ENV == "124":
            self.holiday_mark()

    def calendars_detection(self, ts1, ts2):
        """
        检查两个时间点之间的变动
        :param ts1:
        :param ts2:
        :return:
        """
        DATACENTER = self.DC2()
        DATACENTER.execute("use datacenter;")
        delisted_sql = f"""
            SELECT A.SecuCode from stk_liststatus B,const_secumainall A WHERE 
            A.InnerCode=B.InnerCode 
            AND A.SecuMarket IN(83,90) AND A.SecuCategory=1 
            AND B.ChangeType IN(1,2,3,4,5,6)
            and A.UPDATETIMEJZ > "{ts1}"
            and A.UPDATETIMEJZ <= "{ts2}"
            and B.UPDATETIMEJZ > "{ts1}"
            and B.UPDATETIMEJZ <= "{ts2}"; 
            """
        d_changed = list(DATACENTER.execute(delisted_sql))
        if d_changed:
            d_changed = [j[0] for j in d_changed]
        logger.info(f"d_changed: {d_changed}")

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        s_sql = f"""
           select SecuCode from stk_specialnotice_new
           where NoticeTypei = 18 and NoticeTypeii != 1703 
           and UPDATETIMEJZ > '{ts1}'
           and UPDATETIMEJZ <= '{ts2}'
           ;"""
        s_changed = list(DATACENTER.execute(s_sql))
        if s_changed:
            s_changed = [j[0] for j in s_changed]
        logger.info(f"s_changed: {s_changed}")

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        changed = set(d_changed + s_changed)
        logger.info(f"changed: {changed}")
        # changed 和 all_codes 相交的部分才有意义
        changed = set(self.all_codes) & changed

        if changed:
            logger.info(f"有改动的数据是 {changed}")
            # check_and_update(changed, ts2)
            self.calendars_check(changed, ts2)
        else:
            logger.info(f"{ts1} 和 {ts2} 之间无更新")


def task_day():
    d = CalendarsSync(datetime.datetime.now())
    d.calendars_check(d.all_codes, d.timestamp)


def task_5mins():
    ts2 = datetime.datetime.now()
    d = CalendarsSync(ts2)
    # 将每次的检测时间回溯到近两天
    ts1 = ts2 - datetime.timedelta(days=2)
    mon = d.gen_calendars_mongo_coll()
    d.check_market_calendar(mon, ts2)
    d.calendars_detection(ts1, ts2)


# if __name__ == "__main__":
#     # logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
#
#     task_day()
#
#     task_5mins()



# logger.info("happy")
