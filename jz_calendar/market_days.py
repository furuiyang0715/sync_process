import datetime

from jz_calendar.base_calendar import BaseSync

import jqdatasdk as jqsdk

jqsdk.auth('15626046299', '046299')


class MarketDaysMixin(BaseSync):
    """
    混入计算市场交易日的功能
    """
    def gen_sh000001(self, start, end, timestamp):
        """
        对于整个市场, 从 start 到 end 之间非交易日列表 suspended
        数据源为： JQdata.const_tradingday
        包含 start 和 end
        """
        bulk = list()

        # （1） mongo的形式
        # source = db.JQdata.const_tradingday
        # suspended = source.find(
        #     {"IfTradingDay": 2, "SecuMarket": 83, "Date": {"$gte": start, "$lte": end}}, {"Date": 1}
        # ).sort("Date", pymongo.ASCENDING).distinct("Date")
        # suspended = sorted(suspended)

        # （2） mysql 的形式
        conn = self.DC()

        suspended = list()
        trading = list()

        # REF:  https://blog.csdn.net/wild46cat/article/details/78715099

        # （1）查询出所有标记为 非交易日的
        # （2）查询出所有标记为 交易日的
        # （3）根据 CMFTime 判断到底哪个是 ✅ 的

        # （1）
        sus_sql = f"""
        select distinct(Date) from const_tradingday where 
        IfTradingDay=2
        and SecuMarket=83
        and Date >="{start}"
        and Date <= "{end}"
        and UPDATETIMEJZ <= "{timestamp}"
        order by Date asc;
        """

        try:
            with conn.cursor() as cursor:
                cursor.execute(sus_sql)
                res = cursor.fetchall()
                for column in res:
                    suspended.append(column[0])
        finally:
            conn.commit()
        suspended = sorted(list(set(suspended)))

        # （2）
        trade_sql = f"""
        select distinct(Date) from const_tradingday where 
        IfTradingDay=1
        and SecuMarket=83
        and Date >="{start}"
        and Date <= "{end}"
        and UPDATETIMEJZ <= "{timestamp}"
        order by Date asc;
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute(trade_sql)
                res = cursor.fetchall()
                for column in res:
                    trading.append(column[0])
        finally:
            conn.commit()
        trading = sorted(list(set(trading)))

        # （3）
        for day in (set(trading) & set(suspended)):
            check_sql = f"""
            select IfTradingDay from const_tradingday where Date = "{day}" and 
            UPDATETIMEJZ = (select max(UPDATETIMEJZ) from const_tradingday where Date = "{day}");
            """
            try:
                with conn.cursor() as cursor:
                    cursor.execute(check_sql)
                    res = cursor.fetchall()
                    iftrading = res[0][0]
            finally:
                conn.commit()

            if iftrading == 1:
                suspended.remove(day)
            if iftrading == 2:
                trading.remove(day)

        return suspended, trading
        # return suspended

    def checkout_with_jqdatasdk(self, start: datetime.datetime, end: datetime.datetime):
        """
        与聚宽 sdk 的交易日数据进行对比
        :param start:  对比开始时间
        :param end:  对比结束时间
        :return:
        """

        sus, trades = self.gen_sh000001(start, end, datetime.datetime.now())
        trades = [self.yyyymmdd_date(day) for day in trades]

        jq_trades = jqsdk.get_all_trade_days()
        jq_trades = jq_trades.tolist()
        jq_trades = [self.yyyymmdd_date(day) for day in jq_trades]

        self.log(len(trades), trades[0], trades[-1])
        self.log(len(jq_trades), jq_trades[0], jq_trades[-1])

        return trades == jq_trades


# if __name__ == "__main__":
#     d = MarketDaysMixin()
#     ret = d.checkout_with_jqdatasdk(datetime.datetime(2005,1,1), datetime.datetime(2020, 12, 31))
#     print(ret)
