import datetime
import sys
import traceback

import pymongo
import logging

from jz_index.base_index import BaseSync
from jz_index.configs import MONGO_URL
from jz_index.info_mixin import SyncInfoMixin

logger = logging.getLogger("main_log")


class IndexSync(SyncInfoMixin, BaseSync):
    def gen_index_coll(self):
        """
        生成 mongo 里面的入库连接
        :return:
        """
        mon = pymongo.MongoClient(MONGO_URL)

        coll = mon.JQdata.generate_indexcomponentsweight  # 写死的
        coll = mon[MONGO_DB][MONGO_TABLE]

        try:
            self.ensure_index_coll(coll)
        except Exception:
            logger.warning("创建索引失败")
            traceback.print_exc()
            raise
        else:
            self.log("创建索引成功")
        return coll

    def ensure_index_coll(self, coll):
        """
        将 date 和 index 设置为 mongo 入库的联合唯一索引
        :param coll:
        :return:
        """
        coll.create_index([('index', 1), ("date", 1)], unique=True)

    def generate_index_code(self, connection):
        """
        select SeCucode, InnerCode from const_secumainall where SecuCategory=4 AND
        SecuCode in ["000001", "399001"];
        :return:
        """
        query_sql = """
        select SeCucode, InnerCode from const_secumainall where SecuCategory=4 AND SecuCode in {};
                """.format(tuple(self.month_code().keys()))
        index_code_dict = dict()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for column in res:
                    index_code_dict.update({column[1]: column[0]})
        finally:
            connection.commit()
        return index_code_dict

    def generate_secucode_weight(self, connection, indexcode):
        """
        select SecuCode, Weight from index_indexcomponentsweight where IndexCode = 1 and
        EndDate = (SELECT max(EndDate) FROM index_indexcomponentsweight where IndexCode = 1);
        因为涉及到更新时间是当前这个指数的最新更新时间  所以查询单个进行 而非批量
        """
        query_sql = """select SecuCode, Weight from index_indexcomponentsweight where IndexCode = {}
        and EndDate = (SELECT max(EndDate) FROM index_indexcomponentsweight where IndexCode = {});
                        """.format(indexcode, indexcode)
        logger.info(query_sql)
        # self.log(query_sql)
        ret_dict = dict()  # bson.errors.InvalidDocument: key '000059.XSHE' must not contain '.'
        try:
            with connection.cursor() as cursor:
                cursor.execute(query_sql)
                res = cursor.fetchall()
                for column in res:
                    code = self.convert_8code(column[0])
                    ret_dict.update({code: float(column[1])})
        finally:
            connection.commit()
        # print(ret_dict)
        return ret_dict

    def month_sync(self):
        logger.info("执行指数的月更新任务")

        mysql_con = self.DC()
        coll = self.gen_index_coll()

        inner_code_map = self.generate_index_code(mysql_con)
        logger.info(inner_code_map)

        self.log(inner_code_map)
        # 显示InnerCode 与 SeCucode 之间的关系
        # {3477: '399319', 3478: '399320', 3479: '399321', 3873: '399322', 4078: '399324', ...}

        inner_code_list = list(inner_code_map.keys())

        for inner_code in inner_code_list:
            self.log(inner_code)

            # 生成权重数据
            index_secucode_weight_dict = self.generate_secucode_weight(mysql_con, inner_code)
            # self.log(index_secucode_weight_dict)

            front_index_code = self.month_code().get(inner_code_map.get(inner_code))

            to_insert = dict(date=self.check_date,

                             index=front_index_code,

                             index_info=index_secucode_weight_dict)

            self.log(to_insert)
            logger.info(to_insert)
            try:
                coll.insert_one(to_insert)
            except pymongo.errors.DuplicateKeyError:
                self.log("重复")
            else:
                self.log("插入成功 ")

            logger.info(f"""insert success: \n
            date: {self.check_date} \n
            index: {front_index_code} \n
            index_info: {index_secucode_weight_dict}""")

            logger.info("")

    def daily_indexs(self):
        """
        生成日更新数据库 index_weight 里面的全部种类的指数
        :return:
        """
        conn = self.DC2()
        check_new_indexes_query = "select distinct(IndexCode) from datacenter.index_weight;"
        daily_indexes = list((i[0] for i in conn.execute(check_new_indexes_query).fetchall()))
        return daily_indexes

    def process_daily(self, dt: datetime.datetime):
        """
        日更新流程
        :param dt:  要更新的某日
        :return:
        """

        dt = dt.strftime("%Y-%m-%d %H:%M:%S")

        coll = self.gen_index_coll()
        conn = self.DC2()

        daily_map = self.daily_code()
        self.log("需要进行日更新的相关代码信息是 {}".format(daily_map))

        for index_code in list(daily_map.keys()):
            logger.info(f"index_code: {index_code}")
            self.log(index_code)

            conn.execute("use datacenter;")
            query_sql = f"""
            SELECT B.Date, B.IndexCode, A.SecuCode, B.Weight from const_secumainall A,index_weight B
            WHERE A.SecuCategory=1 AND A.SecuMarket IN(83,90) AND A.ListedSector IN(1,2,6,7)
            AND A.InnerCode=B.InnerCode  AND B.IndexCode= '{index_code}' AND B.Date='{dt}' AND B.Flag=3;
            """

            # self.log(f"query_sql: \n {query_sql}")
            res = conn.execute(query_sql).fetchall()

            if not res:
                logger.info("{} 今日 {} 无更新".format(index_code, dt))
                self.log("{} 今日 {} 无更新".format(index_code, dt))
                continue

            infos = dict()
            for r in res:
                infos.update({self.convert_6code(r[2]): float(r[3])})

            data = {
                "date": self.check_date,
                "index": daily_map.get(index_code),
                "index_info": infos,
            }
            self.log(data)
            try:
                coll.insert_one(data)
            except pymongo.errors.DuplicateKeyError:
                self.log("重复")
            else:
                self.log("插入成功 ")

    def index_run(self):
        self.log("开始今天的指数更新服务 {}".format(self.check_date))

        self.process_daily(self.check_date)

        # self.month_sync()

        if (self.check_date + datetime.timedelta(days=1)).month != self.check_date.month:
            self.month_sync()
