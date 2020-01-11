import datetime
import logging
import re
import pymysql
from sqlalchemy import create_engine

from jz_index.configs import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

logger = logging.getLogger("index")


class BaseSync(object):
    # 股票格式的正则匹配
    stock_format = [r'^[SI][ZHX]\d{6}$',
                    r'^\d{6}\.[A-Z]{4}$']

    def __init__(self):
        """
        更新时间,需要每天实例化一次
        """
        # self.check_date = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        self.check_date = datetime.datetime(2020, 1, 10)

    def little8code(self, x):
        """
        将后缀转换为对应的前缀
        :param x:
        :return:
        """
        assert len(x) == 5
        if x == '.XSHG':
            x = 'SH'

        elif x == '.XSHE':
            x = 'SZ'

        elif x == '.INDX':
            x = 'IX'
        return x

    def convert_8code(self, code):
        """
        股票格式转换: 转换为前缀模式
        :param code:
        :return:
        """
        if re.match(self.stock_format[1], code):  # 600001.XSHG
            code = self.little8code(code[6:]) + code[:6]
        elif re.match(self.stock_format[0], code):  # SH600001
            pass
        else:
            logger.info("股票格式错误.")
        return code

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

    def DC(self):
        # 使用 pymysql 获取一个针对于 mysql 数据库的连接
        return pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            charset='utf8mb4',
            db=MYSQL_DB,
        )

    def DC2(self):
        # 使用 sqlalchemy 生成的一个针对于 mysql 数据库的连接
        mysql_string = f"""mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/
                          {MYSQL_DB}?charset=gbk"""
        cli = create_engine(mysql_string)

        return cli

    def log(self, some):
        print("====== {} ======".format(some))
