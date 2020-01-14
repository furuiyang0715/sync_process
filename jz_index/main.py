import functools
import logging
import os
import sys
import time
import datetime

import schedule
from raven import Client

sys.path.insert(0, "./..")

from jz_index.mylog import logger
from jz_index.configs import SENTRY_DSN
from jz_index.sync_index import IndexSync


sentry = Client(SENTRY_DSN)


def catch_exceptions(cancel_on_failure=False):
    # 定时任务中的异常捕获
    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                return job_func(*args, **kwargs)
            except:
                import traceback
                logger.warning(traceback.format_exc())
                sentry.captureException(exc_info=True)
                if cancel_on_failure:
                    # print(schedule.CancelJob)
                    # schedule.cancel_job()
                    return schedule.CancelJob
        return wrapper
    return catch_exceptions_decorator


@catch_exceptions(cancel_on_failure=True)
def task():
    """
    定时任务函数 每天执行一次 当前类需要每天实例化一次
    :return:
    """
    runner = IndexSync()
    runner.index_run()


def main():
    task()

    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 开始增量 stock.calendars 交易日历 ")
    schedule.every().day.at("16:00").do(task)

    while True:
        logger.info(schedule.jobs)
        schedule.run_pending()
        time.sleep(300)
        logger.info("no work to do, waiting ...")


main()

# logger.info("main")
