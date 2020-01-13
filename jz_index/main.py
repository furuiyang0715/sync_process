import functools
import logging
import time

import schedule
from raven import Client

from jz_index.configs import SENTRY_DSN
from jz_index.sync_index import IndexSync


logger = logging.getLogger("index_log")

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
    schedule.every().day.at("17:30").do(task)

    while True:
        logger.info(schedule.jobs)
        schedule.run_pending()
        time.sleep(300)
        logger.info("no work to do, waiting ...")


main()
