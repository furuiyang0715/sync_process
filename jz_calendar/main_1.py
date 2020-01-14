import functools
import sys
import time
import datetime
import schedule

from raven import Client

sys.path.insert(0, "./..")

from jz_calendar.sync_calendar import task_day
from jz_calendar.configs import SENTRY_DSN
from jz_calendar.my_log import logger_1 as logger

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
def task_day_run():
    """
    定时任务函数 每天执行一次
    :return:
    """
    task_day()


def main():
    task_day_run()

    sentry.captureMessage(f"现在是 {datetime.datetime.today()}, 更新 stock.calendar 交易日历 ")
    schedule.every().day.at("05:00").do(task_day_run)

    while True:
        logger.info(schedule.jobs)
        schedule.run_pending()
        time.sleep(300)
        logger.info("no work to do, waiting")


main()

# logger.info("main")
