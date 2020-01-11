from jz_index.sync_index import IndexSync


def main():
    """
    定时任务函数 每天执行一次 当前类需要每天实例化一次
    :return:
    """
    runner = IndexSync()
    runner.index_run()



main()
