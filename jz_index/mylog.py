import logging.config
import os


log_dir = os.path.dirname(__file__)


logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    # 规定日志的输出格式
    "formatters": {
        "simple": {
            "format": "[%(levelname)1.1s %(asctime)s|%(module)s|%(funcName)s|%(lineno)d] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        # 位于标准输出的日志
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": 'simple',
            "stream": "ext://sys.stdout"
        },
        # 位于文件的日志
        "index_file_log": {
            "level": "DEBUG",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(log_dir, "main.log"),
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },
    },
    "loggers": {
        "index_log": {
            "level": "DEBUG",
            "handlers": ["console", "index_file_log"]
        },
    }
})


# print(log_dir)
# logger = logging.getLogger("index_log")
