import os
import configparser

env = os.environ.get

cf = configparser.ConfigParser()
cf.read('env.conf')

MYSQL_HOST = env("MYSQL_HOST", cf.get('mysql', 'MYSQL_HOST'))
MYSQL_PORT = int(env("MYSQL_PORT", cf.get('mysql', 'MYSQL_PORT')))
MYSQL_USER = env("MYSQL_USER", cf.get('mysql', 'MYSQL_USER'))
MYSQL_PASSWORD = env("MYSQL_PASSWORD", cf.get('mysql', 'MYSQL_PASSWORD'))
MYSQL_DB = env("MYSQL_DB", cf.get('mysql', 'MYSQL_DB'))
MONGO_URL = env("MONGO_URL", cf.get('mongodb', 'MONGO_URL'))
SENTRY_DSN = env("SENTRY_DSN", cf.get('sentry', 'SENTRY_DSN'))


# print(MYSQL_HOST)
# print(MYSQL_PORT)
# print(MYSQL_USER)
# print(MYSQL_PASSWORD)
# print(MYSQL_DB)
# print(MONGO_URL)
# print(SENTRY_DSN)