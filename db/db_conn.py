# -*- coding: utf-8 -*-
# @Time : 2023/3/13/013 21:42
# @Author : 不归
# @FileName: db_conn.py
import time
from contextlib import contextmanager
from http.client import RemoteDisconnected
from queue import Empty, Queue

import clickhouse_connect
from singleton_decorator import singleton

from conf.constants import *


class DB(object):
    def __init__(self):
        init_db_param()
        self.host = os.getenv("db_host", "127.0.0.1")
        self.port = os.getenv("db_port", "8123")
        self.user = os.getenv("db_user", "default")
        self.password = os.getenv("db_pass", "")

    def __repr__(self):
        return f"DB(host={self.host}, port={self.port}, user={self.user}, password={self.password})"

    def create_conn(self) -> clickhouse_connect.driver.httpclient.HttpClient:
        conn = clickhouse_connect.get_client(host=self.host, port=self.port, user=self.user, password=self.password)
        assert isinstance(conn, clickhouse_connect.driver.httpclient.HttpClient), "连接出错，请检查"
        return conn


@singleton
class DBPool:
    maxsize = 10
    db = DB()
    q = Queue(maxsize)

    def get_conn(self):
        try:
            conn = self.q.get_nowait()
        except Empty:
            conn = self.db.create_conn()
        except RemoteDisconnected:
            time.sleep(0.5)
            log.error("conn errors - RemoteDisconnected")
            conn = self.db.create_conn()
        return conn

    def put_conn(self, con):
        self.q.put(con)


db_pool = DBPool()


@contextmanager
def get_conn(pool=db_pool):
    conn = pool.get_conn()
    try:
        yield conn
    finally:
        pool.put_conn(conn)


if __name__ == '__main__':
    with get_conn() as conn:
        sql = "show databases;"
        res = conn.command(sql)
        print(res)
