# -*- coding: utf-8 -*-
# @Time : 2023/3/26/026 21:45
# @Author : 不归
# @FileName: base.py
"""
不同数据源的实现封装类，目前完成掘金（GM）、QMT、TS
"""

import traceback
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from http.client import RemoteDisconnected

import pandas as pd
from clickhouse_connect.driver import ProgrammingError
from clickhouse_connect.driver.exceptions import OperationalError
from urllib3.exceptions import ProtocolError

from conf.constants import *
from db.db_conn import get_conn

"""
将不同数据源的频率封装在此页面上
F1 代表1分钟，后面3个参数分别对应 init传入的sql(中文翻译)，gm掘金，qmt，TS等平台，可以继续填充
"""

Dtype = namedtuple('Dtype', ['sql', 'gm', 'qmt', 'ts'])
MIN = Dtype('min', '60s', '1m', '1min')
DAY = Dtype('day', '1d', '1d', 'D')
ADJ = Dtype('adj', 'adj', 'adj', 'adj')

FQ = namedtuple('FQ', ['sql', 'gm', 'qmt', 'ts'])
NONE = FQ('none', 0, 'none', 'None')
PRE = FQ('pre', 1, 'front', 'qfq')
POST = FQ('post', 2, 'back', 'hfq')


@dataclass
class DataSource:
    """
    数据源抽象类
    """

    dtype: Dtype = None
    fq: FQ = NONE  # 是否复权
    limit: int = 32400  # 单次获取行限制,建议设置为240的倍数
    thread_num: int = 4  # 默认允许的多线程数量
    source_date: str = "19900101"  # 默认数据源提供的初始时间

    def set_source(self):
        raise NotImplementedError

    def _to_clickhouse(self, db_tab, dataf):
        with get_conn() as conn:
            try:
                conn.insert_df(db_tab, dataf)
            except ProgrammingError as e:
                ex_str = traceback.format_exc()
                log.error(ex_str)
                print(e)
                dataf.to_csv(f"errs/insert_{db_tab}.csv")
                return 0
            except (OperationalError, RemoteDisconnected, ProtocolError) as e:
                log.error("服务器过热, 断开连接...")
                return 0
            except Exception as e:
                ex_str = traceback.format_exc()
                log.error(ex_str)
                print(e)
                dataf.to_csv(BASE_PATH / f"errs/insert_{db_tab}.csv")
                exit()
        return 1

    @staticmethod
    def _query_clickhouse(sql: str) -> pd.DataFrame:
        with get_conn() as conn:
            res = conn.query_df(sql)
            log.debug(f"查询【{sql}】完成")
        return res

    @staticmethod
    def _command_clickhouse(sql: str):
        with get_conn() as conn:
            res = conn.command(sql)
            log.debug(f"执行【{sql}】完成")
        return res

    @staticmethod
    def get_symbols_info() -> pd.DataFrame:
        sql = "SELECT code,sdt FROM quant.codes FINAL WHERE status=1"
        res = DataSource._query_clickhouse(sql)
        return res

    def update_hist(self, symbols: list[str], sdt: datetime, edt: datetime) -> pd.DataFrame:
        raise NotImplementedError

    def update_adjust(self, symbols: list[str], sdt: datetime, edt: datetime) -> pd.DataFrame:
        raise NotImplementedError

    def update_symbols_info(self):
        raise NotImplementedError

    def sel_kline_for_symbol(self, symbol: str, sdt: str, edt: str):
        raise NotImplementedError

    def __str__(self) -> str:
        return self.__class__.__name__


def gm_symbol_to_ts(symbol: str) -> str:
    """掘金代码转Tushare代码"""
    exchange, code = symbol.split(".")
    if exchange == 'SHSE':
        ts_symbol = code + ".SH"
    elif exchange == 'SZSE':
        ts_symbol = code + ".SZ"
    else:
        raise ValueError
    return ts_symbol


def ts_symbol_to_gm(symbol):
    """将 Tushare 代码转成掘金代码"""
    # print(f"symbol:{symbol}")
    code, ex = symbol.split(".")
    if ex == 'SH':
        gm_symbol = "SHSE." + code
    elif ex == 'SZ':
        gm_symbol = "SZSE." + code
    else:
        raise ValueError
    return gm_symbol
