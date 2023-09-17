# -*- coding: utf-8 -*-
# @Time : 2023/5/10/010 12:30
# @Author : 不归
# @FileName: ts.py
"""
Tushare数据源的实现类
"""
import time
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
import tushare as ts

from conf.constants import *
from db.source.base import ADJ, DAY, DataSource, MIN
from libs.dtTools import delta_datetime, now_str, str2datetime, timetag2datetime


@dataclass
class TSSource(DataSource):

    def __post_init__(self):
        self.thread_num = 4

        ts_token = os.getenv("ts_token")
        assert ts_token, "请在.env配置中设置 TS_TOKEN"
        ts.set_token(ts_token)
        self.pro = ts.pro_api()

    def set_source(self):
        self.source_date = ("19900101", "20090101")[self.dtype is MIN]
        self.limit = (5900, 7920)[self.dtype is MIN]

    def _get_ts(self, symbol, sdt, edt):
        log.debug(f"开始访问ts接口:{symbol=},{sdt=},{edt=}")
        for n in range(1, 4):
            try:
                if self.dtype in [MIN]:
                    df = ts.pro_bar(ts_code=symbol, adj=self.fq.ts, freq=self.dtype.ts, start_date=sdt, end_date=edt)
                elif self.dtype in [DAY]:
                    df = self.pro.daily(ts_code=symbol, start_date=sdt, end_date=edt)
                elif self.dtype in [ADJ]:
                    df = self.pro.adj_factor(ts_code=symbol, start_date=sdt, end_date=edt)
                time.sleep(60 / 1000 * self.thread_num)
            except Exception as e:
                log.error(e)
                log.error(f"获取{symbol}数据出错，稍后进行第{n}次重试...")
                time.sleep(1)
            else:
                return df
        log.error(f"获取{symbol=},{sdt=},{edt=}数据出错，重试3次失败！")

    def update_hist(self, symbols: list[str], sdt: datetime, edt: datetime):

        # print('当前函数名称:', inspect.stack())

        log.info(f"正在获取{symbols}.{self.dtype.sql}数据，时间范围{sdt}~{edt}请稍后...")
        err_ls = []
        df = pd.DataFrame()

        if self.dtype is DAY:
            n = 2000
            for s in [symbols[i:i + n] for i in range(0, len(symbols), n)]:
                str_symbols = ','.join(s)
                df_ = self._get_ts(str_symbols, sdt, edt)
                if not isinstance(df_, pd.DataFrame) or df_.empty:
                    err_ls.append((symbols, sdt, edt))
                    continue
                df = pd.concat([df, df_], ignore_index=True)
        else:
            for symbol in symbols:
                df_ = self._get_ts(symbol, sdt, edt)
                if not isinstance(df_, pd.DataFrame) or df_.empty:
                    tp = self.pro.suspend_d(suspend_type='S', start_date=sdt, end_date=edt, ts_code=symbol)
                    if tp.empty:
                        err_ls.append(([symbol], sdt, edt))
                    continue
                df = pd.concat([df, df_], ignore_index=True)

        if df.empty:
            log.warning(f"未找到数据,可能是停牌{symbols, sdt, edt}")
            return edt

        if err_ls:
            log.warning(f"部分数据未找到，请关注：{err_ls}")

        time_field = ("trade_date", "trade_time")[self.dtype is MIN]
        df = df[['ts_code', time_field, 'open', 'high', 'low', 'close', 'vol', 'amount']]
        df.rename(columns={time_field: "date", "ts_code": "code", "vol": "volume"}, inplace=True)

        if self.dtype is MIN:
            df['date'] = df['date'].apply(str2datetime, _format="%Y-%m-%d %H:%M:%S")
        else:
            df['date'] = pd.to_datetime(df['date'])
            df['volume'] = df['volume'] * 100
            df['amount'] = df['amount'] * 1000

        # 删掉全为空值的行
        df.dropna(axis=0, how='all')
        # 自动处理amount空值
        df['amount'] = df['amount'].fillna(df['close'] * df['volume'])

        if not df.shape[0]:
            sql = f"SELECT * FROM quant.ts_{self.dtype.sql} Final where code='{symbols}' " \
                  f"AND	`date` BETWEEN toDateTime('{sdt}') and toDateTime('{edt}')"
            log.warning(f"{symbols}未从TS找到数据，请确认。\n{sql}\n")
            return 0

        log.info(f"{symbols}.{self.dtype.sql}.{sdt}~{edt}获取到{df.shape[0]}条数据")

        # df["date"] = df["date"].dt.tz_localize('Asia/Shanghai')
        df.sort_values(by=["code", "date"], inplace=True, ascending=False)
        max_tag = timetag2datetime(df.max().date.value).strftime(dt_format)
        min_tag = timetag2datetime(df.min().date.value).strftime(dt_format)

        # 此处代码主要用于判断数据是否有误，如QMT当时的错误脏数据。
        if max_tag > edt or min_tag < sdt:
            df.to_csv(f"zang_{symbols}_{self.dtype.sql}.csv", index=False)
            log.error(
                f"查询结果出错，{edt=}-{sdt=},{min_tag=}-{max_tag=}")
            return 0

        db_tab = f"quant.ts_{self.dtype.sql}"
        if super()._to_clickhouse(db_tab, df):
            if self.dtype is MIN:
                return delta_datetime(strdt=max_tag, _format=dt_format, days=1)
            return max_tag
        return 0

    def update_symbols_info(self):
        """
        更新股票基本信息
        :return: 1表示成功，0表示失败
        """
        df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,list_date,delist_date')
        df1 = self.pro.stock_basic(exchange='', list_status='D', fields='ts_code,name,list_date,delist_date')
        tp = self.pro.suspend_d(suspend_type='S', trade_date=now_str("%Y%m%d"))
        tp_code = np.array(tp.ts_code).tolist()
        assert not df.empty, "update_symbols_info 时未获取到数据"
        # 这里是先创建一个空列，然后再根据条件进行赋值
        df['status'] = True
        df.loc[df['ts_code'].isin(tp_code), 'status'] = False
        df1['status'] = False
        df = pd.concat([df, df1], ignore_index=True)
        df.rename(columns={"list_date": "sdt", "ts_code": "code", "delist_date": "edt"}, inplace=True)
        db_tab = "quant.codes"
        return super()._to_clickhouse(db_tab, df)

    def update_adjust(self, symbols: list[str], sdt: datetime, edt: datetime):

        err_ls = []
        df = pd.DataFrame()
        n = 2000
        for s in [symbols[i:i + n] for i in range(0, len(symbols), n)]:
            str_symbols = ','.join(s)
            df_ = self._get_ts(str_symbols, sdt, edt)
            if not isinstance(df_, pd.DataFrame) or df_.empty:
                err_ls.append((symbols, sdt, edt))
                continue
            df = pd.concat([df, df_], ignore_index=True)

        if df.empty:
            log.warning(f"未找到数据,可能是停牌{symbols, sdt, edt}")
            return edt

        if err_ls:
            log.warning(f"部分数据未找到，请关注：{err_ls}")

        log.info(f"{symbols}.{self.dtype.sql}.{sdt}~{edt}获取到{df.shape[0]}条数据")

        df.rename(columns={"trade_date": "date", "ts_code": "code", "adj_factor": "num"}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])

        df.sort_values(by=["code", "date"], inplace=True, ascending=False)
        max_tag = timetag2datetime(df.max().date.value).strftime(dt_format)
        min_tag = timetag2datetime(df.min().date.value).strftime(dt_format)

        # 此处代码主要用于判断数据是否有误，如QMT当时的错误脏数据。
        if max_tag > edt or min_tag < sdt:
            df.to_csv(f"脏_{symbols}_{self.dtype.sql}.csv", index=False)
            log.error(
                f"查询结果出错，{edt=}-{sdt=},{min_tag=}-{max_tag=}")
            return 0

        db_tab = f"quant.ts_{self.dtype.sql}"
        if super()._to_clickhouse(db_tab, df):
            return max_tag
        return 0

    def sel_kline_for_symbol(self, symbol: str, sdt: str, edt: str):
        sdt = sdt[:4] + '-' + sdt[4:6] + '-' + sdt[6:]
        edt = edt[:4] + '-' + edt[4:6] + '-' + edt[6:]
        db_tab = f"quant.ts_{self.dtype.sql}"
        sql = f"SELECT * FROM {db_tab} final WHERE code='{symbol}' AND date BETWEEN '{sdt}' AND '{edt}'"
        df = super()._query_clickhouse(sql)
        return df
