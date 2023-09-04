# -*- coding: utf-8 -*-
# @Time : 2023/5/10/010 12:30
# @Author : 不归
# @FileName: ts.py.py
import time
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
import tushare as ts

from conf.constants import *
from db.source.base import D, DataSource, Dtype, F1
from libs.dtTools import delta_datetime, now_str, now_timetag, str2datetime, str2timetag, timetag2datetime, timetag2sec


@dataclass
class TSSource(DataSource):

    def __post_init__(self):
        self.limit = 7920
        self.thread_num = 4

        ts_token = os.getenv("ts_token")
        assert ts_token, "请在.env配置中设置 TS_TOKEN"
        ts.set_token(ts_token)
        self.pro = ts.pro_api()

    def set_source_date(self):
        self.source_date = ("20090101", "19900101",)[self.freq is D]

    def _get_ts(self, symbol, sdt, edt):
        for n in range(1, 4):
            try:
                df = ts.pro_bar(ts_code=symbol, adj=self.fq.ts, freq=self.freq.ts, start_date=sdt, end_date=edt)
            except Exception:
                log.error(f"获取{symbol}数据出错，稍后进行第{n}次重试...")
                time.sleep(1)
            else:
                return df
        log.error(f"获取{symbol}数据出错，重试3次失败！")

    def update_hist(self, symbols: list[str], sdt: datetime, edt: datetime):

        # print('当前函数名称:', inspect.stack())

        log.info(f"正在获取{symbols}.{self.dtype}数据，时间范围{sdt}~{edt}请稍后...")
        err_ls = []
        df = pd.DataFrame()

        for symbol in symbols:
            log.debug(f"开始访问ts接口:{symbol=},{sdt=},{edt=}")
            df_ = self._get_ts(symbol, sdt, edt)
            if not isinstance(df_, pd.DataFrame) or df_.empty:
                tp = self.pro.suspend_d(suspend_type='S', start_date=sdt, end_date=edt, ts_code=symbol)
                if not tp.empty:
                    continue
                err_ls.append(([symbol], sdt, edt))

            df = pd.concat([df, df_], ignore_index=True)

        if err_ls:
            log.warning(err_ls)

        if df.empty:
            log.debug(f"未找到数据,可能是停牌{symbols, sdt, edt}")
            return edt

        if self.freq == F1:
            df = df[['ts_code', 'trade_time', 'open', 'high', 'low', 'close', 'vol', 'amount']]
            df.rename(columns={"trade_time": "date", "ts_code": "code", "vol": "volume"}, inplace=True)
            df['date'] = df['date'].apply(str2datetime, _format="%Y-%m-%d %H:%M:%S")
            df['volume'] = df['volume'].astype('Int64')
        else:
            df = df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount']]
            df.rename(columns={"trade_date": "date", "ts_code": "code", "vol": "volume"}, inplace=True)
            df['date'] = pd.to_datetime(df['date'])
            df['volume'] = df['volume'] * 100
            df['amount'] = df['amount'] * 1000

        # 删掉全为空值的行
        df.dropna(axis=0, how='all')
        # 自动处理amount空值
        df['amount'] = df['amount'].fillna(df['close'] * df['volume'])

        log.info(f"{symbols}.{self.dtype}.{sdt}~{edt}获取到{df.shape[0]}条数据")

        if not df.shape[0]:
            sql = f"SELECT * FROM quant.ts_{self.dtype} Final where code='{symbols}' " \
                  f"AND	`date` BETWEEN toDateTime('{sdt}') and toDateTime('{edt}')"
            log.warning(f"{symbols}未从TS找到数据，请确认。\n{sql}\n")
            return 0

        # 此处代码主要用于判断数据是否有误，如QMT当时的错误脏数据。
        # df["date"] = df["date"].dt.tz_localize('Asia/Shanghai')
        df.sort_values(by=["code", "date"], inplace=True, ascending=False)
        max_tag = timetag2sec(df.max().date.value)
        min_tag = timetag2sec(df.min().date.value)
        now_tag = now_timetag()
        start_tag = str2timetag(sdt, "%Y%m%d")

        # log.debug(f"{symbols=} -{sdt=}~{edt=} - {timetag2datetime(min_tag)}~{timetag2datetime(max_tag)}\n")

        # if max_tag > now_tag or min_tag < start_tag:
        #     df.to_csv(f"脏数据{symbols}_{self.dtype}.csv", index=False)
        #     log.error(
        #         f"查询结果出错，最大日期{timetag2datetime(max_tag)}，提交结束日期{edt} -"
        #         f" 最小日期{timetag2datetime(min_tag)}，提交开始日期{sdt}\n",
        #         "-" * 10)
        #     return 0
        #
        # else:
        # df.to_csv(f"{symbols}_{self.dtype}.csv", index=False)
        db_tab = f"quant.ts_{self.dtype}"
        if super()._to_clickhouse(db_tab, df):
            if self.dtype is Dtype.min:
                stime = timetag2datetime(max_tag).strftime(self.dt_format)
                return delta_datetime(strdt=stime, _format=self.dt_format, days=1)
            return timetag2datetime(max_tag).strftime(self.dt_format)
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
        for symbol in symbols:
            df_ = self.pro.adj_factor(ts_code=symbol, start_date=sdt, end_date=edt)
            if not isinstance(df_, pd.DataFrame) or df_.empty:
                tp = self.pro.suspend_d(suspend_type='S', start_date=sdt, end_date=edt, ts_code=symbol)
                if not tp.empty:
                    continue
                err_ls.append(([symbol], sdt, edt))

            df = pd.concat([df, df_], ignore_index=True)
            time.sleep(60 / 1000 * self.thread_num / 2)

        if err_ls:
            log.warning(err_ls)

        log.info(f"{symbols}.adj.{sdt}~{edt}获取到{df.shape[0]}条数据")

        if df.empty:
            log.debug(f"未找到数据,可能是停牌{symbols, sdt, edt}")
            return edt

        if not df.shape[0]:
            sql = f"SELECT * FROM quant.ts_adj Final where code='{symbols}' " \
                  f"AND	`date` BETWEEN toDateTime('{sdt}') and toDateTime('{edt}')"
            log.warning(f"{symbols}未从TS找到数据，请确认。\n{sql}\n")
            return 0
        df.rename(columns={"trade_date": "date", "ts_code": "code", "adj_factor": "num"}, inplace=True)
        df['date'] = df['date'].apply(str2datetime, _format=self.dt_format)

        # 此处代码主要用于判断数据是否有误，如QMT当时的错误脏数据。
        # df["date"] = df["date"].dt.tz_localize('Asia/Shanghai')
        df.sort_values(by=["code", "date"], inplace=True, ascending=False)
        max_tag = timetag2sec(df.max().date.value)
        min_tag = timetag2sec(df.min().date.value)
        now_tag = now_timetag()
        start_tag = str2timetag(sdt, "%Y%m%d")

        # log.debug(f"{symbols=} -{sdt=}~{edt=} - {timetag2datetime(min_tag)}~{timetag2datetime(max_tag)}\n")

        if max_tag > now_tag or min_tag < start_tag:
            df.to_csv(f"脏数据{symbols}_{self.dtype}.csv", index=False)
            log.error(
                f"查询结果出错，最大日期{timetag2datetime(max_tag)}，提交结束日期{edt} -"
                f" 最小日期{timetag2datetime(min_tag)}，提交开始日期{sdt}\n",
                "-" * 10)
            return 0

        else:
            db_tab = f"quant.ts_{self.dtype}"
            if super()._to_clickhouse(db_tab, df):
                return timetag2datetime(max_tag).strftime(self.dt_format)
            return 0
