# # -*- coding: utf-8 -*-
# # @Time : 2023/5/10/010 12:30
# # @Author : 不归
# # @FileName: qmt.py.py
# from dataclasses import dataclass
# from datetime import datetime
#
# import pandas as pd
# from xtquant import xtdata
# from xtquant.xtdata import get_instrument_detail
#
# from conf.constants import *
# from .base import DataSource
# from ...libs.dtTools import now_timetag, str2timetag, timetag2datetime, timetag2sec
#
#
# @dataclass
# class QMTSource(DataSource):
#     """
#     QMT数据源实现类
#     """
#
#     def __post_init__(self):
#         self.limit = 32400
#         self.thread_num = 1
#         self.source_date: str = "19900101"
#
#     def update_hist(self, symbols: list[str], sdt: str = "20230101", edt: str = "20230415"):
#         """
#         :param symbols: 股票代码
#         :param sdt: 开始时间
#         :param edt: 结束时间
#         :return: 成功为1，失败为0
#         """
#
#         def on_progress(data):
#             pass
#             # log.debug(data)
#
#         #  QMT从服务器先行缓存数据到本地，需占用本地文件空间，目录地址："userdata_mini\datadir"
#         #  http://docs.thinktrader.net/pages/36f5df/#下载历史行情数据
#         #  API   1分钟103次访问限制
#         xtdata.download_history_data2(symbols, self.freq.qmt, start_time=sdt, end_time=edt, callback=on_progress)
#
#         dict1 = xtdata.get_market_data(field_list=['time', 'open', 'close', 'high', 'low', 'volume', 'amount'],
#                                        stock_list=symbols,
#                                        period=self.freq.qmt,
#                                        start_time=sdt,
#                                        end_time=edt,
#                                        count=-1, dividend_type=self.fq.qmt, fill_data=False)
#
#         log.debug(f"{symbols}获取到{len(dict1)}列信息条")
#         df = pd.DataFrame()
#         keys = dict1.keys()
#
#         # QMT数据源为按单字段获取，需要单独处理
#         for k in keys:
#             if k not in ['suspendFlag', 'openInterest', 'settelementPrice']:
#                 df_ = pd.DataFrame(dict1[k])
#                 tmp = df_.T
#                 tmp['index'] = tmp.index
#                 tmp = pd.melt(tmp, id_vars="index", var_name='code', value_name=f'{k}')
#                 df = tmp if df.empty else pd.merge(df, tmp, on=['index', 'code'], how='inner')
#
#         log.info(f"{symbols}.{self.freq.sql}.{sdt}~{edt}获取到{df.shape[0]}条数据")
#
#         if not df.shape[0]:
#             sql = f"SELECT * FROM quant.gm_{self.freq.sql} Final where code='{symbols}' " \
#                   f"AND	`date` BETWEEN toDateTime('{sdt}') and toDateTime('{edt}')"
#             log.warning(f"{symbols}未从QMT找到数据，请确认。\n{sql}\n")
#             return 0
#
#         df.dropna(axis=0, subset=["open", "close", "high", "low"], inplace=True)
#         df.drop('index', axis=1, inplace=True)
#         df.rename(columns={"time": "date"}, inplace=True)
#         df['date'] = df['date'].apply(lambda x: datetime.fromtimestamp(x / 1000.0))
#         df["date"] = df["date"].dt.tz_localize('Asia/Shanghai')
#
#         # 此处代码主要用于判断数据是否有误，如QMT当时的错误脏数据。
#         max_tag = timetag2sec(df.date.iloc[-1:].values[0].astype(datetime))
#         min_tag = timetag2sec(df.date.iloc[0:].values[0].astype(datetime))
#         now_tag = now_timetag()
#         start_tag = str2timetag(sdt, "%Y%m%d")
#
#         # print(f"{max_tag=}\n{min_tag=}\n{now_tag=}\n{start_tag=}\n{sdt=}")
#
#         if max_tag > now_tag or min_tag < start_tag:
#             df.to_csv(f"脏数据{symbols}_{self.freq.sql}.csv", index=False)
#             log.error(
#                 f"查询结果出错，最大日期{timetag2datetime(max_tag)}，提交结束日期{edt} -"
#                 f" 最小日期{timetag2datetime(min_tag)}，提交开始日期{sdt}\n",
#                 "-" * 10)
#             return 0
#
#         else:
#             # df.to_csv(f"{symbols}_{self.freq.sql}.csv", index=False)
#             db_tab = f"quant.qmt_{self.freq.sql}"
#             if super()._to_clickhouse(db_tab, df):
#                 return timetag2datetime(max_tag).strftime("%Y%m%d")
#             else:
#                 return 0
#
#     def update_symbols_info(self):
#         """
#         QMT的获取指定行业代码,并更新基本信息
#          http://docs.thinktrader.net/pages/36f5df/#获取合约基础信息
#         get_instrument_detail(stock_code)
#         :return:
#         """
#         codes: list = xtdata.get_stock_list_in_sector('沪深A股')
#
#         def is_open(row):
#             # 此处 OpenDate 有可能返回19700101，代表未上市
#             if (int(row["ExpireDate"]) == 99999999 or int(row["ExpireDate"]) < int(row["OpenDate"])) \
#                     and int(row["OpenDate"]) > 19900101:
#                 return 1
#             else:
#                 return 0
#
#         df = pd.DataFrame()
#         for code in codes:
#             dic = get_instrument_detail(code)
#             df = pd.concat([df, pd.DataFrame([dic])], ignore_index=True)
#         assert not df.empty, "update_symbols_info 时未获取到数据"
#
#         df["code"] = df["InstrumentID"] + "." + df["ExchangeID"]
#         df["status"] = df.apply(is_open, axis=1)
#         df = df[["code", "InstrumentName", "OpenDate", "status"]]
#         df.rename(columns={"InstrumentName": "name", "OpenDate": "sdt"}, inplace=True)
#
#         db_tab = "quant.codes_info"
#         if super()._to_clickhouse(db_tab, df):
#             return df
#         return 0
#
#     def __str__(self):
#         return f"QMTSource(freq={self.freq.sql})"
