# -*- coding: utf-8 -*-
# @Time : 2023/5/10/010 12:29
# @Author : 不归
# @FileName: gm.py.py
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from gm.api import get_symbol_infos, history, set_token

from conf.constants import *
from .base import D, DataSource, gm_symbol_to_ts, ts_symbol_to_gm
from ...libs.dtTools import now_timetag, str2timetag, timetag2datetime, timetag2sec


@dataclass
class GMSource(DataSource):
    """
    掘金，简称GM。数据源实现类
    """

    def __post_init__(self):
        self.source_date = "19910101" if self.freq is D else "20170101"
        self.limit: int = 32400  # 单次获取行限制,建议设置为240的倍数
        self.thread_num: int = 4  # 默认允许的多线程数量
        # 设置token， 查看已有token ID,在用户-密钥管理里获取
        token = os.getenv("gm_token")
        assert token, "请在.env配置中设置 GM_TOKEN"
        set_token(token)

    def update_hist(self, symbols: list[str], sdt: str = "", edt: str = ""):
        """
        获取历史数据
        :param symbols: 股票代码
        :param sdt: 开始时间
        :param edt: 结束时间
        :return: 成功返回str格式最大时间（19710113082750），失败为0
        """

        sdt1 = pd.to_datetime(sdt, format="%Y%m%d")
        edt1 = pd.to_datetime(edt, format="%Y%m%d") + timedelta(days=1)
        # log.debug(f"正在获取{symbols}.{self.freq.sql}数据，时间范围{sdt}~{edt}请稍后...")
        # 把股票代码默认格式（'600001.SH'）转换为GM支持的格式
        symbols = [ts_symbol_to_gm(ll) for ll in symbols]
        df = history(symbol=symbols, start_time=sdt1, end_time=edt1,
                     frequency=self.freq.gm, adjust=self.fq.gm,
                     fields='eob, symbol, open, close, high, low, volume, amount',
                     fill_missing='Last', df=True)

        if not df.shape[0]:
            sql = f"SELECT * FROM quant.gm_{self.freq.sql} Final where code='{symbols}' " \
                  f"AND	`date` BETWEEN toDateTime('{sdt}')  and toDateTime('{edt}')"
            log.warning(f"{symbols}未找到数据，可能是新股上市，请确认。\n{sql}\n")
            return 0

        # 把股票GM的代码转为默认格式（'600001.SH'）
        df['code'] = df['symbol'].apply(gm_symbol_to_ts)
        df.drop('symbol', axis=1, inplace=True)
        df.rename(columns={"eob": "date"}, inplace=True)
        db_tab = f"quant.gm_{self.freq.sql}"

        # np.datetime64 转时间戳,再转化为s秒级别
        max_tag = timetag2sec(df.date.iloc[-1:].values[0].astype(datetime))
        min_tag = timetag2sec(df.date.iloc[0:].values[0].astype(datetime))
        now_tag = now_timetag()
        start_timetag = str2timetag(sdt, "%Y%m%d")

        # print(f"max_tag:{max_tag},{type(max_tag)}\nmin_tag:{min_tag},{type(min_tag)}\nnow_tag:{now_tag},"
        #       f"{type(now_tag)}\nstart_tag:{start_timetag},{type(start_timetag)}")

        # 此处代码主要用于判断数据是否有误，如QMT当时的错误脏数据。
        if max_tag > now_tag or min_tag < start_timetag:
            df.to_csv(f"脏数据{symbols}_{self.freq.sql}.csv", index=False)
            log.error(
                f"查询结果出错，最大日期{timetag2datetime(max_tag)}，提交日期{edt} -"
                f" 最小日期{timetag2datetime(min_tag)}，提交日期{sdt}，请打开CSV文件核对\n",
                "-" * 10)
            return 0

        else:
            df["date"] = df["date"].dt.tz_localize(None)
            # log.debug(f"{symbols}本次获取最大日期为:{df.iloc[-1].tolist()[0]}")
            df.volume = df.volume.astype(np.uint32)
            df.amount = df.volume.astype(np.uint64)
            log.debug(f"{symbols}本次共获取到{df.shape[0]}条数据。")
            if super()._to_clickhouse(db_tab, df):
                return timetag2datetime(max_tag).strftime("%Y%m%d")
            else:
                return 0

    def update_symbols_info(self):
        """
        https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#get-symbol-infos-查询标的基本信息
        get_symbol_infos("1010", sec_type2="101001", exchanges=None, symbols=None, df=True)
        1010: 股票，1020: 基金， 1030: 债券，1040: 期货， 1050: 期权，1060: 指数
        sec_type2 - 股票 101001:A 股，101002:B 股，101003:存托凭证
        - 基金 102001:ETF，102002:LOF，102005:FOF
        - 债券 103001:可转债，103003:国债，103006:企业债，103008:回购
        - 期货 104001:股指期货，104003:商品期货，104006:国债期货
        - 期权 105001:股票期权，105002:指数期权，105003:商品期权
        - 指数 106001:股票指数，106002:基金指数，106003:债券指数，106004:期货指数
        :return:
        """

        # 所有A股基本信息
        df = get_symbol_infos(1010, sec_type2=101001, df=True)

        # db_tab = "kcodes"
        # print(stocks)
        # super()._to_clickhouse(db_tab, stocks)

        return df

    def __str__(self):
        return f"GMSource(freq={self.freq.sql}"
