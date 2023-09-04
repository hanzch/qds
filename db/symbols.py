# -*- coding: utf-8 -*-
# @Time : 2023/3/26/026 16:45
# @Author : 不归
# @FileName: symbols.py
"""
封装获取股票标的相关的类和函数，如：获取A股代码，获取带上市日期的股票信息等，未完成。
"""
from dataclasses import dataclass

import numpy as np
import pandas as pd

from conf.constants import *
from db.source.base import DataSource
from db.source.ts import TSSource
from libs.cache import Cache


@dataclass
class Symbols:
    cfg = Cache("codes")
    ds: DataSource

    def __post_init__(self):
        assert self.ds, "ds 数据源不能为空"

    def update(self):
        """
        从数据源ds更新股票标的信息
        :param ds: DataSource 数据源
        :return: list 更新后的信息
        """
        if self.ds.update_symbols_info():
            self.cfg.remove()
            log.success(f"从数据源{self.ds}更新symbols已完成,已删除本地缓存。")
        return self.infos

    @property
    def infos(self) -> list:
        """
        返回带上市日期的所有股票代码
        :return: list [['300640.SZ', '20170417'], ['300642.SZ', '20170421'], ...]
        """
        res = self.cfg.get()
        if not res:
            df = DataSource.get_symbols_info()
            res = np.array(df).tolist()
            self.cfg.set(res)
        return res

    @property
    def codes(self) -> list:
        """
        返回所有股票代码
        :return: list ['600112.SH', '600509.SH', '600166.SH'...]
        """
        res = self.infos
        df = pd.DataFrame(res)
        return np.array(df[0]).tolist()


if __name__ == '__main__':
    init()
    ds = TSSource()
    s = Symbols(ds=ds)
    res = s.update()
    print(res[:3], type(res), len(res))

    a = s.codes
    b = s.infos
    print(a[:3], type(a), len(a), "\n", b[:3], type(b), len(b))
