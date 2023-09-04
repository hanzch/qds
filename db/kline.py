# -*- coding: utf-8 -*-
# @Time : 2023/3/26/026 16:34
# @Author : 不归
# @FileName: kline.py
"""
通用K线管理，可以接入不同数据源，支持多线程。
"""
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Lock, Manager, TimeoutError
from multiprocessing.pool import ThreadPool

import pandas as pd
from tqdm import tqdm

from conf.constants import *
from db.source.base import D, Dtype, F1
from db.source.base import DataSource
from db.source.ts import TSSource
from libs.cache import Cache
from libs.dtTools import delta_datetime
from libs.tools import diff_date, paging

CPU_SIZE = 4  # 并发计算的核心数量
loc = Lock()


@dataclass
class KlineBase:
    """
    K线数据下载基类
    :param ds: DataSource  # 传入的数据源，可以是GMSource
    :param sdt: str 字符串格式19900101 格式
    :param edt: str
    :param over_map: dict 用于装入已完成的股票代码及历史数据进度
    :param cache: Cache 缓存类读写进度
    """

    ds: DataSource = None
    sdt: str = '19900101'  # A股起始日期
    edt: str = ''
    over_map = None
    all_num = 0
    cache: Cache = None

    def __post_init__(self):
        assert self.ds, "ds 数据源不能为空"
        self.ds.set_source_date()
        self.edt = datetime.now().strftime(self.ds.dt_format)

    def load_cache(self):
        cache_name = f"{self.ds.__class__.__name__}_{self.ds.dtype}"
        self.cache = Cache(cache_name)
        load_cache = self.cache.get()
        # 进度条读取
        self.over_map, self.all_num = load_cache if load_cache else [{}, 0]
        log.info(f"读取缓存{cache_name}数据，已发现{len(self.over_map)}条。")
        # log.debug(f"缓存数据:{self.over_map}")

    def save_err(self, ls: list):
        """
        保存下载过程中的错误列表
        :param ls: 参照格式：[(['code'], 'sdt', 'edt'), (['301390.SZ'], '20110908', '20230426'), ...]
        :return:
        """
        if not ls:
            log.success(f"{self.ds.dtype}数据均已下载完成，未发现出错的列表。")
            return ""
        ls_cache = Cache(f"err_ls/{self.ds.__class__.__name__}_{self.ds.dtype}"
                         f"_{datetime.now():%Y%m%d%H%M%S}")
        ls_cache.set(list(ls))
        log.success(f"错误列表已保存到文件中：{ls_cache.cfg_path}\n列表内容：{ls}")
        return ls_cache.cfg_path

    def load_err(self, fpath: str):
        """
        加载下载错误列表并重新下载
        :param fpath: 错误列表的名字
        :return:
        """
        self.load_cache()
        fpath = Path(fpath.rstrip('.pickle').rstrip('.json'))
        cache = Cache(fpath)
        down = cache.get()
        self.down(down)
        cache.remove()

    def _down_k(self, codes_n, dic, err_ls: list):
        """
        多线程调用的K线下载函数
        :param codes_n:
        :param dic:
        :return:
        """
        symbol, sdt, edt = codes_n
        if self.ds.dtype is Dtype.adj:
            max_edt = self.ds.update_adjust(symbol, sdt, edt)
        else:
            max_edt = self.ds.update_hist(symbol, sdt, edt)

        if not max_edt:
            log.error(f"{symbol}.{self.ds.dtype}.{sdt}~{edt}插入数据库失败")
            with loc:
                err_ls.append(codes_n)
            return

        with loc:
            for key in symbol:
                old_sdt, old_edt = dic.get(key, ("99999999", "0"))
                dic[key] = (min(old_sdt, sdt), max(old_edt, max_edt))
            self.cache.set([dic.copy(), len(dic)])
            log.success(f"已完成{symbol}.{self.ds.dtype}的数据下载更新,更新后日期{max_edt}。")

    def down(self, down_list: list):
        """
        下载方法，传入list可以直接调用 [(['code'],'sdt','edt'), ...] 具体格式参照参数示例
        :param down_list: list [(['601882.SH'], '20200207', '20200621'), (['601882.SH'], '20200621', '20201103'),  ...]
        :return:
        """
        # print(down_list)
        with Manager() as manager:
            dic = manager.dict(self.over_map)
            thread_list = []
            err_ls = manager.list()
            log.info(f"当前数据源允许线程数：{self.ds.thread_num}")
            pool = ThreadPool(self.ds.thread_num)
            for d in down_list:
                thread_list.append(pool.apply_async(self._down_k, args=(d, dic, err_ls)))

            # 定义超时及进度条
            for t in tqdm(thread_list, desc="数据更新进度"):
                try:
                    t.get(timeout=15 * 60)
                except TimeoutError:
                    log.error("线程超时错误，请稍后自动重试...")

            pool.close()
            pool.join()
            self.over_map = dict(dic)
            self.save_err(err_ls)
            return list(err_ls)

    def _pre_check(self, symbol: list[str, str]):
        """
        多线程调用的预处理方法
        :param symbol: 传入的带日期的单支股票
        :return:
        """
        # 此处opendt=上市时间
        code, opendt = symbol
        if opendt > self.edt:
            log.error(f"上市时间 > 结束时间 错误：{code=},{opendt=},{self.edt=}")
            return []
        # 增加(数据源，上市时间，开始时间)判断
        # log.debug(f"{self.ds.source_date=},{opendt=},{self.sdt=}")
        opendt = max(self.ds.source_date, opendt, self.sdt)

        # if opendt != self.sdt:
        #     # log.debug(f"当前开始时间已调整：{code=}, {self.ds.source_date=}，{opendt=}，{self.sdt=}")
        #     self.sdt = opendt

        up1, up2 = self.over_map.get(code, (None, None))
        # log.debug(f"{code=},{up1=},{up2=}")
        if not up2 or not up1:
            # log.debug([[code, (opendt, self.edt)]])
            return [[code, (opendt, self.edt)]]
        else:
            up_ls = diff_date(up1, up2, opendt, self.edt)
            if not up_ls:
                # log.debug(f"数据已存在，自动跳过： {code=} - {up1=}~{up2=}, {opendt=}~{self.edt=}")
                return []
            return [[code, (up[0], up[1])] for up in up_ls]

    def pre(self, symbols: list):
        """
        根据股票列表，计算需要补全的数据段，返回list。注意：symbols需要带有上市日期，格式如下：
        :param symbols: list [['code', 'sdt'], ['300642.SZ', '20170421'], ...]
        :return: list [(['code'], 'sdt', 'edt'), (['601882.SH'], '20200621', '20201103'),  ...]
        """

        assert self.edt >= self.sdt, "结束日期必须大于开始日期！"

        hour = datetime.now().strftime('%H')
        if int(hour) < 17:
            self.edt = delta_datetime(strdt=self.edt, _format=self.ds.dt_format, days=-1)
            log.warning("当天17点前，数据未更新，已将结束日期自动调整到前一天")

        # 如果是周末，日期自动回调到周五，防止不必要的重复下载覆盖
        week = datetime.strptime(self.edt, self.ds.dt_format).weekday() + 1
        log.debug(f"{week=}")
        if week > 5:
            self.edt = delta_datetime(strdt=self.edt, _format=self.ds.dt_format, days=-week + 5)

        # 如果是分钟，日期自动+1，避免当日数据获取不到
        if self.ds.dtype == Dtype.min:
            self.edt = delta_datetime(strdt=self.edt, _format=self.ds.dt_format, days=1)

        assert symbols, "没有获取到沪深A股，请重新确认。"
        self.all_num = len(symbols)
        log.debug(f"当前共获取到{self.all_num}支股票。")

        pool = ThreadPool(CPU_SIZE)
        res = pool.map(self._pre_check, symbols)
        pre_list = []
        for tmp in res:
            pre_list.extend(tmp)

        pool.close()
        pool.join()

        df = pd.DataFrame(pre_list, columns=["code", "date"])
        # 查看有几种不同的日期段
        # date_kind = df['date'].unique()
        # 输出为字典
        date_kind = df['date'].value_counts().to_dict()
        log.debug(f"{date_kind=}")

        down_list = []
        # 遍历每一个日期段
        for k in date_kind:
            codes = df[(df['date'] == k)].code.to_list()
            pg = paging(k, self.ds.dtype, self.ds.limit)
            tmp, num = 0, len(codes)
            if pg > 1:
                while tmp < num:
                    next_tmp = tmp + pg
                    i1 = min(next_tmp, num)
                    codes_n = codes[tmp:i1]
                    down_list.append((codes_n, k[0], k[1]))
                    tmp = i1
            else:
                for code in codes:
                    _sdt, _edt = k[0], k[0]
                    while _edt < k[1]:
                        # 如果频率为F1
                        if self.ds.dtype is Dtype.min:
                            _edt = delta_datetime(_sdt, int(self.ds.limit / 240))
                        else:
                            _edt = delta_datetime(_sdt, int(self.ds.limit))

                        # 如果日期段小于等于给定的日期段，则跳过
                        _edt = min(_edt, k[1])
                        down_list.append(([code], _sdt, _edt))
                        _sdt = _edt
                # log.warning(f"当前下载列表：{down_list}")
        return down_list

    def down_day(self, symbols: list = None):

        if not symbols:
            symbols = Symbols(ds=self.ds).update()

        self.ds.freq = D
        self.ds.dtype = Dtype.day
        self.load_cache()
        pre = self.pre(symbols)
        self.down(pre)

    def down_min(self, symbols: list = None):
        if not symbols:
            symbols = Symbols(ds=self.ds).update()
        self.ds.freq = F1
        self.ds.dtype = Dtype.min
        self.load_cache()
        pre = self.pre(symbols)
        self.down(pre)

    def down_adj(self, symbols: list = None):
        if not symbols:
            symbols = Symbols(ds=self.ds).update()
        self.ds.dtype = Dtype.adj
        self.load_cache()
        pre = self.pre(symbols)
        self.down(pre)


class KLineQuery:
    ...


def save(ls: list):
    if not ls:
        log.info("数据均已下载完成，未发现出错的列表。")
        return ""
    ls_cache = Cache(f"err_ls_{datetime.now():%Y%m%d%H%M%S}.pickle")
    ls_cache.set(list(ls))
    log.success(f"错误列表已保存到文件中：{ls_cache.cfg_path}")
    return ls_cache.cfg_path


def load(cfg_path: str):
    return Cache(cfg_path).get()


if __name__ == '__main__':
    from db.symbols import Symbols

    init()
    ds = TSSource()
    kd = KlineBase(ds=ds)
    symbols = Symbols(ds=ds).update()
    kd.down_day(symbols=symbols)
    kd.down_min(symbols=symbols)
    kd.down_adj(symbols=symbols)
