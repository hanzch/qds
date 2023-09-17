# -*- coding: utf-8 -*-
# @Time : 2023/3/26/026 16:34
# @Author : 不归
# @FileName: kline.py
"""
通用K线管理，可以接入不同数据源，支持多线程。
"""
import signal
import time
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Lock, Manager, TimeoutError
from multiprocessing.pool import ThreadPool

import pandas as pd
from tqdm import tqdm

from conf.constants import *
from db.source.base import ADJ, DAY, MIN
from db.source.base import DataSource
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
        self.edt = datetime.now().strftime(dt_format)

        assert self.edt >= self.sdt, "结束日期必须大于开始日期！"

        hour = datetime.now().strftime('%H')
        if int(hour) < 17:
            self.edt = delta_datetime(strdt=self.edt, _format=dt_format, days=-1)
            log.warning(f"当天17点前,数据未更新,已将结束日期自动调整到前一天,调整后{self.edt=}")

        # 如果是周末，日期自动回调到周五，防止不必要的重复下载覆盖
        week = datetime.strptime(self.edt, dt_format).weekday() + 1
        if week > 5:
            self.edt = delta_datetime(strdt=self.edt, _format=dt_format, days=-week + 5)
            log.warning(f"周末日期自动调整：{week=},调整后：{self.edt=}")

    def load_cache(self):
        cache_name = f"{self.ds.__class__.__name__}_{self.ds.dtype.sql}"
        self.cache = Cache(cache_name)
        load_cache = self.cache.get()
        # 进度条读取
        self.over_map, self.all_num = load_cache if load_cache else [{}, 0]
        log.info(f"读取缓存{cache_name}数据，已发现{len(self.over_map)}条。")

    def save_err(self, ls: list) -> Cache | None:
        """
        保存下载过程中的错误列表
        :param ls: 参照格式：[(['code'], 'sdt', 'edt'), (['301390.SZ'], '20110908', '20230426'), ...]
        :return:
        """
        if not ls:
            log.success(f"{self.ds.dtype.sql}数据下载已结束，未发现出错的列表。")
            return None
        err_cache = Cache(f"err_ls/{self.ds.__class__.__name__}_{self.ds.dtype.sql}"
                          f"_{datetime.now():%Y%m%d%H%M%S}")
        err_cache.set(list(ls))
        log.error(f"错误列表已保存到文件中：{err_cache.path}\n列表内容：{ls}")
        return err_cache

    def load_err(self, err_cache: Cache):
        """
        加载下载错误列表并重新下载
        :param err_cache: 下载出错的cache对象
        :return:
        """
        log.warning(f"加载出错缓存并重新下载:{err_cache.path}")
        err_ls = err_cache.get()
        self.download(err_ls)
        err_cache.remove()

    def _download_k(self, codes_n, dic, err_ls: list):
        """
        多线程调用的K线下载函数
        :param codes_n: 需要下载的股票代码，开始时间，结束时间
        :param dic: 加载进多线程缓存的over_map，用于更新下载进度
        :param err_ls: 加载进多线程的错误列表，用于接收保存出错的标的
        :return:
        """
        symbol, sdt, edt = codes_n
        if self.ds.dtype is ADJ:
            max_edt = self.ds.update_adjust(symbol, sdt, edt)
        else:
            max_edt = self.ds.update_hist(symbol, sdt, edt)

        if not max_edt:
            log.error(f"{symbol}.{self.ds.dtype.sql}.{sdt}~{edt}插入数据库失败")
            with loc:
                err_ls.append(codes_n)
            return

        with loc:
            for key in symbol:
                old_sdt, old_edt = dic.get(key, ("99999999", "0"))
                end_edt = max(old_edt, max_edt)
                dic[key] = (min(old_sdt, sdt), end_edt)
            self.cache.set([dic.copy(), len(dic)])
            log.success(f"已完成{symbol}.{self.ds.dtype.sql}的数据下载更新,更新后日期{end_edt}。")

    def download(self, down_list: list) -> Cache:
        """
        下载方法，传入list可以直接调用 [(['code'],'sdt','edt'), ...] 具体格式参照参数示例
        :param down_list: list [(['601882.SH'], '20200207', '20200621'), (['601882.SH'], '20200621', '20201103'),  ...]
        :return: 返回出错的cache对象
        """

        with Manager() as manager:

            dic = manager.dict(self.over_map)
            thread_list = []
            err_ls = manager.list()
            log.info(f"当前数据源允许线程数：{self.ds.thread_num}")

            def signal_handler(signum, frame):
                self.over_map = dict(dic)
                err = self.save_err(err_ls)
                log.error(f"已手动终止程序,程序将自动保存错误列表,如有请使用load({err})加载处理")
                exit()

            signal.signal(signal.SIGINT, signal_handler)

            pool = ThreadPool(self.ds.thread_num)
            for d in down_list:
                thread_list.append(pool.apply_async(self._download_k, args=(d, dic, err_ls)))

            # 定义超时及进度条
            for t in tqdm(thread_list, desc="数据更新进度"):
                try:
                    t.get(timeout=15 * 60)
                except TimeoutError:
                    print("线程超时错误，已中断所有线程，请稍后重试...")
                    pool.terminate()

            pool.close()
            pool.join()
            self.over_map = dic.copy()
            err_cache = self.save_err(err_ls)
            return err_cache

    def _pre_check(self, symbol: list[str, str]):
        """
        多线程调用的预处理方法
        :param symbol: 传入的带日期的单支股票
        :return:
        """

        # 如果是分钟，日期自动+1，避免当日数据获取不到
        edt = delta_datetime(strdt=self.edt, _format=dt_format,
                             days=1) if self.ds.dtype is MIN else self.edt

        # 此处opendt=上市时间
        code, opendt = symbol
        if opendt > edt:
            log.warning(f"上市时间 > 结束时间 错误：{code=},{opendt=},{edt=}")
            return []
        # 增加(数据源，上市时间，开始时间)判断
        opendt = max(self.ds.source_date, opendt, self.sdt)

        # if opendt != self.sdt:
        #     # log.debug(f"当前开始时间已调整：{code=}, {self.ds.source_date=}，{opendt=}，{self.sdt=}")
        #     self.sdt = opendt

        up1, up2 = self.over_map.get(code, (None, None))
        if not up2 or not up1:
            # log.debug([[code, (opendt, edt)]])
            return [[code, (opendt, edt)]]
        else:
            up_ls = diff_date(up1, up2, opendt, edt)
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
        self.ds.set_source()
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
                        if self.ds.dtype is MIN:
                            _edt = delta_datetime(_sdt, int(self.ds.limit / 240))
                        else:
                            _edt = delta_datetime(_sdt, int(self.ds.limit))

                        # 如果日期段小于等于给定的日期段，则跳过
                        _edt = min(_edt, k[1])
                        down_list.append(([code], _sdt, _edt))
                        _sdt = _edt
                # log.warning(f"当前下载列表：{down_list}")
        return down_list

    def download_day(self, symbols: list = None):

        if not symbols:
            symbols = Symbols(ds=self.ds).update()

        self.ds.dtype = DAY
        self.load_cache()
        pre = self.pre(symbols)
        err_cache = self.download(pre)
        self.load_err(err_cache) if err_cache else None

    def download_min(self, symbols: list = None):
        if not symbols:
            symbols = Symbols(ds=self.ds).update()
        self.ds.dtype = MIN
        self.load_cache()
        pre = self.pre(symbols)
        err_cache = self.download(pre)
        self.load_err(err_cache) if err_cache else None

    def download_adj(self, symbols: list = None):
        if not symbols:
            symbols = Symbols(ds=self.ds).update()
        self.ds.dtype = ADJ
        self.load_cache()
        pre = self.pre(symbols)
        err_cache = self.download(pre)
        self.load_err(err_cache) if err_cache else None


@dataclass
class KLineQuery:
    ds: DataSource = None
    sdt: str = '19900101'  # 起始日期
    edt: str = '20221231'

    def __post_init__(self):
        assert self.ds, "ds 数据源不能为空"
        self.ds.dtype = self.ds.dtype if self.ds.dtype else DAY
        assert self.edt >= self.sdt, "结束日期必须大于开始日期！"

    def get_kline(self, symbol):
        t1 = time.time()
        df = self.ds.sel_kline_for_symbol(symbol, self.sdt, self.edt)
        t2 = time.time() - t1
        log.debug(f"查询{symbol}-{self.ds.dtype.sql}-{self.sdt}~{self.edt}完成，共计耗时：{t2} 秒")
        return df


def load_errs_cache(err_cache_name: str):
    """
    根据错误缓存列表的名字，重新尝试下载，如果成功会删除对应缓存文件
    :param err_cache_name:
    :return:
    """
    cache = Cache("err_ls/" + err_cache_name)
    path = cache.path

    if not path.exists():
        log.info(f"未发现缓存文件,请重新核对：{path=}")
        return

    ls = err_cache_name.split("_")
    ds = eval(ls[0] + "()")
    kd = KlineBase(ds=ds)
    kd.ds.dtype = eval(str(ls[1]).upper())
    kd.load_cache()

    err_ls = cache.get()
    kd.download(err_ls)
    cache.remove()


if __name__ == '__main__':
    from db.symbols import Symbols

    init()
    # ds = TSSource()
    # kd = KlineBase(ds=ds)
    # symbols = Symbols(ds=ds).update()
    # kd.download_day(symbols=symbols)
    # kd.download_adj(symbols=symbols)
    # kd.download_min(symbols=symbols)

    # # 加载错误缓存，重新尝试下载
    cache_name = "TSSource_min_20230905222004"
    """
    如需手动创建cache格式如下,TSSource_min_20230905222004.json
    [[["000951.SZ"],"20211108","20211211"],[["300284.SZ"],"20191223","20200125"]]

    """
    load_errs_cache(cache_name)
