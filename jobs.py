# -*- coding: utf-8 -*-
# @Time : 2023/4/23/023 10:30
# @Author : 不归
# @FileName: jobs.py.py
from datetime import datetime

from czsc import fsa
from pyinstrument import Profiler

from conf.constants import *
from db.kline import KlineBase, load_errs_cache
from db.source.ts import TSSource
from db.symbols import Symbols
from libs.tools import get_err_cache_names


def update_kline_job(**kwargs):
    err_ls = get_err_cache_names()
    for e in err_ls:
        load_errs_cache(e)

    ds = TSSource()
    kd = KlineBase(ds=ds)
    symbols = Symbols(ds=ds).update()
    kd.download_day(symbols=symbols)
    kd.download_adj(symbols=symbols)
    kd.download_min(symbols=symbols)

    if kwargs.get('feishu_app_id') and kwargs.get('feishu_app_secret'):
        fsa.push_message(f"更新数据任务完成：{KlineBase}！", **kwargs)
        files = ["error", "warning"]
        file_ls = [BASE_PATH / f'logs/{x}_{datetime.now():%Y-%m-%d}.log'
                   for x in files]
        for path in file_ls:
            if path.stat().st_size:
                fsa.push_message(path, msg_type='file', **kwargs)


if __name__ == '__main__':
    init()
    init_feishu_param()
    optim_conf = {
        # 飞书消息推送
        # 'feishu_app_id': os.getenv('feishu_app_id'),
        # 'feishu_app_secret': os.getenv('feishu_app_secret'),
        # 'feishu_members': [os.getenv('feishu_members')],
    }

    profiler = Profiler()
    profiler.start()

    update_kline_job(**optim_conf)

    profiler.stop()
    profiler.print()
