# -*- coding: utf-8 -*-
# @Time : 2023/4/23/023 10:30
# @Author : 不归
# @FileName: jobs.py
from datetime import datetime

from czsc import fsa

from conf.constants import *
from db.kline import KlineBase
from db.source.ts import TSSource
from db.symbols import Symbols


def update_kline_job(**kwargs):
    ds = TSSource()
    kd = KlineBase(ds=ds)
    symbols = Symbols(ds=ds).update()
    kd.down_day(symbols=symbols)
    kd.down_min(symbols=symbols)
    kd.down_adj(symbols=symbols)

    if kwargs.get('feishu_app_id') and kwargs.get('feishu_app_secret'):
        fsa.push_message(f"{datetime.now():%Y-%m-%d}更新数据任务完成！", **kwargs)

        files = ["error", "warning"]
        file_ls = [BASE_PATH / f'logs/{x}_{datetime.now():%Y-%m-%d}.log'
                   for x in files]
        for path in file_ls:
            if path.stat().st_size:
                fsa.push_message(path, msg_type='file', **kwargs)


if __name__ == '__main__':
    init()
    init_feishu_parm()
    optim_conf = {
        # 飞书消息推送
        'feishu_app_id': os.getenv('feishu_app_id'),
        'feishu_app_secret': os.getenv('feishu_app_secret'),
        'feishu_members': [os.getenv('feishu_members')],
    }

    update_kline_job(**optim_conf)
    print()
    print()
