# -*- coding: utf-8 -*-
# @Time : 2023/3/13/013 21:42
# @Author : 不归
# @FileName: tools.py
"""
通用工具函数类
"""
import math
from pathlib import Path

from db.source.base import Dtype
from libs.dtTools import howdays


def check_path_exist(path):
    """check if path is not None and if dir exists, else mkdir"""
    if path:
        if not Path(path).is_dir():
            Path(path).mkdir(parents=True, exist_ok=True)


def diff_date(old_start, old_end, new_start, new_end):
    """
    根据4个日期计算去重部分，节约资源。
    :param old_start: str 8位字符串时间，"19900101"
    :param old_end: str 8位字符串时间，"19900101"
    :param new_start: str 8位字符串时间，"19900101"
    :param new_end: str 8位字符串时间，"19900101"
    :return:
    """

    if new_start >= old_start and new_end <= old_end:
        return []
    if new_end < old_start or new_start < old_start < new_end <= old_end:
        return [(new_start, old_start)]
    if new_start > old_end or new_end > old_end > new_start >= old_start:
        return [(old_end, new_end)]
    if new_start < old_start and new_end > old_end:
        return [(new_start, old_start), (old_end, new_end)]


def diff_date2(old_end, new_end):
    """
    根据2个日期计算去重部分，节约资源，以上方法的精简版，只保留结束日期。
    :param old_end:
    :param new_end:
    :return:
    """

    if new_end <= old_end:
        return []
    else:
        return [old_end, new_end]


def paging(date_kind: tuple, dtype: Dtype, limit: int) -> int:
    """
    - 模拟分页防止数据过大,等待时间过长，pg为计算出来的翻页数量
    - 根据日期差及周期，计算分页参数，向上取整
    :param date_kind:  格式（'19900101','20230415'）
    :param dtype: 用于数据类型判断，如：day,min,adj。 注意：对象的内容比较，用is方法
    :param limit: 数据源最大获取行数限制
    :return: int 最小为1，大于1，代表单次下载可以覆盖完整周期数据，否则需要分段
    """

    sdt, edt = date_kind
    day = howdays(sdt, edt) + 1
    if dtype in [Dtype.day, Dtype.adj]:
        return math.ceil(limit / day)
    elif dtype is Dtype.min:
        return math.ceil(limit / day / 4 / 60)
