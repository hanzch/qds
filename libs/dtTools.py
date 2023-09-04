#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/11/5 15:01
# @Author  : lishenluo
# @Email   : lishenluo@163.com
# @Update  : 2023/4/28 不归
"""
时间函数相关工具类
"""
import time
from datetime import datetime


def now_str(_format="%Y-%m-%d %H:%M:%S"):
    """
    获取当前日期时间
    :param _format: str 输出日期的时间格式，默认是"%Y-%m-%d %H:%M:%S",要显示毫秒"%Y-%m-%d %H:%M:%S.%f"
    :return: str 日期时间
    """
    return datetime.now().strftime(_format)


def now_timetag():
    """
    获取当前日期时间对应的时间戳（秒级）
    :return: int 秒级时间戳
    """
    return int(time.time())


def now_mtimetag():
    """
    获取当前日期时间对应的时间戳（milli毫秒级）
    :return:int 毫秒级时间戳
    """
    return int(round(time.time() * 1000))


def str2datetime(strdate: str = "2022-01-01", _format="%Y%-m-%d"):
    """
    字符串转datetime
    :param strdate: str 日期
    :param _format: str 输出日期的时间格式
    :return: datetime
    """
    return datetime.strptime(strdate, _format)


def str2timetag(strdt, _format="%Y-%m-%d %H:%M:%S"):
    """
    日期时间转换成秒级时间戳
    :param strdt: str，日期时间
    :param _format: str，传入的日期时间的格式,默认是"%Y-%m-%d %H:%M:%S"
    :return: int，时间戳(10位), 1679400548
    """
    datetime_mid = datetime.strptime(strdt, _format)
    return int(time.mktime(datetime_mid.timetuple()))


def str2mtimetag(strdt, _format="%Y-%m-%d %H:%M:%S.%f"):
    """
    日期时间转换成毫秒milli级时间戳
    :param strdt: str，日期时间
    :param _format: str，传入的日期时间的格式，默认是"%Y-%m-%d %H:%M:%S.%f"
    :return: int，时间戳
    """
    datetime_mid = datetime.strptime(strdt, _format)
    return int(time.mktime(datetime_mid.timetuple())) * 1000 + int(round(datetime_mid.microsecond / 1000.0))


def timetag2sec(time_tag: int):
    """
    毫秒级（13），微秒级（16），纳秒级（19），时间戳统一为s秒级（10）
    :param time_tag: int
    :return: int 秒级时间戳(10位)
    """
    assert type(time_tag) in [int, float], f"数据类型不符，当前数据为：{time_tag}，类型为{type(time_tag)}"

    if len(str(time_tag)) > 16:
        time_tag = time_tag / 10 ** 9
    elif len(str(time_tag)) > 13:
        time_tag = time_tag / 10 ** 6
    elif len(str(time_tag)) > 10:
        time_tag = time_tag / 10 ** 3

    return int(time_tag)


def timetag2datetime(time_tag):
    """
    时间戳转换成日期时间
    :param time_tag:时间戳，int，float，秒级或毫秒级时间戳都行
    :return:datetime
    """
    # 转化为秒级
    time_tag = timetag2sec(time_tag)
    datetime_array = datetime.fromtimestamp(time_tag)
    return datetime_array


def delta_datetime(strdt=None, days=0, hours=0, minutes=0, seconds=0, milliseconds=0, _format="%Y-%m-%d %H:%M:%S"):
    """
    获取某个时间间隔后的日期时间
    :param strdt: 参照日期时间，如果不填则使用当前日期时间
    :param days:间隔的天数，默认0
    :param hours:间隔小时数，默认0
    :param minutes:间隔的分钟数，默认0
    :param seconds:间隔的秒数，默认0
    :param milliseconds:间隔的毫秒数，默认0
    :param _format:参照日期时间格式和输出的日期时间格式
    :return:str
    """
    if strdt is None:
        order_millis = now_mtimetag()
    else:
        order_millis = str2mtimetag(strdt, _format)
    delta = days * 24 * 60 * 60 * 1000 + hours * 60 * 60 * 1000 + minutes * 60 * 1000 + seconds * 1000 + milliseconds
    return timetag2datetime(order_millis + delta).strftime(_format)


def howdays(strdate1, strdate2, _format="%Y%m%d"):
    """
    获取两个字符串日期之间的天数
    :param strdate1: str 日期,"19900101"
    :param strdate2: str 日期
    :param _format: 传入的日期时间的格式, 默认"%Y%m%d"
    :return:
    """
    dt1 = str2datetime(strdate1, _format)
    dt2 = str2datetime(strdate2, _format)
    return (dt2 - dt1).days


def conv_time(ct):
    """
    QMT自带时间戳函数
    conv_time(1476374400000) --> '20161014000000.000'
    """
    local_time = time.localtime(ct / 1000)
    data_head = time.strftime('%Y%m%d%H%M%S', local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = '%s.%03d' % (data_head, data_secs)
    return time_stamp


if __name__ == '__main__':
    a1 = now_str("%Y%m%d")
    print("a1获取当前时间指定格式Str：", a1, type(a1))
    # 20230321 <class 'str'>

    a3 = str2timetag(now_str())
    print("a3当前时间戳：", a3, type(a3))
    # 1682512547 <class 'int'>

    a4 = timetag2datetime(a3)
    print("a4时间戳转datetime:", a4, type(a4))
    # 2023-03-21 20:09:08 <class 'datetime.datetime'>

    a5 = str2timetag("2023-03-17 09:31:00")
    print("a5字符串格式转时间戳:", a5, type(a5))
    # 1679016660 <class 'int'>

    a6 = delta_datetime(strdt="20200101", _format="%Y%m%d", days=-1)
    print(a6)
